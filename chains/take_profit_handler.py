import asyncio
import json
import os

from tg_bot import TelegramClient
from utils import get_logger
from config import TP_LADDERS, TP_CACHE_PATH, PRICE_UPDATE_DELAY
from .dexes import DexscreenerClient



class TakeProfitHandler:

    def __init__(
        self,
        tg_client: TelegramClient,
        trade_handler,
        chain_name: str,
    ):
        self.logger = get_logger(f"TP_{chain_name}")
        self.tg_client = tg_client
        self.trade_handler = trade_handler
        self.chain_name = chain_name
        self.dexscreener = DexscreenerClient()
        self.ds_chain_id = chain_name.lower()
        self._cache: dict = {}
        self._tasks: list[asyncio.Task] = []
        self._cache_path = os.path.join(TP_CACHE_PATH, f'{chain_name}_TP_cache.json')

    # ------------------------------------------------------------------
    # Async initializer — call after construction
    # ------------------------------------------------------------------

    @classmethod 
    async def create(cls, tg_client: TelegramClient, trade_handler, chain_name: str):
        instance = cls(tg_client, trade_handler, chain_name)
        await instance._initialize()
        return instance

    async def _initialize(self):
        await self._load_cache()
        await self._restore_from_cache()

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    async def _load_cache(self):
        if os.path.exists(self._cache_path):
            try:
                with open(self._cache_path, 'r', encoding='utf-8') as f:
                    self._cache = json.load(f)
                self.logger.info(f"Loaded {len(self._cache)} TP entries from cache")
            except Exception as e:
                self.logger.error(f"Failed to load TP cache: {e}")
                self._cache = {}

    async def _save_cache(self):
        try:
            os.makedirs(os.path.dirname(self._cache_path), exist_ok=True)
            with open(self._cache_path, 'w', encoding='utf-8') as f:
                json.dump(self._cache, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save TP cache: {e}")

    async def _restore_from_cache(self):
        if not self._cache:
            return
        self.logger.info(f"Restoring {len(self._cache)} TP tasks from cache")
        for token_address, data in list(self._cache.items()):
            self.start_task(
                token_address=token_address,
                base_token_address=data['base_token_address'],
                tp_ladder_id=data['tp_ladder_id'],
                price_bought_usd=data['price_bought_usd'],
                steps_done=data.get('steps_done', 0),
                original_total_raw=data.get('original_total_raw'),
                custom_tp_ladder=data.get('custom_tp_ladder'),
            )

    # ------------------------------------------------------------------
    # USD price via Dexscreener
    # ------------------------------------------------------------------

    async def _get_price_usd(self, token_address: str) -> float | None:
        try:
            return await self.dexscreener.quote_price(self.ds_chain_id, token_address)
        except Exception as e:
            self.logger.error(f"Dexscreener price error {token_address}: {e}")
        return None

    # ------------------------------------------------------------------
    # Task management
    # ------------------------------------------------------------------

    def start_task(
        self,
        token_address: str,
        base_token_address: str,
        tp_ladder_id: int,
        price_bought_usd: float,
        steps_done: int = 0,
        original_total_raw: int = None,
        custom_tp_ladder: dict = None,
    ) -> asyncio.Task:
        task = asyncio.create_task(
            self._run_tp_task(
                token_address=token_address,
                base_token_address=base_token_address,
                tp_ladder_id=tp_ladder_id,
                price_bought_usd=price_bought_usd,
                steps_done=steps_done,
                original_total_raw=original_total_raw,
                custom_tp_ladder=custom_tp_ladder,
            )
        )
        self._tasks.append(task)
        return task

    async def _run_tp_task(
        self,
        token_address: str,
        base_token_address: str,
        tp_ladder_id: int,
        price_bought_usd: float,
        steps_done: int = 0,
        original_total_raw: int = None,
        custom_tp_ladder: dict = None,
        max_failures: int = 5,
    ):
        try:
            await self._run_tp_task_inner(
                token_address=token_address,
                base_token_address=base_token_address,
                tp_ladder_id=tp_ladder_id,
                price_bought_usd=price_bought_usd,
                steps_done=steps_done,
                original_total_raw=original_total_raw,
                custom_tp_ladder=custom_tp_ladder,
                max_failures=max_failures,
            )
        except Exception as e:
            self.logger.error(f"TP task crashed for {token_address}: {str(e)}", exc_info=True)

    async def _run_tp_task_inner(
        self,
        token_address: str,
        base_token_address: str,
        tp_ladder_id: int,
        price_bought_usd: float,
        steps_done: int = 0,
        original_total_raw: int = None,
        custom_tp_ladder: dict = None,
        max_failures: int = 5,
    ):
        ladder = custom_tp_ladder if custom_tp_ladder is not None else TP_LADDERS.get(tp_ladder_id)
        if not ladder or not ladder.get('enabled'):
            self.logger.warning(f"TP ladder {tp_ladder_id} disabled/missing | {token_address}")
            await self.tg_client.send_error_alert(
                "TP DISABLED", f"Ladder {tp_ladder_id} not available", token_address
            )
            return

        # Fetch current token balance (needed to validate position still exists)
        total_raw, _ = await self.trade_handler.get_token_balance(token_address)
        if not total_raw and self.chain_name != 'SOLANA':
            self.logger.warning(f"TP | Zero balance for {token_address}")
            await self.tg_client.send_error_alert("TP SKIPPED", "Zero balance", token_address)
            return

        # On fresh buy use current balance; on restore use the original stored balance
        # so that sell_raw percentages always reference the same base amount.
        sizing_total = original_total_raw if original_total_raw is not None else total_raw

        # Persist to cache before starting monitoring loop
        self._cache[token_address] = {
            'base_token_address': base_token_address,
            'tp_ladder_id': tp_ladder_id,
            'price_bought_usd': price_bought_usd,
            'steps_done': steps_done,
            'original_total_raw': sizing_total,
            'custom_tp_ladder': custom_tp_ladder,
        }
        await self._save_cache()

        # Build TP levels from ladder config
        first_pct = float(ladder['first_tp_percent'])
        total_pct = float(ladder['total_percent'])
        steps = int(ladder['steps'])
        distribution = [float(d) for d in ladder['distribution']]
        step_size = (total_pct - first_pct) / (steps - 1) if steps > 1 else 0
        stop_loss_price = price_bought_usd * (1 + ladder['SL_from_entry_percent'])

        tp_levels = [
            {
                'step': i + 1,
                'target_price': price_bought_usd * (1 + first_pct + step_size * i),
                'sell_raw': int(sizing_total * distribution[i] / 100),
                'pct': distribution[i],
                'done': (i < steps_done),
            }
            for i in range(steps)
        ]

        self.logger.info(
            f"TP | {token_address[:8]}... | entry=${price_bought_usd:.8f} "
            f"| SL=${stop_loss_price:.8f} | {steps - steps_done}/{steps} remaining"
        )

        poll_interval = PRICE_UPDATE_DELAY[self.chain_name]
        failures = 0
        
        if self.chain_name!='SOLANA':
            spender = await self.trade_handler.okx_client.get_approve_address(
                self.trade_handler.chain_id, token_address
            )
            if spender:
                approved = await self.trade_handler._approve_token_for_swap(token_address, spender)
                if not approved:
                    self.logger.error(f"Approval failed for {token_address}")
                    return None

        while True:
            if failures >= max_failures:
                self.logger.error(f"TP | Max failures reached for {token_address}")
                await self.tg_client.send_error_alert(
                    "TP FAILED", "Max retries exceeded", token_address
                )
                break

            if all(lvl['done'] for lvl in tp_levels):
                self.logger.success(f"TP | All levels completed for {token_address}")
                self._cache.pop(token_address, None)
                await self._save_cache()
                break

            try:
                current_price = await self._get_price_usd(token_address)
                if current_price is None:
                    await asyncio.sleep(poll_interval)
                    continue

                # Stop-loss check
                if current_price <= stop_loss_price:
                    self.logger.warning(
                        f"TP | SL triggered for {token_address} @ ${current_price:.8f}"
                    )
                    sl_raw, _ = await self.trade_handler.get_token_balance(token_address)
                    result = await self.trade_handler.execute_sell(
                        token_address, base_token_address, sl_raw or total_raw
                    )
                    await self.tg_client.tp_task_message(
                        self.chain_name, token_address,
                        price_bought_usd, current_price, 0, tx_hash=result or ""
                    )
                    if result:
                        self._cache.pop(token_address, None)
                        await self._save_cache()
                        break
                    failures += 1
                    await asyncio.sleep(poll_interval)
                    continue

                # Take-profit levels
                for lvl in tp_levels:
                    if lvl['done']:
                        continue
                    if current_price >= lvl['target_price']:
                        self.logger.info(
                            f"TP | Step {lvl['step']} triggered | "
                            f"${current_price:.8f} >= ${lvl['target_price']:.8f}"
                        )
                        result = await self.trade_handler.execute_sell(
                            token_address, base_token_address, lvl['sell_raw']
                        )
                        if result:
                            lvl['done'] = True
                            self._cache[token_address]['steps_done'] = lvl['step']
                            await self._save_cache()
                            self.logger.success(
                                f"TP | Step {lvl['step']} sold {lvl['pct']}% | TX: {result}"
                            )
                            await self.tg_client.tp_task_message(
                                self.chain_name, token_address,
                                price_bought_usd, current_price, lvl['step'], tx_hash=result
                            )
                        else:
                            failures += 1
                            self.logger.error(
                                f"TP | Step {lvl['step']} sell failed (failures={failures})"
                            )
                        break  # re-evaluate after sleep

                await asyncio.sleep(poll_interval)

            except Exception as e:
                self.logger.error(f"TP loop error for {token_address}: {e}")
                failures += 1
                await asyncio.sleep(poll_interval)

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def close(self):
        for t in self._tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
