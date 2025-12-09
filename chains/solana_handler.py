# chains/solana_handler.py

import asyncio
import time
import json
import os
import warnings
from datetime import datetime, timedelta
from config import (
    GAS_UPDATE_INTERVAL,
    GAS_MULTPLIER, 
    GAS_LIMIT, 
    DELAY_BEFORE_TP,
    RPC,
    WS_RPC,
    USE_WEBSOCKET,
    ERROR_429_RETRIES,
    ERROR_429_DELAY,
    SLIPPAGE_PERCENT,
    #SHYFT_API_KEY,
    TP_LADDERS,
    SOLANA_PRIORITY_FEE,
    TOKEN_DATA_BASE_PATH,
    USABLE_TOKENS,
    MARKET_CAP_CONFIG,
    PRICE_UPDATE_DELAY,
    MIN_POOL_TVL
)
from typing import Literal, Callable
from raydium_lib import RaydiumClient
from solders.message import MessageV0
import base58
from tg_bot import TelegramClient
from utils import get_logger
from typing import Optional
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed, Processed
from solana.rpc.types import TxOpts
from .consts import DEX_ROUTER_DATA
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.message import to_bytes_versioned
from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price
import base64
import ujson
from solders.transaction import VersionedTransaction
from curl_cffi.requests import AsyncSession
import traceback

#TODO: 
#1. Перенести http запросы в отдельный класс
#2. Перенести парсер в отдельный класс 
#3. Перенсети тп логику в отдельный класс (общий для обоих евм и сол)

class SolanaHandler:

    def __init__(
        self,
        tg_client: TelegramClient,
        private_key_base58: str,
        blockhash_update_interval: int = GAS_UPDATE_INTERVAL,
        dex: Literal['RAYDIUM', 'JUPITER'] = 'RAYDIUM'
    ):

        self.logger = get_logger("SOLANA")

        self.chain_name = 'SOLANA'
        self.client = None
        self.tg_client = tg_client

        self.jup_api_base = "https://lite-api.jup.ag/"
        
        # Base tokens configuration
        self.usable_tokens = [
            token for token in USABLE_TOKENS if token in DEX_ROUTER_DATA[self.chain_name]
        ]
        self.token_decimals = DEX_ROUTER_DATA['SOLANA']['token_decimals']

        self._initialized = False
        
        #background tasks
        self._gas_token_price_updater_task = None
        self._blockhash_updater_task = None
        self._pool_updater_task = None
        self._take_profit_tasks = []
        
        #account data
        secret_key = base58.b58decode(private_key_base58)
        self.keypair = Keypair.from_bytes(secret_key)
        self.pubkey = self.keypair.pubkey()
        self.raydium_client = RaydiumClient(
            rpc_url=RPC[self.chain_name],
            private_key_base58=private_key_base58,
            compute_unit_limit=GAS_LIMIT[self.chain_name],
            compute_unit_price=SOLANA_PRIORITY_FEE,
        )
        self.client = AsyncClient(RPC[self.chain_name])
        
        #cache
        self._blockhash_cache = None
        self.blockhash_update_interval = blockhash_update_interval
        self._gas_token_price = 0
        self.tp_cache_path = TOKEN_DATA_BASE_PATH + '/TP_data/'+ f'{self.chain_name}_TP_cache.json' #путь к файлу с тп кэшем
        self._take_profit_cache = {}

    @classmethod
    async def create(
        cls,
        tg_client: TelegramClient,
        private_key_base58: str,
        blockhash_update_interval: int = GAS_UPDATE_INTERVAL,
        dex: Literal['RAYDIUM', 'JUPITER'] = 'RAYDIUM'
    ):
        instance = cls(tg_client, private_key_base58, blockhash_update_interval, dex)
        await instance._initialize()
        return instance
    
    async def _initialize(self):
        if self._initialized:
            return
        
        # Initialize blockhash cache
        await self._initialize_blockchain_cache_vars()
        
        # Start background tasks
        self._blockhash_updater_task = asyncio.create_task(self._blockhash_updater_loop())
        self._gas_token_price_updater_task = asyncio.create_task(self._gas_token_price_updater_loop())
        await self._start_take_profit_tasks()
        
        self._initialized = True

    async def close(self):
        """Close handler and stop all background tasks"""
        
        tasks_to_cancel = []
        if self._blockhash_updater_task and not self._blockhash_updater_task.done():
            tasks_to_cancel.append(self._blockhash_updater_task)
        
        if self._gas_token_price_updater_task and not self._gas_token_price_updater_task.done():
            tasks_to_cancel.append(self._gas_token_price_updater_task)

        if len(self._take_profit_tasks) > 0:
            tasks_to_cancel.extend([t for t in self._take_profit_tasks if not t.done()])
        
        #закрываем все таски
        if tasks_to_cancel:
            for task in tasks_to_cancel:
                task.cancel()
            
            #ожидаем завершения всех тасков
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            self.logger.info(f"All background tasks cancelled ({len(tasks_to_cancel)} tasks)")

    async def _initialize_blockchain_cache_vars(self):
        blockhash_resp = await self.client.get_latest_blockhash()
        self._blockhash_cache = blockhash_resp.value.blockhash
        self._gas_token_price = await self._gas_token_price_updater_loop(init=True)
        #self.priority_fee = await self._priority_fee_updater_loop(init=True)
        return 
        
    async def _blockhash_updater_loop(self):
        while True:
            try:
                blockhash_cache = await self.client.get_latest_blockhash()
                self._blockhash_cache = blockhash_cache.value.blockhash
                await asyncio.sleep(self.blockhash_update_interval)
            except Exception as e:
                if any(i in str(traceback.format_exc()) for i in ['ReadTimeout', 'Timeout', 'ConnectTimeout']):
                    self.logger.warning("Blockhash update - RPC ReadTimeout error")
                    await asyncio.sleep(self.blockhash_update_interval)
                self.logger.error(f"Blockhash update error: {traceback.format_exc()}")
                await asyncio.sleep(self.blockhash_update_interval)

    async def _gas_token_price_updater_loop(self, init:bool=False):
        """
        Цикл обновления цены нативки, нужен для рассчета мкапы если свапаем к нативке
        """
        pool_address = "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2" 
        while True:
            try:
                _, price = await self.raydium_client.get_swap_data_and_price(
                    pool_address,
                    DEX_ROUTER_DATA[self.chain_name]['gas_token'],
                    DEX_ROUTER_DATA[self.chain_name]['USDC'],
                    self._blockhash_cache
                )
                if price: 
                    self.gas_token_price = price
                if init:
                    return price
                await asyncio.sleep(10)
            except Exception as e:
                self.logger.error(f"Native token price update error: {str(e)}")
                await asyncio.sleep(10)
    
    async def get_jupiter_quote(
        self,
        input_mint: str,
        output_mint: str,
        amount: int,
        slippage_bps: int = 50
    ) -> dict:
        
        url = self.jup_api_base + "ultra/v1/order"
        
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "taker": str(self.pubkey),
            "excludeRouters": "all-except-metis",
            "excludeDexes": "all-except-raydium",
        }
        async with AsyncSession() as session:
            try: 
                resp = await session.get(url, params=params)
                data = ujson.loads(resp.text)
                if data.get('transaction'):
                    #print(ujson.dumps(data, indent=4))
                    return data
                else:
                    self.logger.error(f"Jupiter quote error: {data.get('error')}")
                    return None
            except Exception as e:
                self.logger.error(f"Jupiter quote error: {str(e)}")
                return None
    
    async def _sign_raw_tx(self, raw_tx: str, in_base64: bool = False) -> str:
        tx_bytes = base64.b64decode(raw_tx)
        transaction = VersionedTransaction.from_bytes(tx_bytes)
        
        message_bytes = to_bytes_versioned(transaction.message)
        signature = self.keypair.sign_message(message_bytes)
        
        signed_transaction = VersionedTransaction.populate(
            transaction.message,
            [signature],
        )
        
        signed_tx_bytes = bytes(signed_transaction)
        signed_tx_base64 = base64.b64encode(signed_tx_bytes).decode('utf-8')
        
        return signed_tx_base64 if in_base64 else signed_tx_bytes

    async def execute_jupiter_swap(
        self,
        token_in_mint: str,
        token_out_mint: str,
        amount_in: int,
        #slippage_bps: int = 100
    ) -> str:
        
        start_time = time.perf_counter()
        
        quote_tx = await self.get_jupiter_quote(
            input_mint=token_in_mint,
            output_mint=token_out_mint,
            amount=amount_in*10**self.token_decimals
        )
        get_quote_time = time.perf_counter() - start_time
        self.logger.info(f"Get quote time: {(get_quote_time)*1000:.2f}ms")

        tx = quote_tx.get('transaction') 
        signed_transaction = await self._sign_raw_tx(tx)
        tx_opts = TxOpts(
            skip_preflight=True,
            preflight_commitment=Processed,
            max_retries=0
        )
        response = await self.client.send_raw_transaction(
            bytes(signed_transaction),
            opts=tx_opts
        )
        
        end_time = time.perf_counter()
        self.logger.info(f"TX sent: {response.value} | Time: {(end_time - start_time)*1000:.2f}ms")
        return str(response.value)
    
    async def query_raydium_swap_data_with_jupiter(
        self,
        token_one_decimals: int,
        token_one: str, 
        token_two: str, 
        amount: int,
        
    ) -> dict: 
        url = f'https://public.jupiterapi.com/quote?onlyDirectRoutes=true&inputMint={token_one}&outputMint={token_two}&amount={amount*10**token_one_decimals}&dexes=Raydium'
        async with AsyncSession() as session:
            try:
                resp = await session.get(url)
                resp.raise_for_status()
                data = ujson.loads(resp.text)
                route_plan = data.get('routePlan')
                if route_plan and len(route_plan) > 0:
                    return route_plan[0].get('swapInfo').get('ammKey')
                else:
                    self.logger.error(f"Jupiter quote error: {data.get('error')}")
                    return None
            except Exception as e:
                self.logger.error(f"Jupiter quote error: {str(e)}")
                return None
    
    def _get_buy_size_and_tp_id(self, mcap: int, sell_token: str) -> int:
        
        for config in MARKET_CAP_CONFIG:
            if mcap >= config['min_cap'] and mcap <= config['max_cap']:
                if config['enabled']:
                    return config['size'][sell_token], config['tp_ladder_id']
                else: 
                    self.logger.warning(f"Market cap config {config['min_cap']} - {config['max_cap']} is disabled, skipping")
        return 0, None

    """
    async def _create_jupiter_trigger_order(
        self,
        sell_token_address: str,
        base_token_address: str,
        amount_in: int,
        amount_out: int,
    ): 
        url_build = self.jup_api_base + "trigger/v1/createOrder"
        url_execute = self.jup_api_base + "trigger/v1/execute"

        payload = {
            "maker": str(self.pubkey),
            "payer": str(self.pubkey),
            "inputMint": sell_token_address,
            "outputMint": base_token_address,
            "params": {
                "makingAmount": str(amount_in),
                "takingAmount": str(amount_out),
                "slippageBps": "40"
            },
            "computeUnitPrice": "auto"
        }
        headers = {"Content-Type": "application/json"}

        async with AsyncSession() as session:
            response = await session.post(url_build, json=payload, headers=headers)
            response.raise_for_status()
            data = ujson.loads(response.text)
            unsigned_tx = data.get('transaction')
            request_id = data.get('requestId')
            
            if not (unsigned_tx and request_id):
                self.logger.error(f"Failed to create Jupiter trigger order")
                raise Exception(f"Failed to parse Jupiter trigger order: {data}")

            signed_tx = await self._sign_raw_tx(unsigned_tx, in_base64=True)
            payload = {
                "signedTransaction": signed_tx,
                "requestId": request_id,
            }
            #print(payload)
            
            response = await session.post(url_execute, json=payload, headers=headers)
            #print(response.text)
            response.raise_for_status()
            data = ujson.loads(response.text)
            if not data.get('status', '').lower() == 'success':
                raise Exception(f"Failed to execute Jupiter trigger order: {data}")
            else: 
                self.logger.info(f"Jupiter trigger order placed successfully")
            
            return 1

    async def _create_jupiter_tp_ladder(
        self,
        base_token_name: str,
        sell_token_address: str,
        tp_ladder_id: int,
        price_bought: float,
        
    ):
        balances = await self.raydium_client.get_token_balances(Pubkey.from_string(sell_token_address))
        total_balance = balances.get('uiAmount', 0)
        sell_token_decimals = balances.get('decimals', 6)
        base_token_address = DEX_ROUTER_DATA[self.chain_name][base_token_name]
        base_decimals = DEX_ROUTER_DATA[self.chain_name]['token_decimals'][base_token_name]
        if not total_balance:
            self.logger.error(f"Failed to get token balance for {sell_token_address}")
            return None
        
        ladder_config = TP_LADDERS[tp_ladder_id]
        total_percent = ladder_config['total_percent']
        steps = ladder_config['steps']
        first_tp_percent = ladder_config['first_tp_percent']
        distribution = ladder_config['distribution']

        price_step_percent = (total_percent - first_tp_percent) / (steps - 1) if steps > 1 else 0
        
        for i in range(steps):
            target_percent = first_tp_percent + (price_step_percent * i)
            target_price = price_bought * (1 + target_percent)
            sell_amount = (total_balance * distribution[i]) / 100
            buy_amount = (sell_amount * target_price)
            for j in range(3): 
                try:
                    await self._create_jupiter_trigger_order(
                        sell_token_address,
                        base_token_address,
                        int(buy_amount*10**base_decimals),
                        int(sell_amount*10**sell_token_decimals)
                    )
                    self.logger.info(f"[{i+1}/{steps}] tp order on {sell_token_address} created successfully")
                    await asyncio.sleep(0.5)
                    break
                except Exception as e:
                    self.logger.error(f"[{j+1}/3] retrying to create tp order on {sell_token_address} due to error: {str(e)}")
                    await asyncio.sleep(5)
    """

    async def _load_take_profit_cache(self,):
        #Загружаем кэш из json
        if os.path.exists(self.tp_cache_path):
            try:
                with open(self.tp_cache_path, 'r', encoding='utf-8') as f:
                    self._take_profit_cache = json.load(f)
            except Exception as e:
                self.logger.error(f"Failed to load TP cache: {str(e)}")
                self._take_profit_cache = {}
        
        return self._take_profit_cache

    async def _update_take_profit_json(self):
        #сохрянаем в json
        try:
            with open(self.tp_cache_path, 'w', encoding='utf-8') as f:
                json.dump(self._take_profit_cache, f, indent=4)
        except Exception as e:
            self.logger.error(f"Failed to save TP cache in json: {str(e)}")

    async def _start_take_profit_tasks(self,):
        await self._load_take_profit_cache()
        if len(self._take_profit_cache) > 0:
            self.logger.info(f"Starting TP tasks for {len(self._take_profit_cache)} tokens")
        else: 
            self.logger.info(f"No TP tasks to start")
            return
        for token_address, tp_data in self._take_profit_cache.items():
            self._take_profit_tasks.append(
                asyncio.create_task(
                    self._create_take_profit_task(
                        token_address,
                        tp_data['base_token_address'],
                        tp_data['pool_address'],
                        tp_data['take_profit_ladder_id'],
                        tp_data['price_bought'],
                        tp_data['steps_done'],
                        custom_tp_ladder=tp_data['custom_tp_ladder']
                    )
                )
            )

    async def _create_take_profit_task(
        self, 
        token_address_to_sell: str, 
        base_token_address: str,
        pool_address: str,
        take_profit_ladder_id: int, 
        price_bought: float,
        steps_done: int = 0,
        tx_failure_counter: int = 10,
        custom_tp_ladder: dict = None
    ):
        """
        Мониторит цену токена и продает по лестнице тейк-профитов или в стоплосс
        
        Args:
            token_address_to_sell: Адрес токена для продажи
            base_token_address: Адрес токена, в который продаем (USDT/USDC/WETH)
            take_profit_ladder_id: ID конфигурации лестницы из TP_LADDERS
            price_bought: Цена покупки токена в token_sell_to
            stop_loss_price: Цена продажи токена в token_sell_to (default - price_bought)
            steps_done: Количество проданных шагов (default - 0)
        """
        
        # Получаем конфигурацию лестницы
        ladder_config = TP_LADDERS.get(take_profit_ladder_id) if custom_tp_ladder is None else custom_tp_ladder
        if not ladder_config or not ladder_config.get('enabled'):
            self.logger.warning(f"TP task | TP ladder {take_profit_ladder_id} is disabled or not found")
            await self.tg_client.send_error_alert(
                "TP task FAILED", 
                f"{self.chain_name} TP ladder {take_profit_ladder_id} is disabled or not found",
                "Need to check manually"
                )
            return

        #конвертируем в доллары цену покупки. Если нет цены нативки, то считаем в нативных токенах
        if base_token_address != DEX_ROUTER_DATA[self.chain_name]['gas_token'] or self.gas_token_price == 0:
            price_corrector = 1
        else: 
            price_corrector = self.gas_token_price
        raw_price_bought = price_bought
        price_bought = price_bought * price_corrector

        #ставим дефолтный стоплосс
        stop_loss_price = price_bought * (1 + ladder_config['SL_from_entry_percent'])
        
        # Получаем параметры лестницы
        first_tp_percent = ladder_config['first_tp_percent']
        total_percent = ladder_config['total_percent']
        steps = ladder_config['steps']
        distribution = ladder_config['distribution']
        
        # Получаем баланс токена
        try:
            
            balances = await self.raydium_client.get_token_balances(Pubkey.from_string(token_address_to_sell))
            total_balance = balances.get('uiAmount',0)
        except Exception as e:
            self.logger.error(f"TP task | Failed to get token balance: {str(e)}")
            await self.tg_client.send_error_alert(
                "TP task FAILED", 
                f"{self.chain_name} Failed to get token balance for {token_address_to_sell}",
                "Need to check manually"
                )
            return 0
        
        if total_balance == 0:
            self.logger.warning(f"TP task | Zero balance for {token_address_to_sell}")
            await self.tg_client.send_error_alert(
                "TP task DISABLED", 
                f"{self.chain_name} Zero balance for {token_address_to_sell}",
                "TP task stopped"
                )
            return 0
        
        #Сохраняем данные в кэш и в json
        self._take_profit_cache[token_address_to_sell] = {
            'base_token_address': base_token_address,
            'pool_address': pool_address,
            'take_profit_ladder_id': take_profit_ladder_id,
            'price_bought': raw_price_bought,
            'steps_done': steps_done,
            'custom_tp_ladder': custom_tp_ladder

        }
        await self._update_take_profit_json()
        
        self.logger.info(f"TP task | Starting TP task for {token_address_to_sell} | Ladder id: {take_profit_ladder_id} | Balance: {total_balance:.4f} | SL at {stop_loss_price:.4f}")
        
        # Рассчитываем ценовые уровни для каждого шага
        price_step_percent = (total_percent - first_tp_percent) / (steps - 1) if steps > 1 else 0
        tp_levels = []
        
        for i in range(steps):
            target_percent = first_tp_percent + (price_step_percent * i)
            target_price = price_bought * (1 + target_percent)
            sell_amount = (total_balance * distribution[i]) / 100
            tp_levels.append({
                'step': i + 1,
                'target_price': target_price,
                'target_percent': target_percent,
                'sell_amount': sell_amount,
                'size_percent': distribution[i],
                'executed': False if i >= steps_done else True
            })
        self.logger.info(f"TP task | TP levels calculated: {steps-steps_done}/{steps} steps left from {tp_levels[0]['target_price']:.6f} to {tp_levels[-1]['target_price']:.6f}")
        
        # Мониторинг цены и выполнение продаж
        poll_interval = PRICE_UPDATE_DELAY[self.chain_name]

        tx_failure = 0
        while True:

            #Если количество неуспешных транзакций превысило лимит, завершаем задачу
            if tx_failure >= tx_failure_counter:
                self.logger.error(f"TP task | Failed to execute TP levels for {token_address_to_sell}")
                await self.tg_client.send_error_alert(
                    "TP task FAILED", 
                    f"{self.chain_name} TP task failed for {token_address_to_sell}, can't sell token",
                    "Need to check manually"
                    )
                break
            #Проверяем, все ли уровни выполнены
            if all(level['executed'] for level in tp_levels):
                self.logger.success(f"TP task | All TP levels executed for {token_address_to_sell}")
                #удаляем данные о тп когда он выполнен
                if token_address_to_sell in self._take_profit_cache:
                    del self._take_profit_cache[token_address_to_sell]
                    await self._update_take_profit_json()
                break
    
            try:
                # Получаем текущую цену
                token_data = await self.raydium_client.get_swap_data_and_price(
                    pool_address,
                    token_address_to_sell,
                    base_token_address,
                    self._blockhash_cache
                )
                if token_data is None:
                    self.logger.warning(f"TP task | Failed to get current price for {token_address_to_sell}")
                    await asyncio.sleep(poll_interval)
                    continue
                else: 
                    _, current_price = token_data
                    current_price = current_price * price_corrector

                #Триггерим стоплосс, передаем единственный тейкпрофит - продажа всего
                if current_price <= stop_loss_price:
                    self.logger.warning(f"TP task | Stop loss triggered for {token_address_to_sell}")
                    balances = await self.raydium_client.get_token_balances(Pubkey.from_string(token_address_to_sell))
                    balance = balances.get('uiAmount',0)
                    if not balance:
                        raise Exception("Failed to get token balance")
                    tp_levels = [
                        {
                            'step': 0,
                            'target_price': 0,
                            'target_percent': 100,
                            'sell_amount': balance,
                            'size_percent': 100,
                            'executed': False
                        }
                    ]
                #Проверяем каждый уровень
                for level in tp_levels:
                    if level['executed']:
                        continue
                    price_reached = current_price >= level['target_price']
                    
                    #готовим продажу если цена тп достигнута
                    if price_reached:
                        self.logger.info(f"TP task | {token_address_to_sell} | TP level {level['step']} triggered: Current: {current_price:.6f}, Target: {level['target_price']:.6f}")
                        
                        # Строим и отправляем транзакцию                       
                        tx = await self.raydium_client.swap(
                            pool_address,
                            token_address_to_sell,
                            base_token_address,
                            level['sell_amount'],
                            SLIPPAGE_PERCENT,
                            cached_blockhash=self._blockhash_cache,
                            skip_confirmation = False
                        )

                        #проверям результат, убираем флаг на тп
                        if tx:
                            level['executed'] = True
                            #обновляем кэш
                            if token_address_to_sell in self._take_profit_cache:
                                self._take_profit_cache[token_address_to_sell]['steps_done'] = level['step']
                                await self._update_take_profit_json()
                            self.logger.success(f"TP task | {token_address_to_sell} | TP level {level['step']} executed: {level['size_percent']}% sold at {current_price:.6f} | TX: {tx}")
                            await self.tg_client.tp_task_message(
                                self.chain_name,
                                token_address_to_sell,
                                price_bought,
                                current_price,
                                level['step'],
                                tx_hash = tx
                            )
                        else:
                            tx_failure += 1
                            self.logger.error(f"TP task | {token_address_to_sell} | Failed to execute TP level {level['step']}")
                            await self.tg_client.send_error_alert(
                                "TP task FAILED", 
                                f"{self.chain_name} Failed to execute TP level {level['step']}",
                                f"retries: {tx_failure}/{tx_failure_counter}"
                                )
                             
                    await asyncio.sleep(poll_interval)       
                
            except Exception as e:
                self.logger.error(f"TP task | Error in TP monitoring loop: {traceback.format_exc()}")
                await self.tg_client.send_error_alert(
                    "TP task ERROR", 
                    f"{self.chain_name} Error in TP monitoring loop but still running",
                    str(e)
                    )
                tx_failure += 1
                await asyncio.sleep(poll_interval)
    
    async def execute_swap(
        self,
        token_supply:int,
        pool_data:dict,
        position_size: int = None,
        custom_tp_ladder: dict = None
    ) -> str:

        """
            supply_data = 1234
            pool_data = {
                'token_address': token_address,
                'chain': 'ARBITRUM',
                'base_token': base_token_name,
                'dex_type': 'amm',
                'liquidity': pool_tvl,
                'pair_address': pool_address,
                'fee_tier': 0
            }
        Выполняет свап токена
        Если base_token не указан, использует первый доступный пул из кэша
        """
       
        base_token_name = pool_data.get('base_token')
        base_token_address = DEX_ROUTER_DATA[self.chain_name][base_token_name]
        pool_address = pool_data.get('pair_address')
        token_address = pool_data.get('token_address')
        dex_type = pool_data.get('dex_type')
        
        t3 = time.perf_counter()
        token_data = await self.raydium_client.get_swap_data_and_price(
            pool_address, 
            base_token_address,
            token_address, 
            cached_blockhash=self._blockhash_cache
        )
        t4 = time.perf_counter()
        self.logger.debug(f"| execute_swap | Swap data query time: {(t4-t3)*1000:.2f}ms")

        t5 = time.perf_counter()
        if token_data: 
            pool_data, price = token_data
            if base_token_address == DEX_ROUTER_DATA[self.chain_name]['gas_token']:
                mcap_usd_converter = self.gas_token_price * price
            else:
                mcap_usd_converter = price
            mcap = mcap_usd_converter * token_supply
            amount_in, tp_id = self._get_buy_size_and_tp_id(mcap, base_token_name)

            if position_size:
                if base_token_address == DEX_ROUTER_DATA[self.chain_name]['gas_token']:
                    amount_in = position_size/self.gas_token_price
                else:
                    amount_in = position_size

            if not amount_in:
                return None
        else: 
            self.logger.error(f"No Raydium data found for token {token_address}")
            return None

        swap =  await self.raydium_client.swap(
            pool_address,
            base_token_address,
            token_address,
            amount_in,
            slippage = SLIPPAGE_PERCENT,
            pool_data = pool_data,

        )
        t6 = time.perf_counter()
        self.logger.debug(f"| execute_swap | send swap time: {(t6-t5)*1000:.2f}ms")

        if swap:
            await asyncio.sleep(DELAY_BEFORE_TP)
            asyncio.create_task(self._create_take_profit_task(
                token_address,
                base_token_address,
                pool_address,
                tp_id,
                1/price,
                custom_tp_ladder=custom_tp_ladder
            ))
        return swap

        
