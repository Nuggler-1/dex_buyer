import asyncio
import base64
import base58

from web3 import AsyncWeb3, AsyncHTTPProvider, WebSocketProvider
from eth_account import Account
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.message import to_bytes_versioned
from solders.transaction import VersionedTransaction
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Processed
from solana.rpc.types import TxOpts, TokenAccountOpts

from tg_bot import TelegramClient
from utils import get_logger
from config import (
    GAS_MULTPLIER,
    RPC,
    RPC_FOR_LATENCY_ACTIONS,
    TOKEN_TO_SELL,
    USE_WEBSOCKET,
    SLIPPAGE_PERCENT,
    DELAY_BEFORE_SL,
    GAS_LIMIT_MULTIPLIER
)
from .consts import DEX_ROUTER_DATA, erc20_abi
from .dexes import OkxDexClient, DexscreenerClient



_TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
_ASSOC_TOKEN_PROG = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe1bxe")


class TradeHandler:

    def __init__(
        self,
        tg_client: TelegramClient,
        chain_name: str,
        private_key_evm: str = None,
        private_key_sol: str = None,
        gas_multiplier: float = GAS_MULTPLIER,
        use_websocket: bool = USE_WEBSOCKET,
    ):
        self.logger = get_logger(chain_name)
        self.chain_name = chain_name
        self.tg_client = tg_client
        self.okx_client = None
        self.is_solana: bool = (chain_name == 'SOLANA')
        self.chain_id: int = DEX_ROUTER_DATA[chain_name]['chain_id']
        self.token_decimals: dict = DEX_ROUTER_DATA[chain_name]['token_decimals']
        self.gas_token_price: float = 0.0
        self.dexscreener =  DexscreenerClient()
        self.ds_chain_id = self.chain_name.lower()
        self._take_profit_handler = None
        self._initialized: bool = False

        if not self.is_solana:
            self.gas_multiplier = gas_multiplier
            self._cached_nonce: int = 0

            rpc_wss = RPC.get('wss', {}).get(chain_name, '')
            if use_websocket and rpc_wss and rpc_wss.startswith('wss'):
                self.w3 = AsyncWeb3(WebSocketProvider(
                    rpc_wss, websocket_kwargs={'ping_interval': 20, 'ping_timeout': 10}
                ))
                self.using_websocket = True
            else:
                self.w3 = AsyncWeb3(AsyncHTTPProvider(RPC['http'][chain_name]))
                self.using_websocket = False

            latency_url = RPC_FOR_LATENCY_ACTIONS[chain_name]
            if latency_url.startswith('wss'):
                self.w3_fast = AsyncWeb3(WebSocketProvider(
                    latency_url, websocket_kwargs={'ping_interval': 20, 'ping_timeout': 10}
                ))
                self._fast_is_ws = True
            else:
                self.w3_fast = AsyncWeb3(AsyncHTTPProvider(latency_url))
                self._fast_is_ws = False

            self.account = Account.from_key(private_key_evm)
            self.okx_client = OkxDexClient(self.account.address)
            self._ws_checker_task: asyncio.Task | None = None
            self._gas_token_price_task: asyncio.Task | None = None
        else:
            secret = base58.b58decode(private_key_sol)
            self.keypair = Keypair.from_bytes(secret)
            self.pubkey = self.keypair.pubkey()
            self.okx_client = OkxDexClient(str(self.pubkey))
            self.client = AsyncClient(RPC['http']['SOLANA'])
            self.client_fast = AsyncClient(RPC_FOR_LATENCY_ACTIONS['SOLANA'])
            self._blockhash_cache = None
            self._blockhash_task: asyncio.Task | None = None
            self._gas_token_price_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Factory / async init
    # ------------------------------------------------------------------

    @classmethod
    async def create(
        cls,
        tg_client: TelegramClient,
        chain_name: str,
        private_key_evm: str = None,
        private_key_sol: str = None,
    ) -> "TradeHandler":
        instance = cls(
            tg_client, chain_name,
            private_key_evm, private_key_sol,
        )
        await instance._initialize()
        return instance

    async def _initialize(self):
        if self._initialized:
            return

        if not self.is_solana:
            if self.using_websocket:
                await self.w3.provider.connect()
            if self._fast_is_ws:
                await self.w3_fast.provider.connect()

            self._cached_nonce = await self.w3.eth.get_transaction_count(
                self.account.address, 'pending'
            )
            self._ws_checker_task = asyncio.create_task(self._ws_checker_loop())
            await self._approve_all_base_tokens()
        else:
            resp = await self.client.get_latest_blockhash()
            self._blockhash_cache = resp.value.blockhash
            self._blockhash_task = asyncio.create_task(self._blockhash_loop())

        if TOKEN_TO_SELL[self.chain_name] == DEX_ROUTER_DATA[self.chain_name]['gas_token_ticker']:
            self._gas_token_price_task = asyncio.create_task(self._gas_token_price_loop())

        self._initialized = True
        self.logger.info(f"TradeHandler initialized | chain {self.chain_name}")

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------

    async def _ws_checker_loop(self):
        while True:
            await asyncio.sleep(5)
            try:
                if self.using_websocket and not await self.w3.provider.is_connected():
                    await self.w3.provider.connect()
                    self.logger.warning("WS reconnected (main)")
                if self._fast_is_ws and not await self.w3_fast.provider.is_connected():
                    await self.w3_fast.provider.connect()
                    self.logger.warning("WS reconnected (fast)")
            except Exception as e:
                self.logger.error(f"WS checker: {e}")

    async def _gas_token_price_loop(self):
        token_ticker = 'W'+DEX_ROUTER_DATA[self.chain_name]['gas_token_ticker']
        address = DEX_ROUTER_DATA[self.chain_name][token_ticker]
        while True:
            try:
                price = await self.dexscreener.quote_price(self.ds_chain_id, address)
                if price is not None:
                    self.gas_token_price = price
            except Exception as e:
                self.logger.error(f"Gas token price loop: {str(e)}")
            await asyncio.sleep(60)

    async def _blockhash_loop(self):
        while True:
            try:
                resp = await self.client.get_latest_blockhash()
                self._blockhash_cache = resp.value.blockhash
            except Exception:
                import traceback
                self.logger.error(f"Blockhash update: {traceback.format_exc()}")
            await asyncio.sleep(5)

    # ------------------------------------------------------------------
    # EVM token approval
    # ------------------------------------------------------------------

    async def _approve_token_for_swap(self, token_address: str, spender: str) -> bool:
        try:
            checksum_token = AsyncWeb3.to_checksum_address(token_address)
            checksum_spender = AsyncWeb3.to_checksum_address(spender)
            contract = self.w3.eth.contract(address=checksum_token, abi=erc20_abi)
            allowance = await contract.functions.allowance(
                self.account.address, checksum_spender
            ).call()
            if allowance >= 2 ** 128:
                return True
            self.logger.info(f"Approving {checksum_token[:10]}... -> {checksum_spender[:10]}...")
            approve_tx = await contract.functions.approve(
                checksum_spender, 2 ** 256 - 1
            ).build_transaction({
                'from': self.account.address,
                'nonce': self._cached_nonce,
                'gas': 200_000,
                'gasPrice': int(await self.w3.eth.gas_price * self.gas_multiplier),
                'chainId': self.chain_id,
            })
            result = await self._sign_and_send_evm(approve_tx, wait=True)
            return result is not None
        except Exception as e:
            self.logger.error(f"Approve error {token_address}: {str(e)}")
            return False

    async def _approve_all_base_tokens(self):
        gas_token = DEX_ROUTER_DATA[self.chain_name].get('gas_token')
        ticker = TOKEN_TO_SELL[self.chain_name]
        address = DEX_ROUTER_DATA[self.chain_name].get(ticker)
        if not address or address == gas_token:
            return
        try:
            spender = await self.okx_client.get_approve_address(self.chain_id, address)
            if spender:
                await self._approve_token_for_swap(address, spender)
        except Exception as e:
            self.logger.error(f"Pre-approve {ticker}: {str(e)}")

    # ------------------------------------------------------------------
    # EVM sign & send
    # ------------------------------------------------------------------

    async def _sign_and_send_evm(
        self, tx: dict, wait: bool = False, fast: bool = False
    ) -> str | None:
        signed = self.account.sign_transaction(tx)
        w3 = self.w3_fast if fast else self.w3
        try:
            tx_hash = await w3.eth.send_raw_transaction(signed.raw_transaction)
            self._cached_nonce += 1
            self.logger.info(f"EVM TX: {tx_hash.hex()}")
            if wait:
                receipt = await w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
                if receipt.status != 1:
                    self.logger.error(f"EVM TX reverted: {tx_hash.hex()}")
                    return None
            return tx_hash.hex()
        except Exception as e:
            self.logger.error(f"send_raw_transaction: {str(e)}")
            return None

    # ------------------------------------------------------------------
    # Solana sign & send
    # ------------------------------------------------------------------

    async def _sign_and_send_solana(self, tx_b64: str) -> str | None:
        try:
            tx_bytes = base58.b58decode(tx_b64)
            transaction = VersionedTransaction.from_bytes(tx_bytes)
            sig = self.keypair.sign_message(to_bytes_versioned(transaction.message))
            signed_tx = VersionedTransaction.populate(transaction.message, [sig])
            opts = TxOpts(skip_preflight=True, preflight_commitment=Processed, max_retries=0)
            resp = await self.client_fast.send_raw_transaction(bytes(signed_tx), opts=opts)
            self.logger.info(f"Solana TX: {resp.value}")
            return str(resp.value)
        except Exception as e:
            self.logger.error(f"Solana send error: {str(e)}")
            return None

    # ------------------------------------------------------------------
    # Token balance (used by TakeProfitHandler)
    # ------------------------------------------------------------------

    async def get_token_balance(self, token_address: str) -> tuple[int, int]:
        """Returns (raw_balance: int, decimals: int)."""
        if self.is_solana:
            try:
                mint = Pubkey.from_string(token_address)
                resp = await self.client.get_token_accounts_by_owner_json_parsed(
                    self.pubkey,
                    TokenAccountOpts(mint=mint),
                )
                if resp.value:
                    info = resp.value[0].account.data.parsed['info']['tokenAmount']
                    return int(info['amount']), int(info['decimals'])
                return 0, 9
            except Exception as e:
                self.logger.debug(f"SOL balance {token_address}: {str(e)}")
                return 0, 9
        else:
            try:
                contract = self.w3.eth.contract(
                    address=AsyncWeb3.to_checksum_address(token_address), abi=erc20_abi
                )
                balance, decimals = await asyncio.gather(
                    contract.functions.balanceOf(self.account.address).call(),
                    contract.functions.decimals().call(),
                )
                return balance, decimals
            except Exception as e:
                self.logger.error(f"EVM balance {token_address}: {str(e)}")
                return None, None

    # ------------------------------------------------------------------
    # Market-cap / position sizing
    # ------------------------------------------------------------------

    def _get_buy_size_and_tp_id(self,mcap_config:list, mcap: float, base_token_ticker: str) -> tuple[float, int | None]:
        for cfg in mcap_config:
            if cfg['min_cap'] <= mcap <= cfg['max_cap']:
                if cfg['enabled']:
                    return cfg['size'].get(base_token_ticker, 0), cfg['tp_ladder_id']
                self.logger.warning(f"Mcap range disabled: {cfg['min_cap']}-{cfg['max_cap']}")
        return 0.0, None

    # ------------------------------------------------------------------
    # EVM receipt / price helpers
    # ------------------------------------------------------------------

    def _parse_token_transfers(self, receipt) -> list[dict]:
        transfers = []
        for log in receipt['logs']:
            if len(log['topics']) >= 3:
                topic0 = log['topics'][0].hex()
                if topic0 == 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef':
                    transfers.append({
                        'token': log['address'],
                        'from': '0x' + log['topics'][1].hex()[-40:],
                        'to':   '0x' + log['topics'][2].hex()[-40:],
                        'value': int(log['data'].hex(), 16),
                    })
        return transfers

    def _parse_actual_buy_price(
        self,
        receipt,
        token_address: str,
        base_token_name: str,
        amount_in_raw: int,
        base_decimals: int,
        token_decimals: int,
    ) -> float:
        """Returns actual USD price paid per token derived from tx receipt transfers."""
        transfers = self._parse_token_transfers(receipt)
        for t in transfers:
            if (
                t['to'].lower() == self.account.address.lower()
                and t['token'].lower() == token_address.lower()
                and t['value'] > 0
            ):
                price_in_base = (
                    (amount_in_raw / 10 ** base_decimals)
                    / (t['value'] / 10 ** token_decimals)
                )
                gas_token = DEX_ROUTER_DATA[self.chain_name]['gas_token']
                base_addr = DEX_ROUTER_DATA[self.chain_name][base_token_name]
                if base_addr.lower() == gas_token.lower() and self.gas_token_price:
                    return price_in_base * self.gas_token_price
                return price_in_base  # stable base → already USD
        return 0.0

    # ------------------------------------------------------------------
    # Core broadcast (shared by buy and sell paths)
    # ------------------------------------------------------------------

    async def _broadcast_okx_swap(
        self, swap_data: dict, token_in_address: str, fast: bool = False
    ) -> str | None:
        try:
            data_list = swap_data.get('data', [])
            if not data_list:
                self.logger.error(f"Empty OKX swap response: {swap_data}")
                return None

            tx_info = data_list[0].get('tx', {})

            if not self.is_solana:
                receiver = tx_info.get('to')
                tx_data = tx_info.get('data')
                value = int(tx_info.get('value', 0))

                if not receiver or not tx_data:
                    self.logger.error(f"Missing EVM tx fields: to={receiver}")
                    return None
                    
                tx = {
                    'to': AsyncWeb3.to_checksum_address(receiver),
                    'data': tx_data,
                    'value': value,
                    'gas': int(float(tx_info.get('gas', 1_000_000)) * GAS_LIMIT_MULTIPLIER),
                    'gasPrice': int(float(tx_info.get('gasPrice', await self.w3.eth.gas_price)) * self.gas_multiplier),
                    'nonce': self._cached_nonce,
                    'chainId': self.chain_id,
                }
                return await self._sign_and_send_evm(tx, wait=True, fast=fast)
            else:
                tx_b64 = tx_info.get('data') if isinstance(tx_info, dict) else tx_info
                if not tx_b64 or not isinstance(tx_b64, str):
                    self.logger.error(f"No tx data in Solana OKX response: {tx_info!r}")
                    return None
                return await self._sign_and_send_solana(tx_b64)

        except Exception as e:
            self.logger.error(f"_broadcast_okx_swap: {e}")
            return None

    # ------------------------------------------------------------------
    # Public: execute buy swap
    # ------------------------------------------------------------------

    async def execute_swap(
        self,
        token_address: str,
        base_token_name: str,
        mcap: float,
        mcap_config:list,
        delay_before_sl:int = DELAY_BEFORE_SL,
        position_size: float = None,
        custom_tp_ladder: dict = None,
    ) -> str | None:
        chain_data = DEX_ROUTER_DATA[self.chain_name]
        base_token_address = chain_data[base_token_name]
        base_decimals = self.token_decimals.get(base_token_name, 18)

        self.logger.info(f"execute_swap | {token_address} | base={base_token_name}")

        # --- Resolve position size ---
        if position_size is not None:
            gas_token = chain_data.get('gas_token')
            if base_token_address == gas_token and self.gas_token_price:
                amount_in = position_size / self.gas_token_price
            else:
                amount_in = position_size
            if not custom_tp_ladder: 
                raise ValueError("custom_tp_ladder is required when position_size is provided")
        else:
            amount_in, tp_ladder_id = self._get_buy_size_and_tp_id(mcap_config, mcap, base_token_name)

        if not amount_in:
            self.logger.warning(f"No position size resolved (mcap=${mcap:.0f})")
            return None

        amount_in_raw = int(amount_in * 10 ** base_decimals)
        self.logger.info(f"execute_swap | mcap=${mcap:.0f} | amount_in={amount_in} {base_token_name}")

        # --- Single quote + tx data ---
        swap_quote = await self.okx_client.quote_swap(
            self.chain_id,
            base_token_address,
            token_address,
            amount_in_raw,
            SLIPPAGE_PERCENT[self.chain_name],
        )
        if not swap_quote:
            self.logger.error("OKX swap quote failed")
            return None

        # Resolve token decimals from quote response
        to_token_info = swap_quote.get('toToken', {})
        token_dec = int(to_token_info.get('decimals', 9 if self.is_solana else 18))

        # Quote-derived price (used as fallback / Solana entry price)
        quote_price_usd: float = 0.0
        if to_token_info.get('tokenUnitPrice'):
            quote_price_usd = float(to_token_info['tokenUnitPrice'])
        else:
            from_amt = int(swap_quote.get('fromTokenAmount', 1))
            to_amt = int(swap_quote.get('toTokenAmount', 0))
            if to_amt > 0:
                price_in_base = (from_amt / 10 ** base_decimals) / (to_amt / 10 ** token_dec)
                quote_price_usd = price_in_base * self.gas_token_price if self.gas_token_price else price_in_base

        swap_data = await self.okx_client.get_swap_data(swap_quote)
        if not swap_data:
            self.logger.error("OKX get_swap_data failed")
            return None

        tx_hash = await self._broadcast_okx_swap(swap_data, base_token_address, fast=True)
        if not tx_hash:
            return None

        self.logger.success(f"Buy TX: {tx_hash}")
        await self.tg_client.send_trade_alert(self.chain_name, token_address, tx_hash=tx_hash)

        # --- Actual entry price ---
        token_price_usd = quote_price_usd
        if not self.is_solana:
            try:
                receipt = await self.w3.eth.get_transaction_receipt(tx_hash)
                if receipt and receipt.status == 1:
                    actual = self._parse_actual_buy_price(
                        receipt, token_address, base_token_name,
                        amount_in_raw, base_decimals, token_dec,
                    )
                    if actual > 0:
                        token_price_usd = actual
                        self.logger.info(f"Actual buy price: ${actual:.8f}")
                    else:
                        self.logger.warning("Transfer parse failed, using quote price")
            except Exception as e:
                self.logger.error(f"Receipt parse error: {e}")

        # --- Start TP task ---
        if self._take_profit_handler and tp_ladder_id is not None:
            self._take_profit_handler.start_task(
                token_address=token_address,
                base_token_address=base_token_address,
                tp_ladder_id=tp_ladder_id,
                price_bought_usd=token_price_usd,
                custom_tp_ladder=custom_tp_ladder,
                original_total_raw=int(swap_quote.get('toTokenAmount', 0)) if self.chain_name == 'SOLANA' else None,
                delay_before_sl=delay_before_sl,
            )

        return tx_hash

    # ------------------------------------------------------------------
    # Public: execute sell swap (called by TakeProfitHandler)
    # ------------------------------------------------------------------

    async def execute_sell(
        self,
        token_address: str,
        base_token_address: str,
        amount_in_raw: int,
    ) -> str | None:
        quote = await self.okx_client.quote_swap(
            self.chain_id,
            token_address,
            base_token_address,
            amount_in_raw,
            SLIPPAGE_PERCENT[self.chain_name],
        )
        if not quote:
            self.logger.error(f"execute_sell | quote failed for {token_address}")
            return None

        swap_data = await self.okx_client.get_swap_data(quote)
        if not swap_data:
            self.logger.error("execute_sell | get_swap_data failed")
            return None

        return await self._broadcast_okx_swap(swap_data, token_address, fast=True)

    # ------------------------------------------------------------------
    # TP handler wiring
    # ------------------------------------------------------------------

    def set_take_profit_handler(self, tp_handler) -> None:
        self._take_profit_handler = tp_handler

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def close(self):
        task_attrs = (
            '_ws_checker_task',
            '_gas_token_price_task', '_blockhash_task',
        )
        tasks = [getattr(self, a, None) for a in task_attrs]
        for t in tasks:
            if t and not t.done():
                t.cancel()
        await asyncio.gather(*[t for t in tasks if t], return_exceptions=True)

        if not self.is_solana:
            if self.using_websocket:
                try:
                    await self.w3.provider.disconnect()
                except Exception:
                    pass
        else:
            await self.client.close()
            await self.client_fast.close()
