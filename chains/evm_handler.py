
import asyncio
import time
import json
import os
from datetime import datetime, timedelta
from web3 import AsyncWeb3, AsyncHTTPProvider, WebSocketProvider
from web3.middleware import ExtraDataToPOAMiddleware
from eth_account import Account
from eth_abi import encode
from curl_cffi.requests import AsyncSession
from tg_bot import TelegramClient
from utils import get_logger
from typing import Callable
from fake_useragent import UserAgent
from config import (
    ALL_BASE_TOKEN_TICKERS,
    DELAY_BETWEEN_BATCHES,
    GAS_UPDATE_INTERVAL,
    CACHE_UPDATE_BATCH_SIZE,
    GAS_MULTPLIER, 
    GAS_LIMIT, 
    CHAIN_NAMES,
    MARKET_CAP_CONFIG,
    RPC,
    USE_WEBSOCKET,
    SLIPPAGE_PERCENT,
    GAS_LIMIT,
    TOKEN_DATA_BASE_PATH,
    USABLE_TOKENS,
    TP_LADDERS,
    PRICE_UPDATE_DELAY,
    DELAY_BEFORE_TP,
    MIN_POOL_TVL,
    RPC_FOR_LATENCY_ACTIONS
    
)
from .consts import DEX_ROUTER_DATA, ZERO_ADDRESS, erc20_abi, factory_abi, permit2_abi
from typing import Literal

from .dexes import (
    UniswapV2,
    UniswapV3,
    UniswapV4,
    CakeswapV2,
    CakeswapV3,
    CakeswapV4, 
    DEX_MAP
)



#TODO: 
#1. Перенести web3 клиент в отдельный класс с обработкой 429 
#2. Перенести парсер в отдельный класс 

class EVMHandler:
    
    def __init__(
        self, 
        tg_client: TelegramClient,
        private_key: str, 
        chain_name: Literal[*CHAIN_NAMES], 
        gas_update_interval: int = GAS_UPDATE_INTERVAL,
        gas_multiplier: int = GAS_MULTPLIER,
        use_websocket: bool = USE_WEBSOCKET
    ):
        self.logger = get_logger(chain_name)
        if use_websocket:
            self.w3 = AsyncWeb3(WebSocketProvider(RPC['wss'][chain_name]))
            self.using_websocket = True
            self.logger.info("Using WebSocket RPC")
        else:
            self.w3 = AsyncWeb3(AsyncHTTPProvider(RPC['http'][chain_name]))
            self.using_websocket = False
            self.logger.debug("Using HTTP RPC")

        self.w3_latency = AsyncWeb3(WebSocketProvider(RPC_FOR_LATENCY_ACTIONS[chain_name]))

        # if chain_name in ['BSC', 'POLYGON']:
        #     self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        
        #misc
        self.tg_client = tg_client
        self.ws_connection_check_interval = 5
        self.account = Account.from_key(private_key)
        self.gas_update_interval = gas_update_interval
        self.gas_multiplier = gas_multiplier
        
        #задаем chain-specific data

        #chain-data
        self.chain_name = chain_name
        self.chain_id = DEX_ROUTER_DATA[chain_name]['chain_id']

        #dex instances
        self.dex_instances = {}

        #token-data
        self.usable_tokens = [
            token for token in USABLE_TOKENS if token in DEX_ROUTER_DATA[chain_name]#базовые токены для свапа (USDT;USDC;WETH)
        ] 
        self.gas_token_price = 0
        self.token_decimals = DEX_ROUTER_DATA[chain_name]['token_decimals'] #десятичность базовых токенов

        #кэш
        self._cached_nonce = None #манагерим нонс руками, чтобы не тратить время на запросы к рпс
        self._gas_price_cache = None #cached gas price
        self._initialized = False
        self._take_profit_cache = {}  # {token_address: tp_data}
        self.tp_cache_path = TOKEN_DATA_BASE_PATH + '/TP_data/'+ f'{chain_name}_TP_cache.json' #путь к файлу с тп кэшем

        #background tasks
        self._gas_updater_task = None
        self._gas_token_price_updater_task = None
        self._take_profit_tasks = []
        self._ws_connection_checker_task = None

    @classmethod
    async def create(
        cls,
        tg_client: TelegramClient,
        private_key: str,
        chain_name: Literal[*CHAIN_NAMES],
        gas_update_interval: int = GAS_UPDATE_INTERVAL,
        gas_multiplier: int = GAS_MULTPLIER,
        use_websocket: bool = USE_WEBSOCKET
    ):
        instance = cls(tg_client, private_key, chain_name, gas_update_interval, gas_multiplier, use_websocket)
        await instance._initialize()
        return instance
    
    async def _initialize(self):
        if self._initialized:
            return
        
        await self.w3.provider.connect()
        await self.w3_latency.provider.connect()

        self.dex_instances = {}
        for dex_name, dex_class in DEX_MAP.items():
            try:
                if dex_name not in DEX_ROUTER_DATA[self.chain_name]['dex_contracts']:
                    continue
                self.dex_instances[dex_name] = await dex_class.create(self.w3, self.w3_latency, self.account, self.chain_name)
            except Exception as e:
                self.logger.error(f"Failed to initialize {dex_name}: {e}")
                continue

        #готовим кэшированные данные и отправляем approve
        await self._initialize_blockchain_cache_vars()
        await self._approve_all_dexes()
        
        self.logger.info(f"Initialized {len(self.dex_instances)} DEX instances for {self.chain_name}")
        
        #фоновый луп обновления газа
        self._gas_updater_task = asyncio.create_task(self._gas_price_updater_loop())
        #фоновый луп обновления цены газтокена
        self._gas_token_price_updater_task = asyncio.create_task(self._gas_token_price_updater_loop())
        #фоновый луп проверки подключения к ws
        self._ws_connection_checker_task = asyncio.create_task(self._ws_connection_checker())
        #запускаем тейкпрофиты
        await self._start_take_profit_tasks()
        
        self._initialized = True

        
    async def _initialize_blockchain_cache_vars(self,):
        self._cached_nonce = await self.w3.eth.get_transaction_count(self.account.address, 'pending')
        self._gas_price_cache = int(await self.w3.eth.gas_price * self.gas_multiplier)
        self.gas_token_price = await self._gas_token_price_updater_loop(init=True)
        return 
        
    async def _gas_price_updater_loop(self):
        self.logger.info(f"Gas price updater loop started")
        while True:
            try:
                self._gas_price_cache = int(await self.w3.eth.gas_price * self.gas_multiplier)
                #self._cached_nonce = await self.w3.eth.get_transaction_count(self.account.address, 'pending')
                await asyncio.sleep(self.gas_update_interval)
            except Exception as e:
                self.logger.error(f"Gas price update error: {str(e)}")
                await asyncio.sleep(self.gas_update_interval)

    async def _ws_connection_checker(self):
        while True:
            try:
                if not await self.w3.provider.is_connected():
                    await self.w3.provider.connect()
                    self.logger.info(f"WebSocket connection reestablished")
                if not await self.w3_latency.provider.is_connected():
                    await self.w3_latency.provider.connect()
                    self.logger.info(f"WebSocket latency-sensetive connection reestablished")
                await asyncio.sleep(self.ws_connection_check_interval)
            except Exception as e:
                self.logger.error(f"WebSocket connection error: {str(e)}")
                await self.tg_client.send_error_alert(
                    "RPC WEBSCOKET DISCONNECTED",
                    f"{self.chain_name} WebSocket connection error: {str(e)}",
                )
                await asyncio.sleep(self.ws_connection_check_interval)

    async def _gas_token_price_updater_loop(self, init:bool=False):
        self.logger.info(f"Gas token price updater loop started")
        dex = self.dex_instances.get('cake_v3') if self.chain_name == 'BSC' else self.dex_instances.get('uni_v3')
        if not dex:
            self.logger.error(f"No V3 DEX available for gas token price updates")
            return None
            
        while True:
            try:
                price = await dex.check_token_price(
                    DEX_ROUTER_DATA[self.chain_name]['gas_token'],
                    18,
                    DEX_ROUTER_DATA[self.chain_name]['USDT'],
                    self.token_decimals['USDT'],
                    500,
                    amount_in=1*10**18
                )
                if price:
                    self.gas_token_price = price 
                if init:
                    return price
                await asyncio.sleep(30)
            except Exception as e:
                self.logger.error(f"Native token price update error: {str(e)}")
                await asyncio.sleep(30)

    async def close(self):
        """Close handler and stop all background tasks"""
        self.logger.info(f"Closing handler...")
        
        tasks_to_cancel = []
        
        if self._gas_updater_task and not self._gas_updater_task.done():
            tasks_to_cancel.append(self._gas_updater_task)
        
        if self._gas_token_price_updater_task and not self._gas_token_price_updater_task.done():
            tasks_to_cancel.append(self._gas_token_price_updater_task)
        
        if self._ws_connection_checker_task and not self._ws_connection_checker_task.done():
            tasks_to_cancel.append(self._ws_connection_checker_task)

        if len(self._take_profit_tasks) > 0:
            tasks_to_cancel.extend([t for t in self._take_profit_tasks if not t.done()])
        
        #закрываем все таски
        if tasks_to_cancel:
            for task in tasks_to_cancel:
                task.cancel()
            
            #ожидаем завершения всех тасков
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            self.logger.info(f"All background tasks cancelled ({len(tasks_to_cancel)} tasks)")
        
        #закрываем ws
        if self.using_websocket:
            try:
                await self.w3.provider.disconnect()
                self.logger.info(f"WebSocket connection closed")
            except Exception as e:
                self.logger.warning(f"Error closing WebSocket: {e}")
    

    async def _approve_token_for_swap(self,approve_receiver_address: str, token_address:str):
        try: 
            contract = self.w3.eth.contract(address=token_address, abi=erc20_abi)
            allowance = await contract.functions.allowance(
                self.account.address, approve_receiver_address
            ).call()

            if allowance < 2**128:
                self.logger.info(f"Not enough allowance for {token_address}, sending approve tx")
                approve_tx = await contract.functions.approve(
                    approve_receiver_address, 2**256 - 1
                ).build_transaction(
                    {
                        'from': self.account.address,
                        'nonce': self._cached_nonce,
                        'gas': GAS_LIMIT[self.chain_name],
                        'gasPrice': self._gas_price_cache,
                        'chainId': self.chain_id
                    }
                )
                tx = await self._sign_and_send(approve_tx, True)
                if not tx:
                    self.logger.error(f"Approve tx for {token_address} failed")
                    return None
                else: 
                    return tx
            else:
                self.logger.info(f"Approve not required for {token_address}")
                return True
        except Exception as e:
            self.logger.error(f"Error checking/sending approve for {token_address}: {str(e)}")
            return None

    async def _permit2_approve(self,permit_address:str, token_address: str, spender_address: str):
        """
        Permit2 approval flow:
        1. Approve token to Permit2 (standard ERC20 approve)
        2. Call permit2.approve(token, spender, amount, expiration)
        """
        try:
            await self._approve_token_for_swap(permit_address, token_address)

            permit2_contract = self.w3.eth.contract(address=permit_address, abi=permit2_abi)
            permit2_allowance = await permit2_contract.functions.allowance(
                self.account.address, token_address, spender_address
            ).call()
            
            # permit2_allowance returns (amount, expiration, nonce)
            current_amount = permit2_allowance[0]
            current_expiration = permit2_allowance[1]
            
            #self.logger.debug(f"Current Permit2 allowance for {token_address}: amount={current_amount}, expiration={current_expiration}")
            
            if current_amount < 2**128 or current_expiration < int(time.time()):
                self.logger.info(f"Setting Permit2 allowance for {token_address} to router")
                permit2_approve_tx = await permit2_contract.functions.approve(
                    token_address,
                    spender_address,
                    2**160 - 1,  # max uint160
                    2**48 - 1   # max expiration (never expires)
                ).build_transaction({
                    'from': self.account.address,
                    'nonce': self._cached_nonce,
                    'gas': GAS_LIMIT[self.chain_name],
                    'gasPrice': self._gas_price_cache,
                    'chainId': self.chain_id
                })
                tx = await self._sign_and_send(permit2_approve_tx, True)
                if not tx:
                    self.logger.error(f"Permit2 approve failed for {token_address}")
                    return None
                return tx
            else:
                self.logger.info(f"Permit2 approval not required for {token_address}")
                return True
        except Exception as e:
            self.logger.error(f"Error with Permit2 approval for {token_address}: {str(e)}")
            return None

    async def _approve_for_dex(self, dex_type: str, token_address: str):
        """
        Polymorphic approval method that handles all DEX types.
        - V4 DEXes use Permit2 approval flow
        - V2/V3 DEXes use standard ERC20 approval
        
        Args:
            dex_type: DEX type (uni_v2, uni_v3, uni_v4, cake_v2, cake_v3, cake_v4)
            token_address: Token address to approve
            
        Returns:
            Transaction hash or True if approval not needed, None on failure
        """
        dex_data = DEX_ROUTER_DATA[self.chain_name]['dex_contracts'].get(dex_type)
        if not dex_data:
            self.logger.error(f"DEX data not found for {dex_type}")
            return None
        
        try:
            if dex_type in ['uni_v4', 'cake_v4']:
                # V4 uses Permit2
                permit_address = dex_data['permit_address']
                router_address = dex_data['router_address']
                return await self._permit2_approve(permit_address, token_address, router_address)
            else:
                # V2/V3 use regular ERC20 approval
                router_address = dex_data['router_address']
                return await self._approve_token_for_swap(router_address, token_address)
        except Exception as e:
            self.logger.error(f"Error approving {token_address} for {dex_type}: {str(e)}")
            return None

    async def _approve_all_dexes(self):
        """
        Approve all usable tokens for trading across all DEXes
        """
        dex_contracts = DEX_ROUTER_DATA[self.chain_name].get('dex_contracts', {})
        for dex_name, dex_data in dex_contracts.items():
            for token in ALL_BASE_TOKEN_TICKERS:
                token_address = DEX_ROUTER_DATA[self.chain_name].get(token)
                if token_address and token_address != DEX_ROUTER_DATA[self.chain_name]['gas_token']:
                    self.logger.info(f"Approving {token} for {dex_name}")
                    await self._approve_for_dex(dex_name, token_address)


    def _parse_token_transfers(self, receipt):
        """
        Парсит все transfers из транзакции
        
        Возвращает список трансферов: [
            {'from': '0x...', 'to': '0x...', 'value': 1000000000000000000},
            ...
        ]
        """

        transfers = []
    
        for log in receipt['logs']:
            # Проверяем, это ли Transfer событие ( 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef)
            if len(log['topics']) >= 3:
                topic0 = log['topics'][0].hex()

                if topic0 == 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef':
                    from_address = '0x' + log['topics'][1].hex()[-40:]  # берём последние 40 символов (20 байт)
                    to_address = '0x' + log['topics'][2].hex()[-40:]

                    value = int(log['data'].hex(), 16)
                    token_address = log['address']
                    
                    transfers.append({
                        'token': token_address,
                        'from': from_address,
                        'to': to_address,
                        'value': value
                    })
    
        return transfers
    
    async def _sign_and_send(self, tx: dict, wait_for_confirmation: bool = False, fast: bool = False) -> str:
        """
        Sign and send transaction.
        
        Args:
            tx: Transaction dict
            wait_for_confirmation: Wait for tx receipt
            fast: Use w3_latency provider for faster sending
        """
        signed = self.account.sign_transaction(tx)
        try:
            w3_provider = self.w3_latency if fast else self.w3
            tx_hash = await w3_provider.eth.send_raw_transaction(signed.raw_transaction)
            self._cached_nonce += 1 #локально управляем нонсом
            self.logger.info(f"TX sent{' (fast)' if fast else ''}: {tx_hash.hex()}")  
            if wait_for_confirmation:
                self.logger.info(f"Waiting for tx confirmation")
                receipt = await w3_provider.eth.wait_for_transaction_receipt(tx_hash)
                if receipt.status == 1:
                    self.logger.info(f"TX confirmed: {tx_hash.hex()}")
                    return tx_hash.hex()
                else:
                    self.logger.error(f"TX failed: {tx_hash.hex()}")
                    return None
            else:
                return tx_hash.hex()
              
        except Exception as e:
            self.logger.error(f"Transaction error: {str(e)}")
            return None

    def _get_buy_size_and_tp_id(self, mcap: int, sell_token: str) -> int:
        
        for config in MARKET_CAP_CONFIG:
            if mcap >= config['min_cap'] and mcap <= config['max_cap']:
                if config['enabled']:
                    return config['size'][sell_token], config['tp_ladder_id']
                else: 
                    self.logger.warning(f"Market cap config {config['min_cap']} - {config['max_cap']} is disabled, skipping")
        return 0, None

    def _get_swapper(self, dex_type: str):
        """Get the appropriate DEX swapper instance."""
        swapper = self.dex_instances.get(dex_type)
        if not swapper:
            self.logger.error(f"Swapper '{dex_type}' not initialized or not available")
        return swapper

    async def _get_token_price(
        self,
        swapper,
        dex_type: str,
        sell_token: str,
        sell_decimals: int,
        buy_token: str,
        buy_decimals: int,
        pool_info: dict,
        fast: bool = False
    ) -> float:
        """Get token price using the appropriate DEX swapper (polymorphic)."""
        try:
            if dex_type in ['uni_v2', 'cake_v2']:
                return await swapper.check_token_price(
                    sell_token,
                    sell_decimals,
                    buy_token,
                    buy_decimals,
                    gas_token_price=self.gas_token_price,
                    fast=fast
                )
            elif dex_type in ['uni_v3', 'cake_v3']:
                return await swapper.check_token_price(
                    sell_token,
                    sell_decimals,
                    buy_token,
                    buy_decimals,
                    pool_info.get('fee_tier'),
                    gas_token_price=self.gas_token_price,
                    fast=fast
                )
            elif dex_type in ['uni_v4', 'cake_v4']:
                return await swapper.check_token_price(
                    sell_token,
                    sell_decimals,
                    buy_token,
                    buy_decimals,
                    pool_info.get('pool_data'),
                    gas_token_price=self.gas_token_price,
                    fast=fast
                )
            else:
                self.logger.error(f"Unknown DEX type: {dex_type}")
                return None
        except Exception as e:
            self.logger.error(f"Error getting price from {dex_type}: {e}")
            return None

    async def _build_swap_transaction(
        self,
        swapper,
        dex_type: str,
        token_in: str,
        token_out: str,
        amount_in: int,
        amount_out_minimum: int,
        pool_info: dict
    ) -> dict:
        """Build swap transaction using the appropriate DEX swapper (polymorphic)."""
        try:
            #nonce = await self.w3.eth.get_transaction_count(self.account.address)
            
            if dex_type in ['uni_v2', 'cake_v2']:
                return await swapper.build_swap_transaction(
                    token_in,
                    token_out,
                    amount_in,
                    amount_out_minimum,
                    self._gas_price_cache,
                    self._cached_nonce
                )
            elif dex_type in ['uni_v3', 'cake_v3']:
                return await swapper.build_swap_transaction(
                    token_in,
                    token_out,
                    amount_in,
                    pool_info.get('fee_tier'),
                    amount_out_minimum,
                    self._gas_price_cache,
                    self._cached_nonce
                )
            elif dex_type in ['uni_v4', 'cake_v4']:
                return await swapper.build_swap_transaction(
                    token_in,
                    token_out,
                    amount_in,
                    amount_out_minimum,
                    pool_info.get('pool_data'),
                    self._gas_price_cache,
                    self._cached_nonce
                )
            else:
                self.logger.error(f"Unknown DEX type: {dex_type}")
                return None
        except Exception as e:
            self.logger.error(f"Error building swap transaction for {dex_type}: {e}")
            return None

    async def execute_swap(
        self,
        token_supply:int, 
        pool_data:dict, 
        position_size:int=None,
        custom_tp_ladder:dict=None
    ) -> str:

        """
            supply_data = 1234
            pool_data = {
                'token_address': token_address,
                'chain': 'ARBITRUM',
                'base_token': base_token_name,
                'dex_type': 'v3',
                'liquidity': pool_tvl,
                'pair_address': pool_address,
                'fee_tier': fee_tier
            }
        """

        # Extract pool data
        token_address = AsyncWeb3.to_checksum_address(pool_data.get('token_address'))
        dex_type = pool_data.get('dex_type')
        buy_token_decimals = pool_data.get('token_decimals', 18)
        base_token_name = pool_data.get('base_token')
        base_token_address = DEX_ROUTER_DATA[self.chain_name].get(base_token_name)
        
        self.logger.info(f"Starting swap execution for {token_address} on {dex_type}")
        
        # Get the appropriate swapper instance
        swapper = self._get_swapper(dex_type)
        if not swapper:
            self.logger.error(f"Cannot execute swap: swapper for {dex_type} not available")
            return None
        
        # Query token price using DEX-specific method (use fast connection for buy operations)
        self.logger.info(f"Querying token price based on cached pool data")
        t_before_query = time.perf_counter()
        price = await self._get_token_price(
            swapper,
            dex_type,
            base_token_address,
            self.token_decimals[base_token_address],
            token_address,
            buy_token_decimals,
            pool_data,
            fast=True  # Use fast connection for latency-sensitive buy operations
        )
        t_after_query = time.perf_counter()
        self.logger.debug(f"Real-time price query took {(t_after_query - t_before_query)*1000:.2f}ms")
        
        if not price:
            self.logger.error(f"Failed to get price for {token_address}")
            return None

        if base_token_address == DEX_ROUTER_DATA[self.chain_name]['gas_token']:
            mcap_usd_converter = self.gas_token_price * 1/price
        else:
            mcap_usd_converter = 1/price 

        mcap = int(float(token_supply) * mcap_usd_converter) if token_supply else pool_data.get('gecko_mcap', 0)
        amount_in, tp_ladder_id = self._get_buy_size_and_tp_id(mcap, base_token_name)
        if position_size:
            if base_token_address == DEX_ROUTER_DATA[self.chain_name]['gas_token']:
                amount_in = position_size/self.gas_token_price
            else:
                amount_in = position_size
        if not amount_in:
            return None
            
        # Build swap transaction using DEX-specific method
        self.logger.info(f"Swapping {amount_in} {base_token_name} for {token_address}")
        t_before_build = time.perf_counter()
        amount_out_minimum = int(amount_in * price * (100-SLIPPAGE_PERCENT[self.chain_name])/100) * 10**buy_token_decimals
        amount_in_normalized = int(amount_in * 10**self.token_decimals[base_token_address])
        
        tx = await self._build_swap_transaction(
            swapper,
            dex_type,
            base_token_address,
            token_address,
            amount_in_normalized,
            amount_out_minimum,
            pool_data
        )
        t_after_build = time.perf_counter()
        self.logger.debug(f"Build swap took {(t_after_build - t_before_build)*1000:.2f}ms")
        
        if not tx:
            self.logger.error(f"Failed to build swap transaction")
            return None

        #отправляем транзакцию (use fast connection for buy operations)
        t_before_send = time.perf_counter()
        result = await self._sign_and_send(tx, fast=True)  # Use fast connection for latency-sensitive buy
        if not result:
            return None
        t_after_send = time.perf_counter()
        self.logger.debug(f"send swap took {(t_after_send - t_before_send)*1000:.2f}ms")

        #ждем подтверждения транзакции
        t_before_receipt = time.perf_counter()
        receipt = await self.w3.eth.wait_for_transaction_receipt(result)
        t_after_receipt = time.perf_counter()
        self.logger.debug(f"Getting tx receipt took {(t_after_receipt - t_before_receipt)*1000:.2f}ms")
        if receipt is None or receipt.get('status',0) == 0:
            self.logger.error(f"Swap failed: tx status: {receipt.get('status', 'Tx not found')}")
            await self.tg_client.send_error_alert(
                'BUY FAILED',
                f"{self.chain_name} Swap failed: tx status: {receipt.get('status', 'Tx not found')}",
                'Need to check tx logs'
            )
                
            return None
        #запускаем тп таск
        else: 
            #считаем фактическую цену исполнения
            transfers = self._parse_token_transfers(receipt)
            actual_price = 0
            for transfer in transfers:
                if (
                    transfer['to'].lower() == self.account.address.lower() 
                    and transfer['token'].lower() == token_address.lower()
                ):
                    amount_received = transfer['value']
                    actual_price = (amount_in/amount_received) * (10**buy_token_decimals /10**self.token_decimals[base_token_address])
                
            if actual_price == 0:
                self.logger.warning(f"failed to calculate actual price, using cached price")
            else: 
                if base_token_address == DEX_ROUTER_DATA[self.chain_name]['gas_token']:
                    self.logger.info(f"actual price: {actual_price * self.gas_token_price}")
                else: 
                    self.logger.info(f"actual price: {actual_price}")

            #запускаем тп таск  
            await asyncio.sleep(DELAY_BEFORE_TP)
            self._take_profit_tasks.append(
                asyncio.create_task(
                    self._create_take_profit_task(
                        token_address,
                        base_token_address,
                        dex_type,
                        pool_data,
                        tp_ladder_id,
                        actual_price if actual_price > 0 else 1/price,
                        custom_tp_ladder=custom_tp_ladder
                    )
                )
            )
        return result

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
                        tp_data['dex_type'],
                        tp_data['pool_info'],
                        tp_data['take_profit_ladder_id'],
                        tp_data['price_bought'],
                        tp_data['steps_done'],
                        custom_tp_ladder=tp_data.get('custom_tp_ladder')
                    )
                )
            )
        return 
        
    async def _create_take_profit_task(
        self, 
        token_address_to_sell: str, 
        base_token_address: str,
        dex_type: str,
        pool_info: dict,
        take_profit_ladder_id: int, 
        price_bought: float,
        steps_done: int = 0,
        tx_failure_counter: int = 5,
        custom_tp_ladder: dict = None
    ):
        """
        Мониторит цену токена и продает по лестнице тейк-профитов или в стоплосс
        
        Args:
            token_address_to_sell: Адрес токена для продажи
            base_token_address: Адрес токена, в который продаем (USDT/USDC/WETH)
            dex_type: Тип DEX (uni_v2, uni_v3, uni_v4, cake_v2, cake_v3, cake_v4)
            pool_info: Информация о пуле (fee_tier для V3, pool_data для V4)
            take_profit_ladder_id: ID конфигурации лестницы из TP_LADDERS
            price_bought: Цена покупки токена в token_sell_to
            steps_done: Количество проданных шагов (default - 0)
        """
        #аппрув токена для свапа
        for i in range(tx_failure_counter):
            approved = await self._approve_for_dex(dex_type, token_address_to_sell)
            if approved:
                break
            else: 
                if i == tx_failure_counter - 1:
                    self.logger.error(f"TP task | Failed to approve token for swap")
                    await self.tg_client.send_error_alert(
                        "TP task FAILED", 
                        f"{self.chain_name} Failed to approve token {token_address_to_sell} for swap",
                        "Need to check manually"
                        )
                await asyncio.sleep(5)
        
        #получаем конфигурацию тп сетки
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

        #ставим стоплосс
        stop_loss_price = price_bought * (1 + ladder_config['SL_from_entry_percent'])
        
        #получаем параметры сетки
        first_tp_percent = ladder_config['first_tp_percent']
        total_percent = ladder_config['total_percent']
        steps = ladder_config['steps']
        distribution = ladder_config['distribution']
        
        #получаем баланс токена
        token_contract = self.w3.eth.contract(address=AsyncWeb3.to_checksum_address(token_address_to_sell), abi=erc20_abi)
        try:
            balance_task = token_contract.functions.balanceOf(self.account.address)
            decimals_task = token_contract.functions.decimals()  
            total_balance, sell_token_decimals = await asyncio.gather(balance_task.call(), decimals_task.call())
            
        except Exception as e:
            self.logger.error(f"TP task | {token_address_to_sell} | Failed to get token balance: {str(e)}")
            await self.tg_client.send_error_alert(
                "TP task FAILED", 
                f"{self.chain_name} Failed to get token balance for {token_address_to_sell}",
                "Need to check manually"
                )
            return 0
        
        if total_balance == 0:
            self.logger.warning(f"TP task | {token_address_to_sell} Zero balance ")
            await self.tg_client.send_error_alert(
                "TP task DISABLED", 
                f"{self.chain_name} Zero balance for {token_address_to_sell}",
                "TP task stopped"
                )
            return 0
        
        #Сохраняем данные в кэш и в json
        self._take_profit_cache[token_address_to_sell] = {
            'base_token_address': base_token_address,
            'dex_type': dex_type,
            'pool_info': pool_info,
            'take_profit_ladder_id': take_profit_ladder_id,
            'price_bought': raw_price_bought,
            'steps_done': steps_done,
            'custom_tp_ladder': custom_tp_ladder
        }
        await self._update_take_profit_json()
        
        self.logger.info(f"TP task | Starting TP task for {token_address_to_sell} | Ladder id: {take_profit_ladder_id} | Balance: {total_balance/10**sell_token_decimals:.3f} | SL at {stop_loss_price:.4f}")
        
        #Рассчитываем ценовые уровни для каждого шага
        price_step_percent = (total_percent - first_tp_percent) / (steps - 1) if steps > 1 else 0
        tp_levels = []
        
        for i in range(steps):
            target_percent = first_tp_percent + (price_step_percent * i)
            target_price = price_bought * (1 + target_percent)
            sell_amount = int((total_balance * distribution[i]) / 100)
            tp_levels.append({
                'step': i + 1,
                'target_price': target_price,
                'target_percent': target_percent,
                'sell_amount': sell_amount,
                'size_percent': distribution[i],
                'executed': False if i >= steps_done else True
            })
        self.logger.info(f"TP task | TP levels calculated: {steps-steps_done}/{steps} steps left from {tp_levels[0]['target_price']:.6f} to {tp_levels[-1]['target_price']:.6f}")
        
        #мониторинг цены и выполнение продаж
        poll_interval = PRICE_UPDATE_DELAY[self.chain_name]
        tx_failure = 0
        while True:

            #Если количество неуспешных транзакций превысило лимит, завершаем задачу
            if tx_failure > tx_failure_counter:
                self.logger.error(f"TP task | failed for {token_address_to_sell}")
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
                # Get swapper instance
                swapper = self._get_swapper(dex_type)
                if not swapper:
                    self.logger.error(f"TP task | Swapper {dex_type} not available")
                    await asyncio.sleep(poll_interval)
                    continue
                
                # Получаем текущую цену
                current_price = await self._get_token_price(
                    swapper,
                    dex_type,
                    token_address_to_sell,
                    sell_token_decimals,
                    base_token_address,
                    self.token_decimals[base_token_address],
                    pool_info
                )
                
                if current_price is None:
                    self.logger.warning(f"TP task | Failed to get current price for {token_address_to_sell}")
                    await asyncio.sleep(poll_interval)
                    continue
                current_price = current_price * price_corrector

                #Триггерим стоплосс, передаем единственный тейкпрофит - продажа всего
                if current_price <= stop_loss_price:
                    self.logger.warning(f"TP task | Stop loss triggered for {token_address_to_sell}")
                    total_balance = await token_contract.functions.balanceOf(self.account.address).call()
                    tp_levels = [
                        {
                            'step': 0,
                            'target_price': 0,
                            'target_percent': 100,
                            'sell_amount': total_balance,
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
                        decimals_corrector = 10**self.token_decimals[base_token_address]/10**sell_token_decimals
                        amount_out_minimum = int(level['sell_amount'] * decimals_corrector * current_price * (100 - SLIPPAGE_PERCENT[self.chain_name]) / (price_corrector*100))
                        
                        tx = await self._build_swap_transaction(
                            swapper,
                            dex_type,
                            token_address_to_sell,
                            base_token_address,
                            level['sell_amount'],
                            amount_out_minimum,
                            pool_info
                        )
                        result = await self._sign_and_send(tx, True)

                        #проверям результат, убираем флаг на тп
                        if result:
                            level['executed'] = True
                            #обновляем кэш
                            if token_address_to_sell in self._take_profit_cache:
                                self._take_profit_cache[token_address_to_sell]['steps_done'] = level['step']
                                await self._update_take_profit_json()
                            self.logger.success(f"TP task | {token_address_to_sell} | TP level {level['step']} executed: {level['size_percent']}% sold at {current_price:.6f} | TX: {result}")
                            await self.tg_client.tp_task_message(
                                self.chain_name,
                                token_address_to_sell,
                                price_bought,
                                current_price,
                                level['step'],
                                tx_hash = result
                            )
                        else:
                            tx_failure += 1
                            self.logger.error(f"TP task | {token_address_to_sell} | Failed to execute TP level {level['step']}") 
                            await self.tg_client.send_error_alert(
                                "TP task FAILED", 
                                f"{self.chain_name} Failed to execute TP level {level['step']}",
                                f"retries: {tx_failure}/{tx_failure_counter}"
                                ) 
                            break  
                await asyncio.sleep(poll_interval)

            except Exception as e:
                self.logger.error(f"TP task | {token_address_to_sell} | Error in TP monitoring loop: {str(e)}")
                await self.tg_client.send_error_alert(
                    "TP task ERROR", 
                    f"{self.chain_name} Error in TP monitoring loop but still running",
                    str(e)
                    ) 
                tx_failure += 1
                await asyncio.sleep(poll_interval)
    