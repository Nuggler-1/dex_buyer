
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
    DELAY_BETWEEN_BATCHES,
    GAS_UPDATE_INTERVAL,
    CACHE_UPDATE_BATCH_SIZE,
    GAS_MULTPLIER, 
    GAS_LIMIT, 
    CHAIN_NAMES,
    MARKET_CAP_CONFIG,
    RPC,
    WS_RPC,
    USE_WEBSOCKET,
    SLIPPAGE_PERCENT,
    GAS_LIMIT,
    TOKEN_DATA_BASE_PATH,
    USABLE_TOKENS,
    TP_LADDERS,
    PRICE_UPDATE_DELAY,
    DELAY_BEFORE_TP,
    MIN_POOL_TVL,
    
)
from .consts import DEX_ROUTER_DATA, ZERO_ADDRESS, erc20_abi, quoter_abi, factory_abi
from typing import Literal



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
        if use_websocket and WS_RPC.get(chain_name):
            self.w3 = AsyncWeb3(WebSocketProvider(WS_RPC[chain_name]))
            self.using_websocket = True
            self.logger.info("Using WebSocket RPC")
        else:
            self.w3 = AsyncWeb3(AsyncHTTPProvider(RPC[chain_name]))
            self.using_websocket = False
            self.logger.debug("Using HTTP RPC")

        # if chain_name in ['BSC', 'POLYGON']:
        #     self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        
        #misc
        self.tg_client = tg_client
        self.ws_connection_check_interval = 5
        self.account = Account.from_key(private_key)
        self.quoter_contract = self.w3.eth.contract(address=DEX_ROUTER_DATA[chain_name]['quoter_address'], abi=quoter_abi)
        self.factory_contract = self.w3.eth.contract(address=DEX_ROUTER_DATA[chain_name]['factory_address'], abi=factory_abi)
        self._build_swap_tx: Callable = { #для каждого чейна своя функция свапа, коллим сразу _build_swap_tx
            'ETHEREUM': self._build_uniswap_v3_swap_transaction,     
            'BSC': self._build_bsc_swap,          
            'ARBITRUM': self._build_uniswap_v3_swap_transaction,
        }[chain_name]
        self.gas_update_interval = gas_update_interval
        self.gas_multiplier = gas_multiplier
        
        #задаем chain-specific data

        #chain-data
        self.chain_name = chain_name
        self.chain_id = DEX_ROUTER_DATA[chain_name]['chain_id']
        self.dex_router_address = DEX_ROUTER_DATA[chain_name]['router_address']

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
        
        if self.using_websocket:
            await self.w3.provider.connect()

        #готовим кэшированные данные и отправляем approve
        await self._initialize_blockchain_cache_vars()
        await self._send_approve()
        
        #фоновый луп обновления газа
        self._gas_updater_task = asyncio.create_task(self._gas_price_updater_loop())
        #фоновый луп обновления цены газтокена
        self._gas_token_price_updater_task = asyncio.create_task(self._gas_token_price_updater_loop())
        #фоновый луп проверки подключения к ws
        self._ws_connection_checker_task = asyncio.create_task(self._ws_connection_checker())
        #запускаем тейкпрофиты
        await self._start_take_profit_tasks()
        
        self._initialized = True

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

    async def _approve_token_for_swap(self,token_address:str):
        try: 
            contract = self.w3.eth.contract(address=token_address, abi=erc20_abi)
            allowance = await contract.functions.allowance(
                self.account.address, DEX_ROUTER_DATA[self.chain_name]['spender_address']
            ).call()

            if allowance < 2**128:
                self.logger.info(f"Not enough allowance for {token_address}, sending approve tx")
                approve_tx = await contract.functions.approve(
                    DEX_ROUTER_DATA[self.chain_name]['spender_address'], 2**256 - 1
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

    async def _send_approve(self):
        """
        Approve all usable tokens for trading
        """
        for base_token in self.usable_tokens:
            token_address = DEX_ROUTER_DATA[self.chain_name].get(base_token)
            if token_address == DEX_ROUTER_DATA[self.chain_name]['gas_token']:
                continue
            await self._approve_token_for_swap(token_address)
        
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
        while True:
            try:
                price = await self._check_token_price(
                    DEX_ROUTER_DATA[self.chain_name]['gas_token'],
                    18,
                    DEX_ROUTER_DATA[self.chain_name]['USDT'],
                    self.token_decimals['USDT'],
                    500,
                    1*10**18
                )
                if price:
                    self.gas_token_price = price 
                if init:
                    return price
                await asyncio.sleep(30)
            except Exception as e:
                self.logger.error(f"Native token price update error: {str(e)}")
                await asyncio.sleep(30)
    
    async def _check_token_price(
        self, 
        sell_token_address: str, 
        sell_token_decimals: int,
        buy_token_address: str, 
        buy_token_decimals:int,
        cached_fee_tier: int,
        amount_in: int = None,
    ) -> tuple:
        """

        !!!!Прайс напрямую зависит от количества amount_in

        Проверяет фии тиры и цену перебирая все доступные по возрастанию. Если нет пула - возвращает None

        если cached_fee_tier не None, то проверяет только этот тир

        Возвращает цену токена sell_token_address в токенах buy_token_address

        Args:
            sell_token_address (str): Адрес токена продажи (WETH/USDC/USDT)
            buy_token_address (str): Адрес токена покупки
            amount_in (int, optional): Количество токена продажи нормализованное 
        """
        if amount_in is None:
            if sell_token_address == DEX_ROUTER_DATA[self.chain_name]['gas_token']:
                amount_in = int((1/self.gas_token_price) * 10 ** sell_token_decimals)  # 1 usd in WETH/WBNB
            else:
                amount_in = int(1 * 10 ** sell_token_decimals)  # 1 USDT/USDC/TOKEN

        try:
            #Квотим свап
            amount_out = await self.quoter_contract.functions.quoteExactInputSingle(
                (
                    sell_token_address,
                    buy_token_address,
                    amount_in,
                    cached_fee_tier,
                    0
                )
            ).call()
            #Если успех, то пул существует
            return ( amount_out / (10 ** buy_token_decimals) ) / (amount_in / (10 ** sell_token_decimals))
        except Exception as e:
            self.logger.warning(f"error getting onchain swap data for {buy_token_address} with {sell_token_address}: {str(e)}")
            return None

    def encode_uniswap_v2_single_swap(
        self,
        token_in: str,
        token_out:str,
        amount_in: int,
        amount_out_minimum: int,
        deadline: int
        ) -> bytes:
        function_selector = self.w3.keccak(text="swapExactTokensForTokens(uint256,uint256,address[],address,uint256)")[:4]
        
        path = [token_in, token_out]
        
        encoded_params = encode(
            ['uint256', 'uint256', 'address[]', 'address', 'uint256'],
            [amount_in, amount_out_minimum, path, self.account.address, deadline]
        )
        
        data = function_selector + encoded_params
        
        return data

    def encode_uniswap_v3_single_swap(
        self,
        token_in: str,
        token_out: str,
        fee: int,
        amount_in: int,
        amount_out_minimum: int,
        deadline: int
    ) -> bytes:

        function_signature = "exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))"
        function_selector = self.w3.keccak(text=function_signature)[:4]
        
        params = (
            self.w3.to_checksum_address(token_in),     # tokenIn
            self.w3.to_checksum_address(token_out),    # tokenOut
            fee,                                        # fee (uint24)
            self.account.address,                       # recipient
            deadline,                                   # deadline
            amount_in,                                  # amountIn
            amount_out_minimum,                         # amountOutMinimum
            0                                           # sqrtPriceLimitX96 (0 = no limit)
        )

        encoded_params = encode(
            ['(address,address,uint24,address,uint256,uint256,uint256,uint160)'],
            [params]
        )
        
        return function_selector + encoded_params
    
    async def _build_uniswap_v3_swap_transaction(
        self,
        token_in: str,
        token_out: str,
        amount: int,
        fee: int,
        amount_out_minimum: int
    ) -> dict:
        
        if token_in == DEX_ROUTER_DATA[self.chain_name].get('gas_token'):
            is_eth_in = True
        else:
            is_eth_in = False
        
        deadline = int(time.time()) + 300
        
        data = self.encode_uniswap_v3_single_swap(
            token_in=token_in,
            token_out=token_out,
            fee=fee,
            amount_in=amount,
            amount_out_minimum=amount_out_minimum,
            deadline=deadline
        )
    
        # Строим транзакцию
        tx = {
            'from': self.account.address,
            'to': self.dex_router_address,
            'value': amount if is_eth_in else 0,  
            'gas': GAS_LIMIT[self.chain_name],  
            'gasPrice': self._gas_price_cache,  
            'nonce': self._cached_nonce,
            'chainId': self.chain_id,
            'data': data
        }
        
        return tx

    async def _build_bsc_swap(
        self, 
        base_token_address: str, 
        sell_token_address: str, 
        amount_in: int,
        fee: int, 
        amount_out_minimum: int
    ) -> dict:
        tx = await self._build_uniswap_v3_swap_transaction(
            base_token_address,
            sell_token_address,
            amount_in,
            fee,
            amount_out_minimum
        )
        return tx

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
    
    async def _sign_and_send(self, tx: dict, wait_for_confirmation: bool = False) -> str:

        signed = self.account.sign_transaction(tx)
        try: 
            tx_hash = await self.w3.eth.send_raw_transaction(signed.raw_transaction)
            self._cached_nonce += 1 #локально управляем нонсом
            self.logger.info(f"TX sent: {tx_hash.hex()}")  
            if wait_for_confirmation:
                self.logger.info(f"Waiting for tx confirmation")
                receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
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

        token_address = AsyncWeb3.to_checksum_address(pool_data.get('token_address'))
        dex_type = pool_data.get('dex_type')
        pair_address = pool_data.get('pair_address')
        buy_token_decimals = pool_data.get('token_decimals')
        fee_tier = pool_data.get('fee_tier')
        base_token_name = pool_data.get('base_token')
        base_token_address = DEX_ROUTER_DATA[self.chain_name].get(base_token_name)
        self.logger.info(f"Starting swap execution for {token_address}")
        
        #в кэше есть данные о fee-tier
        self.logger.info(f"quering token price based on cahced fee-tier data")
        t_before_query = time.perf_counter()
        price = await self._check_token_price(
            base_token_address, 
            self.token_decimals[base_token_address],
            token_address,
            18,
            fee_tier,
        )
        t_after_query = time.perf_counter()
        self.logger.debug(f"Real-time price query took {(t_after_query - t_before_query)*1000:.2f}ms")

        if not base_token_address:
            self.logger.error(f"No available data for token {token_address}")
            return None

        if base_token_address == DEX_ROUTER_DATA[self.chain_name]['gas_token']:
            mcap_usd_converter = self.gas_token_price * 1/price
        else:
            mcap_usd_converter = 1/price 

        mcap = int(float(token_supply) * mcap_usd_converter)
        amount_in, tp_ladder_id = self._get_buy_size_and_tp_id(mcap, base_token_name)
        if position_size:
            if base_token_address == DEX_ROUTER_DATA[self.chain_name]['gas_token']:
                amount_in = position_size/self.gas_token_price
            else:
                amount_in = position_size
        if not amount_in:
            return None
            
        #билдим транзу
        self.logger.info(f"Swapping {amount_in} {base_token_name} for {token_address}")
        t_before_build = time.perf_counter()
        amount_out_minimum = int(amount_in * price * (100-SLIPPAGE_PERCENT)/100) * 10**buy_token_decimals
        amount_in = int(amount_in * 10**self.token_decimals[base_token_address])
        tx = await self._build_swap_tx(base_token_address, token_address, amount_in, fee_tier, amount_out_minimum)
        t_after_build = time.perf_counter()
        self.logger.debug(f"Build swap took {(t_after_build - t_before_build)*1000:.2f}ms")

        #отправляем транзакцию
        t_before_send = time.perf_counter()
        result = await self._sign_and_send(tx)
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
                self.logger.info(f"actual price: {actual_price * self.gas_token_price}")

            #запускаем тп таск  
            await asyncio.sleep(DELAY_BEFORE_TP)
            self._take_profit_tasks.append(
                asyncio.create_task(
                    self._create_take_profit_task(
                        token_address,
                        base_token_address,
                        fee_tier,
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
                        tp_data['fee_tier'],
                        tp_data['take_profit_ladder_id'],
                        tp_data['price_bought'],
                        tp_data['steps_done'],
                        custom_tp_ladder=tp_data['custom_tp_ladder']
                    )
                )
            )
        return 
        
    async def _create_take_profit_task(
        self, 
        token_address_to_sell: str, 
        base_token_address: str,
        token_pool_fee_tier:int,
        take_profit_ladder_id: int, 
        price_bought: float,
        steps_done: int = 0,
        tx_failure_counter: int = 5,
        custom_tp_ladder:dict=None
    ):
        """
        Мониторит цену токена и продает по лестнице тейк-профитов или в стоплосс
        
        Args:
            token_address_to_sell: Адрес токена для продажи
            base_token_address: Адрес токена, в который продаем (USDT/USDC/WETH)
            token_pool_fee_tier: Fee-tier пула
            take_profit_ladder_id: ID конфигурации лестницы из TP_LADDERS
            price_bought: Цена покупки токена в token_sell_to
            steps_done: Количество проданных шагов (default - 0)
        """
        #аппрув токена для свапа
        for i in range(tx_failure_counter):
            approved = await self._approve_token_for_swap(token_address_to_sell)
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
            'fee_tier': token_pool_fee_tier,
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
                # Получаем текущую цену
                current_price = await self._check_token_price(
                    token_address_to_sell,
                    18,
                    base_token_address,
                    self.token_decimals[base_token_address],
                    token_pool_fee_tier,
                    1*10**sell_token_decimals
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
                        amount_out_minimum = int(level['sell_amount'] * decimals_corrector * current_price * (100 - SLIPPAGE_PERCENT) / (price_corrector*100))
                        tx = await self._build_swap_tx(
                            token_address_to_sell,
                            base_token_address,
                            level['sell_amount'],
                            token_pool_fee_tier,
                            amount_out_minimum
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
    