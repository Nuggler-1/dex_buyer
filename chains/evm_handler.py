
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
from loguru import logger
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
    CACHE_PRICE,
    PARSED_DATA_CHECK_DELAY_DAYS,
    FORCE_UPDATE_ON_START,
    USABLE_TOKENS,
    ERROR_429_DELAY,
    ERROR_429_RETRIES, 
    GECKO_API_KEY,
    GECKO_CHAIN_NAMES,
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
        token_pool: list,
        gas_update_interval: int = GAS_UPDATE_INTERVAL,
        gas_multiplier: int = GAS_MULTPLIER,
        use_websocket: bool = USE_WEBSOCKET
    ):
        if use_websocket and WS_RPC.get(chain_name):
            self.w3 = AsyncWeb3(WebSocketProvider(WS_RPC[chain_name]))
            self.using_websocket = True
            logger.info(f"[{chain_name}] Using WebSocket RPC")
        else:
            self.w3 = AsyncWeb3(AsyncHTTPProvider(RPC[chain_name]))
            self.using_websocket = False
            logger.debug(f"[{chain_name}] Using HTTP RPC")

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
        self.token_pool = token_pool #пул кэшированных токенов
        self.fee_tier_cached = False #флаг, указывающий на то, что fee tiers уже загружены
        self.token_fee_tiers = [500, 3000, 10000] if chain_name != 'BSC' else [500, 2500, 10000] #возможные fee tiers для токенов
        self.token_data = {
            token: {
                'price': {
                    'USDC': 0,
                    'USDT': 0,
                    'WETH': 0,
                    'WBNB': 0,
                }, 
                'fee_tier': {
                    'USDC': 0,
                    'USDT': 0,
                    'WETH': 0,
                    'WBNB': 0,
                }
            } for token in token_pool
        }

        #кэш
        self._last_fee_tier_update_time = None
        self._cached_nonce = None #манагерим нонс руками, чтобы не тратить время на запросы к рпс
        self._gas_price_cache = None #cached gas price
        self._initialized = False
        self._take_profit_cache = {}  # {token_address: tp_data}
        self.fee_tier_path = TOKEN_DATA_BASE_PATH + f'{chain_name}_pool_data.json' #путь к файлу с кэшированным fee tiers
        self.tp_cache_path = TOKEN_DATA_BASE_PATH + '/TP_data/'+ f'{chain_name}_TP_cache.json' #путь к файлу с тп кэшем

        #background tasks
        self._gas_updater_task = None
        self._gas_token_price_updater_task = None
        self._price_updater_task = None
        self._take_profit_tasks = []
        self._ws_connection_checker_task = None
    @classmethod
    async def create(
        cls,
        tg_client: TelegramClient,
        private_key: str,
        chain_name: Literal[*CHAIN_NAMES],
        token_pool: list,
        gas_update_interval: int = GAS_UPDATE_INTERVAL,
        gas_multiplier: int = GAS_MULTPLIER,
        use_websocket: bool = USE_WEBSOCKET
    ):
        instance = cls(tg_client, private_key, chain_name, token_pool, gas_update_interval, gas_multiplier, use_websocket)
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
        
        #загружаем кэшированные fee tiers
        cached_fee_tiers = self._load_fee_tier_cache()
        if cached_fee_tiers:
            for token, fee_tier in cached_fee_tiers.items():
                self.token_data[token]['fee_tier'] = fee_tier
            self.fee_tier_cached = True
        
        #фоновый луп обновления газа
        self._gas_updater_task = asyncio.create_task(self._gas_price_updater_loop())
        #фоновый луп обновления цен токенов и fee tiers
        self._price_updater_task = asyncio.create_task(self._price_and_fee_tier_updater_loop(update_price=CACHE_PRICE)) #поменять на True когда будет нужен апдейт цен
        #фоновый луп обновления цены газтокена
        self._gas_token_price_updater_task = asyncio.create_task(self._gas_token_price_updater_loop())
        #фоновый луп проверки подключения к ws
        self._ws_connection_checker_task = asyncio.create_task(self._ws_connection_checker())
        #запускаем тейкпрофиты
        await self._start_take_profit_tasks()
        
        self._initialized = True

    async def close(self):
        """Close handler and stop all background tasks"""
        logger.info(f"[{self.chain_name}] Closing handler...")
        
        tasks_to_cancel = []
        
        if self._gas_updater_task and not self._gas_updater_task.done():
            tasks_to_cancel.append(self._gas_updater_task)
        
        if self._price_updater_task and not self._price_updater_task.done():
            tasks_to_cancel.append(self._price_updater_task)
        
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
            logger.info(f"[{self.chain_name}] All background tasks cancelled ({len(tasks_to_cancel)} tasks)")
        
        #закрываем ws
        if self.using_websocket:
            try:
                await self.w3.provider.disconnect()
                logger.info(f"[{self.chain_name}] WebSocket connection closed")
            except Exception as e:
                logger.warning(f"[{self.chain_name}] Error closing WebSocket: {e}")

    async def _approve_token_for_swap(self,token_address:str):
        try: 
            contract = self.w3.eth.contract(address=token_address, abi=erc20_abi)
            allowance = await contract.functions.allowance(
                self.account.address, DEX_ROUTER_DATA[self.chain_name]['spender_address']
            ).call()

            if allowance < 2**128:
                logger.info(f"[{self.chain_name}] Not enough allowance for {token_address}, sending approve tx")
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
                    logger.error(f"[{self.chain_name}] Approve tx for {token_address} failed")
                    return None
                else: 
                    return tx
            else:
                logger.info(f"[{self.chain_name}] Approve not required for {token_address}")
                return True
        except Exception as e:
            logger.error(f"[{self.chain_name}] Error checking/sending approve for {token_address}: {str(e)}")
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
        logger.info(f"[{self.chain_name}] Gas price updater loop started")
        while True:
            try:
                self._gas_price_cache = int(await self.w3.eth.gas_price * self.gas_multiplier)
                #self._cached_nonce = await self.w3.eth.get_transaction_count(self.account.address, 'pending')
                await asyncio.sleep(self.gas_update_interval)
            except Exception as e:
                logger.error(f"[{self.chain_name}] Gas price update error: {str(e)}")
                await asyncio.sleep(self.gas_update_interval)

    async def _ws_connection_checker(self):
        
        while True:
            try:
                if not await self.w3.provider.is_connected():
                    await self.w3.provider.connect()
                    logger.info(f"[{self.chain_name}] WebSocket connection reestablished")
                await asyncio.sleep(self.ws_connection_check_interval)
            except Exception as e:
                logger.error(f"[{self.chain_name}] WebSocket connection error: {str(e)}")
                await self.tg_client.send_error_alert(
                    "RPC WEBSCOKET DISCONNECTED",
                    f"{self.chain_name} WebSocket connection error: {str(e)}",
                )
                await asyncio.sleep(self.ws_connection_check_interval)

    async def _gas_token_price_updater_loop(self, init:bool=False):
        logger.info(f"[{self.chain_name}] Gas token price updater loop started")
        while True:
            try:
                _, price = await self._check_token_fee_tier_and_price(
                    DEX_ROUTER_DATA[self.chain_name]['gas_token'],
                    18,
                    DEX_ROUTER_DATA[self.chain_name]['USDT'],
                    self.token_decimals['USDT'],
                    1*10**18,
                    500
                )
                if price:
                    self.gas_token_price = price 
                if init:
                    return price
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"[{self.chain_name}] Native token price update error: {str(e)}")
                await asyncio.sleep(30)

    def _load_fee_tier_cache(self) -> dict:
        """
        Загружает кэш fee tiers из файла, если он существует и не старше CHECK_DELAY_DAYS, иначе возвращает пустой словарь
        """
        if FORCE_UPDATE_ON_START:
            logger.info(f"[{self.chain_name}] Forcing fee tier cache update")
            return {}
        #проверяем наличие файлов
        if not os.path.exists(self.fee_tier_path):
            logger.info(f"[{self.chain_name}] No fee tier cache found")
            return {}
        #загружаем сохраненный кэш из файла
        with open(self.fee_tier_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
            if not isinstance(raw_data, list) or len(raw_data) != 2:
                logger.info(f"[{self.chain_name}] Invalid fee tier json format, creating new")
                return {}
            cache_update_time = raw_data[0]
            cache_data = raw_data[1]

        if not cache_data:
            logger.info(f"[{self.chain_name}] No cached fee tiers for this chain")
            return {}
        
        #достаем дату последнего чека
        last_time = cache_update_time
        if last_time == '':
            return {}
        last_check = datetime.fromisoformat(last_time)
        
        #если кэш слишком старый игнорируем
        if datetime.now() - last_check > timedelta(days=PARSED_DATA_CHECK_DELAY_DAYS):
            logger.info(f"[{self.chain_name}] Fee tier cache is older than {PARSED_DATA_CHECK_DELAY_DAYS} days, will refresh")
            return {}
        else:
            self._last_fee_tier_update_time = last_check
            self.fee_tier_cached = True

        not_zero_fee_tiers = {
            token: data for token, data in cache_data.items() if any(v != 0 for v in data.values())
            }
        logger.warning(f"[{self.chain_name}] Loaded {len(not_zero_fee_tiers)} cached fee tiers out of {len(self.token_pool)} parsed tokens")
        return not_zero_fee_tiers
    
    def _save_fee_tier_cache(self):
        """
        Сохраняет текущие фии тиры из self.token_data в файл
        """

        os.makedirs(os.path.dirname(self.fee_tier_path), exist_ok=True)
        
        #Загружаем текущий кэш
        if os.path.exists(self.fee_tier_path):
            with open(self.fee_tier_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
                if not isinstance(raw_data, list) or len(raw_data) != 2:
                    logger.info(f"[{self.chain_name}] Invalid fee tier json format, creating new")
                    fee_tiers = {} 
                else: 
                    fee_tiers = raw_data[1]
        else:
            fee_tiers = {}
        
        #записываем текущие тиры
        fee_tiers = {
            token: {
                token_name: data['fee_tier'].get(token_name, 0) for token_name in self.usable_tokens
            } 
            for token, data in self.token_data.items() 
            if data.get('fee_tier') and any(data['fee_tier'].values())
        }
        
        with open(self.fee_tier_path, 'w', encoding='utf-8') as f:
            json.dump([datetime.now().isoformat(), fee_tiers], f, indent=2)
        
        self.fee_tier_cached = True
        self._last_fee_tier_update_time = datetime.now()
        logger.info(f"[{self.chain_name}] Saved {len(fee_tiers)} fee tiers to cache")

    """
    async def _get_pool_tvl_uniswap(self, pool_address: str): 

        url = 'https://interface.gateway.uniswap.org/v1/graphql'

        headers = {
            '_dd-custom-header-graph-ql-operation-name': 'V3Pool',
            '_dd-custom-header-graph-ql-operation-type': 'query',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            #'Connection': 'keep-alive',
            'Origin': 'https://app.uniswap.org',
            'Referer': 'https://app.uniswap.org',
            #'sec-ch-ua': '"Google Chrome";v="122", "Chromium";v="122", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'Content-Type': 'application/json',
            'User-Agent': UserAgent().random

        }

        query = "query V3Pool($chain: Chain!, $address: String!) {\n  v3Pool(chain: $chain, address: $address) {\n    totalLiquidity {\n      value\n      __typename\n    }\n} \n}"
                
        payload = {
            'operationName': 'V3Pool',
            'query': query,
            'variables': {
                'chain': self.chain_name,
                'address': pool_address
            }
        }

        for i in range(ERROR_429_RETRIES):
            try:
                async with AsyncSession() as session:
                    response = await session.post(url, json=payload, headers=headers)
                    response.raise_for_status()
                    data = response.json()
                return data['data']['v3Pool']['totalLiquidity']['value']
            except Exception as e:
                if any(['429' in str(e), 'rate limit' in str(e)]):
                    logger.error(f"[{self.chain_name}] tvl query Rate limited, waiting {ERROR_429_DELAY} seconds")
                    await asyncio.sleep(ERROR_429_DELAY)
                else:
                    logger.error(f"[{self.chain_name}] Error getting pool TVL: {str(e)}")
                    break
        return None
    

    async def _get_pool_tvl_pancake(self, pool_address: str): 
        url = 'https://explorer.pancakeswap.com/api/cached/pools/bsc/'+pool_address
        for i in range(ERROR_429_RETRIES):
            try:
                async with AsyncSession() as session:
                    response = await session.get(url)
                    response.raise_for_status()
                    data = response.json()
                return float(data['tvlUSD'])
            except Exception as e:
                if any(['429' in str(e), 'rate limit' in str(e)]):
                    logger.error(f"[{self.chain_name}] tvl query Rate limited, waiting {ERROR_429_DELAY} seconds")
                    await asyncio.sleep(ERROR_429_DELAY)
                else:
                    logger.error(f"[{self.chain_name}] Error getting pool TVL: {str(e)}")
                    break

    """

    """async def _get_pool_base_token_tvlUSD(
        self, 
        pool_tokens:list,
        pool_address: str
    ) -> float: 
        base_token_addresses = [ DEX_ROUTER_DATA[self.chain_name].get(ticker) for ticker in USABLE_TOKENS]
        base_token_address = [i for i in base_token_addresses if i in pool_tokens][0]
        other_token_address = [i for i in pool_tokens if i != base_token_address][0]
        base_token_contract = self.w3.eth.contract(address=base_token_address, abi=erc20_abi)
        base_token_decimals = self.token_decimals[base_token_address]
        
        other_token_contract = self.w3.eth.contract(address=other_token_address, abi=erc20_abi)
        other_token_balance = await other_token_contract.functions.balanceOf(pool_address).call()
        if other_token_balance == 0:
            return 0

        balance_of_pool = await base_token_contract.functions.balanceOf(pool_address).call()
        if balance_of_pool == 0:
            return 0
        if base_token_address == DEX_ROUTER_DATA[self.chain_name]['gas_token']:
            return self.gas_token_price * balance_of_pool / 10 ** base_token_decimals
        else:
            return balance_of_pool / 10 ** base_token_decimals"""

    
    async def _get_pool_tvlUSD(self, pool_address: str):
        headers = {
            'x-cg-demo-api-key': GECKO_API_KEY
        }
        chain_name = GECKO_CHAIN_NAMES.get(self.chain_name)
        if not chain_name:
            return None
        url = f'https://api.coingecko.com/api/v3/onchain/networks/{chain_name}/pools/{pool_address}'
        for _ in range(ERROR_429_RETRIES):
            try:
                async with AsyncSession() as session:
                    response = await session.get(url, headers=headers)
                    response.raise_for_status()
                    data = response.json()
                tvl_usd = data.get('data',{}).get('attributes',{}).get('reserve_in_usd', 0)
                if not tvl_usd:
                    logger.warning(f"[{self.chain_name}] Pool TVL for {pool_address} not found")
                return tvl_usd

            except Exception as e:
                if any(['429' in str(e), 'rate limit' in str(e)]):
                    logger.error(f"[{self.chain_name}] tvl query Rate limited, waiting {ERROR_429_DELAY} seconds")
                    await asyncio.sleep(ERROR_429_DELAY)
                else:
                    logger.error(f"[{self.chain_name}] Error getting pool TVL: {str(e)}")
                    break
        return None
    
    async def _check_token_fee_tier_and_price(
        self, 
        sell_token_address: str, 
        sell_token_decimals: int,
        buy_token_address: str, 
        buy_token_decimals:int,
        amount_in: int = None,
        cached_fee_tier: int = None,
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
        
        fee_tier = 0
        if not cached_fee_tier: 
            for fee in self.token_fee_tiers:
                pool_address= await self.factory_contract.functions.getPool(
                    sell_token_address,
                    buy_token_address,
                    fee
                ).call()
                if pool_address != ZERO_ADDRESS:
                    fee_tier = fee
                    tvl = await self._get_pool_tvlUSD(pool_address)
                    if not tvl or tvl < MIN_POOL_TVL:
                        fee_tier = 0
                        continue
                    break
        else: 
            fee_tier = cached_fee_tier
            
        if not fee_tier: 
            return None, None

        try:
            #Квотим свап
            amount_out = await self.quoter_contract.functions.quoteExactInputSingle(
                (
                    sell_token_address,
                    buy_token_address,
                    amount_in,
                    fee_tier,
                    0
                )
            ).call()
            #Если успех, то пул существует
            return fee_tier,( amount_out / (10 ** buy_token_decimals) ) / (amount_in / (10 ** sell_token_decimals))
        except Exception as e:
            logger.warning(f"[{self.chain_name}] error getting onchain swap data for {buy_token_address} with {sell_token_address}: {str(e)}")
            return None, None
    
    async def _update_single_token_data_cache(
        self, 
        token_address: str, 
        update_fee_tier: bool = False, 
        update_price: bool = False,
    ) -> tuple:
        """
        Получает цены и fee_tiers(если включено) для токена по отношению к каждому включенному базовому токену
        Записывает полученные данные в кэш (self.token_data)
        Возвращает (token_address, {token: price}, {token: fee_tier}, success:bool)
        """
        try:
            token_address_checksum = self.w3.to_checksum_address(token_address)
            prices = {}
            fee_tiers = {}
            any_success = False
            
            #Проверяем каждый базовый токен
            for base_token in self.usable_tokens:

                base_token_address = DEX_ROUTER_DATA[self.chain_name].get(base_token)  
                base_token_decimals = self.token_decimals[base_token]
                amount_in = int(1 * 10 ** base_token_decimals)

                #получаем фи тир из кэша или определяем через запрос
                if update_fee_tier:
                    cached_fee_tier = None
                else:
                    cached_fee_tier = self.token_data[token_address]['fee_tier'].get(base_token, 0)
                    if cached_fee_tier == 0:#если в кэше нет фи тира, то скипаем токен 
                        continue
                
                #берем фи тир и цену для токена на текущем базовом токене
                fee_tier, price = await self._check_token_fee_tier_and_price(
                    base_token_address,
                    base_token_decimals,
                    token_address_checksum,
                    18,
                    amount_in,
                    cached_fee_tier
                )
                
                if fee_tier is None or price is None:#нет пула
                    prices[base_token] = 0
                    fee_tiers[base_token] = 0
                    continue
                
                prices[base_token] = price
                fee_tiers[base_token] = fee_tier
                any_success = True
                

            #update_cache
            if any_success:
                if update_price:
                    self.token_data[token_address_checksum]['price'] = prices
                if update_fee_tier:
                    self.token_data[token_address_checksum]['fee_tier'] = fee_tiers
                return (token_address_checksum, prices, fee_tiers, any_success)
            else: 
                #logger.warning(f"[{self.chain_name}] No available data for token {token_address}")
                return (None, None, None, False)
            
        except Exception as e:
            logger.error(f"[{self.chain_name}] Failed to update price for {token_address}: {str(e)}")
            # Return empty dicts for all usable tokens
            empty_prices = {token: 0 for token in self.usable_tokens}
            empty_fee_tiers = {token: 0 for token in self.usable_tokens}
            return (None, empty_prices, empty_fee_tiers, False)
    
    async def _price_and_fee_tier_updater_loop(self, update_price: bool = True):
        """
        Цикл обновления цен на все токены и записи их в self.token_data
        1. Делит токены на группы размера PRICE_UPDATE_BATCH_SIZE
        2. Для каждого токена в группе создает параллельную задачу на обновление цены
        3. Итерирует группы последовательно
        4. Обновляет цены в self.token_data
        """
        
        update_fee_tiers = True if not self.fee_tier_cached else False
        first_run = True
        if update_price: 
            logger.info(f"[{self.chain_name}] Starting price updater loop")
        if update_fee_tiers:
            logger.info(f"[{self.chain_name}] Updating fee-tiers")

        while True:
            try:
                
                #триггерим апдейт фи тиров если пришло время
                if self._last_fee_tier_update_time and self._last_fee_tier_update_time < datetime.now() - timedelta(days=PARSED_DATA_CHECK_DELAY_DAYS):
                    update_fee_tiers = True
                    logger.debug(f"[{self.chain_name}] refreshing fee tiers")

                #ждем пока не придет время апдейта если у нас режим только под фи тиры
                if not update_price and not update_fee_tiers:
                    logger.debug(f"[{self.chain_name}] fee tiers data is up to date")
                    time_sleep = self._last_fee_tier_update_time + timedelta(days=PARSED_DATA_CHECK_DELAY_DAYS) - datetime.now()
                    await asyncio.sleep(int(time_sleep.total_seconds()))
                    continue

                if not self.token_pool:
                    logger.warning(f"[{self.chain_name}] Token pool is empty, skipping price update")
                    return 0
                
                start_time = time.perf_counter()
                total_successful = 0
                total_failed = 0
                
                #итерируем токены пачками
                for i in range(0, len(self.token_pool), CACHE_UPDATE_BATCH_SIZE):
                    batch = self.token_pool[i:i + CACHE_UPDATE_BATCH_SIZE]
                    batch_start = time.perf_counter()
                    
                    #создаем задачи для этой пачки
                    tasks = [
                        self._update_single_token_data_cache(token_address, update_fee_tiers, update_price)
                        for token_address in batch
                    ]
                    
                    #параллельно выполняем задачи
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    #обрабатываем результаты
                    batch_successful = 0
                    batch_failed = 0
                    for result in results:
                        if isinstance(result, tuple) and len(result) == 4:
                            _, _, _, success = result
                            if success:
                                batch_successful += 1
                            else:
                                batch_failed += 1
                        else:
                            batch_failed += 1
                            logger.warning(f"[{self.chain_name}] Unexpected result format in _update_single_token_data_cache: {type(result)}: {str(result)}")
                    total_successful += batch_successful
                    total_failed += batch_failed   
                    batch_elapsed = (time.perf_counter() - batch_start) * 1000
                    
                    if first_run:#выводим только при первом запуске
                        logger.info(f"[{self.chain_name}] Batch {i//CACHE_UPDATE_BATCH_SIZE + 1}/{len(self.token_pool)//CACHE_UPDATE_BATCH_SIZE + 1}: {batch_successful}/{len(batch)} tokens updated in {batch_elapsed:.2f}ms")
                    
                    #задержка между пачками (отсутствует когда обновляем цены)
                    if i + CACHE_UPDATE_BATCH_SIZE < len(self.token_pool) and not update_price:
                        await asyncio.sleep(DELAY_BETWEEN_BATCHES)
                
                end_time = time.perf_counter()
                elapsed_ms = (end_time - start_time) * 1000
                
                if update_fee_tiers:#если обновляем fee_tiers, то сохраняем их
                    self._save_fee_tier_cache()
                    update_fee_tiers = False
                    self.fee_tier_cached = True

                if first_run:#выводим только при первом запуске
                    logger.info(f"[{self.chain_name}] Initial {update_fee_tiers and 'fee_tier' or 'price'} update completed: {total_successful} successful, {total_failed} no poolData in {elapsed_ms:.2f}ms")
                    first_run = False
                else:
                    logger.debug(f"[{self.chain_name}] {update_fee_tiers and 'fee_tier' or 'price'} update: {total_successful}/{len(self.token_pool)} tokens updated in {elapsed_ms:.2f}ms")
                
            except Exception as e:
                import traceback 
                logger.error(f"[{self.chain_name}] Cache updater loop error: {traceback.format_exc()}")
                await asyncio.sleep(10)

    async def get_token_data_real_time(self, token_address: str) -> dict:

        """
        Возвращает данные для свапа на первом доступном пуле, запрашивая их в реальном времени
        возвращает цену базового токена в токенах sell_token_address. (Для получения цены в weth/usdt нужно 1/Price)
        
        Возвращает:
        (base_token_name, base_token_address, fee_tier, price) или (None, None, None, None)

        """
        tasks:list[
            {
                'base_token_name': None,
                'based_token_address': None,
                'fee_and_price_task': None
            }
        ] = []
        for base_token in self.usable_tokens:
            base_token_address = DEX_ROUTER_DATA[self.chain_name].get(base_token)
            base_token_decimals = self.token_decimals[base_token]
            tasks.append(
                {
                    'base_token_name': base_token,
                    'based_token_address': base_token_address,
                    'fee_and_price_task': self._check_token_fee_tier_and_price(
                        base_token_address, 
                        base_token_decimals, 
                        token_address,
                        18
                    )
                }
            )
        results = await asyncio.gather(*[task['fee_and_price_task'] for task in tasks])
        for result,task_data in zip(results,tasks):
            if result[1]:
                cached_fee, cached_price = result
                return (task_data['base_token_name'], task_data['based_token_address'], cached_fee, cached_price)
        return (None,None,None,None)
    
    def get_token_data_cached(self, token_address: str) -> dict:
        """
        Возвращает кэшированную цену и fee tier для токена
        в формате словаря
        {
            'price': {token: price for token in self.usable_tokens},
            'fee_tier': {token: fee_tier for token in self.usable_tokens}
        }
        
        """
        default = {
            'price': {token: 0 for token in self.usable_tokens},
            'fee_tier': {token: 0 for token in self.usable_tokens}
        }
        return self.token_data.get(token_address, default)
    
    def _get_first_available_pool_swap_data_cached(self, token_address: str) -> tuple:
        """
        Возвращает данные для свапа на первом доступном пуле из кэша
        (base_token_name, base_token_address, fee_tier, price) или (None, None, None, None)
        """
        token_data = self.get_token_data_cached(token_address)
        
        for base_token in self.usable_tokens:
            fee_tier = token_data['fee_tier'].get(base_token, 0)
            if fee_tier > 0:
                base_token_address = DEX_ROUTER_DATA[self.chain_name].get(base_token)
                price = token_data['price'].get(base_token, 0)
                return (base_token, base_token_address, fee_tier, price)
        
        return (None, None, None, None)
    
    def get_token_fee_tier_cache(self, token_address: str, base_token: str = None) -> int:
        """
        Возвращает fee tier для токена из кэша
        Если base_token не указан, возвращает fee tier для первого доступного пула
        """
        if base_token is None:
            _, _, fee_tier, _ = self._get_first_available_pool_swap_data_cached(token_address)
            return fee_tier if fee_tier else 0
        
        return self.token_data.get(token_address, {}).get('fee_tier', {}).get(base_token, 0)
    
    def get_token_price_cache(self, token_address: str, base_token: str = None) -> float:
        """
        Возвращает цену токена из кэша
        Если base_token не указан, возвращает цену для первого доступного пула
        """
        if base_token is None:
            _, _, _, price = self._get_first_available_pool_swap_data_cached(token_address)
            return price if price else 0
        
        return self.token_data.get(token_address, {}).get('price', {}).get(base_token, 0)

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
            logger.info(f"[{self.chain_name}] TX sent: {tx_hash.hex()}")  
            if wait_for_confirmation:
                logger.info(f"[{self.chain_name}] Waiting for tx confirmation")
                receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
                if receipt.status == 1:
                    logger.info(f"[{self.chain_name}] TX confirmed: {tx_hash.hex()}")
                    return tx_hash.hex()
                else:
                    logger.error(f"[{self.chain_name}] TX failed: {tx_hash.hex()}")
                    return None
            else:
                return tx_hash.hex()
              
        except Exception as e:
            logger.error(f"[{self.chain_name}] Transaction error: {str(e)}")
            return None

    def _get_buy_size_and_tp_id(self, mcap: int, sell_token: str) -> int:
        
        for config in MARKET_CAP_CONFIG:
            if mcap >= config['min_cap'] and mcap <= config['max_cap']:
                if config['enabled']:
                    return config['size'][sell_token], config['tp_ladder_id']
                else: 
                    logger.warning(f"[{self.chain_name}] Market cap config {config['min_cap']} - {config['max_cap']} is disabled, skipping")
        return 0, None

    async def execute_swap(self, token_address: str, token_supply: int) -> str:

        token_address = AsyncWeb3.to_checksum_address(token_address)
        logger.info(f"[{self.chain_name}] Starting swap execution for {token_address}")
        base_token_name, base_token_address, cached_fee, cached_price = self._get_first_available_pool_swap_data_cached(token_address)
        """if CACHE_PRICE: # включено кэширование цены
            #Если в кэше нет данных, то считаем в реальном времени
            if not cached_price:
                logger.warning(f"[{self.chain_name}] No available cache data found for token {token_address} - Starting real-time query")
                t_before_query = time.perf_counter()
                base_token_name, base_token_address, cached_fee, cached_price = await self.get_token_data_real_time(token_address)
                t_after_query = time.perf_counter()
                logger.debug(f"[{self.chain_name}] Real-time query took {(t_after_query - t_before_query)*1000:.2f}ms")
            else: #переходим к свапу
                pass

        else: # берем цену в реальном времени"""
        if not cached_fee: #в кэше нет данных о fee-tier
            logger.warning(f"[{self.chain_name}] No available cache data found for token {token_address} - Starting real-time query")
            t_before_query = time.perf_counter()
            base_token_name, base_token_address, cached_fee, cached_price = await self.get_token_data_real_time(token_address)
            t_after_query = time.perf_counter()
            logger.debug(f"[{self.chain_name}] Real-time query took {(t_after_query - t_before_query)*1000:.2f}ms")
        else: #в кэше есть данные о fee-tier
            logger.info(f"[{self.chain_name}] quering token price based on cahced fee-tier data")
            t_before_query = time.perf_counter()
            _, cached_price = await self._check_token_fee_tier_and_price(
                base_token_address, 
                self.token_decimals[base_token_address],
                token_address,
                18,
                cached_fee_tier = cached_fee,
            )
            t_after_query = time.perf_counter()
            logger.debug(f"[{self.chain_name}] Real-time price query took {(t_after_query - t_before_query)*1000:.2f}ms")

        if not base_token_address:
            logger.error(f"[{self.chain_name}] No available data for token {token_address}")
            return None

        if base_token_address == DEX_ROUTER_DATA[self.chain_name]['gas_token']:
            mcap_usd_converter = self.gas_token_price * 1/cached_price
        else:
            mcap_usd_converter = 1/cached_price 

        mcap = int(float(token_supply) * mcap_usd_converter)
        amount_in, tp_ladder_id = self._get_buy_size_and_tp_id(mcap, base_token_name)
        if not amount_in:
            return None
            
        #билдим транзу
        logger.info(f"[{self.chain_name}] Swapping {amount_in} {base_token_name} for {token_address}")
        t_before_build = time.perf_counter()
        amount_out_minimum = int(amount_in * cached_price * (100-SLIPPAGE_PERCENT)/100) * 10**18
        amount_in = int(amount_in * 10**self.token_decimals[base_token_address])
        tx = await self._build_swap_tx(base_token_address, token_address, amount_in, cached_fee, amount_out_minimum)
        t_after_build = time.perf_counter()
        logger.debug(f"[{self.chain_name}] Build swap took {(t_after_build - t_before_build)*1000:.2f}ms")

        #отправляем транзакцию
        t_before_send = time.perf_counter()
        result = await self._sign_and_send(tx)
        if not result:
            return None
        t_after_send = time.perf_counter()
        logger.debug(f"[{self.chain_name}] send swap took {(t_after_send - t_before_send)*1000:.2f}ms")

        #ждем подтверждения транзакции
        t_before_receipt = time.perf_counter()
        receipt = await self.w3.eth.wait_for_transaction_receipt(result)
        t_after_receipt = time.perf_counter()
        logger.debug(f"[{self.chain_name}] Getting tx receipt took {(t_after_receipt - t_before_receipt)*1000:.2f}ms")
        if receipt is None or receipt.get('status',0) == 0:
            logger.error(f"[{self.chain_name}] Swap failed: tx status: {receipt.get('status', 'Tx not found')}")
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
                    actual_price = (amount_in/amount_received) * (10**18 /10**self.token_decimals[base_token_address])
            if actual_price == 0:
                logger.warning(f"[{self.chain_name}] failed to calculate actual price, using cached price")
            else: 
                logger.info(f"[{self.chain_name}] actual price: {actual_price}")

            #запускаем тп таск  
            await asyncio.sleep(DELAY_BEFORE_TP)
            self._take_profit_tasks.append(
                asyncio.create_task(
                    self._create_take_profit_task(
                        token_address,
                        base_token_address,
                        cached_fee,
                        tp_ladder_id,
                        actual_price if actual_price > 0 else 1/cached_price
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
                logger.error(f"[{self.chain_name}] Failed to load TP cache: {str(e)}")
                self._take_profit_cache = {}
        
        return self._take_profit_cache

    async def _update_take_profit_json(self):
        #сохрянаем в json
        try:
            with open(self.tp_cache_path, 'w', encoding='utf-8') as f:
                json.dump(self._take_profit_cache, f, indent=4)
        except Exception as e:
            logger.error(f"[{self.chain_name}] Failed to save TP cache in json: {str(e)}")

    async def _start_take_profit_tasks(self,):
        await self._load_take_profit_cache()
        if len(self._take_profit_cache) > 0:
            logger.info(f"[{self.chain_name}] Starting TP tasks for {len(self._take_profit_cache)} tokens")
        else: 
            logger.info(f"[{self.chain_name}] No TP tasks to start")
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
                        tp_data['steps_done']
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
                    logger.error(f"[{self.chain_name}] | TP task | Failed to approve token for swap")
                    await self.tg_client.send_error_alert(
                        "TP task FAILED", 
                        f"{self.chain_name} Failed to approve token {token_address_to_sell} for swap",
                        "Need to check manually"
                        )
                await asyncio.sleep(5)
        
        #получаем конфигурацию тп сетки
        ladder_config = TP_LADDERS.get(take_profit_ladder_id)
        if not ladder_config or not ladder_config.get('enabled'):
            logger.warning(f"[{self.chain_name}] | TP task | TP ladder {take_profit_ladder_id} is disabled or not found")
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
        stop_loss_price = price_bought * (1 + ladder_config['SP_from_entry_percent'])
        
        #получаем параметры сетки
        first_tp_percent = ladder_config['first_tp_percent']
        total_percent = ladder_config['total_percent']
        steps = ladder_config['steps']
        distribution = ladder_config['distribution']
        
        #получаем баланс токена
        token_contract = self.w3.eth.contract(address=AsyncWeb3.to_checksum_address(token_address_to_sell), abi=erc20_abi)
        try:
            total_balance = await token_contract.functions.balanceOf(self.account.address).call()
        except Exception as e:
            logger.error(f"[{self.chain_name}] | TP task | {token_address_to_sell} | Failed to get token balance: {str(e)}")
            await self.tg_client.send_error_alert(
                "TP task FAILED", 
                f"{self.chain_name} Failed to get token balance for {token_address_to_sell}",
                "Need to check manually"
                )
            return 0
        
        if total_balance == 0:
            logger.warning(f"[{self.chain_name}] | TP task | {token_address_to_sell} Zero balance ")
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

        }
        await self._update_take_profit_json()
        
        logger.info(f"[{self.chain_name}] | TP task | Starting TP task for {token_address_to_sell} | Ladder id: {take_profit_ladder_id} | Balance: {total_balance/10**18:.3f} | SL at {stop_loss_price:.4f}")
        
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
        logger.info(f"[{self.chain_name}] | TP task | TP levels calculated: {steps-steps_done}/{steps} steps left from {tp_levels[0]['target_price']:.6f} to {tp_levels[-1]['target_price']:.6f}")
        
        #мониторинг цены и выполнение продаж
        poll_interval = PRICE_UPDATE_DELAY[self.chain_name]
        tx_failure = 0
        while True:

            #Если количество неуспешных транзакций превысило лимит, завершаем задачу
            if tx_failure > tx_failure_counter:
                logger.error(f"[{self.chain_name}] | TP task | failed for {token_address_to_sell}")
                await self.tg_client.send_error_alert(
                    "TP task FAILED", 
                    f"{self.chain_name} TP task failed for {token_address_to_sell}, can't sell token",
                    "Need to check manually"
                    )
                break

            #Проверяем, все ли уровни выполнены
            if all(level['executed'] for level in tp_levels):
                logger.success(f"[{self.chain_name}] | TP task | All TP levels executed for {token_address_to_sell}")
                #удаляем данные о тп когда он выполнен
                if token_address_to_sell in self._take_profit_cache:
                    del self._take_profit_cache[token_address_to_sell]
                    await self._update_take_profit_json()
                break
    
            try:
                # Получаем текущую цену
                _, current_price = await self._check_token_fee_tier_and_price(
                    token_address_to_sell,
                    18,
                    base_token_address,
                    self.token_decimals[base_token_address],
                    1*10**18,
                    token_pool_fee_tier
                )
                
                if current_price is None:
                    logger.warning(f"[{self.chain_name}] | TP task | Failed to get current price for {token_address_to_sell}")
                    await asyncio.sleep(poll_interval)
                    continue
                current_price = current_price * price_corrector

                #Триггерим стоплосс, передаем единственный тейкпрофит - продажа всего
                if current_price <= stop_loss_price:
                    logger.warning(f"[{self.chain_name}] | TP task | Stop loss triggered for {token_address_to_sell}")
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
                        logger.info(f"[{self.chain_name}] | TP task | {token_address_to_sell} | TP level {level['step']} triggered: Current: {current_price:.6f}, Target: {level['target_price']:.6f}")
                        
                        # Строим и отправляем транзакцию
                        decimals_corrector = 10**self.token_decimals[base_token_address]/10**18
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
                            logger.success(f"[{self.chain_name}] | TP task | {token_address_to_sell} | TP level {level['step']} executed: {level['size_percent']}% sold at {current_price:.6f} | TX: {result}")
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
                            logger.error(f"[{self.chain_name}] | TP task | {token_address_to_sell} | Failed to execute TP level {level['step']}") 
                            await self.tg_client.send_error_alert(
                                "TP task FAILED", 
                                f"{self.chain_name} Failed to execute TP level {level['step']}",
                                f"retries: {tx_failure}/{tx_failure_counter}"
                                ) 
                            break  
                await asyncio.sleep(poll_interval)

            except Exception as e:
                logger.error(f"[{self.chain_name}] | TP task | {token_address_to_sell} | Error in TP monitoring loop: {str(e)}")
                await self.tg_client.send_error_alert(
                    "TP task ERROR", 
                    f"{self.chain_name} Error in TP monitoring loop but still running",
                    str(e)
                    ) 
                tx_failure += 1
                await asyncio.sleep(poll_interval)
    