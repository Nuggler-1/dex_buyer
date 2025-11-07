
import asyncio
import time
from web3 import AsyncWeb3, AsyncHTTPProvider, WebSocketProvider
from web3.middleware import ExtraDataToPOAMiddleware
from eth_account import Account
from eth_abi import encode
from loguru import logger
from typing import Callable
from config import (
    GAS_UPDATE_INTERVAL,
    GAS_MULTPLIER, 
    GAS_LIMIT, 
    AMOUNT_BUY, 
    RPC,
    WS_RPC,
    USE_WEBSOCKET,
    TOKEN,
    API_CHAIN_NAMES,
    EVM_QUOTE_MIN_OUT,
    SLIPPAGE_PERCENT,
    GAS_LIMIT
)
from .consts import DEX_ROUTER_DATA, erc20_abi, quoter_abi
from typing import Literal

"""
Fee-tiers (+ три параллельных запроса к апи)
по факту можно еще добавить перебор пулов по fee-тирам (500/3000(2500)/10000), 
если планируется торговать что-то низколиквидное типа ноунейм щитков, потому что ставить 10_000 на все токены не эффективно,
а для щитков может просто не оказаться пула с меньшим fee. 
Напрямую спросить какие есть пулы нельзя, нужен перебор или сторонний сервис (для всех норм токенов всегда есть пул 3000)


"""

class EVMHandler:
    
    def __init__(
        self, 
        private_key: str, 
        chain_name: Literal[*list(API_CHAIN_NAMES.keys())], 
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
        
        self.account = Account.from_key(private_key)

        self.quoter_contract = self.w3.eth.contract(address=DEX_ROUTER_DATA[chain_name]['quoter_address'], abi=quoter_abi)

        #задаем chain-specific data
        self.chain_name = chain_name
        self.chain_id = DEX_ROUTER_DATA[chain_name]['chain_id']
        self.dex_router_address = DEX_ROUTER_DATA[chain_name]['router_address']
        self.sell_token_address = DEX_ROUTER_DATA[chain_name][TOKEN]
        self.token_decimals = DEX_ROUTER_DATA[chain_name]['token_decimals']
        self.weth_address = DEX_ROUTER_DATA[chain_name]['weth_address']
        #для каждого чейна своя функция свапа, коллим сразу _build_swap_tx
        self._build_swap_tx: Callable = { 
            'ETHEREUM': self.build_uniswap_v3_swap_transaction,     
            'BSC': self._build_bsc_swap,          
            'ARBITRUM': self.build_uniswap_v3_swap_transaction,
        }[chain_name]

        self.gas_update_interval = gas_update_interval
        self.gas_multiplier = gas_multiplier
        
        #кэш для  нонса и газа
        self._cached_nonce = None #манагерим нонс руками, чтобы не тратить время на запросы к рпс
        self._gas_price_cache = None
        self._initialized = False
    
    @classmethod
    async def create(
        cls,
        private_key: str,
        chain_name: Literal[*list(API_CHAIN_NAMES.keys())],
        gas_update_interval: int = GAS_UPDATE_INTERVAL,
        gas_multiplier: int = GAS_MULTPLIER,
        use_websocket: bool = USE_WEBSOCKET
    ):
        instance = cls(private_key, chain_name, gas_update_interval, gas_multiplier, use_websocket)
        await instance._initialize()
        return instance
    
    async def _initialize(self):
        if self._initialized:
            return
        
        if self.using_websocket:
            await self.w3.provider.connect()

        await self._initialize_nonce_and_gas()
        await self._send_approve()
        
        #фоновый луп обновления газа
        asyncio.create_task(self._gas_price_updater_loop())
        
        self._initialized = True

    async def _send_approve(self):
        
        contract = self.w3.eth.contract(address=self.sell_token_address, abi=erc20_abi)
        
        allowance = await contract.functions.allowance(
            self.account.address, DEX_ROUTER_DATA[self.chain_name]['spender_address']
            ).call()

        if allowance < 2**128:
            logger.info(f"[{self.chain_name}] Not enough allowance, sending approve tx")
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
            
            signed = self.account.sign_transaction(approve_tx)
            try: 
                tx_hash = await self.w3.eth.send_raw_transaction(signed.raw_transaction)
                self._cached_nonce += 1  #инкрементим нонс после ааппрува
                logger.info(f"[{self.chain_name}] Approve tx sent: {tx_hash.hex()}")
            except Exception as e:
                logger.error(f"[{self.chain_name}] Approve tx failed: {str(e)}")
        else:
            logger.info(f"[{self.chain_name}] Approve not required")
        
    async def _initialize_nonce_and_gas(self):
        self._cached_nonce = await self.w3.eth.get_transaction_count(self.account.address, 'pending')
        self._gas_price_cache = int(await self.w3.eth.gas_price * self.gas_multiplier)
        return 
        
    async def _gas_price_updater_loop(self):
        while True:
            try:
                self._gas_price_cache = int(await self.w3.eth.gas_price * self.gas_multiplier)
                await asyncio.sleep(self.gas_update_interval)
            except Exception as e:
                logger.error(f"[{self.chain_name}] Gas price update error: {str(e)}")
                await asyncio.sleep(self.gas_update_interval)

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
    
    async def build_uniswap_v3_swap_transaction(
        self,
        token_in: str,
        token_out: str,
        amount: int,
        fee: int = 3000, #для щитков наверное лучше ставить 10_000, для BSC 3000 = 2500
        quote_amount_out_min: bool = EVM_QUOTE_MIN_OUT
    ) -> dict:
        
        """
        0x в токен адрес для использования нативки 
        """
        # Для большей скорости на fcfs чейнах и на парах с толстой ликвой лучше отключить
        
        if quote_amount_out_min:
            start_time = time.perf_counter()
            amount_out_minimum = await self.quoter_contract.functions.quoteExactInputSingle(
                (
                    token_in,
                    token_out,
                    amount,
                    fee,
                    0
                )
            ).call()
            amount_out_minimum = int(amount_out_minimum * (100 - SLIPPAGE_PERCENT) / 100)
            end_time = time.perf_counter()
            logger.info(f"[{self.chain_name}] Quote min amount out: {(end_time - start_time)*1000:.2f}ms")
        else:
            amount_out_minimum = 0  
        
        deadline = int(time.time()) + 300
        
        is_eth_in = token_in.lower() == "0x"
        if is_eth_in:
            token_in = self.weth_address  # используем WETH
        
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

    async def _build_bsc_swap(self, sell_token_address: str, buy_token_address:str, amount_in: int):
        tx = await self.build_uniswap_v3_swap_transaction(sell_token_address, buy_token_address, amount_in, fee = 2500)
        return tx
    
    async def _sign_and_send(self, tx: dict) -> str:

        signed = self.account.sign_transaction(tx)
        try: 
            tx_hash = await self.w3.eth.send_raw_transaction(signed.raw_transaction)
            logger.info(f"[{self.chain_name}] TX sent: {tx_hash.hex()}")    
        except Exception as e:
            logger.error(f"[{self.chain_name}] Transaction error: {str(e)}")
            return None

        self._cached_nonce += 1 #локально управляем нонсом, прибавляем только если транза ушла без ошибок
        return tx_hash.hex()

    async def close(self):
        if self.using_websocket:
            try:
                await self.w3.provider.disconnect()
                logger.info(f"[{self.chain_name}] WebSocket connection closed")
            except Exception as e:
                logger.warning(f"[{self.chain_name}] Error closing WebSocket: {e}")
    
    #!!! данная концепция пока только под токен - токен свапы, если делать под нативка - токен, то нужно будет прописать дополнительно value в транзах и WETH 
    async def execute_swap(self, token_address: str, amount_in: int = AMOUNT_BUY) -> str:
        token_address = AsyncWeb3.to_checksum_address(token_address)
        logger.info(f"[{self.chain_name}] Starting swap execution for {token_address}")
        amount_in = int(amount_in * 10 ** self.token_decimals)
        
        t_before_build = time.perf_counter()
        tx = await self._build_swap_tx(self.sell_token_address, token_address, amount_in)
        t_after_build = time.perf_counter()
        logger.debug(f"[{self.chain_name}] Build swap took {(t_after_build - t_before_build)*1000:.2f}ms")
        t_before_send = time.perf_counter()
        result = await self._sign_and_send(tx)
        t_after_send = time.perf_counter()
        logger.debug(f"[{self.chain_name}] RPC call took {(t_after_send - t_before_send)*1000:.2f}ms")
        return result