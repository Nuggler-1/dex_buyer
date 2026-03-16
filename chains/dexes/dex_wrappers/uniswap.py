from web3 import AsyncWeb3
from typing import Literal
from config import CHAIN_NAMES, GAS_LIMIT
from ..consts import DEX_ROUTER_DATA, quoter_abi_v3, quoter_abi_v4, router_abi_v4, router_abi_v2, ZERO_ADDRESS
from utils import get_logger
from eth_account import Account
from eth_abi import encode
import time
from abc import ABC, abstractmethod

class UniswapBase(ABC): 
    def __init__(
        self,
        w3: AsyncWeb3,
        w3_latency: AsyncWeb3,
        account: Account,
        chain_name: Literal[*CHAIN_NAMES]
    ):
        self.w3 = w3
        self.w3_latency = w3_latency
        self.account = account
        self.chain_name = chain_name
        self.gas_token = DEX_ROUTER_DATA[chain_name]['gas_token']

    @classmethod
    @abstractmethod
    async def create(cls):
        pass

    @abstractmethod
    async def build_swap_transaction(self):
        pass

    @abstractmethod
    async def check_token_price(self):
        pass

    

class UniswapV3(UniswapBase):
    def __init__(
        self, 
        w3: AsyncWeb3,
        w3_latency: AsyncWeb3,
        account: Account,
        chain_name: Literal[*CHAIN_NAMES]
    ):
        super().__init__(w3, w3_latency, account, chain_name)
        self.logger = None
        self.quoter_contract = None
        self.quoter_contract_latency = None
        self.dex_router_address = None

    @classmethod
    async def create(
        cls,
        w3: AsyncWeb3,
        w3_latency: AsyncWeb3,
        account: Account,
        chain_name: Literal[*CHAIN_NAMES]
    ): 
        instance = cls(w3, w3_latency, account, chain_name)
        instance.logger = get_logger(f"UNI_V3 {chain_name}")
        quoter_address = DEX_ROUTER_DATA[chain_name]['dex_contracts']['uni_v3']['quoter_address']
        instance.quoter_contract = instance.w3.eth.contract(address=quoter_address, abi=quoter_abi_v3)
        instance.quoter_contract_latency = instance.w3_latency.eth.contract(address=quoter_address, abi=quoter_abi_v3)
        instance.dex_router_address = DEX_ROUTER_DATA[chain_name]['dex_contracts']['uni_v3']['router_address']
        return instance
    
    def _encode_uniswap_v3_single_swap(
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

    async def build_swap_transaction(
        self,
        token_in: str,
        token_out: str,
        amount: int,
        fee: int,
        amount_out_minimum: int,
        gas_price: int, 
        nonce: int,
    ) -> dict:
        
        if token_in in [DEX_ROUTER_DATA[self.chain_name].get('gas_token'), ZERO_ADDRESS]:
            is_eth_in = True
        else:
            is_eth_in = False
        
        deadline = int(time.time()) + 300
        
        data = self._encode_uniswap_v3_single_swap(
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
            'gasPrice': gas_price,  
            'nonce': nonce,
            'chainId': await self.w3.eth.chain_id,
            'data': data
        }
        
        return tx

    async def check_token_price(
        self, 
        sell_token_address: str, 
        sell_token_decimals: int, 
        buy_token_address: str, 
        buy_token_decimals: int, 
        fee_tier: int, 
        gas_token_price: float = None,
        amount_in: int = None,
        fast: bool = False
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
            if sell_token_address in [self.gas_token, ZERO_ADDRESS]:
                if gas_token_price is None:
                    raise ValueError("gas_token_price was not provided")
                amount_in = int((1/gas_token_price) * 10 ** sell_token_decimals)  # 1 usd in WETH/WBNB
            else:
                amount_in = int(1 * 10 ** sell_token_decimals)  # 1 USDT/USDC/TOKEN
        
        if buy_token_address in [self.gas_token, ZERO_ADDRESS]: 
            buy_token_address = self.gas_token

        if sell_token_address in [self.gas_token, ZERO_ADDRESS]:
            sell_token_address = self.gas_token

        try:
            #Квотим свап - use fast connection if requested
            quoter = self.quoter_contract_latency if fast else self.quoter_contract
            amount_out = await quoter.functions.quoteExactInputSingle(
                (
                    sell_token_address,
                    buy_token_address,
                    amount_in,
                    fee_tier,
                    0
                )
            ).call()
            #Если успех, то пул существует
            return ( amount_out / (10 ** buy_token_decimals) ) / (amount_in / (10 ** sell_token_decimals))
        except Exception as e:
            self.logger.warning(f"error getting onchain swap data for {buy_token_address} with {sell_token_address}: {str(e)}")
            return None

class UniswapV2(UniswapBase):
    def __init__(
        self, 
        w3: AsyncWeb3,
        w3_latency: AsyncWeb3,
        account: Account,
        chain_name: Literal[*CHAIN_NAMES]
    ):
        super().__init__(w3, w3_latency, account, chain_name)
        self.logger = None
        self.router_contract = None
        self.router_contract_latency = None

    @classmethod
    async def create(
        cls,
        w3: AsyncWeb3,
        w3_latency: AsyncWeb3,
        account: Account,
        chain_name: Literal[*CHAIN_NAMES]
    ): 
        instance = cls(w3, w3_latency, account, chain_name)
        instance.logger = get_logger(f"UNI_V2 {chain_name}")
        router_address = DEX_ROUTER_DATA[chain_name]['dex_contracts']['uni_v2']['router_address']
        instance.router_contract = instance.w3.eth.contract(address=router_address, abi=router_abi_v2)
        instance.router_contract_latency = instance.w3_latency.eth.contract(address=router_address, abi=router_abi_v2)
        return instance

    async def build_swap_transaction(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        amount_out_minimum: int,
        gas_price: int, 
        nonce: int,
    ) -> dict:
        
        deadline = int(time.time()) + 300
        is_eth_in = token_in in [self.gas_token, ZERO_ADDRESS]
        is_eth_out = token_out in [self.gas_token, ZERO_ADDRESS]
        
        path = [
            self.gas_token if is_eth_in else self.w3.to_checksum_address(token_in),
            self.gas_token if is_eth_out else self.w3.to_checksum_address(token_out)
        ]

        if is_eth_in:
            tx = await self.router_contract.functions.swapExactETHForTokens(
                amount_out_minimum,
                path,
                self.account.address,
                deadline
            ).build_transaction({
                'from': self.account.address,
                'value': amount_in,
                'gas': GAS_LIMIT[self.chain_name],
                'gasPrice': gas_price,
                'nonce': nonce,
            })
        elif is_eth_out:
            tx = await self.router_contract.functions.swapExactTokensForETH(
                amount_in,
                amount_out_minimum,
                path,
                self.account.address,
                deadline
            ).build_transaction({
                'from': self.account.address,
                'value': 0,
                'gas': GAS_LIMIT[self.chain_name],
                'gasPrice': gas_price,
                'nonce': nonce,
            })
        else:
            tx = await self.router_contract.functions.swapExactTokensForTokens(
                amount_in,
                amount_out_minimum,
                path,
                self.account.address,
                deadline
            ).build_transaction({
                'from': self.account.address,
                'value': 0,
                'gas': GAS_LIMIT[self.chain_name],
                'gasPrice': gas_price,
                'nonce': nonce,
            })
        
        return tx

    async def check_token_price(
        self, 
        sell_token_address: str, 
        sell_token_decimals: int, 
        buy_token_address: str, 
        buy_token_decimals: int, 
        gas_token_price: float = None,
        amount_in: int = None,
        fast: bool = False
    ) -> float:
        
        if amount_in is None:
            if sell_token_address in [self.gas_token, ZERO_ADDRESS]:
                if gas_token_price is None:
                    raise ValueError("gas_token_price was not provided")
                amount_in = int((1/gas_token_price) * 10 ** sell_token_decimals)
            else:
                amount_in = int(1 * 10 ** sell_token_decimals)

        if buy_token_address in [self.gas_token, ZERO_ADDRESS]: 
            buy_token_address = self.gas_token

        if sell_token_address in [self.gas_token, ZERO_ADDRESS]:
            sell_token_address = self.gas_token

        try:
            path = [
                self.w3.to_checksum_address(sell_token_address),
                self.w3.to_checksum_address(buy_token_address)
            ]
            router = self.router_contract_latency if fast else self.router_contract
            amounts_out = await router.functions.getAmountsOut(
                amount_in,
                path
            ).call()
            amount_out = amounts_out[-1]
            return (amount_out / (10 ** buy_token_decimals)) / (amount_in / (10 ** sell_token_decimals))
        except Exception as e:
            self.logger.warning(f"error getting onchain swap data for {buy_token_address} with {sell_token_address}: {str(e)}")
            return None


class UniswapV4(UniswapBase):
    def __init__(
        self, 
        w3: AsyncWeb3,
        w3_latency: AsyncWeb3,
        account: Account, 
        chain_name: Literal[*CHAIN_NAMES]
    ):
        super().__init__(w3, w3_latency, account, chain_name)
        self.logger = None
        self.quoter_contract = None
        self.quoter_contract_latency = None
        self.router_contract = None

    @classmethod
    async def create(
        cls,
        w3: AsyncWeb3,
        w3_latency: AsyncWeb3,
        account: Account,
        chain_name: Literal[*CHAIN_NAMES]
    ): 
        instance = cls(w3, w3_latency, account, chain_name)
        instance.logger = get_logger(f"UNI_V4 {chain_name}")
        quoter_address = DEX_ROUTER_DATA[chain_name]['dex_contracts']['uni_v4']['quoter_address']
        router_address = DEX_ROUTER_DATA[chain_name]['dex_contracts']['uni_v4']['router_address']
        instance.quoter_contract = instance.w3.eth.contract(address=quoter_address, abi=quoter_abi_v4)
        instance.quoter_contract_latency = instance.w3_latency.eth.contract(address=quoter_address, abi=quoter_abi_v4)
        instance.router_contract = instance.w3.eth.contract(address=router_address, abi=router_abi_v4)
        return instance

    def _encode_uniswap_v4_single_swap(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        amount_out_minimum: int,
        pool_data: dict,
        deadline: int
    ) -> tuple:

        commands = []
        inputs = []

        token0 = pool_data.get('currency0')
        token1 = pool_data.get('currency1')
        if any([
                token_in not in [token_in, token_out], token_out not in [token_in, token_out]
            ]): 
            raise Exception(f"Mismatch in passed tokens and pooldata: {[token_in, token_out]} != {[pool_data.get('currency0'), pool_data.get('currency1')]}")
            
        fee = pool_data.get('fee')
        tick_spacing = pool_data.get('tick_spacing')
        hooks = pool_data.get('hook')

        is_native_in = token_in == ZERO_ADDRESS or token_in.lower() == self.gas_token.lower()
        #is_native_out = token_out == ZERO_ADDRESS or token_out.lower() == self.gas_token.lower()
        
        if is_native_in:
            zero_for_one = (token0 == ZERO_ADDRESS)
        else:
            zero_for_one = (token_in.lower() == token0.lower())

        # Note: For token → ETH, SETTLE_ALL handles Permit2 transfer internally
        # No need for separate PERMIT2_TRANSFER_FROM command

        pool_key = (
            self.w3.to_checksum_address(token0),
            self.w3.to_checksum_address(token1),
            fee,
            tick_spacing,
            self.w3.to_checksum_address(hooks),
        )

        currency_in = self.w3.to_checksum_address(token0 if zero_for_one else token1)
        currency_out = self.w3.to_checksum_address(token1 if zero_for_one else token0)

        # V4_SWAP needs: actions bytes + params array
        # Actions: SWAP_EXACT_IN_SINGLE (6) + TAKE_ALL (15) + SETTLE_ALL (12)
        actions = bytes([0x06, 0x0f, 0x0c])
        
        params = []
        
        # Param 0: SWAP_EXACT_IN_SINGLE - wrapped in outer tuple
        params.append(encode(
            ["((address,address,uint24,int24,address),bool,uint128,uint128,bytes)"],
            [(pool_key, zero_for_one, amount_in, amount_out_minimum, b"")]
        ))
        
        # Param 1: TAKE_ALL (currency_out, minAmount=0)
        params.append(encode(["address", "uint256"], [currency_out, amount_out_minimum]))
        
        # Param 2: SETTLE_ALL (currency_in, maxAmount)
        params.append(encode(["address", "uint256"], [currency_in, amount_in]))

        # V4_SWAP input = (actions, params[])
        v4_swap_input = encode(["bytes", "bytes[]"], [actions, params])
        
        commands.append(0x10)
        inputs.append(v4_swap_input)

        return bytes(commands), inputs, deadline
        

    async def build_swap_transaction(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        amount_out_minimum: int,
        pool_data:list,
        gas_price: int, 
        nonce: int,
    ):

        value = 0 if token_in not in [self.gas_token, ZERO_ADDRESS] else amount_in 

        deadline = int(time.time()) + 120
        commands, inputs, deadline = self._encode_uniswap_v4_single_swap(token_in, token_out, amount_in, amount_out_minimum, pool_data, deadline)
        tx_dict = await self.router_contract.functions.execute(
            commands,
            inputs, 
            deadline 
        ).build_transaction(
            {
                'from': self.account.address,
                'value': value,
                'gas': GAS_LIMIT[self.chain_name],
                'gasPrice': gas_price,
                'nonce': nonce,
            }
        )

        return tx_dict
        

    async def check_token_price(
        self, 
        sell_token_address: str, 
        sell_token_decimals: int, 
        buy_token_address: str, 
        buy_token_decimals: int, 
        pool_data: dict, 
        gas_token_price: float = None,
        amount_in: int = None,
        fast: bool = False
    ):
        """
        pool_data: dict = {
            "currency0": "0xabc",
            "currency1": "0xabsdf",
            "fee": 1000,
            "tick_spacing": 200,
            "hook": "0x0000000"
        }
        """
        if amount_in is None:
            if sell_token_address in [self.gas_token, ZERO_ADDRESS]:
                if gas_token_price is None:
                    raise ValueError("gas_token_price was not provided")
                amount_in = int((1/gas_token_price) * 10 ** sell_token_decimals)  # 1 usd in WETH/WBNB
            else:
                amount_in = int(1 * 10 ** sell_token_decimals)  # 1 USDT/USDC/TOKEN

        try:
            zero_for_one = True if sell_token_address.lower() == pool_data.get('currency0', '').lower() else False
            quoter = self.quoter_contract_latency if fast else self.quoter_contract
            amount_out = await quoter.functions.quoteExactInputSingle(
                (
                    (
                        pool_data.get("currency0"),
                        pool_data.get("currency1"),
                        pool_data.get("fee"),
                        pool_data.get("tick_spacing"),
                        pool_data.get("hook")
                    ),
                    zero_for_one,
                    amount_in,
                    b""  # hookData - empty bytes
                )
            ).call()
            return ( amount_out[0] / (10 ** buy_token_decimals) ) / (amount_in / (10 ** sell_token_decimals))
        except Exception as e:
            self.logger.warning(f"error getting onchain swap data for {buy_token_address} with {sell_token_address}: {str(e)}")
            return None


