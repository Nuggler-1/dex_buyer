from .uniswap import UniswapV4, UniswapV2, UniswapV3

from web3 import AsyncWeb3
from typing import Literal
from config import CHAIN_NAMES, GAS_LIMIT
from ..consts import DEX_ROUTER_DATA, quoter_abi_v3, quoter_abi_v4_cake, router_abi_v4, router_abi_v2, ZERO_ADDRESS
from utils import get_logger
from eth_account import Account
from eth_abi import encode
import time
from abc import ABC, abstractmethod

class CakeswapV2(UniswapV2): 
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
        instance.logger = get_logger(f"CAKE_V2 {chain_name}")
        router_address = DEX_ROUTER_DATA[chain_name]['dex_contracts']['cake_v2']['router_address']
        instance.router_contract = instance.w3.eth.contract(address=router_address, abi=router_abi_v2)
        instance.router_contract_latency = instance.w3_latency.eth.contract(address=router_address, abi=router_abi_v2)
        return instance

class CakeswapV3(UniswapV3): 
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
        instance.logger = get_logger(f"CAKE_V3 {chain_name}")
        quoter_address = DEX_ROUTER_DATA[chain_name]['dex_contracts']['cake_v3']['quoter_address']
        instance.quoter_contract = instance.w3.eth.contract(address=quoter_address, abi=quoter_abi_v3)
        instance.quoter_contract_latency = instance.w3_latency.eth.contract(address=quoter_address, abi=quoter_abi_v3)
        instance.dex_router_address = DEX_ROUTER_DATA[chain_name]['dex_contracts']['cake_v3']['router_address']
        return instance

class CakeswapV4(UniswapV4): 
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
        instance.logger = get_logger(f"CAKE_V4 {chain_name}")
        quoter_address = DEX_ROUTER_DATA[chain_name]['dex_contracts']['cake_v4']['quoter_address']
        router_address = DEX_ROUTER_DATA[chain_name]['dex_contracts']['cake_v4']['router_address']
        instance.quoter_contract = instance.w3.eth.contract(address=quoter_address, abi=quoter_abi_v4_cake)
        instance.quoter_contract_latency = instance.w3_latency.eth.contract(address=quoter_address, abi=quoter_abi_v4_cake)
        instance.router_contract = instance.w3.eth.contract(address=router_address, abi=router_abi_v4)
        return instance

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
            "hook": "0x0000",
            "pool_manager": "0x0000",
            "fee": 1000,
            "parameters": 200
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
            zero_for_one = True if sell_token_address.lower() == pool_data.get('currency0').lower() else False
            quoter = self.quoter_contract_latency if fast else self.quoter_contract
            amount_out = await quoter.functions.quoteExactInputSingle(
                (
                    (
                        pool_data.get("currency0"),
                        pool_data.get("currency1"),
                        pool_data.get("hook"),
                        pool_data.get("pool_manager"),
                        pool_data.get("fee"),
                        bytes.fromhex(pool_data.get("parameters").replace('0x', ''))
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

    def _encode_uniswap_v4_single_swap(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        amount_out_minimum: int,
        pool_data: dict,
        deadline: int
    ) -> tuple:
        """
        PancakeSwap CLMM swap encoding.
        PoolKey format: (currency0, currency1, hooks, poolManager, fee, parameters)
        """
        commands = []
        inputs = []

        token0 = pool_data.get('currency0')
        token1 = pool_data.get('currency1')
        fee = pool_data.get('fee')
        hooks = pool_data.get('hook')
        pool_manager = pool_data.get('pool_manager')
        parameters = pool_data.get('parameters')
        # # Convert hex string back to bytes if needed
        if isinstance(parameters, str):
            parameters = bytes.fromhex(parameters.replace('0x', ''))

        is_native_in = token_in == ZERO_ADDRESS or token_in.lower() == self.gas_token.lower()
        
        if is_native_in:
            zero_for_one = (token0.lower() == ZERO_ADDRESS.lower())
        else:
            zero_for_one = (token_in.lower() == token0.lower())

        # PancakeSwap CLMM PoolKey: (currency0, currency1, hooks, poolManager, fee, parameters)
        pool_key = (
            self.w3.to_checksum_address(token0),
            self.w3.to_checksum_address(token1),
            self.w3.to_checksum_address(hooks),
            self.w3.to_checksum_address(pool_manager),
            fee,
            parameters,  # bytes32
        )

        currency_in = self.w3.to_checksum_address(token0 if zero_for_one else token1)
        currency_out = self.w3.to_checksum_address(token1 if zero_for_one else token0)

        # V4_SWAP needs: actions bytes + params array
        # Actions: SWAP_EXACT_IN_SINGLE (6) + TAKE_ALL (15) + SETTLE_ALL (12)
        actions = bytes([0x06, 0x0f, 0x0c])
        
        params = []
        
        # Param 0: SWAP_EXACT_IN_SINGLE with PancakeSwap PoolKey format
        # PoolKey: (address,address,address,address,uint24,bytes32)
        params.append(encode(
            ["((address,address,address,address,uint24,bytes32),bool,uint128,uint128,bytes)"],
            [(pool_key, zero_for_one, amount_in, amount_out_minimum, b"")]
        ))
        
        # Param 1: TAKE_ALL (currency_out, minAmount)
        params.append(encode(["address", "uint256"], [currency_out, amount_out_minimum]))
        
        # Param 2: SETTLE_ALL (currency_in, maxAmount)
        params.append(encode(["address", "uint256"], [currency_in, amount_in]))

        # V4_SWAP input = (actions, params[])
        v4_swap_input = encode(["bytes", "bytes[]"], [actions, params])
        
        commands.append(0x10)
        inputs.append(v4_swap_input)

        return bytes(commands), inputs, deadline
