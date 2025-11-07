# chains/solana_handler.py

import asyncio
import time
from config import (
    GAS_UPDATE_INTERVAL,
    GAS_MULTPLIER, 
    GAS_LIMIT, 
    AMOUNT_BUY, 
    RPC,
    WS_RPC,
    USE_WEBSOCKET,
    TOKEN,
    SLIPPAGE_PERCENT,
    #SHYFT_API_KEY,
    SOLANA_PRIORITY_FEE,
)
from typing import Literal, Callable
from raydium_lib import RaydiumClient
from solders.message import MessageV0
import base58
from loguru import logger
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

class SolanaHandler:

    def __init__(
        self,
        private_key_base58: str,
        blockhash_update_interval: int = GAS_UPDATE_INTERVAL,
        dex: Literal['RAYDIUM', 'JUPITER'] = 'RAYDIUM'
    ):

        self.sell_token_address = DEX_ROUTER_DATA['SOLANA']['USDT']
        self.token_decimals = DEX_ROUTER_DATA['SOLANA']['token_decimals']
        self.chain_name = 'SOLANA'
        self.client = None
        
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
        self.swap_func: Callable = {
            'RAYDIUM': self.execute_raydium_swap,
            'JUPITER': self.execute_jupiter_swap
        }[dex]
        
        #кэшируем блокхэш
        self._blockhash_cache = None
        self.blockhash_update_interval = blockhash_update_interval

    @classmethod
    async def create(
        cls,
        private_key_base58: str,
        blockhash_update_interval: int = GAS_UPDATE_INTERVAL,
        dex: Literal['RAYDIUM', 'JUPITER'] = 'RAYDIUM'
    ):
        instance = cls(private_key_base58, blockhash_update_interval, dex)
        await instance._initialize_blockhash_cache()
        asyncio.create_task(instance._blockhash_updater_loop())
        return instance

    async def _initialize_blockhash_cache(self):
        blockhash_cache = await self.client.get_latest_blockhash()
        self._blockhash_cache = blockhash_cache.value.blockhash
        return 
        
    async def _blockhash_updater_loop(self):
        while True:
            try:
                blockhash_cache = await self.client.get_latest_blockhash()
                self._blockhash_cache = blockhash_cache.value.blockhash
                await asyncio.sleep(self.blockhash_update_interval)
            except Exception as e:
                logger.error(f"[{self.chain_name}] Blockhash update error: {str(e)}")
                await asyncio.sleep(self.blockhash_update_interval)
    
    async def get_jupiter_quote(
        self,
        input_mint: str,
        output_mint: str,
        amount: int,
        slippage_bps: int = 50
    ) -> dict:
        
        url = "https://lite-api.jup.ag/ultra/v1/order"
        
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
                    logger.error(f"[{self.chain_name}] Jupiter quote error: {data.get('error')}")
                    return None
            except Exception as e:
                logger.error(f"[{self.chain_name}] Jupiter quote error: {str(e)}")
                return None
    
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
        logger.info(f"[{self.chain_name}] Get quote time: {(get_quote_time)*1000:.2f}ms")

        tx = quote_tx.get('transaction')
        tx_bytes = base64.b64decode(tx)
        transaction = VersionedTransaction.from_bytes(tx_bytes)
        
        message_bytes = to_bytes_versioned(transaction.message)
        signature = self.keypair.sign_message(message_bytes)
        
        signed_transaction = VersionedTransaction.populate(
            transaction.message,
            [signature]
        )
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
        logger.info(f"[{self.chain_name}] TX sent: {response.value} | Time: {(end_time - start_time)*1000:.2f}ms")
        return str(response.value)

    #по факту костыль, надо использовать какой-то сервис типа Yellowstone\laserstream
    #потому что джупитер/шифт выдают адерс пула с задержкой ~200ms оба 

    
    async def query_raydium_swap_data_with_jupiter(
        self,
        token_one: str, 
        token_two: str, 
        amount: int,
        
    ) -> dict: 
        url = f'https://public.jupiterapi.com/quote?inputMint={token_one}&outputMint={token_two}&amount={amount*10**self.token_decimals}&dexes=Raydium'
        async with AsyncSession() as session:
            try:
                resp = await session.get(url)
                data = ujson.loads(resp.text)
                route_plan = data.get('routePlan')
                if route_plan and len(route_plan) > 0:
                    return route_plan[0].get('swapInfo').get('ammKey')
                else:
                    logger.error(f"[{self.chain_name}] Jupiter quote error: {data.get('error')}")
                    return None
            except Exception as e:
                logger.error(f"[{self.chain_name}] Jupiter quote error: {str(e)}")
                return None
    
    # async def query_raydium_lp_by_tokens(
    #     self, 
    #     token_one: str, 
    #     token_two: str, 
    #     api_key: str = SHYFT_API_KEY
    # ) -> dict:

    #     endpoint = f"https://programs.shyft.to/v0/graphql/?api_key={api_key}"

    #     query = """
    #     query MyQuery($where: Raydium_LiquidityPoolv4_bool_exp) {
    #         Raydium_LiquidityPoolv4(
    #         where: $where
    #         order_by: {lpReserve: desc}
    #         limit: 1
    #         ) {
    #         pubkey
    #         }
    #     }
    #     """

    #     # Search for specific token pair (checks both directions)
    #     variables = {
    #         "where": {
    #             "_or": [
    #                 {
    #                     "baseMint": {"_eq": token_one},
    #                     "quoteMint": {"_eq": token_two}
    #                 },
    #                 {
    #                     "baseMint": {"_eq": token_two},
    #                     "quoteMint": {"_eq": token_one}
    #                 }
    #             ]
    #         }
    #     }

    #     payload = {
    #         "query": query,
    #         "variables": variables
    #     }

    #     async with AsyncSession() as session:
    #         try:
    #             resp = await session.post(endpoint, json=payload)
    #             data = ujson.loads(resp.text)
    #             if data.get('data').get('Raydium_LiquidityPoolv4'):
    #                 return data.get('data').get('Raydium_LiquidityPoolv4')[0].get('pubkey')
    #             else:
    #                 logger.error(f"[{self.chain_name}] Raydium LP not found")
    #                 return None
    #         except Exception as e:
    #             logger.error(f"[{self.chain_name}] Raydium LP query error: {e}")
    #             return None

    async def execute_raydium_swap(self,token_in_mint: str, token_out_mint: str, amount_in: int):
        t_before_get_pool = time.perf_counter()
        pool = await self.query_raydium_swap_data_with_jupiter(token_in_mint, token_out_mint, amount_in)
        t_after_get_pool = time.perf_counter()
        logger.debug(f"[{self.chain_name}] Getting Raydium LP took {(t_after_get_pool - t_before_get_pool)*1000:.2f}ms")
        
        tx_sig = await self.raydium_client.swap(
            pair_address=pool,
            token_in_mint=str(token_in_mint),
            token_out_mint=str(token_out_mint),
            amount_in=amount_in,
            slippage=float(SLIPPAGE_PERCENT),
            cached_blockhash=self._blockhash_cache
        )
        t_after_send_tx = time.perf_counter()
        logger.debug(f"[{self.chain_name}] Sending swap overall took {(t_after_send_tx - t_before_get_pool)*1000:.2f}ms")
        
        return tx_sig

    async def execute_swap(
        self,
        token_out_mint: str,
        amount_in: int = AMOUNT_BUY,
        
    ):
        return await self.swap_func(self.sell_token_address, token_out_mint, amount_in)