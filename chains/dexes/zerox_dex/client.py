"""
0x Swap API v2 — price + quote latency test.
Requires: pip install aiohttp
Get API key: https://dashboard.0x.org/create-account
"""
import time
from urllib.parse import urlencode
from curl_cffi.requests import AsyncSession
from utils import get_logger

# ── Config ──────────────────────────────────────────
API_KEY = "YOUR_0X_API_KEY"
BASE_URL = "https://api.0x.org"

# Ethereum: sell 1 USDC → ETH
CHAIN_ID = "1"
SELL_TOKEN = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # USDC
BUY_TOKEN = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"   # WETH
SELL_AMOUNT = "1000000"  # 1 USDC (6 decimals)
TAKER = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"  # vitalik.eth (dummy for price)

HEADERS = {
    "0x-api-key": API_KEY,
    "0x-version": "v2",
}

class ZeroXClient():

    def __init__(self, user_address:str, api_key: str):
        self.user_address = user_address
        self.api_key = api_key
        self.headers = {
            "0x-api-key": self.api_key,
            "0x-version": "v2",
        }
        self.logger = get_logger("0x_client")
            
    async def call_api(self, endpoint: str, params: dict) -> tuple[dict, float]:
        url = f"{BASE_URL}{endpoint}"
        t0 = time.perf_counter()
        async with AsyncSession() as s:
            resp = await s.get(url, params=params, headers=self.headers)
            data = resp.json()
        ms = (time.perf_counter() - t0) * 1000
        return data, ms

    async def quote_swap(self, chain_id: int, sell_token: str, buy_token: str, sell_amount: str, taker: str) -> tuple[dict, float]:
        params = {
            "chainId": chain_id,
            "sellToken": sell_token,
            "buyToken": buy_token,
            "sellAmount": sell_amount,
            "taker": taker,
        }
        data, latency = await self.call_api("/swap/allowance-holder/quote", params)
        self.logger.info(f"Latency for query is: {latency} ms")
        return data

    async def quote_price(self, chain_id: int, sell_token: str, buy_token: str, sell_amount: str, taker: str) -> tuple[dict, float]:
        params = {
            "chainId": chain_id,
            "sellToken": sell_token,
            "buyToken": buy_token,
            "sellAmount": sell_amount,
            "taker": taker,
        }
        data, latency = await self.call_api("/swap/allowance-holder/price", params)
        self.logger.info(f"Latency for query is: {latency} ms")
        return data, latency

