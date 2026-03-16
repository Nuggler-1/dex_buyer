from datetime import datetime, timezone
from urllib.parse import urlencode
from curl_cffi.requests import AsyncSession
from utils import get_logger
import time

class DexscreenerClient():
    def __init__(self):
        self.logger = get_logger("DEXSCREENER")
        self.base_url = "https://api.dexscreener.com"
        
    async def call_api(self,endpoint:str): 
        url = f"{self.base_url}{endpoint}"
        t0 = time.perf_counter()
        async with AsyncSession() as s:
            resp = await s.get(url)
            data = resp.json()
        latency = (time.perf_counter() - t0) * 1000
        return data, latency

    async def quote_price(self, chain_id: str, token_address: str) -> float | None:
        endpoint = f"/tokens/v1/{chain_id}/{token_address}"
        data, latency = await self.call_api(endpoint)
        #self.logger.info(f"Quoted price for {token_address} on chain {chain_id} in {latency:.2f}ms")
        pairs = data if isinstance(data, list) else (data.get('pairs') or [])
        if pairs and pairs[0].get('priceUsd'):
            return float(pairs[0]['priceUsd'])
        return None
