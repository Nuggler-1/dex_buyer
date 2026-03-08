##C26716D8677712F4D64D024F1D1A53AD
##cb9f20bc-7ca2-4d99-a907-25a06e33d5f0
import time, hmac, hashlib, base64, json
from datetime import datetime, timezone
from urllib.parse import urlencode
from curl_cffi.requests import AsyncSession
from utils import get_logger

BASE_URL = "https://web3.okx.com"

class OkxDexClient:
    def __init__(self, user_address: str, api_key: str, api_secret: str, api_passphrase: str, project_id: str):
        self.user_address = user_address
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.project_id = project_id
        self.base_url = BASE_URL
        self.logger = get_logger("OKX_API")

    def _sign_request(self, timestamp: str, method: str, path: str, body: str = "") -> str:
        msg = f"{timestamp}{method}{path}{body}"
        mac = hmac.new(self.api_secret.encode(), msg.encode(), hashlib.sha256)
        return base64.b64encode(mac.digest()).decode()


    def _headers(self, method: str, path: str, body: str = "") -> dict:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        return {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": self._sign_request(ts, method, path, body),
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": self.api_passphrase,
            "OK-ACCESS-PROJECT": self.project_id,
            "Content-Type": "application/json",
        }


    async def call_api(self, endpoint: str, params: dict) -> tuple[dict, float]:
        """GET request with latency measurement. Returns (data, latency_ms)."""

        qs = urlencode(params)
        path = f"/api/v6{endpoint}?{qs}"
        url = f"{BASE_URL}{path}"
        hdrs = self._headers("GET", path)

        t0 = time.perf_counter()
        async with AsyncSession() as s:
            resp = await s.get(url, headers=hdrs)
            data = resp.json()
        latency = (time.perf_counter() - t0) * 1000

        return data, latency

    async def quote_swap(self,chain_id:int, from_token_address:str, to_token_address:str, amount_in_decimal:int, slippage_in_percent:float = 0.5):
        quote_params = {
            "chainIndex": chain_id,
            "fromTokenAddress": from_token_address,
            "toTokenAddress": to_token_address,
            "amount": int(amount_in_decimal),
            "slippage": slippage_in_percent,
        }
        data, latency = await self.call_api("/dex/aggregator/quote", quote_params)
        self.logger.info(f"Latency for query is: {latency} ms")
        query = data.get('data', [])[0]
        if query:
            query['amount'] = query['fromTokenAmount']
            query['fromTokenAddress'] = from_token_address
            query['toTokenAddress'] = to_token_address
            query['slippagePercent'] = slippage_in_percent
            return query

    async def get_swap_data(self,quote_params:dict): 
        swap_params = {
            **quote_params,
            "userWalletAddress": self.user_address,
            "disableRFQ": "true",      # skip RFQ for lower latency
        }
        swap_data, latency = await self.call_api(
             "/dex/aggregator/swap", swap_params
        )
        self.logger.info(f"Latency for swap data is: {latency} ms")
        return swap_data

    async def get_approve_address(self,chain_id:int, token_to_approve:str):
        quote_params = {
            "chainIndex":chain_id, 
            "tokenContractAddress":token_to_approve,
            "approveAmount": 1000000000000
        }
        data, latency = await self.call_api("/dex/aggregator/approve-transaction", quote_params)
        address = data.get('data', [])[0].get('dexContractAddress')
        self.logger.info(f"Latency for approve is: {latency} ms")
        return address