##C26716D8677712F4D64D024F1D1A53AD
##cb9f20bc-7ca2-4d99-a907-25a06e33d5f0
import time, hmac, hashlib, base64, json
from datetime import datetime, timezone
from urllib.parse import urlencode
from curl_cffi.requests import AsyncSession
from utils import get_logger
import asyncio
from config import OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE, OKX_PROJECT_ID, OKX_RETRY_COUNT, SLIPPAGE_PERCENT, SOLANA_PRIORITY_FEE

BASE_URL = "https://web3.okx.com"

class OkxDexClient:
    def __init__(self, user_address: str):
        self.user_address = user_address
        self.api_key = OKX_API_KEY
        self.api_secret = OKX_API_SECRET
        self.api_passphrase = OKX_API_PASSPHRASE
        self.project_id = OKX_PROJECT_ID
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
            try:
                data = resp.json()
            except Exception:
                self.logger.error(
                    f"OKX non-JSON response [{resp.status_code}] {endpoint}: {resp.text}"
                )
                data = {}
        latency = (time.perf_counter() - t0) * 1000

        return data, latency

    async def quote_all_tokens_for_chain(self, chain_id:int): 
        quote_params = {
            "chainIndex": chain_id,
        }
        data, latency = await self.call_api("/dex/aggregator/all-tokens", quote_params)
        self.logger.info(f"Latency for query is: {latency} ms")
        return data

    async def quote_swap(self,chain_id:int, from_token_address:str, to_token_address:str, amount_in_decimal:int, slippage_in_percent:float = 0.5):
        quote_params = {
            "chainIndex": chain_id,
            "fromTokenAddress": from_token_address,
            "toTokenAddress": to_token_address,
            "amount": int(amount_in_decimal),
            "slippage": slippage_in_percent,
        }
        for attempt in range(1, OKX_RETRY_COUNT + 1):
            data, latency = await self.call_api("/dex/aggregator/quote", quote_params)
            self.logger.info(f"Latency for query is: {latency:.2f} ms")
            if data.get('code', '0') != '0':
                self.logger.error(f"OKX quote error {data.get('code')}: {data.get('msg')} (attempt {attempt}/{OKX_RETRY_COUNT})")
                await asyncio.sleep(0.33)
                continue
            rows = data.get('data') or []
            if not rows:
                self.logger.error(f"OKX quote: empty data list (attempt {attempt}/{OKX_RETRY_COUNT})")
                await asyncio.sleep(0.33)
                continue
            query = rows[0]
            query['chainIndex'] = chain_id
            query['amount'] = query['fromTokenAmount']
            query['fromTokenAddress'] = from_token_address
            query['toTokenAddress'] = to_token_address
            query['slippagePercent'] = str(slippage_in_percent)
            return query
        self.logger.error(f"OKX quote_swap failed after {OKX_RETRY_COUNT} attempts")
        return None

    async def get_swap_data(self,quote_params:dict): 
        swap_params = {
            "chainIndex": quote_params['chainIndex'],
            "fromTokenAddress": quote_params['fromTokenAddress'],
            "toTokenAddress": quote_params['toTokenAddress'],
            "amount": quote_params['amount'],
            "slippagePercent": quote_params.get('slippagePercent', 0.5),
            "userWalletAddress": self.user_address,
            "priceImpactProtectionPercent": '1',
            "disableRFQ": "true",
        }
        if str(swap_params['chainIndex']) == '501':
            swap_params['computeUnitPrice'] = str(SOLANA_PRIORITY_FEE)
        else:
            swap_params['gasLevel'] = "fast"

        for attempt in range(1, OKX_RETRY_COUNT + 1):
            swap_data, latency = await self.call_api("/dex/aggregator/swap", swap_params)
            self.logger.info(f"Latency for swap data is: {latency:.2f} ms")
            if swap_data.get('code', '0') != '0':
                self.logger.error(f"OKX swap error {swap_data.get('code')}: {swap_data.get('msg')} (attempt {attempt}/{OKX_RETRY_COUNT})")
                await asyncio.sleep(0.33)
                continue
            if not (swap_data.get('data') or []):
                self.logger.error(f"OKX swap: empty data list (attempt {attempt}/{OKX_RETRY_COUNT})")
                await asyncio.sleep(0.33)
                continue
            return swap_data
        self.logger.error(f"OKX get_swap_data failed after {OKX_RETRY_COUNT} attempts")
        return None

    async def get_approve_address(self,chain_id:int, token_to_approve:str):
        quote_params = {
            "chainIndex":chain_id, 
            "tokenContractAddress":token_to_approve,
            "approveAmount": 1000000000000
        }
        data, latency = await self.call_api("/dex/aggregator/approve-transaction", quote_params)
        self.logger.info(f"Latency for approve is: {latency:.2f} ms")
        rows = data.get('data') or []
        if not isinstance(rows, list) or not rows:
            return None
        address = rows[0].get('dexContractAddress')
        return address