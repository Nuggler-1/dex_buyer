import random
from typing import Literal
from config import (
    ONLY_PARSED,
    PARSED_DATA_CHECK_DELAY_DAYS,
    MCAP_UPDATE_INTERVAL_HOURS,
    SUPPLY_DATA_PATH,
    CACHE_UPDATE_BATCH_SIZE,
    DELAY_BETWEEN_BATCHES,
    CHAIN_NAMES,
    FORCE_UPDATE_ON_START,
    ERROR_429_DELAY,
    ERROR_429_RETRIES,
    MIN_POOL_TVL,
    SEARCH_ALTERNATE_TO_ETH,
    CMC_SEARCH_LISTS,
    CMC_BLACKLISTS,
    SUPPORTED_CEX_SLUGS,
    PROXIES_PATH,
)
from curl_cffi.requests import AsyncSession
from web3 import Web3
import json
from utils import get_logger
import asyncio
import os
from datetime import datetime, timedelta


class SupplyParser:

    def __init__(self):
        self.logger = get_logger("PARSER")
        self.main_token_data, self._last_update_time = self._load_token_data()
        self._parser_task = None
        self._mcap_task = None
        self._proxies = self._load_proxies()
        if self._proxies:
            self.logger.info(f'Loaded {len(self._proxies)} proxies from {PROXIES_PATH}')
        else:
            self.logger.info('No proxies loaded — using direct connection')
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'platform': 'web',
            'Accept': 'application/json, text/plain, */*',
            'Accept-encoding': 'gzip, deflate, br',
            'Accept-language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Referer': 'https://coinmarketcap.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
        }

    async def stop(self):
        if self._parser_task:
            self._parser_task.cancel()
            self._parser_task = None
        if self._mcap_task:
            self._mcap_task.cancel()
            self._mcap_task = None

    # ------------------------------------------------------------------
    # Proxy helpers
    # ------------------------------------------------------------------

    def _load_proxies(self) -> list[str]:
        """
        Load proxies from PROXIES_PATH.  Supported line formats:
          ip:port
          ip:port:user:pass
          http://ip:port
          http://user:pass@ip:port
        Lines starting with '#' and blank lines are ignored.
        """
        proxies = []
        try:
            with open(PROXIES_PATH, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if line.startswith('http'):
                        proxies.append(line)
                    else:
                        parts = line.split(':')
                        if len(parts) == 2:                          # ip:port
                            proxies.append(f'http://{parts[0]}:{parts[1]}')
                        elif len(parts) == 4:                        # ip:port:user:pass
                            ip, port, user, pwd = parts
                            proxies.append(f'http://{user}:{pwd}@{ip}:{port}')
                        else:
                            self.logger.warning(f'Unrecognised proxy format: {line!r}')
        except FileNotFoundError:
            pass
        except Exception as e:
            self.logger.warning(f'Error loading proxies: {e}')
        return proxies

    def _pick_proxy(self) -> dict | None:
        """Return a random proxy dict for curl_cffi, or None for direct."""
        if not self._proxies:
            return None
        url = random.choice(self._proxies)
        return {'http': url, 'https': url}

    def _make_session(self) -> AsyncSession:
        """AsyncSession pre-configured with a random proxy (if any loaded)."""
        proxy = self._pick_proxy()
        return AsyncSession(proxies=proxy) if proxy else AsyncSession()

    # ------------------------------------------------------------------
    # CMC data fetchers
    # ------------------------------------------------------------------

    async def _search_query(
        self,
        range_start: int,
        range_end: int,
        aux: str = 'circulating_supply,total_supply,self_reported_circulating_supply',
        additional_params: str = ''
    ):
        url = f'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?start={range_start}&limit={range_end}&sortBy=rank&sortType=desc&cryptoType=all&tagType=all&audited=false&aux={aux}&{additional_params}'
        async with self._make_session() as session:
            response = await session.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json().get('data').get('cryptoCurrencyList')
        return data

    async def _get_token_id_from_search(self, token_ticker: str):
        url = 'https://api.coinmarketcap.com/gravity/v4/gravity/global-search'
        payload = {"keyword": token_ticker, "limit": 5, "scene": "community"}
        async with self._make_session() as session:
            response = await session.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            data = response.json().get('data', {}).get('suggestions', [])
        if not data:
            return None
        tokens = []
        for suggestion in data:
            if suggestion.get('type') == 'token':
                tokens = suggestion.get('tokens', [])
        for token in tokens:
            if token.get('symbol', '').lower() == token_ticker.lower():
                return token.get('id')
        return None

    async def _get_supply_by_token_id(self, token_id: int):
        async with self._make_session() as session:
            url = f"https://api.coinmarketcap.com/data-api/v3/cryptocurrency/quote/latest?id={token_id}"
            response = await session.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json().get('data', [])
        if data:
            return max(
                float(data[0].get('circulatingSupply', 0)),
                float(data[0].get('selfReportedCirculatingSupply', 0))
            ) or None
        return None

    async def _get_pools_tvl_sorted(self, token_id: int) -> list[dict]:
        """
        Queries CMC DEX market pairs and returns pools on supported chains sorted by
        liquidity desc. Each entry: { token_address, chain, liquidity }
        """
        url = (
            f'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/market-pairs/latest'
            f'?id={token_id}&start=1&limit=100&category=spot&centerType=dex'
            f'&sort=liquidity_pool_size&direction=desc&spotUntracked=true'
        )
        async with self._make_session() as session:
            for _ in range(ERROR_429_RETRIES):
                response = await session.get(url, headers=self.headers)
                if response.status_code == 429:
                    self.logger.warning(f'Rate limited, retrying in {ERROR_429_DELAY}s')
                    await asyncio.sleep(ERROR_429_DELAY)
                else:
                    break
            if response.status_code != 200:
                return []
            data = response.json().get('data', {}).get('marketPairs', [])

        pools = []
        for pair in data:
            chain_name = pair.get('platformName', '').upper()
            if chain_name not in CHAIN_NAMES:
                continue
            raw_address = pair.get('tokenAddress', '')
            if not raw_address:
                continue
            liquidity = pair.get('liquidity') or 0
            if liquidity < MIN_POOL_TVL:
                continue
            if chain_name != 'SOLANA':
                token_address = Web3.to_checksum_address(raw_address.split('#')[0])
            else:
                token_address = raw_address
            pools.append({'token_address': token_address, 'chain': chain_name, 'liquidity': float(liquidity)})

        return sorted(pools, key=lambda x: x['liquidity'], reverse=True)

    async def _get_supported_listings(self, token_id: int, type: Literal['perpetual', 'spot'] = 'perpetual') -> list[str]:
        """Returns CEX slugs (from SUPPORTED_CEX_SLUGS) that list perpetual futures for this token."""
        url = (
            f'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/market-pairs/latest'
            f'?id={token_id}&start=1&limit=100&category={type}&sort=name&direction=desc&spotUntracked=true'
        )
        for attempt in range(ERROR_429_RETRIES):
            try:
                async with self._make_session() as session:
                    response = await session.get(url, headers=self.headers)
                    response.raise_for_status()
                    market_pairs = response.json().get('data', {}).get('marketPairs', [])
                seen = set()
                result = []
                for pair in market_pairs:
                    slug = pair.get('exchangeSlug', '').lower()
                    if slug in SUPPORTED_CEX_SLUGS and slug not in seen:
                        result.append(slug)
                        seen.add(slug)
                return result
            except Exception as e:
                if attempt == ERROR_429_RETRIES - 1:
                    self.logger.error(f"{type} query failed for {token_id}: {e}")
                    return []
                self.logger.warning(f"{type} query retry {attempt + 1} for {token_id}: {e}")
                await asyncio.sleep(ERROR_429_DELAY)
        return []

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def _load_token_data(self):
        try:
            with open(SUPPLY_DATA_PATH, 'r', encoding='utf-8') as f:
                data = json.loads(f.read())
                if len(data) != 2:
                    return None, None
                else:
                    return data[1], datetime.fromisoformat(data[0])
        except FileNotFoundError:
            self.logger.warning('Token data file not found, returning empty dict')
            return None, None

    def _should_run_parse(self):
        if self.main_token_data is None:
            return True

        if FORCE_UPDATE_ON_START:
            self.logger.info('FORCE_UPDATE_ON_START is set to True, running parse')
            return True

        time_since_last_run = datetime.now() - self._last_update_time
        should_run = time_since_last_run >= timedelta(days=PARSED_DATA_CHECK_DELAY_DAYS)

        if should_run:
            self.logger.info(f'Last parsing run was {time_since_last_run.days} days ago, running parse')
        else:
            days_until_next = PARSED_DATA_CHECK_DELAY_DAYS - time_since_last_run.days
            self.logger.info(f'Last parsing run was {time_since_last_run.days} days ago, next run in {days_until_next} days')

        return should_run

    async def _update_token_cache_json(self):
        self.logger.info(f'Saving data to {SUPPLY_DATA_PATH}')
        os.makedirs(os.path.dirname(SUPPLY_DATA_PATH), exist_ok=True)
        try:
            with open(SUPPLY_DATA_PATH, 'r', encoding='utf-8') as f:
                raw = json.load(f)
                existing = raw[1] if isinstance(raw, list) and len(raw) == 2 else {}
        except Exception:
            existing = {}
        merged = {**existing, **self.main_token_data}
        self._last_update_time = datetime.now()
        with open(SUPPLY_DATA_PATH, 'w', encoding='utf-8') as f:
            json.dump([self._last_update_time.isoformat(), merged], f, indent=4, ensure_ascii=False)

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    async def _fetch_token_list(self) -> list:
        """Fetch CMC_SEARCH_LISTS, remove anything in CMC_BLACKLISTS, dedup by id then by highest mcap."""
        token_list = []
        for name, search_list in CMC_SEARCH_LISTS.items():
            self.logger.info(f'Fetching token list for {name}')
            token_list += await self._search_query(
                1, search_list['limit'], additional_params=search_list['params']
            )

        blacklist_ids: set = set()
        for name, search_list in CMC_BLACKLISTS.items():
            self.logger.info(f'Fetching blacklist for {name}')
            bl_tokens = await self._search_query(
                1, search_list['limit'], additional_params=search_list['params']
            )
            for t in bl_tokens:
                blacklist_ids.add(t['id'])

        raw_token_dict = {t['id']: t for t in token_list if t['id'] not in blacklist_ids}
        return self._dedup_by_mcap(list(raw_token_dict.values()))

    @staticmethod
    def _dedup_by_mcap(tokens: list) -> list:
        """From a list of raw CMC token dicts, keep one entry per normalized ticker (highest mcap wins)."""
        best: dict = {}
        for t in tokens:
            key = (t.get('symbol') or '').lower().replace(' ', '').replace('.', '').replace('$', '')
            mcap = float((t.get('quotes',[{}]))[0].get('marketCap', 0))
            if key not in best or mcap > float((best[key].get('quotes',[{}]))[0].get('marketCap', 0)):
                best[key] = t
        return list(best.values())

    async def _parse_tokens(self):
        unique_tokens = await self._fetch_token_list()
        parsed = [
            {
                'id': t.get('id'),
                'symbol': t.get('symbol'),
                'supply': float(t.get('circulatingSupply', 0) or t.get('selfReportedCirculatingSupply', 0)),
                'mcap': float(t.get('quotes',[{}])[0].get('marketCap', 0))
            }
            for t in unique_tokens
        ]
        self.logger.info(f'Fetched {len(parsed)} tokens from CMC')

        main_data_dict = {}
        total_chunks = (len(parsed) - 1) // CACHE_UPDATE_BATCH_SIZE + 1
        for i in range(0, len(parsed), CACHE_UPDATE_BATCH_SIZE):
            chunk = parsed[i:i + CACHE_UPDATE_BATCH_SIZE]
            self.logger.info(f'Processing chunk {i // CACHE_UPDATE_BATCH_SIZE + 1}/{total_chunks}')

            pool_results, futures_results, spot_results = await asyncio.gather(
                asyncio.gather(*[self._get_pools_tvl_sorted(t['id']) for t in chunk]),
                asyncio.gather(*[self._get_supported_listings(t['id'], type='perpetual') for t in chunk]),
                asyncio.gather(*[self._get_supported_listings(t['id'], type='spot') for t in chunk]),
            )

            for token, pools, futures, spot in zip(chunk, pool_results, futures_results, spot_results):
                if not pools or isinstance(pools, Exception):
                    continue
                best = pools[0]
                if SEARCH_ALTERNATE_TO_ETH and best['chain'] == 'ETHEREUM':
                    alt = next(
                        (p for p in pools[1:] if p['chain'] != 'ETHEREUM' and int(p['liquidity']) >= MIN_POOL_TVL),
                        None
                    )
                    if alt:
                        best = alt
                        self.logger.info(f'Found alternate pool for {token["symbol"]}: {alt["chain"]} with liquidity {alt["liquidity"]}')
                key = token['symbol'].lower().replace(' ', '').replace('.', '').replace('$', '')
                main_data_dict[key] = {
                    'supply': token['supply'],
                    'mcap': token['mcap'],
                    'token_address': best['token_address'],
                    'chain': best['chain'],
                    'liquidity': best['liquidity'],
                    'futures_listed': futures if isinstance(futures, list) else [],
                    'spot_listed': spot if isinstance(spot, list) else [],
                }

            self.logger.success(f'Chunk done | {len(main_data_dict)} tokens so far')
            await asyncio.sleep(DELAY_BETWEEN_BATCHES)

        self.main_token_data = main_data_dict
        self.logger.success(f'Parsed {len(main_data_dict)} tokens')
        await self._update_token_cache_json()

    async def _scheduled_parse_loop(self):
        while True:
            try:
                if self._should_run_parse():
                    await self._parse_tokens()
                await asyncio.sleep(PARSED_DATA_CHECK_DELAY_DAYS * 24 * 3600)
            except Exception as e:
                self.logger.error(f'Scheduled parse error: {e}')
                await asyncio.sleep(3600)

    async def start_scheduled_parsing_loop_task(self):
        if self._parser_task is None or self._parser_task.done():
            if self._should_run_parse():
                await self._parse_tokens()
            self._parser_task = asyncio.create_task(self._scheduled_parse_loop())
            return True
        self.logger.warning('Scheduled parsing already running')
        return False

    async def force_parse(self):
        await self._parse_tokens()

    async def _update_mcap_only(self):
        """Light update: fetches CMC listing and patches only the mcap field for cached tokens."""
        if not self.main_token_data:
            self.logger.warning('No cached data to update mcap for')
            return
        unique_tokens = await self._fetch_token_list()
        updated = 0
        for t in unique_tokens:
            key = (t.get('symbol') or '').lower().replace(' ', '').replace('.', '').replace('$', '')
            if key not in self.main_token_data:
                continue
            mcap = float(t.get('quotes', [{}])[0].get('marketCap', 0))
            self.main_token_data[key]['mcap'] = mcap
            updated += 1
        self.logger.success(f'[mcap] Updated mcap for {updated} tokens')
        await self._update_token_cache_json()

    async def _scheduled_mcap_loop(self):
        while True:
            try:
                await asyncio.sleep(MCAP_UPDATE_INTERVAL_HOURS * 3600)
                await self._update_mcap_only()
            except Exception as e:
                self.logger.error(f'Mcap update error: {e}')
                await asyncio.sleep(3600)

    async def start_mcap_update_loop_task(self):
        if self._mcap_task is None or self._mcap_task.done():
            self._mcap_task = asyncio.create_task(self._scheduled_mcap_loop())
            return True
        self.logger.warning('Mcap update loop already running')
        return False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_token_data(self, token_ticker: str) -> dict:
        """
        Returns cached token data or queries CMC on-the-fly.
        Schema: { supply, token_address, chain, liquidity, futures_listed, spot_listed }
        """
        key = token_ticker.lower().replace(' ', '').replace('.', '').replace('$', '')
        if self.main_token_data:
            cached = self.main_token_data.get(key)
            if cached:
                self.logger.info(f'Returning cached data for {token_ticker}')
                return cached
        if ONLY_PARSED:
            self.logger.info(f'ONLY_PARSED: no data for {token_ticker}')
            return {}
        try:
            self.logger.warning(f'No cached data for {token_ticker}, querying CMC')
            token_id = await self._get_token_id_from_search(key)
            if not token_id:
                return {}
            supply, pools, futures, spot = await asyncio.gather(
                self._get_supply_by_token_id(token_id),
                self._get_pools_tvl_sorted(token_id),
                self._get_supported_listings(token_id, type='perpetual'),
                self._get_supported_listings(token_id, type='spot'),
            )
            if not pools:
                return {}
            best = pools[0]
            return {
                'supply': supply or 0,
                'mcap': best['mcap'],
                'token_address': best['token_address'],
                'chain': best['chain'],
                'liquidity': best['liquidity'],
                'futures_listed': futures or [],
                'spot_listed': spot or [],
            }
        except Exception as e:
            self.logger.error(f'get_token_data error for {token_ticker}: {e}')
            return {}
