from config import (
    CMC_PLATFORM_IDS, 
    CMC_API_KEY,
    PARSED_DATA_CHECK_DELAY_DAYS, 
    SUPPLY_DATA_PATH, 
    EXHANGE_SLUG_TO_BOT_SLUG,
    ALL_BASE_TOKEN_TICKERS,
    EXCHANGE_SLUGS, MIN_POOL_TVL,
    CACHE_UPDATE_BATCH_SIZE,
    DELAY_BETWEEN_BATCHES,
    CHAIN_NAMES,
    ALL_BASE_TOKEN_TICKERS,
    TOKEN_DATA_BASE_PATH, 
    FORCE_UPDATE_ON_START,
    ERROR_429_DELAY,
    ERROR_429_RETRIES,
    GECKO_API_KEY,
    GECKO_CHAIN_NAMES,
    RPC
)
from curl_cffi.requests import AsyncSession
from web3 import Web3
from web3 import AsyncWeb3
import json
from utils import get_logger
import asyncio
import os
import time
from chains.consts import DEX_ROUTER_DATA, pool_abi, erc20_abi
from datetime import datetime, timedelta
import ujson
import base58

class HelperSOL: 

    def __init__(self,):
        self.logger = get_logger("PARSER")

    def _is_valid_solana_address(self, address: str) -> bool:
        """Validate if a string is a valid Solana Base58 address"""
        if not address or not isinstance(address, str):
            return False
        try:
            # Solana addresses are 32-44 characters in Base58
            if len(address) < 32 or len(address) > 44:
                return False
            # Try to decode as base58
            decoded = base58.b58decode(address)
            # Solana public keys are 32 bytes
            if len(decoded) != 32:
                return False
            return True
        except Exception:
            return False

    async def _query_raydium_pool(
        self,
        token_one: str,
        token_two: str,
        ):
        url = f'https://api-v3.raydium.io/pools/info/mint?mint1={token_one}&mint2={token_two}&poolType=standard&poolSortField=liquidity&sortType=desc&pageSize=1&page=1'
        async with AsyncSession() as session:
            for i in range(ERROR_429_RETRIES):
                try:
                    resp = await session.get(url)
                    resp.raise_for_status()
                    data = ujson.loads(resp.text).get('data', {})
                    #print(resp.text)
                    
                    if data and data.get('count', 0):
                        pool_data = data.get('data', [{}])[0]
                        pool_address = pool_data.get('id', None)
                        pool_tvl = pool_data.get('tvl', 0)
                        if pool_address and pool_tvl:
                            return pool_address, pool_tvl
                        #self.logger.success(f"[{self.chain_name}] {token_one} - {token_two} Raydium pool found")
                    return None, None

                except Exception as e:
                    if '429' in str(e) or 'rate limit' in str(e):
                        self.logger.warning(f"[{i+1}/{ERROR_429_RETRIES} retries] Rate limit exceeded for {token_one} with {token_two}: {str(e)}")
                        await asyncio.sleep(ERROR_429_DELAY)
                    else:
                        self.logger.warning(f"error getting raydium pool for {token_one} with {token_two}: {str(e)}")
                        return None, None

    async def get_pools_tvl_sorted(self, token_address: str):
        pools = []
        for base_token in ALL_BASE_TOKEN_TICKERS: 
            base_token_address = DEX_ROUTER_DATA['SOLANA'].get(base_token)
            if not base_token_address: 
                continue
            pool_address, pool_tvl = await self._query_raydium_pool(base_token_address, token_address)
            if pool_address and pool_tvl > MIN_POOL_TVL:
                pools.append({
                    'token_address': token_address,
                    'chain': 'SOLANA',
                    'base_token': base_token,
                    'dex_type': 'raydium',
                    'liquidity': pool_tvl,
                    'pair_address': pool_address,
                })

        sorted_pools = sorted(pools, key=lambda x: x['liquidity'], reverse=True)
        return sorted_pools
        

class HelperEVM:

    def __init__(self):
        self.headers = {
            'x-cg-demo-api-key': GECKO_API_KEY
        }
        self.logger = get_logger("PARSER")
        self.w3_providers = {
            chain_name: AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(RPC[chain_name])) for chain_name in CHAIN_NAMES if chain_name != 'SOLANA'
        }

    async def _disconnect_all_providers(self):
        for provider in self.w3_providers.values():
            if not provider.provider.is_connected():
                continue
            await provider.provider.disconnect()

    async def _connect_all_providers(self):
        for provider in self.w3_providers.values():
            if provider.provider.is_connected():
                continue
            await provider.provider.connect()

    async def _get_pools_tvl_sorted(
        self,
        chain_name:str, 
        token_address: str
    ): 

        gecko_chain_name = GECKO_CHAIN_NAMES.get(chain_name)
        base_token_address_to_name = {
            DEX_ROUTER_DATA[chain_name].get(token_name, ''): token_name for token_name in ALL_BASE_TOKEN_TICKERS
        }

        url = f'https://api.coingecko.com/api/v3/onchain/search/pools?query={token_address}&network={gecko_chain_name}&include=base_token'
        for _ in range(ERROR_429_RETRIES):
            try:
                async with AsyncSession() as session:
                    response = await session.get(url, headers=self.headers)
                    data = ujson.loads(response.text)
                    response.raise_for_status()

            except Exception as e:
                if any(['429' in str(e), 'rate limit' in str(e)]):
                    self.logger.warning(f"tvl query Rate limited, waiting {ERROR_429_DELAY} seconds")
                    await asyncio.sleep(ERROR_429_DELAY)
                else:
                    self.logger.error(f"Error getting pool TVL for {token_address} on {chain_name}: {str(e)}: {data}")
                    return []

        parsed_pools = data.get('data',[])
        if not parsed_pools:
            self.logger.warning(f"Pools for {token_address} on {chain_name} not found")

        pools = []
        for pool in parsed_pools:
            
            #check if dex is supported and get its local name
            dex = pool.get('relationships',{}).get('dex',{}).get('data',{}).get('id','')
            if dex not in EXCHANGE_SLUGS: 
                continue
            dex = EXHANGE_SLUG_TO_BOT_SLUG[dex]

            #get pool tvl
            tvl_usd = float(pool.get('attributes',{}).get('reserve_in_usd', 0))
            if tvl_usd < MIN_POOL_TVL:
                continue

            #check if the base token is supported and get its name
            base_token_address = pool.get('relationships', {}).get('base_token',{}).get('data',{}).get('id', '0x')
            base_token_address = base_token_address.split('_')[1]
            quote_token_address = pool.get('relationships', {}).get('quote_token',{}).get('data',{}).get('id', '0x')
            quote_token_address = quote_token_address.split('_')[1]
            base_token_name ='' 
            for address in [base_token_address, quote_token_address]: 
                base_token_name = base_token_address_to_name.get(Web3.to_checksum_address(address))
                if base_token_name: 
                    break 
            if not base_token_name:
                continue

            pool_address = pool.get('attributes',{}).get('address','')
            if pool_address: 
                pool_address = Web3.to_checksum_address(pool_address)

            pools.append(
                {
                    'token_address': token_address,
                    'chain': chain_name,
                    'base_token': base_token_name,
                    'dex_type': dex,
                    'liquidity': tvl_usd,
                    'pair_address': pool_address,
                }
            )
        sorted_pools = sorted(pools, key=lambda x: x['liquidity'], reverse=True)
        return sorted_pools

    async def _get_pool_by_token_address(self, token_address: str, base_token_name:str, chain_name: str):
        pools = await self._get_pools_tvl_sorted(chain_name, token_address)
        for pool in pools:
            if pool['base_token'] == base_token_name:
                return pool
        return None
    
    async def _get_token_decimals(self,token_address:str, chain_name: str):
        try:
            w3= self.w3_providers.get(chain_name)
            token_contract = w3.eth.contract(address=token_address, abi=erc20_abi)
            decimals = await token_contract.functions.decimals().call()
            return decimals
        except Exception as e:
            self.logger.error(f"Error getting token decimals for {token_address} on {chain_name}: {str(e)}")
            return None
    
    async def _get_pool_fee_tier(self,pool_address: str, chain_name: str):
        try:
            w3= self.w3_providers.get(chain_name)
            pool_contract = w3.eth.contract(address=pool_address, abi=pool_abi)
            fee = await pool_contract.functions.fee().call()
            return fee
        except Exception as e:
            self.logger.error(f"Error getting pool fee tier for {pool_address} on {chain_name}: {str(e)}")
            return None

    async def _get_single_pool_data(self, pool_address: str, chain_name: str):

        chain_name = GECKO_CHAIN_NAMES.get(chain_name)
        if not chain_name:
            return {}
        url = f'https://api.coingecko.com/api/v3/onchain/networks/{chain_name}/pools/{pool_address}'
        for _ in range(ERROR_429_RETRIES):
            try:
                async with AsyncSession() as session:
                    response = await session.get(url, headers=self.headers)
                    response.raise_for_status()
                    data = response.json()

                data = data.get('data', {})
                #get pool tvl
                tvl_usd = float(data.get('attributes',{}).get('reserve_in_usd', 0))
                
                #get pool fee tier
                pool_fee = float(data.get('attributes', {}).get('pool_fee_percentage', 0.0))
                pool_fee = int(pool_fee * 10_000)

                return {
                    'chain': chain_name,
                    'liquidity': tvl_usd,
                    'pair_address': pool_address,
                    'fee_tier': pool_fee
                }

            except Exception as e:
                if any(['429' in str(e), 'rate limit' in str(e)]):
                    self.logger.error(f"tvl query Rate limited, waiting {ERROR_429_DELAY} seconds")
                    await asyncio.sleep(ERROR_429_DELAY)
                else:
                    self.logger.error(f"Error getting pool TVL: {str(e)}")
                    break
        return {}

class SupplyParser:

    def __init__(self):
        self.logger = get_logger("PARSER")
        self.supported_platforms_ids = list(CMC_PLATFORM_IDS.keys())
        self.supported_platforms_names = list(CMC_PLATFORM_IDS.values())
        self.main_token_data, self._last_update_time = self._load_token_data()
        self.helper_sol = HelperSOL()
        self.helper_evm = HelperEVM()
        self.chain_separated_pool_dict = {}
        self._parser_task = None
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
    
    async def _search_query(
        self, 
        range_start: int, 
        range_end: int,
        aux: str = 'circulating_supply,total_supply,self_reported_circulating_supply',
        additional_params: str = ''
        
    ):
        """additional_params - дополнительные параметры для запроса в формате key=value&key2=value2"""

        url = f'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?start={range_start}&limit={range_end}&sortBy=rank&sortType=desc&cryptoType=all&tagType=all&audited=false&aux={aux}&{additional_params}'

        async with AsyncSession() as session: 
            response = await session.get(url, headers=self.headers)
            response.raise_for_status() 
            data = response.json().get('data').get('cryptoCurrencyList')
        return data
        
    async def _get_cmc_tokens_data_by_ids(self, token_ids: list):

        token_ids = ','.join(str(token_id) for token_id in token_ids)
        url = f'https://pro-api.coinmarketcap.com/v2/cryptocurrency/info?id={token_ids}&aux=platform'
        headers = {
            'X-CMC_PRO_API_KEY': CMC_API_KEY,
            'Accept': 'application/json'
        }
        async with AsyncSession() as session: 
            response = await session.get(url, headers=headers)
            response.raise_for_status()
            data = response.json().get('data')
        return data

    async def _get_token_id_from_search(self, token_ticker: str):

        url = f'https://api.coinmarketcap.com/gravity/v4/gravity/global-search'
        payload = { 
            "keyword": token_ticker,
            "limit": 5,
            "scene": "community"
        }
        async with AsyncSession() as session: 
            response = await session.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            data = response.json().get('data',{}).get('suggestions',[])
            if not data:
                return None

            tokens = []
            for suggestion in data:
                if suggestion.get('type') == 'token':
                    tokens = suggestion.get('tokens', [])
            if not tokens:
                self.logger.error(f'No tokens found for {token_ticker}')
                self.logger.debug(json.dumps(data, indent=4))
                return None

            tk_id = 0
            for token in tokens:
                if token.get('symbol', '').lower() == token_ticker.lower():
                    tk_id = token.get('id')
                    break
            if not tk_id:
                return None
        return tk_id
        
    async def _get_supply_by_token_id(self, token_id: int):
        async with AsyncSession() as session:
            url = f"https://api.coinmarketcap.com/data-api/v3/cryptocurrency/quote/latest?id={token_id}"
            response = await session.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json().get('data',[])
            if data:
                supply = max(float(data[0].get('circulatingSupply', 0)), float(data[0].get('selfReportedCirculatingSupply')))
                if not supply:
                    self.logger.error(f'No supply found for {token_id}')
                    self.logger.debug(json.dumps(data, indent=4))
                    return None
                return supply
            return None

    async def _get_token_data_by_token_ticker(self, token_ticker: str):
        token_id = await self._get_token_id_from_search(token_ticker)
        if not token_id:
            self.logger.error(f'No token id found for {token_ticker}')
            return None
        supply = await self._get_supply_by_token_id(token_id)
        if not supply:
            self.logger.error(f'No supply found for {token_ticker}')
            return None
        pools = await self._get_pools_tvl_sorted(token_id, trace=True)
        if not pools:
            self.logger.error(f'No pools found for {token_ticker}')
            return None
        return {
            'circulating_supply': supply,
            'pools': pools
        }

    def _load_token_data(self):
        try:
            with open(SUPPLY_DATA_PATH, 'r', encoding='utf-8') as f:
                data = json.loads(f.read())
                if len(data) != 2:
                    return None, None
                else: 
                    return data[1], datetime.fromisoformat(data[0])
        except FileNotFoundError:
            self.logger.warning(f'Token data file not found, returning empty dict')
            return None, None

    def _should_run_parse(self):
        if self.main_token_data is None:
            return True
        
        if FORCE_UPDATE_ON_START:
            self.logger.info(f'FORCE_UPDATE_ON_START is set to True, running parse')
            return True
        
        time_since_last_run = datetime.now() - self._last_update_time
        should_run = time_since_last_run >= timedelta(days=PARSED_DATA_CHECK_DELAY_DAYS)
        
        if should_run:
            self.logger.info(f'Last parsing run was {time_since_last_run.days} days ago, running parse')
        else:
            days_until_next = PARSED_DATA_CHECK_DELAY_DAYS - time_since_last_run.days
            self.logger.info(f'Last parsing run was {time_since_last_run.days} days ago, next run in {days_until_next} days')
        
        return should_run

    async def _get_pools_tvl_sorted(self, token_id: int, trace: bool = False):
        """
        returns list of pools supported sorted by TVL 
        [
            {
                'address': '0x123',
                'chain': 'SOLANA',
                'base_token': 'WSOL',
                'pool_type': 'v3',
                'liquidity': 1000000,
                'pair_address': '0x123'
            }
        ]
        """
        url = f'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/market-pairs/latest?id={token_id}&start=1&limit=100&category=spot&centerType=dex&sort=liquidity_pool_size&direction=desc&spotUntracked=true'
        async with AsyncSession() as session:

            for _ in range(ERROR_429_RETRIES):
                response = await session.get(url, headers=self.headers)
                if response.status_code == 429:
                    self.logger.warning(f'Received 429 error, retrying in {ERROR_429_DELAY} seconds')
                    await asyncio.sleep(ERROR_429_DELAY)
                else: 
                    break

            if response.status_code != 200:
                self.logger.warning(f'Received non-200 status code: {response.status_code}')
                return []

            data = response.json().get('data',{}).get('marketPairs',[])
            if not data:
                if trace:
                    self.logger.warning(f'No pools found for {token_id}')
                    self.logger.debug(json.dumps(response.json(), indent=4))
                return []
            supported_pools = []
            base_tokens_unwrapped = ['SOL', 'ETH', 'BNB']
            base_tokens_unused = ALL_BASE_TOKEN_TICKERS.copy() + base_tokens_unwrapped
            for pool_data in data:

                if pool_data.get('exchangeSlug','').lower() not in EXCHANGE_SLUGS:
                    continue
                if not pool_data.get('tokenAddress', ''):
                    continue

                base_symbol = ''
                if pool_data.get('baseSymbol','').upper() in base_tokens_unused:
                    base_symbol = pool_data.get('baseSymbol','').upper()
                
                if pool_data.get('quoteSymbol','').upper() in base_tokens_unused and not base_symbol:
                    base_symbol = pool_data.get('quoteSymbol','').upper()
                
                if not base_symbol:
                    continue
                
                liquidity = pool_data.get('liquidity')
                if not liquidity:
                    liquidity = 0
                elif liquidity < MIN_POOL_TVL:
                    continue
    
                if pool_data.get('platformName').upper() != 'SOLANA':
                    token_address = Web3.to_checksum_address(pool_data.get('tokenAddress'))
                    pair_address = '' if not pool_data.get('pairContractAddress') else Web3.to_checksum_address(pool_data.get('pairContractAddress'))
                else: 
                    token_address = pool_data.get('tokenAddress')
                    pair_address = pool_data.get('pairContractAddress', '')
                    # Validate Solana address - if invalid, clear it so we fetch from Raydium
                    if pair_address and not self.helper_sol._is_valid_solana_address(pair_address):
                        pair_address = ''

                if 'W' in base_symbol: #found wrapped base token, removing unwrapped as well
                    base_tokens_unused.remove(base_symbol.split('W')[1])

                if base_symbol in base_tokens_unwrapped: #found unwrapped base token, removing wrapped as well
                    base_tokens_unused.remove(base_symbol)
                    base_symbol = 'W' + base_symbol  
                
                base_tokens_unused.remove(base_symbol) #removing token from unused list
                exchange_slug = EXHANGE_SLUG_TO_BOT_SLUG[pool_data.get('exchangeSlug').lower()]
                chain_name = pool_data.get('platformName').upper()

                #quote data from gecko/raydium
                fee = 0
                if chain_name != 'SOLANA' and 'v2' not in exchange_slug:
                    if not pair_address:
                        token_pool = await self.helper_evm._get_pool_by_token_address(token_address, base_symbol, chain_name)
                        if not token_pool:
                            continue
                        pair_address = token_pool.get('pair_address')
                        liquidity = token_pool.get('liquidity')
                    fee = await self.helper_evm._get_pool_fee_tier(pair_address, chain_name)
                    if not fee:
                        continue
                else: 
                    if not pair_address: 
                        base_token_address = DEX_ROUTER_DATA['SOLANA'].get(base_symbol)
                        if not base_token_address: 
                            continue
                        buy_token_address = token_address
                        pair_address, liquidity = await self.helper_sol._query_raydium_pool(base_token_address, buy_token_address)
                        if not(pair_address and liquidity > MIN_POOL_TVL):
                            continue
                        
                decimals = 0 if chain_name == 'SOLANA' else await self.helper_evm._get_token_decimals(token_address, chain_name)
                supported_pools.append(
                    {
                        'token_address': token_address,
                        'token_decimals': decimals,
                        'chain': pool_data.get('platformName').upper(),
                        'base_token': base_symbol,
                        'dex_type': exchange_slug,
                        'liquidity': liquidity,
                        'pair_address': pair_address,
                        'fee_tier': fee,
                    }
                )
            
            if not supported_pools and trace:
                self.logger.warning(f'No supported pools found for {token_id}')
                self.logger.debug(json.dumps(response.json(), indent=4))
                return []

            sorted_pools = sorted(supported_pools, key=lambda x: x['liquidity'], reverse=True)
            return sorted_pools

    async def _update_token_cache_json(self):
        self.logger.info(f'Saving main data to {SUPPLY_DATA_PATH}')

        # Создаем директорию для главного файла, если не существует
        os.makedirs(os.path.dirname(SUPPLY_DATA_PATH), exist_ok=True)

        with open(SUPPLY_DATA_PATH, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
            if isinstance(raw_data, list) and len(raw_data) == 2:
                existing_data = raw_data[1]
            else: 
                existing_data = {}

        merged_data = existing_data.copy()
        for ticker, data in self.main_token_data.items():
            merged_data[ticker] = data
        
        self._last_update_time = datetime.now()
        with open(SUPPLY_DATA_PATH, 'w', encoding='utf-8') as f:
            f.write(json.dumps(
                [self._last_update_time.isoformat(), merged_data], 
                indent=4
            ))
        """
        # Update pool data for each chain
        for chain_name in CHAIN_NAMES:
                
            pool_data_path = f'{TOKEN_DATA_BASE_PATH}/{chain_name}_pool_data.json'
            
            # Создаем директорию, если не существует
            os.makedirs(os.path.dirname(pool_data_path), exist_ok=True)
            
            # Load existing data
            existing_data = {}
            original_update = self._last_update_time.isoformat()
            
            if os.path.exists(pool_data_path):
                with open(pool_data_path, 'r', encoding='utf-8') as f:
                    raw_data = json.load(f)
                    if isinstance(raw_data, list) and len(raw_data) == 2:
                        original_update = raw_data[0]
                        existing_data = raw_data[1]
            else:
                self.logger.info(f'Creating new pool data file for {chain_name}')
            
            # Merge
            new_pool_data = self.chain_separated_pool_dict.get(chain_name, {})
            merged_data = existing_data.copy()
            
            for token_addr, dex_types_data in new_pool_data.items():
                if token_addr not in merged_data:
                    merged_data[token_addr] = dex_types_data
                else:
                    for dex_type, base_tokens_data in dex_types_data.items():
                        if dex_type not in merged_data[token_addr]:
                            merged_data[token_addr][dex_type] = base_tokens_data
                        else:
                            for base_token, new_info in base_tokens_data.items():
                                if base_token not in merged_data[token_addr][dex_type]:
                                    merged_data[token_addr][dex_type][base_token] = new_info
                                else:
                                    if chain_name != 'SOLANA':
                                        # EVM: сравниваем pool_address в dict
                                        old_info = merged_data[token_addr][dex_type][base_token]
                                        new_addr = new_info.get('pool_address', '')
                                        old_addr = old_info.get('pool_address', '') if isinstance(old_info, dict) else ''
                                        
                                        if new_addr != old_addr:
                                            merged_data[token_addr][dex_type][base_token] = new_info
                                    else:
                                        # Solana: сравниваем pool_address как строку
                                        old_pool_addr = merged_data[token_addr][dex_type][base_token]
                                        if new_info != old_pool_addr:
                                            merged_data[token_addr][dex_type][base_token] = new_info 
            # Save
            with open(pool_data_path, 'w', encoding='utf-8') as f:
                json.dump([original_update, merged_data], f, indent=4)
            
            self.logger.info(f'Updated pool data for {chain_name}: {len(merged_data)} tokens')
            """
    async def _parse_tokens(self, ):
        
        #получаем весь набор токенов мекс + топ 2000 кмк (айди и цирк сапплай)
        self.logger.info(f'Fetching tokens list for MEXC')
        mexc_token_list = await self._search_query(1, 2500, additional_params='exchangeIds=544')
        self.logger.info(f'Fetching tokens list for CMC top 2000')
        top_2000_token_list = await self._search_query(1, 2000)
        self.logger.info(f'Fetching tokens list for Uniswap v3 ARB')
        top_200_univ3_arb = await self._search_query(1, 200, additional_params='exchangeIds=1478')
        self.logger.info(f'Fetching tokens list for Uniswap v3 ETH')
        top_400_univ3_eth = await self._search_query(1, 400, additional_params='exchangeIds=1348')
        self.logger.info(f'Fetching tokens list for Uniswap v3 BSC')
        top_400_cakev3_bsc = await self._search_query(1, 400, additional_params='exchangeIds=6706')
        self.logger.info(f'Fetching tokens list for Raydium')
        top_200_raydium = await self._search_query(1, 200, additional_params='exchangeIds=1342')
        raw_token_dict = {token['id']: token for token in mexc_token_list + top_2000_token_list + top_200_univ3_arb + top_400_univ3_eth + top_400_cakev3_bsc + top_200_raydium}
        unique_tokens = list(raw_token_dict.values())
        parsed_token_list = [ 
            {
                'id': token.get('id'),
                'name': token.get('name'),
                'symbol': token.get('symbol'),
                'circulating_supply': max(float(token.get('circulatingSupply', 0)), float(token.get('selfReportedCirculatingSupply', 0)))
            }
            for token in unique_tokens
        ]
        self.logger.info(f'Parsed {len(parsed_token_list)} tokens')

        main_data_dict = {}
        chunk_size = CACHE_UPDATE_BATCH_SIZE
        pool_count = 0
        for i in range(0, len(parsed_token_list), chunk_size):
            self.logger.info(f'Processing chunk {i//chunk_size+1}/{len(parsed_token_list)//chunk_size+1}')
            chunk = parsed_token_list[i:i + chunk_size]

            tasks = []
            for token in chunk:
                token_id = token.get('id')
                tasks.append(self._get_pools_tvl_sorted(token_id))
            results = await asyncio.gather(*tasks)

            for token, result in zip(chunk, results):
                if not result:
                    continue
                main_data_dict[token.get('symbol','').lower().replace(' ', '').replace('.', '').replace('$', '')] = {
                    'circulating_supply': token.get('circulating_supply'),
                    'pools': result
                }
                pool_count += len(result)
                """
                for pool in result:
                    chain_name = pool.get('chain')
                    if chain_name != 'SOLANA':
                        chain_separated_pool_dict[chain_name][pool.get('token_address')] = {
                            pool.get('dex_type'): {
                                pool.get('base_token'): {
                                    'pool_address': pool.get('pair_address'),
                                    'fee_tier': 0
                                }
                            }
                        }
                    else: 
                        chain_separated_pool_dict[chain_name][pool.get('token_address')] = {
                            pool.get('dex_type'): {
                                pool.get('base_token'): pool.get('pair_address')
                            }
                        }
                """

            self.logger.success(f'Processed chunk {i//chunk_size+1}/{len(parsed_token_list)//chunk_size+1}')
            await asyncio.sleep(DELAY_BETWEEN_BATCHES)
        
        # Обновляем данные в памяти
        self.main_token_data = main_data_dict
        self.logger.success(f'Found {pool_count} pools for {len(main_data_dict)} tokens')
        
        # Сохраняем данные в JSON файлы
        await self._update_token_cache_json()
        
        self.logger.success(f'Token data updated and saved successfully')
        
    async def _scheduled_parse_loop(self):
        while True:
            try:
                if self._should_run_parse():
                    self.logger.info(f'Starting scheduled parse')
                    await self._parse_tokens()
                
                await asyncio.sleep(PARSED_DATA_CHECK_DELAY_DAYS * 24 * 60 * 60)
                
            except Exception as e:
                self.logger.error(f'Error in scheduled parse loop: {str(e)}')
                self.logger.warning(f'Waiting 1 hour before retrying')
                await asyncio.sleep(60 * 60)

    async def start_scheduled_parsing_loop_task(self):
        if self._parser_task is None or self._parser_task.done():
            if self._should_run_parse():
                await self._parse_tokens()
            self._parser_task = asyncio.create_task(self._scheduled_parse_loop())
            return True
        else:
            self.logger.warning(f'Scheduled parsing task already running')
            return False

    async def force_parse(self):
        self.logger.info(f'Force parsing requested')
        await self._parse_tokens()

    async def get_token_data(self, token_ticker: str):

        """
        Запросит токен сапплай из базы данных
        Если не найдет, запросит через API
        Если апи не даст ответ - вернет {}

        Возвращает:
            dict: Словарь с данными: {circulating_supply: int, pools: list<dict>}
        """
        
        try: 
            token_ticker = token_ticker.lower().replace(' ', '').replace('.', '').replace('$', '')
            token_data = self.main_token_data.get(token_ticker)
            if token_data:
                return token_data
            else:
                self.logger.warning(f'No parsed token data for {token_ticker}. Quering from API')
                t_start = time.perf_counter()
                token_data = await self._get_token_data_by_token_ticker(token_ticker)
                t_end = time.perf_counter()
                self.logger.debug(f'Query token data took {(t_end - t_start)*1000:.2f}ms')
                return token_data
        except Exception as e:
            import traceback
            self.logger.error(f'Error getting token data for {token_ticker}: {str(e)}')
            self.logger.error(traceback.format_exc())
            return {}