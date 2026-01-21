import requests
import time
import re
from typing import List, Dict, Set, Tuple, Callable, Optional
from .supply_parser import SupplyParser
from utils import get_logger
import asyncio
from web3 import Web3, contract


class WhitelistParser:
    
    def __init__(
        self,
        supply_parser: SupplyParser,
        api_key: str,
        platform_identifiers: Dict[str, List[str]],
        rate_limit_delay: float = 1.2
    ):
        self.supply_parser = supply_parser
        self.api_key = api_key
        self.platform_identifiers = platform_identifiers
        self.rate_limit_delay = rate_limit_delay
        self.logger = get_logger("WL_PARSER")
        self.gecko_base_url = "https://pro-api.coingecko.com/api/v3"
        self.cg_headers = {
            "accept": "application/json",
            "x-cg-pro-api-key": self.api_key
        }
    
    def set_filter_function(self, filter_func: Callable[[str], bool]):
        self.filter_func = filter_func
    
    async def parse_gecko_exchange_tokens(
        self,
        exchange_id: str,
        filter_func: Optional[Callable[[str], bool]] = None,
        max_pages: int = 100
    ) -> List[str]:
        base_url = f"{self.gecko_base_url}/exchanges/{exchange_id}/tickers"
        
        coin_ids = []
        page = 1
        
        self.logger.info(f"Starting to parse tickers from exchange: {exchange_id}")
        
        while page <= max_pages:
            try:
                self.logger.info(f"Fetching page {page}...")
                response = requests.get(
                    f"{base_url}?page={page}&dex_pair_format=symbol",
                    headers=self.cg_headers
                )
                response.raise_for_status()
                data = response.json()
                
                if not data or 'tickers' not in data:
                    self.logger.info(f"No data returned for page {page}. Stopping.")
                    break
                
                tickers = data.get('tickers', [])
                
                if not tickers or len(tickers) == 0:
                    self.logger.info(f"No tickers found on page {page}. Reached end.")
                    break
                
                self.logger.info(f"Processing {len(tickers)} tickers from page {page}...")
                
                for ticker in tickers:
                    base = ticker.get('base', '')
                    coin_id = ticker.get('coin_id', '')
                    
                    if filter_func and filter_func(base) and coin_id:
                        coin_ids.append(coin_id)
                        self.logger.info(f"Found matching ticker: {base} (coin_id: {coin_id})")
                    elif not filter_func and coin_id:
                        coin_ids.append(coin_id)
                
                time.sleep(self.rate_limit_delay)
                page += 1
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching page {page}: {e}")
                break
        
        return list(set(coin_ids))
    
    async def get_gecko_coin_contract(
        self,
        coin_id: str,
        target_chains: Optional[List[str]] = None
    ) -> List[Tuple[str, str, str]]:
        if not coin_id or coin_id == 'UNKNOWN':
            return []
        
        url = f"{self.gecko_base_url}/coins/{coin_id}"
        
        try:
            response = requests.get(url, headers=self.cg_headers)
            response.raise_for_status()
            data = response.json()
            
            symbol = data.get('symbol', '').upper()
            platforms = data.get('platforms', {})
            
            results = []
            
            for platform_key, contract_address in platforms.items():
                if not contract_address:
                    continue
                
                for chain_name, identifiers in self.platform_identifiers.items():
                    if target_chains and chain_name not in target_chains:
                        continue
                    
                    if platform_key in identifiers:
                        results.append((symbol, contract_address, chain_name))
                        break
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error fetching contract for {coin_id}: {e}")
            return []
    
    async def resolve_gecko_contracts(
        self,
        coin_ids: List[str],
        target_chains: Optional[List[str]] = None,
        existing_tickers: Optional[Set[str]] = None
    ) -> List[Tuple[str, str, str]]:
        resolved = []
        
        for coin_id in coin_ids:
            self.logger.info(f"Resolving ticker and contract (coin_id: {coin_id})...")
            contracts = await self.get_gecko_coin_contract(coin_id, target_chains)
            
            for symbol, contract, chain in contracts:
                if existing_tickers and symbol.upper() in existing_tickers:
                    self.logger.info(f"Skipping {symbol} - already in whitelist")
                    continue
                    
                resolved.append((symbol, contract, chain))
                self.logger.info(f"Resolved: {symbol} -> {contract} on {chain}")
            
            if not contracts:
                self.logger.warning(f"Could not resolve contract for coin_id: {coin_id}")
            
            time.sleep(self.rate_limit_delay)
        
        return resolved
    
    async def parse_cmc_search_lists(
        self,
        search_lists: Dict[str, Dict[str, any]],
        target_chains: Optional[List[str]] = None
    ) -> List[Tuple[str, str, str]]:
        all_tokens = []
        
        token_list = []
        for search_list_name, search_config in search_lists.items():
            self.logger.info(f"Fetching tokens from CMC search list: {search_list_name}")
            limit = search_config.get('limit', 100)
            params = search_config.get('params', '')
            
            tokens = await self.supply_parser._search_query(1, limit, additional_params=params)
            token_list.extend(tokens)
            self.logger.info(f"Found {len(tokens)} tokens in {search_list_name}")
        
        raw_token_dict = {token['id']: token for token in token_list}
        unique_tokens = list(raw_token_dict.values())
        
        self.logger.info(f"Total unique tokens from CMC: {len(unique_tokens)}")
        
        token_ids = [token.get('id') for token in unique_tokens]
        
        if not token_ids:
            self.logger.warning("No token IDs found")
            return []
        
        self.logger.info(f"Fetching contract data for {len(token_ids)} tokens...")
        cmc_data = await self.supply_parser._get_cmc_tokens_data_by_ids(token_ids)
        
        if not cmc_data:
            self.logger.warning("No contract data returned from CMC")
            return []
        
        used_symbols = []
        for token in unique_tokens:
            token_id = token.get('id')
            symbol = token.get('symbol', '').upper()
            
            token_data = cmc_data.get(str(token_id))
            if not token_data:
                continue
            
            contract_addresses = token_data.get('contract_address', [])
            if not contract_addresses:
                continue
            
            for contract_data in contract_addresses:
                address = contract_data.get('contract_address', '').split('#')[0]
                if not address or address.lower() == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee":
                    self.logger.debug(f"Skipped {symbol} (ID: {token_id}): No valid contract address (got: {address})")
                    continue
                
                platform = contract_data.get('platform')
                if not platform:
                    self.logger.warning(f"Skipped {symbol} (ID: {token_id}): No platform data in contract_data")
                    continue
                
                platform_id = platform.get('coin', {}).get('id')
                if not platform_id:
                    platform_name = platform.get('name', 'Unknown')
                    self.logger.debug(f"Skipped {symbol} (ID: {token_id}): No platform ID (platform: {platform_name})")
                    continue
                
                chain_name = self.platform_identifiers.get(int(platform_id))
                if not chain_name:
                    platform_name = platform.get('name', 'Unknown')
                    self.logger.debug(f"Skipped {symbol} (ID: {token_id}): Unsupported platform ID {platform_id} ({platform_name})")
                    continue
                
                if target_chains and chain_name not in target_chains:
                    self.logger.debug(f"Skipped {symbol} (ID: {token_id}): Chain {chain_name} not in target chains {target_chains}")
                    continue
                
                if symbol in used_symbols:
                    self.logger.debug(f"Skipped {symbol} (ID: {token_id}): Symbol already used")
                    continue
                
                used_symbols.append(symbol)
                all_tokens.append((symbol, address, chain_name))
                self.logger.info(f"Found: {symbol} -> {address} on {chain_name}")
        
        return all_tokens
    
    def load_existing_whitelist(self, whitelist_path: str) -> Set[str]:
        existing_tickers = set()
        
        try:
            with open(whitelist_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    
                    parts = line.split(':')
                    if len(parts) >= 1:
                        ticker = parts[0].strip().upper()
                        existing_tickers.add(ticker)
            
            self.logger.info(f"Loaded {len(existing_tickers)} existing tickers from whitelist")
            
        except FileNotFoundError:
            self.logger.info(f"Whitelist file not found, will create new one")
        except Exception as e:
            self.logger.error(f"Error loading existing whitelist: {e}")
        
        return existing_tickers
    
    def save_to_whitelist(
        self,
        tokens: List[Tuple[str, str, str]],
        whitelist_path: str,
        append: bool = True
    ):
        existing_tickers = self.load_existing_whitelist(whitelist_path) if append else set()
        
        new_tokens = []
        for ticker, address, chain in tokens:
            clean_ticker = ticker.upper().replace('$', '')
            if clean_ticker not in existing_tickers:
                new_tokens.append((clean_ticker, address, chain))
        
        if not new_tokens:
            self.logger.info("No new tokens to add to whitelist")
            return
        
        unique_tokens = list(set(new_tokens))
        
        mode = 'a' if append else 'w'
        with open(whitelist_path, mode, encoding='utf-8') as f:
            if append:
                f.seek(0, 2)
                if f.tell() > 0:
                    f.write("\n")
            else:
                f.write("# Whitelist\n")
                f.write("# Format: ticker:address:chain\n\n")
            
            for ticker, address, chain in sorted(unique_tokens):
                if chain != 'SOLANA':
                    try:
                        address = Web3.to_checksum_address(address)
                    except Exception as e:
                        self.logger.warning(f"Invalid address for {ticker}: {address}")
                        continue
                
                f.write(f"{ticker}:{address}:{chain}\n")
        
        self.logger.success(f"Added {len(unique_tokens)} new tokens to {whitelist_path}")
    
    async def populate_whitelist_from_gecko(
        self,
        exchange_id: str,
        whitelist_path: str,
        filter_func: Optional[Callable[[str], bool]] = None,
        target_chains: Optional[List[str]] = None,
        max_pages: int = 100
    ):
        self.logger.info("=" * 60)
        self.logger.info(f"Starting Gecko whitelist population for {exchange_id}")
        self.logger.info("=" * 60)
        
        existing_tickers = self.load_existing_whitelist(whitelist_path)
        
        coin_ids = await self.parse_gecko_exchange_tokens(
            exchange_id=exchange_id,
            filter_func=filter_func,
            max_pages=max_pages
        )
        
        self.logger.info(f"Found {len(coin_ids)} unique coin IDs")
        
        if not coin_ids:
            self.logger.warning("No coin IDs found")
            return
        
        resolved_tokens = await self.resolve_gecko_contracts(
            coin_ids, 
            target_chains,
            existing_tickers=existing_tickers
        )
        
        self.logger.info(f"Resolved {len(resolved_tokens)} tokens with contracts")
        
        if resolved_tokens:
            self.save_to_whitelist(resolved_tokens, whitelist_path, append=True)
        else:
            self.logger.warning("No tokens with valid contracts found")
    
    async def populate_whitelist_from_cmc(
        self,
        search_lists: Dict[str, Dict[str, any]],
        whitelist_path: str,
        target_chains: Optional[List[str]] = None
    ):
        self.logger.info("=" * 60)
        self.logger.info("Starting CMC whitelist population")
        self.logger.info("=" * 60)
        
        tokens = await self.parse_cmc_search_lists(search_lists, target_chains)
        
        self.logger.info(f"Found {len(tokens)} tokens from CMC")
        
        if tokens:
            self.save_to_whitelist(tokens, whitelist_path, append=True)
        else:
            self.logger.warning("No tokens found from CMC")
