import os
from web3 import Web3
from utils import get_logger


class WhitelistHandler:
    """Manages whitelist files (TICKER:ADDRESS:CHAIN format). Saved for future use."""

    def __init__(self, whitelist_path: str, valid_chains: list[str]):
        self.whitelist_path = whitelist_path
        self.valid_chains = valid_chains
        self.logger = get_logger("WHITELIST")
        self._cache: dict[str, dict] = {}

    def _load_whitelist(self, whitelist_name: str) -> dict:
        """
        Loads a whitelist file. Format per line: TICKER:ADDRESS:CHAIN
        Returns: { ticker_lower: {'address': str, 'chain': str} }
        """
        if whitelist_name in self._cache:
            return self._cache[whitelist_name]

        path = os.path.join(self.whitelist_path, whitelist_name)
        data = {}
        try:
            with open(path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split(':')
                    if len(parts) != 3:
                        self.logger.warning(f"Invalid entry at line {line_num}: {line}")
                        continue
                    ticker, address, chain = parts
                    ticker = ticker.strip().lower()
                    address = address.strip()
                    chain = chain.strip().upper()
                    if chain not in self.valid_chains:
                        self.logger.warning(f"Unknown chain '{chain}' at line {line_num}")
                        continue
                    if chain != 'SOLANA':
                        address = Web3.to_checksum_address(address)
                    data[ticker] = {'address': address, 'chain': chain}
            self._cache[whitelist_name] = data
            self.logger.info(f"Loaded '{whitelist_name}' with {len(data)} entries")
        except FileNotFoundError:
            self.logger.error(f"Whitelist file not found: {path}")
        except Exception as e:
            self.logger.error(f"Error loading '{whitelist_name}': {e}")
        return data

    def is_ticker_in_whitelist(self, ticker: str, whitelist_name: str) -> bool:
        """Returns True if ticker is in whitelist. No whitelist = all allowed."""
        if not whitelist_name:
            return True
        return ticker.lower() in self._load_whitelist(whitelist_name)

    def get_whitelist_token_data(self, ticker: str, whitelist_name: str) -> dict | None:
        """Returns {'address': str, 'chain': str} for ticker, or None."""
        if not whitelist_name:
            return None
        return self._load_whitelist(whitelist_name).get(ticker.lower())
