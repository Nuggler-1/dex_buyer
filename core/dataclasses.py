from dataclasses import dataclass

@dataclass
class TokenTrade: 
    chain: str | None = None
    mcap: int | None = None
    ticker: str | None = None
    token_address: str | None = None
    circulating_supply: int | None = None
    mcap_config: list | None = None
    ticker_to_sell: str | None = None
    error: str | None = None
    custom_size: int | None = None
    custom_tp_ladder: list | None = None
    delay_before_tp: int | None = None