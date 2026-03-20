import asyncio
from .websocket_client import WebSocketClient
from .executor import TransactionExecutor
from chains import TradeHandler, TakeProfitHandler, DexscreenerClient, OkxDexClient
from config import CHAIN_NAMES, DEFAULT_MARKETCAP_CONFIG, TP_LADDERS, WS_URLS, MIN_POOL_TVL, EVENTS, TOKEN_TO_SELL, LISTING_FILTER
from utils import get_logger
from supply_parser import SupplyParser
from tg_bot import TelegramClient
from web3 import Web3
from typing import Literal
from .dataclasses import TokenTrade

class NewsController:

    def __init__(self, supply_parser: SupplyParser):
        self.logger = get_logger('NEWS')
        self.supply_parser = supply_parser

    async def process_news(self, data: dict) -> list[TokenTrade]:
        trades = []
        ticker = data.get('ticker', '')
        if data.get('direction', '').lower() != 'long':
            self.logger.info(f"signal received on {ticker} but direction is not long")
            return [
                TokenTrade(
                    error=f'direction {data.get("direction")} not supported'
                )
            ]
        if 'dex' not in data.get('exchanges', '') and 'all' not in data.get('exchanges', ''):
            self.logger.info(f"signal received on {ticker} but exchange is not dex")
            return [
                TokenTrade(
                    error=f'exchange {data.get("exchange")} not supported'
                )
            ]

        chain = data.get('chain')
        contract = data.get('contract_address')
        delay_before_sl = int(data.get('sl_time', 0))

        if not (chain and contract): 
            token_data = await self.supply_parser.get_token_data(ticker)
            if not token_data:
                self.logger.error(f"Buy signal received on {ticker} but no token data found")
                return [
                    TokenTrade(
                        error=f'{ticker} - no token data in supply parser'
                    )
                ]
            else: 
                chain = token_data.get('chain')
                contract = token_data.get('token_address')
                self.logger.info(f"found token data for {ticker}: chain={chain}, contract={contract}")

        if data.get('margin', '') != 'auto': 
            custom_size = float(data.get('margin', 0))
            self.logger.info(f"set custom size: {custom_size}$ for {ticker}")
        else:
            return [
                TokenTrade(
                    error=f'margin {data.get("margin")} not supported'
                )
            ]
        if data.get('custom_tp_ladder', {}):
            custom_tp_ladder = data.get('custom_tp_ladder', {})
            self.logger.info(f"set custom tp ladder: {custom_tp_ladder} for {ticker}")
        else:
            return [
                TokenTrade(
                    error=f'tp ladder {data.get("custom_tp_ladder")} not supported'
                )
            ]
        trades.append(TokenTrade(
            ticker=ticker,
            chain=chain,
            token_address=contract,
            custom_size=custom_size,
            custom_tp_ladder=custom_tp_ladder,
            delay_before_sl=delay_before_sl
        ))
        return trades

class ListingsController:

    def __init__(self, supply_parser:SupplyParser):
        self.logger = get_logger('LISTINGS')
        self.supply_parser = supply_parser

    def _filter_out(
        self,
        token_data: dict,
        filters: list
    ) -> bool:
        """Returns True if token passes all filters, False if any filter fails."""

        def _check_listing(listed: list, config_key: str) -> bool:
            cfg = LISTING_FILTER.get(config_key, {})
            must_have = cfg.get('must_have', [])
            must_not_have = cfg.get('must_not_have', [])
            if must_have and not any(e in listed for e in must_have):
                return False
            if must_not_have and any(e in listed for e in must_not_have):
                return False
            return True

        for f in filters:
            match f:
                case 'only_chinese':
                    ticker = token_data.get('ticker', '')
                    if not any('\u4e00' <= ch <= '\u9fff' for ch in ticker):
                        self.logger.warning(f"Buy signal received on {token_data.get('ticker')} on {token_data.get('chain')} but failed chinese filter")
                        return False
                case 'futures_filter':
                    listed = token_data.get('futures_listed', [])
                    if not _check_listing(listed, 'futures'):
                        self.logger.warning(f"Buy signal received on {token_data.get('ticker')} on {token_data.get('chain')} but failed futures filter")
                        return False
                case 'spot_filter':
                    listed = token_data.get('spot_listed', [])
                    if not _check_listing(listed, 'spot'):
                        self.logger.warning(f"Buy signal received on {token_data.get('ticker')} on {token_data.get('chain')} but failed spot filter")
                        return False
                case _:
                    self.logger.warning(f"Unknown filter: {f}")

        return True

    async def process_listings(self, data: dict) -> list[TokenTrade]:

        tickers = []
        contracts = []
        trades_to_execute = []

        exchange = data.get('exchange')
        if not exchange in EVENTS:
            self.logger.debug(f"Buy signal on {exchange} received but exchange not supported")
            trades_to_execute.append(
                TokenTrade(
                    error=f'exchange {exchange} not supported'
                )
            )
            return trades_to_execute
            
        event_type = data.get('type')
        event_settings = EVENTS.get(exchange, {}).get(event_type, {})
        if not event_settings.get('enabled', False):
            self.logger.debug(f"Buy signal on {exchange} - {event_type} received but event type not supported")
            trades_to_execute.append(
                TokenTrade(
                    error=f'event type {event_type} not supported'
                )
            )
            return trades_to_execute

        detections = data.get('detections', [])
        for detection in detections:
            ticker = detection.get('ticker', '')
            if ticker not in tickers:
                tickers.append(ticker)
            contract = detection.get('onchain', {}).get('contract', '')
            chain = detection.get('onchain', {}).get('chain', '')
            contract_data = {
                'contract': contract,
                'chain': chain.upper()
            } if chain.upper() in CHAIN_NAMES else {}
            contracts.append(contract_data)
        
        for ticker, contract_data in zip(tickers, contracts):
            
            token_data = await self.supply_parser.get_token_data(ticker)
            if not (token_data or contract_data):
                self.logger.error(f"Buy signal received on {ticker} but no token data found")
                trades_to_execute.append(
                    TokenTrade(
                        error=f'{ticker} - no token data in supply parser'
                    )
                )
                continue

            if not token_data and contract_data:
                self.logger.warning(f"Buy signal received on {ticker} - not in supply parser, using signal data")
                token_data = {
                    'chain':          contract_data.get('chain', ''),
                    'token_address':  contract_data.get('contract', ''),
                    'mcap':           0,
                    'supply':         0,
                    'futures_listed': [],
                    'spot_listed':    [],
                }

            #===================== match data with api
            api_contract = contract_data.get('contract', False)
            api_chain = contract_data.get('chain', False)
            if api_contract and api_contract.lower() != token_data.get('token_address', '').lower():
                self.logger.warning(f"Buy signal received on {ticker} but token address mismatch api contract, using api data")
                token_data['mcap'] = 0
                token_data['token_address'] = api_contract

            if api_chain and api_chain.lower() != token_data.get('chain', '').lower():
                self.logger.warning(f"Buy signal received on {ticker} but chain mismatch, using api chain")
                token_data['chain'] = api_chain.upper()

            token_data['ticker'] = ticker

            #===================== blacklist      
            if ticker in event_settings.get('blacklist', []): 
                self.logger.warning(f"Buy signal received on {ticker} on {token_data['chain']} but blacklisted")
                trades_to_execute.append(
                    TokenTrade(
                        error=f'token {ticker} is blacklisted'
                    )
                )
                continue
            
            marketcap_config = event_settings.get('marketcap_config', [])
            if not marketcap_config or not token_data.get('mcap', 0):
                self.logger.warning(f"Buy signal received on {ticker} on {token_data['chain']} - mcap_config {bool(marketcap_config)} | mcap {bool(token_data.get('mcap', 0))} | using default config")
                marketcap_config = [event_settings.get('default_mcap_config', DEFAULT_MARKETCAP_CONFIG)]
            
            custom_filters = event_settings.get('custom_filters', [])   
            passed_filter = self._filter_out(token_data, custom_filters)
            if not passed_filter:
                self.logger.warning(f"Buy signal received on {ticker} on {token_data['chain']} but failed custom filters")
                trades_to_execute.append(
                    TokenTrade(
                        error=f'token {ticker} failed custom filters'
                    )
                )
                continue
            
            token_trade = TokenTrade(
                chain=token_data['chain'],
                mcap=token_data['mcap'],
                ticker=ticker,
                token_address=token_data['token_address'],
                circulating_supply=token_data['supply'],
                mcap_config=marketcap_config,
                ticker_to_sell=TOKEN_TO_SELL.get(token_data['chain'], ''),
            )

            self.logger.info(f"Buy signal received: {ticker} on {token_data['chain']} | Contract: {token_data['token_address']}")
            trades_to_execute.append(token_trade)

        return trades_to_execute

class TradingBot:
    
    def __init__(
        self, 
        pk_sol: str,
        pk_evm: str,
    ):
        self.executor = TransactionExecutor()
        self.supply_parser = SupplyParser()
        self.tg_client = TelegramClient()
        self.listing_controller = ListingsController(self.supply_parser)
        self.news_controller = NewsController(self.supply_parser)
        self.logger = get_logger("SIGNAL")
        self.ws_clients: list[WebSocketClient] = []
        self._pk_sol = pk_sol
        self._pk_evm = pk_evm
        
    async def _init_handlers(self):
        """Регистрируем хендлеры по именам из апи вебсокета"""

        for chain_name in CHAIN_NAMES: 
            if chain_name == 'SOLANA': 
                key_kwargs = {
                    'private_key_evm': None,
                    'private_key_sol': self._pk_sol
                }
            else: 
                key_kwargs = {
                    'private_key_evm': self._pk_evm,
                    'private_key_sol': None
                }

            trade_handler = await TradeHandler.create(
                tg_client=self.tg_client,
                chain_name=chain_name,
                **key_kwargs
            )
            tp_handler = await TakeProfitHandler.create(
                tg_client=self.tg_client,
                trade_handler=trade_handler,
                chain_name=chain_name
            )
            trade_handler.set_take_profit_handler(tp_handler)
            self.executor.register_handler(chain_name, trade_handler)
    
    async def on_token_signal(self, data: dict):
        #функция кидает в пул задачи экзекьютора на выполнение свапов параллельно 

        trades = []
        msg_type = data.get('service_type')

        match msg_type:
            
            case "news": 
                trades:list[TokenTrade] = await self.news_controller.process_news(data)
                
            case "listings": 
                trades:list[TokenTrade] = await self.listing_controller.process_listings(data)

            case _:   
                self.logger.error(f"unknown message type: {msg_type}")
                return
            
        #execute trades
        for trade in trades: 
            if trade.error:
                self.logger.error(f"error in message: {trade.error}")
                continue
            else: 
                asyncio.create_task(self.executor.execute_trade(trade))

        #send tg messages for errors         
        for trade in trades:
            if trade.error:
                await self.tg_client.send_error_alert(
                    "BUY FAILED",
                    trade.error
                )
                
        if not trades:
            self.logger.info(f"no trades found in {msg_type} signal")
            return

        
    
    async def start(self):
        
        #запускаем фоновую задачу парсинга 
        await self.supply_parser.start_scheduled_parsing_loop_task()
        
        await self.tg_client.start_status_monitor(CHAIN_NAMES)
        #регаем хендлеры сначала
        await self._init_handlers()
        
        #подключаем вебсокеты
        for name, url in WS_URLS.items():
            client = WebSocketClient(
                name=name,
                uri=url,
                on_message_callback_handler=self.on_token_signal,
                tg_client=self.tg_client
            )
            self.ws_clients.append(client)

        #слушаем все источники параллельно
        await asyncio.gather(*[client.listen() for client in self.ws_clients])

    async def stop(self):
        await self.executor.stop_handlers()
        for client in self.ws_clients:
            await client.close()
        self.ws_clients.clear()
        await self.tg_client.close()
        await self.supply_parser.stop()
        
