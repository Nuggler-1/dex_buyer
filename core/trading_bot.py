import asyncio
from .websocket_client import WebSocketClient
from .executor import TransactionExecutor
from chains import EVMHandler, SolanaHandler
from config import CHAIN_NAMES, TP_LADDERS, WS_URL, MIN_POOL_TVL, USABLE_TOKENS, EVENTS
from utils import get_logger
from supply_parser import SupplyParser
from tg_bot import TelegramClient
from web3 import Web3
from typing import Literal

class TradingBot:
    
    def __init__(
        self, 
        pk_sol: str,
        pk_evm: str,
    ):
        self.executor = TransactionExecutor()
        self.supply_parser = SupplyParser()
        self.tg_client = TelegramClient()
        self.ws_client = None
        self._pk_sol = pk_sol
        self._pk_evm = pk_evm
        
    async def _init_handlers(self):
        """Регистрируем хендлеры по именам из апи вебсокета"""
        
        # EVM
        for chain_name in CHAIN_NAMES:
            if chain_name == 'SOLANA':
                continue
            handler = await EVMHandler.create(
                tg_client=self.tg_client,
                private_key=self._pk_evm,
                chain_name=chain_name,
            )
            self.executor.register_handler(chain_name, handler)
        
        # Solana
        solana_handler = await SolanaHandler.create(
            tg_client=self.tg_client,
            private_key_base58=self._pk_sol,
        )
        self.executor.register_handler('SOLANA', solana_handler)
    
    async def on_token_signal(self, data: dict):
        #функция кидает в пул задачи экзекьютора на выполнение свапов параллельно 

        logger = get_logger(f"SIGNAL")

        tickers = []
        contracts = []
        custom_size = None
        custom_tp_ladder = None
        msg_type = data.get('service_type')
        if msg_type == "news_ms": 
            ticker = data.get('ticker', '')
            if data.get('direction', '').lower() != 'long':
                logger.info(f"signal received on {ticker} but direction is not long")
                return
            if 'dex' not in data.get('exchange', '') and 'all' not in data.get('exchange', ''):
                logger.info(f"signal received on {ticker} but exchange is not dex")
                return
            if data.get('margin', '') != 'auto': 
                custom_size = float(data.get('margin', 0))
                logger.info(f"set custom size: {custom_size}$ for {ticker}")
            if data.get('custom_tp_ladder', {}):
                custom_tp_ladder = data.get('custom_tp_ladder', {})
                logger.info(f"set custom tp ladder: {custom_tp_ladder} for {ticker}")
            tickers.append(ticker)

        if msg_type == "listing_ms": 
            exchange = data.get('exchange')
            if not exchange in EVENTS:
                logger.debug(f"Buy signal on {exchange} received but exchange not supported")
                return 
                
            event_type = data.get('type')
            event_settings = EVENTS.get(exchange, {}).get(event_type)
            if not event_settings.get('enabled'):
                logger.debug(f"Buy signal on {exchange} - {event_type} received but event type not supported")
                return

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
                
        if not tickers:
            logger.info(f"no ticker found in {msg_type} signal")
            return

        for ticker, contract_data in zip(tickers, contracts):
            
            token_data = await self.supply_parser.get_token_data(
                ticker, 
                token_contract = contract_data.get('contract'),
                chain = contract_data.get('chain')
            )
            if not token_data:
                logger.error(f"Buy signal received on {ticker} but no token data found")
                await self.tg_client.send_error_alert(
                    "BUY_FAILED",
                f"Buy ticker {ticker} failed",
                "token not found"
            )
                continue
            
            circulating_supply = token_data.get('circulating_supply', 0)
            selected_pool = token_data.get('pool_selected')
            if not circulating_supply and not selected_pool:
                logger.error(f"Buy signal received on {ticker} but no circulating supply and no pool found")
                await self.tg_client.send_error_alert(
                    "BUY_FAILED",
                    f"Buy {ticker} failed",
                    "no circulating supply found"
                )
                continue

            if not selected_pool:
                pools = token_data.get('pools', [])
                if not pools:
                    logger.error(f"Buy signal received on {ticker} but no pools found")
                    await self.tg_client.send_error_alert(
                        "BUY_FAILED",
                        f"Buy {ticker} failed",
                        "no pools found"
                    )
                    continue

                for pool in pools:
                    if pool.get('liquidity') < MIN_POOL_TVL:
                        continue
                    if pool.get('base_token') not in USABLE_TOKENS:
                        continue
                    selected_pool = pool
                    break
            
            if not selected_pool:
                logger.error(f"Buy signal received on {ticker} but no usable pools found due to TVL and BASE_TOKEN filters")
                await self.tg_client.send_error_alert(
                    "BUY_FAILED",
                    f"Buy {ticker} failed",
                    "no usable pools found due to filters"
                )
                continue
            
            chain = selected_pool.get('chain', '')
            address = selected_pool.get('token_address', '')
            
            # Check whitelist if specified
            whitelist_name = event_settings.get('whitelist')
            if whitelist_name:
                if not self.supply_parser.is_ticker_in_whitelist(ticker, whitelist_name):
                    logger.warning(f"Buy signal received on {ticker} on {chain} but not in whitelist '{whitelist_name}'")
                    return None
                    
            else: #check if blacklist is specified
                if ticker in event_settings.get('blacklist', []): 
                    logger.warning(f"Buy signal received on {ticker} on {chain} but blacklisted")
                    return None
            
            if not custom_size: 
                base_token = selected_pool.get("base_token")
                custom_size = event_settings.get('size', {}).get(base_token)
            if not custom_tp_ladder: 
                tp_ladder_id = event_settings.get('tp_ladder_id',0)
                custom_tp_ladder = TP_LADDERS.get(tp_ladder_id)
            
            token_data = {
                'chain': chain,
                'ticker': ticker,
                'token_address': address,
                'circulating_supply': circulating_supply,
                'pool_data': selected_pool,
                'custom_size': custom_size,
                'custom_tp_ladder': custom_tp_ladder,
            }

            logger.info(f"Buy signal received: {ticker} on {chain} | Address: {address}")
    
            asyncio.create_task(self.executor.execute_trade(token_data))
    
    async def start(self):
        
        #запускаем фоновую задачу парсинга 
        await self.supply_parser.start_scheduled_parsing_loop_task()
        
        await self.tg_client.start_status_monitor(CHAIN_NAMES)
        #регаем хендлеры сначала
        await self._init_handlers()
        
        #подключаем вебсокеты
        self.ws_client = WebSocketClient(
            name="WS_CLIENT",
            uri=WS_URL,
            on_message_callback_handler=self.on_token_signal,
            tg_client=self.tg_client
        )

        #слушаем
        await asyncio.create_task(self.ws_client.listen())

    async def stop(self):
        await self.executor.stop_handlers()
        await self.ws_client.close()
        await self.tg_client.close()
        await self.supply_parser.stop()
        
