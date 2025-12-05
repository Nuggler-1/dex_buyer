import asyncio
from .websocket_client import WebSocketClient
from .executor import TransactionExecutor
from chains import EVMHandler, SolanaHandler
from config import CHAIN_NAMES, PK_EVM, PK_SOL, NEWS_WS_URL, LISTINGS_WS_URL, MIN_POOL_TVL, USABLE_TOKENS
from utils import get_logger
from supply_parser import SupplyParser
from tg_bot import TelegramClient
from web3 import Web3
from typing import Literal

class TradingBot:
    
    def __init__(
        self, 
        pk_sol: str = PK_SOL,
        pk_evm: str = PK_EVM,
    ):
        self.executor = TransactionExecutor()
        self.supply_parser = SupplyParser()
        self.tg_client = TelegramClient()
        self.ws_clients = []
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
    
    async def on_token_signal(self, msg_type: Literal['NEWS', 'LISTINGS'], data: dict):
        #функция кидает в пул задачи экзекьютора на выполнение свапов параллельно 

        logger = get_logger(f"{msg_type}_WS")
        
        ticker = ''
        custom_size = None
        custom_tp_ladder = None
        if msg_type == "NEWS": 
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

        if msg_type == "LISTINGS": 
            detections = data.get('detections', [])
            if not detections:
                return
            for detection in detections:
                ticker = detection.get('ticker', '')
                if ticker:
                    break
        
        if not ticker:
            logger.info(f"no ticker found in {msg_type} signal")
            return

        token_data = await self.supply_parser.get_token_data(ticker)
        if not token_data:
            logger.error(f"Buy signal received on {ticker} but no token data found")
            await self.tg_client.send_error_alert(
                "BUY_FAILED",
                f"Buy ticker {ticker} failed",
                "token not found"
            )
            return

        circulating_supply = token_data.get('circulating_supply', 0)
        if not circulating_supply:
            logger.error(f"Buy signal received on {ticker} but no circulating supply found")
            await self.tg_client.send_error_alert(
                "BUY_FAILED",
                f"Buy {ticker} failed",
                "no circulating supply found"
            )
            return
        
        pools = token_data.get('pools', [])
        if not pools:
            logger.error(f"Buy signal received on {ticker} but no pools found")
            await self.tg_client.send_error_alert(
                "BUY_FAILED",
                f"Buy {ticker} failed",
                "no pools found"
            )
            return

        selected_pool = None
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
            return
        
        chain = selected_pool.get('chain', '')
        address = selected_pool.get('token_address', '')
        token_data = {
            'chain': chain,
            'ticker': ticker,
            'token_address': address,
            'circulating_supply': circulating_supply,
            'pool_data': selected_pool,
            'custom_size': custom_size,
            'custom_tp_ladder': custom_tp_ladder
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
        ws_news_client = WebSocketClient(
            name="NEWS_WS",
            msg_type="NEWS",
            uri=NEWS_WS_URL,
            on_message_callback_handler=self.on_token_signal
        )
        ws_listings_client = WebSocketClient(
            name="LISTINGS_WS",
            msg_type="LISTINGS",
            uri=LISTINGS_WS_URL,
            on_message_callback_handler=self.on_token_signal
        )
        self.ws_clients = [ws_news_client, ws_listings_client]

        #слушаем
        await asyncio.gather(
            *[client.listen() for client in self.ws_clients]
        )

    async def stop(self):
        await self.executor.stop_handlers()
        for client in self.ws_clients:
            await client.close()
        await self.tg_client.close()
        await self.supply_parser.stop()
        
