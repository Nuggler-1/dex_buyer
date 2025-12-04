import asyncio
from .websocket_client import WebSocketClient
from .executor import TransactionExecutor
from chains import EVMHandler, SolanaHandler
from config import CHAIN_NAMES, PK_EVM, PK_SOL, WS_URL
from loguru import logger
from supply_parser import SupplyParser
from tg_bot import TelegramClient
from web3 import Web3

class TradingBot:
    
    def __init__(
        self, 
        pk_sol: str = PK_SOL,
        pk_evm: str = PK_EVM,
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
            token_pool = list(self.supply_parser.supply_data.get(chain_name).keys())
            handler = await EVMHandler.create(
                tg_client=self.tg_client,
                private_key=self._pk_evm,
                chain_name=chain_name,
                token_pool=token_pool,
            )
            self.executor.register_handler(chain_name, handler)
        
        # Solana
        solana_handler = await SolanaHandler.create(
            tg_client=self.tg_client,
            private_key_base58=self._pk_sol,
            token_pool = list(self.supply_parser.supply_data.get('SOLANA').keys())
        )
        self.executor.register_handler('SOLANA', solana_handler)
    
    async def on_token_signal(self, data: dict):
        #функция кидает в пул задачи экзекьютора на выполнение свапов параллельно 
        data = await self.supply_parser.get_token_data(data.get('ticker'))
        logger.info(f"[WS_CLIENT] Buy signal received: {data.get('token_name')} on {data.get('chain')} | Address: {data.get('token_address')}")
 
        asyncio.create_task(self.executor.execute_trade(data))
    
    async def start(self):
        
        #запускаем фоновую задачу парсинга supply (проверяет каждые 6 часов, обновляет раз в 7 дней)
        self.supply_parser.start_scheduled_parsing_loop_task()
        if self.supply_parser.main_token_data is None:
            await asyncio.sleep(20)
        
        await self.tg_client.start_status_monitor(CHAIN_NAMES)
        #регаем хендлеры сначала
        await self._init_handlers()
        
        #подключаем вебсокет
        self.ws_client = WebSocketClient(
            uri=WS_URL,
            on_message_callback_handler=self.on_token_signal
        )
        #слушаем
        await self.ws_client.listen()

    async def stop(self):
        logger.info("[TRADING_BOT] Stopping bot")
        await self.executor.stop_handlers()
        await self.ws_client.close()
        await self.tg_client.close()
        await self.supply_parser.stop()
        
