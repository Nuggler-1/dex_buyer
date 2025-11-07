import asyncio
from core.websocket_client import WebSocketClient
from core.executor import TransactionExecutor
from chains.evm_handler import EVMHandler
from chains.solana_handler import SolanaHandler
from config import API_CHAIN_NAMES, RPC, PK_EVM, PK_SOL, WS_URL
from loguru import logger


class TradingBot:
    
    def __init__(self):
        self.executor = TransactionExecutor()
        self.ws_client = None
        
    async def _init_handlers(self):
        """Регистрируем хендлеры по именам из апи вебсокета"""
        
        # EVM
        for chain_name, chain_api_name in API_CHAIN_NAMES.items():
            if chain_name == 'SOLANA':
                continue
            
            handler = await EVMHandler.create(
                private_key=PK_EVM,
                chain_name=chain_name,
            )
            self.executor.register_handler(chain_api_name, handler)
        
        # Solana
        solana_handler = await SolanaHandler.create(
            private_key_base58=PK_SOL,
        )
        self.executor.register_handler(API_CHAIN_NAMES['SOLANA'], solana_handler)
    
    async def on_token_signal(self, data: dict):
        #функция кидает в пул задачи экзекьютора на выполнение свапов параллельно 

        token_address = data.get('token_address')
        chain = data.get('chain')
        token_name = data.get('token_name')
        logger.info(f"[WS_CLIENT] Buy signal received: {token_name} on {chain} | Address: {token_address}")
 
        asyncio.create_task(self.executor.execute_trade(data))
    
    async def start(self):
        #регаем хендлеры сначала
        await self._init_handlers()
        
        #подключаем вебсокет
        self.ws_client = WebSocketClient(
            uri=WS_URL,
            on_message_callback_handler=self.on_token_signal
        )
        #слушаем
        await self.ws_client.listen()
