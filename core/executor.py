from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Callable
from chains.evm_handler import EVMHandler
from chains.solana_handler import SolanaHandler
from loguru import logger
from typing import Literal
from config import API_CHAIN_NAMES
from web3 import Web3
import traceback

#диспетчер обработки транзакций для отправки данных в нужный обработчик (EVM/SOL)

class TransactionExecutor:

    def __init__(self,):
        
        self.handlers: Dict[
            Literal[*API_CHAIN_NAMES.values()],
            EVMHandler | SolanaHandler
        ] = {
            #chain_name: TxHandlerClass (EVMHandler/SolanaHandler)
        }
        
    def register_handler(
        self, 
        chain: Literal[*API_CHAIN_NAMES.values()], 
        handler: EVMHandler | SolanaHandler
    ):
        """Регистрируем хендлеры по именам из вебсокета"""
        self.handlers[chain] = handler
        logger.info(f"[{handler.chain_name}] TX Handler registered")

    async def stop_handlers(self):
        for handler in self.handlers.values():
            await handler.close()
        return 

    #коллбек функция вебсокета, которая дальше определяет в какую сеть идет свап 
    async def execute_trade(self, token_data: dict):
        chain = token_data.get('chain')
        token_address = token_data.get('token_address')
        token_name = token_data.get('token_name')
 
        try:
            tx_handler = self.handlers.get(chain)
            
            tx_hash = await tx_handler.execute_swap(token_address)
            if tx_hash:
                logger.info(f"[{tx_handler.chain_name}] sent buy {token_name} ({token_address}) | TX: {tx_hash}")
            else:
                logger.error(f"[{tx_handler.chain_name}] buy {token_name} ({token_address}) failed")            
        except Exception as e:
            logger.error(f"[{tx_handler.chain_name}] buy {token_name} ({token_address}) failed | Error: {e}")