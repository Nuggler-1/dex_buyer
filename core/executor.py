
from typing import Dict, Callable
from chains import EVMHandler, SolanaHandler
from loguru import logger
from typing import Literal
from config import CHAIN_NAMES
from web3 import Web3
import traceback

#диспетчер обработки транзакций для отправки данных в нужный обработчик (EVM/SOL)

class TransactionExecutor:

    def __init__(self,):
        
        self.handlers: Dict[
            Literal[*CHAIN_NAMES],
            EVMHandler | SolanaHandler
        ] = {
            #chain_name: TxHandlerClass (EVMHandler/SolanaHandler)
        }
        
    def register_handler(
        self, 
        chain: Literal[*CHAIN_NAMES], 
        handler: EVMHandler | SolanaHandler
    ):
        """Регистрируем хендлеры по локальным именам в софте"""
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
        token_supply = token_data.get('token_supply')
 
        try:
            tx_handler = self.handlers.get(chain)
            
            tx_hash = await tx_handler.execute_swap(token_address, token_supply)
            if tx_hash:
                logger.info(f"[{tx_handler.chain_name}] bought {token_name} ({token_address}) | TX: {tx_hash}")
                await tx_handler.tg_client.send_trade_alert(
                    tx_handler.chain_name,
                    token_address,
                    token_name,
                    tx_hash=tx_hash
                )
            else:
                logger.error(f"[{tx_handler.chain_name}] buy {token_name} ({token_address}) failed")     
                await tx_handler.tg_client.send_error_alert(
                    "SWAP_FAILED",
                    f"{tx_handler.chain_name} buy {token_name} {token_address} failed",
                    "tx failed or not sent - check logs"
                )       
        except Exception as e:
            logger.error(f"[{tx_handler.chain_name}] buy {token_name} ({token_address}) failed | Error: {e}")
            await tx_handler.tg_client.send_error_alert(
                "SWAP_FAILED",
                f"{tx_handler.chain_name} buy {token_name} {token_address} failed",
                f"{str(e)}"
            )