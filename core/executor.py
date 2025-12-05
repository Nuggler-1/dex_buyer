
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
        handler.logger.info("TX Handler registered")

    async def stop_handlers(self):
        for handler in self.handlers.values():
            await handler.close()
        return 

    #коллбек функция вебсокета, которая дальше определяет в какую сеть идет свап 
    async def execute_trade(self, token_data: dict):
        """
         token_data = {
            'chain': chain,
            'ticker': ticker,
            'token_address': address,
            'circulating_supply': circulating_supply,
            'pool_data': pool_data
        }"""
        chain = token_data.get('chain')
        ticker = token_data.get('ticker')
        token_address = token_data.get('token_address')
        circulating_supply = token_data.get('circulating_supply')
        pool_data = token_data.get('pool_data')
 
        try:
            tx_handler = self.handlers.get(chain)
            custom_size = token_data.get('custom_size')
            custom_tp_ladder = token_data.get('custom_tp_ladder')
            tx_hash = await tx_handler.execute_swap(circulating_supply, pool_data, custom_size, custom_tp_ladder)
            if tx_hash:
                tx_handler.logger.success(f"bought {ticker} ({token_address}) | TX: {tx_hash}")
                await tx_handler.tg_client.send_trade_alert(
                    tx_handler.chain_name,
                    token_address,
                    ticker,
                    tx_hash=tx_hash
                )
            else:
                tx_handler.logger.error(f"buy {ticker} ({token_address}) failed")     
                await tx_handler.tg_client.send_error_alert(
                    "SWAP_FAILED",
                    f"{tx_handler.chain_name} buy {ticker} {token_address} failed",
                    "tx failed or not sent - check logs"
                )       
        except Exception as e:
            tx_handler.logger.error(f"buy {ticker} ({token_address}) failed | Error: {e}")
            await tx_handler.tg_client.send_error_alert(
                "SWAP_FAILED",
                f"{tx_handler.chain_name} buy {ticker} {token_address} failed",
                f"{str(e)}"
            )