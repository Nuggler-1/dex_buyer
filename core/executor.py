
from typing import Dict, Callable
from chains import TradeHandler
from loguru import logger
from typing import Literal
from config import CHAIN_NAMES, DELAY_BEFORE_SL
from web3 import Web3
import traceback
from .dataclasses import TokenTrade

#диспетчер обработки транзакций для отправки данных в нужный обработчик (EVM/SOL)

class TransactionExecutor:

    def __init__(self,):
        
        self.handlers: Dict[
            Literal[*CHAIN_NAMES],
            TradeHandler
        ] = {
            #chain_name: TxHandlerClass (EVMHandler/SolanaHandler)
        }
        
    def register_handler(
        self, 
        chain: Literal[*CHAIN_NAMES], 
        handler: TradeHandler
    ):
        """Регистрируем хендлеры по локальным именам в софте"""
        self.handlers[chain] = handler
        handler.logger.info("TX Handler registered")

    async def stop_handlers(self):
        for handler in self.handlers.values():
            await handler.close()
        return 

    #коллбек функция вебсокета, которая дальше определяет в какую сеть идет свап 
    async def execute_trade(self, token_data: TokenTrade):
        chain = token_data.chain
        ticker = token_data.ticker
        token_address = token_data.token_address

        tx_handler = self.handlers.get(chain.upper())
        if not tx_handler:
            logger.error(f"no handler registered for chain {chain}")
            return

        try:
            tx_hash = await tx_handler.execute_swap(
                token_address=token_address,
                base_token_name=token_data.ticker_to_sell,
                mcap=token_data.mcap,
                mcap_config=token_data.mcap_config,
                position_size=token_data.custom_size,
                custom_tp_ladder=token_data.custom_tp_ladder,
                delay_before_sl=token_data.delay_before_sl if token_data.delay_before_sl else DELAY_BEFORE_SL,
            )
            if tx_hash:
                tx_handler.logger.success(f"bought {ticker} ({token_address}) on {chain} | TX: {tx_hash}")
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
            tx_handler.logger.error(f"buy {ticker} ({token_address}) failed | Error: {str(e)}")
            await tx_handler.tg_client.send_error_alert(
                "SWAP_FAILED",
                f"{tx_handler.chain_name} buy {ticker} {token_address} failed",
                f"{str(e)}"
            )