import asyncio
from asyncio import WindowsSelectorEventLoopPolicy
from core.trading_bot import TradingBot
from core.key_manager import KeyManager
import warnings
import traceback
import sys
from loguru import logger
from config import DEFAULT_LOGS_FILE, LOGS_SIZE, SOFT_NAME

asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())
warnings.filterwarnings("ignore", message="Curlm alread closed")

logger.remove()
logger.add(
    sys.stdout,
    format=f"<green>{{time:HH:mm:ss}}</green> | [{SOFT_NAME}] | <level>{{level: <8}}</level> | <level>{{message}}</level>",
    colorize=True
)
logger.add(DEFAULT_LOGS_FILE, rotation=LOGS_SIZE)

async def main():
    # Initialize key manager and load/decrypt keys
    key_manager = KeyManager()
    sol_pk, evm_pk = key_manager.initialize_keys()
    
    # Create bot with decrypted keys
    bot = TradingBot(pk_sol=sol_pk, pk_evm=evm_pk)
    try:
        await bot.start()
    except Exception:
        logger.error(f"Bot error {traceback.format_exc()}")
        await bot.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped")