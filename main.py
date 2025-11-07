import asyncio
#import uvloop
from core.trading_bot import TradingBot

#Ставим uvloop в дефолт (ворк только на unix)
#asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def main():
    bot = TradingBot()
    await bot.start()

if __name__ == "__main__":
    asyncio.run(main())