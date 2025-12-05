SOFT_NAME = "SOFT_NAME"


RPC = {
    "ARBITRUM": 'https://arb-mainnet.g.alchemy.com/v2/',
    "ETHEREUM": 'https://eth-mainnet.g.alchemy.com/v2/',
    "BSC": 'https://lb.drpc.live/bsc/', 
    "SOLANA": 'https://lb.drpc.live/solana/',
}


WS_RPC = {
    "ARBITRUM": 'wss://lb.drpc.live/arbitrum/',
    "ETHEREUM": 'wss://lb.drpc.live/ethereum/',
    "BSC": 'wss://lb.drpc.live/bsc/',
    "SOLANA": '',#соль без поддержки вебсокета пока
}
#для EVM чейнов
USE_WEBSOCKET = True

#адрес сервера который присылает сигнал
LISTINGS_WS_URL = ""
NEWS_WS_URL = ""
RECONNECT_ATTEMPTS = 10
RECONNECT_DELAY = 5

#имена чейнов для софта
CHAIN_NAMES = {
    'ETHEREUM',
    'ARBITRUM',
    'BSC',
    'SOLANA'
}


#===========================POSITION CONFIG===========================

#макс слиппадж
SLIPPAGE_PERCENT = 30 #максимально допустимый слиппадж
DELAY_BEFORE_TP = 10 #сколько секунд ждать перед запуском таска на продажу 

#какие токены будет использовать для покупку (нужно иметь баланс во всех включенных токенах)
#(будет брать первый самый ликвидный пул, если токен пула есть в этом списке)
USABLE_TOKENS = [
    'USDT', 
    'USDC', 
    'WBNB',
    'WETH', 
    'WSOL'
]

MARKET_CAP_CONFIG = [
        {
            "min_cap": 0,
            "max_cap": 75_000_000,
            "tp_ladder_id": 1,
            "size": {
                'USDT': 1,
                'USDC': 1,
                'WETH': 0.00035,
                'WSOL': 0.001,
                'WBNB': 0.01,
            },
            "enabled": True
        },
        {
            "min_cap": 75_000_001,
            "max_cap": 150_000_000,
            "tp_ladder_id": 2,
            "size": {
                'USDT': 1,
                'USDC': 1,
                'WETH': 0.00035,
                'WSOL': 0.001,
                'WBNB': 0.01,
            },
            "enabled": True
        },
        {
            "min_cap": 150_000_001,
            "max_cap": 250_000_000,
            "tp_ladder_id": 3,
            "size": {
                'USDT': 1,
                'USDC': 1,
                'WETH': 0.00035,
                'WSOL': 0.001,
                'WBNB': 0.01,
            },
            "enabled": True
        },
        {
            "min_cap": 250_000_001,
            "max_cap": 400_000_000,
            "tp_ladder_id": 4,
            "size": {
                'USDT': 1,
                'USDC': 1,
                'WETH': 0.00035,
                'WSOL': 0.001,
                'WBNB': 0.01,
            },
            "enabled": True
        },
        {
            "min_cap": 400_000_001,
            "max_cap": float("inf"),
            "tp_ladder_id": 5,
            "size": {
                'USDT': 1,
                'USDC': 1,
                'WETH': 0.00035,
                'WSOL': 0.001,   
                'WBNB': 0.01,
            },
            "enabled": True
        }
    
]

TP_LADDERS = {
    1: {
        "enabled": True,
        "first_tp_percent": 0.25,
        "total_percent": 1,
        "steps": 8,
        "distribution": [5, 7, 10, 15, 13, 10, 10, 10],
        "SL_from_entry_percent": -0.05,
    },
    2: {
        "enabled": True,
        "first_tp_percent": 0.15,
        "total_percent": 0.8,
        "steps": 9,
        "distribution": [10, 10, 14, 14, 10, 10, 9, 7, 6],
        "SL_from_entry_percent": -0.02,
    },
    3: {
        "enabled": True,
        "first_tp_percent": 0.1,
        "total_percent": 0.7,
        "steps": 10,
        "distribution": [7, 9, 12, 14, 14, 10, 10, 10, 7, 7],
        "SL_from_entry_percent": -0.02,
    },
        4: {
        "enabled": True,
        "first_tp_percent": 0.1,
        "total_percent": 0.5,
        "steps": 7,
        "distribution": [15, 15, 15, 15, 15, 15, 10],
        "SL_from_entry_percent": -0.02,
    },
        5: {
        "enabled": True,
        "first_tp_percent": 0.005,
        "total_percent": 0.03,
        "steps": 2,
        "distribution": [50,50],
        "SL_from_entry_percent": -0.02,
    }
}

#===========================TELEGRAM CONFIG===========================

#Оставить пустым чтобы отключить уведомления в тг
TG_BOT_TOKEN = "8498289922:"  
TG_CHAT_ID = "341122695"    

#===========================ONCHAIN CONFIG==========================

PK_EVM = ""
PK_SOL = ""

#интервал обновления газа/блокхэша в кэше
GAS_UPDATE_INTERVAL = 5
GAS_MULTPLIER = 2 #мультипликатор на газпрайс только под евм чейны 
GAS_LIMIT = {
    "ETHEREUM": 450_000,
    "ARBITRUM": 550_000,
    "BSC": 550_000,
    "SOLANA": 500_000,
}
SOLANA_PRIORITY_FEE = 1_000_000 #приорити фии под солану
MIN_POOL_TVL = 10_000 #минимальный tvl для пула

#===========================PARSER CONFIG===========================

PARSED_DATA_CHECK_DELAY_DAYS = 7 #раз в сколько дней обновлять данные 

FORCE_UPDATE_ON_START = False #обновить данные пулов для евм/соланы на запуске 

CACHE_PRICE = False #кэшировать цену или нет (отключено т.к. пока решили запрашивать ее в реальном времени)
PRICE_UPDATE_DELAY = { #интервал обновления цен токенов в секундах (соответствуют времени блока)
    'ETHEREUM': 12,
    'ARBITRUM': 0.25,
    'BSC': 0.75,
    'SOLANA': 0.4,
}

ALL_BASE_TOKEN_TICKERS = [
    'USDT', 
    'USDC', 
    'WBNB',
    'WETH', 
    'WSOL'
]

#------CMC DATA

EXCHANGE_SLUGS = [
    'uniswap-v3-arbitrum',
    'uniswap_v3_arbitrum',
    'pancakeswap-v3-bsc',
    'pancakeswap-v3',
    'uniswap-v3',
    'uniswap_v3',
    'raydium',

    #'pancakeswap-v2',
    #'uniswap-v2',
    #'uniswap-v2-arbitrum',
    #'meteora-dlmm'
]

EXHANGE_SLUG_TO_BOT_SLUG = {
    'uniswap-v3-arbitrum': 'v3',
    'uniswap_v3_arbitrum': 'v3',
    'pancakeswap-v3': 'v3',
    'pancakeswap-v3-bsc': 'v3',
    'uniswap-v3': 'v3',
    'uniswap_v3': 'v3',
    'raydium': 'amm',
    #'pancakeswap-v2': 'v2',
    #'uniswap-v2': 'v2',
    #'uniswap-v2-arbitrum': 'v2',
    #'meteora-dlmm': 'dlmm'
}

CMC_PLATFORM_IDS = {
    1027: 'ETHEREUM',
    5426: 'SOLANA',
    11841: 'ARBITRUM',
    1839: 'BSC',
}
CMC_API_KEY = ''

#------GECKO DATA

GECKO_API_KEY = ''
GECKO_CHAIN_NAMES = {
    'ETHEREUM': 'eth',
    'SOLANA': 'solana',
    'BSC': 'bsc',
    'ARBITRUM': 'arbitrum'
}

#------RATELIMIT SETTINGS

CACHE_UPDATE_BATCH_SIZE = 50  #количество распаралелленых запросов в пачке при обновлении ончейн-данных
DELAY_BETWEEN_BATCHES = 10 #Задержка между пачками токенов, (на платной по идее можно в ноль поставить)
ERROR_429_RETRIES = 3 #попытки  при рейтлимите
ERROR_429_DELAY = 60 #задеркжи при рейтлимите 

#=========================FILE PATHS===========================================

TOKEN_DATA_BASE_PATH = 'cache_data/'

SUPPLY_DATA_PATH = TOKEN_DATA_BASE_PATH + 'token_data.json'
LAST_CHECK_PATH = TOKEN_DATA_BASE_PATH + 'last_check.txt'

TP_CACHE_PATH = TOKEN_DATA_BASE_PATH + '/TP_data/'

DEFAULT_LOGS_FILE = 'logs.txt'
LOGS_SIZE = '10 MB'
