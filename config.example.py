SOFT_NAME = "SOFT_NAME"


RPC = {
    "http":{
        "ARBITRUM": 'https://rpc.ankr.com/arbitrum/460c3368e39ee4029476310fc1f098c5608bdb458a469b08c6857e373ef02b84',
        "ETHEREUM": 'https://eth-mainnet.g.alchemy.com/v2/m3E4cfjkK53jSu1zq9C7DVQ5YdNmhMgp',
        "BSC": 'wss://lb.drpc.live/bsc/Ao7ya0msFUeiigkiZRZwqx8xQhizyg0R8JYQQmlfqV1j', 
        "SOLANA": 'https://mainnet.helius-rpc.com/?api-key=d1491ed3-d3e8-4348-a6e1-fd61c13b5580',
    },
    "wss": {
        "ARBITRUM": 'wss://arbitrum-one-rpc.publicnode.com',
        "ETHEREUM": 'wss://lb.drpc.live/ethereum/Ao7ya0msFUeiigkiZRZwqx8xQhizyg0R8JYQQmlfqV1j',
        "BSC": 'wss://lb.drpc.live/bsc/Ao7ya0msFUeiigkiZRZwqx8xQhizyg0R8JYQQmlfqV1j',
        "SOLANA": '',#соль без поддержки вебсокета пока
    },
}

RPC_FOR_LATENCY_ACTIONS = {
    "ARBITRUM": 'wss://arbitrum-one-rpc.publicnode.com',
    "ETHEREUM": 'wss://lb.drpc.live/ethereum/Ao7ya0msFUeiigkiZRZwqx8xQhizyg0R8JYQQmlfqV1j',
    "BSC": 'wss://lb.drpc.live/bsc/Ao7ya0msFUeiigkiZRZwqx8xQhizyg0R8JYQQmlfqV1j',
    "SOLANA": 'https://mainnet.helius-rpc.com/?api-key=d1491ed3-d3e8-4348-a6e1-fd61c13b5580',
}

#для EVM чейнов
USE_WEBSOCKET = True

#адрес сервера который присылает сигнал
WS_URL = "ws://54.64.177.89:8443"
RECONNECT_ATTEMPTS = 10
RECONNECT_DELAY = 5


#===========================POSITION CONFIG===========================

#макс слиппадж
SLIPPAGE_PERCENT = {
    'ETHEREUM': 15,
    'BSC': 20, 
    'ARBITRUM': 20,
    'SOLANA': 15
} #максимально допустимый слиппадж
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

EVENTS = {
    'binance': [
        'alpha',
        'spot',
        'futures',
        #'hodler_airdrop',
        #'megadrop',
        #'launchpool',
        #'pre-market'
    ],
    'bybit': [
        'spot',
        'futures',
        'soon-spot',
        'soon-futures',
        #'launchpad',
        #'pre-market'
    ],
    'coinbase': [
        'spot'
    ],
    'coinbaseinternational':[
        'futures'
    ],
    'robinhood': [
        'spot'
    ],
    'bithumb': [
        'spot'
    ],
    'upbit':[
        'spot'
    ]
}

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

V4_MAX_POOL_FEE = 10_000 #1% 
V4_MAX_POOL_TICK = 200

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

CMC_SEARCH_LISTS = {
    "mexc": {
        "params": 'exchangeIds=544',
        "limit": 2500
    },
    "base top 300": {
        "params": 'platformIds=199',
        "limit": 300
    },
    "bsc top 600": {
        "params": 'platformIds=1839',
        "limit": 600
    },
    "bsc pancake v4 top 100": {
        "params": 'exchangeIds=12714',
        "limit": 100
    },
    "bsc pancake v2 top 400": {
        "params": 'exchangeIds=1344',
        'limit': 400,
    },
    "arbitrum top 200": {
        "params": 'platformIds=11841',
        "limit": 200
    },
    "eth top 500": {
        "params": 'platformIds=1027',
        "limit": 500
    }, 
    "raydium top 200": { 
        "params": 'exchangeIds=1342',
        "limit": 200
    }
}

EXCHANGE_SLUGS = [
    'uniswap-v3-arbitrum',
    'uniswap_v3_arbitrum',
    'pancakeswap-v3-bsc',
    'pancakeswap-v3',
    'uniswap-v3',
    'uniswap_v3',
    'raydium',

    'uniswap-v4',
    'uniswap_v4',
    'uniswap-v4-ethereum',
    'uniswap-v4-arbitrum',
    'uniswap_v4_arbitrum',
    'pancakeswap-v4-clamm-bsc',
    'pancakeswap_v4_clamm_bsc',
    'pancakeswap-infinity-clmm',

    'uniswap-v2',
    'uniswap_v2',
    'pancakeswap-v2',
    'pancakeswap_v2',

    #'uniswap-v2-arbitrum',
    #'meteora-dlmm'
]

EXHANGE_SLUG_TO_BOT_SLUG = {
    'uniswap-v3-arbitrum': 'uni_v3',
    'uniswap_v3_arbitrum': 'uni_v3',
    'pancakeswap-v3': 'cake_v3',
    'pancakeswap-v3-bsc': 'cake_v3',
    'uniswap-v3': 'uni_v3',
    'uniswap_v3': 'uni_v3',
    'raydium': 'amm',

    'uniswap-v4': 'uni_v4',
    'uniswap_v4': 'uni_v4',
    'uniswap-v4-ethereum': 'uni_v4',
    'uniswap-v4-arbitrum': 'uni_v4',
    'uniswap_v4_arbitrum': 'uni_v4',
    'pancakeswap-v4-clamm-bsc': 'cake_v4',
    'pancakeswap_v4_clamm_bsc': 'cake_v4',
    'pancakeswap-infinity-clmm': 'cake_v4',
    
    'pancakeswap-v2': 'cake_v2',
    'pancakeswap_v2': 'cake_v2',
    'uniswap-v2': 'uni_v2',
    'uniswap_v2': 'uni_v2',
    'uniswap-v2-arbitrum': 'uni_v2',
    'uniswap_v2_arbitrum': 'uni_v2',
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
