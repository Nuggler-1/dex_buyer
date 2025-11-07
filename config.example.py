
RPC = {
    "ARBITRUM": '',
    "ETHEREUM": '',
    "BSC": '', 
    "SOLANA": 'https://fluent-indulgent-morning.solana-mainnet.quiknode.pro/ab7fa4dea37e6f768130c76cd9941f15255db327/',
}

#SHYFT_API_KEY = "gAd471RYaEGchf-y" #для фетчинга lp pool raydium

WS_RPC = {
    "ARBITRUM": 'wss://arb-mainnet.g.alchemy.com/v2/eEaS-mAt1oQzqmeC1fzDe8inoqYKSV-C',
    "ETHEREUM": 'wss://eth-mainnet.g.alchemy.com/v2/eEaS-mAt1oQzqmeC1fzDe8inoqYKSV-C',
    "BSC": 'wss://bsc-rpc.publicnode.com',
    "SOLANA": '',#соль без поддержки вебсокета пока
}

USE_WEBSOCKET = True

#имена чейнов для софта/вебсокет сервера
#если на сервере имя чейна как-то отличается, то нужно его просто поменять в значении словаря
#пример: 'ETHEREUM': 'eth' - если на сервера имя эфир чейна это eth
API_CHAIN_NAMES = {
    'ETHEREUM': 'ethereum',
    'ARBITRUM': 'arbitrum',
    'BSC': 'bsc',
    'SOLANA': 'solana',
}

#адрес сервера который присылает сигнал
WS_URL = "ws://localhost:8765"

#позже можно заебаться и хранить их в зашифрованном виде в тхт
PK_EVM = ""
PK_SOL = ""

#(добавить поддержку нативки в будущем/сделать словарь для каждого токена свое количество)
AMOUNT_BUY = 1 #сколько тезеров потратить 
TOKEN = "USDT"

#макс слиппадж
SLIPPAGE_PERCENT = 2

#интервал обновления газа/блокхэша в кэше
GAS_UPDATE_INTERVAL = 5
GAS_MULTPLIER = 2 #мультипликатор на газпрайс только под евм чейны 
GAS_LIMIT = {
    "ETHEREUM": 450_000,
    "ARBITRUM": 550_000,
    "BSC": 550_000,
    "SOLANA": 500_000,
}
SOLANA_PRIORITY_FEE = 1_000_000 #газ прайс под солану

#делать ли доп запрос к рпс для доп защиты от мев ботов (+ запрос)
#в фкфс чейнах и на парах с толстой ликвой не нужно, для щитков, особенно на эфире, лучше включить
EVM_QUOTE_MIN_OUT = False
