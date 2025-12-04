DEX_ROUTER_DATA = {
    'ETHEREUM': {
        'chain_id': 1,
        'router_address': '0xE592427A0AEce92De3Edee1F18E0157C05861564',
        'spender_address': '0xE592427A0AEce92De3Edee1F18E0157C05861564',
        'quoter_address': '0x61fFE014bA17989E743c5F6cB21bF9697530B21e',
        'factory_address': '0x1F98431c8aD98523631AE4a59f267346ea31F984',
        'token_decimals': {
            '0xdAC17F958D2ee523a2206206994597C13D831ec7': 6,
            '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48': 6,
            '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2': 18,
            'USDT': 6,
            'USDC': 6,
            'WETH': 18
        },
        'USDT': '0xdAC17F958D2ee523a2206206994597C13D831ec7',
        'USDC': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
        'WETH': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
        'gas_token': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
    },
    'ARBITRUM': {
        'chain_id': 42161,
        'router_address': '0xE592427A0AEce92De3Edee1F18E0157C05861564',
        'spender_address': '0xE592427A0AEce92De3Edee1F18E0157C05861564',
        'quoter_address': '0x61fFE014bA17989E743c5F6cB21bF9697530B21e',
        'factory_address': '0x1F98431c8aD98523631AE4a59f267346ea31F984',
        'token_decimals': {
            '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9': 6,
            '0xaf88d065e77c8cC2239327C5EDb3A432268e5831': 6,
            '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1': 18,
            'USDT': 6,
            'USDC': 6,
            'WETH': 18
        },
        'USDT': '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9',
        'USDC': '0xaf88d065e77c8cC2239327C5EDb3A432268e5831',
        'WETH': '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1',
        'gas_token': '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1',
    },
    'BSC': {
        'chain_id': 56,
        'router_address': '0x1b81D678ffb9C0263b24A97847620C99d213eB14',
        'spender_address': '0x1b81D678ffb9C0263b24A97847620C99d213eB14',
        'quoter_address': '0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997',
        'factory_address': '0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865',
        'token_decimals': {
            '0x55d398326f99059fF775485246999027B3197955': 18,
            '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d': 18,
            '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c': 18,  
            'USDT': 18,
            'USDC': 18,
            'WBNB': 18
        },
        'USDT': '0x55d398326f99059fF775485246999027B3197955',
        'USDC': '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d',
        'WBNB': '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c',   
        'gas_token': '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c',
    },
    'SOLANA': {
        'token_decimals': {
            'So11111111111111111111111111111111111111112': 9,
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 6,
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': 6,
            'USDC': 6,
            'WSOL': 9,
            'USDT': 6
        },
        'WSOL': 'So11111111111111111111111111111111111111112',
        'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
        'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
        'gas_token': 'So11111111111111111111111111111111111111112',
    }
}

ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'

quoter_abi = [
  {
    "name": "quoteExactInputSingle",
    "type": "function",
    "inputs": [
      {
        "name": "params",
        "type": "tuple",
        "components": [
          {"name": "tokenIn", "type": "address"},
          {"name": "tokenOut", "type": "address"},
          {"name": "amountIn", "type": "uint256"},
          {"name": "fee", "type": "uint24"},
          {"name": "sqrtPriceLimitX96", "type": "uint160"}
        ]
      }
    ],
    "outputs": [
      {"name": "amountOut", "type": "uint256"}
    ]
  }
]

factory_abi = [
    {
        "inputs": [
            {
                "internalType": "address",
                "name": "tokenA",
                "type": "address"
            },
            {
                "internalType": "address",
                "name": "tokenB",
                "type": "address"
            },
            {
                "internalType": "uint24",
                "name": "fee",
                "type": "uint24"
            }
        ],
        "name": "getPool",
        "outputs": [
            {
                "internalType": "address",
                "name": "",
                "type": "address"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

pool_abi = [
    {
        "inputs": [],
        "name": "fee",
        "outputs": [
            {
                "internalType": "uint24",
                "name": "",
                "type": "uint24"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

erc20_abi = [
            {
                "constant": True,
                "inputs": [
                    {
                        "name": "_owner",
                        "type": "address"
                    }
                ],
                "name": "balanceOf",
                "outputs": [
                    {
                        "name": "",
                        "type": "uint256"
                    }
                ],
                "payable": False,
                "stateMutability": "view",
                "type": "function"
            },
            {
                "constant": False,
                "inputs": [
                    {
                        "name": "_spender",
                        "type": "address"
                    },
                    {
                        "name": "_value",
                        "type": "uint256"
                    }
                ],
                "name": "approve",
                "outputs": [],
                "payable": False,
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "constant": False,
                "inputs": [
                    {
                        "name": "_owner",
                        "type": "address"
                    },
                    {
                        "name": "_spender",
                        "type": "address"
                    }
                ],
                "name": "allowance",
                "outputs": [
                    {
                        "name": "",
                        "type": "uint256"
                    }
                ],
                "payable": False,
                "stateMutability": "view",
                "type": "function"
            }
        ]