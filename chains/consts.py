DEX_ROUTER_DATA = {
    'ETHEREUM': {
        'chain_id': 1,
        'dex_contracts':{
            'uni_v2': {
                'router_address': '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D',
                'factory_address': '0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f',
            },
            'uni_v3': {
                'router_address': '0xE592427A0AEce92De3Edee1F18E0157C05861564',
                'quoter_address': '0x61fFE014bA17989E743c5F6cB21bF9697530B21e',
                'factory_address': '0x1F98431c8aD98523631AE4a59f267346ea31F984',
            },
            'uni_v4': {
                'quoter_address': '0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203',
                'manager_address': '0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e',
                'router_address': '0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af',
                'permit_address': '0x000000000022D473030F116dDEE9F6B43aC78BA3'
            },
        },
        'token_decimals': {
            '0xdAC17F958D2ee523a2206206994597C13D831ec7': 6,
            '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48': 6,
            '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE': 18,
            'USDT': 6,
            'USDC': 6,
            'ETH': 18
        },
        'USDT': '0xdAC17F958D2ee523a2206206994597C13D831ec7',
        'USDC': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
        'ETH': '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',
        'WETH': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
        'gas_token': '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',
        'gas_token_ticker': 'ETH'
    },
    'ARBITRUM': {
        'chain_id': 42161,
        'dex_contracts': {
            'uni_v2': {
                'router_address': '0x4752ba5DBc23f44D87826276BF6Fd6b1C372aD24',
                'factory_address': '0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9',
            },
            'uni_v3': {
                'router_address': '0xE592427A0AEce92De3Edee1F18E0157C05861564',
                'quoter_address': '0x61fFE014bA17989E743c5F6cB21bF9697530B21e',
                'factory_address': '0x1F98431c8aD98523631AE4a59f267346ea31F984',
            },
            'uni_v4': {
                'quoter_address': '0x3972C00f7ed4885e145823eb7C655375d275A1C5',
                'manager_address': '0xd88F38F930b7952f2DB2432Cb002E7abbF3dD869',
                'router_address': '0xA51afAFe0263b40EdaEf0Df8781eA9aa03E381a3',
                'permit_address': '0x000000000022D473030F116dDEE9F6B43aC78BA3'
            }
        },
        'token_decimals': {
            '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9': 6,
            '0xaf88d065e77c8cC2239327C5EDb3A432268e5831': 6,
            '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE': 18,
            'USDT': 6,
            'USDC': 6,
            'ETH': 18
        },
        'USDT': '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9',
        'USDC': '0xaf88d065e77c8cC2239327C5EDb3A432268e5831',
        'ETH': '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',
        'WETH': '0x4200000000000000000000000000000000000006',
        'gas_token': '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',
        'gas_token_ticker': 'ETH'
    },
    'BASE': {
        'chain_id': 8453,
        'dex_contracts': {
        
        },
        'token_decimals': {
            '0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2': 18,
            '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913': 18,
            '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE': 18,  
            'USDT': 18,
            'USDC': 18,
            'ETH': 18
        },
        'USDT': '0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2',
        'USDC': '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913',
        'ETH': '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE', 
        'WETH': '0x4200000000000000000000000000000000000006',  
        'gas_token': '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',
        'gas_token_ticker': 'ETH'
    },
    'BSC': {
        'chain_id': 56,
        'dex_contracts': {
            'cake_v2': {
                'router_address': '0x10ED43C718714eb63d5aA57B78B54704E256024E',
                'factory_address': '0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73',
            },
            'cake_v3': {
                'router_address': '0x1b81D678ffb9C0263b24A97847620C99d213eB14',
                'quoter_address': '0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997',
                'factory_address': '0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865',
            }, 
            'cake_v4': {
                'quoter_address': '0xd0737C9762912dD34c3271197E362Aa736Df0926',
                'manager_address': '0x55f4c8abA71A1e923edC303eb4fEfF14608cC226',
                'router_address': '0xd9C500DfF816a1Da21A48A732d3498Bf09dc9AEB',
                'permit_address': '0x31c2F6fcFf4F8759b3Bd5Bf0e1084A055615c768'
            },  
        },
        'token_decimals': {
            '0x55d398326f99059fF775485246999027B3197955': 18,
            '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d': 18,
            '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE': 18,  
            'USDT': 18,
            'USDC': 18,
            'BNB': 18
        },
        'USDT': '0x55d398326f99059fF775485246999027B3197955',
        'USDC': '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d',
        'BNB': '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',   
        'WBNB': '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c',
        'gas_token': '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',
        'gas_token_ticker': 'BNB'
    },
    'SOLANA': {
        'chain_id': 501,
        'token_decimals': {
            'So11111111111111111111111111111111111111111': 9,
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 6,
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': 6,
            'USDC': 6,
            'SOL': 9,
            'USDT': 6
        },
        'SOL': 'So11111111111111111111111111111111111111111',
        'WSOL': 'So11111111111111111111111111111111111111112',
        'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
        'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
        'gas_token': 'So11111111111111111111111111111111111111111',
        'gas_token_ticker': 'SOL'
    }
}

ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'

quoter_abi_v3 = [
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

manager_abi = [
    {
        "name": "poolKeys",
        "type": "function",
        "inputs": [
            {"name": "poolId", "type": "bytes25"}
        ],
        "outputs": [
            {"name": "currency0", "type": "address"},
            {"name": "currency1", "type": "address"},
            {"name": "fee", "type": "uint24"},
            {"name": "tickSpacing", "type": "int24"},
            {"name": "hooks", "type": "address"}
        ],
        "stateMutability": "view"
    }
]

manager_abi_cake = [
    {
        "name": "poolKeys",
        "type": "function",
        "inputs": [
            {"name": "poolId", "type": "bytes25"}
        ],
        "outputs": [
            {"name": "currency0", "type": "address"},
            {"name": "currency1", "type": "address"},
            {"name": "hooks", "type": "address"},
            {"name": "poolManager", "type": "address"},
            {"name": "fee", "type": "uint24"},
            {"name": "parameters", "type": "bytes32"},
            
        ],
        "stateMutability": "view"
    }
]

router_abi_v2 = [
    {
        "name": "swapExactETHForTokens",
        "type": "function",
        "stateMutability": "payable",
        "inputs": [
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"}
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}]
    },
    {
        "name": "swapExactTokensForTokens",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"}
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}]
    },
    {
        "name": "swapExactTokensForETH",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"}
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}]
    },
    {
        "name": "getAmountsOut",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "path", "type": "address[]"}
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}]
    }
]

router_abi_v4 = [
        {
            "name": "execute",
            "type": "function",
            "stateMutability": "payable",
            "inputs": [
                {"name": "commands", "type": "bytes"},
                {"name": "inputs", "type": "bytes[]"},
                {"name": "deadline", "type": "uint256"},
            ],
            "outputs": [],
        }
    ]

permit2_abi = [
    {
        "name": "approve",
        "type": "function",
        "inputs": [
            {"name": "token", "type": "address"},
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint160"},
            {"name": "expiration", "type": "uint48"}
        ],
        "outputs": [],
        "stateMutability": "nonpayable"
    },
    {
        "name": "allowance",
        "type": "function",
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "token", "type": "address"},
            {"name": "spender", "type": "address"}
        ],
        "outputs": [
            {"name": "amount", "type": "uint160"},
            {"name": "expiration", "type": "uint48"},
            {"name": "nonce", "type": "uint48"}
        ],
        "stateMutability": "view"
    }
]

quoter_abi_v4 = [
    {
        "name": "quoteExactInputSingle",
        "type": "function",
        "inputs": [
            {
                "name": "params",
                "type": "tuple",
                "components": [
                    {
                        "name": "poolKey",
                        "type": "tuple",
                        "components": [
                            {"name": "currency0", "type": "address"},
                            {"name": "currency1", "type": "address"},
                            {"name": "fee", "type": "uint24"},
                            {"name": "tickSpacing", "type": "int24"},
                            {"name": "hooks", "type": "address"}
                        ]
                    },
                    {"name": "zeroForOne", "type": "bool"},
                    {"name": "exactAmount", "type": "uint128"},
                    {"name": "hookData", "type": "bytes"}
                ]
            }
        ],
        "outputs": [
            {"name": "amountOut", "type": "uint256"},
            {"name": "gasEstimate", "type": "uint256"}
        ]
    }
]

quoter_abi_v4_cake = [
    {
        "name": "quoteExactInputSingle",
        "type": "function",
        "inputs": [
            {
                "name": "params",
                "type": "tuple",
                "components": [
                    {
                        "name": "poolKey",
                        "type": "tuple",
                        "components": [
                            {"name": "currency0", "type": "address"},
                            {"name": "currency1", "type": "address"},
                            {"name": "hooks", "type": "address"},
                            {"name": "poolManager", "type": "address"},
                            {"name": "fee", "type": "uint24"},
                            {"name": "parameters", "type": "bytes32"},
                        ]
                    },
                    {"name": "zeroForOne", "type": "bool"},
                    {"name": "exactAmount", "type": "uint128"},
                    {"name": "hookData", "type": "bytes"}
                ]
            }
        ],
        "outputs": [
            {"name": "amountOut", "type": "uint256"},
            {"name": "gasEstimate", "type": "uint256"}
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
                "inputs": [],
                "name": "totalSupply",
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
                "constant": True,
                "payable": False,
                "stateMutability": "view",
                "type": "function",
                "name": "decimals",
                "inputs": [],
                "outputs": [
                    {
                        "name": "",
                        "type": "uint8"
                    }
                ]
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