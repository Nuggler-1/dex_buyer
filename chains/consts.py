DEX_ROUTER_DATA = {
    'ETHEREUM': {
        'chain_id': 1,
        'weth_address': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
        'router_address': '0xE592427A0AEce92De3Edee1F18E0157C05861564',
        'spender_address': '0xE592427A0AEce92De3Edee1F18E0157C05861564',
        'quoter_address': '0x61fFE014bA17989E743c5F6cB21bF9697530B21e',
        'token_decimals': 6,
        'USDT': '0xdAC17F958D2ee523a2206206994597C13D831ec7'
    },
    'ARBITRUM': {
        'chain_id': 42161,
        'weth_address': '0x',
        'router_address': '0xE592427A0AEce92De3Edee1F18E0157C05861564',
        'spender_address': '0xE592427A0AEce92De3Edee1F18E0157C05861564',
        'quoter_address': '0x61fFE014bA17989E743c5F6cB21bF9697530B21e',
        'token_decimals': 6,
        'USDT': '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9'
    },
    'BSC': {
        'chain_id': 56,
        'weth_address': '0x',
        'router_address': '0x1b81D678ffb9C0263b24A97847620C99d213eB14',
        'spender_address': '0x1b81D678ffb9C0263b24A97847620C99d213eB14',
        'quoter_address': '0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997',
        'token_decimals': 18,
        'USDT': '0x55d398326f99059fF775485246999027B3197955'
    },
    'SOLANA': {
        'WSOL': 'So11111111111111111111111111111111111111112',
        'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
        'token_decimals': 6
    }
}

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

erc20_abi = [
            {
                "constant": True,
                "inputs": [],
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