from .pancakeswap import CakeswapV2, CakeswapV3, CakeswapV4
from .uniswap import UniswapV2, UniswapV3, UniswapV4

DEX_MAP = {
    'uni_v2': UniswapV2,
    'uni_v3': UniswapV3,
    'uni_v4': UniswapV4,
    'cake_v2': CakeswapV2,
    'cake_v3': CakeswapV3,
    'cake_v4': CakeswapV4
}