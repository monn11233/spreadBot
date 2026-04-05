"""
PancakeSwap v3 (BSC) WebSocket feed.

PancakeSwap v3 forked Uniswap v3's ABI, so the Swap event structure
and sqrtPriceX96 conversion are identical. Only the RPC URL and
pool addresses differ.
"""
from __future__ import annotations

import logging
from typing import List

from core.events import PriceEventBus
from core.models import Chain
from feeds.uniswap_v3_ws import PoolConfig, UniswapV3Feed

logger = logging.getLogger(__name__)

# Top PancakeSwap v3 BSC pools (USDT-quoted alts)
BSC_POOLS: List[PoolConfig] = [
    # WBNB/USDT 0.05% on BSC
    PoolConfig(
        address="0x36696169C63e42cd08ce11f5deeBbCeBae652050",
        pair="BNB/USDT",
        token0_decimals=18,
        token1_decimals=18,  # BSC USDT is 18 decimals on older contracts
        fee_tier=500,
        token0_is_base=True,
    ),
]


class PancakeSwapV3Feed(UniswapV3Feed):
    """
    PancakeSwap v3 BSC feed — identical logic to UniswapV3Feed, different pools/chain.
    """

    def __init__(self, bus: PriceEventBus, ws_url: str, pools: List[PoolConfig]):
        super().__init__(bus, ws_url=ws_url, pools=pools, chain=Chain.BSC)
        self.name = "pancakeswap_bsc"
