"""
DEX price poller — fetches current sqrtPriceX96 from Uniswap v3 / PancakeSwap v3
pools via eth_call(slot0()) on a fixed interval.

Complements the Swap event WebSocket feed:
  - Swap events: instant but only fires when a trade happens (sparse on low-volume pools)
  - Poller: fires every N seconds regardless — gives dense continuous price series for backtest

slot0() ABI:
  function slot0() external view returns (
      uint160 sqrtPriceX96,
      int24 tick,
      uint16 observationIndex,
      uint16 observationCardinality,
      uint16 observationCardinalityNext,
      uint8 feeProtocol,
      bool unlocked
  )
  Selector: 0x3850c7bd
  Returns 7 × 32 bytes; sqrtPriceX96 is the first word.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, List, Optional

import aiohttp

from core.events import PriceEventBus
from core.models import Chain, PriceEvent
from feeds.uniswap_v3_ws import PoolConfig, sqrt_price_x96_to_price

logger = logging.getLogger(__name__)

# slot0() function selector (keccak256("slot0()")[:4])
SLOT0_SELECTOR = "0x3850c7bd"


class DexPricePoller:
    """
    Polls all configured DEX pools via eth_call every `interval` seconds.
    Pushes a PriceEvent to the bus for each pool on every poll cycle.

    Uses a single aiohttp session and fires all pool calls concurrently
    per cycle to minimise total latency.
    """

    def __init__(
        self,
        bus: PriceEventBus,
        http_url: str,
        pools: List[PoolConfig],
        chain: Chain,
        interval: float = 3.0,
        source_name: Optional[str] = None,
    ):
        self._bus = bus
        self._http_url = http_url
        self._pools = pools
        self._chain = chain
        self._interval = interval
        self._source = source_name or f"poller_{chain.value}"
        self._running = False
        self._poll_count = 0
        self._error_count = 0

    async def start(self) -> None:
        self._running = True
        logger.info(
            "[%s] Starting — polling %d pools every %.1fs via %s",
            self._source, len(self._pools), self._interval,
            self._http_url[:40],
        )
        async with aiohttp.ClientSession() as session:
            while self._running:
                t0 = time.monotonic()
                await self._poll_all(session)
                elapsed = time.monotonic() - t0
                sleep_for = max(0.0, self._interval - elapsed)
                await asyncio.sleep(sleep_for)

    async def stop(self) -> None:
        self._running = False

    async def _poll_all(self, session: aiohttp.ClientSession) -> None:
        """Fire eth_call for all pools concurrently."""
        tasks = [self._poll_pool(session, pool) for pool in self._pools]
        await asyncio.gather(*tasks, return_exceptions=True)
        self._poll_count += 1

    async def _poll_pool(self, session: aiohttp.ClientSession, pool: PoolConfig) -> None:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {"to": pool.address, "data": SLOT0_SELECTOR},
                "latest",
            ],
        }
        recv_ns = time.time_ns()
        try:
            async with session.post(
                self._http_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=4),
            ) as resp:
                data = await resp.json(content_type=None)
        except Exception as exc:
            self._error_count += 1
            logger.debug("[%s] eth_call error for %s: %s", self._source, pool.address[:10], exc)
            return

        result_hex = data.get("result", "")
        if not result_hex or result_hex == "0x" or data.get("error"):
            err = data.get("error", {})
            logger.debug("[%s] empty/error result for %s: %s", self._source, pool.address[:10], err)
            return

        sqrt_price = self._decode_sqrt_price(result_hex)
        if not sqrt_price:
            return

        price = sqrt_price_x96_to_price(
            sqrt_price,
            pool.token0_decimals,
            pool.token1_decimals,
            pool.token0_is_base,
        )
        if price <= 0:
            return

        half_spread = price * pool.fee_pct / 2
        event = PriceEvent(
            source=self._source,
            pair=pool.pair,
            bid=price - half_spread,
            ask=price + half_spread,
            timestamp_ns=recv_ns,
            chain=self._chain,
            pool_address=pool.address,
        )
        self._bus.put_nowait(event)
        logger.debug(
            "[%s] %s = %.4f (pool %s)",
            self._source, pool.pair, price, pool.address[:10],
        )

    @staticmethod
    def _decode_sqrt_price(result_hex: str) -> Optional[int]:
        """
        Decode sqrtPriceX96 from slot0() return data.
        slot0() returns 7 ABI-encoded values; sqrtPriceX96 is the first word (32 bytes).
        """
        try:
            hex_data = result_hex[2:] if result_hex.startswith("0x") else result_hex
            if len(hex_data) < 64:
                return None
            return int(hex_data[:64], 16)
        except (ValueError, IndexError):
            return None

    @property
    def stats(self) -> dict:
        return {
            "source": self._source,
            "polls": self._poll_count,
            "errors": self._error_count,
            "pools": len(self._pools),
        }
