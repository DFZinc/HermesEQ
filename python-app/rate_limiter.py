"""
Rate Limiter
------------
Enforces Etherscan's free tier limit of 5 calls per second.
Every Etherscan API call must go through this limiter.
Built in from day one — not a retrofit.
"""

import asyncio
import time
import logging

log = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, calls_per_second: int = 5):
        self.calls_per_second = calls_per_second
        self.min_interval     = 1.0 / calls_per_second  # 0.2s between calls
        self._lock            = asyncio.Lock()
        self._last_call       = 0.0

    async def acquire(self):
        """Call before every Etherscan API request."""
        async with self._lock:
            now     = time.monotonic()
            elapsed = now - self._last_call
            wait    = self.min_interval - elapsed
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call = time.monotonic()
