"""
Volume Monitor
--------------
Detects ETH tokens with genuine volume activity.

DISCOVERY SOURCES:

  1. Uniswap V3 Pool Creation Events (Etherscan event logs)
     Catches new V3 token launches within the lookback window.

  2. Uniswap V2 Pair Creation Events (Etherscan event logs)
     Catches new V2 token launches within the lookback window.

  3. Active swap activity (Etherscan token transfers from routers)
     Catches any token currently being swapped regardless of age.
     No cap on number of tokens returned.

  4. GeckoTerminal trending pools (free public API, no key required)
     Returns pools trending on Ethereum by on-chain activity.
     Catches volume spikes on established tokens like XCN regardless
     of age or which router they use.

All candidates validated against DexScreener for real volume/liquidity/txns.
DexScreener calls are rate limited to avoid triggering 429 responses.
"""

import asyncio
import aiohttp
import logging

log = logging.getLogger(__name__)

DEXSCREENER   = "https://api.dexscreener.com"
GECKOTERMINAL = "https://api.geckoterminal.com/api/v2"
ETHERSCAN     = "https://api.etherscan.io/v2/api"
CHAIN_ID      = 1

# Load API key from config.json
import json as _json, os as _os
def _load_cfg():
    p = _os.path.join(_os.path.dirname(__file__), "config.json")
    try:
        with open(p) as f: return _json.load(f)
    except Exception: return {}
_cfg    = _load_cfg()
API_KEY = _cfg.get("etherscan_api_key", "")

# Uniswap factory addresses
UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
UNISWAP_V2_FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"

# Event topic signatures
V3_POOL_CREATED_TOPIC = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
V2_PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"

# Router addresses
UNISWAP_V2_ROUTER     = "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"
UNISWAP_V3_ROUTER     = "0xe592427a0aece92de3edee1f18e0157c05861564"
UNISWAP_V3_ROUTER2    = "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45"
UNISWAP_UNIVERSAL     = "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad"  # Universal Router V3
UNISWAP_UNIVERSAL_V4  = "0x66a9893cc07d91d95644aedd05d03f95e1dba8af"  # Universal Router V4 — handles majority of current volume

# Infrastructure tokens — never trading opportunities
EXCLUDED_TOKENS = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
    "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
    "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",  # WBTC
    "0x0000000000000000000000000000000000000000",  # Null
    "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d",  # USD1
}

# ~6 hours of blocks at 12s/block
BLOCK_LOOKBACK = 1800

# Max concurrent DexScreener validation calls — avoid rate limiting
DEXSCREENER_CONCURRENCY = 5

# GeckoTerminal rate limit: 30 calls/minute = 1 call per 2 seconds
# Use a single semaphore + delay to stay within limit
GECKO_CONCURRENCY = 1
GECKO_DELAY_SECS  = 5.0


class VolumeMonitor:
    def __init__(self, rate_limiter=None):
        self.rl = rate_limiter

        self.MIN_VOLUME_USD_1H = _cfg.get("min_volume_usd_1h", 5_000)
        self.MIN_LIQUIDITY_USD = _cfg.get("min_liquidity_usd", 5_000)
        self.MIN_TXNS_1H       = _cfg.get("min_txns_1h",       10)

        self._seen_this_cycle = set()
        self._latest_block    = None

    async def get_active_tokens(self) -> list[dict]:
        self._seen_this_cycle = set()

        # Step 1: Get latest block
        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            self._latest_block = await self._get_latest_block(session)

        if not self._latest_block:
            log.warning("Could not fetch latest block number")
            return []

        log.info(f"  Block range: {self._latest_block - BLOCK_LOOKBACK} -> {self._latest_block}")

        # Step 2: Run all discovery sources concurrently
        # gecko_semaphore created here so it is shared across trending + fallback validation
        gecko_semaphore = asyncio.Semaphore(GECKO_CONCURRENCY)
        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            v3_new, v2_new, active, gecko = await asyncio.gather(
                self._fetch_v3_new_pools(session),
                self._fetch_v2_new_pairs(session),
                self._fetch_active_swaps(session),
                self._fetch_gecko_trending(session, gecko_semaphore),
                return_exceptions=True
            )

        v3_list    = v3_new if isinstance(v3_new,  list) else []
        v2_list    = v2_new if isinstance(v2_new,  list) else []
        act_list   = active if isinstance(active,  list) else []
        gecko_list = gecko  if isinstance(gecko,   list) else []

        log.info(
            f"  Discovery: {len(v3_list)} V3 new pools | "
            f"{len(v2_list)} V2 new pairs | "
            f"{len(act_list)} active swap tokens | "
            f"{len(gecko_list)} GeckoTerminal trending"
        )

        # Only validate tokens that have confirmed trading activity.
        # Sources 1 & 2 (pool/pair creation events) fire on pool creation alone —
        # a pool can exist with zero liquidity and zero trades.
        # Sources 3 & 4 (router swaps + GeckoTerminal trending) require real volume.
        # A token from Sources 1/2 is only worth validating if it also appears
        # in Source 3 or 4 — meaning it has actual swap activity.
        active_set  = set(act_list) | set(gecko_list)
        new_pool_set = set(v3_list) | set(v2_list)

        all_candidates = list({
            addr for addr in active_set | (new_pool_set & active_set)
            if addr not in EXCLUDED_TOKENS
        })

        # In practice: validate everything from Sources 3+4, plus any new pool
        # that already has swap activity confirming it launched with liquidity.
        all_candidates = list({
            addr for addr in act_list + gecko_list
            if addr not in EXCLUDED_TOKENS
        } | {
            addr for addr in v3_list + v2_list
            if addr not in EXCLUDED_TOKENS and addr in active_set
        })

        log.info(f"  Validating {len(all_candidates)} unique candidates (volume-confirmed only)...")

        # Validate against DexScreener with controlled concurrency
        # gecko_semaphore is shared with trending source above — same 30 call/min budget
        dex_semaphore = asyncio.Semaphore(DEXSCREENER_CONCURRENCY)
        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            tasks   = [self._fetch_pair_data(session, addr, dex_semaphore, gecko_semaphore) for addr in all_candidates]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        active_tokens = []
        failed = 0
        for token in results:
            if isinstance(token, Exception):
                failed += 1
                continue
            if not token:
                continue
            address = token.get("address", "").lower()
            if not address or address in self._seen_this_cycle or address in EXCLUDED_TOKENS:
                continue
            if self._passes_thresholds(token):
                self._seen_this_cycle.add(address)
                active_tokens.append(token)
                log.info(
                    f"  Active: {token.get('symbol')} ({token.get('dex','?')}) | "
                    f"Vol 1h: ${token.get('volume_usd_1h', 0):,.0f} | "
                    f"Txns 1h: {token.get('txns_1h', 0)} | "
                    f"Price ch: {token.get('price_change_pct_1h', 0):.1f}% | "
                    f"Liq: ${token.get('liquidity_usd', 0):,.0f}"
                )
            else:
                log.debug(
                    f"  Below threshold: {token.get('symbol','?')} | "
                    f"Vol: ${token.get('volume_usd_1h',0):,.0f} | "
                    f"Liq: ${token.get('liquidity_usd',0):,.0f} | "
                    f"Txns: {token.get('txns_1h',0)}"
                )

        if failed:
            log.warning(f"  {failed} DexScreener lookups failed")

        log.info(f"  {len(active_tokens)} active token(s) from {len(all_candidates)} candidates")
        return active_tokens

    # ── Source 1: V3 new pool creation events ────────────────────────

    async def _fetch_v3_new_pools(self, session: aiohttp.ClientSession) -> list[str]:
        try:
            params = {
                "chainid":   CHAIN_ID,
                "module":    "logs",
                "action":    "getLogs",
                "address":   UNISWAP_V3_FACTORY,
                "topic0":    V3_POOL_CREATED_TOPIC,
                "fromBlock": self._latest_block - BLOCK_LOOKBACK,
                "toBlock":   self._latest_block,
                "page":      1,
                "offset":    100,
                "apikey":    API_KEY,
            }
            if self.rl:
                await self.rl.acquire()
            async with session.get(ETHERSCAN, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    log.warning(f"V3 pool events HTTP {resp.status}")
                    return []
                data = await resp.json()
                logs = data.get("result", [])
                if not isinstance(logs, list):
                    log.warning(f"V3 pool events non-list: {logs}")
                    return []
                tokens = []
                for entry in logs:
                    topics = entry.get("topics", [])
                    for i in [1, 2]:
                        if i < len(topics):
                            addr = ("0x" + topics[i][-40:]).lower()
                            if addr not in EXCLUDED_TOKENS:
                                tokens.append(addr)
                return list(set(tokens))
        except Exception as e:
            log.warning(f"V3 pool events error: {e}")
            return []

    # ── Source 2: V2 new pair creation events ────────────────────────

    async def _fetch_v2_new_pairs(self, session: aiohttp.ClientSession) -> list[str]:
        try:
            params = {
                "chainid":   CHAIN_ID,
                "module":    "logs",
                "action":    "getLogs",
                "address":   UNISWAP_V2_FACTORY,
                "topic0":    V2_PAIR_CREATED_TOPIC,
                "fromBlock": self._latest_block - BLOCK_LOOKBACK,
                "toBlock":   self._latest_block,
                "page":      1,
                "offset":    100,
                "apikey":    API_KEY,
            }
            if self.rl:
                await self.rl.acquire()
            async with session.get(ETHERSCAN, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    log.warning(f"V2 pair events HTTP {resp.status}")
                    return []
                data = await resp.json()
                logs = data.get("result", [])
                if not isinstance(logs, list):
                    log.warning(f"V2 pair events non-list: {logs}")
                    return []
                tokens = []
                for entry in logs:
                    topics = entry.get("topics", [])
                    for i in [1, 2]:
                        if i < len(topics):
                            addr = ("0x" + topics[i][-40:]).lower()
                            if addr not in EXCLUDED_TOKENS:
                                tokens.append(addr)
                return list(set(tokens))
        except Exception as e:
            log.warning(f"V2 pair events error: {e}")
            return []

    # ── Source 3: Active swap tokens ─────────────────────────────────

    async def _fetch_active_swaps(self, session: aiohttp.ClientSession) -> list[str]:
        token_counts: dict[str, int] = {}
        for router in [UNISWAP_V2_ROUTER, UNISWAP_V3_ROUTER, UNISWAP_V3_ROUTER2, UNISWAP_UNIVERSAL, UNISWAP_UNIVERSAL_V4]:
            try:
                params = {
                    "chainid": CHAIN_ID,
                    "module":  "account",
                    "action":  "tokentx",
                    "address": router,
                    "page":    1,
                    "offset":  1000,
                    "sort":    "desc",
                    "apikey":  API_KEY,
                }
                if self.rl:
                    await self.rl.acquire()
                async with session.get(ETHERSCAN, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        log.warning(f"Router {router} HTTP {resp.status}")
                        continue
                    data = await resp.json()
                    txs  = data.get("result", [])
                    if not isinstance(txs, list):
                        continue
                    for tx in txs:
                        addr = tx.get("contractAddress", "").lower()
                        if addr and addr not in EXCLUDED_TOKENS:
                            token_counts[addr] = token_counts.get(addr, 0) + 1
            except Exception as e:
                log.warning(f"Router activity error: {e}")
        return list(token_counts.keys())

    # ── Source 4: GeckoTerminal trending pools ────────────────────────

    async def _fetch_gecko_trending(self, session: aiohttp.ClientSession, gecko_semaphore: asyncio.Semaphore) -> list[str]:
        """
        GeckoTerminal free public API — no key required.
        Returns trending Ethereum pools by on-chain activity.
        Catches volume spikes on any token regardless of age or router.
        Rate limit: 30 calls/minute — shared semaphore with fallback validator.
        """
        await gecko_semaphore.acquire()
        try:
            await asyncio.sleep(GECKO_DELAY_SECS)
            url = f"{GECKOTERMINAL}/networks/eth/trending_pools"
            params = {"include": "base_token", "page": 1}
            async with session.get(url, params=params, headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    log.warning(f"GeckoTerminal trending HTTP {resp.status}")
                    return []
                data  = await resp.json()
                pools = data.get("data", [])
                tokens = []
                for pool in pools:
                    attrs = pool.get("attributes", {})
                    # base_token_price_usd being present means it's a real trading pair
                    relationships = pool.get("relationships", {})
                    base_token = relationships.get("base_token", {}).get("data", {})
                    token_id   = base_token.get("id", "")  # format: "eth_0xADDRESS"
                    if token_id.startswith("eth_"):
                        addr = token_id[4:].lower()
                        if addr not in EXCLUDED_TOKENS:
                            tokens.append(addr)
                log.debug(f"GeckoTerminal returned {len(tokens)} trending ETH tokens")
                return tokens
        except Exception as e:
            log.warning(f"GeckoTerminal error: {e}")
            return []
        finally:
            gecko_semaphore.release()

    # ── DexScreener validation ────────────────────────────────────────

    async def _fetch_pair_data(
        self,
        session: aiohttp.ClientSession,
        token_address: str,
        semaphore: asyncio.Semaphore,
        gecko_semaphore: asyncio.Semaphore
    ) -> dict | None:
        # Step 1: Try DexScreener — max DEXSCREENER_CONCURRENCY concurrent calls
        dex_result = None
        async with semaphore:
            try:
                url = f"{DEXSCREENER}/token-pairs/v1/ethereum/{token_address}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data  = await resp.json()
                        pairs = data if isinstance(data, list) else data.get("pairs", [])
                        eth_pairs = [p for p in pairs if p.get("chainId") == "ethereum"]
                        if eth_pairs:
                            best = max(eth_pairs, key=lambda p: float((p.get("volume") or {}).get("h1", 0) or 0))
                            return self._normalize(best)
                    elif resp.status == 429:
                        log.warning("DexScreener rate limited (429)")
                    else:
                        log.debug(f"DexScreener {resp.status} for {token_address} — trying GeckoTerminal")
            except Exception as e:
                log.debug(f"DexScreener lookup error for {token_address}: {e}")

        # Step 2: DexScreener failed — fallback to GeckoTerminal
        # gecko_semaphore ensures only 1 GeckoTerminal call at a time
        # sleep AFTER call and BEFORE releasing so next caller waits full 2 seconds
        await gecko_semaphore.acquire()
        try:
            await asyncio.sleep(GECKO_DELAY_SECS)
            url = f"{GECKOTERMINAL}/networks/eth/tokens/{token_address}/pools"
            params = {"page": 1}
            async with session.get(url, params=params, headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    log.warning(f"GeckoTerminal fallback HTTP {resp.status} for {token_address}")
                    return None
                data  = await resp.json()
                pools = data.get("data", [])
                if not pools:
                    return None
                best  = max(
                    pools,
                    key=lambda p: float(p.get("attributes", {}).get("volume_usd", {}).get("h24", 0) or 0)
                )
                attrs = best.get("attributes", {})
                vol   = attrs.get("volume_usd", {})
                pc    = attrs.get("price_change_percentage", {})
                return {
                    "address":             token_address.lower(),
                    "symbol":              attrs.get("name", "UNKNOWN").split("/")[0].strip(),
                    "name":                attrs.get("name", ""),
                    "pair_address":        attrs.get("address", ""),
                    "dex":                 "geckoterminal",
                    "volume_usd_1h":       float(vol.get("h1", 0) or 0),
                    "volume_usd_6h":       float(vol.get("h6", 0) or 0),
                    "volume_usd_24h":      float(vol.get("h24", 0) or 0),
                    "liquidity_usd":       float(attrs.get("reserve_in_usd", 0) or 0),
                    "price_change_pct_1h": float(pc.get("h1", 0) or 0),
                    "txns_1h":             int(attrs.get("transactions", {}).get("h1", {}).get("buys", 0))
                                         + int(attrs.get("transactions", {}).get("h1", {}).get("sells", 0)),
                    "market_cap":          float(attrs.get("market_cap_usd", 0) or 0),
                }
        except Exception as e:
            log.debug(f"GeckoTerminal fallback error for {token_address}: {e}")
            return None
        finally:
            gecko_semaphore.release()

    def _normalize(self, pair: dict) -> dict:
        vol  = pair.get("volume", {})
        pc   = pair.get("priceChange", {})
        txns = pair.get("txns", {}).get("h1", {})
        return {
            "address":             pair.get("baseToken", {}).get("address", "").lower(),
            "symbol":              pair.get("baseToken", {}).get("symbol", "UNKNOWN"),
            "name":                pair.get("baseToken", {}).get("name", ""),
            "pair_address":        pair.get("pairAddress", ""),
            "dex":                 pair.get("dexId", ""),
            "volume_usd_1h":       float(vol.get("h1", 0) or 0),
            "volume_usd_6h":       float(vol.get("h6", 0) or 0),
            "volume_usd_24h":      float(vol.get("h24", 0) or 0),
            "liquidity_usd":       float((pair.get("liquidity") or {}).get("usd", 0) or 0),
            "price_change_pct_1h": float(pc.get("h1", 0) or 0),
            "txns_1h":             int(txns.get("buys", 0)) + int(txns.get("sells", 0)),
            "market_cap":          float(pair.get("marketCap", 0) or 0),
        }

    def _passes_thresholds(self, token: dict) -> bool:
        if token.get("volume_usd_1h", 0) < self.MIN_VOLUME_USD_1H:
            return False
        if token.get("liquidity_usd", 0)  < self.MIN_LIQUIDITY_USD:
            return False
        if token.get("txns_1h", 0)         < self.MIN_TXNS_1H:
            return False
        return True

    # ── Utility ───────────────────────────────────────────────────────

    async def _get_latest_block(self, session: aiohttp.ClientSession) -> int | None:
        try:
            params = {
                "chainid": CHAIN_ID,
                "module":  "proxy",
                "action":  "eth_blockNumber",
                "apikey":  API_KEY,
            }
            if self.rl:
                await self.rl.acquire()
            async with session.get(ETHERSCAN, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data   = await resp.json()
                    result = data.get("result", "")
                    if result and isinstance(result, str) and result.startswith("0x"):
                        block = int(result, 16)
                        if block > 0:
                            return block
                    log.debug(f"eth_blockNumber raw: {data}")

            # Fallback
            if self.rl:
                await self.rl.acquire()
            params2 = {
                "chainid": CHAIN_ID,
                "module":  "account",
                "action":  "txlist",
                "address": UNISWAP_V3_FACTORY,
                "page":    1,
                "offset":  1,
                "sort":    "desc",
                "apikey":  API_KEY,
            }
            async with session.get(ETHERSCAN, params=params2, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    txs  = data.get("result", [])
                    if isinstance(txs, list) and txs:
                        block = int(txs[0].get("blockNumber", 0))
                        if block > 0:
                            return block

            log.warning("All block number methods failed")
            return None
        except Exception as e:
            log.warning(f"Block number error: {e}")
            return None
