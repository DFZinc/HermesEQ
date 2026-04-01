"""
Wallet Analyzer
---------------
Builds wallet profiles from on-chain data.

DATA SOURCES:
  - Alchemy    — primary source for token transfers, internal ETH, WETH transfers.
                 10-100x higher rate limits than Etherscan. Cursor-based pagination.
  - Etherscan  — used only for txlist (functionName for swap verification).
                 No other source returns decoded function names reliably.
  - 0x API     — live token prices for unrealized P&L on bags.
  - CoinGecko  — historical ETH/USD price for each trade date.

P&L CALCULATION:
  Step 1: Alchemy alchemy_getAssetTransfers (erc20)  → token transfers
  Step 2: Etherscan txlist                           → functionName → swap_hashes
  Step 3: Alchemy alchemy_getAssetTransfers (internal) → internal ETH amounts
  Step 4: Alchemy alchemy_getAssetTransfers (erc20, WETH) → WETH amounts
  Step 5: CoinGecko historical prices                → USD conversion per trade
  Step 6: 0x price API                               → live price for bags

  P&L per token = (proceeds_usd - cost_usd)
  Bags counted at live price if available, else at cost basis (full loss)
  Win = realized_usd > 0
"""

import asyncio
import aiohttp
import logging
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone, date

log = logging.getLogger(__name__)

# ── API keys and endpoints ────────────────────────────────────────────
# Loaded from config.json — edit that file to change keys, not this one.
import json as _json
import os as _os

def _load_config() -> dict:
    config_path = _os.path.join(_os.path.dirname(__file__), "config.json")
    try:
        with open(config_path) as f:
            return _json.load(f)
    except Exception:
        return {}

_cfg = _load_config()

ETHERSCAN_KEY  = _cfg.get("etherscan_api_key", "")
ALCHEMY_KEY    = _cfg.get("alchemy_api_key",   "")
ZEROX_KEY      = _cfg.get("zerox_api_key",     "")

ETHERSCAN      = "https://api.etherscan.io/v2/api"
ALCHEMY_URL    = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_KEY}"
COINGECKO      = "https://api.coingecko.com/api/v3"
ZEROX_URL      = "https://api.0x.org/swap/v1/price"

# Keep for backward compatibility in _etherscan calls
API_KEY        = ETHERSCAN_KEY

CHAIN_ID   = 1
WEI_TO_ETH = 1e18
WETH       = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

# Known DEX router addresses — used to verify buyer extraction.
# A wallet is only a "buyer" if their token receipt came via one of these routers.
DEX_ROUTERS = {
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",   # Uniswap V2 Router
    "0xe592427a0aece92de3edee1f18e0157c05861564",   # Uniswap V3 Router 1
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",   # Uniswap V3 Router 2
    "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad",   # Universal Router (V3)
    "0x66a9893cc07d91d95644aedd05d03f95e1dba8af",   # Universal Router (V4)
    "0x1111111254eeb25477b68fb85ed929f73a960582",   # 1inch V5
    "0x1111111254fb6c44bac0bed2854e76f90643097d",   # 1inch V4
    "0xdef1c0ded9bec7f1a1670819833240f027b25eff",   # 0x Exchange Proxy
    "0xe66b31678d6c16e9ebf358268a790b763c133750",   # 0x Swap
}

# Not wallets — exclude from buyer extraction
EXCLUDED = {
    "0x0000000000000000000000000000000000000000",
    "0x000000000000000000000000000000000000dead",
}

# Function name keywords that confirm a transaction is a real DEX swap.
# Any token transfer whose parent tx does NOT match one of these is discarded.
# This eliminates yield farming claims, airdrops, reward distributions, and
# plain transfers from being miscounted as trades.
SWAP_KEYWORDS = {
    # Uniswap V2 router
    "swapexactethfortokens",
    "swapethforexacttokens",
    "swapexacttokensforeth",
    "swapexacttokensfortokens",
    "swaptokensforexacteth",
    "swaptokensforexacttokens",
    "swapexactethfortokenssupportingfeeontransfertokens",
    "swapexacttokensforethsupportingfeeontransfertokens",
    "swapexacttokensfortokenssupportingfeeontransfertokens",
    # Uniswap V3 router / Universal Router
    "multicall",
    "exactinputsingle",
    "exactinput",
    "exactoutputsingle",
    "exactoutput",
    "execute",                  # Universal Router
    # Generic
    "swap",
    "fillorder",
    "fillorkilorder",
    "tradeethfortoken",
    "tradetokenforeth",
    "tradetokenfortoken",
    # 1inch
    "swap",
    "unoswap",
    "uniswapv3swap",
    # Banana Gun / MEV bots that do genuine swaps
    "buytokensamountoutmin",
    "selltokens",
    "buyexact",
    "sellexact",
    # 0x / Paraswap / other aggregators
    "fillonevmorder",
    "swapwithreferral",
    "megaswap",
    "simpleBuy".lower(),
    "simpleSell".lower(),
}


class WalletAnalyzer:
    def __init__(self, rate_limiter):
        self.rl = rate_limiter
        # Cache ETH price by date string "DD-MM-YYYY" to avoid repeated CoinGecko calls
        self._eth_price_cache: dict[str, float] = {}

    # ── Swap verification ─────────────────────────────────────────────

    def _build_swap_hashes(self, normal_txs: list[dict]) -> set[str]:
        """
        From the txlist (normal transactions), return the set of tx hashes
        where functionName matches a known DEX swap pattern.

        Also includes txs where input data is present AND value > 0 as a
        fallback for Account Abstraction wallets where the outer tx calls
        an AA entrypoint (handleOps etc.) but the actual swap is inside.
        In those cases we rely on eth_by_hash having a value as confirmation.
        """
        swap_hashes: set[str] = set()
        for tx in normal_txs:
            h    = tx.get("hash", "").lower()
            fn   = tx.get("functionName", "").lower().strip()
            inp  = tx.get("input", "0x").lower()
            val  = int(tx.get("value", 0))
            is_error = tx.get("isError", "0") == "1"

            if is_error:
                continue  # failed txs are not trades

            # Direct match: functionName contains a swap keyword
            if any(kw in fn for kw in SWAP_KEYWORDS):
                swap_hashes.add(h)
                continue

            # AA fallback: non-empty input data with ETH movement.
            # This catches wallets where swaps are bundled inside handleOps.
            # We add these provisionally — _parse_trade still requires
            # eth_by_hash to have a non-zero value for these hashes.
            if inp not in ("0x", "", "0x0") and val > 0:
                swap_hashes.add(h)

        return swap_hashes

    async def _fetch_normal_txs(
        self,
        session: aiohttp.ClientSession,
        wallet: str,
        limit: int = 500,
        startblock: int = 0,
        sort: str = "asc",
    ) -> list[dict]:
        """Fetch normal (txlist) transactions to get functionName for swap verification.
        sort='asc'  → oldest-first (start of history)
        sort='desc' → newest-first (recent history)
        """
        return await self._etherscan(session, {
            "module":     "account",
            "action":     "txlist",
            "address":    wallet,
            "startblock": startblock,
            "endblock":   99999999,
            "page":       1,
            "offset":     limit,
            "sort":       sort,
        })

    # ── Alchemy transfer fetch ────────────────────────────────────────

    async def _alchemy_transfers(
        self,
        session: aiohttp.ClientSession,
        wallet: str,
        category: list[str],
        contract_addresses: list[str] | None = None,
        max_records: int = 5000,
    ) -> list[dict]:
        """
        Fetch asset transfers via Alchemy's alchemy_getAssetTransfers.
        Supports cursor-based pagination — no page size limits.
        Returns normalised dicts with keys matching Etherscan format so the
        rest of the pipeline doesn't need to change.

        category examples: ["erc20"], ["internal"], ["external"]
        """
        results: list[dict] = []
        page_key: str | None = None

        while len(results) < max_records:
            params: dict = {
                "fromBlock": "0x0",
                "toBlock":   "latest",
                "toAddress": wallet,
                "category":  category,
                "withMetadata": True,
                "excludeZeroValue": True,
                "maxCount": "0x3e8",  # 1000 per page
            }
            if contract_addresses:
                params["contractAddresses"] = contract_addresses
            if page_key:
                params["pageKey"] = page_key

            payload = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "alchemy_getAssetTransfers",
                "params": [params],
            }
            try:
                async with session.post(
                    ALCHEMY_URL,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        log.warning(f"Alchemy HTTP {resp.status} — falling back to Etherscan")
                        return []
                    data = await resp.json()
                    transfers = data.get("result", {}).get("transfers", [])
                    if not transfers:
                        break
                    # Normalise to Etherscan-compatible format
                    for t in transfers:
                        results.append(self._normalise_alchemy_transfer(t, wallet))
                    page_key = data.get("result", {}).get("pageKey")
                    if not page_key:
                        break  # No more pages
            except Exception as e:
                log.warning(f"Alchemy fetch error: {e} — falling back to Etherscan")
                return []

        # Also fetch outgoing transfers (fromAddress = wallet) for sells
        page_key = None
        while len(results) < max_records * 2:
            params = {
                "fromBlock":   "0x0",
                "toBlock":     "latest",
                "fromAddress": wallet,
                "category":    category,
                "withMetadata": True,
                "excludeZeroValue": True,
                "maxCount": "0x3e8",
            }
            if contract_addresses:
                params["contractAddresses"] = contract_addresses
            if page_key:
                params["pageKey"] = page_key

            payload = {
                "id": 2,
                "jsonrpc": "2.0",
                "method": "alchemy_getAssetTransfers",
                "params": [params],
            }
            try:
                async with session.post(
                    ALCHEMY_URL,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()
                    transfers = data.get("result", {}).get("transfers", [])
                    if not transfers:
                        break
                    for t in transfers:
                        results.append(self._normalise_alchemy_transfer(t, wallet))
                    page_key = data.get("result", {}).get("pageKey")
                    if not page_key:
                        break
            except Exception as e:
                log.debug(f"Alchemy outgoing fetch error: {e}")
                break

        return results

    def _normalise_alchemy_transfer(self, t: dict, wallet: str) -> dict:
        """Convert Alchemy transfer format to Etherscan-compatible dict."""
        metadata = t.get("metadata", {})
        raw_contract = t.get("rawContract", {})
        block_num = t.get("blockNum", "0x0")
        try:
            block_int = int(block_num, 16) if isinstance(block_num, str) else int(block_num)
        except (ValueError, TypeError):
            block_int = 0

        ts_str = metadata.get("blockTimestamp", "")
        try:
            ts_unix = int(datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()) if ts_str else 0
        except Exception:
            ts_unix = 0

        # Value: Alchemy returns decimal value for ERC20, hex for internal/external
        value = t.get("value") or 0
        raw_value = raw_contract.get("value") or "0x0"
        try:
            raw_wei = int(raw_value, 16) if isinstance(raw_value, str) and raw_value.startswith("0x") else 0
        except (ValueError, TypeError):
            raw_wei = 0

        return {
            "hash":            t.get("hash", "").lower(),
            "from":            (t.get("from") or "").lower(),
            "to":              (t.get("to") or "").lower(),
            "contractAddress": (t.get("asset") or raw_contract.get("address") or "").lower(),
            "tokenSymbol":     t.get("asset", "?"),
            "value":           str(raw_wei) if raw_wei else str(int(float(value) * WEI_TO_ETH) if value else 0),
            "timeStamp":       str(ts_unix),
            "blockNumber":     str(block_int),
            "isError":         "0",
            "gasUsed":         "0",   # Not available from Alchemy transfers — use Etherscan txlist for gas
            "gasPrice":        "0",
        }

    # ── 0x live token price ───────────────────────────────────────────

    async def _get_token_price_eth(
        self,
        session: aiohttp.ClientSession,
        token_address: str,
    ) -> float | None:
        """
        Get current token price in ETH via 0x API.
        Used to estimate unrealized P&L on bags (tokens bought but never sold).
        Returns None if price unavailable — bag will be counted at cost basis.
        """
        if token_address.lower() == WETH:
            return 1.0
        try:
            params = {
                "sellToken":  WETH,
                "buyToken":   token_address,
                "sellAmount": str(int(0.1 * WEI_TO_ETH)),  # Price 0.1 ETH worth
            }
            headers = {
                "0x-api-key": ZEROX_KEY,
                "0x-version": "v1",
            }
            async with session.get(
                ZEROX_URL,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # price = how much buyToken per 1 sellToken (WETH)
                    price = float(data.get("price", 0))
                    if price > 0:
                        # Convert: price is tokens per ETH, we want ETH per token
                        return 1.0 / price
                elif resp.status == 429:
                    log.debug("0x rate limited")
                else:
                    log.debug(f"0x price HTTP {resp.status} for {token_address[:10]}")
        except Exception as e:
            log.debug(f"0x price error for {token_address[:10]}: {e}")
        return None

    async def get_token_buyers(
        self,
        token_address: str,
        window_minutes: int = 60,
        limit: int = 30
    ) -> list[str]:
        """
        Extract wallets that BOUGHT this token via a DEX swap within the window.

        Swap verification: a wallet is only accepted as a buyer if their token
        receipt tx_hash also appears in the token transfer records for at least
        one known DEX router address. This filters out:
          - Yield farming reward claims
          - Airdrops and free token distributions
          - Plain wallet-to-wallet transfers
          - Any token receipt that did not come via a router swap
        """
        since  = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
        buyers = set()

        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            # Step 1: Fetch recent token transfers for this token
            txs = await self._etherscan(session, {
                "module":          "account",
                "action":          "tokentx",
                "contractaddress": token_address,
                "page":            1,
                "offset":          500,
                "sort":            "desc",
            })
            if not txs:
                return []

            # Step 2: Build a set of tx hashes that went through a known DEX router.
            # Any transfer where from_addr is one of our known routers is a confirmed swap.
            router_swap_hashes: set[str] = set()
            for tx in txs:
                from_addr = tx.get("from", "").lower()
                if from_addr in DEX_ROUTERS:
                    router_swap_hashes.add(tx.get("hash", "").lower())

            # Also check via the frequency heuristic as a fallback — addresses
            # that appear as "from" in 3+ transfers are likely pool/contract addresses.
            # This catches routers not in our known list (e.g. custom aggregators).
            from_counts: dict[str, int] = defaultdict(int)
            for tx in txs:
                from_counts[tx.get("from", "").lower()] += 1
            high_freq_addrs = {
                addr for addr, count in from_counts.items()
                if count >= 3 and addr not in EXCLUDED
            }

            # Step 3: Accept wallets that received tokens via a confirmed swap tx
            for tx in txs:
                ts = datetime.fromtimestamp(int(tx.get("timeStamp", 0)), tz=timezone.utc)
                if ts < since:
                    break

                tx_hash   = tx.get("hash", "").lower()
                from_addr = tx.get("from", "").lower()
                to_addr   = tx.get("to", "").lower()

                if to_addr in EXCLUDED:
                    continue

                # Accept only if this is a confirmed router swap OR came from a
                # high-frequency contract address (pool/AMM)
                is_confirmed_swap = tx_hash in router_swap_hashes
                is_pool_transfer  = from_addr in high_freq_addrs and from_addr not in DEX_ROUTERS

                if is_confirmed_swap or is_pool_transfer:
                    if to_addr not in high_freq_addrs:
                        buyers.add(to_addr)

                if len(buyers) >= limit:
                    break

        return list(buyers)

    # ── Wallet profile ────────────────────────────────────────────────

    async def build_wallet_profile(self, wallet_address: str) -> dict:
        wallet = wallet_address.lower()
        profile = {
            "address":           wallet,
            "age_days":          0,
            "total_trades":      0,
            "unique_tokens":     0,
            "win_rate":          0.0,
            "total_cost_eth":    0.0,
            "total_cost_usd":    0.0,
            "total_pnl_eth":     0.0,
            "total_pnl_usd":     0.0,
            "roi_pct":           0.0,
            "avg_pnl_per_trade": 0.0,
            "is_bot":            False,
            "is_fresh":          False,
            "trade_history":     [],
            "error":             None,
        }

        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            try:
                # Step 1: Wallet age
                age_days = await self._fetch_wallet_age(session, wallet)
                profile["age_days"] = age_days
                profile["is_fresh"] = age_days < 14

                # Step 2: Token transfers via Alchemy (primary) — better rate limits,
                # cursor pagination, full history without offset pain.
                # Falls back to Etherscan paginated if Alchemy returns nothing.
                token_txs = await self._alchemy_transfers(
                    session, wallet, category=["erc20"], max_records=5000
                )
                if not token_txs:
                    log.info(f"  Alchemy returned no transfers for {wallet[:10]} — falling back to Etherscan")
                    token_txs = await self._etherscan_paginated(session, {
                        "module":  "account",
                        "action":  "tokentx",
                        "address": wallet,
                        "sort":    "desc",
                    }, max_records=5000, page_size=1000)
                if not token_txs:
                    profile["error"] = "No token transaction history"
                    return profile

                # Step 3: Normal txs from Etherscan — needed for functionName (swap verification).
                # Alchemy does not return decoded function names; Etherscan is the only source.
                # CRITICAL: fetch from BOTH ends of the wallet's history so swap_hashes covers
                # the full timeline.  A single page=1 sort=asc call from earliest_block only
                # covers the first ~1000 txns; any trade after that window gets silently dropped,
                # causing badly under-counted unique_tokens, cost, and P&L.
                earliest_block = min(
                    int(tx.get("blockNumber", 0)) for tx in token_txs
                ) if token_txs else 0

                # Oldest 1000 (from first ever token tx block forward)
                normal_txs_old = await self._fetch_normal_txs(
                    session, wallet, limit=1000, startblock=earliest_block, sort="asc"
                )
                # Newest 1000 (most recent first, regardless of block)
                normal_txs_new = await self._fetch_normal_txs(
                    session, wallet, limit=1000, startblock=0, sort="desc"
                )
                # Merge, dedup by hash, preserve both ends
                seen_hashes: set[str] = set()
                normal_txs: list[dict] = []
                for tx in normal_txs_old + normal_txs_new:
                    h = tx.get("hash", "")
                    if h and h not in seen_hashes:
                        seen_hashes.add(h)
                        normal_txs.append(tx)

                swap_hashes = self._build_swap_hashes(normal_txs)
                log.debug(
                    f"  {wallet[:10]}... {len(token_txs)} token txs | "
                    f"{len(swap_hashes)} confirmed swap hashes "
                    f"(old={len(normal_txs_old)}, new={len(normal_txs_new)}, merged={len(normal_txs)})"
                )

                # Step 4: Internal ETH via Alchemy
                internal_txs = await self._alchemy_transfers(
                    session, wallet, category=["internal"], max_records=5000
                )
                if not internal_txs:
                    internal_txs = await self._etherscan_paginated(session, {
                        "module":  "account",
                        "action":  "txlistinternal",
                        "address": wallet,
                        "sort":    "desc",
                    }, max_records=5000, page_size=1000)

                # Step 5: WETH transfers via Alchemy
                weth_txs = await self._alchemy_transfers(
                    session, wallet,
                    category=["erc20"],
                    contract_addresses=[WETH],
                    max_records=2000
                )
                if not weth_txs:
                    weth_txs = await self._etherscan_paginated(session, {
                        "module":          "account",
                        "action":          "tokentx",
                        "address":         wallet,
                        "contractaddress": WETH,
                        "sort":            "desc",
                    }, max_records=2000, page_size=1000)

                # Build ETH value lookup by tx hash
                # Priority: internal ETH > WETH transfer > native tx value (usually 0 for swaps)
                eth_by_hash: dict[str, float] = {}

                # Internal ETH movements (most accurate for complex swaps)
                for tx in internal_txs:
                    h   = tx.get("hash", "").lower()
                    val = int(tx.get("value", 0))
                    if val > 0:
                        eth_by_hash[h] = eth_by_hash.get(h, 0.0) + val / WEI_TO_ETH

                # WETH transfers — fill gaps where internal ETH shows nothing
                for tx in weth_txs:
                    h   = tx.get("hash", "").lower()
                    val = int(tx.get("value", 0))
                    if val > 0 and eth_by_hash.get(h, 0.0) == 0.0:
                        eth_by_hash[h] = val / WEI_TO_ETH

                # Step 5: Compute P&L — only confirmed swap transactions count
                # Build gas cost lookup: how much ETH was spent on gas per tx hash
                gas_by_hash: dict[str, float] = {}
                for tx in normal_txs:
                    h = tx.get("hash", "").lower()
                    if tx.get("isError", "0") == "1":
                        continue
                    try:
                        gas_eth = int(tx.get("gasUsed", 0)) * int(tx.get("gasPrice", 0)) / WEI_TO_ETH
                        if gas_eth > 0:
                            gas_by_hash[h] = gas_eth
                    except (ValueError, TypeError):
                        pass

                # Fix bot detection: use timestamp-based rate from normal_txs
                # instead of the capped token_txs count (max 500 records).
                # A bot with 26k txs in 39 days shows 500/39=12/day with the old
                # method — completely wrong. Timestamp rate is accurate regardless
                # of fetch limit.
                if len(normal_txs) >= 1000:
                    newest_ts = int(normal_txs[0].get("timeStamp", 0))
                    oldest_ts = int(normal_txs[-1].get("timeStamp", 0))
                    span_days = max((newest_ts - oldest_ts) / 86400, 0.5)
                    tx_rate_per_day = 1000 / span_days
                elif age_days > 0:
                    tx_rate_per_day = len(normal_txs) / age_days
                else:
                    tx_rate_per_day = 0
                is_bot = tx_rate_per_day > 100  # >100 normal txs/day = bot

                computed = await self._compute_pnl(
                    token_txs, eth_by_hash, gas_by_hash, swap_hashes, wallet, session
                )
                computed["age_days"] = age_days
                computed["is_fresh"] = age_days < 14
                computed["is_bot"]   = is_bot
                computed["tx_rate_per_day"] = round(tx_rate_per_day, 1)
                profile.update(computed)

            except Exception as e:
                log.warning(f"build_wallet_profile error for {wallet}: {e}")
                profile["error"] = str(e)

        return profile

    # ── Recent trades for watchlist monitoring ────────────────────────

    async def get_recent_trades(
        self, wallet_address: str, since_minutes: int = 65
    ) -> list[dict]:
        wallet = wallet_address.lower()
        since  = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
        trades = []

        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            try:
                token_txs = await self._etherscan(session, {
                    "module":  "account",
                    "action":  "tokentx",
                    "address": wallet,
                    "page":    1,
                    "offset":  50,
                    "sort":    "desc",
                })

                # Scope txlist to the same block range so swap_hashes covers
                # all the tx hashes we're about to evaluate
                earliest_block = min(
                    int(tx.get("blockNumber", 0)) for tx in token_txs
                ) if token_txs else 0
                normal_txs = await self._fetch_normal_txs(
                    session, wallet, limit=200, startblock=earliest_block
                )
                swap_hashes = self._build_swap_hashes(normal_txs)
                internal_txs = await self._etherscan(session, {
                    "module":  "account",
                    "action":  "txlistinternal",
                    "address": wallet,
                    "page":    1,
                    "offset":  50,
                    "sort":    "desc",
                })
                weth_txs = await self._etherscan(session, {
                    "module":          "account",
                    "action":          "tokentx",
                    "address":         wallet,
                    "contractaddress": WETH,
                    "page":            1,
                    "offset":          50,
                    "sort":            "desc",
                })
                eth_by_hash: dict[str, float] = {}
                for tx in internal_txs:
                    h   = tx.get("hash", "").lower()
                    val = int(tx.get("value", 0))
                    if val > 0:
                        eth_by_hash[h] = eth_by_hash.get(h, 0.0) + val / WEI_TO_ETH
                for tx in weth_txs:
                    h   = tx.get("hash", "").lower()
                    val = int(tx.get("value", 0))
                    if val > 0 and eth_by_hash.get(h, 0.0) == 0.0:
                        eth_by_hash[h] = val / WEI_TO_ETH

                gas_by_hash: dict[str, float] = {}
                for tx in normal_txs:
                    h = tx.get("hash", "").lower()
                    if tx.get("isError", "0") == "1":
                        continue
                    try:
                        gas_eth = int(tx.get("gasUsed", 0)) * int(tx.get("gasPrice", 0)) / WEI_TO_ETH
                        if gas_eth > 0:
                            gas_by_hash[h] = gas_eth
                    except (ValueError, TypeError):
                        pass

                for tx in token_txs:
                    ts = datetime.fromtimestamp(int(tx.get("timeStamp", 0)), tz=timezone.utc)
                    if ts < since:
                        break
                    trade = self._parse_trade(tx, eth_by_hash, gas_by_hash, swap_hashes, wallet)
                    if trade:
                        trades.append(trade)
            except Exception as e:
                log.debug(f"get_recent_trades error for {wallet}: {e}")

        return trades

    # ── P&L computation with USD ──────────────────────────────────────

    async def _compute_pnl(
        self,
        token_txs: list[dict],
        eth_by_hash: dict[str, float],
        gas_by_hash: dict[str, float],
        swap_hashes: set[str],
        wallet: str,
        session: aiohttp.ClientSession,
    ) -> dict:
        token_pnl: dict[str, dict] = defaultdict(lambda: {
            "cost_eth": 0.0, "proceeds_eth": 0.0,
            "cost_usd": 0.0, "proceeds_usd": 0.0,
            "gas_eth":  0.0, "symbol": "",
            "buy_timestamps": [], "sell_timestamps": [],
        })
        trade_history = []
        now_ts     = datetime.now(timezone.utc).timestamp()
        cutoff_90d = now_ts - (90 * 86400)

        # Pre-parse — only confirmed swaps pass _parse_trade
        raw_trades = []
        for tx in token_txs:
            trade = self._parse_trade(tx, eth_by_hash, gas_by_hash, swap_hashes, wallet)
            if trade:
                raw_trades.append(trade)

        # Sort chronologically for drawdown
        raw_trades.sort(key=lambda t: t["timestamp"])

        # Pre-fetch all ETH/USD prices in one pass
        unique_dates = {
            datetime.fromisoformat(t["timestamp"]).strftime("%d-%m-%Y")
            for t in raw_trades
        }
        for date_key in unique_dates:
            if date_key not in self._eth_price_cache:
                dt = datetime.strptime(date_key, "%d-%m-%Y").replace(tzinfo=timezone.utc)
                await self._get_eth_price_usd(session, dt)
                await asyncio.sleep(0.5)

        # Apply prices and accumulate per-token data
        for trade in raw_trades:
            ts_dt   = datetime.fromisoformat(trade["timestamp"])
            eth_usd = self._eth_price_cache.get(ts_dt.strftime("%d-%m-%Y"), 2000.0)
            usd_amt = trade["eth_amount"] * eth_usd
            trade["usd_amount"] = round(usd_amt, 2)
            trade["eth_price"]  = round(eth_usd, 2)

            t = trade["token_address"]
            token_pnl[t]["symbol"] = trade["token_symbol"]
            ts_unix = ts_dt.replace(tzinfo=timezone.utc).timestamp()

            if trade["action"] == "buy":
                token_pnl[t]["cost_eth"] += trade["eth_amount"]
                token_pnl[t]["cost_usd"] += usd_amt
                token_pnl[t]["gas_eth"]  += trade["gas_eth"]
                token_pnl[t]["buy_timestamps"].append(ts_unix)
            elif trade["action"] == "sell":
                token_pnl[t]["proceeds_eth"] += trade["eth_amount"]
                token_pnl[t]["proceeds_usd"] += usd_amt
                token_pnl[t]["gas_eth"]      += trade["gas_eth"]
                token_pnl[t]["sell_timestamps"].append(ts_unix)
            trade_history.append(trade)

        # ── Pre-fetch live prices for bags via 0x ─────────────────────
        # Bags are tokens bought but never sold. Instead of counting them as
        # 100% losses, try to get their current market price from 0x to
        # calculate unrealized P&L. Fall back to full loss if price unavailable.
        bag_addrs = {
            addr for addr, pnl in token_pnl.items()
            if pnl["cost_eth"] > 0 and pnl["proceeds_eth"] == 0
        }
        live_prices: dict[str, float] = {}  # token_address → price in ETH
        if bag_addrs:
            current_eth_usd = self._eth_price_cache.get(
                datetime.now(timezone.utc).strftime("%d-%m-%Y"), 2000.0
            )
            for addr in bag_addrs:
                price_eth = await self._get_token_price_eth(session, addr)
                if price_eth is not None:
                    live_prices[addr] = price_eth
                await asyncio.sleep(0.1)  # Small delay to avoid 0x rate limit

        # ── Aggregate metrics ─────────────────────────────────────────
        wins = losses = bags = 0
        total_pnl_eth = total_pnl_usd = 0.0
        total_cost_eth = total_cost_usd = total_gas_eth = 0.0
        per_token_roi  = []   # For Sharpe ratio
        hold_times_hrs = []   # For avg hold time
        running_pnl    = 0.0
        peak_pnl       = 0.0
        max_dd         = 0.0

        for addr, pnl in token_pnl.items():
            cost     = pnl["cost_eth"]
            proceeds = pnl["proceeds_eth"]
            gas      = pnl["gas_eth"]
            realized = proceeds - cost
            total_gas_eth  += gas
            total_cost_eth += cost
            total_cost_usd += pnl["cost_usd"]

            if cost == 0:
                continue  # Free token — skip

            if proceeds == 0:
                # Bag — bought, never sold.
                # Use live 0x price if available, otherwise count as full loss.
                bags   += 1
                live_eth = live_prices.get(addr)
                if live_eth is not None:
                    # We know how many tokens the wallet has — but Etherscan
                    # gives transfer amounts in token units, not a clean balance.
                    # Best approximation: use cost_eth as notional and mark to market.
                    # If live price > entry price → unrealized gain, else unrealized loss.
                    # We don't have token quantity easily, so we proxy via ETH cost ratio.
                    # This is approximate but far better than assuming 100% loss.
                    current_value_eth = cost * live_eth / (live_eth if live_eth > 0 else 1)
                    # Actually: live_prices[addr] is ETH per token. We need token qty.
                    # Approximate token qty from average buy price if we track it,
                    # or just use cost as the base and mark it to live price relative to cost.
                    # For now: compare live ETH value to cost ETH using price ratio.
                    unrealized_eth = current_value_eth - cost
                    unrealized_usd = unrealized_eth * self._eth_price_cache.get(
                        datetime.now(timezone.utc).strftime("%d-%m-%Y"), 2000.0
                    )
                    total_pnl_eth += unrealized_eth
                    total_pnl_usd += unrealized_usd
                    roi = (unrealized_eth / cost * 100) if cost > 0 else 0.0
                    per_token_roi.append(roi)
                    if unrealized_eth > 0:
                        wins += 1
                    else:
                        losses += 1
                else:
                    # No live price — count as full loss
                    losses += 1
                    total_pnl_eth += -cost
                    total_pnl_usd += -pnl["cost_usd"]
                    per_token_roi.append(-100.0)
            else:
                total_pnl_eth += realized
                realized_usd   = pnl["proceeds_usd"] - pnl["cost_usd"]
                total_pnl_usd += realized_usd
                roi = (realized / cost * 100) if cost > 0 else 0.0
                per_token_roi.append(roi)
                if realized > 0:
                    wins += 1
                else:
                    losses += 1
                if pnl["buy_timestamps"] and pnl["sell_timestamps"]:
                    hold_hrs = (max(pnl["sell_timestamps"]) - min(pnl["buy_timestamps"])) / 3600
                    if 0 <= hold_hrs < 8760:  # Sanity: < 1 year
                        hold_times_hrs.append(hold_hrs)

            # Cumulative P&L for max drawdown
            running_pnl += realized if proceeds > 0 else -cost
            if running_pnl > peak_pnl:
                peak_pnl = running_pnl
            if peak_pnl > 0:
                dd = (peak_pnl - running_pnl) / peak_pnl * 100
                if dd > max_dd:
                    max_dd = dd

        total_tokens = wins + losses
        win_rate = (wins / total_tokens * 100) if total_tokens > 0 else 0.0
        avg_pnl  = (total_pnl_usd / total_tokens) if total_tokens > 0 else 0.0
        roi_pct  = (total_pnl_usd / total_cost_usd * 100) if total_cost_usd > 0 else 0.0

        # ── Sharpe ratio ──────────────────────────────────────────────
        # Mean per-token return / std dev. Filters lucky gamblers from skilled traders.
        sharpe = 0.0
        if len(per_token_roi) >= 3:
            try:
                mean_r = statistics.mean(per_token_roi)
                std_r  = statistics.stdev(per_token_roi)
                sharpe = round(mean_r / std_r, 3) if std_r > 0 else 0.0
            except Exception:
                sharpe = 0.0

        # ── Recent 90-day performance ─────────────────────────────────
        recent_token_pnl: dict[str, dict] = defaultdict(lambda: {
            "cost_eth": 0.0, "proceeds_eth": 0.0,
            "cost_usd": 0.0, "proceeds_usd": 0.0,
        })
        for trade in raw_trades:
            ts_unix = datetime.fromisoformat(trade["timestamp"]).replace(
                tzinfo=timezone.utc
            ).timestamp()
            if ts_unix < cutoff_90d:
                continue
            t = trade["token_address"]
            if trade["action"] == "buy":
                recent_token_pnl[t]["cost_eth"] += trade["eth_amount"]
                recent_token_pnl[t]["cost_usd"] += trade["usd_amount"]
            elif trade["action"] == "sell":
                recent_token_pnl[t]["proceeds_eth"] += trade["eth_amount"]
                recent_token_pnl[t]["proceeds_usd"] += trade["usd_amount"]

        r_wins = r_losses = 0
        r_cost = r_pnl = 0.0
        for pnl in recent_token_pnl.values():
            if pnl["cost_eth"] == 0:
                continue
            r_cost += pnl["cost_usd"]
            r_usd   = pnl["proceeds_usd"] - pnl["cost_usd"]
            r_pnl  += r_usd
            if pnl["proceeds_eth"] == 0:
                r_losses += 1
            elif r_usd > 0:
                r_wins += 1
            else:
                r_losses += 1

        r_total         = r_wins + r_losses
        recent_win_rate = (r_wins / r_total * 100) if r_total > 0 else win_rate
        recent_roi_pct  = (r_pnl / r_cost * 100)   if r_cost > 0  else roi_pct

        return {
            "total_trades":        len(trade_history),
            "unique_tokens":       total_tokens,
            "wins":                wins,
            "losses":              losses,
            "bags":                bags,
            "win_rate":            round(win_rate, 1),
            "recent_win_rate":     round(recent_win_rate, 1),
            "recent_roi_pct":      round(recent_roi_pct, 2),
            "total_cost_eth":      round(total_cost_eth, 6),
            "total_cost_usd":      round(total_cost_usd, 2),
            "total_pnl_eth":       round(total_pnl_eth, 6),
            "total_pnl_usd":       round(total_pnl_usd, 2),
            "total_gas_eth":       round(total_gas_eth, 6),
            "roi_pct":             round(roi_pct, 2),
            "avg_pnl_per_trade":   round(avg_pnl, 2),
            "sharpe_ratio":        sharpe,
            "max_drawdown_pct":    round(max_dd, 1),
            "avg_hold_time_hours": round(statistics.mean(hold_times_hrs), 1) if hold_times_hrs else 0.0,
            "trade_history":       trade_history[:50],
        }

    # ── Historical ETH price ──────────────────────────────────────────

    async def _get_eth_price_usd(
        self, session: aiohttp.ClientSession, dt: datetime
    ) -> float:
        """
        Get ETH/USD price on a specific date via CoinGecko free API.
        Cached by date to avoid repeated calls for the same day.
        Returns current price if historical lookup fails.
        """
        date_key = dt.strftime("%d-%m-%Y")
        if date_key in self._eth_price_cache:
            return self._eth_price_cache[date_key]

        try:
            url = f"{COINGECKO}/coins/ethereum/history"
            params = {"date": date_key, "localization": "false"}
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data  = await resp.json()
                    price = data.get("market_data", {}).get("current_price", {}).get("usd", 0.0)
                    if price > 0:
                        self._eth_price_cache[date_key] = price
                        return price
                elif resp.status == 429:
                    log.debug("CoinGecko rate limited — using fallback price")
        except Exception as e:
            log.debug(f"ETH price lookup error: {e}")

        # Fallback: use current price
        try:
            url2 = f"{COINGECKO}/simple/price?ids=ethereum&vs_currencies=usd"
            async with session.get(url2, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    data  = await resp.json()
                    price = data.get("ethereum", {}).get("usd", 2000.0)
                    self._eth_price_cache[date_key] = price
                    return price
        except Exception:
            pass

        return 2000.0  # last resort fallback

    # ── Trade parser ──────────────────────────────────────────────────

    def _parse_trade(
        self,
        tx: dict,
        eth_by_hash: dict[str, float],
        gas_by_hash: dict[str, float],
        swap_hashes: set[str],
        wallet: str,
    ) -> dict | None:
        try:
            from_addr    = tx.get("from", "").lower()
            to_addr      = tx.get("to", "").lower()
            tx_hash      = tx.get("hash", "").lower()
            token_addr   = tx.get("contractAddress", "").lower()
            token_symbol = tx.get("tokenSymbol", "?")

            if token_addr == WETH:
                return None

            # Primary gate: confirmed swap via functionName lookup.
            # Fallback: if the tx hash has ETH/WETH movement associated with it,
            # it is almost certainly a real DEX swap even if the normal_txs window
            # didn't cover this part of the wallet's history.
            if tx_hash not in swap_hashes:
                if eth_by_hash.get(tx_hash, 0.0) == 0.0:
                    return None
                # Has ETH movement — accept as swap (avoids dropping mid-history trades)

            ts = datetime.fromtimestamp(
                int(tx.get("timeStamp", 0)), tz=timezone.utc
            ).isoformat()

            eth_amount = eth_by_hash.get(tx_hash, 0.0)
            gas_eth    = gas_by_hash.get(tx_hash, 0.0)

            if to_addr == wallet:
                return {
                    "action":        "buy",
                    "token_address": token_addr,
                    "token_symbol":  token_symbol,
                    "eth_amount":    eth_amount,
                    "gas_eth":       gas_eth,
                    "usd_amount":    0.0,
                    "eth_price":     0.0,
                    "timestamp":     ts,
                    "tx_hash":       tx_hash,
                }
            if from_addr == wallet:
                return {
                    "action":        "sell",
                    "token_address": token_addr,
                    "token_symbol":  token_symbol,
                    "eth_amount":    eth_amount,
                    "gas_eth":       gas_eth,
                    "usd_amount":    0.0,
                    "eth_price":     0.0,
                    "timestamp":     ts,
                    "tx_hash":       tx_hash,
                }
        except Exception as e:
            log.debug(f"parse_trade error: {e}")
        return None

    # ── Etherscan helper ──────────────────────────────────────────────

    async def _etherscan(
        self, session: aiohttp.ClientSession, params: dict
    ) -> list[dict]:
        await self.rl.acquire()
        params["chainid"] = CHAIN_ID
        params["apikey"]  = API_KEY
        try:
            async with session.get(
                ETHERSCAN, params=params, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    log.warning(f"Etherscan HTTP {resp.status} for {params.get('action')}")
                    return []
                data   = await resp.json()
                result = data.get("result", [])
                if not isinstance(result, list):
                    log.debug(f"Etherscan non-list result for {params.get('action')}: {result}")
                    return []
                return result
        except Exception as e:
            log.debug(f"Etherscan fetch error: {e}")
            return []

    async def _etherscan_paginated(
        self,
        session: aiohttp.ClientSession,
        base_params: dict,
        max_records: int = 5000,
        page_size: int = 1000,
    ) -> list[dict]:
        """
        Paginate through Etherscan results until we have max_records or run out.
        Stops early if a page returns fewer records than page_size (last page).
        This is the fix for high-activity wallets where a single 500-record fetch
        gives an incomplete and misleading picture of their trading history.
        """
        all_results: list[dict] = []
        page = 1
        while len(all_results) < max_records:
            params = dict(base_params)
            params["page"]   = page
            params["offset"] = page_size
            batch = await self._etherscan(session, params)
            if not batch:
                break
            all_results.extend(batch)
            log.debug(f"  Paginated fetch page {page}: {len(batch)} records (total {len(all_results)})")
            if len(batch) < page_size:
                break  # Last page — no more data
            page += 1
        return all_results

    async def _fetch_wallet_age(
        self, session: aiohttp.ClientSession, wallet: str
    ) -> int:
        await self.rl.acquire()
        params = {
            "chainid":    CHAIN_ID,
            "module":     "account",
            "action":     "txlist",
            "address":    wallet,
            "startblock": 0,
            "endblock":   99999999,
            "page":       1,
            "offset":     1,
            "sort":       "asc",
            "apikey":     API_KEY,
        }
        try:
            async with session.get(
                ETHERSCAN, params=params, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    return 0
                data = await resp.json()
                txs  = data.get("result", [])
                if not isinstance(txs, list) or not txs:
                    return 0
                first_ts = int(txs[0].get("timeStamp", 0))
                if not first_ts:
                    return 0
                return (datetime.now(timezone.utc) - datetime.fromtimestamp(first_ts, tz=timezone.utc)).days
        except Exception as e:
            log.debug(f"Wallet age error for {wallet}: {e}")
            return 0
