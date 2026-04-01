"""
ETH Wallet Intelligence Agent
------------------------------
Monitors ETH token volume → extracts buyers → scores wallets → builds watchlist.
Tracks quality wallets for pattern and narrative analysis.
"""

import asyncio
import json
from pathlib import Path
import sys
import logging
from datetime import datetime, timezone

from rate_limiter  import RateLimiter
from volume_monitor import VolumeMonitor
from wallet_analyzer import WalletAnalyzer
from wallet_scorer   import WalletScorer
from watchlist       import Watchlist
from cache           import Cache

BASE_DIR = Path(__file__).parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


class ETHWalletAgent:
    def __init__(self):
        self.rate_limiter    = RateLimiter(calls_per_second=5)
        self.volume_monitor  = VolumeMonitor(self.rate_limiter)
        self.wallet_analyzer = WalletAnalyzer(self.rate_limiter)
        self.wallet_scorer   = WalletScorer()
        self.watchlist       = Watchlist()
        self.cache           = Cache()

        cfg_path = BASE_DIR / "config.json"
        cfg = {}
        try:
            with open(cfg_path) as f:
                cfg = json.load(f)
        except Exception:
            pass

        self.BUYERS_TO_ANALYZE      = cfg.get("buyers_to_analyze",       30)
        self.MIN_SCORE_TO_WATCHLIST = cfg.get("min_score",                65)
        self.POLL_INTERVAL_SECONDS  = cfg.get("poll_interval_seconds",   120)

    async def run(self):
        log.info("ETH Wallet Agent started.")
        log.info(
            f"Cache: {self.cache.wallet_count()} wallets known, "
            f"{self.cache.token_count()} tokens previously scanned"
        )
        log.info(f"Watchlist: {self.watchlist.count()} wallets tracked")

        while True:
            try:
                await self._cycle()
            except Exception as e:
                log.error(f"Cycle error: {e}")
            await asyncio.sleep(self.POLL_INTERVAL_SECONDS)

    async def _cycle(self):
        # Reload watchlist from disk each cycle so dashboard changes (deletes, disables)
        # are reflected immediately without restarting the agent.
        self.watchlist.reload()

        # Stage 1: Detect active tokens
        active_tokens = await self.volume_monitor.get_active_tokens()
        if not active_tokens:
            log.info("No active tokens detected this cycle.")
            await self._refresh_watchlist()
            return

        # All active tokens are processed every cycle — no token caching gate.
        # Token volume changes every cycle and must always be re-evaluated.
        # Only wallet analysis results are cached (expensive Etherscan calls).
        log.info(f"{len(active_tokens)} active token(s) detected.")

        for token in active_tokens:
            if self.cache.is_token_disabled(token["address"]):
                log.info(f"  Skipping disabled token: {token.get('symbol')} | {token['address']}")
                continue
            await self._process_token(token)

        await self._refresh_watchlist()

    async def _process_token(self, token: dict):
        address = token["address"]
        symbol  = token.get("symbol", address[:8])
        log.info(f"Analyzing: {symbol} | {address}")

        # Mark scanned immediately — store price change for dashboard display
        self.cache.save_token(address, symbol, token.get("price_change_pct_1h", 0.0), token.get("volume_usd_1h", 0.0))

        buyers = await self.wallet_analyzer.get_token_buyers(
            token_address=address,
            window_minutes=60,
            limit=self.BUYERS_TO_ANALYZE
        )

        if not buyers:
            log.info(f"  No buyers found for {symbol}")
            return

        new_buyers    = [w for w in buyers if not self.cache.has_wallet(w)]
        cached_buyers = [w for w in buyers if self.cache.has_wallet(w)]

        log.info(
            f"  {len(buyers)} buyer(s) — "
            f"{len(new_buyers)} new, {len(cached_buyers)} from cache"
        )

        quality_wallets = []

        # Cached wallets — instant, no API calls
        for wallet in cached_buyers:
            cached  = self.cache.get_wallet(wallet)
            profile = cached["profile"]
            score   = cached["score"]
            self._log_wallet(wallet, profile, score, cached=True)
            if score["total"] >= self.MIN_SCORE_TO_WATCHLIST:
                quality_wallets.append(self._entry(wallet, profile, score, symbol))

        # New wallets — full Etherscan analysis
        for wallet in new_buyers:
            profile = await self.wallet_analyzer.build_wallet_profile(wallet)
            score   = self.wallet_scorer.score(profile)
            self.cache.save_wallet(wallet, profile, score)
            self._log_wallet(wallet, profile, score, cached=False)
            if score["total"] >= self.MIN_SCORE_TO_WATCHLIST:
                quality_wallets.append(self._entry(wallet, profile, score, symbol))

        # Volume legitimacy
        ratio   = round(len(quality_wallets) / len(buyers) * 100, 1) if buyers else 0
        verdict = (
            "ORGANIC" if ratio >= 40 else
            "MIXED"   if ratio >= 20 else
            "SUSPICIOUS"
        )
        log.info(f"  Volume verdict for {symbol}: {verdict} ({ratio}% quality wallets)")

        for w in quality_wallets:
            if self.watchlist.add(w):
                log.info(f"  ✅ Watchlist: {w['address']} | Score: {w['score']['total']} | {w['score']['verdict']}")
                # Auto-export spreadsheet
                try:
                    import subprocess
                    subprocess.Popen(
                        [sys.executable, str(BASE_DIR / "export_watchlist.py")],
                        cwd=str(BASE_DIR)
                    )
                    log.info("  📊 Watchlist spreadsheet updated.")
                except Exception as e:
                    log.debug(f"Auto-export error: {e}")

    async def _refresh_watchlist(self):
        wallets = self.watchlist.get_active()  # skip disabled wallets
        if not wallets:
            return
        log.info(f"Refreshing {len(wallets)} watchlisted wallet(s)...")
        new_trade_count = 0
        for wallet in wallets:
            seen_hashes = {
                t.get("tx_hash", "")
                for t in wallet.get("activity", [])
            }
            trades = await self.wallet_analyzer.get_recent_trades(
                wallet_address=wallet["address"],
                since_minutes=self.POLL_INTERVAL_SECONDS // 60 + 5
            )
            new_trades_this_wallet = 0
            for trade in trades:
                if trade.get("tx_hash", "") in seen_hashes:
                    continue
                log.info(
                    f"  👁 {wallet['address']} "
                    f"-> {trade['action'].upper()} {trade['token_symbol']} "
                    f"| {trade['eth_amount']:.4f} ETH"
                )
                self.watchlist.log_activity(wallet["address"], trade)
                seen_hashes.add(trade.get("tx_hash", ""))
                new_trade_count += 1
                new_trades_this_wallet += 1

            # Recalculate profile and score when new trades are detected
            if new_trades_this_wallet > 0:
                old_score = wallet.get("score", {}).get("total", 0)
                new_profile = await self.wallet_analyzer.build_wallet_profile(wallet["address"])
                new_score   = self.wallet_scorer.score(new_profile)
                self.watchlist.update_profile(wallet["address"], new_profile, new_score)
                self.cache.save_wallet(wallet["address"], new_profile, new_score)
                log.info(
                    f"  🔄 Recalculated {wallet['address']} | "
                    f"Score: {old_score} -> {new_score['total']} | "
                    f"P&L: ${new_profile.get('total_pnl_usd', 0):,.0f} | "
                    f"ROI: {new_profile.get('roi_pct', 0):.1f}%"
                )
                if new_score["total"] < self.MIN_SCORE_TO_WATCHLIST and old_score >= self.MIN_SCORE_TO_WATCHLIST:
                    log.info(f"  ⚠️  {wallet['address']} dropped below watchlist threshold — score now {new_score['total']}")

        if new_trade_count == 0:
            log.info("  No new activity.")

    def _log_wallet(self, address: str, profile: dict, score: dict, cached: bool):
        tag = "[CACHE]" if cached else "      "
        reason = score.get("disqualify_reason", "")
        path   = f" [{score['path']}]" if score.get("path") else ""
        log.info(
            f"  {tag} {address}... | "
            f"Age: {profile['age_days']}d | "
            f"Win rate: {profile['win_rate']}% | "
            f"Cost: ${profile.get('total_cost_usd', 0):,.0f} | "
            f"P&L: ${profile.get('total_pnl_usd', 0):,.0f} | "
            f"ROI: {profile.get('roi_pct', 0):.1f}% | "
            f"Score: {score['total']}/100 | "
            f"{score['verdict']}{path}"
            + (f" | {reason}" if reason else "")
        )

    def _entry(self, address, profile, score, symbol) -> dict:
        return {
            "address":  address,
            "profile":  profile,
            "score":    score,
            "found_on": symbol,
            "found_at": datetime.now(timezone.utc).isoformat(),
        }


if __name__ == "__main__":
    agent = ETHWalletAgent()
    asyncio.run(agent.run())
