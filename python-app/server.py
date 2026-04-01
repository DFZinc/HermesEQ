"""
server.py
---------
FastAPI backend for the ETH Wallet Intelligence Dashboard.

Serves:
  - REST API endpoints for all agent data
  - WebSocket for live agent log streaming
  - Agent start/stop/status control
  - Static frontend files

Run:
    pip install fastapi uvicorn
    python server.py

Then open: http://localhost:8000
"""

import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.requests import Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="WalletEQ Agent")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── File paths ────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).parent
WATCHLIST_FILE = BASE_DIR / "watchlist.json"
STATIC_DIR     = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ── Agent process management ──────────────────────────────────────────
agent_process: Optional[asyncio.subprocess.Process] = None
agent_logs: list[dict] = []
MAX_LOGS = 500
log_clients: list[WebSocket] = []


def load_json(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {}


def get_cache():
    """Return a Cache instance backed by SQLite. Lightweight — reuses existing DB."""
    from cache import Cache
    return Cache()


# ── REST API ──────────────────────────────────────────────────────────

@app.get("/api/status")
async def get_status():
    global agent_process
    running = agent_process is not None and agent_process.returncode is None
    wl      = load_json(WATCHLIST_FILE)
    cache   = get_cache()
    return {
        "agent_running":   running,
        "watchlist_count": len(wl),
        "wallet_cache":    cache.wallet_count(),
        "token_cache":     cache.token_count(),
        "timestamp":       datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/watchlist")
async def get_watchlist():
    data = load_json(WATCHLIST_FILE)
    wallets = []
    for addr, entry in data.items():
        p = entry.get("profile", {})
        s = entry.get("score", {})
        wallets.append({
            "address":        addr,
            "short":          addr[:6] + "..." + addr[-4:],
            "found_on":       entry.get("found_on", ""),
            "found_at":       entry.get("found_at", "")[:10],
            "age_days":       p.get("age_days", 0),
            "score":          s.get("total", 0),
            "verdict":        s.get("verdict", ""),
            "path":           s.get("path", ""),
            "win_rate":       p.get("win_rate", 0),
            "unique_tokens":  p.get("unique_tokens", 0),
            "total_trades":   p.get("total_trades", 0),
            "cost_eth":       p.get("total_cost_eth", 0),
            "pnl_eth":        p.get("total_pnl_eth", 0),
            "roi_pct":        p.get("roi_pct", 0),
            "is_bot":         p.get("is_bot", False),
            "activity_count": len(entry.get("activity", [])),
            "recent_activity": entry.get("activity", [])[-10:],
            "disabled":        entry.get("disabled", False),
        })
    wallets.sort(key=lambda x: (not x["disabled"], x["score"]), reverse=True)
    return wallets


@app.get("/api/wallet/{address}")
async def get_wallet(address: str):
    data = load_json(WATCHLIST_FILE)
    entry = data.get(address.lower())
    if not entry:
        # Try SQLite wallet cache
        cache = get_cache()
        entry = cache.get_wallet(address.lower())
        if not entry:
            return JSONResponse(status_code=404, content={"error": "Wallet not found"})
        return entry
    return entry


@app.get("/api/activity")
async def get_activity(limit: int = 50):
    data    = load_json(WATCHLIST_FILE)
    all_activity = []
    for addr, entry in data.items():
        short = addr[:6] + "..." + addr[-4:]
        for trade in entry.get("activity", []):
            all_activity.append({
                "wallet":       addr,
                "short":        short,
                "action":       trade.get("action", ""),
                "token_symbol": trade.get("token_symbol", ""),
                "eth_amount":   trade.get("eth_amount", 0),
                "timestamp":    trade.get("timestamp", ""),
                "tx_hash":      trade.get("tx_hash", ""),
            })
    all_activity.sort(key=lambda x: x["timestamp"], reverse=True)
    return all_activity[:limit]


@app.get("/api/tokens")
async def get_tokens():
    from datetime import datetime, timezone, timedelta
    cache  = get_cache()
    tc     = cache.get_all_tokens()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    active = []
    history = []
    for addr, entry in tc.items():
        item = {
            "address":      addr,
            "short":        addr[:6] + "..." + addr[-4:],
            "symbol":       entry.get("symbol", ""),
            "price_change": entry.get("price_change", None),
            "peak_volume":  entry.get("peak_volume", 0.0),
            "first_seen":   entry.get("first_seen", entry.get("scanned_at", "")),
            "last_seen":    entry.get("last_seen",  entry.get("scanned_at", "")),
            "scanned_at":   entry.get("scanned_at", ""),
            "disabled":     entry.get("disabled", False),
        }
        try:
            last = datetime.fromisoformat(item["last_seen"])
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            if last >= cutoff:
                active.append(item)
            else:
                history.append(item)
        except Exception:
            history.append(item)
    active.sort(key=lambda x: x["last_seen"], reverse=True)
    history.sort(key=lambda x: x["last_seen"], reverse=True)
    return {"active": active, "history": history}


@app.get("/api/pnl_chart")
async def get_pnl_chart():
    """Returns wallet P&L data formatted for charting."""
    data   = load_json(WATCHLIST_FILE)
    labels = []
    pnl    = []
    roi    = []
    costs  = []
    for addr, entry in data.items():
        if entry.get("disabled", False):
            continue
        p = entry.get("profile", {})
        s = entry.get("score", {})
        labels.append(addr[:6] + "..." + addr[-4:])
        pnl.append(round(p.get("total_pnl_eth", 0), 4))
        roi.append(round(p.get("roi_pct", 0), 2))
        costs.append(round(p.get("total_cost_eth", 0), 4))
    return {"labels": labels, "pnl": pnl, "roi": roi, "costs": costs}


@app.get("/api/logs")
async def get_logs():
    return agent_logs[-200:]


# ── Manual wallet scan ───────────────────────────────────────────────

@app.post("/api/wallet/scan")
async def manual_wallet_scan(req: Request):
    """
    Manually scan a single wallet address through the full analyzer and scorer.
    Returns the profile and score. If score qualifies, adds to watchlist.

    Pass {"address": "0x...", "force": true} to bypass the cache and re-analyze.
    The cache is also bypassed automatically when it contains bad/empty data
    (age_days=0, no trades, or an error field set).
    """
    from rate_limiter    import RateLimiter
    from wallet_analyzer import WalletAnalyzer
    from wallet_scorer   import WalletScorer
    from watchlist       import Watchlist
    from cache           import Cache
    from datetime        import datetime, timezone

    try:
        body = await req.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON body"})

    address = body.get("address", "").strip().lower()
    force   = bool(body.get("force", False))

    if not address or not address.startswith("0x") or len(address) != 42:
        return JSONResponse(status_code=400, content={"error": "Invalid wallet address"})

    rl        = RateLimiter(calls_per_second=5)
    analyzer  = WalletAnalyzer(rl)
    scorer    = WalletScorer()
    cache     = Cache()
    watchlist = Watchlist()

    def _cache_is_valid(cached: dict) -> bool:
        """Return False if the cached profile is empty/errored and should be re-fetched."""
        if cached is None:
            return False
        p = cached.get("profile", {})
        # Treat as invalid if there was an error, or if core fields are zeroed out
        if p.get("error"):
            return False
        if p.get("age_days", 0) == 0 and p.get("unique_tokens", 0) == 0:
            return False
        return True

    try:
        cached = cache.get_wallet(address) if not force else None
        if cached and _cache_is_valid(cached):
            return {
                "address": address,
                "profile": cached["profile"],
                "score":   cached["score"],
                "cached":  True,
                "added_to_watchlist": False,
            }

        # Fresh analysis — always happens when force=True, cache miss, or bad cache
        profile = await analyzer.build_wallet_profile(address)
        score   = scorer.score(profile)
        cache.save_wallet(address, profile, score)

        added = False
        if score["total"] >= 65:
            added = watchlist.add({
                "address":  address,
                "profile":  profile,
                "score":    score,
                "found_on": "manual",
                "found_at": datetime.now(timezone.utc).isoformat(),
            })

        return {
            "address": address,
            "profile": profile,
            "score":   score,
            "cached":  False,
            "added_to_watchlist": added,
        }
    except Exception as e:
        import traceback, logging
        logging.error(f"manual_wallet_scan error for {address}: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── Manual wallet rescan ─────────────────────────────────────────────

@app.post("/api/wallet/{address}/rescan")
async def rescan_wallet(address: str):
    from rate_limiter    import RateLimiter
    from wallet_analyzer import WalletAnalyzer
    from wallet_scorer   import WalletScorer
    from watchlist       import Watchlist
    from cache           import Cache

    addr     = address.lower()
    rl       = RateLimiter(calls_per_second=5)
    analyzer = WalletAnalyzer(rl)
    scorer   = WalletScorer()
    wl       = Watchlist()
    cache    = Cache()

    profile = await analyzer.build_wallet_profile(addr)
    score   = scorer.score(profile)
    wl.update_profile(addr, profile, score)
    cache.save_wallet(addr, profile, score)

    return {
        "address": addr,
        "score":   score["total"],
        "verdict": score["verdict"],
        "pnl_usd": profile.get("total_pnl_usd", 0),
        "roi_pct": profile.get("roi_pct", 0),
        "win_rate": profile.get("win_rate", 0),
    }


# ── Token disable/enable ─────────────────────────────────────────────

@app.post("/api/token/{address}/disable")
async def disable_token(address: str):
    from cache import Cache
    c = Cache()
    c.disable_token(address.lower())
    return {"status": "disabled", "address": address.lower()}

@app.post("/api/token/{address}/enable")
async def enable_token(address: str):
    from cache import Cache
    c = Cache()
    c.enable_token(address.lower())
    return {"status": "enabled", "address": address.lower()}


# ── Token metadata cache ─────────────────────────────────────────────

TOKEN_META_FILE = BASE_DIR / "token_metadata_cache.json"

def load_token_meta() -> dict:
    if TOKEN_META_FILE.exists():
        try:
            return json.loads(TOKEN_META_FILE.read_text())
        except Exception:
            pass
    return {}

def save_token_meta(data: dict):
    try:
        TOKEN_META_FILE.write_text(json.dumps(data, indent=2))
    except Exception as e:
        logging.warning(f"token meta save error: {e}")

@app.get("/api/token_meta")
async def get_token_meta():
    return load_token_meta()

@app.post("/api/token_meta")
async def save_token_meta_endpoint(req: Request):
    try:
        body = await req.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON"})
    data = load_token_meta()
    data.update(body)
    save_token_meta(data)
    return {"saved": len(body)}


# ── Watchlist controls ───────────────────────────────────────────────

@app.post("/api/watchlist/{address}/disable")
async def disable_wallet(address: str):
    from watchlist import Watchlist
    wl = Watchlist()
    wl.disable(address.lower())
    return {"status": "disabled", "address": address.lower()}

@app.post("/api/watchlist/{address}/enable")
async def enable_wallet(address: str):
    from watchlist import Watchlist
    wl = Watchlist()
    wl.enable(address.lower())
    return {"status": "enabled", "address": address.lower()}

@app.delete("/api/watchlist/{address}")
async def delete_wallet(address: str):
    from watchlist import Watchlist
    wl = Watchlist()
    deleted = wl.delete(address.lower())
    return {"status": "deleted" if deleted else "not_found", "address": address.lower()}


# ── Manual token scan ───────────────────────────────────────────────

@app.post("/api/token/scan")
async def manual_token_scan(req: Request):
    """
    Manually scan a token address — extract buyers and analyze them.
    Identical pipeline to the agent's automatic token processing.
    """
    from rate_limiter    import RateLimiter
    from wallet_analyzer import WalletAnalyzer
    from wallet_scorer   import WalletScorer
    from watchlist       import Watchlist
    from cache           import Cache
    from datetime        import datetime, timezone

    try:
        body = await req.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON body"})

    address = body.get("address", "").strip().lower()
    if not address or not address.startswith("0x") or len(address) != 42:
        return JSONResponse(status_code=400, content={"error": "Invalid token address"})

    rl        = RateLimiter(calls_per_second=5)
    analyzer  = WalletAnalyzer(rl)
    scorer    = WalletScorer()
    cache     = Cache()
    watchlist = Watchlist()

    try:
        # Resolve real token symbol from DexScreener before saving
        real_symbol = f"MANUAL:{address[:6]}"
        try:
            import aiohttp as _aiohttp
            async with _aiohttp.ClientSession() as _session:
                async with _session.get(
                    f"https://api.dexscreener.com/token-pairs/v1/ethereum/{address}",
                    timeout=_aiohttp.ClientTimeout(total=8),
                ) as _resp:
                    if _resp.status == 200:
                        _pairs = await _resp.json()
                        if isinstance(_pairs, list) and _pairs:
                            _sym = _pairs[0].get("baseToken", {}).get("symbol", "")
                            if _sym:
                                real_symbol = _sym
        except Exception:
            pass  # fallback to MANUAL:address

        # Save token to cache so it appears in the scanned tokens panel
        cache.save_token(address, real_symbol, 0.0, 0.0)

        buyers = await analyzer.get_token_buyers(
            token_address=address,
            window_minutes=60,
            limit=30
        )

        if not buyers:
            return {"address": address, "buyers_found": 0, "results": [], "added_to_watchlist": 0}

        results = []
        added_count = 0

        for wallet in buyers:
            if cache.has_wallet(wallet):
                cached  = cache.get_wallet(wallet)
                profile = cached["profile"]
                score   = cached["score"]
                was_cached = True
            else:
                profile    = await analyzer.build_wallet_profile(wallet)
                score      = scorer.score(profile)
                cache.save_wallet(wallet, profile, score)
                was_cached = False

            added = False
            if score["total"] >= 65:
                added = watchlist.add({
                    "address":  wallet,
                    "profile":  profile,
                    "score":    score,
                    "found_on": f"manual:{address[:10]}",
                    "found_at": datetime.now(timezone.utc).isoformat(),
                })
                if added:
                    added_count += 1

            results.append({
                "address":    wallet,
                "age_days":   profile.get("age_days", 0),
                "win_rate":   profile.get("win_rate", 0),
                "pnl_usd":    profile.get("total_pnl_usd", 0),
                "roi_pct":    profile.get("roi_pct", 0),
                "score":      score["total"],
                "verdict":    score["verdict"],
                "disqualify": score.get("disqualify_reason", ""),
                "cached":     was_cached,
                "added":      added,
            })

        results.sort(key=lambda x: x["score"], reverse=True)
        return {
            "address":            address,
            "buyers_found":       len(buyers),
            "results":            results,
            "added_to_watchlist": added_count,
        }
    except Exception as e:
        import traceback, logging
        logging.error(f"manual_token_scan error for {address}: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── Agent control ─────────────────────────────────────────────────────

@app.post("/api/agent/start")
async def start_agent():
    global agent_process, agent_logs
    if agent_process and agent_process.returncode is None:
        return {"status": "already_running"}
    agent_logs = []
    import os as _os
    env = _os.environ.copy()
    # Ensure API key is explicitly passed to the subprocess
    if not env.get("ETHERSCAN_API_KEY"):
        env_file = BASE_DIR / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("ETHERSCAN_API_KEY="):
                    env["ETHERSCAN_API_KEY"] = line.split("=", 1)[1].strip()
                    break
    agent_process = await asyncio.create_subprocess_exec(
        sys.executable, str(BASE_DIR / "agent.py"),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=str(BASE_DIR),
        env=env,
    )
    asyncio.create_task(_stream_agent_output(agent_process))
    return {"status": "started"}


@app.post("/api/agent/stop")
async def stop_agent():
    global agent_process
    if agent_process and agent_process.returncode is None:
        agent_process.terminate()
        try:
            await asyncio.wait_for(agent_process.wait(), timeout=5)
        except asyncio.TimeoutError:
            agent_process.kill()
        agent_process = None
        return {"status": "stopped"}
    return {"status": "not_running"}


async def _stream_agent_output(proc: asyncio.subprocess.Process):
    """Reads agent stdout and broadcasts to WebSocket clients."""
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        text = line.decode("utf-8", errors="replace").rstrip()
        entry = {"text": text, "ts": datetime.now(timezone.utc).isoformat()}
        agent_logs.append(entry)
        if len(agent_logs) > MAX_LOGS:
            agent_logs.pop(0)
        dead = []
        for ws in log_clients:
            try:
                await ws.send_json(entry)
            except Exception:
                dead.append(ws)
        for ws in dead:
            log_clients.remove(ws)


# ── WebSocket ─────────────────────────────────────────────────────────

@app.websocket("/ws/logs")
async def websocket_logs(websocket: WebSocket):
    await websocket.accept()
    log_clients.append(websocket)
    # Send existing logs on connect
    for entry in agent_logs[-100:]:
        await websocket.send_json(entry)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in log_clients:
            log_clients.remove(websocket)


# ── Config read/write ─────────────────────────────────────────────────

CONFIG_FILE = BASE_DIR / "config.json"

CONFIG_DEFAULTS = {
    "etherscan_api_key":     "",
    "alchemy_api_key":       "",
    "zerox_api_key":         "",
    "min_volume_usd_1h":     5000,
    "min_liquidity_usd":     5000,
    "min_txns_1h":           10,
    "min_score":             65,
    "buyers_to_analyze":     30,
    "poll_interval_seconds": 120,
}

def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except Exception:
            pass
    return dict(CONFIG_DEFAULTS)

def save_config(data: dict):
    merged = dict(CONFIG_DEFAULTS)
    merged.update(data)
    CONFIG_FILE.write_text(json.dumps(merged, indent=2))

@app.get("/api/config")
async def get_config():
    cfg = load_config()
    # Mask keys in response — show only last 6 chars so user can confirm which key is set
    masked = dict(cfg)
    for key in ("etherscan_api_key", "alchemy_api_key", "zerox_api_key"):
        val = masked.get(key, "")
        masked[key] = ("*" * (len(val) - 6) + val[-6:]) if len(val) > 6 else val
    return masked

@app.post("/api/config")
async def post_config(request: Request):
    data = await request.json()
    existing = load_config()
    # If an API key field contains ANY asterisk it's the masked display value — preserve existing
    for key in ("etherscan_api_key", "alchemy_api_key", "zerox_api_key"):
        if key in data and "*" in str(data.get(key, "")):
            data[key] = existing.get(key, "")
    existing.update(data)
    save_config(existing)
    return {"status": "saved"}


# ── Frontend ──────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    index = STATIC_DIR / "index.html"
    if index.exists():
        return HTMLResponse(index.read_text(encoding='utf-8'))
    return HTMLResponse("<h1>Frontend not found. Check static/index.html</h1>")


if __name__ == "__main__":
    print("WalletEQ Agent Dashboard")
    print("Open: http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
