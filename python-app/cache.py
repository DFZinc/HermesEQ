"""
Cache
-----
Persists analyzed wallets and scanned tokens between runs.
Uses SQLite for indexed row-level reads and writes.

Old JSON approach: every save reserializes the entire file (O(n) per write).
SQLite approach:   every save/read is a single indexed row op (O(1) per write).
At 7000+ wallets the difference is seconds vs milliseconds.

DB file: agent_cache.db
  Table: wallets  — address (PK), profile JSON, score JSON, analyzed_at
  Table: tokens   — address (PK), symbol, price_change, peak_volume,
                    first_seen, last_seen, scanned_at, disabled
"""

import json
import sqlite3
import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

DB_FILE = "agent_cache.db"


class Cache:
    def __init__(
        self,
        db_file: str         = DB_FILE,
        token_ttl_hours: int = 24,
    ):
        self.db_file         = db_file
        self.token_ttl_hours = token_ttl_hours
        self._conn           = self._connect()
        self._create_tables()
        log.info(f"Cache: {self.wallet_count()} wallets, {self.token_count()} tokens")

    # ── Connection ────────────────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        # WAL mode: concurrent reads don't block writes
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _create_tables(self):
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS wallets (
                address     TEXT PRIMARY KEY,
                profile     TEXT NOT NULL,
                score       TEXT NOT NULL,
                analyzed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tokens (
                address      TEXT PRIMARY KEY,
                symbol       TEXT,
                price_change REAL DEFAULT 0.0,
                peak_volume  REAL DEFAULT 0.0,
                first_seen   TEXT,
                last_seen    TEXT,
                scanned_at   TEXT,
                disabled     INTEGER DEFAULT 0
            );
        """)
        self._conn.commit()

    # ── Wallet cache ──────────────────────────────────────────────────

    def has_wallet(self, address: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM wallets WHERE address = ?", (address.lower(),)
        ).fetchone()
        return row is not None

    def get_wallet(self, address: str) -> dict | None:
        row = self._conn.execute(
            "SELECT profile, score, analyzed_at FROM wallets WHERE address = ?",
            (address.lower(),)
        ).fetchone()
        if not row:
            return None
        try:
            return {
                "profile":     json.loads(row["profile"]),
                "score":       json.loads(row["score"]),
                "analyzed_at": row["analyzed_at"],
            }
        except Exception as e:
            log.warning(f"Cache get_wallet parse error for {address}: {e}")
            return None

    def save_wallet(self, address: str, profile: dict, score: dict):
        try:
            self._conn.execute(
                """
                INSERT INTO wallets (address, profile, score, analyzed_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(address) DO UPDATE SET
                    profile     = excluded.profile,
                    score       = excluded.score,
                    analyzed_at = excluded.analyzed_at
                """,
                (
                    address.lower(),
                    json.dumps(profile),
                    json.dumps(score),
                    datetime.now(timezone.utc).isoformat(),
                )
            )
            self._conn.commit()
        except Exception as e:
            log.error(f"Cache save_wallet error for {address}: {e}")

    def wallet_count(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) FROM wallets").fetchone()
        return row[0] if row else 0

    # ── Token cache ───────────────────────────────────────────────────

    def has_token(self, address: str) -> bool:
        row = self._conn.execute(
            "SELECT scanned_at FROM tokens WHERE address = ?", (address.lower(),)
        ).fetchone()
        if not row or not row["scanned_at"]:
            return False
        try:
            scanned_at = datetime.fromisoformat(row["scanned_at"])
            return datetime.now(timezone.utc) - scanned_at < timedelta(hours=self.token_ttl_hours)
        except Exception:
            return False

    def save_token(self, address: str, symbol: str, price_change: float = 0.0, volume_usd: float = 0.0):
        addr = address.lower()
        now  = datetime.now(timezone.utc).isoformat()
        try:
            existing = self._conn.execute(
                "SELECT first_seen, peak_volume FROM tokens WHERE address = ?", (addr,)
            ).fetchone()
            first_seen   = existing["first_seen"] if existing and existing["first_seen"] else now
            current_peak = existing["peak_volume"] if existing and existing["peak_volume"] else 0.0
            peak_volume  = max(current_peak, volume_usd)

            self._conn.execute(
                """
                INSERT INTO tokens (address, symbol, price_change, peak_volume,
                                    first_seen, last_seen, scanned_at, disabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(address) DO UPDATE SET
                    symbol       = excluded.symbol,
                    price_change = excluded.price_change,
                    peak_volume  = excluded.peak_volume,
                    last_seen    = excluded.last_seen,
                    scanned_at   = excluded.scanned_at
                """,
                (addr, symbol, round(price_change, 2), peak_volume, first_seen, now, now)
            )
            self._conn.commit()
        except Exception as e:
            log.error(f"Cache save_token error for {address}: {e}")

    def disable_token(self, address: str):
        try:
            self._conn.execute(
                "UPDATE tokens SET disabled = 1 WHERE address = ?", (address.lower(),)
            )
            self._conn.commit()
        except Exception as e:
            log.error(f"Cache disable_token error: {e}")

    def enable_token(self, address: str):
        try:
            self._conn.execute(
                "UPDATE tokens SET disabled = 0 WHERE address = ?", (address.lower(),)
            )
            self._conn.commit()
        except Exception as e:
            log.error(f"Cache enable_token error: {e}")

    def is_token_disabled(self, address: str) -> bool:
        row = self._conn.execute(
            "SELECT disabled FROM tokens WHERE address = ?", (address.lower(),)
        ).fetchone()
        return bool(row["disabled"]) if row else False

    def token_count(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) FROM tokens").fetchone()
        return row[0] if row else 0

    # ── Server API helpers (used by server.py for dashboard counts) ───

    def get_all_tokens(self) -> dict:
        """Returns all tokens as a dict keyed by address — for server.py /api/tokens."""
        rows = self._conn.execute(
            "SELECT address, symbol, price_change, peak_volume, "
            "first_seen, last_seen, scanned_at, disabled FROM tokens"
        ).fetchall()
        result = {}
        for row in rows:
            result[row["address"]] = {
                "symbol":       row["symbol"],
                "price_change": row["price_change"],
                "peak_volume":  row["peak_volume"],
                "first_seen":   row["first_seen"],
                "last_seen":    row["last_seen"],
                "scanned_at":   row["scanned_at"],
                "disabled":     bool(row["disabled"]),
            }
        return result

    def get_all_wallets(self) -> dict:
        """Returns all wallets as a dict — for server.py wallet cache count."""
        rows = self._conn.execute(
            "SELECT address, profile, score, analyzed_at FROM wallets"
        ).fetchall()
        result = {}
        for row in rows:
            try:
                result[row["address"]] = {
                    "profile":     json.loads(row["profile"]),
                    "score":       json.loads(row["score"]),
                    "analyzed_at": row["analyzed_at"],
                }
            except Exception:
                pass
        return result

    # ── Cleanup ───────────────────────────────────────────────────────

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass
