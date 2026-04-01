"""
Watchlist
---------
Persistent store for wallets that passed scoring.
Tracks their ongoing activity each cycle.

Deletion is permanent: deleted addresses are stored in a separate
watchlist_deleted.json file. add() refuses to re-insert any address
that has been deleted, even if the agent finds it again as a buyer.
"""

import json
import os
import logging
from datetime import datetime

log = logging.getLogger(__name__)

WATCHLIST_FILE  = "watchlist.json"
DELETED_FILE    = "watchlist_deleted.json"


class Watchlist:
    def __init__(self, filepath: str = WATCHLIST_FILE):
        self.filepath      = filepath
        self.deleted_path  = os.path.join(os.path.dirname(os.path.abspath(filepath)), DELETED_FILE)
        self._data: dict   = self._load()
        self._deleted: set = self._load_deleted()
        log.info(f"Watchlist: {len(self._data)} wallets, {len(self._deleted)} permanently deleted")

    # ── Public API ────────────────────────────────────────────────────

    def add(self, entry: dict) -> bool:
        address = entry["address"].lower()
        # Never re-add a wallet that the user explicitly deleted
        if address in self._deleted:
            return False
        if address in self._data:
            return False
        self._data[address] = {
            "address":   address,
            "profile":   entry["profile"],
            "score":     entry["score"],
            "found_on":  entry.get("found_on", ""),
            "found_at":  entry.get("found_at", datetime.utcnow().isoformat()),
            "activity":  [],
        }
        self._save()
        return True

    def get_all(self) -> list[dict]:
        return list(self._data.values())

    def log_activity(self, address: str, trade: dict):
        addr = address.lower()
        if addr not in self._data:
            return
        self._data[addr]["activity"].append(trade)
        self._data[addr]["activity"] = self._data[addr]["activity"][-200:]
        self._save()

    def update_profile(self, address: str, profile: dict, score: dict):
        """Update wallet profile and score in place after recalculation."""
        addr = address.lower()
        if addr in self._data:
            self._data[addr]["profile"]      = profile
            self._data[addr]["score"]        = score
            self._data[addr]["rescanned_at"] = datetime.utcnow().isoformat()
            self._save()

    def disable(self, address: str):
        addr = address.lower()
        if addr in self._data:
            self._data[addr]["disabled"] = True
            self._save()

    def enable(self, address: str):
        addr = address.lower()
        if addr in self._data:
            self._data[addr]["disabled"] = False
            self._save()

    def delete(self, address: str) -> bool:
        addr = address.lower()
        found = addr in self._data
        if found:
            del self._data[addr]
            self._save()
        # Always register as permanently deleted so the agent won't re-add it
        self._deleted.add(addr)
        self._save_deleted()
        return found

    def get_active(self) -> list[dict]:
        """Returns only non-disabled wallets for agent monitoring."""
        return [w for w in self._data.values() if not w.get("disabled", False)]

    def count(self) -> int:
        return len(self._data)

    # ── Reload (called by agent each cycle) ───────────────────────────

    def reload(self):
        """Re-read both watchlist and deleted set from disk."""
        self._data    = self._load()
        self._deleted = self._load_deleted()

    # ── Internal ──────────────────────────────────────────────────────

    def _load(self) -> dict:
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r") as f:
                    return json.load(f)
            except Exception as e:
                log.warning(f"Watchlist load error: {e}")
        return {}

    def _save(self):
        try:
            with open(self.filepath, "w") as f:
                json.dump(self._data, f, indent=2)
        except Exception as e:
            log.error(f"Watchlist save error: {e}")

    def _load_deleted(self) -> set:
        if os.path.exists(self.deleted_path):
            try:
                with open(self.deleted_path, "r") as f:
                    return set(json.load(f))
            except Exception as e:
                log.warning(f"Deleted-wallets load error: {e}")
        return set()

    def _save_deleted(self):
        try:
            with open(self.deleted_path, "w") as f:
                json.dump(list(self._deleted), f, indent=2)
        except Exception as e:
            log.error(f"Deleted-wallets save error: {e}")
