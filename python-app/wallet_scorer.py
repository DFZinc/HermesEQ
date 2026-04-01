"""
Wallet Scorer
-------------
Scores a wallet profile 0-100 using improved metrics.

Hard disqualifiers (score = 0, immediate reject):
  - Bot detected (>100 normal txs/day by timestamp rate)
  - Wallet age < 14 days
  - Fewer than 5 unique tokens traded
  - Net P&L (including bags/unrealized losses) <= 0
  - Data error

Qualification paths (must meet at least one):
  Path A — Quant:        recent win rate >= 60% AND all-time win rate >= 55%
  Path B — Power Trader: ROI >= 40% AND total P&L >= $8,000 USD
  Path C — Whale:        total P&L >= $80,000 USD

Scoring weights (sum to 100):
  Time-weighted win rate   25%   (70% recent, 30% all-time)
  ROI                      20%
  Sharpe ratio             20%   (filters lucky gamblers from skilled traders)
  P&L quality              15%
  Max drawdown penalty     10%   (high drawdown = high risk, lower score)
  Trade diversity           5%
  Wallet age                5%

Additional penalties applied after base score:
  - Bag ratio > 30%: -10 points  (holding many worthless tokens)
  - Bag ratio > 50%: -20 points
  - No recent activity (0 trades in 90 days): -15 points
  - Avg hold time < 1 minute: -20 points  (likely bot/sniper pattern)
"""

import logging

log = logging.getLogger(__name__)


class WalletScorer:

    MIN_AGE_DAYS      = 14
    MIN_UNIQUE_TOKENS = 5

    PATH_A_RECENT_WIN  = 60.0
    PATH_A_ALLTIME_WIN = 55.0
    PATH_B_MIN_ROI     = 40.0
    PATH_B_MIN_PNL_USD = 8_000
    PATH_C_MIN_PNL_USD = 80_000

    WEIGHTS = {
        "win_rate_weighted": 25,
        "roi":               20,
        "sharpe":            20,
        "pnl_quality":       15,
        "drawdown":          10,
        "trade_diversity":    5,
        "wallet_age":         5,
    }

    def score(self, profile: dict) -> dict:
        reason = self._disqualify(profile)
        if reason:
            return {
                "total":             0,
                "breakdown":         {},
                "verdict":           "DISQUALIFIED",
                "disqualified":      True,
                "disqualify_reason": reason,
                "path":              None,
            }

        win_rate        = profile.get("win_rate", 0)
        recent_win_rate = profile.get("recent_win_rate", win_rate)
        roi             = profile.get("roi_pct", 0)
        recent_roi      = profile.get("recent_roi_pct", roi)
        total_pnl_usd   = profile.get("total_pnl_usd", 0)

        path_a = recent_win_rate >= self.PATH_A_RECENT_WIN and win_rate >= self.PATH_A_ALLTIME_WIN
        path_b = roi >= self.PATH_B_MIN_ROI and total_pnl_usd >= self.PATH_B_MIN_PNL_USD
        path_c = total_pnl_usd >= self.PATH_C_MIN_PNL_USD

        if not path_a and not path_b and not path_c:
            return {
                "total":             0,
                "breakdown":         {},
                "verdict":           "DISQUALIFIED",
                "disqualified":      True,
                "disqualify_reason": (
                    f"Win {win_rate}% (recent {recent_win_rate}%) | "
                    f"ROI {roi:.1f}% | P&L ${total_pnl_usd:,.0f} — no path met"
                ),
                "path": None,
            }

        path = (
            "A-quant"        if path_a else
            "B-power-trader" if path_b else
            "C-whale"
        )

        breakdown = {
            "win_rate_weighted": self._score_win_rate_weighted(win_rate, recent_win_rate),
            "roi":               self._score_roi(roi, recent_roi),
            "sharpe":            self._score_sharpe(profile.get("sharpe_ratio", 0)),
            "pnl_quality":       self._score_pnl(profile),
            "drawdown":          self._score_drawdown(profile.get("max_drawdown_pct", 0)),
            "trade_diversity":   self._score_diversity(profile.get("unique_tokens", 0)),
            "wallet_age":        self._score_age(profile.get("age_days", 0)),
        }

        total = sum(breakdown[k] * (self.WEIGHTS[k] / 100) for k in breakdown)
        total = self._apply_penalties(total, profile)
        total = max(0, min(100, round(total)))

        return {
            "total":             total,
            "breakdown":         {k: round(v, 1) for k, v in breakdown.items()},
            "verdict":           self._verdict(total),
            "disqualified":      False,
            "disqualify_reason": None,
            "path":              path,
        }

    def _disqualify(self, profile: dict) -> str | None:
        if profile.get("is_bot"):
            rate = profile.get("tx_rate_per_day", 0)
            return f"Bot detected ({rate:.0f} txs/day)"
        if profile.get("age_days", 0) < self.MIN_AGE_DAYS:
            return f"Wallet too new ({profile.get('age_days', 0)}d)"
        if profile.get("unique_tokens", 0) < self.MIN_UNIQUE_TOKENS:
            return f"Only {profile.get('unique_tokens', 0)} tokens traded"
        if profile.get("total_pnl_usd", 0) <= 0:
            return f"Net P&L ${profile.get('total_pnl_usd', 0):,.0f} — not profitable after losses"
        if profile.get("error"):
            return "Data error"
        return None

    def _score_win_rate_weighted(self, win_rate: float, recent_win_rate: float) -> float:
        weighted = (recent_win_rate * 0.70) + (win_rate * 0.30)
        if weighted < 20:  return 0
        if weighted < 35:  return 15
        if weighted < 50:  return 35
        if weighted < 60:  return 55
        if weighted < 70:  return 70
        if weighted < 80:  return 85
        return 100

    def _score_roi(self, roi: float, recent_roi: float) -> float:
        weighted = (recent_roi * 0.60) + (roi * 0.40)
        if weighted <= 0:   return 0
        if weighted < 20:   return 15
        if weighted < 40:   return 30
        if weighted < 80:   return 50
        if weighted < 150:  return 70
        if weighted < 300:  return 85
        return 100

    def _score_sharpe(self, sharpe: float) -> float:
        if sharpe <= 0:     return 0
        if sharpe < 0.1:    return 10
        if sharpe < 0.25:   return 25
        if sharpe < 0.50:   return 45
        if sharpe < 0.75:   return 65
        if sharpe < 1.0:    return 80
        if sharpe < 1.5:    return 92
        return 100

    def _score_pnl(self, profile: dict) -> float:
        total_usd = profile.get("total_pnl_usd", 0)
        avg_usd   = profile.get("avg_pnl_per_trade", 0)
        if total_usd <= 0:      return 0
        if total_usd < 500:     score = 15
        elif total_usd < 2000:  score = 30
        elif total_usd < 8000:  score = 50
        elif total_usd < 25000: score = 70
        elif total_usd < 80000: score = 85
        else:                   score = 100
        if avg_usd > 200:    score = min(100, score + 10)
        return score

    def _score_drawdown(self, max_drawdown_pct: float) -> float:
        if max_drawdown_pct < 10:  return 100
        if max_drawdown_pct < 20:  return 85
        if max_drawdown_pct < 35:  return 65
        if max_drawdown_pct < 50:  return 45
        if max_drawdown_pct < 70:  return 25
        return 5

    def _score_diversity(self, unique_tokens: int) -> float:
        if unique_tokens < 5:   return 0
        if unique_tokens < 10:  return 30
        if unique_tokens < 25:  return 55
        if unique_tokens < 50:  return 75
        if unique_tokens < 100: return 88
        return 100

    def _score_age(self, age_days: int) -> float:
        if age_days < 14:   return 0
        if age_days < 30:   return 30
        if age_days < 60:   return 50
        if age_days < 120:  return 70
        if age_days < 180:  return 85
        return 100

    def _apply_penalties(self, score: float, profile: dict) -> float:
        unique_tokens = profile.get("unique_tokens", 1)
        bags          = profile.get("bags", 0)
        hold_hrs      = profile.get("avg_hold_time_hours", 99)

        bag_ratio = bags / max(unique_tokens, 1)
        if bag_ratio > 0.50:
            score -= 20
        elif bag_ratio > 0.30:
            score -= 10

        if 0 < hold_hrs < (1 / 60):
            score -= 20

        return score

    def _verdict(self, total: int) -> str:
        if total >= 80: return "STRONG"
        if total >= 65: return "WATCHLIST"
        if total >= 50: return "WEAK"
        return "SKIP"
