"""
Earnings Calendar — fetches upcoming earnings dates and computes pre/post-earnings state.

Uses yfinance (free, no API key needed).
Results are written to the positions table (earnings_date column) and read
by exit_engine Pillar 5 to determine pre-earnings sell windows.

States (returned as strings for JSON/BQ storage):
  'far'         — > 30 days to earnings (normal monitoring)
  'approaching' — 15–30 days (IV expansion often begins)
  'imminent'    — 7–14 days  (prime IV sell window)
  'week_of'     — 1–7 days   (elevated urgency)
  'day_of'      — same calendar day
  'post'        — 1–3 days after (IV crush, re-assess window)
  'unknown'     — no earnings date found
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

import yfinance as yf

logger = logging.getLogger(__name__)


def get_earnings_date(ticker: str) -> Optional[date]:
    """
    Returns the next upcoming earnings date for a ticker, or None if not found.
    Uses yfinance Ticker.calendar (free).
    """
    try:
        t = yf.Ticker(ticker.upper())
        cal = t.calendar

        # yfinance returns calendar as a dict with 'Earnings Date' key (list of dates)
        if cal is None:
            return None

        if isinstance(cal, dict):
            ed = cal.get("Earnings Date")
            if ed:
                # May be a list; take the first upcoming date
                if hasattr(ed, "__iter__") and not isinstance(ed, str):
                    for d in ed:
                        try:
                            candidate = _to_date(d)
                            if candidate >= date.today():
                                return candidate
                        except Exception:
                            continue
                else:
                    return _to_date(ed)

        # Try the DataFrame format (older yfinance versions)
        import pandas as pd
        if hasattr(cal, "loc"):
            try:
                val = cal.loc["Earnings Date"].iloc[0]
                return _to_date(val)
            except Exception:
                pass

    except Exception as e:
        logger.debug(f"get_earnings_date({ticker}): {e}")

    return None


def _to_date(val) -> date:
    """Convert various date representations to datetime.date."""
    import pandas as pd
    if isinstance(val, date):
        return val
    if hasattr(val, "date"):
        return val.date()
    if isinstance(val, str):
        return date.fromisoformat(str(val)[:10])
    return pd.Timestamp(val).date()


def get_earnings_state(ticker: str, earnings_date: Optional[date]) -> str:
    """
    Determine the earnings state relative to today.
    Returns one of: far, approaching, imminent, week_of, day_of, post, unknown.
    """
    if not earnings_date:
        # Try to fetch it on the fly
        earnings_date = get_earnings_date(ticker)

    if not earnings_date:
        return "unknown"

    today = date.today()
    delta = (earnings_date - today).days

    if delta == 0:
        return "day_of"
    elif -3 <= delta < 0:
        return "post"
    elif 1 <= delta <= 7:
        return "week_of"
    elif 8 <= delta <= 14:
        return "imminent"
    elif 15 <= delta <= 30:
        return "approaching"
    elif delta > 30:
        return "far"
    else:
        # delta < -3 — old earnings date, data stale
        return "unknown"


def get_earnings_state_for_position(position: dict) -> str:
    """
    Convenience wrapper: read earnings_date from the position dict (as stored in BQ),
    fall back to live yfinance fetch if missing.
    """
    ticker = position.get("ticker", "")
    ed_raw = position.get("earnings_date")

    earnings_date = None
    if ed_raw:
        try:
            earnings_date = _to_date(ed_raw)
        except Exception:
            pass

    return get_earnings_state(ticker, earnings_date)


def refresh_earnings_dates():
    """
    Fetch upcoming earnings dates for all ACTIVE + WATCHLIST positions
    and update the earnings_date column in BigQuery.
    Called daily at 6:10 AM ET.
    """
    import db
    from google.cloud import bigquery

    logger.info("Refreshing earnings dates for all positions…")

    try:
        positions = db.get_positions()
    except Exception as e:
        logger.error(f"refresh_earnings_dates: could not load positions: {e}")
        return

    seen: set[str] = set()
    updated = 0

    for pos in positions:
        mode   = pos.get("mode", "")
        ticker = (pos.get("ticker") or "").upper()
        pos_id = str(pos.get("id", ""))

        if mode not in ("ACTIVE", "WATCHLIST") or not ticker or ticker in seen:
            continue
        seen.add(ticker)

        try:
            ed = get_earnings_date(ticker)
            if ed:
                db.update_position(pos_id, {"earnings_date": ed.isoformat()})
                logger.info(f"  {ticker}: earnings_date = {ed}")
                updated += 1
            else:
                logger.debug(f"  {ticker}: no earnings date found")
        except Exception as e:
            logger.warning(f"  {ticker}: refresh failed — {e}")

    logger.info(f"Earnings refresh complete — {updated} tickers updated.")


def get_upcoming_earnings_for_email(positions: list[dict]) -> str:
    """
    Build a plain-text earnings calendar section for the daily summary email.
    Shows positions with earnings in the next 14 days.
    """
    upcoming = []
    seen: set[str] = set()

    for pos in positions:
        ticker = (pos.get("ticker") or "").upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)

        ed_raw = pos.get("earnings_date")
        ed = None
        if ed_raw:
            try:
                ed = _to_date(ed_raw)
            except Exception:
                pass

        if not ed:
            ed = get_earnings_date(ticker)

        if ed:
            delta = (ed - date.today()).days
            if 0 <= delta <= 14:
                state = get_earnings_state(ticker, ed)
                upcoming.append((delta, ticker, ed, state))

    if not upcoming:
        return ""

    upcoming.sort()
    lines = ["📅 EARNINGS CALENDAR (next 14 days):", ""]
    for delta, ticker, ed, state in upcoming:
        urgency = {
            "day_of":    "⚠️  TODAY",
            "week_of":   "🔴 THIS WEEK",
            "imminent":  "🔵 NEXT WEEK",
            "approaching": "🟡 APPROACHING",
        }.get(state, "")
        lines.append(f"  {ticker:<8}  {ed.strftime('%b %d')}  ({delta}d)  {urgency}")

    return "\n".join(lines)
