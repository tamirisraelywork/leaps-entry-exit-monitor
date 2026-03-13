"""
Background monitoring loop.

Uses APScheduler to run position checks every 30 minutes during US market
hours (9:30-16:00 ET, Mon-Fri) and send a daily summary at 9:35 AM ET.

The scheduler is started once via @st.cache_resource so it survives
Streamlit reruns and shares a single instance across all sessions.
"""

import time
import logging
import streamlit as st
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import pytz

import db
import options_data
import email_alerts
import score_thesis
from exit_engine import evaluate, evaluate_entry
from technical import get_price_and_range, get_weekly_rsi

logger = logging.getLogger(__name__)

ET = pytz.timezone("America/New_York")


# ---------------------------------------------------------------------------
# Market data helper
# ---------------------------------------------------------------------------

def _fetch_market_data(position: dict) -> dict:
    """
    Fetch live market data for an active position.
    Returns a dict with: mid, delta, dte, iv_rank, thesis_score,
    earnings_state, earnings_tone_score, earnings_guidance_change,
    earnings_tone_delta, thesis_impact, news_sentiment_score.
    Missing values are None (handled gracefully by exit_engine).
    """
    import re as _re
    from datetime import date as _date

    ticker   = position.get("ticker", "")
    contract = (position.get("contract", "") or "").strip()

    # Construct OCC from position fields if contract is missing
    if not contract:
        _strike = position.get("strike")
        _exp    = position.get("expiration_date")
        if _strike and _exp:
            try:
                _exp_d = _exp if hasattr(_exp, "strftime") else _date.fromisoformat(str(_exp))
                _ot    = position.get("option_type", "C") or "C"
                contract = options_data.to_occ(ticker, _exp_d, _ot, float(_strike))
            except Exception as e:
                logger.warning(f"Could not construct OCC for {ticker}: {e}")

    snapshot = {}
    if contract:
        try:
            snapshot = options_data.get_option_snapshot(ticker, contract) or {}
            if "_error" in snapshot:
                logger.warning(f"Snapshot error for {ticker}: {snapshot['_error']}")
                snapshot = {}
        except Exception as e:
            logger.warning(f"Snapshot failed for {ticker}: {e}")

    # DTE fallback — always compute from expiration_date if snapshot missing
    if snapshot.get("dte") is None:
        _exp = position.get("expiration_date")
        if _exp:
            try:
                _exp_d = _exp if hasattr(_exp, "toordinal") else _date.fromisoformat(str(_exp))
                snapshot["dte"] = max(0, (_exp_d - _date.today()).days)
            except Exception:
                pass

    # IV Rank
    iv_rank = None
    try:
        from iv_rank import get_iv_rank_advanced
        _iv_hint = (snapshot.get("implied_volatility") or 0) * 100 or None
        result = get_iv_rank_advanced(ticker, current_iv_pct=_iv_hint)
        if result and "Success" in result:
            m = _re.search(r"([\d.]+)", result.split("is:")[-1])
            if m:
                iv_rank = float(m.group(1))
            else:
                logger.warning(f"IV rank parse failed for {ticker}: '{result}'")
    except Exception as e:
        logger.warning(f"IV rank fetch failed for {ticker}: {e}")

    thesis_score = db.get_leaps_monitor_score(ticker)

    # Earnings data (Pillar 5 signals)
    earnings_state = earnings_tone_score = earnings_guidance_change = None
    earnings_tone_delta = earnings_thesis_impact = news_sentiment_score = None
    try:
        # earnings_state from stored earnings_date in position (no extra API call)
        _ed = position.get("earnings_date")
        if _ed:
            try:
                _ed_d   = _ed if hasattr(_ed, "toordinal") else _date.fromisoformat(str(_ed))
                _days   = (_ed_d - _date.today()).days
                if _days < 0:
                    earnings_state = "post"
                elif _days == 0:
                    earnings_state = "day_of"
                elif _days <= 7:
                    earnings_state = "week_of"
                elif _days <= 14:
                    earnings_state = "imminent"
                else:
                    earnings_state = "upcoming"
            except Exception:
                pass
    except Exception:
        pass

    return {
        "mid":                       snapshot.get("mid"),
        "delta":                     snapshot.get("delta"),
        "dte":                       snapshot.get("dte"),
        "iv_rank":                   iv_rank,
        "thesis_score":              thesis_score,
        "earnings_state":            earnings_state,
        "earnings_tone_score":       earnings_tone_score,
        "earnings_guidance_change":  earnings_guidance_change,
        "earnings_tone_delta":       earnings_tone_delta,
        "thesis_impact":             earnings_thesis_impact,
        "news_sentiment_score":      news_sentiment_score,
    }


# ---------------------------------------------------------------------------
# Core check functions (called by scheduler)
# ---------------------------------------------------------------------------

def run_active_checks():
    """Evaluate all ACTIVE positions and send alerts if triggered."""
    logger.info("Running active position checks...")
    try:
        positions = db.get_positions(mode="ACTIVE")
    except Exception as e:
        logger.error(f"Failed to fetch active positions: {e}")
        return

    for pos in positions:
        try:
            pos_id = str(pos.get("id", ""))
            mkt    = _fetch_market_data(pos)
            alerts = evaluate(pos, mkt)

            # ── Posture change detection ─────────────────────────────────────
            new_posture  = _posture_from_alerts(alerts)
            prev_posture = pos.get("last_posture")   # None if column is new
            # Only record a real change — not first-time initialisation to HOLD
            _real_change = (
                prev_posture is not None            # already had a known posture
                and new_posture != prev_posture     # and it changed
            ) or (
                prev_posture is None                # first observation
                and new_posture != "HOLD"           # but it's already actionable
            )
            if _real_change:
                logger.info(f"{pos.get('ticker')}: posture changed {prev_posture} → {new_posture}")
                try:
                    db.update_position_posture(pos_id, new_posture)
                except Exception as e:
                    logger.warning(f"Could not save posture for {pos.get('ticker')}: {e}")
            elif prev_posture is None:
                # Quietly initialise — don't fire a "change" but do persist the baseline
                try:
                    db.update_position_posture(pos_id, new_posture)
                except Exception:
                    pass

            # ── Alert emails (one per type per day) ──────────────────────────
            for alert in alerts:
                if db.already_sent_today(alert.type, pos_id):
                    continue

                sent = email_alerts.send_alert(alert.subject, alert.body)
                db.save_alert({
                    "position_id":          pos_id,
                    "ticker":               pos.get("ticker"),
                    "alert_type":           alert.type,
                    "severity":             alert.severity,
                    "subject":              alert.subject,
                    "body":                 alert.body,
                    "current_delta":        mkt.get("delta"),
                    "current_dte":          mkt.get("dte"),
                    "current_pnl_pct":      _pnl_pct(pos.get("entry_price"), mkt.get("mid")),
                    "current_iv_rank":      mkt.get("iv_rank"),
                    "current_thesis_score": mkt.get("thesis_score"),
                    "email_sent":           sent,
                })

            time.sleep(2)

        except Exception as e:
            logger.error(f"Error checking position {pos.get('ticker')}: {e}")


def run_watchlist_checks():
    """Evaluate WATCHLIST positions for entry signals."""
    logger.info("Running watchlist entry checks...")
    try:
        watchlist = db.get_positions(mode="WATCHLIST")
    except Exception as e:
        logger.error(f"Failed to fetch watchlist: {e}")
        return

    for pos in watchlist:
        try:
            ticker = pos.get("ticker", "")
            stock_data = get_price_and_range(ticker)
            stock_data["weekly_rsi"] = get_weekly_rsi(ticker)

            iv_rank = None
            try:
                from iv_rank import get_iv_rank_advanced
                result = get_iv_rank_advanced(ticker)
                if result and "Success" in result:
                    import re
                    m = re.search(r"([\d.]+)", result.split("is:")[-1])
                    iv_rank = float(m.group(1)) if m else None
            except Exception:
                pass

            alert = evaluate_entry(pos, stock_data, iv_rank)
            if not alert:
                continue

            pos_id = str(pos.get("id", ""))
            if db.already_sent_today(alert.type, pos_id):
                continue

            sent = email_alerts.send_alert(alert.subject, alert.body)
            db.save_alert({
                "position_id":          pos_id,
                "ticker":               ticker,
                "alert_type":           alert.type,
                "severity":             alert.severity,
                "subject":              alert.subject,
                "body":                 alert.body,
                "current_delta":        None,
                "current_dte":          None,
                "current_pnl_pct":      None,
                "current_iv_rank":      iv_rank,
                "current_thesis_score": db.get_leaps_monitor_score(ticker),
                "email_sent":           sent,
            })

            time.sleep(2)

        except Exception as e:
            logger.error(f"Error checking watchlist position {pos.get('ticker')}: {e}")


def run_thesis_refresh():
    """
    Re-score thesis for every active ticker whose score is stale.

    Staleness thresholds (expert recommendation):
      Normal months:   > 30 days since last score
      Earnings months: >  7 days since last score  (Feb / May / Aug / Nov)

    Runs daily at 6 AM ET so the score going into each trading day is fresh.
    Each ticker takes ~20-40 seconds (yfinance + one Gemini call).
    """
    logger.info("Running thesis refresh check...")
    try:
        positions = db.get_positions(mode="ACTIVE")
    except Exception as e:
        logger.error(f"Thesis refresh: could not fetch positions: {e}")
        return

    seen = set()   # deduplicate tickers (multiple contracts on same stock)
    for pos in positions:
        ticker = pos.get("ticker", "")
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        try:
            if score_thesis.needs_refresh(ticker):
                logger.info(f"Thesis refresh: scoring {ticker}")
                score, verdict = score_thesis.compute_and_save_score(ticker)
                if score is not None:
                    logger.info(f"Thesis refresh: {ticker} → {score} ({verdict})")
                else:
                    logger.warning(f"Thesis refresh: scoring failed for {ticker}")
            else:
                logger.info(f"Thesis refresh: {ticker} score is fresh — skipping")
        except Exception as e:
            logger.error(f"Thesis refresh error for {ticker}: {e}")


def send_morning_summary():
    """
    Build and send the 10 AM daily portfolio email.

    Section 1 — ACTIVE POSITIONS: live market data + posture changes since yesterday.
    Section 2 — WATCHLIST: entry signal status + current thesis scores.
    """
    logger.info("Sending daily summary...")
    try:
        positions = db.get_positions()

        # ── Fetch live data for active positions ──────────────────────────────
        snapshots = {}
        for pos in positions:
            if pos.get("mode") == "ACTIVE":
                mkt = _fetch_market_data(pos)
                snapshots[str(pos["id"])] = mkt
                time.sleep(2)

        # ── Fetch entry signals for watchlist positions ───────────────────────
        watchlist_signals = {}
        for pos in positions:
            if pos.get("mode") != "WATCHLIST":
                continue
            try:
                ticker = pos.get("ticker", "")
                stock_data = get_price_and_range(ticker)
                stock_data["weekly_rsi"] = get_weekly_rsi(ticker)
                iv_rank = None
                try:
                    from iv_rank import get_iv_rank_advanced
                    result = get_iv_rank_advanced(ticker)
                    if result and "Success" in result:
                        import re
                        m = re.search(r"([\d.]+)", result.split("is:")[-1])
                        iv_rank = float(m.group(1)) if m else None
                except Exception:
                    pass
                entry_alert = evaluate_entry(pos, stock_data, iv_rank)
                thesis_score = db.get_leaps_monitor_score(ticker)
                watchlist_signals[str(pos["id"])] = {
                    "entry_alert": entry_alert,
                    "thesis_score": thesis_score,
                    "iv_rank": iv_rank,
                    "price": stock_data.get("price"),
                    "rsi": stock_data.get("weekly_rsi"),
                }
                time.sleep(2)
            except Exception as e:
                logger.warning(f"Watchlist signal fetch failed for {pos.get('ticker')}: {e}")

        # ── Posture changes since last check ──────────────────────────────────
        try:
            changed_rows = db.get_recent_posture_changes(hours=26)
            posture_changes = {r["id"]: r.get("last_posture", "HOLD") for r in changed_rows}
        except Exception as e:
            logger.warning(f"Could not fetch posture changes: {e}")
            posture_changes = {}

        email_alerts.send_daily_summary(
            positions, snapshots,
            posture_changes=posture_changes,
            watchlist_signals=watchlist_signals,
        )
    except Exception as e:
        logger.error(f"Daily summary failed: {e}")


def _posture_from_alerts(alerts) -> str:
    """Collapse a list of Alert objects to a single posture label."""
    if not alerts:
        return "HOLD"
    priority = {"RED": 4, "BLUE": 3, "AMBER": 2, "GREEN": 1}
    worst = max(alerts, key=lambda a: priority.get(a.severity, 0))
    return {"RED": "EXIT", "BLUE": "ROLL", "AMBER": "WATCH", "GREEN": "HOLD"}.get(worst.severity, "HOLD")


def _pnl_pct(entry_price, mid):
    if entry_price and mid and entry_price > 0:
        return round((mid - entry_price) / entry_price * 100, 1)
    return None


# ---------------------------------------------------------------------------
# Scheduler setup — called once via @st.cache_resource
# ---------------------------------------------------------------------------

@st.cache_resource
def start_scheduler():
    """
    Start the APScheduler background scheduler.
    Cached as a resource so it is created only once per Streamlit process.
    """
    scheduler = BackgroundScheduler(timezone=ET)

    # Active position checks — every 30 min, Mon-Fri, 9:30-16:00 ET
    scheduler.add_job(
        run_active_checks,
        CronTrigger(
            day_of_week="mon-fri",
            hour="9-15",
            minute="*/30",
            timezone=ET,
        ),
        id="active_checks",
        replace_existing=True,
    )

    # Watchlist entry checks — every hour, Mon-Fri, 9:30-16:00 ET
    scheduler.add_job(
        run_watchlist_checks,
        CronTrigger(
            day_of_week="mon-fri",
            hour="9-15",
            minute="0",
            timezone=ET,
        ),
        id="watchlist_checks",
        replace_existing=True,
    )

    # Daily summary — 10:00 AM ET, Mon-Fri (after market open volatility settles)
    scheduler.add_job(
        send_morning_summary,
        CronTrigger(
            day_of_week="mon-fri",
            hour=10,
            minute=0,
            timezone=ET,
        ),
        id="daily_summary",
        replace_existing=True,
    )

    # Thesis refresh — 6:00 AM ET, Mon-Fri
    # Re-scores stale tickers before the trading day opens.
    # Staleness = 30 days normally, 7 days in earnings months (Feb/May/Aug/Nov).
    scheduler.add_job(
        run_thesis_refresh,
        CronTrigger(
            day_of_week="mon-fri",
            hour=6,
            minute=0,
            timezone=ET,
        ),
        id="thesis_refresh",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("LEAPS Exit Agent scheduler started.")
    return scheduler
