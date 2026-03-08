"""
Monitor Engine — Standalone Background Scheduler.

Runs as a pure Python process completely independent of Streamlit.
Survives UI crashes, restarts, and timeout events.

Run:
    python -m monitor_engine.main

Checks:
  - Active position exit/trim/roll signals   every 30 min (market hours)
  - Watchlist entry signals                  every hour  (market hours)
  - Thesis staleness refresh                 6:00 AM ET daily
  - Earnings date refresh                    6:10 AM ET daily  [NEW]
  - Post-earnings call analysis              10:00 AM ET daily [NEW]
  - News sentiment checks                    every 4 hours     [NEW]
  - Daily portfolio summary email            9:35 AM ET daily

Architecture:
  - Communicates with UI (Streamlit app) ONLY through BigQuery.
  - No Streamlit imports. No st.secrets. All secrets via shared/config.py.
  - @st.cache_resource replaced with module-level singleton.
"""

from __future__ import annotations

import re
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# Add project root to path so imports work from any working directory
_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

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
    Returns dict with: mid, delta, dte, iv_rank, thesis_score,
    earnings_state, earnings_tone_score, earnings_guidance_change,
    earnings_tone_delta, news_sentiment_score.
    Missing values are None (handled gracefully by exit_engine).
    """
    ticker   = position.get("ticker", "")
    contract = position.get("contract", "")

    snapshot = {}
    if contract:
        try:
            snapshot = options_data.get_option_snapshot(ticker, contract) or {}
        except Exception as e:
            logger.warning(f"Option snapshot failed for {ticker}: {e}")

    # IV Rank (optional)
    iv_rank = None
    try:
        from iv_rank import get_iv_rank_advanced
        result = get_iv_rank_advanced(ticker)
        if result and "Success" in result:
            m = re.search(r"([\d.]+)", result.split("is:")[-1])
            iv_rank = float(m.group(1)) if m else None
    except Exception:
        pass

    thesis_score = db.get_leaps_monitor_score(ticker)

    # Earnings state (new)
    earnings_state = None
    earnings_tone_score = None
    earnings_guidance_change = None
    earnings_tone_delta = None
    try:
        from monitor_engine.earnings_calendar import get_earnings_state_for_position
        earnings_state = get_earnings_state_for_position(position)
    except Exception:
        pass

    try:
        from monitor_engine.earnings_call_analysis import get_latest_call_data, get_tone_delta
        call_data = get_latest_call_data(ticker)
        if call_data:
            earnings_tone_score = call_data.get("tone_score")
            earnings_guidance_change = call_data.get("guidance_change")
            earnings_tone_delta = get_tone_delta(ticker)
    except Exception:
        pass

    # News sentiment (new — cached 4 hrs, called here for active checks)
    news_sentiment_score = None
    try:
        from monitor_engine.news_sentiment import get_news_sentiment_cached
        news_data = get_news_sentiment_cached(ticker)
        news_sentiment_score = news_data.get("score") if news_data else None
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
        "news_sentiment_score":      news_sentiment_score,
    }


def _pnl_pct(entry_price, mid):
    if entry_price and mid and entry_price > 0:
        return round((mid - entry_price) / entry_price * 100, 1)
    return None


# ---------------------------------------------------------------------------
# Core check functions
# ---------------------------------------------------------------------------

def run_active_checks():
    """Evaluate all ACTIVE positions and send alerts if triggered."""
    logger.info("Running active position checks…")
    try:
        positions = db.get_positions(mode="ACTIVE")
    except Exception as e:
        logger.error(f"Failed to fetch active positions: {e}")
        return

    for pos in positions:
        try:
            mkt    = _fetch_market_data(pos)
            alerts = evaluate(pos, mkt)

            for alert in alerts:
                pos_id = str(pos.get("id", ""))
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

            time.sleep(12)  # Polygon free tier: 5 req/min

        except Exception as e:
            logger.error(f"Error checking position {pos.get('ticker')}: {e}")


def run_watchlist_checks():
    """Evaluate WATCHLIST positions for entry signals."""
    logger.info("Running watchlist entry checks…")
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
            logger.error(f"Error checking watchlist {pos.get('ticker')}: {e}")


def run_thesis_refresh():
    """
    Re-score thesis for stale tickers.
    Normal months: > 30 days. Earnings months (Feb/May/Aug/Nov): > 7 days.
    """
    logger.info("Running thesis refresh check…")
    try:
        positions = db.get_positions(mode="ACTIVE")
    except Exception as e:
        logger.error(f"Thesis refresh: could not fetch positions: {e}")
        return

    seen = set()
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
                logger.debug(f"Thesis refresh: {ticker} score is fresh — skipping")
        except Exception as e:
            logger.error(f"Thesis refresh error for {ticker}: {e}")


def run_earnings_refresh():
    """
    Fetch upcoming earnings dates for all active + watchlist positions.
    Updates the earnings_date field in BigQuery.
    """
    logger.info("Running earnings date refresh…")
    try:
        from monitor_engine.earnings_calendar import refresh_earnings_dates
        refresh_earnings_dates()
    except Exception as e:
        logger.error(f"Earnings refresh failed: {e}")


def run_post_earnings_analysis():
    """
    For positions whose earnings date was yesterday, run Perplexity
    earnings call analysis and store tone + guidance in BigQuery.
    """
    logger.info("Running post-earnings call analysis…")
    try:
        from monitor_engine.earnings_call_analysis import run_post_earnings_analysis_job
        run_post_earnings_analysis_job()
    except Exception as e:
        logger.error(f"Post-earnings analysis failed: {e}")


def run_news_checks():
    """
    Check Alpha Vantage news sentiment for all active positions.
    Saves bearish scores to BigQuery and sends alerts if triggered.
    """
    logger.info("Running news sentiment checks…")
    try:
        from monitor_engine.news_sentiment import run_news_check_job
        run_news_check_job()
    except Exception as e:
        logger.error(f"News sentiment check failed: {e}")


def send_morning_summary():
    """Build and send the daily portfolio summary email (with earnings calendar)."""
    logger.info("Sending morning summary…")
    try:
        positions = db.get_positions()
        snapshots = {}
        for pos in positions:
            if pos.get("mode") == "ACTIVE":
                mkt = _fetch_market_data(pos)
                snapshots[str(pos["id"])] = mkt
                time.sleep(12)

        # Build earnings calendar section for email
        earnings_section = ""
        try:
            from monitor_engine.earnings_calendar import get_upcoming_earnings_for_email
            earnings_section = get_upcoming_earnings_for_email(positions)
        except Exception:
            pass

        email_alerts.send_daily_summary(positions, snapshots, earnings_section=earnings_section)
    except Exception as e:
        logger.error(f"Daily summary failed: {e}")


# ---------------------------------------------------------------------------
# Scheduler singleton — no @st.cache_resource, works standalone
# ---------------------------------------------------------------------------

_scheduler: BackgroundScheduler | None = None


def get_scheduler() -> BackgroundScheduler:
    """Return the singleton scheduler, creating and starting it if needed."""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        return _scheduler

    sched = BackgroundScheduler(timezone=ET)

    # Active position checks — every 30 min, Mon-Fri, 9:30-16:00 ET
    sched.add_job(
        run_active_checks,
        CronTrigger(day_of_week="mon-fri", hour="9-15", minute="*/30", timezone=ET),
        id="active_checks", replace_existing=True,
    )
    # Watchlist entry checks — hourly, Mon-Fri, 9:00-16:00 ET
    sched.add_job(
        run_watchlist_checks,
        CronTrigger(day_of_week="mon-fri", hour="9-15", minute="0", timezone=ET),
        id="watchlist_checks", replace_existing=True,
    )
    # Daily portfolio summary — 9:35 AM ET, Mon-Fri
    sched.add_job(
        send_morning_summary,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=35, timezone=ET),
        id="daily_summary", replace_existing=True,
    )
    # Thesis refresh — 6:00 AM ET, Mon-Fri
    sched.add_job(
        run_thesis_refresh,
        CronTrigger(day_of_week="mon-fri", hour=6, minute=0, timezone=ET),
        id="thesis_refresh", replace_existing=True,
    )
    # Earnings date refresh — 6:10 AM ET, Mon-Fri [NEW]
    sched.add_job(
        run_earnings_refresh,
        CronTrigger(day_of_week="mon-fri", hour=6, minute=10, timezone=ET),
        id="earnings_refresh", replace_existing=True,
    )
    # Post-earnings call analysis — 10:00 AM ET, Mon-Fri [NEW]
    sched.add_job(
        run_post_earnings_analysis,
        CronTrigger(day_of_week="mon-fri", hour=10, minute=0, timezone=ET),
        id="post_earnings_analysis", replace_existing=True,
    )
    # News sentiment checks — every 4 hours, Mon-Fri [NEW]
    sched.add_job(
        run_news_checks,
        CronTrigger(day_of_week="mon-fri", hour="9,13,17,21", minute=0, timezone=ET),
        id="news_checks", replace_existing=True,
    )

    sched.start()
    _scheduler = sched
    logger.info("LEAPS Monitor Engine scheduler started — 7 jobs registered.")
    return sched
