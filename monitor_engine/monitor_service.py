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
  - Daily portfolio summary email            5:00 PM ET daily (after market close)

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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    from datetime import date as _date
    ticker   = position.get("ticker", "")
    contract = position.get("contract", "") or ""

    # If contract field is empty, try to construct OCC symbol from stored fields
    if not contract:
        _strike = position.get("strike")
        _exp    = position.get("expiration_date")
        if _strike and _exp:
            try:
                _exp_d = _exp if hasattr(_exp, "strftime") else _date.fromisoformat(str(_exp))
                contract = options_data.to_occ(ticker, _exp_d, "C", float(_strike))
                logger.debug(f"Constructed OCC contract for {ticker}: {contract}")
            except Exception as e:
                logger.warning(f"Could not construct OCC for {ticker}: {e}")

    snapshot = {}
    if contract:
        try:
            snapshot = options_data.get_option_snapshot(ticker, contract) or {}
            if "_error" in snapshot:
                logger.warning(f"Option snapshot error for {ticker} ({contract}): {snapshot['_error']}")
                snapshot = {}
        except Exception as e:
            logger.warning(f"Option snapshot failed for {ticker}: {e}")

    # Always compute DTE from expiration_date if snapshot didn't provide it
    if snapshot.get("dte") is None:
        _exp = position.get("expiration_date")
        if _exp:
            try:
                _exp_d = _exp if hasattr(_exp, "toordinal") else _date.fromisoformat(str(_exp))
                snapshot["dte"] = max(0, (_exp_d - _date.today()).days)
            except Exception:
                pass

    # IV Rank (optional)
    iv_rank = None
    try:
        from iv_rank import get_iv_rank_advanced
        _iv_hint = (snapshot.get("implied_volatility") or 0) * 100 or None
        result = get_iv_rank_advanced(ticker, current_iv_pct=_iv_hint)
        if result and "Success" in result:
            m = re.search(r"([\d.]+)", result.split("is:")[-1])
            if m:
                iv_rank = float(m.group(1))
            else:
                logger.warning(f"IV rank parse failed for {ticker}: unexpected format '{result}'")
        elif result:
            logger.warning(f"IV rank unavailable for {ticker}: {result}")
    except Exception as e:
        logger.warning(f"IV rank fetch error for {ticker}: {e}")

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

    earnings_thesis_impact = None
    try:
        from monitor_engine.earnings_call_analysis import get_latest_call_data, get_tone_delta
        call_data = get_latest_call_data(ticker)
        if call_data:
            earnings_tone_score      = call_data.get("tone_score")
            earnings_guidance_change = call_data.get("guidance_change")
            earnings_thesis_impact   = call_data.get("thesis_impact")   # WEAKENED / UNCHANGED / STRENGTHENED
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
        "thesis_impact":             earnings_thesis_impact,   # for EARNINGS_THESIS_BREAK alert
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
        positions = db.get_positions()   # ACTIVE + WATCHLIST both need fresh scores
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


def _fetch_watchlist_signal(pos: dict) -> tuple[str, dict]:
    """Fetch entry signal + contract recommendation for a single watchlist position."""
    ticker = pos.get("ticker", "")
    pos_id = str(pos.get("id", ""))
    try:
        stock_data = get_price_and_range(ticker)
        stock_data["weekly_rsi"] = get_weekly_rsi(ticker)
        price = stock_data.get("price")
        if price is None:
            logger.warning(f"Watchlist {ticker}: price fetch returned None (yfinance may be rate-limited)")

        iv_rank = None
        try:
            from iv_rank import get_iv_rank_advanced
            result = get_iv_rank_advanced(ticker)
            if result and "Success" in result:
                m = re.search(r"([\d.]+)", result.split("is:")[-1])
                iv_rank = float(m.group(1)) if m else None
                if iv_rank is not None and iv_rank < 1:
                    iv_rank = None  # < 1% is a fallback proxy, not real IV data
        except Exception:
            pass

        entry_alert  = evaluate_entry(pos, stock_data, iv_rank)
        thesis_score = db.get_leaps_monitor_score(ticker)

        rec_strike = rec_expiry = rec_premium = rec_delta = rec_otm = None
        try:
            from recommender import recommend_asymmetric
            rec = recommend_asymmetric(ticker)
            if rec.get("contracts"):
                top = rec["contracts"][0]
                rec_strike  = top.get("strike")
                rec_expiry  = str(top.get("expiration_date", ""))
                rec_premium = top.get("mid")
                rec_delta   = top.get("delta")
                rec_otm     = top.get("otm_pct")
        except Exception:
            pass

        return pos_id, {
            "entry_alert":  entry_alert,
            "thesis_score": thesis_score,
            "iv_rank":      iv_rank,
            "price":        price,
            "rsi":          stock_data.get("weekly_rsi"),
            "pct_from_low": stock_data.get("pct_from_low"),   # 52wk position: 0=low,1=high
            "rec_strike":   rec_strike,
            "rec_expiry":   rec_expiry,
            "rec_premium":  rec_premium,
            "rec_delta":    rec_delta,
            "rec_otm_pct":  rec_otm,
        }
    except Exception as e:
        logger.warning(f"Watchlist signal fetch failed for {ticker}: {e}")
        return pos_id, {}


def _build_earnings_tone_section(active_tickers: list[str]) -> str:
    """
    Build a text block summarising latest earnings call tone for active positions.
    Returns empty string if no data available.
    """
    try:
        from monitor_engine.earnings_call_analysis import get_latest_call_data, get_tone_delta
    except Exception:
        return ""

    rows = []
    for ticker in active_tickers:
        try:
            data = get_latest_call_data(ticker)
            if not data:
                continue
            tone_label = data.get("tone_label") or data.get("overall_tone", "?")
            guidance   = data.get("guidance_change") or data.get("forward_guidance", "?")
            quarter    = data.get("quarter", "")
            delta      = get_tone_delta(ticker) or "—"
            tone_icon  = {"BULLISH": "🟢", "NEUTRAL": "🟡", "BEARISH": "🔴"}.get(tone_label, "⚪")
            guide_icon = {"RAISED": "↑", "LOWERED": "↓", "WITHDRAWN": "✗"}.get(guidance, "→")
            rows.append(
                f"  {ticker:<6}  {tone_icon} {tone_label:<8} | Guidance {guide_icon} {guidance:<11}"
                f"| Trend {delta:<14}| {quarter}"
            )
        except Exception:
            pass

    if not rows:
        return ""

    header = "  Ticker   Tone            Guidance             Trend              Quarter\n"
    header += "  " + "─" * 70 + "\n"
    return header + "\n".join(rows)


def _build_news_section(active_tickers: list[str]) -> str:
    """
    Build a news sentiment section for active positions.
    Only includes tickers with BEARISH or VERY_BEARISH signals.
    Returns empty string if no concerning news.
    """
    try:
        from monitor_engine.news_sentiment import get_news_sentiment_cached
    except Exception:
        return ""

    bearish_rows = []
    for ticker in active_tickers:
        try:
            data = get_news_sentiment_cached(ticker)
            if not data:
                continue
            signal = data.get("signal", "NEUTRAL")
            if signal not in ("BEARISH", "VERY_BEARISH"):
                continue
            score     = data.get("score", 0)
            headline  = data.get("top_headline", "")
            count     = data.get("article_count", 0)
            icon      = "🔴" if signal == "VERY_BEARISH" else "⚠️"
            bearish_rows.append(
                f"  {icon} {ticker:<6}  score {score:+.2f} ({count} articles)\n"
                f"       → {headline[:80]}"
            )
        except Exception:
            pass

    if not bearish_rows:
        return "  ✓ No bearish news signals for active positions.\n"

    return "\n".join(bearish_rows)


def send_morning_summary():
    """Build and send the 5 PM daily portfolio summary email."""
    logger.info("Sending daily summary…")
    try:
        positions = db.get_positions()
        active    = [p for p in positions if p.get("mode") == "ACTIVE"]
        watchlist = [p for p in positions if p.get("mode") == "WATCHLIST"]
        active_tickers = list({p.get("ticker", "") for p in active if p.get("ticker")})

        # ── Active position live data — parallel fetch ────────────────────────
        snapshots: dict = {}
        if active:
            logger.info(f"Fetching market data for {len(active)} active positions (parallel)…")
            with ThreadPoolExecutor(max_workers=6) as ex:
                futures = {ex.submit(_fetch_market_data, pos): pos for pos in active}
                for future in as_completed(futures):
                    pos = futures[future]
                    pos_id = str(pos.get("id", ""))
                    try:
                        mkt = future.result()
                        snapshots[pos_id] = mkt
                        if mkt.get("mid") is None:
                            logger.warning(
                                f"Active {pos.get('ticker')}: no option mid price "
                                f"(contract='{pos.get('contract', '')}' "
                                f"strike={pos.get('strike')} exp={pos.get('expiration_date')})"
                            )
                    except Exception as e:
                        logger.warning(f"Market data fetch failed for {pos.get('ticker')}: {e}")
                        snapshots[pos_id] = {}

        # ── Watchlist entry signals — parallel fetch ──────────────────────────
        watchlist_signals: dict = {}
        if watchlist:
            logger.info(f"Fetching signals for {len(watchlist)} watchlist positions (parallel)…")
            with ThreadPoolExecutor(max_workers=4) as ex:
                futures = {ex.submit(_fetch_watchlist_signal, pos): pos for pos in watchlist}
                for future in as_completed(futures):
                    pos = futures[future]
                    try:
                        pos_id, sig = future.result()
                        watchlist_signals[pos_id] = sig
                    except Exception as e:
                        logger.warning(f"Watchlist signal failed for {pos.get('ticker')}: {e}")

        # ── Posture changes since yesterday ───────────────────────────────────
        posture_changes = {}
        try:
            changed_rows = db.get_recent_posture_changes(hours=26)
            posture_changes = {str(r["id"]): r.get("last_posture", "HOLD") for r in changed_rows}
        except Exception as e:
            logger.warning(f"Could not fetch posture changes: {e}")

        # ── Earnings calendar section ─────────────────────────────────────────
        earnings_section = ""
        try:
            from monitor_engine.earnings_calendar import get_upcoming_earnings_for_email
            earnings_section = get_upcoming_earnings_for_email(positions)
        except Exception:
            pass

        # ── Earnings call tone trends section ─────────────────────────────────
        earnings_tone_section = _build_earnings_tone_section(active_tickers)

        # ── News sentiment section ────────────────────────────────────────────
        news_section = _build_news_section(active_tickers)

        ok, err = email_alerts.send_daily_summary(
            positions,
            snapshots,
            posture_changes=posture_changes,
            watchlist_signals=watchlist_signals,
            earnings_section=earnings_section,
            earnings_tone_section=earnings_tone_section,
            news_section=news_section,
        )
        if ok:
            logger.info("Daily summary email sent successfully.")
        else:
            logger.error(f"Daily summary email failed: {err}")
    except Exception as e:
        logger.error(f"Daily summary failed: {e}")


# ---------------------------------------------------------------------------
# Scheduler singleton — no @st.cache_resource, works standalone
# ---------------------------------------------------------------------------

_scheduler: BackgroundScheduler | None = None


def _startup_check():
    """
    Log the status of all required secrets and config on startup.
    Makes missing secrets immediately visible in the log instead of
    causing silent failures hours later when the first job runs.
    """
    from shared.config import cfg

    checks = [
        ("GMAIL_SENDER",           cfg("GMAIL_SENDER") or cfg("ALERT_EMAIL_FROM")),
        ("GMAIL_APP_PASSWORD",     cfg("GMAIL_APP_PASSWORD") or cfg("ALERT_EMAIL_PASS")),
        ("ALERT_RECIPIENT_EMAIL",  cfg("ALERT_RECIPIENT_EMAIL")),
        ("SERVICE_ACCOUNT_JSON",   cfg("SERVICE_ACCOUNT_JSON")[:12] if cfg("SERVICE_ACCOUNT_JSON") else ""),
        ("MARKETDATA_TOKEN",       cfg("MARKETDATA_TOKEN")),
        ("ALPHA_VANTAGE_API_KEY_1",cfg("ALPHA_VANTAGE_API_KEY_1")),
        ("GEMINI_API_KEY",         cfg("GEMINI_API_KEY")),
    ]

    logger.info("─" * 55)
    logger.info("Startup configuration check:")
    for k, v in checks:
        status = "OK  " if v else "MISSING"
        logger.info(f"  {status}  {k}")
    logger.info("─" * 55)

    email_ok = (
        (cfg("GMAIL_SENDER") or cfg("ALERT_EMAIL_FROM")) and
        (cfg("GMAIL_APP_PASSWORD") or cfg("ALERT_EMAIL_PASS"))
    )
    if not email_ok:
        logger.warning(
            "Email is NOT configured. Daily summary and alert emails will fail silently. "
            "Set GMAIL_SENDER + GMAIL_APP_PASSWORD in .streamlit/secrets.toml."
        )
    if not cfg("SERVICE_ACCOUNT_JSON"):
        logger.error(
            "SERVICE_ACCOUNT_JSON is MISSING. BigQuery operations will fail. "
            "Monitor will not be able to read positions or save alerts."
        )


def get_scheduler() -> BackgroundScheduler:
    """Return the singleton scheduler, creating and starting it if needed."""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        return _scheduler

    _startup_check()

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
    # Daily portfolio summary — 5:00 PM ET, Mon-Fri (after market close)
    sched.add_job(
        send_morning_summary,
        CronTrigger(day_of_week="mon-fri", hour=17, minute=0, timezone=ET),
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
