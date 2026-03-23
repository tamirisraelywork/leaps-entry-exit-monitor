"""
LEAPS Command Center — Unified App
===================================
All LEAPS workflows in one place:

  1. New Analysis    — run full 7-source thesis evaluation
  2. Past Analyses   — history + per-ticker detailed breakdown
  3. Dashboard       — live position cards with exit/entry signals
  4. Add Position    — recommend options or log an existing contract
  5. Alert History   — full alert log with filters
  6. Settings        — email, thresholds, rescore, closed positions
"""

import re
import time
import asyncio
import threading
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date, datetime, timedelta
import logging

import pandas as pd
import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account

import db
import options_data
import email_alerts
import score_thesis
from recommender import recommend_options, format_recommendation, recommend_asymmetric
from technical import get_price_and_range, get_weekly_rsi
from exit_engine import evaluate, evaluate_entry
# Monitor engine runs as a separate process (monitor_engine/main.py).
# The Streamlit UI reads alerts + positions from BigQuery — no direct scheduler import needed.

# ---------------------------------------------------------------------------
# Page config & startup
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="LEAPS Command Center",
    page_icon="📈",
    layout="wide",
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    db.ensure_tables()
except Exception as e:
    st.error(f"BigQuery setup error: {e}")

# ---------------------------------------------------------------------------
# Shared state for background analysis jobs
# ---------------------------------------------------------------------------

@st.cache_resource
def _get_eval_state():
    return {
        "background_jobs": {},
        "batch_job":        {},
        "jobs_lock":        threading.Lock(),
        "batch_lock":       threading.Lock(),
    }

_ES = _get_eval_state()


# ---------------------------------------------------------------------------
# Position-manager helpers (existing)
# ---------------------------------------------------------------------------

_SEVERITY_COLOR = {
    "RED":   "#FF4B4B",
    "BLUE":  "#1E90FF",
    "AMBER": "#FFA500",
    "GREEN": "#21C55D",
}
_POSTURE_LABEL = {"RED": "EXIT / STOP", "BLUE": "ROLL", "AMBER": "WATCH", "GREEN": "HOLD"}
_POSTURE_EMOJI = {"RED": "🔴", "BLUE": "🔵", "AMBER": "⚠️", "GREEN": "🟢"}


@st.cache_data(ttl=300)
def _get_iv_rank_cached(ticker: str, current_iv_pct: float | None = None) -> float | None:
    """
    IV Rank cached 5 min.

    Fast path (current_iv_pct known): rank the given IV directly against the
    1-year rolling realized-vol range — one price-history call, no option chain.

    Slow path (no IV): delegate to iv_rank module which tries the option chain
    first then falls back to realized-vol percentile.

    Last-resort fallback: inline realized-vol percentile from price history only.
    """
    import yfinance as yf
    import math as _math

    def _hist_rank(iv_pct: float) -> float | None:
        """Rank iv_pct vs 1-year rolling realized-vol range. Returns 0-100 or None."""
        try:
            hist = yf.Ticker(ticker).history(period="1y", interval="1d", auto_adjust=True)
            if hist is None or hist.empty or len(hist) < 31:
                return round(iv_pct, 1)   # no history — return raw IV as rank
            rolling = (
                hist["Close"].pct_change().dropna()
                .rolling(30).std().dropna()
                * _math.sqrt(252) * 100
            )
            if rolling.empty:
                return round(iv_pct, 1)
            vmin, vmax = float(rolling.min()), float(rolling.max())
            if vmax > vmin:
                return round(max(0.0, min(100.0, (iv_pct - vmin) / (vmax - vmin) * 100)), 1)
            return 50.0   # flat vol — neutral
        except Exception:
            return round(iv_pct, 1)   # absolute fallback: return IV itself as rank

    # ── Fast path: we already have IV from the option snapshot ───────────────
    if current_iv_pct and current_iv_pct > 0:
        result = _hist_rank(current_iv_pct)
        if result is not None:
            return result

    # ── Slow path: no IV — try iv_rank module (option chain + history) ───────
    try:
        from iv_rank import get_iv_rank_advanced
        result = get_iv_rank_advanced(ticker, current_iv_pct=current_iv_pct)
        if result and "Success" in result:
            m = re.search(r"([\d.]+)", result.split("is:")[-1])
            val = float(m.group(1)) if m else None
            if val is not None:
                return val
    except Exception:
        pass

    # ── Last resort: realized-vol percentile — no options needed ─────────────
    try:
        hist = yf.Ticker(ticker).history(period="1y", interval="1d", auto_adjust=True)
        if hist is not None and not hist.empty and len(hist) >= 31:
            rolling = (
                hist["Close"].pct_change().dropna()
                .rolling(30).std().dropna()
                * _math.sqrt(252) * 100
            )
            if len(rolling) >= 2:
                cur  = float(rolling.iloc[-1])
                vmin = float(rolling.min())
                vmax = float(rolling.max())
                if vmax > vmin:
                    return round(max(0.0, min(100.0, (cur - vmin) / (vmax - vmin) * 100)), 1)
    except Exception:
        pass

    return None


def _clear_snapshot_cache(ticker: str, contract: str):
    """Remove a specific snapshot from session-state cache."""
    key = f"_snap::{ticker}::{contract}"
    st.session_state.pop(key, None)
    # Also clear the module-level cache in options_data
    options_data._cache.pop(f"{ticker}::{contract}", None)


def _clear_all_snapshot_cache():
    """Remove all snapshot cache entries from session state and module cache."""
    keys = [k for k in st.session_state if k.startswith("_snap::")]
    for k in keys:
        del st.session_state[k]
    with options_data._cache_lock:
        options_data._cache.clear()


def _get_snapshot_cached(ticker: str, contract: str) -> dict:
    """
    Fetch option snapshot with session-state caching (10 min TTL).
    Errors are NOT cached — retry immediately on next load.
    Results where mid == 0 or mid is suspiciously stale are NOT cached.
    """
    import time as _time
    key = f"_snap::{ticker}::{contract}"
    entry = st.session_state.get(key)
    if entry:
        ts, result = entry
        if _time.time() - ts < 600 and "_error" not in result:
            return result
    result = options_data.get_option_snapshot(ticker, contract)
    # Only cache if we have a real mid (not zero, not error)
    if "_error" not in result and result.get("mid") and result.get("mid", 0) > 0:
        st.session_state[key] = (_time.time(), result)
    return result


def _resolve_contract(pos: dict) -> str:
    """
    Return the stored OCC contract symbol, or build one from strike/expiry when
    the contract field is empty but the position data is complete.
    """
    contract = (pos.get("contract") or "").strip()
    if contract:
        return contract
    ticker  = pos.get("ticker", "")
    strike  = pos.get("strike")
    exp     = pos.get("expiration_date")
    opttype = (pos.get("option_type") or "CALL")[0].upper()
    if ticker and strike and exp:
        try:
            exp_date = exp if isinstance(exp, date) else date.fromisoformat(str(exp))
            return options_data.to_occ(ticker, exp_date, opttype, float(strike))
        except Exception:
            pass
    return ""


@st.cache_data(ttl=3600)
def _get_earnings_data_cached(ticker: str) -> dict:
    """
    Fetch latest earnings call tone/guidance from BigQuery (cached 1 hr).
    Returns keys: earnings_tone_score, earnings_guidance_change, thesis_impact, earnings_tone_delta.
    Falls back gracefully to all-None if the table doesn't exist yet.
    """
    result = {
        "earnings_tone_score":       None,
        "earnings_guidance_change":  None,
        "thesis_impact":             None,
        "earnings_tone_delta":       None,
        "news_sentiment_score":      None,
    }
    try:
        from monitor_engine.earnings_call_analysis import get_latest_call_data, get_tone_delta
        call_data = get_latest_call_data(ticker)
        if call_data:
            result["earnings_tone_score"]      = call_data.get("tone_score")
            result["earnings_guidance_change"] = call_data.get("guidance_change")
            result["thesis_impact"]            = call_data.get("thesis_impact")
            result["earnings_tone_delta"]      = get_tone_delta(ticker)
    except Exception:
        pass
    return result


def _earnings_state_from_pos(pos: dict) -> str | None:
    """Compute earnings_state from the stored earnings_date field — no API call needed."""
    _ed = pos.get("earnings_date")
    if not _ed:
        return None
    try:
        _ed_d = _ed if hasattr(_ed, "toordinal") else date.fromisoformat(str(_ed))
        _days = (_ed_d - date.today()).days
        if _days < 0:
            return "post"
        if _days == 0:
            return "day_of"
        if _days <= 7:
            return "week_of"
        if _days <= 14:
            return "imminent"
        return "upcoming"
    except Exception:
        return None


def _get_positions_cached() -> list[dict]:
    """
    Return db.get_positions() with a 3-minute session-state TTL.
    Eliminates redundant BigQuery round-trips on every Streamlit rerun
    (which happens on every widget interaction).
    Call _invalidate_positions_cache() after any write to the positions table.
    """
    import time as _t
    entry = st.session_state.get("_positions_cache")
    if entry and (_t.time() - entry[0]) < 60:
        return entry[1]
    try:
        positions = db.get_positions()
        st.session_state["_positions_cache"] = (_t.time(), positions)
        return positions
    except Exception as e:
        st.error(f"Could not load positions: {e}")
        return []


def _invalidate_positions_cache():
    """Force-expire the positions cache so the next read hits BigQuery."""
    st.session_state.pop("_positions_cache", None)


def _prefetch_active_data(active: list[dict], force_check: bool = False) -> dict:
    """
    Fetch all market data for active positions IN PARALLEL using ThreadPoolExecutor.

    Each position's snapshot, IV rank, and earnings data is fetched concurrently
    rather than sequentially — cuts total dashboard load time from N×T to ~T
    (where T is the slowest single fetch, typically 1-2 s).

    Returns {pos_id: {"contract": str, "snap": dict, "iv_rank": float|None, "earnings": dict}}

    Note: Uses options_data module cache directly (thread-safe) instead of
    session-state caching (not thread-safe). Session state is updated in the
    main thread after all futures complete.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time as _t

    def _fetch_one(pos: dict) -> tuple:
        pos_id   = str(pos.get("id", ""))
        ticker   = pos.get("ticker", "")
        contract = _resolve_contract(pos)

        snap: dict = {}
        if contract:
            cache_key = f"{ticker}::{contract}"
            # On force-check: evict module-level cache so a fresh fetch is triggered
            if force_check:
                with options_data._cache_lock:
                    options_data._cache.pop(cache_key, None)
            else:
                # Try the module-level cache first — avoids duplicate in-flight requests
                cached = options_data._cache_get(cache_key)
                if cached is not None and "_error" not in cached:
                    snap = cached

            if not snap:
                raw = options_data.get_option_snapshot(ticker, contract)
                if "_error" not in raw:
                    snap = raw

        iv_pct   = (snap.get("implied_volatility") or 0) * 100 or None
        iv_rank  = _get_iv_rank_cached(ticker, iv_pct)     # @st.cache_data — thread-safe
        earnings = _get_earnings_data_cached(ticker)        # @st.cache_data — thread-safe

        return pos_id, contract, snap, iv_rank, earnings

    results: dict = {}
    max_workers = min(len(active), 8)  # cap threads; BigQuery/yfinance don't need more
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_one, pos): pos for pos in active}
        for future in as_completed(futures):
            try:
                pos_id, contract, snap, iv_rank, earnings = future.result()
                results[pos_id] = {
                    "contract": contract,
                    "snap":     snap,
                    "iv_rank":  iv_rank,
                    "earnings": earnings,
                }
            except Exception:
                pos_id = str(futures[future].get("id", ""))
                results[pos_id] = {"contract": "", "snap": {}, "iv_rank": None, "earnings": {}}

    # Write successful snapshots back to session-state cache in the main thread
    now = _t.time()
    for pos in active:
        pos_id  = str(pos.get("id", ""))
        pf      = results.get(pos_id, {})
        ticker   = pos.get("ticker", "")
        contract = pf.get("contract", "")
        snap     = pf.get("snap", {})
        if contract and snap and "_error" not in snap and snap.get("mid", 0) > 0:
            st.session_state[f"_snap::{ticker}::{contract}"] = (now, snap)

    return results


def _live_market(pos: dict) -> dict:
    ticker   = pos.get("ticker", "")
    contract = _resolve_contract(pos)
    snapshot = _get_snapshot_cached(ticker, contract) if contract else {}
    snapshot = snapshot or {}
    iv_pct   = (snapshot.get("implied_volatility") or 0) * 100 or None
    raw_exp  = pos.get("expiration_date")
    dte_fallback = None
    if raw_exp:
        try:
            exp_d        = raw_exp if isinstance(raw_exp, date) else date.fromisoformat(str(raw_exp))
            dte_fallback = max(0, (exp_d - date.today()).days)
        except Exception:
            pass
    earnings = _get_earnings_data_cached(ticker)
    return {
        "mid":                       snapshot.get("mid"),
        "bid":                       snapshot.get("bid"),
        "ask":                       snapshot.get("ask"),
        "delta":                     snapshot.get("delta"),
        "dte":                       snapshot.get("dte") or dte_fallback,
        "iv_rank":                   _get_iv_rank_cached(ticker, iv_pct),
        "thesis_score":              None,
        "earnings_state":            _earnings_state_from_pos(pos),
        "earnings_tone_score":       earnings["earnings_tone_score"],
        "earnings_guidance_change":  earnings["earnings_guidance_change"],
        "thesis_impact":             earnings["thesis_impact"],
        "earnings_tone_delta":       earnings["earnings_tone_delta"],
        "news_sentiment_score":      earnings["news_sentiment_score"],
    }


def _pnl_pct(entry_price, mid):
    if entry_price and mid and entry_price > 0:
        return round((mid - entry_price) / entry_price * 100, 1)
    return None


def _alert_priority(a) -> int:
    return {"RED": 3, "BLUE": 2, "AMBER": 1, "GREEN": 0}.get(a.severity, 0)


# ---------------------------------------------------------------------------
# Evaluator — BigQuery helpers
# ---------------------------------------------------------------------------

def safe_float(val):
    if val is None or str(val).lower() in ("n/a", "none", "rejected", "", "nan"):
        return 0.0
    try:
        return float(re.sub(r"[^\d.-]", "", str(val)) or "0")
    except Exception:
        return 0.0


def _verdict_from_score(score, is_rejected):
    if is_rejected:
        return "❌ Rejected"
    if score >= 75:
        return "🔥 Elite LEAPS Candidate"
    if score >= 60:
        return "✅ Qualified"
    if score >= 45:
        return "⚠️ Watchlist"
    return "❌ Rejected"


def _verdict_color(verdict):
    v = str(verdict).lower()
    if "elite"     in v: return "#28a745"
    if "qualified" in v: return "#007bff"
    if "watchlist" in v: return "#e6a817"
    return "#dc3545"


def _eval_table_path(client, table_name: str) -> str:
    """Build a fully-qualified BigQuery table path.
    Pass table_name as-is for system tables (master_table),
    or pre-clean ticker symbols before calling.
    """
    raw = (
        st.secrets.get("DATASET_ID")
        or st.secrets.get("LEAPS_MONITOR_DATASET", "leaps_monitor")
    )
    if "." in str(raw):
        return f"{raw}.{table_name}"
    return f"{client.project}.{raw}.{table_name}"


def _ticker_table_name(ticker: str) -> str:
    """Clean a ticker symbol into a valid BigQuery table name."""
    return ticker.strip().upper().replace("-", "_").replace(".", "_")


@st.cache_data(ttl=300, show_spinner=False)
def get_eval_master_data() -> pd.DataFrame:
    try:
        client = db.get_client()
        path = _eval_table_path(client, "master_table")
        return client.query(
            f"SELECT Ticker, date, Score, Verdict FROM `{path}`"
        ).to_dataframe()
    except Exception as e:
        logger.error(f"master_table read error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def get_eval_ticker_detail(ticker: str) -> pd.DataFrame:
    try:
        client = db.get_client()
        path = _eval_table_path(client, _ticker_table_name(ticker))
        return client.query(f"SELECT * FROM `{path}`").to_dataframe()
    except Exception as e:
        err = str(e)
        if "404" in err or "not found" in err.lower():
            return pd.DataFrame()
        logger.error(f"Ticker detail read error for {ticker}: {e}")
        return pd.DataFrame()


def save_eval_analysis(ticker: str, table_rows: list, sws_data: dict, llm_data: dict,
                        final_score: float, verdict: str) -> tuple[bool, str | None]:
    """Save full analysis to evaluator BigQuery tables (per-ticker + master_table)."""
    try:
        client = db.get_client()

        ticker_path  = _eval_table_path(client, _ticker_table_name(ticker))
        master_path  = _eval_table_path(client, "master_table")

        rows = [
            {
                "Matric name":    r["Metric Name"],
                "Source":         r.get("Source", ""),
                "Value":          str(r.get("Value", "")),
                "Obtained Score": str(r["Obtained points"]),
                "Total score":    str(r["Total points"]),
                "LLM":            None,
            }
            for r in table_rows
            if r["Metric Name"] != "TOTAL"
        ]
        rows += [
            {"Matric name": "Risks",               "LLM": "\n".join(sws_data.get("risks",    []))},
            {"Matric name": "Rewards",             "LLM": "\n".join(sws_data.get("rewards",  []))},
            {"Matric name": "Company Description", "LLM": llm_data.get("description",       "N/A")},
            {"Matric name": "Value Proposition",   "LLM": llm_data.get("value_proposition", "N/A")},
            {"Matric name": "Moat Analysis",       "LLM": llm_data.get("moat",              "N/A")},
            {"Matric name": "DATE",                "LLM": date.today().strftime("%Y-%m-%d")},
        ]

        client.load_table_from_dataframe(
            pd.DataFrame(rows), ticker_path,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        ).result()

        today  = date.today().strftime("%Y-%m-%d")
        params = [
            bigquery.ScalarQueryParameter("ticker",  "STRING", ticker),
            bigquery.ScalarQueryParameter("score",   "INT64",  int(float(final_score))),
            bigquery.ScalarQueryParameter("verdict", "STRING", verdict),
            bigquery.ScalarQueryParameter("date",    "STRING", today),
        ]
        exists = client.query(
            f"SELECT Ticker FROM `{master_path}` WHERE Ticker = @ticker",
            job_config=bigquery.QueryJobConfig(query_parameters=[params[0]]),
        ).to_dataframe()

        sql = (
            f"UPDATE `{master_path}` SET Score=@score, Verdict=@verdict, date=@date WHERE Ticker=@ticker"
            if not exists.empty else
            f"INSERT INTO `{master_path}` (Ticker, Score, Verdict, date) VALUES (@ticker,@score,@verdict,@date)"
        )
        client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
        return True, None

    except Exception as e:
        logger.error(f"save_eval_analysis error for {ticker}: {e}")
        return False, str(e)


def rescore_ticker_in_bq(ticker: str) -> tuple[int, int, str, str]:
    """Re-apply current scoring rules to stored VALUES. No API calls. Returns (old, new, old_v, new_v)."""
    try:
        client      = db.get_client()
        ticker_path = _eval_table_path(client, _ticker_table_name(ticker))
        master_path = _eval_table_path(client, "master_table")

        old_df = client.query(
            f"SELECT Score, Verdict FROM `{master_path}` WHERE Ticker = @ticker",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("ticker", "STRING", ticker)
            ]),
        ).to_dataframe()
        old_score   = int(safe_float(old_df["Score"].iloc[0]))   if not old_df.empty else 0
        old_verdict = str(old_df["Verdict"].iloc[0])              if not old_df.empty else "N/A"

        try:
            detail_df = client.query(f"SELECT * FROM `{ticker_path}`").to_dataframe()
        except Exception as tbl_err:
            _emsg = str(tbl_err).lower()
            if "404" in _emsg or "not found" in _emsg:
                # No detail table — ticker was added to watchlist but never analyzed; skip
                return old_score, old_score, old_verdict, old_verdict
            raise
        if detail_df.empty:
            return old_score, old_score, old_verdict, old_verdict

        m_col = "Matric name"    if "Matric name"    in detail_df.columns else "Metric Name"
        s_col = "Obtained Score" if "Obtained Score" in detail_df.columns else "Obtained points"
        t_col = "Total score"    if "Total score"    in detail_df.columns else "Total points"

        QUAL = {"Risks","Rewards","Company Description","Value Proposition","Moat Analysis","DATE"}

        # ── Full pre-pass: seed all thread-local values before scoring ────────
        # This ensures correct ordering regardless of BQ row order —
        # Net Debt / EBITDA sign check and Business Model (uses GF moat score)
        # both depend on values from other metric rows.
        tl = score_thesis._thread_local
        tl.gf_score     = 0.0
        tl.net_debt_val = None
        tl.ebitda_val   = None
        for _, row in detail_df.iterrows():
            mn  = str(row.get(m_col, "")).lower()
            val = str(row.get("Value", ""))
            if mn == "net debt":
                tl.net_debt_val = val
            elif mn == "ebitda":
                tl.ebitda_val = val
            elif "moat score" in mn:
                # Pre-seed GF moat score so Business Model scores correctly even if
                # BQ returns it after the Business Model row.
                try:
                    tl.gf_score = float(score_thesis.safe_float(val))
                except Exception:
                    pass

        new_rows    = []
        total_score = 0.0
        is_rejected = False

        # Expiration date is time-sensitive: a date stored 2 years ago may now
        # appear expired and falsely trigger rejection.  Preserve the stored
        # score/total for this metric instead of re-evaluating with today's date.
        SKIP_RESCORE = {"latest expiration date", "expiration date"}

        for _, row in detail_df.iterrows():
            r           = row.to_dict()
            metric_name = str(r.get(m_col, ""))
            if metric_name in QUAL or not metric_name:
                new_rows.append(r)
                continue

            # Preserve stored score for time-sensitive expiration metric
            if metric_name.lower() in SKIP_RESCORE:
                stored_pts = safe_float(str(r.get(s_col, "0") or "0"))
                stored_tot = safe_float(str(r.get(t_col, "0") or "0"))
                if str(r.get(s_col, "")).lower() == "rejected":
                    is_rejected = True
                elif stored_tot > 0:
                    total_score += stored_pts
                new_rows.append(r)
                continue

            value              = str(r.get("Value", "N/A"))
            pts, total_pts, rej = score_thesis.calculate_scoring(metric_name, value)
            if rej:
                is_rejected = True
                r[s_col]    = "rejected"
                r[t_col]    = str(total_pts) if total_pts > 0 else ""
            else:
                r[s_col] = str(pts)       if total_pts > 0 else ""
                r[t_col] = str(total_pts) if total_pts > 0 else ""
                if total_pts > 0:
                    total_score += pts
            new_rows.append(r)

        # Replace NaN with None so BigQuery doesn't reject string columns
        rows_df = pd.DataFrame(new_rows).where(pd.notnull(pd.DataFrame(new_rows)), None)
        client.load_table_from_dataframe(
            rows_df, ticker_path,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        ).result()

        new_score   = int(round(total_score))
        new_verdict = _verdict_from_score(new_score, is_rejected)

        client.query(
            f"UPDATE `{master_path}` SET Score=@score, Verdict=@verdict WHERE Ticker=@ticker",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("score",   "INT64",  new_score),
                bigquery.ScalarQueryParameter("verdict", "STRING", new_verdict),
                bigquery.ScalarQueryParameter("ticker",  "STRING", ticker),
            ]),
        ).result()

        return old_score, new_score, old_verdict, new_verdict

    except Exception as e:
        logger.error(f"rescore_ticker_in_bq failed for {ticker}: {e}", exc_info=True)
        return 0, 0, "⚠️ Error", f"[{ticker}] {e}"


def delete_eval_ticker(ticker: str) -> bool:
    try:
        client = db.get_client()
        client.delete_table(_eval_table_path(client, _ticker_table_name(ticker)), not_found_ok=True)
        master_path = _eval_table_path(client, "master_table")
        client.query(
            f"DELETE FROM `{master_path}` WHERE Ticker = @ticker",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("ticker", "STRING", ticker)
            ]),
        ).result()
        st.cache_data.clear()
        return True
    except Exception as e:
        logger.error(f"delete_eval_ticker error for {ticker}: {e}")
        return False


def delete_all_eval_tickers(tickers: list) -> bool:
    try:
        client = db.get_client()
        master_path = _eval_table_path(client, "master_table")
        for t in tickers:
            client.delete_table(_eval_table_path(client, _ticker_table_name(t)), not_found_ok=True)
        client.query(f"DELETE FROM `{master_path}` WHERE TRUE").result()
        st.cache_data.clear()
        return True
    except Exception as e:
        logger.error(f"delete_all_eval_tickers error: {e}")
        return False


def send_alert_email(to_addr: str, subject: str, body: str) -> tuple[bool, str | None]:
    """Send an HTML email via SMTP. Reads credentials from secrets (GMAIL_SENDER or ALERT_EMAIL_FROM)."""
    try:
        from shared.config import cfg as _cfg
        # Accept both key name conventions
        from_addr = _cfg("ALERT_EMAIL_FROM") or _cfg("GMAIL_SENDER")
        password  = _cfg("ALERT_EMAIL_PASS") or _cfg("GMAIL_APP_PASSWORD")
        smtp_host = _cfg("ALERT_SMTP_HOST") or "smtp.gmail.com"
        smtp_port = int(_cfg("ALERT_SMTP_PORT") or 587)
        if not from_addr or not password or not to_addr:
            return False, "Missing email credentials or recipient (add GMAIL_SENDER + GMAIL_APP_PASSWORD to secrets)"
        msg            = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = from_addr
        msg["To"]      = to_addr
        msg.attach(MIMEText(body, "html"))
        with smtplib.SMTP(smtp_host, smtp_port) as srv:
            srv.ehlo(); srv.starttls(); srv.login(from_addr, password)
            srv.sendmail(from_addr, to_addr, msg.as_string())
        return True, None
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# Evaluator — background workers
# ---------------------------------------------------------------------------

def _single_worker(ticker: str):
    with _ES["jobs_lock"]:
        _ES["background_jobs"][ticker] = {"status": "running"}
    try:
        raw              = score_thesis._run_analysis(ticker)
        score, verdict, table_rows, llm, sws = score_thesis._build_report(ticker, raw)

        # TOTAL row for display
        total_pts = sum(safe_float(r["Total points"])   for r in table_rows if r.get("Total points"))
        table_rows_display = table_rows + [{
            "Metric Name": "TOTAL", "Source": "", "Value": "",
            "Obtained points": str(int(score)), "Total points": str(int(total_pts)),
        }]

        save_eval_analysis(ticker, table_rows, sws, llm, score, verdict)

        with _ES["jobs_lock"]:
            _ES["background_jobs"][ticker] = {
                "status":    "complete",
                "table_rows": table_rows_display,
                "llm_parsed": llm,
                "sws_data":   sws,
                "score":      score,
                "verdict":    verdict,
            }
    except Exception as e:
        with _ES["jobs_lock"]:
            _ES["background_jobs"][ticker] = {"status": "error", "error": str(e)}


def _batch_worker(tickers: list, delay_seconds: int):
    bj = _ES["batch_job"]
    bl = _ES["batch_lock"]
    try:
        for i, ticker in enumerate(tickers):
            with bl:
                bj["current"] = ticker
            try:
                raw           = score_thesis._run_analysis(ticker)
                score, verdict, table_rows, llm, sws = score_thesis._build_report(ticker, raw)
                saved, err    = save_eval_analysis(ticker, table_rows, sws, llm, score, verdict)
                if not saved:
                    time.sleep(3)
                    saved, err = save_eval_analysis(ticker, table_rows, sws, llm, score, verdict)
                row = {
                    "Ticker": ticker,
                    "Score":  f"{score:.0f}",
                    "Verdict": verdict,
                    "DB":     "✓ Saved" if saved else f"✗ {str(err)[:120]}",
                }
            except Exception as e:
                row = {"Ticker": ticker, "Score": "N/A", "Verdict": "N/A",
                       "DB": f"Error: {str(e)[:120]}"}
            with bl:
                bj["results"].append(row)
                bj["done"] = i + 1
            if i < len(tickers) - 1:
                time.sleep(delay_seconds)
    except Exception as e:
        with bl:
            bj["error"] = str(e)
    finally:
        with bl:
            bj["status"]  = "complete"
            bj["current"] = ""


# ---------------------------------------------------------------------------
# Session state init
# ---------------------------------------------------------------------------

for _k, _v in [
    ("eval_report_data",   None),
    ("eval_risk_reward",   None),
    ("eval_llm",           None),
    ("eval_ticker",        ""),
    ("past_view",          "history"),
    ("past_selected",      None),
    ("confirm_delete_all", False),
    ("rescore_results",    []),
    ("alert_email",        ""),
    ("alert_trigger",      "Verdict changes"),
    ("alert_enabled",      False),
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------

st.sidebar.title("📈 LEAPS Command Center")

page = st.sidebar.radio(
    "Navigate",
    [
        "🔍 New Analysis",
        "📋 Past Analyses",
        "📊 Dashboard",
        "➕ Add Position",
        "🔔 Alert History",
        "⚙️ Settings",
    ],
    label_visibility="collapsed",
)

st.sidebar.markdown("---")
st.sidebar.caption(
    "Monitoring active during US market hours.\n"
    "Daily summary at 9:35 AM ET."
)


# ===========================================================================
# PAGE: NEW ANALYSIS
# ===========================================================================

if page == "🔍 New Analysis":

    st.title("New Analysis")
    st.caption(
        "Institutional-grade LEAPS evaluation. Enter a ticker to run a full "
        "7-source thesis analysis and save results to your database."
    )

    cur = st.session_state.eval_ticker

    # ── Single-ticker polling ─────────────────────────────────────────────────
    if cur and st.session_state.eval_report_data is None:
        job     = _ES["background_jobs"].get(cur, {})
        jstatus = job.get("status")
        if jstatus == "running":
            st.info(f"⏳ Analyzing **{cur}**… (~60 seconds)  You can leave and return later.")
            time.sleep(1)
            st.rerun()
        elif jstatus == "complete":
            st.session_state.eval_report_data = job["table_rows"]
            st.session_state.eval_llm         = job["llm_parsed"]
            st.session_state.eval_risk_reward = job["sws_data"]
            with _ES["jobs_lock"]:
                _ES["background_jobs"].pop(cur, None)
            st.rerun()
        elif jstatus == "error":
            st.error(f"Analysis failed for **{cur}**: {job.get('error', '')}")
            with _ES["jobs_lock"]:
                _ES["background_jobs"].pop(cur, None)
            st.session_state.eval_ticker = ""

    # ── Batch polling ─────────────────────────────────────────────────────────
    with _ES["batch_lock"]:
        _bj = dict(_ES["batch_job"])

    if _bj.get("status") in ("running", "complete"):
        done    = _bj.get("done",    0)
        total   = _bj.get("total",   1)
        current = _bj.get("current", "")
        results = _bj.get("results", [])

        if _bj["status"] == "running":
            st.info(f"⏳ Batch — **{done}/{total}** complete.  Currently: **{current}**")
            st.progress(done / max(total, 1))
        else:
            st.success(f"✅ Batch complete — **{total}** tickers processed.")
            if st.button("Start New Analysis"):
                with _ES["batch_lock"]:
                    _ES["batch_job"].clear()
                st.cache_data.clear()
                st.rerun()

        if results:
            st.subheader(f"Results ({len(results)}/{total})")
            st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)

        if _bj["status"] == "running":
            time.sleep(1)
            st.rerun()

        st.stop()

    # ── Input form ────────────────────────────────────────────────────────────
    if st.session_state.eval_report_data is None:
        _, center, _ = st.columns([1, 2, 1])
        with center:
            ticker_input = st.text_input(
                "Ticker",
                placeholder="Single: TSLA   |   Batch: TSLA, NVDA, AAPL",
                label_visibility="collapsed",
            )
            is_batch = "," in ticker_input
            if is_batch:
                delay_seconds = st.slider(
                    "Delay between tickers (seconds)", 5, 30, 10, 5,
                    help="Prevents rate-limiting.",
                )
                btn_label = "Run Batch Analysis"
            else:
                delay_seconds = 20
                btn_label     = "Generate Report"

            if st.button(btn_label, type="primary", disabled=not ticker_input.strip()):
                if is_batch:
                    tickers = list(dict.fromkeys(
                        t.strip().upper() for t in ticker_input.split(",") if t.strip()
                    ))
                    with _ES["batch_lock"]:
                        already = _ES["batch_job"].get("status") == "running"
                    if not already:
                        with _ES["batch_lock"]:
                            _ES["batch_job"].update({
                                "status":  "running", "total": len(tickers),
                                "done":    0,         "current": tickers[0] if tickers else "",
                                "results": [],
                            })
                        threading.Thread(
                            target=_batch_worker, args=(tickers, delay_seconds), daemon=False
                        ).start()
                    st.rerun()
                else:
                    ticker = ticker_input.strip().upper()
                    with _ES["jobs_lock"]:
                        already = _ES["background_jobs"].get(ticker, {}).get("status") == "running"
                    if not already:
                        with _ES["jobs_lock"]:
                            _ES["background_jobs"][ticker] = {"status": "running"}
                        threading.Thread(
                            target=_single_worker, args=(ticker,), daemon=False
                        ).start()
                    st.session_state.eval_ticker      = ticker
                    st.session_state.eval_report_data = None
                    st.rerun()

    # ── Results display ───────────────────────────────────────────────────────
    else:
        report = st.session_state.eval_report_data
        ticker = st.session_state.eval_ticker

        if st.button("← New Analysis"):
            st.session_state.eval_report_data = None
            st.session_state.eval_ticker      = ""
            st.session_state.eval_llm         = None
            st.session_state.eval_risk_reward = None
            st.rerun()

        st.markdown(f"## Results: {ticker}")

        if report:
            # ── Metrics table ─────────────────────────────────────────────────
            st.subheader("Financial Metrics")
            # Truly deprecated metrics — hide from display entirely
            _HIDDEN_KW = {"cash burn", "capital structure"}
            def _is_hidden(name):
                n = str(name).lower()
                return any(d in n for d in _HIDDEN_KW)

            # Build display table — show ALL metrics, info-only rows show "—"
            disp_rows = []
            for r in report:
                mn = r.get("Metric Name", "")
                if _is_hidden(mn):
                    continue
                obt = r.get("Obtained points", "")
                tot = r.get("Total points", "")
                # Info-only rows (no scoring): show value but mark Score as "—"
                score_display = str(obt) if str(obt) != "" else "—"
                max_display   = str(tot) if str(tot) not in ("", "0") else ("—" if str(obt) == "" else "")
                disp_rows.append({
                    "Metric":  mn,
                    "Source":  r.get("Source", ""),
                    "Value":   r.get("Value", ""),
                    "Score":   score_display,
                    "Max":     max_display,
                })

            if disp_rows:
                metrics_display = pd.DataFrame(disp_rows)
                # Colour rejected rows red via styling
                def _style_row(row):
                    if str(row["Score"]).lower() == "rejected":
                        return ["background-color:#fee2e2;color:#991b1b;font-weight:600"] * len(row)
                    if row["Score"] not in ("", "0") and row["Max"] not in ("", "0"):
                        try:
                            pct = float(row["Score"]) / float(row["Max"])
                            if pct == 1.0:
                                return ["background-color:#dcfce7"] * len(row)
                        except Exception:
                            pass
                    return [""] * len(row)
                st.dataframe(
                    metrics_display.style.apply(_style_row, axis=1),
                    use_container_width=True, hide_index=True,
                )

            st.markdown("<br>", unsafe_allow_html=True)
            st.subheader("Score Summary")

            # ── Two-pillar score breakdown ─────────────────────────────────────
            def _pts(k1, k2=None):
                for r in report:
                    n = r["Metric Name"].lower()
                    if r.get("Obtained points") in ("rejected", ""): continue
                    if k2 and k1 in n and k2 in n: return r["Obtained points"]
                    elif not k2 and k1 in n:        return r["Obtained points"]
                return "0"

            def _f(v):
                try:    return float(v)
                except: return 0.0

            # Pillar 1 — Sustainability (35 pts max)
            p1 = (_f(_pts("runway")) + _f(_pts("assets","liabilities")) +
                  _f(_pts("net debt","ebitda")) + _f(_pts("share count")) +
                  _f(_pts("gross margin")) + _f(_pts("expiration")))
            # Pillar 2 — Upside Potential (65 pts max)
            p2 = (_f(_pts("revenue growth")) + _f(_pts("growth-to-val")) +
                  _f(_pts("eps growth")) + _f(_pts("market cap")) +
                  _f(_pts("business model")) + _f(_pts("ceo ownership")) +
                  _f(_pts("buying vs selling")) +
                  _f(_pts("institutional")) + _f(_pts("short float")))

            final_score = p1 + p2
            is_rej      = any(r["Obtained points"] == "rejected" for r in report)
            verdict     = _verdict_from_score(final_score, is_rej)
            vc          = _verdict_color(verdict)

            bar_p1 = min(int(p1 / 34 * 100), 100)
            bar_p2 = min(int(p2 / 66 * 100), 100)
            st.markdown(f"""
<div style="background:#0f172a;padding:20px 24px;border-radius:12px;margin:8px 0">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
    <span style="color:#94a3b8;font-size:0.85em;letter-spacing:0.05em">FINAL SCORE — {ticker}</span>
    <span style="background:{vc};color:white;padding:5px 14px;border-radius:6px;font-weight:bold;font-size:0.95em">{verdict}</span>
  </div>
  <div style="font-size:2.8em;font-weight:800;color:white;margin-bottom:18px">{int(final_score)}<span style="font-size:0.4em;color:#94a3b8;font-weight:400"> / 100</span></div>
  <div style="margin-bottom:12px">
    <div style="display:flex;justify-content:space-between;margin-bottom:4px">
      <span style="color:#60a5fa;font-size:0.82em;font-weight:600">Pillar 1 — Sustainability</span>
      <span style="color:#e2e8f0;font-size:0.82em">{int(p1)} / 35</span>
    </div>
    <div style="background:#1e293b;border-radius:6px;height:10px">
      <div style="background:#3b82f6;width:{bar_p1}%;height:10px;border-radius:6px;transition:width .3s"></div>
    </div>
  </div>
  <div>
    <div style="display:flex;justify-content:space-between;margin-bottom:4px">
      <span style="color:#a78bfa;font-size:0.82em;font-weight:600">Pillar 2 — Upside Potential</span>
      <span style="color:#e2e8f0;font-size:0.82em">{int(p2)} / 65</span>
    </div>
    <div style="background:#1e293b;border-radius:6px;height:10px">
      <div style="background:#8b5cf6;width:{bar_p2}%;height:10px;border-radius:6px;transition:width .3s"></div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

            # Add to watchlist button (no rerun to keep results visible)
            existing_pos = db.get_positions()
            existing_tickers = {p.get("ticker","").upper() for p in existing_pos}
            if ticker in existing_tickers:
                st.info(f"**{ticker}** is already in your position watchlist.")
            else:
                if st.button("📌 Add to Position Watchlist", key="new_add_watchlist"):
                    db.save_position({
                        "ticker":          ticker,
                        "contract":        "",
                        "option_type":     "CALL",
                        "strike":          None,
                        "expiration_date": None,
                        "entry_date":      None,
                        "entry_price":     None,
                        "quantity":        None,
                        "entry_delta":     None,
                        "entry_iv_rank":   None,
                        "entry_thesis_score": int(final_score),
                        "position_type":   "WATCHLIST",
                        "target_return":   "5-10x",
                        "mode":            "WATCHLIST",
                        "notes":           f"Added from thesis analysis. Score: {int(final_score)}, Verdict: {verdict}",
                    })
                    st.toast(f"Added {ticker} to watchlist! Go to Dashboard to manage it.")

            sws = st.session_state.eval_risk_reward or {}
            st.markdown("<br>", unsafe_allow_html=True)
            st.subheader("Risks & Rewards")
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### ✅ Rewards")
                for item in sws.get("rewards", []):
                    st.markdown(
                        f'<p style="color:#28a745;font-weight:600">• {item}</p>',
                        unsafe_allow_html=True,
                    )
            with col2:
                st.markdown("#### ⚠️ Risks")
                for item in sws.get("risks", []):
                    st.markdown(
                        f'<p style="color:#dc3545;font-weight:600">• {item}</p>',
                        unsafe_allow_html=True,
                    )

            llm = st.session_state.eval_llm
            if llm:
                st.markdown("---")
                st.subheader("Business Profile")
                st.markdown("#### 🏢 Company Description")
                st.markdown(
                    f'<div style="background:#f8f9fa;padding:14px;border-radius:8px;'
                    f'white-space:pre-wrap;line-height:1.6">{llm["description"]}</div>',
                    unsafe_allow_html=True,
                )
                st.markdown("#### 💎 Value Proposition")
                st.markdown(
                    f'<div style="background:#f8f9fa;padding:14px;border-radius:8px;'
                    f'white-space:pre-wrap;line-height:1.6">{llm["value_proposition"]}</div>',
                    unsafe_allow_html=True,
                )
                st.markdown("#### 🛡️ Moat Analysis")
                st.markdown(
                    f'<div style="background:#f8f9fa;padding:14px;border-radius:8px;'
                    f'white-space:pre-wrap;line-height:1.6">{llm["moat"]}</div>',
                    unsafe_allow_html=True,
                )


# ===========================================================================
# PAGE: PAST ANALYSES
# ===========================================================================

elif page == "📋 Past Analyses":

    if st.session_state.past_view == "history":
        st.title("Past Analyses")

        master_df = get_eval_master_data()

        if master_df.empty:
            st.info("No analyses yet. Run a **New Analysis** to get started.")
            st.stop()

        # Confirm delete-all flow
        if st.session_state.confirm_delete_all:
            st.warning("⚠️ This will permanently delete ALL analyses. Are you sure?")
            yes_col, no_col, _ = st.columns([1, 1, 5])
            if yes_col.button("✅ Yes, delete all", type="primary"):
                if delete_all_eval_tickers(master_df["Ticker"].tolist()):
                    st.session_state.confirm_delete_all = False
                    st.toast("All analyses deleted.")
                    st.rerun()
            if no_col.button("❌ Cancel"):
                st.session_state.confirm_delete_all = False
                st.rerun()

        # Filters
        fc1, fc2, fc3 = st.columns([1.4, 2.2, 2])
        with fc1:
            search = st.text_input("search", placeholder="Search ticker…",
                                   label_visibility="collapsed").upper()
        with fc2:
            verdict_opts   = sorted(master_df["Verdict"].dropna().unique().tolist())
            verdict_filter = st.multiselect(
                "verdict", verdict_opts,
                placeholder="Filter by verdict…", label_visibility="collapsed",
            )
        with fc3:
            sort_by = st.selectbox(
                "sort",
                ["Score ↓ (best first)", "Score ↑ (worst first)",
                 "Date ↓ (newest)", "Date ↑ (oldest)", "Ticker A→Z"],
                label_visibility="collapsed",
            )

        scores_num = master_df["Score"].apply(safe_float)
        s_min, s_max = int(scores_num.min()), int(scores_num.max())
        score_range = (
            st.slider("Score range", s_min, s_max, (s_min, s_max), format="%d pts")
            if s_min < s_max else (s_min, s_max)
        )

        df = master_df.copy()
        if search:
            df = df[df["Ticker"].str.upper().str.contains(search, na=False)]
        if verdict_filter:
            df = df[df["Verdict"].isin(verdict_filter)]
        df = df[df["Score"].apply(safe_float).between(score_range[0], score_range[1])]

        _sort_map = {
            "Score ↓ (best first)":  ("Score",  False),
            "Score ↑ (worst first)": ("Score",  True),
            "Date ↓ (newest)":       ("date",   False),
            "Date ↑ (oldest)":       ("date",   True),
            "Ticker A→Z":            ("Ticker", True),
        }
        _scol, _sasc = _sort_map[sort_by]
        df["_k"] = df[_scol].apply(safe_float) if _scol == "Score" else df[_scol].astype(str)
        df = df.sort_values("_k", ascending=_sasc, na_position="last").drop(columns=["_k"])
        df = df.reset_index(drop=True)

        _, del_col = st.columns([8, 1])
        with del_col:
            if st.button("🗑️ Delete All"):
                st.session_state.confirm_delete_all = True
                st.rerun()

        st.caption(f"Showing **{len(df)}** of {len(master_df)} analyses")
        st.divider()

        # Build ticker → position mode map for status badges (uses 3-min session cache)
        try:
            _all_pos = _get_positions_cached()
            _pos_status = {}
            for _p in _all_pos:
                _t = _p.get("ticker", "").upper()
                _m = _p.get("mode", "")
                if _t not in _pos_status or _m == "ACTIVE":
                    _pos_status[_t] = _m
        except Exception:
            _pos_status = {}

        if df.empty:
            st.info("No analyses match the current filters.")
        else:
            _STATUS_BADGE = {
                "ACTIVE":    '<span style="background:#166534;color:#fff;padding:1px 6px;border-radius:4px;font-size:0.75em">📊 Portfolio</span>',
                "WATCHLIST": '<span style="background:#1e3a5f;color:#fff;padding:1px 6px;border-radius:4px;font-size:0.75em">📌 Watchlist</span>',
            }

            # ── Bulk action bar — reads checkbox state from previous render ──────
            # Key uses row_num to avoid duplicates when same ticker appears twice.
            _eligible_rows = [
                (rn, row["Ticker"])
                for rn, row in df.iterrows()
                if _pos_status.get(row["Ticker"].upper()) not in ("ACTIVE", "WATCHLIST")
            ]
            # De-duplicate: if same ticker selected from multiple rows, count once
            _selected_tickers_set = {}
            for _rn, _t in _eligible_rows:
                if st.session_state.get(f"chk_{_rn}_{_t}", False):
                    _selected_tickers_set[_t.upper()] = _t  # latest wins; deduped by ticker
            _selected_now = list(_selected_tickers_set.values())

            if _selected_now:
                _ba1, _ba2, _ba3 = st.columns([3, 2.5, 1.2])
                _ba1.markdown(f"**{len(_selected_now)} selected**")
                if _ba2.button(
                    f"📌 Add {len(_selected_now)} to Watchlist",
                    type="primary", key="bulk_wl_add",
                ):
                    _added = 0
                    for _bt in _selected_now:
                        _bscore = db.get_leaps_monitor_score(_bt)
                        db.save_position({
                            "ticker":             _bt,
                            "contract":           "",
                            "option_type":        "CALL",
                            "strike":             None,
                            "expiration_date":    None,
                            "entry_date":         None,
                            "entry_price":        None,
                            "quantity":           None,
                            "entry_delta":        None,
                            "entry_iv_rank":      None,
                            "entry_thesis_score": _bscore,
                            "position_type":      "WATCHLIST",
                            "target_return":      "5-10x",
                            "mode":               "WATCHLIST",
                            "notes":              "Added from Past Analyses (bulk).",
                        })
                        _added += 1
                    for _rn, _t in _eligible_rows:
                        st.session_state.pop(f"chk_{_rn}_{_t}", None)
                    st.toast(f"✅ Added {_added} tickers to watchlist!")
                    st.rerun()
                if _ba3.button("✗ Clear", key="bulk_wl_clear"):
                    for _rn, _t in _eligible_rows:
                        st.session_state.pop(f"chk_{_rn}_{_t}", None)
                    st.rerun()

            hdr = st.columns([0.5, 2.8, 1.8, 1.2, 2.2, 1.4, 1.0, 0.8])
            for txt, c in zip(
                ["", "**Ticker**", "**Date**", "**Score**", "**Verdict**", "", "", ""],
                hdr,
            ):
                c.write(txt)
            st.divider()

            for row_num, row in df.iterrows():
                ticker     = row["Ticker"]
                vc         = _verdict_color(row["Verdict"])
                cur_status = _pos_status.get(ticker.upper(), "")
                rc         = st.columns([0.5, 2.8, 1.8, 1.2, 2.2, 1.4, 1.0, 0.8])
                _already_tracked = cur_status in ("ACTIVE", "WATCHLIST")
                rc[0].checkbox(
                    "", key=f"chk_{row_num}_{ticker}",
                    disabled=_already_tracked,
                    help=None if not _already_tracked else f"{ticker} is already in {cur_status.lower()}",
                )
                _badge = _STATUS_BADGE.get(cur_status, "")
                rc[1].markdown(f"**{ticker}** {_badge}", unsafe_allow_html=True)
                rc[2].write(str(row.get("date", "N/A")))
                rc[3].write(str(row["Score"]))
                rc[4].markdown(
                    f'<span style="color:{vc};font-weight:700">{row["Verdict"]}</span>',
                    unsafe_allow_html=True,
                )

                # Watchlist / Portfolio toggle button — one click, no detail view needed
                if cur_status == "WATCHLIST":
                    if rc[5].button("📌 Watching", key=f"wl_{row_num}_{ticker}",
                                    help="Click to remove from watchlist"):
                        _p = next((p for p in _all_pos if p.get("ticker","").upper() == ticker.upper()), None)
                        if _p:
                            db.delete_position(str(_p["id"]))
                            _pos_status.pop(ticker.upper(), None)
                            st.toast(f"Removed {ticker} from watchlist.")
                            st.rerun()
                elif cur_status == "ACTIVE":
                    rc[5].markdown(
                        '<span style="color:#16a34a;font-size:0.85em">📊 Portfolio</span>',
                        unsafe_allow_html=True,
                    )
                else:
                    if rc[5].button("📌 Watch", key=f"wl_{row_num}_{ticker}",
                                    help="Add to watchlist — monitoring agent will track entry signals"):
                        _score = db.get_leaps_monitor_score(ticker)
                        db.save_position({
                            "ticker":             ticker,
                            "contract":           "",
                            "option_type":        "CALL",
                            "strike":             None,
                            "expiration_date":    None,
                            "entry_date":         None,
                            "entry_price":        None,
                            "quantity":           None,
                            "entry_delta":        None,
                            "entry_iv_rank":      None,
                            "entry_thesis_score": _score,
                            "position_type":      "WATCHLIST",
                            "target_return":      "5-10x",
                            "mode":               "WATCHLIST",
                            "notes":              "Added from Past Analyses.",
                        })
                        st.toast(f"✅ {ticker} added to watchlist! Entry signals will be monitored.")
                        st.rerun()

                if rc[6].button("👁️", key=f"view_{row_num}_{ticker}",
                                help="View full analysis"):
                    st.session_state.past_selected = ticker
                    st.session_state.past_view     = "detail"
                    st.rerun()

                if rc[7].button("🗑️", key=f"del_{row_num}_{ticker}"):
                    if delete_eval_ticker(ticker):
                        st.toast(f"Deleted {ticker}")
                        st.rerun()

    # ── Detail view ──────────────────────────────────────────────────────────
    elif st.session_state.past_view == "detail":
        ticker = st.session_state.past_selected

        btn_c1, btn_c2, btn_c3, btn_c4 = st.columns([1.2, 1.8, 1.8, 1.8])
        if btn_c1.button("← Back"):
            st.session_state.past_view    = "history"
            st.session_state.past_selected = None
            st.rerun()

        # Load current position state for this ticker
        existing_pos     = db.get_positions()
        existing_by_mode = {p.get("ticker","").upper(): p for p in existing_pos}
        cur_pos          = existing_by_mode.get(ticker)
        cur_mode         = cur_pos.get("mode") if cur_pos else None

        # ── Watchlist / Remove button ─────────────────────────────────────────
        if cur_mode == "WATCHLIST":
            if btn_c2.button("✅ In Watchlist — Remove", key="detail_rm"):
                db.delete_position(str(cur_pos["id"]))
                for _k in list(st.session_state.keys()):
                    if _k.startswith(f"wl_rec_{cur_pos['id']}") or _k.startswith(f"wl_sig_{cur_pos['id']}"):
                        del st.session_state[_k]
                st.toast(f"Removed {ticker} from watchlist.")
                st.rerun()
        elif cur_mode == "ACTIVE":
            btn_c2.markdown(
                '<span style="background:#166534;color:#fff;padding:4px 10px;border-radius:6px;font-size:0.9em">📊 In Portfolio</span>',
                unsafe_allow_html=True,
            )
        else:
            if btn_c2.button("📌 Add to Watchlist", key="detail_add"):
                score_from_master = db.get_leaps_monitor_score(ticker)
                db.save_position({
                    "ticker":          ticker,
                    "contract":        "",
                    "option_type":     "CALL",
                    "strike":          None,
                    "expiration_date": None,
                    "entry_date":      None,
                    "entry_price":     None,
                    "quantity":        None,
                    "entry_delta":     None,
                    "entry_iv_rank":   None,
                    "entry_thesis_score": score_from_master,
                    "position_type":   "WATCHLIST",
                    "target_return":   "5-10x",
                    "mode":            "WATCHLIST",
                    "notes":           f"Added from Past Analyses.",
                })
                st.session_state["detail_show_rec"] = ticker
                st.toast(f"Added {ticker} to watchlist! See contract recommendation below.")
                st.rerun()

        # Contract recommendation panel (shown after adding to watchlist)
        if st.session_state.get("detail_show_rec") == ticker:
            with st.container(border=True):
                st.markdown(f"**Recommended LEAPS Contract for {ticker} (40–50% OTM)**")
                with st.spinner("Fetching live options chain..."):
                    _det_rec = recommend_asymmetric(ticker)
                _det_sp  = _det_rec.get("stock_price")
                _det_cs  = _det_rec.get("contracts", [])
                if _det_rec.get("error") and not _det_cs:
                    st.warning(_det_rec["error"])
                elif not _det_cs:
                    st.info("No qualifying contracts found at this time.")
                else:
                    for _ci, _c in enumerate(_det_cs):
                        _lbl = f"**#{_ci+1}** ${_c.get('strike')}C · {_c.get('expiration_date')} · " \
                               f"${_c.get('mid','?')}/share · Δ {_c.get('delta','?')} · " \
                               f"{_c.get('otm_pct','?')}% OTM · {_c.get('dte','?')}d"
                        st.markdown(_lbl)
                        if _ci == 0:
                            st.caption(
                                f"10x target requires stock at +{_c.get('10x_required_move_pct','?')}% "
                                f"from ${_det_sp:.2f}"
                            )
                    st.info("Go to **Dashboard → Watchlist** to monitor entry signals and mark as bought.")
                if st.button("✕ Dismiss", key="detail_dismiss_rec"):
                    del st.session_state["detail_show_rec"]
                    st.rerun()

        # ── In Portfolio button ───────────────────────────────────────────────
        if cur_mode != "ACTIVE":
            if btn_c4.button("📊 Already Bought", key="detail_portfolio"):
                st.session_state["detail_portfolio_form"] = ticker
        else:
            if btn_c4.button("🗑️ Remove from Portfolio", key="detail_rm_portfolio"):
                db.delete_position(str(cur_pos["id"]))
                st.toast(f"Removed {ticker} from portfolio.")
                st.rerun()

        # In-Portfolio entry form
        if st.session_state.get("detail_portfolio_form") == ticker:
            with st.container(border=True):
                st.markdown(f"**Log {ticker} Purchase**")
                pf1, pf2, pf3, pf4 = st.columns(4)
                pf_contract = pf1.text_input("Contract (OCC)", placeholder="e.g. O:NVDA270115C00150000", key="pf_contract")
                pf_price    = pf2.number_input("Entry Price ($/contract)", min_value=0.01, step=0.05, value=1.0, key="pf_price")
                pf_qty      = pf3.number_input("Quantity", min_value=1, step=1, value=1, key="pf_qty")
                pf_date     = pf4.date_input("Entry Date", value=date.today(), key="pf_date")
                pf_strike   = st.number_input("Strike", min_value=0.0, step=0.5, value=0.0, key="pf_strike")
                pf_exp      = st.date_input("Expiration", key="pf_exp")
                _pc1, _pc2, _ = st.columns([1, 1, 4])
                if _pc1.button("💾 Save to Portfolio", key="pf_save", type="primary"):
                    _score = db.get_leaps_monitor_score(ticker)
                    if cur_pos:
                        # Update existing watchlist position → ACTIVE
                        db.update_position(str(cur_pos["id"]), {
                            "contract":        pf_contract,
                            "strike":          pf_strike if pf_strike > 0 else None,
                            "expiration_date": pf_exp,
                            "entry_price":     pf_price,
                            "quantity":        int(pf_qty),
                            "entry_date":      pf_date,
                            "entry_thesis_score": _score,
                            "mode":            "ACTIVE",
                        })
                    else:
                        # Brand-new position
                        db.save_position({
                            "ticker":          ticker,
                            "contract":        pf_contract,
                            "option_type":     "CALL",
                            "strike":          pf_strike if pf_strike > 0 else None,
                            "expiration_date": pf_exp,
                            "entry_date":      pf_date,
                            "entry_price":     pf_price,
                            "quantity":        int(pf_qty),
                            "entry_delta":     None,
                            "entry_iv_rank":   None,
                            "entry_thesis_score": _score,
                            "position_type":   "CORE",
                            "target_return":   "5-10x",
                            "mode":            "ACTIVE",
                            "notes":           "Added from Past Analyses.",
                        })
                    del st.session_state["detail_portfolio_form"]
                    st.toast(f"{ticker} added to portfolio! Monitor it on the Dashboard.")
                    st.rerun()
                if _pc2.button("Cancel", key="pf_cancel"):
                    del st.session_state["detail_portfolio_form"]
                    st.rerun()

        # Re-analyze button
        _analyzing = st.session_state.get("detail_analyzing")
        if btn_c3.button("🔍 Re-analyze", key=f"reanalyze_{ticker}"):
            threading.Thread(target=_single_worker, args=(ticker,), daemon=False).start()
            st.session_state["detail_analyzing"] = ticker
            st.rerun()

        if _analyzing == ticker:
            _job     = _ES["background_jobs"].get(ticker, {})
            _jstatus = _job.get("status")
            if _jstatus == "running":
                st.info(f"⏳ Analyzing **{ticker}**…")
                time.sleep(5)
                st.rerun()
            elif _jstatus in ("complete", "error"):
                del st.session_state["detail_analyzing"]
                with _ES["jobs_lock"]:
                    _ES["background_jobs"].pop(ticker, None)
                st.cache_data.clear()
                if _jstatus == "error":
                    st.error(f"Analysis failed: {_job.get('error','')}")
                st.rerun()

        df = get_eval_ticker_detail(ticker)

        if df.empty:
            st.info(f"No detailed data for **{ticker}** yet. Click **Re-analyze** to run a full analysis.")
        else:
            st.markdown(f"<h2 style='text-align:center'>Analysis: {ticker}</h2>",
                        unsafe_allow_html=True)

            m_col = "Matric name"    if "Matric name"    in df.columns else "Metric Name"
            s_col = "Obtained Score" if "Obtained Score" in df.columns else "Obtained points"
            t_col = "Total score"    if "Total score"    in df.columns else "Total points"

            date_row = df[df[m_col].str.upper() == "DATE"] if m_col in df.columns else pd.DataFrame()
            date_val = date_row["LLM"].iloc[0] if not date_row.empty else "N/A"
            st.caption(f"Analysis Date: {date_val}")

            QUAL = {"Risks","Rewards","Company Description","Value Proposition","Moat Analysis","DATE"}

            def _is_dep(name):
                n = str(name).lower()
                return any(d in n for d in ("cash burn","capital structure"))

            metrics_df = df[~df[m_col].isin(QUAL)].copy()
            # Remove only truly deprecated rows; keep info-only rows (total=0) for display
            metrics_df = metrics_df[~metrics_df[m_col].apply(_is_dep)].copy()
            # Replace empty/zero score values with "—" for info-only rows
            if s_col in metrics_df.columns and t_col in metrics_df.columns:
                mask = metrics_df[t_col].apply(safe_float) == 0
                metrics_df.loc[mask, s_col] = metrics_df.loc[mask, s_col].replace("", "—").fillna("—")
                metrics_df.loc[mask, t_col] = "—"

            # Rename columns for clean display
            col_rename = {m_col: "Metric", "Source": "Source", "Value": "Value",
                          s_col: "Score", t_col: "Max"}
            display_cols = [c for c in [m_col, "Source", "Value", s_col, t_col]
                            if c in metrics_df.columns]
            disp_df = metrics_df[display_cols].rename(columns=col_rename)

            def _style_past_row(row):
                score_val = str(row.get("Score", "")).lower()
                if score_val == "rejected":
                    return ["background-color:#fee2e2;color:#991b1b;font-weight:600"] * len(row)
                try:
                    if float(row["Score"]) == float(row["Max"]) and float(row["Max"]) > 0:
                        return ["background-color:#dcfce7"] * len(row)
                except Exception:
                    pass
                return [""] * len(row)

            score_sum = metrics_df[s_col].apply(safe_float).sum() if s_col in metrics_df.columns else 0
            st.subheader("Financial Metrics")
            st.dataframe(
                disp_df.style.apply(_style_past_row, axis=1),
                use_container_width=True, hide_index=True,
            )
            st.caption(f"Sum of scored rows: **{int(round(score_sum))}** pts")

            # ── Two-pillar score summary (substring matching — robust across BQ schema variants) ──
            def _pillar_score(keywords):
                """Sum obtained scores for metrics whose name contains any of the given keywords."""
                if s_col not in df.columns: return 0
                total = 0.0
                for _, row in df.iterrows():
                    name = str(row.get(m_col, "")).lower()
                    val  = str(row.get(s_col, ""))
                    if val.lower() in ("rejected", "", "nan"): continue
                    if any(kw in name for kw in keywords):
                        total += safe_float(val)
                return int(round(total))

            P1_KW = ["runway", "assets", "liabilities", "net debt / ebitda", "share count",
                     "gross margin", "expiration"]
            P2_KW = ["revenue growth", "growth-to-val", "eps growth", "market cap",
                     "business model", "ceo ownership", "buying vs selling",
                     "institutional", "short float"]

            p1      = _pillar_score(P1_KW)
            p2      = _pillar_score(P2_KW)
            final   = p1 + p2
            is_rej  = "rejected" in df[s_col].astype(str).str.lower().values if s_col in df.columns else False
            verdict = _verdict_from_score(final, is_rej)
            vc      = _verdict_color(verdict)

            bar_p1 = min(int(p1 / 34 * 100), 100)
            bar_p2 = min(int(p2 / 66 * 100), 100)

            st.subheader("Score Summary")
            st.markdown(f"""
<div style="background:#0f172a;padding:20px 24px;border-radius:12px;margin:8px 0">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
    <span style="color:#94a3b8;font-size:0.85em;letter-spacing:0.05em">LEAPS SCORE — {ticker}</span>
    <span style="background:{vc};color:white;padding:5px 14px;border-radius:6px;font-weight:bold;font-size:0.95em">{verdict}</span>
  </div>
  <div style="font-size:2.8em;font-weight:800;color:white;margin-bottom:18px">{final}<span style="font-size:0.4em;color:#94a3b8;font-weight:400"> / 100</span></div>
  <div style="margin-bottom:12px">
    <div style="display:flex;justify-content:space-between;margin-bottom:4px">
      <span style="color:#60a5fa;font-size:0.82em;font-weight:600">Pillar 1 — Sustainability</span>
      <span style="color:#e2e8f0;font-size:0.82em">{p1} / 35</span>
    </div>
    <div style="background:#1e293b;border-radius:6px;height:10px">
      <div style="background:#3b82f6;width:{bar_p1}%;height:10px;border-radius:6px"></div>
    </div>
  </div>
  <div>
    <div style="display:flex;justify-content:space-between;margin-bottom:4px">
      <span style="color:#a78bfa;font-size:0.82em;font-weight:600">Pillar 2 — Upside Potential</span>
      <span style="color:#e2e8f0;font-size:0.82em">{p2} / 65</span>
    </div>
    <div style="background:#1e293b;border-radius:6px;height:10px">
      <div style="background:#8b5cf6;width:{bar_p2}%;height:10px;border-radius:6px"></div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

            def _llm(metric):
                if m_col not in df.columns or "LLM" not in df.columns: return "N/A"
                res = df[df[m_col] == metric]["LLM"]
                return res.iloc[0] if not res.empty and pd.notnull(res.iloc[0]) else "N/A"

            st.markdown("#### 💰 Rewards")
            st.markdown(
                f'<p style="color:#28a745;font-weight:600">{_llm("Rewards")}</p>',
                unsafe_allow_html=True,
            )
            st.markdown("#### 🚨 Risks")
            st.markdown(
                f'<p style="color:#dc3545;font-weight:600">{_llm("Risks")}</p>',
                unsafe_allow_html=True,
            )
            st.markdown("#### 🏭 Company Description")
            st.write(_llm("Company Description"))
            st.markdown("#### 🤝 Value Proposition")
            st.write(_llm("Value Proposition"))
            st.markdown("#### 🛡️ Moat Analysis")
            st.write(_llm("Moat Analysis"))


# ===========================================================================
# PAGE: DASHBOARD
# ===========================================================================

elif page == "📊 Dashboard":
    st.title("Portfolio Dashboard")

    # Check marketdata.app token — read directly from st.secrets, no abstraction layer
    try:
        _md_token = st.secrets["MARKETDATA_TOKEN"]
        _md_token_ok = bool(_md_token and str(_md_token).strip())
    except Exception:
        _md_token = None
        _md_token_ok = False

    if not _md_token_ok:
        _secret_keys = []
        try:
            _secret_keys = list(st.secrets.keys())
        except Exception:
            pass
        st.warning(
            f"**⚠️ MARKETDATA_TOKEN not found in secrets.**  \n"
            f"Keys currently in secrets.toml: `{_secret_keys}`  \n"
            f"Add `MARKETDATA_TOKEN = \"your_token\"` at the **top level** "
            f"(not under any `[section]`) of `.streamlit/secrets.toml`, then restart.",
            icon="⚠️",
        )

    col_refresh, col_bust, col_spacer = st.columns([1, 1, 4])
    with col_refresh:
        force_check = st.button("🔄 Run Check Now", use_container_width=True)
    with col_bust:
        if st.button("🗑️ Refresh Data", use_container_width=True,
                     help="Clear cached prices and re-fetch all positions from scratch"):
            _clear_all_snapshot_cache()
            _invalidate_positions_cache()
            st.rerun()

    positions = _get_positions_cached()

    active    = [p for p in positions if p.get("mode") == "ACTIVE"]
    watchlist = [p for p in positions if p.get("mode") == "WATCHLIST"]

    if not positions:
        st.info("No positions yet. Go to **Add Position** to add your first ticker or existing LEAPS.")
        st.stop()

    # Portfolio summary strip
    _strip_cost = sum(
        (p.get("entry_price") or 0) * (p.get("quantity") or 0) * 100
        for p in active
    )
    _strip_realized = sum(
        float(p.get("proceeds_from_trims") or 0)
        - (p.get("entry_price") or 0) * int(p.get("quantity_trimmed") or 0) * 100
        for p in active
    )
    _strip_net_cost = _strip_cost - sum(float(p.get("proceeds_from_trims") or 0) for p in active)
    _strip_house_ct = sum(
        1 for p in active
        if float(p.get("proceeds_from_trims") or 0) >= (p.get("entry_price") or 0) * (p.get("quantity") or 0) * 100
        and (p.get("entry_price") or 0) > 0
    )

    s1, s2, s3, s4, s5, s6 = st.columns(6)
    s1.metric("Active Positions", len(active))
    s2.metric("Watchlist",        len(watchlist))
    s3.metric("Portfolio Cost",   f"${_strip_cost:,.0f}" if _strip_cost else "N/A")
    s4.metric("Net At-Risk",      f"${max(0, _strip_net_cost):,.0f}" if _strip_cost else "N/A",
              help="Cost basis minus proceeds already recovered via trims")
    s5.metric("Total Realized P&L",
              f"${_strip_realized:+,.0f}" if _strip_realized != 0 else "$0",
              delta=f"{'▲' if _strip_realized >= 0 else '▼'} {abs(_strip_realized):,.0f}" if _strip_realized != 0 else None,
              delta_color="normal" if _strip_realized >= 0 else "inverse")
    s6.metric("House Money", f"{_strip_house_ct} / {len(active)}",
              help="Positions where trims have fully recovered original cost")

    st.markdown("---")

    # ── Active position cards ─────────────────────────────────────────────────
    if active:
        st.subheader("Active Positions")

        # Pre-fetch ALL market data in parallel before rendering any card.
        # This is the key performance win: N positions in ~T seconds instead of N×T.
        with st.spinner("Loading live market data…"):
            _prefetched = _prefetch_active_data(active, force_check=force_check)

        # Batch-fetch thesis scores: one BigQuery query for all tickers at once.
        _active_tickers   = [p.get("ticker", "") for p in active]
        _all_scores       = db.get_all_leaps_scores_with_age(_active_tickers)

        # Pre-compute any missing scores BEFORE the render loop (parallel, single spinner).
        # Avoids blocking spinners inside per-card rendering.
        _missing_score_tickers = [
            p.get("ticker", "") for p in active
            if _all_scores.get(p.get("ticker", "").upper(), (None, None))[0] is None
            and p.get("ticker")
        ]
        if _missing_score_tickers:
            from concurrent.futures import ThreadPoolExecutor as _TPE, as_completed as _ac
            with st.spinner(
                f"Computing first-time thesis scores for "
                f"{', '.join(_missing_score_tickers)} (~30s each)…"
            ):
                with _TPE(max_workers=3) as _ex:
                    _score_futures = {
                        _ex.submit(score_thesis.compute_and_save_score, t): t
                        for t in _missing_score_tickers
                    }
                    for _f in _ac(_score_futures):
                        _t = _score_futures[_f]
                        try:
                            _s, _v = _f.result()
                            if _s is not None:
                                _all_scores[_t.upper()] = (_s, 0)
                        except Exception:
                            pass

        # Persist auto-built contracts for positions that lack one (runs in main thread)
        for _p in active:
            _pid = str(_p.get("id", ""))
            _pf  = _prefetched.get(_pid, {})
            _ct  = _pf.get("contract", "")
            if _ct and not (_p.get("contract") or "").strip():
                try:
                    db.update_position(_pid, {"contract": _ct})
                except Exception:
                    pass

        for pos in active:
            ticker   = pos.get("ticker", "?")
            strike   = pos.get("strike", "?")
            exp      = pos.get("expiration_date", "?")
            qty      = pos.get("quantity", "?")
            ep       = pos.get("entry_price")
            notes    = pos.get("notes", "")
            pos_id   = str(pos.get("id", ""))
            cost_basis = (ep or 0) * (qty or 0) * 100

            # Use pre-fetched data — all API calls already done in parallel above
            pf       = _prefetched.get(pos_id, {})
            contract = pf.get("contract") or _resolve_contract(pos)
            snap     = pf.get("snap") or {}
            snap_error   = snap.get("_error") if snap else None
            dte_fallback = None
            try:
                raw_exp = pos.get("expiration_date")
                if raw_exp:
                    exp_d = raw_exp if isinstance(raw_exp, date) else date.fromisoformat(str(raw_exp))
                    dte_fallback = (exp_d - date.today()).days
            except Exception:
                pass

            _mid_is_live  = snap.get("_mid_is_live", False)
            _mid_reliable = snap.get("_mid_reliable", True)
            _earnings_d   = pf.get("earnings") or {}
            mkt = {
                "mid":                       snap.get("mid"),
                "mid_display":               snap.get("mid"),
                "bid":                       snap.get("bid"),
                "ask":                       snap.get("ask"),
                "delta":                     snap.get("delta"),
                "dte":                       snap.get("dte") or dte_fallback,
                "iv_rank":                   pf.get("iv_rank"),
                "thesis_score":              None,
                "_mid_source":               snap.get("_mid_source", "market"),
                "_mid_is_live":              _mid_is_live,
                "_mid_reliable":             _mid_reliable,
                # Pillar 5 — earnings & news signals
                "earnings_state":            _earnings_state_from_pos(pos),
                "earnings_tone_score":       _earnings_d.get("earnings_tone_score"),
                "earnings_guidance_change":  _earnings_d.get("earnings_guidance_change"),
                "thesis_impact":             _earnings_d.get("thesis_impact"),
                "earnings_tone_delta":       _earnings_d.get("earnings_tone_delta"),
                "news_sentiment_score":      _earnings_d.get("news_sentiment_score"),
            }

            # ── Data quality notes ────────────────────────────────────────────
            _mid_is_live   = mkt.get("_mid_is_live", False)
            _mid_reliable  = mkt.get("_mid_reliable", True)
            _mid_display   = mkt.get("mid_display")
            _mid_source    = mkt.get("_mid_source", "market")
            data_gaps = []
            if not contract:
                data_gaps.append("❌ No contract symbol — add contract details to enable live pricing")
            elif snap_error or not _mid_display:
                data_gaps.append(f"⚠️ Price fetch failed — {snap_error or 'no price data'}. "
                                 "DTE and delta signals still active.")
            elif not _mid_is_live:
                data_gaps.append("ℹ️ No active bid/ask — using last traded price. P&L is approximate.")

            mid      = mkt.get("mid")
            delta    = mkt.get("delta")
            dte_days = mkt.get("dte")
            pnl      = _pnl_pct(ep, mid)

            score, score_age = _all_scores.get(ticker.upper(), (None, None))

            # Thesis score: use for evaluate() only if reasonably fresh (≤90 days).
            # An exit recommendation based on a 4-month-old score is unreliable.
            thesis_for_engine = score
            if score is not None and score_age is not None and score_age > 90:
                thesis_for_engine = None
                data_gaps.append(f"⚠️ Thesis score is {score_age} days old — thesis exit signal disabled. "
                                 "Re-score to re-enable.")

            mkt["thesis_score"] = thesis_for_engine
            position_alerts = evaluate(pos, mkt)

            # DTE is always reliable — hard stops must always show regardless of data gaps.
            # For all other pillars: if we have data gaps that affect them, and the ONLY
            # alerts are from pillars that can't fire reliably, don't show a HOLD label —
            # show INCOMPLETE instead so the user knows they need to get data first.
            _dte_only_alerts = all(
                a.type in ("EXIT_TIME_URGENT", "EXIT_TIME_WARNING", "ROLL_TIME")
                for a in position_alerts
            )
            if position_alerts:
                worst_alert   = max(position_alerts, key=_alert_priority)
                severity      = worst_alert.severity
                posture_label = _POSTURE_LABEL.get(severity, "HOLD")
            else:
                worst_alert   = None
                severity      = "GREEN"
                posture_label = "HOLD"

            # Auto-email EXIT/ROLL signals (once per session per alert key)
            # Only email if data was complete enough for the alert to be reliable.
            _ae = st.session_state.get("alert_email", "")
            if _ae and st.session_state.get("alert_enabled", False):
                _emailed = st.session_state.setdefault("emailed_alerts", set())
                for _alert in position_alerts:
                    if _alert.severity in ("RED", "BLUE"):
                        _ekey = f"{ticker}_{_alert.type}"
                        if _ekey not in _emailed:
                            _ok, _ = send_alert_email(
                                _ae,
                                f"LEAPS Alert: {ticker} — {_alert.subject}",
                                f"<html><body style='font-family:Arial,sans-serif'>"
                                f"<h2>LEAPS Exit Alert — {ticker}</h2>"
                                f"<pre style='background:#f1f3f5;padding:12px;border-radius:4px'>"
                                f"{_alert.body}</pre></body></html>",
                            )
                            if _ok:
                                _emailed.add(_ekey)

            _SEVERITY_COLOR["GREY"] = "#6b7280"
            _POSTURE_EMOJI["GREY"]  = "⚠️"
            color = _SEVERITY_COLOR.get(severity, "#21C55D")
            emoji = _POSTURE_EMOJI.get(severity, "🟢")

            qty_trimmed   = int(pos.get("quantity_trimmed") or 0)
            proceeds      = float(pos.get("proceeds_from_trims") or 0.0)
            qty_remaining = int(qty or 0) - qty_trimmed

            # P&L breakdown — unrealized only when live price is available
            _orig_cost        = (ep or 0) * int(qty or 0) * 100
            _cost_of_trimmed  = (ep or 0) * qty_trimmed * 100
            _realized_pnl     = proceeds - _cost_of_trimmed
            _unrealized_pnl   = ((mid - ep) * qty_remaining * 100) if (mid is not None and ep and ep > 0) else None
            _total_pnl        = (_realized_pnl + _unrealized_pnl) if _unrealized_pnl is not None else _realized_pnl
            _cost_recovery    = (proceeds / _orig_cost * 100) if _orig_cost > 0 else 0.0
            _house_money      = _cost_recovery >= 100.0
            # mid_display already set above — use it for cost-recovery display if live unavailable

            with st.container(border=True):
                h1, h2 = st.columns([5, 1])
                with h1:
                    cb_str   = f"  ·  Cost Basis ${cost_basis:,.0f}" if cost_basis else ""
                    trim_str = ""
                    if qty_trimmed > 0:
                        trim_str = (
                            f"  ·  {qty_remaining} remaining ({qty_trimmed} trimmed"
                            + (f", ${proceeds:,.0f} recovered" if proceeds else "")
                            + ")"
                        )
                    house_badge = (
                        " &nbsp;<span style='background:#21C55D;color:white;padding:2px 8px;"
                        "border-radius:4px;font-size:0.75em;font-weight:bold'>HOUSE MONEY</span>"
                        if _house_money else ""
                    )
                    st.markdown(
                        f"### {ticker} — {exp} ${strike} Call  ·  "
                        f"{qty} contract{'s' if int(qty or 1) > 1 else ''}"
                        f"{cb_str}{trim_str}{house_badge}",
                        unsafe_allow_html=True,
                    )
                with h2:
                    st.markdown(
                        f"<div style='background:{color};color:white;padding:8px 12px;"
                        f"border-radius:8px;text-align:center;font-weight:bold;font-size:1.1em'>"
                        f"{emoji} {posture_label}</div>",
                        unsafe_allow_html=True,
                    )

                # Data gaps warning — shown immediately under posture badge
                if data_gaps:
                    for _gap in data_gaps:
                        _gap_color = "#dc2626" if _gap.startswith("❌") else ("#2563eb" if _gap.startswith("ℹ️") else "#d97706")
                        st.markdown(
                            f"<div style='background:{_gap_color}18;border-left:3px solid {_gap_color};"
                            f"padding:6px 10px;border-radius:4px;font-size:0.82em;color:{_gap_color};"
                            f"margin-bottom:4px'>{_gap}</div>",
                            unsafe_allow_html=True,
                        )

                m1, m2, m3, m4, m5, m6 = st.columns(6)
                # Use mid_display for the price metric (shows last traded price with label)
                # Use mid (engine mid) for P&L — live bid/ask or reliable exchange last-trade
                _mid_source_val = mkt.get("_mid_source", "market")
                _snap_source    = snap.get("_source", "") if snap else ""
                if not _mid_is_live and _mid_display and _mid_reliable:
                    # marketdata.app last-trade — exchange feed, reliable
                    _mid_label = "Last Trade (exchange)"
                    _mid_help  = ("No active market maker (bid/ask = 0). Showing the last traded "
                                  "price from the exchange feed via marketdata.app. "
                                  "P&L is calculated from this price.")
                elif not _mid_is_live and _mid_display and not _mid_reliable:
                    _mid_label = "Last Trade (stale)"
                    _mid_help  = ("No live bid/ask. This is the last traded price from yfinance — "
                                  "it may be weeks or months old (could be your own entry trade). "
                                  "P&L and stop-loss signals are disabled until a reliable price is available.")
                elif _mid_source_val != "market":
                    _mid_label = "Mid (no live mkt)"
                    _mid_help  = "No live bid/ask — showing last available price."
                else:
                    _mid_label = "Current Mid"
                    _mid_help  = None
                m1.metric("Avg. Price",  f"${ep:.2f}"  if ep  else "N/A")
                m2.metric(_mid_label,    f"${_mid_display:.2f}" if _mid_display else "N/A",
                          help=_mid_help)
                m3.metric("Unrlzd P&L",
                          f"{pnl:+.1f}%" if pnl is not None else "N/A",
                          delta_color="normal" if pnl is None else ("normal" if pnl >= 0 else "inverse"),
                          help="Unrealized P&L % vs avg entry price. Only shown when live price is available.")
                m4.metric("Delta",    f"{delta:.2f}" if delta is not None else "N/A")
                m5.metric("DTE",      f"{dte_days}d" if dte_days is not None else "N/A")
                if score is not None:
                    age_label = f" ({score_age}d ago)" if score_age is not None else ""
                    stale_30  = score_age is not None and score_age > 30
                    stale_90  = score_age is not None and score_age > 90
                    m6.metric("Thesis Score", f"{score}/100{age_label}",
                              delta="⚠️ signals disabled — re-score" if stale_90 else
                                    ("⚠️ stale" if stale_30 else None),
                              delta_color="inverse" if stale_30 else "normal")
                else:
                    m6.metric("Thesis Score", "N/A")
                with m6:
                    if st.button("↻ Re-score", key=f"rescore_{pos_id}",
                                 help="Re-compute thesis score using yfinance + Gemini"):
                        with st.spinner(f"Scoring {ticker} (~30s)..."):
                            new_score, new_verdict = score_thesis.compute_and_save_score(ticker)
                        if new_score is not None:
                            st.success(f"Score updated: {new_score}/100 ({new_verdict})")
                            st.rerun()
                        else:
                            st.error("Scoring failed — check logs")

                # ── Thesis trend row ──────────────────────────────────────────
                _entry_score = pos.get("entry_thesis_score")
                _entry_date  = pos.get("entry_date")
                _days_held   = None
                try:
                    if _entry_date:
                        _ed = _entry_date if isinstance(_entry_date, date) else date.fromisoformat(str(_entry_date))
                        _days_held = (date.today() - _ed).days
                except Exception:
                    pass

                if _entry_score or _days_held:
                    th1, th2, th3, th4 = st.columns(4)
                    if _entry_score and score is not None:
                        _gap = score - int(_entry_score)
                        th1.metric("Entry Thesis", f"{_entry_score}/100",
                                   help="Thesis score recorded when position was entered")
                        th2.metric("Current Thesis", f"{score}/100")
                        th3.metric("Thesis Δ", f"{_gap:+d} pts",
                                   delta=f"{'improved' if _gap > 0 else 'declined' if _gap < 0 else 'unchanged'}",
                                   delta_color="normal" if _gap >= 0 else "inverse",
                                   help="Change in thesis score since entry")
                    elif score is not None:
                        th1.metric("Thesis", f"{score}/100")
                        th2.metric("Entry Baseline", "Legacy",
                                   help="Position predates scoring system — no entry baseline. "
                                        "Cost recovery is the primary signal.")
                        th3.metric("Primary Signal", f"{_cost_recovery:.0f}% recovered",
                                   delta="HOUSE MONEY" if _house_money else None,
                                   delta_color="normal")
                    if _days_held is not None:
                        th4.metric("Days Held", f"{_days_held}d",
                                   help=f"Position entered on {str(_entry_date)[:10]}")

                # ── Earnings & News chips ─────────────────────────────────────
                _earnings_date = pos.get("earnings_date")
                _earnings_state = None
                if _earnings_date:
                    try:
                        from monitor_engine.earnings_calendar import get_earnings_state, _to_date
                        _ed_date = _to_date(_earnings_date)
                        _earnings_state = get_earnings_state(ticker, _ed_date)
                    except Exception:
                        pass

                _call_data = None
                try:
                    from monitor_engine.earnings_call_analysis import get_latest_call_data
                    _call_data = get_latest_call_data(ticker)
                except Exception:
                    pass

                _news_chip_cols = []
                if _earnings_state and _earnings_state not in ("unknown", "far"):
                    _state_cfg = {
                        "day_of":    ("#dc2626", "⚠️ EARNINGS TODAY"),
                        "week_of":   ("#b91c1c", "🔴"),
                        "imminent":  ("#2563eb", "🔵"),
                        "approaching": ("#d97706", "🟡"),
                        "post":      ("#16a34a", "🟢"),
                    }
                    _sc, _slabel = _state_cfg.get(_earnings_state, ("#6b7280", "📅"))
                    try:
                        from monitor_engine.earnings_calendar import _to_date
                        _edelta = (_to_date(_earnings_date) - date.today()).days
                        _days_txt = f"{_edelta}d" if _edelta >= 0 else f"{abs(_edelta)}d ago"
                    except Exception:
                        _days_txt = ""
                    _earnings_chip = (
                        f"<span style='background:{_sc};color:white;padding:3px 10px;"
                        f"border-radius:12px;font-size:0.78em;font-weight:600'>"
                        f"📅 {_slabel} earnings {_days_txt}</span>"
                    )
                    _news_chip_cols.append(_earnings_chip)

                if _call_data:
                    _ct = _call_data.get("overall_tone", "NEUTRAL")
                    _cq = _call_data.get("quarter", "")
                    _cg = _call_data.get("forward_guidance", "")
                    _tc, _ti = {"BULLISH": ("#16a34a", "🟢"), "BEARISH": ("#dc2626", "🔴")}.get(_ct, ("#6b7280", "🟡"))
                    _guide_txt = f" · guidance {_cg.lower()}" if _cg and _cg != "NOT_PROVIDED" else ""
                    _call_chip = (
                        f"<span style='background:{_tc};color:white;padding:3px 10px;"
                        f"border-radius:12px;font-size:0.78em;font-weight:600'>"
                        f"{_ti} {_cq}: {_ct.capitalize()}{_guide_txt}</span>"
                    )
                    _news_chip_cols.append(_call_chip)

                if _news_chip_cols:
                    st.markdown(
                        "&nbsp;&nbsp;".join(_news_chip_cols),
                        unsafe_allow_html=True,
                    )

                # ── P&L breakdown row ─────────────────────────────────────────
                if qty_trimmed > 0 or _unrealized_pnl is not None:
                    p1, p2, p3, p4 = st.columns(4)
                    p1.metric(
                        "Realized P&L",
                        f"${_realized_pnl:+,.0f}" if _realized_pnl != 0 else "$0",
                        help="Proceeds from trims minus cost of trimmed contracts",
                        delta_color="normal" if _realized_pnl >= 0 else "inverse",
                    )
                    p2.metric(
                        "Unrealized P&L",
                        f"${_unrealized_pnl:+,.0f}" if _unrealized_pnl is not None else "N/A",
                        help="(Current mid − entry price) × remaining contracts × 100",
                        delta_color="normal" if (_unrealized_pnl or 0) >= 0 else "inverse",
                    )
                    p3.metric(
                        "Total P&L",
                        f"${_total_pnl:+,.0f}" if _total_pnl is not None else "N/A",
                        help="Realized + Unrealized",
                        delta_color="normal" if (_total_pnl or 0) >= 0 else "inverse",
                    )
                    _total_return_pct = (
                        (_total_pnl / _orig_cost * 100) if (_orig_cost > 0 and _total_pnl is not None) else None
                    )
                    p4.metric(
                        "Cost Recovery",
                        f"{_cost_recovery:.0f}%",
                        delta="HOUSE MONEY" if _house_money else (
                            f"{_total_return_pct:+.0f}% total return" if _total_return_pct is not None else None
                        ),
                        delta_color="normal",
                        help="Proceeds from trims ÷ original cost. ≥100% = house money (risk-free remaining contracts)",
                    )

                if snap_error:
                    st.warning(f"⚠️ Live data unavailable: {snap_error}")
                    # If we can't get live price, warn explicitly that stop-loss can't be evaluated
                    if ep and ep > 0:
                        _stop_price = round(ep * 0.40, 2)   # -60% stop level
                        st.error(
                            f"🔴 **Cannot evaluate exit signals without live price.** "
                            f"Check your broker — if current mid is below "
                            f"**${_stop_price:.2f}** (-60% stop), EXIT immediately."
                        )

                if worst_alert:
                    sig_color = _SEVERITY_COLOR.get(worst_alert.severity, "#FFA500")
                    st.markdown(
                        f"<div style='background:{sig_color}22;border-left:4px solid {sig_color};"
                        f"padding:10px 14px;border-radius:4px;margin:8px 0'>"
                        f"<strong>Active Signal:</strong>&nbsp; {worst_alert.subject}</div>",
                        unsafe_allow_html=True,
                    )
                    # Surface limit price directly on card for RED/BLUE signals
                    if worst_alert.severity in ("RED", "BLUE"):
                        _lp_m = re.search(r"limit \$([\d.]+)/share", worst_alert.body or "")
                        _pr_m = re.search(r"Estimated proceeds: ~\$([\d,]+)", worst_alert.body or "")
                        if _lp_m:
                            _lp_str  = _lp_m.group(1)
                            _pr_str  = f"  ·  est. proceeds ~${_pr_m.group(1)}" if _pr_m else ""
                            _av      = "EXIT" if worst_alert.severity == "RED" else "ROLL"
                            _lp_col  = "#dc2626" if worst_alert.severity == "RED" else "#2563eb"
                            st.markdown(
                                f"<div style='background:#1e293b;color:#f8fafc;padding:7px 14px;"
                                f"border-left:4px solid {_lp_col};border-radius:4px;"
                                f"font-size:0.92em;margin:4px 0'>"
                                f"💰 <strong>{_av} limit order:</strong> "
                                f"${_lp_str}/share{_pr_str}</div>",
                                unsafe_allow_html=True,
                            )
                    with st.expander("📋 View recommendation & trade instruction"):
                        st.code(worst_alert.body, language=None)
                        others = [a for a in position_alerts if a is not worst_alert]
                        if others:
                            st.caption(f"Additional signals ({len(others)}):")
                            for a in sorted(others, key=_alert_priority, reverse=True):
                                st.markdown(f"- {_POSTURE_EMOJI.get(a.severity,'•')} {a.subject}")
                else:
                    st.success("✅ All pillars clear — HOLD")

                b1, b2, b3, b4, b5 = st.columns([1, 1, 1, 1, 1])
                with b1:
                    if st.button("Mark Closed", key=f"close_{pos_id}"):
                        db.update_position_mode(pos_id, "CLOSED")
                        st.rerun()
                with b2:
                    if st.button("Mark Rolled", key=f"roll_{pos_id}"):
                        db.update_position_mode(pos_id, "ROLLED")
                        st.rerun()
                with b3:
                    if st.button("✏️ Edit", key=f"edit_{pos_id}"):
                        st.session_state["editing_pos_id"] = pos_id
                        st.session_state.pop("trimming_pos_id", None)
                with b4:
                    if st.button("✂️ Record Trim", key=f"trim_{pos_id}",
                                 help="Record contracts you've sold"):
                        st.session_state["trimming_pos_id"] = pos_id
                        st.session_state.pop("editing_pos_id", None)

                if notes:
                    st.caption(f"Notes: {notes}")

            # ---- Inline Edit Panel ----
            if st.session_state.get("editing_pos_id") == pos_id:
                with st.container(border=True):
                    st.markdown("#### Edit Position")
                    st.caption("Avg. Price = option premium per share from IBKR (NOT the stock price)")
                    ec1, ec2, ec3 = st.columns(3)
                    with ec1:
                        new_ep  = st.number_input("Avg. Price $/share",
                            value=float(ep or 0), min_value=0.01, step=0.01, format="%.2f",
                            key=f"ep_{pos_id}")
                        new_qty = st.number_input("Pos (contracts)",
                            value=int(qty or 1), min_value=1, step=1,
                            key=f"qty_{pos_id}")
                    with ec2:
                        _strike_default = float(pos.get("strike") or 0)
                        new_strike = st.number_input("Strike Price",
                            value=_strike_default, min_value=0.01, step=0.5, format="%.2f",
                            key=f"strike_{pos_id}")
                        _exp_raw = pos.get("expiration_date")
                        try:
                            _exp_default = _exp_raw if isinstance(_exp_raw, date) else date.fromisoformat(str(_exp_raw))
                        except Exception:
                            _exp_default = date.today() + timedelta(days=540)
                        new_exp = st.date_input("Expiration Date", value=_exp_default,
                            min_value=date.today(), key=f"exp_{pos_id}")
                    with ec3:
                        new_mode = st.selectbox("Mode", ["ACTIVE", "WATCHLIST"],
                            index=0 if pos.get("mode") == "ACTIVE" else 1,
                            key=f"mode_{pos_id}")
                        cb_preview = new_ep * new_qty * 100
                        st.metric("Cost Basis (preview)", f"${cb_preview:,.0f}")

                    st.markdown("**Trim tracking**")
                    tr1, tr2 = st.columns(2)
                    with tr1:
                        new_qty_trimmed = st.number_input("Contracts sold so far",
                            value=int(pos.get("quantity_trimmed") or 0),
                            min_value=0, max_value=int(new_qty), step=1,
                            key=f"qtrim_{pos_id}")
                    with tr2:
                        new_proceeds = st.number_input("Total proceeds from trims ($)",
                            value=float(pos.get("proceeds_from_trims") or 0.0),
                            min_value=0.0, step=100.0, format="%.0f",
                            key=f"proc_{pos_id}")
                    if new_qty_trimmed > 0 and new_proceeds > 0:
                        remaining_cost = new_ep * (new_qty - new_qty_trimmed) * 100
                        if new_proceeds >= new_ep * new_qty * 100:
                            st.success(f"HOUSE MONEY — cost fully recovered. "
                                       f"Remaining {int(new_qty) - new_qty_trimmed} contracts cost $0 net.")
                        else:
                            st.info(f"Cost recovered: ${new_proceeds:,.0f}  |  "
                                    f"Remaining net cost: ~${remaining_cost:,.0f}")

                    new_notes = st.text_area("Notes", value=notes or "", key=f"notes_{pos_id}")
                    sv1, sv2  = st.columns([1, 5])
                    with sv1:
                        if st.button("Save Changes", type="primary", key=f"save_{pos_id}"):
                            opt_type_char = (pos.get("option_type") or "CALL")[0].upper()
                            new_contract  = options_data.to_occ(ticker, new_exp, opt_type_char, new_strike)
                            try:
                                db.update_position(pos_id, {
                                    "entry_price":         new_ep,
                                    "quantity":            int(new_qty),
                                    "strike":              new_strike,
                                    "expiration_date":     new_exp,
                                    "contract":            new_contract,
                                    "mode":                new_mode,
                                    "notes":               new_notes,
                                    "quantity_trimmed":    int(new_qty_trimmed),
                                    "proceeds_from_trims": float(new_proceeds),
                                })
                                del st.session_state["editing_pos_id"]
                                st.rerun()
                            except Exception as e:
                                st.error(f"Save failed: {e}")
                    with sv2:
                        if st.button("Cancel", key=f"cancel_{pos_id}"):
                            del st.session_state["editing_pos_id"]
                            st.rerun()

            # ---- Inline Trim Panel ----
            if st.session_state.get("trimming_pos_id") == pos_id:
                with st.container(border=True):
                    st.markdown("#### ✂️ Record Trim")
                    st.caption("Enter contracts sold + price received (IBKR 'Avg. Price' on the sell). Additive.")
                    tr1, tr2 = st.columns(2)
                    with tr1:
                        max_trim      = max(1, qty_remaining)
                        trim_qty_now  = st.number_input("Contracts sold in this trim",
                            min_value=1, max_value=max_trim, step=1,
                            key=f"tnq_{pos_id}",
                            help=f"{qty_remaining} contracts remaining")
                    with tr2:
                        trim_price_now = st.number_input("Sale price per share $/share",
                            min_value=0.01, step=0.01, format="%.2f",
                            key=f"tnp_{pos_id}")

                    this_proceeds      = trim_qty_now * trim_price_now * 100
                    new_total_trimmed  = qty_trimmed + trim_qty_now
                    new_total_proceeds = proceeds + this_proceeds
                    original_cost      = (ep or 0) * (int(qty or 0)) * 100

                    st.info(f"This trim: {trim_qty_now} × ${trim_price_now:.2f} × 100 = **${this_proceeds:,.0f}**")

                    pnl_on_trim = None
                    if ep and trim_price_now and ep > 0:
                        pnl_on_trim = round((trim_price_now - ep) / ep * 100, 1)

                    contracts_left = int(qty or 0) - new_total_trimmed
                    pnl_str = f"  ·  P&L on trim: {pnl_on_trim:+.1f}%" if pnl_on_trim is not None else ""

                    if original_cost > 0 and new_total_proceeds >= original_cost:
                        st.success(
                            f"HOUSE MONEY after this trim!  "
                            f"${new_total_proceeds:,.0f} recovered ≥ ${original_cost:,.0f} cost.  "
                            f"{contracts_left} contracts left cost $0 net.{pnl_str}"
                        )
                    else:
                        net_remaining = max(0, original_cost - new_total_proceeds)
                        st.markdown(
                            f"After trim: **{new_total_trimmed}/{int(qty or 0)}** sold  ·  "
                            f"**${new_total_proceeds:,.0f}** recovered  ·  "
                            f"**${net_remaining:,.0f}** still at risk  ·  "
                            f"**{contracts_left}** remaining{pnl_str}"
                        )

                    tc1, tc2 = st.columns([1, 5])
                    with tc1:
                        if st.button("Save Trim", type="primary", key=f"rtrim_{pos_id}"):
                            try:
                                db.update_position(pos_id, {
                                    "quantity_trimmed":    new_total_trimmed,
                                    "proceeds_from_trims": new_total_proceeds,
                                })
                                del st.session_state["trimming_pos_id"]
                                st.rerun()
                            except Exception as e:
                                st.error(f"Trim save failed: {e}")
                    with tc2:
                        if st.button("Cancel", key=f"tcancel_{pos_id}"):
                            del st.session_state["trimming_pos_id"]
                            st.rerun()

    # ── Watchlist ─────────────────────────────────────────────────────────────
    if watchlist:
        st.markdown("---")
        wl_hdr_c1, wl_hdr_c2 = st.columns([4, 1])
        wl_hdr_c1.subheader("Watchlist — Entry Signals & Contract Recommendations")
        if wl_hdr_c2.button("🔄 Refresh All Signals", key="wl_refresh_all", use_container_width=True):
            # Clear cached watchlist signal data so next render re-fetches
            for k in list(st.session_state.keys()):
                if k.startswith("wl_sig_") or k.startswith("wl_rec_"):
                    del st.session_state[k]
            st.rerun()

        for pos in watchlist:
            ticker = pos.get("ticker", "?")
            pos_id = str(pos.get("id", ""))

            # ── Fetch / use cached signals ─────────────────────────────────
            sig_key = f"wl_sig_{pos_id}"
            rec_key = f"wl_rec_{pos_id}"

            if sig_key not in st.session_state:
                with st.spinner(f"Loading signals for {ticker}..."):
                    try:
                        _sd = get_price_and_range(ticker)
                        _rsi = get_weekly_rsi(ticker)
                        _sd["weekly_rsi"] = _rsi
                        _ivr = None
                        try:
                            from iv_rank import get_iv_rank_advanced
                            _ivr_raw = get_iv_rank_advanced(ticker)
                            if _ivr_raw and "Success" in _ivr_raw:
                                import re as _re
                                _m = _re.search(r"([\d.]+)", _ivr_raw.split("is:")[-1])
                                _ivr = float(_m.group(1)) if _m else None
                        except Exception:
                            pass
                        _entry = evaluate_entry(pos, _sd, _ivr)
                        st.session_state[sig_key] = {
                            "price":      _sd.get("price"),
                            "pfl":        _sd.get("pct_from_low"),
                            "rsi":        _rsi,
                            "iv_rank":    _ivr,
                            "entry":      _entry,
                            "entry_score": _entry.context.get("entry_score", 0) if _entry else 0,
                        }
                    except Exception as _e:
                        st.session_state[sig_key] = {"error": str(_e)}

            if rec_key not in st.session_state:
                with st.spinner(f"Loading contract recs for {ticker}..."):
                    try:
                        st.session_state[rec_key] = recommend_asymmetric(ticker)
                    except Exception as _e:
                        st.session_state[rec_key] = {"error": str(_e), "contracts": []}

            sig = st.session_state.get(sig_key, {})
            rec = st.session_state.get(rec_key, {})

            price   = sig.get("price")
            pfl     = sig.get("pfl")
            rsi     = sig.get("rsi")
            iv_rank = sig.get("iv_rank")
            entry   = sig.get("entry")
            entry_score = sig.get("entry_score", 0)

            thesis_score = db.get_leaps_monitor_score(ticker)

            # Derive BUY/WAIT badge
            if entry and entry.severity == "GREEN":
                action_badge = "🟢 BUY NOW"
                badge_color  = "green"
            elif entry and entry.severity == "AMBER":
                action_badge = "🟡 ENTRY IMPROVING"
                badge_color  = "orange"
            elif entry:
                action_badge = "🔵 LOW IV — CONSIDER ENTRY"
                badge_color  = "blue"
            else:
                action_badge = "⚪ WAIT"
                badge_color  = "gray"

            with st.container(border=True):
                # ── Header row ─────────────────────────────────────────────
                hc1, hc2, hc3, hc4, hc5 = st.columns([2, 2, 2, 3, 1])
                hc1.metric("Ticker", ticker)
                hc2.metric("Price",  f"${price:.2f}" if price else "N/A")
                hc3.metric("Thesis", f"{thesis_score}/100" if thesis_score else "N/A")
                hc4.markdown(
                    f"<div style='padding-top:6px'>"
                    f"<span style='font-size:1.15em;font-weight:700;color:{badge_color}'>{action_badge}</span>"
                    f"<span style='color:#888;font-size:0.85em;margin-left:10px'>Entry score: {entry_score}/100</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
                with hc5:
                    if st.button("✕ Remove", key=f"rm_watch_{pos_id}"):
                        db.delete_position(pos_id)
                        st.rerun()

                # ── Body: signals | contract rec ───────────────────────────
                sc1, sc2 = st.columns(2)

                with sc1:
                    st.markdown("**Entry Signals**")
                    # RSI
                    if rsi is not None:
                        if rsi < 30:
                            st.markdown(f"📊 Weekly RSI: **{rsi:.0f}** 🔥 Oversold — strong buy")
                        elif rsi < 40:
                            st.markdown(f"📊 Weekly RSI: **{rsi:.0f}** ⚠️ Approaching oversold")
                        elif rsi > 70:
                            st.markdown(f"📊 Weekly RSI: **{rsi:.0f}** ⛔ Overbought — wait")
                        else:
                            st.markdown(f"📊 Weekly RSI: **{rsi:.0f}** (neutral)")
                    else:
                        st.markdown("📊 Weekly RSI: N/A")

                    # IV Rank
                    if iv_rank is not None:
                        if iv_rank < 20:
                            st.markdown(f"📉 IV Rank: **{iv_rank:.0f}%** ⭐ Options very cheap — great time to buy")
                        elif iv_rank < 30:
                            st.markdown(f"📉 IV Rank: **{iv_rank:.0f}%** ✅ Options cheap")
                        elif iv_rank < 50:
                            st.markdown(f"📉 IV Rank: **{iv_rank:.0f}%** (neutral)")
                        else:
                            st.markdown(f"📉 IV Rank: **{iv_rank:.0f}%** ⚠️ Options expensive — reduces entry quality")
                    else:
                        st.markdown("📉 IV Rank: N/A")

                    # 52w position
                    if pfl is not None:
                        if pfl < 0.15:
                            st.markdown(f"📍 52w Position: **{pfl*100:.0f}% from low** 🔥 Near 52w low")
                        elif pfl < 0.30:
                            st.markdown(f"📍 52w Position: **{pfl*100:.0f}% from low** (lower third)")
                        else:
                            st.markdown(f"📍 52w Position: **{pfl*100:.0f}% from low**")
                    else:
                        st.markdown("📍 52w Position: N/A")

                    # Entry score bar
                    bar_val = min(entry_score, 100)
                    bar_color = "#16a34a" if bar_val >= 60 else "#f59e0b" if bar_val >= 40 else "#6b7280"
                    st.markdown(
                        f"<div style='margin-top:8px'>"
                        f"<div style='font-size:0.8em;color:#888;margin-bottom:3px'>Entry score: {bar_val}/100</div>"
                        f"<div style='background:#e5e7eb;border-radius:4px;height:8px'>"
                        f"<div style='width:{bar_val}%;background:{bar_color};height:8px;border-radius:4px'></div>"
                        f"</div></div>",
                        unsafe_allow_html=True,
                    )

                with sc2:
                    st.markdown("**Recommended Contract (40–50% OTM)**")
                    contracts = rec.get("contracts", [])
                    if rec.get("error") and not contracts:
                        st.caption(f"⚠️ {rec['error']}")
                    elif not contracts:
                        st.caption("No qualifying contracts found.")
                    else:
                        sp = rec.get("stock_price") or price or 0
                        top = contracts[0]
                        strike    = top.get("strike")
                        exp_date  = top.get("expiration_date")
                        dte_val   = top.get("dte")
                        mid_val   = top.get("mid")
                        delta_val = top.get("delta")
                        oi_val    = top.get("open_interest")
                        otm_pct   = top.get("otm_pct")
                        move10    = top.get("10x_required_move_pct")
                        contract_sym = top.get("contract", "")

                        st.markdown(
                            f"**${strike}C  ·  {exp_date}**  \n"
                            f"Premium: **${mid_val:.2f}**/share  ·  {dte_val}d DTE  \n"
                            f"Delta: **{delta_val:.2f}**  ·  OTM: {otm_pct}%  ·  OI: {oi_val or '—'}  \n"
                            f"10x needs stock at: **+{move10}%** from ${sp:.2f}"
                        )
                        if len(contracts) > 1:
                            with st.expander(f"See {len(contracts)-1} more contract(s)"):
                                for alt in contracts[1:]:
                                    st.markdown(
                                        f"• **${alt.get('strike')}C {alt.get('expiration_date')}** — "
                                        f"${alt.get('mid','?')}/share · Δ {alt.get('delta','?')} · "
                                        f"{alt.get('otm_pct','?')}% OTM"
                                    )

                        # Mark as Bought button
                        buy_key = f"buying_{pos_id}"
                        if st.button("✅ Mark as Bought", key=f"buy_btn_{pos_id}", type="primary"):
                            st.session_state[buy_key] = {
                                "contract":        contract_sym,
                                "strike":          strike,
                                "expiration_date": exp_date,
                                "delta":           delta_val,
                                "mid":             mid_val,
                            }

                # ── Mark as Bought inline form ──────────────────────────────
                buy_key = f"buying_{pos_id}"
                if buy_key in st.session_state:
                    prefill = st.session_state[buy_key]
                    st.markdown("---")
                    st.markdown("**Confirm Purchase Details**")
                    fc1, fc2, fc3, fc4 = st.columns(4)
                    buy_contract  = fc1.text_input("Contract (OCC)", value=prefill.get("contract", ""), key=f"bc_{pos_id}")
                    buy_price     = fc2.number_input("Entry Price ($/contract)", min_value=0.01, step=0.05,
                                                     value=float(prefill.get("mid") or 1.0), key=f"bp_{pos_id}")
                    buy_qty       = fc3.number_input("Quantity", min_value=1, step=1, value=1, key=f"bq_{pos_id}")
                    buy_date      = fc4.date_input("Entry Date", value=date.today(), key=f"bd_{pos_id}")

                    dc1, dc2, _ = st.columns([1, 1, 4])
                    if dc1.button("💾 Confirm & Move to Active", key=f"bconfirm_{pos_id}", type="primary"):
                        db.update_position(pos_id, {
                            "contract":        buy_contract,
                            "strike":          prefill.get("strike"),
                            "expiration_date": prefill.get("expiration_date"),
                            "entry_price":     buy_price,
                            "quantity":        int(buy_qty),
                            "entry_date":      buy_date,
                            "entry_delta":     prefill.get("delta"),
                            "entry_iv_rank":   iv_rank,
                            "entry_thesis_score": thesis_score,
                            "mode":            "ACTIVE",
                        })
                        del st.session_state[buy_key]
                        # Also clear cached signal so dashboard refreshes
                        st.session_state.pop(sig_key, None)
                        st.session_state.pop(rec_key, None)
                        st.toast(f"{ticker} moved to Active Positions!")
                        st.rerun()
                    if dc2.button("Cancel", key=f"bcancel_{pos_id}"):
                        del st.session_state[buy_key]
                        st.rerun()


# ===========================================================================
# PAGE: ADD POSITION
# ===========================================================================

elif page == "➕ Add Position":
    st.title("Add Position")

    tab_new, tab_existing, tab_ibkr = st.tabs([
        "🔍 New Entry — Recommend Options",
        "📋 Existing Position — Already Bought",
        "📥 Import from IBKR",
    ])

    with tab_new:
        st.markdown(
            "Enter a stock ticker you've evaluated. The app will fetch available LEAPS "
            "and recommend the best contracts for your 10x (Moonshot) and 3-5x (Core) strategy."
        )

        ticker_new = st.text_input("Stock Ticker", placeholder="e.g. NVDA", max_chars=10).upper().strip()

        if st.button("Get Recommendations", type="primary", disabled=not ticker_new):
            with st.spinner(f"Fetching LEAPS chain for {ticker_new}..."):
                recs = recommend_options(ticker_new)

            if recs.get("error"):
                st.error(recs["error"])
            else:
                stock_price = recs["stock_price"]
                st.success(f"Current price: **${stock_price:.2f}**")

                moonshots = recs["MOONSHOT"]
                cores     = recs["CORE"]

                if not moonshots and not cores:
                    st.warning("No qualifying LEAPS found.")
                else:
                    col_m, col_c = st.columns(2)

                    with col_m:
                        st.markdown("### Moonshot Picks (10x target)")
                        st.caption("Far OTM · Low delta · High risk / High reward")
                        for i, c in enumerate(moonshots):
                            with st.expander(
                                f"Option {i+1}:  ${c.get('strike')}C  {c.get('expiration_date')}  "
                                f"|  Mid ${c.get('mid','?')}  |  Δ {c.get('delta','?')}",
                                expanded=(i == 0),
                            ):
                                st.code(format_recommendation(c, stock_price))
                                if st.button("Add to Watchlist as Moonshot", key=f"add_moon_{i}"):
                                    db.save_position({
                                        "ticker":          ticker_new,
                                        "contract":        c.get("contract", ""),
                                        "option_type":     "CALL",
                                        "strike":          c.get("strike"),
                                        "expiration_date": c.get("expiration_date"),
                                        "entry_date":      None,
                                        "entry_price":     None,
                                        "quantity":        None,
                                        "entry_delta":     c.get("delta"),
                                        "entry_iv_rank":   None,
                                        "entry_thesis_score": db.get_leaps_monitor_score(ticker_new),
                                        "position_type":   "MOONSHOT",
                                        "target_return":   "10x",
                                        "mode":            "WATCHLIST",
                                        "notes": f"Recommended. Δ={c.get('delta')}, 10x move: {c.get('10x_required_move_pct','?')}%",
                                    })
                                    st.success(f"Added {ticker_new} moonshot to watchlist!")

                    with col_c:
                        st.markdown("### Core Picks (3-5x target)")
                        st.caption("Moderate OTM · Medium delta · Balanced risk / reward")
                        for i, c in enumerate(cores):
                            with st.expander(
                                f"Option {i+1}:  ${c.get('strike')}C  {c.get('expiration_date')}  "
                                f"|  Mid ${c.get('mid','?')}  |  Δ {c.get('delta','?')}",
                                expanded=(i == 0),
                            ):
                                st.code(format_recommendation(c, stock_price))
                                if st.button("Add to Watchlist as Core", key=f"add_core_{i}"):
                                    db.save_position({
                                        "ticker":          ticker_new,
                                        "contract":        c.get("contract", ""),
                                        "option_type":     "CALL",
                                        "strike":          c.get("strike"),
                                        "expiration_date": c.get("expiration_date"),
                                        "entry_date":      None,
                                        "entry_price":     None,
                                        "quantity":        None,
                                        "entry_delta":     c.get("delta"),
                                        "entry_iv_rank":   None,
                                        "entry_thesis_score": db.get_leaps_monitor_score(ticker_new),
                                        "position_type":   "CORE",
                                        "target_return":   "3-5x",
                                        "mode":            "WATCHLIST",
                                        "notes": f"Recommended. Δ={c.get('delta')}, 3x move: {c.get('3x_required_move_pct','?')}%",
                                    })
                                    st.success(f"Added {ticker_new} core to watchlist!")

    with tab_existing:
        st.markdown(
            "You've already bought this option. Enter the details and the app "
            "will preview your current position health."
        )
        st.info(
            "**Field guide (from IBKR):**  "
            "Strike = the option's strike price (e.g. $15)  ·  "
            "Avg. Price = option premium per share you paid (e.g. $3.20)  ·  "
            "Pos = number of contracts  ·  "
            "Cost Basis = Avg. Price × Pos × 100"
        )
        with st.form("add_existing_form"):
            c1, c2 = st.columns(2)
            with c1:
                ticker_ex  = st.text_input("Stock Ticker *", placeholder="NVDA").upper().strip()
                strike_ex  = st.number_input("Strike Price *", min_value=0.0, step=0.5, format="%.2f",
                    help="The option's strike price. NOT your avg price.")
                exp_date   = st.date_input("Expiration Date *", min_value=date.today() + timedelta(days=30))
                opt_type   = st.selectbox("Option Type", ["CALL", "PUT"])
            with c2:
                entry_price_ex = st.number_input("Avg. Price $/share *", min_value=0.01, step=0.01, format="%.2f",
                    help="From IBKR 'Avg. Price'. × 100 = cost per contract.")
                qty_ex     = st.number_input("Pos (contracts) *", min_value=1, step=1, value=1)
                cb_display = entry_price_ex * qty_ex * 100 if entry_price_ex and qty_ex else 0
                st.metric("Cost Basis (computed)", f"${cb_display:,.2f}")
            mode_ex  = st.radio("Mode", ["ACTIVE", "WATCHLIST"], horizontal=True,
                                help="ACTIVE = exit alerts | WATCHLIST = entry signals")
            notes_ex = st.text_area("Notes (optional)")
            submitted = st.form_submit_button("Save Position", type="primary")

        if submitted and ticker_ex and strike_ex and exp_date and entry_price_ex:
            contract_sym = options_data.to_occ(ticker_ex, exp_date, opt_type[0], strike_ex)
            with st.spinner("Fetching live data and saving..."):
                snap  = _get_snapshot_cached(ticker_ex, contract_sym) or {}
                score = db.get_leaps_monitor_score(ticker_ex)

            snap_error = snap.get("_error")
            mid   = snap.get("mid")
            delta = snap.get("delta")
            dte_d = snap.get("dte") or (exp_date - date.today()).days
            pnl   = _pnl_pct(entry_price_ex, mid)

            db.save_position({
                "ticker":             ticker_ex,
                "contract":           contract_sym,
                "option_type":        opt_type,
                "strike":             strike_ex,
                "expiration_date":    exp_date,
                "entry_price":        entry_price_ex,
                "quantity":           int(qty_ex),
                "entry_delta":        delta,
                "entry_iv_rank":      None,
                "entry_thesis_score": score,
                "position_type":      "STANDARD",
                "target_return":      "5-10x",
                "mode":               mode_ex,
                "notes":              notes_ex,
            })

            st.success(
                f"✅ Position saved! {ticker_ex} {exp_date} ${strike_ex}C — "
                f"go to **Dashboard** to monitor it."
            )

            if snap_error:
                st.warning(f"⚠️ Live data unavailable: {snap_error}")

            st.markdown("---")
            st.subheader("Position Snapshot")
            pr1, pr2, pr3, pr4, pr5 = st.columns(5)
            pr1.metric("Current Mid",  f"${mid:.2f}"  if mid   else "N/A")
            pr2.metric("P&L",          f"{pnl:+.1f}%" if pnl is not None else "N/A")
            pr3.metric("Delta",        f"{delta:.2f}" if delta else "N/A")
            pr4.metric("DTE",          f"{dte_d}d"    if dte_d else "N/A")
            pr5.metric("Thesis Score", f"{score}/100" if score else "N/A")

            dummy_pos = {"ticker": ticker_ex, "entry_price": entry_price_ex,
                         "expiration_date": exp_date, "mode": mode_ex}
            dummy_mkt = {"mid": mid, "delta": delta, "dte": dte_d,
                         "iv_rank": None, "thesis_score": score}
            preview_alerts = evaluate(dummy_pos, dummy_mkt) if mode_ex == "ACTIVE" else []

            if preview_alerts:
                worst = max(preview_alerts, key=lambda a: {"RED":3,"BLUE":2,"AMBER":1}.get(a.severity,0))
                st.warning(f"Initial signal: {worst.subject}")
            else:
                st.info("All pillars clear — position looks healthy.")

    # ── Tab 3: Import from IBKR ────────────────────────────────────────────
    with tab_ibkr:
        st.markdown(
            "Upload an Interactive Brokers **Activity Statement** (PDF or CSV) to automatically "
            "detect positions you bought, sold, trimmed, or rolled — and sync them to BigQuery."
        )

        with st.expander("How to export from IBKR", expanded=False):
            st.markdown("""
**PDF — Default Activity Statement:**
1. Log into IBKR Client Portal or TWS
2. Go to **Reports → Activity → Activity Statement**
3. Select your date range (e.g. past 6 months or full year)
4. Choose **PDF** format and download

**CSV — Recommended for best accuracy:**
1. Go to **Reports → Flex Queries**
2. Create a new Flex Query: select **Trades** section, all fields
3. Run the query → download as CSV
4. Upload the CSV file here

The CSV Flex Report is more reliably parsed than the PDF.
            """)

        uploaded = st.file_uploader(
            "Upload IBKR Activity Statement",
            type=["pdf", "csv", "txt"],
            help="PDF or CSV from IBKR Account Management",
        )

        if uploaded is not None:
            file_bytes = uploaded.read()
            filename   = uploaded.name

            with st.spinner(f"Parsing {filename}…"):
                try:
                    from ibkr_parser import parse_file, reconcile_with_bq
                    raw_trades, summaries = parse_file(file_bytes, filename)
                except ImportError:
                    st.error("pdfplumber is not installed. Run: `pip install pdfplumber`")
                    st.stop()
                except Exception as e:
                    st.error(f"Failed to parse file: {e}")
                    st.stop()

            if not raw_trades:
                st.warning(
                    "No option trades were detected in this file. "
                    "Make sure the file contains a **Trades** section with **Options** data. "
                    "Try the CSV Flex Report format for better results."
                )
                st.stop()

            st.success(f"Found **{len(raw_trades)} raw trades** → **{len(summaries)} unique contracts**")

            # ── Raw trades preview ─────────────────────────────────────────
            with st.expander(f"Raw trades ({len(raw_trades)})", expanded=False):
                import pandas as _pd
                raw_rows = [
                    {
                        "Date":       t.trade_date.isoformat(),
                        "Symbol":     t.raw_symbol,
                        "Qty":        t.quantity,
                        "Price":      f"${t.price:.2f}",
                        "Proceeds":   f"${t.proceeds:,.2f}",
                        "Code":       t.code,
                    }
                    for t in raw_trades
                ]
                st.dataframe(_pd.DataFrame(raw_rows), use_container_width=True, hide_index=True)

            # ── Reconcile against existing BQ positions ────────────────────
            bq_positions = _get_positions_cached()
            reconciled   = reconcile_with_bq(summaries, bq_positions)

            # ── Preview table ──────────────────────────────────────────────
            st.subheader("Detected Changes")
            st.caption(
                "Review the changes below. Deselect any row you don't want to apply, "
                "then click **Apply Selected Changes**."
            )

            _ACTION_COLOR = {
                "CREATE": "🟢",
                "UPDATE": "🔵",
                "CLOSE":  "🔴",
                "SKIP":   "⚪",
            }

            apply_rows   = []   # rows the user can select
            display_rows = []   # rows for the preview table

            for i, rec in enumerate(reconciled):
                s   = rec["summary"]
                act = rec["bq_action"]
                chg = rec["changes"]

                # Readable change summary
                chg_desc = []
                if "quantity" in chg:
                    chg_desc.append(f"qty→{chg['quantity']}")
                if "entry_price" in chg:
                    chg_desc.append(f"avg→${chg['entry_price']:.2f}")
                if "quantity_trimmed" in chg:
                    chg_desc.append(f"trimmed→{chg['quantity_trimmed']}")
                if "proceeds_from_trims" in chg:
                    chg_desc.append(f"proceeds→${chg['proceeds_from_trims']:,.2f}")
                if "mode" in chg:
                    chg_desc.append(f"mode→{chg['mode']}")

                display_rows.append({
                    "":        _ACTION_COLOR.get(act, "⚪"),
                    "Action":  act,
                    "Ticker":  s.ticker,
                    "Strike":  f"${s.strike}",
                    "Expiry":  s.expiry.isoformat(),
                    "Type":    s.option_type,
                    "Net Qty": s.net_qty,
                    "Avg Buy": f"${s.avg_buy_price:.2f}" if s.avg_buy_price else "-",
                    "Sold":    s.total_sold or "-",
                    "Proceeds":f"${s.gross_proceeds:,.2f}" if s.gross_proceeds else "-",
                    "Changes": ", ".join(chg_desc) if chg_desc else "(no change)",
                    "⚠️":     rec["conflict"] or "",
                })
                apply_rows.append(rec)

            import pandas as _pd2
            preview_df = _pd2.DataFrame(display_rows)
            st.dataframe(preview_df, use_container_width=True, hide_index=True)

            # ── Selection checkboxes ───────────────────────────────────────
            actionable = [(i, r) for i, r in enumerate(apply_rows)
                          if r["bq_action"] in ("CREATE", "UPDATE", "CLOSE")]

            if not actionable:
                st.info("All positions are already up to date — nothing to apply.")
            else:
                st.markdown(f"**{len(actionable)} change(s) ready to apply:**")
                selected_indices = []
                for i, rec in actionable:
                    s   = rec["summary"]
                    act = rec["bq_action"]
                    lbl = (f"{_ACTION_COLOR.get(act, '⚪')} **{act}** — "
                           f"{s.ticker} {s.expiry} ${s.strike}{s.option_type} "
                           f"(net {s.net_qty} contracts)")
                    chk = st.checkbox(lbl, value=(act != "CLOSE"), key=f"ibkr_chk_{i}")
                    if chk:
                        selected_indices.append(i)

                st.markdown("---")
                col_apply, col_cancel = st.columns([1, 3])

                with col_apply:
                    apply_btn = st.button(
                        f"✅ Apply {len(selected_indices)} Selected Change(s)",
                        type="primary",
                        disabled=not selected_indices,
                        use_container_width=True,
                    )

                if apply_btn and selected_indices:
                    applied = 0
                    errors  = []

                    for idx in selected_indices:
                        rec = apply_rows[idx]
                        s   = rec["summary"]
                        act = rec["bq_action"]
                        chg = rec["changes"]

                        try:
                            if act == "CREATE":
                                # Build a full position record
                                from options_data import to_occ
                                contract_sym = to_occ(
                                    s.ticker, s.expiry,
                                    s.option_type,
                                    s.strike,
                                )
                                score = db.get_leaps_monitor_score(s.ticker)
                                db.save_position({
                                    "ticker":          s.ticker,
                                    "contract":        contract_sym,
                                    "option_type":     "CALL" if s.option_type == "C" else "PUT",
                                    "strike":          s.strike,
                                    "expiration_date": s.expiry,
                                    "entry_price":     s.avg_buy_price,
                                    "quantity":        s.net_qty,
                                    "entry_delta":     None,
                                    "entry_iv_rank":   None,
                                    "entry_thesis_score": score,
                                    "position_type":   "STANDARD",
                                    "target_return":   "5-10x",
                                    "mode":            "ACTIVE",
                                    "quantity_trimmed":     s.total_sold,
                                    "proceeds_from_trims":  s.gross_proceeds,
                                    "notes": f"Imported from IBKR {filename}",
                                })
                                applied += 1

                            elif act in ("UPDATE", "CLOSE"):
                                bq_pos = rec["bq_pos"]
                                if bq_pos and chg:
                                    db.update_position(str(bq_pos["id"]), chg)
                                    applied += 1

                        except Exception as e:
                            errors.append(f"{s.ticker} ${s.strike} {s.expiry}: {e}")

                    _invalidate_positions_cache()

                    if applied:
                        st.success(f"✅ Applied {applied} change(s) to BigQuery.")
                        st.balloons()
                    if errors:
                        for err in errors:
                            st.error(f"Failed: {err}")

                    st.caption("Go to the **Dashboard** to see your updated positions.")


# ===========================================================================
# PAGE: ALERT HISTORY
# ===========================================================================

elif page == "🔔 Alert History":
    st.title("Alert History")

    fc1, fc2, fc3 = st.columns([1.5, 2, 2])
    with fc1:
        filter_ticker = st.text_input("Filter ticker", placeholder="NVDA").upper().strip()
    with fc2:
        filter_severity = st.multiselect("Severity",
            ["RED", "BLUE", "AMBER", "GREEN"], placeholder="All severities")
    with fc3:
        filter_type = st.multiselect("Alert type", [
            "EXIT_THESIS", "EXIT_STOP", "EXIT_TIME_URGENT", "EXIT_TIME_WARNING",
            "ROLL_DELTA", "ROLL_TIME",
            "PROFIT_100", "PROFIT_300", "PROFIT_600", "PROFIT_900",
            "ENTRY_SIGNAL", "ENTRY_WATCH", "DELTA_WARN",
            "IV_EXIT_NOW", "IV_TRIM_NOW", "IV_ROLL_SELL_NOW", "IV_ROLL_BUY_NOW",
            "IV_ENTRY_OPTIMAL", "DAILY_SUMMARY",
        ], placeholder="All types")

    try:
        alerts = db.get_alerts(ticker=filter_ticker or None, limit=300)
    except Exception as e:
        st.error(f"Could not load alerts: {e}")
        alerts = []

    if filter_severity:
        alerts = [a for a in alerts if a.get("severity") in filter_severity]
    if filter_type:
        alerts = [a for a in alerts if a.get("alert_type") in filter_type]

    st.caption(f"Showing {len(alerts)} alerts")

    if not alerts:
        st.info("No alerts yet. Alerts appear here once the monitoring loop runs.")
    else:
        for a in alerts:
            sev   = a.get("severity", "GREEN")
            color = _SEVERITY_COLOR.get(sev, "#21C55D")
            emoji = _POSTURE_EMOJI.get(sev, "🟢")
            ts    = a.get("triggered_at", "")
            if isinstance(ts, datetime):
                ts = ts.strftime("%b %d, %Y  %H:%M")

            with st.expander(
                f"{emoji}  {ts}  |  {a.get('ticker','')}  |  {a.get('alert_type','')}",
                expanded=False,
            ):
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("P&L at Alert",
                          f"{a.get('current_pnl_pct'):+.1f}%" if a.get("current_pnl_pct") is not None else "N/A")
                c2.metric("Delta",  f"{a.get('current_delta'):.2f}" if a.get("current_delta") else "N/A")
                c3.metric("DTE",    f"{a.get('current_dte')}d"      if a.get("current_dte")   else "N/A")
                c4.metric("Thesis", f"{a.get('current_thesis_score')}/100" if a.get("current_thesis_score") else "N/A")
                st.code(a.get("body", ""), language=None)
                st.caption("✅ Email sent" if a.get("email_sent") else "❌ Email failed")


# ===========================================================================
# PAGE: SETTINGS
# ===========================================================================

elif page == "⚙️ Settings":
    st.title("Settings")

    # ── IBKR Activity Sync ────────────────────────────────────────────────────
    st.subheader("📥 Sync IBKR Activity Statement")
    st.caption(
        "Upload your IBKR Activity Statement CSV (or PDF) to sync quantities, entry prices, "
        "trims, rolls, and closures — options activity only. Re-upload at any time; "
        "only actual changes are written to BigQuery."
    )

    # ── Portfolio metrics panel ────────────────────────────────────────────
    _pf_all  = _get_positions_cached()
    _pf_act  = [p for p in _pf_all if str(p.get("mode", "")).upper() == "ACTIVE"]
    if _pf_act:
        _pf_cost      = sum(float(p.get("entry_price") or 0) * int(p.get("quantity") or 0) * 100
                            for p in _pf_act)
        _pf_proceeds  = sum(float(p.get("proceeds_from_trims") or 0) for p in _pf_act)
        _pf_net_cost  = max(_pf_cost - _pf_proceeds, 0)
        _pf_c1, _pf_c2, _pf_c3, _pf_c4 = st.columns(4)
        _pf_c1.metric("Active Positions", len(_pf_act))
        _pf_c2.metric("Gross Cost Basis", f"${_pf_cost:,.0f}",
                      help="Sum of entry_price × contracts × 100 for all active positions")
        _pf_c3.metric("Trim Proceeds Collected", f"${_pf_proceeds:,.0f}",
                      help="Total cash received from partial sells / trims so far")
        _pf_c4.metric("Net Capital at Risk", f"${_pf_net_cost:,.0f}",
                      help="Gross cost minus proceeds already collected (money still at risk)")

    with st.expander("How to export from IBKR", expanded=False):
        st.markdown("""
**CSV Flex Report (recommended — best parsing accuracy):**
1. Log into IBKR Client Portal
2. Go to **Reports → Flex Queries → Create** (or run existing)
3. Select **Trades** section, include all columns
4. Run → download as **CSV**

**Default Activity Statement (CSV or PDF):**
1. Go to **Reports → Activity → Activity Statement**
2. Select your full date range
3. Download as **CSV** (preferred) or **PDF**
4. Upload here

> The CSV Flex Report is more reliably parsed. Use a date range that covers your full history.
        """)

    # ── Show persistent apply result from previous run ─────────────────────
    if "ibkr_result" in st.session_state:
        _res = st.session_state["ibkr_result"]
        if _res.get("applied", 0) > 0:
            st.success(
                f"Sync complete — **{_res['applied']}** position(s) updated from `{_res.get('filename','')}`"
                + (f", {_res['baseline_set']} thesis baseline(s) set." if _res.get("baseline_set") else ".")
            )
            if _res.get("log"):
                st.dataframe(pd.DataFrame(_res["log"]), use_container_width=True, hide_index=True)
        for _e in _res.get("errors", []):
            st.error(f"Failed: {_e}")
        if not _res.get("applied") and not _res.get("errors"):
            st.warning("No changes were written — positions may already be up to date or no matching records found.")
        if st.button("Dismiss", key="ibkr_result_dismiss"):
            st.session_state.pop("ibkr_result", None)
            st.rerun()
        st.divider()

    _ibkr_upload = st.file_uploader(
        "Select your IBKR Activity Statement",
        type=["csv", "txt", "pdf"],
        key="settings_ibkr_upload",
        help="CSV Flex Report, default Activity Statement CSV, or Activity Statement PDF",
    )

    _ibkr_parse_col, _ibkr_clear_col = st.columns([3, 1])
    _ibkr_parse_btn = _ibkr_parse_col.button(
        "📤 Parse File",
        type="primary",
        disabled=(_ibkr_upload is None),
        key="settings_ibkr_parse_btn",
    )
    if _ibkr_clear_col.button("✖ Clear", key="settings_ibkr_clear_btn",
                               disabled="ibkr_sync" not in st.session_state):
        # clean up checkbox keys too
        for _k in list(st.session_state.keys()):
            if _k.startswith("ibkr_chk_"):
                del st.session_state[_k]
        st.session_state.pop("ibkr_sync", None)
        st.rerun()

    # ── Step 1: Parse on button click → store in session_state ────────────
    if _ibkr_upload is not None and _ibkr_parse_btn:
        _bytes    = _ibkr_upload.read()
        _filename = _ibkr_upload.name
        with st.spinner(f"Parsing {_filename}…"):
            try:
                from ibkr_parser import parse_file as _ibkr_parse, reconcile_with_bq as _ibkr_reconcile
                _trades, _summaries = _ibkr_parse(_bytes, _filename)
                _bq_positions       = _get_positions_cached()
                _reconciled         = _ibkr_reconcile(_summaries, _bq_positions)
                # clear stale checkbox state from any prior run
                for _k in list(st.session_state.keys()):
                    if _k.startswith("ibkr_chk_"):
                        del st.session_state[_k]
                st.session_state["ibkr_sync"] = {
                    "trades":     _trades,
                    "summaries":  _summaries,
                    "reconciled": _reconciled,
                    "filename":   _filename,
                    "bytes":      _bytes,
                }
                st.rerun()
            except ImportError:
                st.error("pdfplumber is required for PDF parsing. Run: `pip install pdfplumber`")
            except Exception as _pe:
                st.error(f"Failed to parse file: {_pe}")

    # ── Step 2: Show results (persists across rerenders via session_state) ─
    if "ibkr_sync" in st.session_state:
        _sync       = st.session_state["ibkr_sync"]
        _trades     = _sync["trades"]
        _reconciled = _sync["reconciled"]
        _filename   = _sync["filename"]
        _bytes      = _sync["bytes"]

        if not _trades:
            st.warning("No option trades detected.")
            try:
                from ibkr_parser import diagnose_csv as _ibkr_diagnose
                _diag = _ibkr_diagnose(_bytes)
                with st.expander("🔍 File diagnostics", expanded=True):
                    st.markdown(f"**Sections found:** `{'`, `'.join(_diag['sections']) or 'none'}`")
                    st.markdown(f"**Trades section:** {'✅ yes' if _diag['has_trades_section'] else '❌ no'}")
                    if _diag['asset_categories']:
                        st.markdown(f"**Asset categories:** `{'`, `'.join(_diag['asset_categories'])}`")
                    if _diag['symbol_samples']:
                        st.markdown(f"**Option symbols found:** `{'`, `'.join(_diag['symbol_samples'])}`")
                    if _diag['first_rows']:
                        st.markdown("**First rows:**")
                        for _r in _diag['first_rows']:
                            st.code(_r)
                    st.caption("Expected: `Trades,Data,Equity and Index Options,USD,AEHR 15JAN27 40 C,...`")
            except Exception:
                pass
        else:
            st.success(f"Parsed **{len(_trades)} trades** across **{len(_sync['summaries'])} contracts** from `{_filename}`")

            with st.expander(f"Raw trades ({len(_trades)})", expanded=False):
                st.dataframe(pd.DataFrame([
                    {"Date": t.trade_date.isoformat(), "Symbol": t.raw_symbol,
                     "Qty": t.quantity, "Price": f"${t.price:.2f}",
                     "Proceeds": f"${t.proceeds:,.2f}", "Code": t.code}
                    for t in _trades
                ]), use_container_width=True, hide_index=True)

            # ── Build actionable list ──────────────────────────────────────
            _ACT_ICON = {"CREATE": "🟢", "UPDATE": "🔵", "CLOSE": "🔴", "SKIP": "⚪"}
            _actionable = [(i, r) for i, r in enumerate(_reconciled)
                           if r["bq_action"] in ("CREATE", "UPDATE", "CLOSE")]

            if not _actionable:
                st.info("All positions are already up to date — nothing to apply.")
            else:
                st.subheader(f"Detected Changes ({len(_actionable)})")

                # ── Per-row checkboxes stored in session_state ─────────────
                # Default: apply CREATE/UPDATE, skip CLOSE (safer default)
                for _ci, (_orig_idx, _rec) in enumerate(_actionable):
                    _s   = _rec["summary"]
                    _act = _rec["bq_action"]
                    _chg = _rec["changes"]
                    _chg_parts = []
                    if "quantity"            in _chg: _chg_parts.append(f"qty→{_chg['quantity']}")
                    if "entry_price"         in _chg: _chg_parts.append(f"avg→${_chg['entry_price']:.2f}")
                    if "quantity_trimmed"    in _chg: _chg_parts.append(f"trimmed→{_chg['quantity_trimmed']}")
                    if "proceeds_from_trims" in _chg: _chg_parts.append(f"proceeds→${_chg['proceeds_from_trims']:,.2f}")
                    if "mode"                in _chg: _chg_parts.append(f"mode→{_chg['mode']}")
                    _note = _rec["conflict"] or ("ROLL" if _s.action in ("ROLLED", "ROLLED_INTO") else "")
                    _chk_key = f"ibkr_chk_{_ci}"
                    if _chk_key not in st.session_state:
                        st.session_state[_chk_key] = (_act != "CLOSE")
                    _label = (
                        f"{_ACT_ICON.get(_act,'⚪')} **{_act}** — "
                        f"{_s.ticker}  ${_s.strike} {_s.expiry} {_s.option_type}  "
                        f"×{_s.net_qty}"
                        + (f"  |  {', '.join(_chg_parts)}" if _chg_parts else "")
                        + (f"  ⚠️ {_note}" if _note else "")
                    )
                    st.checkbox(_label, key=_chk_key)

                st.markdown("---")
                _n_selected = sum(
                    1 for _ci in range(len(_actionable))
                    if st.session_state.get(f"ibkr_chk_{_ci}", False)
                )
                _apply_btn = st.button(
                    f"✅ Apply {_n_selected} Change(s)",
                    type="primary",
                    disabled=(_n_selected == 0),
                    use_container_width=True,
                    key="settings_ibkr_apply_btn",
                )

                if _apply_btn:
                    _applied = 0
                    _errors  = []
                    _log     = []

                    for _ci, (_orig_idx, _rec) in enumerate(_actionable):
                        if not st.session_state.get(f"ibkr_chk_{_ci}", False):
                            continue
                        _s   = _rec["summary"]
                        _act = _rec["bq_action"]
                        _chg = _rec["changes"]
                        try:
                            if _act == "CREATE":
                                from options_data import to_occ as _to_occ
                                db.save_position({
                                    "ticker":              _s.ticker,
                                    "contract":            _to_occ(_s.ticker, _s.expiry, _s.option_type, _s.strike),
                                    "option_type":         "CALL" if _s.option_type == "C" else "PUT",
                                    "strike":              _s.strike,
                                    "expiration_date":     _s.expiry,
                                    "entry_price":         _s.avg_buy_price,
                                    "quantity":            _s.net_qty,
                                    "entry_delta":         None,
                                    "entry_iv_rank":       None,
                                    "entry_thesis_score":  db.get_leaps_monitor_score(_s.ticker),
                                    "position_type":       "STANDARD",
                                    "target_return":       "5-10x",
                                    "mode":                "ACTIVE",
                                    "quantity_trimmed":    _s.total_sold,
                                    "proceeds_from_trims": _s.gross_proceeds,
                                    "notes":               f"Imported from IBKR {_filename}",
                                })
                                _applied += 1
                                _log.append({"Ticker": _s.ticker, "Action": "CREATED",
                                             "Detail": f"${_s.strike} {_s.expiry} ×{_s.net_qty}"})
                            elif _act in ("UPDATE", "CLOSE"):
                                _bq = _rec["bq_pos"]
                                if _bq and _chg:
                                    db.update_position(str(_bq["id"]), _chg)
                                    _applied += 1
                                    _log.append({"Ticker": _s.ticker, "Action": _act,
                                                 "Detail": " | ".join(f"{k}→{v}" for k, v in _chg.items())})
                                elif not _bq:
                                    _errors.append(f"{_s.ticker} ${_s.strike} {_s.expiry}: no matching position found in BigQuery")
                                elif not _chg:
                                    _errors.append(f"{_s.ticker} ${_s.strike} {_s.expiry}: no fields to update")
                        except Exception as _err:
                            _errors.append(f"{_s.ticker} ${_s.strike} {_s.expiry}: {_err}")

                    # Backfill entry_thesis_score for newly created positions
                    _baseline_set = 0
                    try:
                        _master_scores = {
                            row["Ticker"].upper(): int(row["Score"])
                            for _, row in get_eval_master_data().iterrows()
                            if row.get("Score") is not None
                        }
                        for _rp in db.get_positions():
                            if _rp.get("entry_thesis_score"):
                                continue
                            _rt = (_rp.get("ticker") or "").upper()
                            if _rt in _master_scores:
                                db.update_position(str(_rp["id"]), {"entry_thesis_score": _master_scores[_rt]})
                                _baseline_set += 1
                    except Exception:
                        pass

                    _invalidate_positions_cache()
                    st.cache_data.clear()

                    # Clean up checkbox keys
                    for _k in list(st.session_state.keys()):
                        if _k.startswith("ibkr_chk_"):
                            del st.session_state[_k]

                    # Store result for display after rerun, then clear sync
                    st.session_state["ibkr_result"] = {
                        "applied":      _applied,
                        "errors":       _errors,
                        "log":          _log,
                        "baseline_set": _baseline_set,
                        "filename":     _filename,
                    }
                    st.session_state.pop("ibkr_sync", None)
                    st.rerun()



    # ── Thesis Rescore ────────────────────────────────────────────────────────
    st.subheader("🔄 Rescore All Analyses")
    st.caption(
        "Re-applies the **current scoring rules** to stored BigQuery values. "
        "No new API calls — fast, and fixes score discrepancies from rule changes. "
        "**If scores look wrong** (e.g. incorrect values for Revenue Growth, Runway), "
        "use **🚀 Re-analyze All Tickers** below to fetch fresh data."
    )

    if st.button("🔄 Rescore All", type="primary"):
        master_df = get_eval_master_data()
        if master_df.empty:
            st.warning("No analyses found.")
        else:
            tickers = master_df["Ticker"].tolist()
            results = []
            prog    = st.progress(0.0)
            status  = st.empty()
            for i, ticker in enumerate(tickers):
                status.info(f"Rescoring **{ticker}** ({i+1}/{len(tickers)})…")
                old_s, new_s, old_v, new_v = rescore_ticker_in_bq(ticker)
                results.append({
                    "Ticker":      ticker,
                    "Old Score":   old_s,
                    "New Score":   new_s,
                    "Score Δ":     new_s - old_s,
                    "Old Verdict": old_v,
                    "New Verdict": new_v,
                    "Changed":     "✅" if (old_s != new_s or old_v != new_v) else "—",
                })
                prog.progress((i + 1) / len(tickers))

            st.session_state.rescore_results = results
            st.cache_data.clear()
            prog.empty()

            changed_count = sum(1 for r in results if r["Changed"] == "✅")
            error_count   = sum(1 for r in results if "⚠️ Error" in str(r.get("Old Verdict", "")))
            if error_count:
                status.warning(
                    f"Rescored **{len(tickers)}** tickers — "
                    f"**{changed_count}** updated, **{error_count}** had errors. "
                    f"See 'New Verdict' column below for error details."
                )
            elif changed_count:
                status.success(f"✅ Rescored **{len(tickers)}** tickers — **{changed_count}** scores updated.")
            else:
                status.info(f"✅ Rescored **{len(tickers)}** tickers — all scores are already up to date with current rules.")

            # Email alerts for active positions that changed
            if st.session_state.alert_enabled and st.session_state.alert_email:
                active_tickers = {p["ticker"].upper() for p in db.get_positions(mode="ACTIVE") if p.get("ticker")}
                trigger        = st.session_state.alert_trigger
                changed        = []
                for r in results:
                    if r["Ticker"].upper() not in active_tickers:
                        continue
                    vdict = r["Old Verdict"] != r["New Verdict"]
                    sdiff = abs(r["Score Δ"])
                    if (
                        (trigger == "Verdict changes"          and vdict) or
                        (trigger == "Score changes by ≥ 5 pts" and sdiff >= 5) or
                        (trigger == "Both"                     and (vdict or sdiff >= 5))
                    ):
                        changed.append(r)

                if changed:
                    rows_html = "".join(
                        f"<tr><td>{a['Ticker']}</td><td>{a['Old Score']}</td>"
                        f"<td>{a['New Score']}</td><td>{a['Score Δ']:+d}</td>"
                        f"<td>{a['Old Verdict']}</td><td>{a['New Verdict']}</td></tr>"
                        for a in changed
                    )
                    body = (
                        f'<html><body style="font-family:Arial,sans-serif;">'
                        f'<h2 style="color:#1a1a2e">LEAPS — Score Alert</h2>'
                        f'<table border="1" cellpadding="8" style="border-collapse:collapse">'
                        f'<tr style="background:#f1f3f5"><th>Ticker</th><th>Old</th>'
                        f'<th>New</th><th>Δ</th><th>Old Verdict</th><th>New Verdict</th></tr>'
                        f'{rows_html}</table></body></html>'
                    )
                    ok, err = send_alert_email(
                        st.session_state.alert_email,
                        f"LEAPS: {len(changed)} position(s) changed",
                        body,
                    )
                    if ok:
                        st.success(f"Alert sent for {len(changed)} position(s).")
                    else:
                        st.warning(f"Email failed: {err}")

    if st.session_state.rescore_results:
        _rs_df  = pd.DataFrame(st.session_state.rescore_results)
        _errors = _rs_df[_rs_df["Old Verdict"].astype(str).str.contains("⚠️ Error", na=False)]
        _changed_rs = _rs_df[_rs_df["Changed"] == "✅"]
        _no_detail  = _rs_df[
            (_rs_df["Changed"] == "—") &
            (_rs_df["Old Score"] == _rs_df["New Score"])
        ]

        st.markdown("#### Last Rescore Results")
        if not _errors.empty:
            with st.expander(f"⚠️ {len(_errors)} ticker(s) with errors — click to expand", expanded=True):
                for _, _er in _errors.iterrows():
                    st.error(f"**{_er['Ticker']}**: {_er['New Verdict']}")

        if not _changed_rs.empty:
            st.markdown(f"**{len(_changed_rs)} score(s) updated:**")
        st.dataframe(_rs_df[["Ticker", "Old Score", "New Score", "Score Δ", "Old Verdict", "New Verdict", "Changed"]],
                     use_container_width=True, hide_index=True)

    st.divider()

    # ── Full Re-analyze All Tickers (with API calls, rate-limited) ──────────────
    st.subheader("🚀 Re-analyze All Tickers (Live Data)")
    st.caption(
        "Runs the full 7-source pipeline for **every ticker** in the database "
        "(active + past analyses). Uses an 8-second delay between tickers to avoid rate-limiting. "
        "Runs in the background — safe to navigate away."
    )
    rall1, rall2 = st.columns([1, 3])
    with rall1:
        reanalyze_all = st.button("🚀 Re-analyze All", type="primary",
                                  help="Full 7-source re-analysis for all ~150 tickers (~20 min total).")
    with rall2:
        _bj_ref = _ES["batch_job"]
        _bl_ref = _ES["batch_lock"]
        with _bl_ref:
            _bj_status  = _bj_ref.get("status", "idle")
            _bj_done    = _bj_ref.get("done", 0)
            _bj_total   = _bj_ref.get("total", 0)
            _bj_current = _bj_ref.get("current", "")
            _bj_results = list(_bj_ref.get("results", []))
        if _bj_status == "running":
            _pct = (_bj_done / _bj_total) if _bj_total else 0
            st.progress(_pct, text=f"Analyzing {_bj_current} ({_bj_done}/{_bj_total})…")
        elif _bj_status == "complete" and _bj_results:
            st.success(f"Done — {_bj_done}/{_bj_total} tickers analyzed.")

    if reanalyze_all:
        master_df = get_eval_master_data()
        all_tickers = master_df["Ticker"].tolist() if not master_df.empty else []
        if not all_tickers:
            st.warning("No tickers found in database.")
        else:
            with _ES["batch_lock"]:
                if _ES["batch_job"].get("status") == "running":
                    st.warning("A batch job is already running.")
                else:
                    _ES["batch_job"] = {
                        "status": "running", "total": len(all_tickers),
                        "done": 0, "current": "", "results": [], "error": None,
                    }
                    threading.Thread(
                        target=_batch_worker, args=(all_tickers, 8), daemon=False
                    ).start()
                    st.info(f"Started re-analysis of {len(all_tickers)} tickers in background (8s delay). Refresh to check progress.")
                    st.cache_data.clear()

    if _bj_status == "complete" and _bj_results:
        st.markdown("#### Last Batch Results")
        st.dataframe(pd.DataFrame(_bj_results), use_container_width=True, hide_index=True)

    st.divider()

    # ── Full Re-score (with API calls) ────────────────────────────────────────
    st.subheader("↻ Re-score Active Positions (Full Pipeline)")
    rs1, rs2 = st.columns([1, 3])
    with rs1:
        rescore_full = st.button("↻ Rescore All Active",
                                 help="Re-score every active position via Gemini + Google Search (~30s/ticker).")
    with rs2:
        st.caption("Runs the full 7-source pipeline (yfinance, Finviz, GuruFocus, Gemini, SWS, IV Rank, EPS).")

    if rescore_full:
        try:
            active_tickers = list({p["ticker"] for p in db.get_positions(mode="ACTIVE") if p.get("ticker")})
        except Exception as e:
            st.error(f"Could not fetch positions: {e}")
            active_tickers = []

        if not active_tickers:
            st.info("No active positions to rescore.")
        else:
            results = []
            prog    = st.progress(0.0, text=f"Rescoring 0/{len(active_tickers)}...")
            for i, tk in enumerate(active_tickers):
                prog.progress(i / len(active_tickers), text=f"Scoring {tk} ({i+1}/{len(active_tickers)})...")
                s, v = score_thesis.compute_and_save_score(tk)
                results.append({"Ticker": tk, "Score": s, "Verdict": v,
                                 "Status": "✅" if s else "❌"})
            prog.progress(1.0, text="Done!")
            st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)
            st.success(f"Rescored {len([r for r in results if r['Score']])} of {len(active_tickers)} tickers.")
            st.cache_data.clear()

    st.divider()

    # ── Position Manager email ─────────────────────────────────────────────────
    st.subheader("📧 Position Manager Alerts (Exit / Entry)")
    from shared.config import cfg as _cfg
    _sender    = _cfg("GMAIL_SENDER") or _cfg("ALERT_EMAIL_FROM") or "not configured"
    _recipient = _cfg("ALERT_RECIPIENT_EMAIL") or _sender
    if _sender != "not configured":
        st.info(f"Alerts sent from **{_sender}** to **{_recipient}**  \n"
                f"Change via Streamlit Cloud → App settings → Secrets.")
    else:
        st.warning("Gmail not configured. Add **GMAIL_SENDER** and **GMAIL_APP_PASSWORD** to Streamlit Cloud secrets.")

    _email_btn_c1, _email_btn_c2 = st.columns(2)
    if _email_btn_c1.button("✉️ Send Test Email", key="send_test_email_monitor"):
        with st.spinner("Sending..."):
            ok, err = email_alerts.send_test_email()
        if ok:
            st.success("Test email sent successfully!")
        else:
            st.error(f"Test email failed: {err}")

    if _email_btn_c2.button("📊 Send Daily Summary Now", help="Send today's portfolio + watchlist summary email immediately (same as the 5 PM scheduled email)"):
        with st.spinner("Building summary and sending — this may take 30–60 seconds..."):
            try:
                import time as _time
                import re as _re
                from datetime import date as _date_cls
                from iv_rank import get_iv_rank_advanced as _ivr

                def _get_ivr(ticker, implied_vol_pct=None):
                    """Fetch IV rank, using known option IV as a fast hint."""
                    try:
                        res = _ivr(ticker, current_iv_pct=implied_vol_pct)
                        if res and "Success" in res:
                            m = _re.search(r"([\d.]+)", res.split("is:")[-1])
                            return float(m.group(1)) if m else None
                    except Exception:
                        pass
                    return None

                def _dte_from_pos(pos):
                    _ex = pos.get("expiration_date")
                    if _ex:
                        try:
                            _ex_d = _ex if hasattr(_ex, "toordinal") else _date_cls.fromisoformat(str(_ex))
                            return max(0, (_ex_d - _date_cls.today()).days)
                        except Exception:
                            pass
                    return None

                _sum_positions = db.get_positions()
                _sum_snapshots = {}
                for _sp in _sum_positions:
                    if _sp.get("mode") == "ACTIVE":
                        try:
                            _tk  = _sp.get("ticker", "")
                            _con = _sp.get("contract", "") or ""
                            # Construct OCC from position fields if contract is missing
                            if not _con:
                                _sk = _sp.get("strike")
                                _ex = _sp.get("expiration_date")
                                if _sk and _ex:
                                    try:
                                        _ex_d = _ex if hasattr(_ex, "strftime") else _date_cls.fromisoformat(str(_ex))
                                        _ot   = _sp.get("option_type", "C") or "C"
                                        _con  = options_data.to_occ(_tk, _ex_d, _ot, float(_sk))
                                    except Exception:
                                        pass
                            _snap = options_data.get_option_snapshot(_tk, _con) if _con else {}
                            _ok   = _snap and "_error" not in _snap
                            _iv_hint = (_snap.get("implied_volatility") or 0) * 100 if _ok else None
                            _ivrank  = _get_ivr(_tk, _iv_hint)
                            _dte     = (_snap.get("dte") if _ok else None) or _dte_from_pos(_sp)
                            if _ok:
                                _sum_snapshots[str(_sp["id"])] = {
                                    "mid":          _snap.get("mid"),
                                    "delta":        _snap.get("delta"),
                                    "dte":          _dte,
                                    "iv_rank":      _ivrank,
                                    "thesis_score": db.get_leaps_monitor_score(_tk),
                                }
                            else:
                                _sum_snapshots[str(_sp["id"])] = {
                                    "dte":          _dte,
                                    "iv_rank":      _ivrank,
                                    "thesis_score": db.get_leaps_monitor_score(_tk),
                                }
                            _time.sleep(1.5)  # respect yfinance rate limit
                        except Exception:
                            _sum_snapshots[str(_sp["id"])] = {}

                _sum_wl_signals = {}
                for _sp in _sum_positions:
                    if _sp.get("mode") != "WATCHLIST":
                        continue
                    try:
                        from technical import get_price_and_range as _gpr, get_weekly_rsi as _grsi
                        _tk = _sp.get("ticker", "")
                        _sd = _gpr(_tk)
                        _sd["weekly_rsi"] = _grsi(_tk)
                        _ivrank = _get_ivr(_tk)
                        _ea  = evaluate_entry(_sp, _sd, _ivrank)  # re-evaluate with real IV
                        _rec = recommend_asymmetric(_tk)
                        _top = _rec.get("contracts", [None])[0] or {}
                        _sum_wl_signals[str(_sp["id"])] = {
                            "entry_alert":  _ea,
                            "thesis_score": db.get_leaps_monitor_score(_tk),
                            "iv_rank":      _ivrank,
                            "price":        _sd.get("price"),
                            "rsi":          _sd.get("weekly_rsi"),
                            "rec_strike":   _top.get("strike"),
                            "rec_expiry":   str(_top.get("expiration_date","")) if _top.get("expiration_date") else None,
                            "rec_premium":  _top.get("mid"),
                            "rec_delta":    _top.get("delta"),
                            "rec_otm_pct":  _top.get("otm_pct"),
                        }
                        _time.sleep(1.5)  # respect yfinance rate limit
                    except Exception:
                        pass

                ok, err = email_alerts.send_daily_summary(
                    _sum_positions, _sum_snapshots,
                    watchlist_signals=_sum_wl_signals,
                )
                if ok:
                    st.success("Daily summary email sent!")
                else:
                    st.error(f"Email failed: {err}")
            except Exception as _e:
                st.error(f"Failed to build summary: {_e}")

    st.divider()

    # ── Evaluator email alerts ────────────────────────────────────────────────
    st.subheader("📧 Evaluator Score-Change Alerts")
    col1, col2 = st.columns(2)
    with col1:
        st.session_state.alert_enabled = st.toggle(
            "Enable evaluator email alerts", value=st.session_state.alert_enabled
        )
        st.session_state.alert_email = st.text_input(
            "Alert recipient email",
            value=st.session_state.alert_email,
            placeholder="you@example.com",
            disabled=not st.session_state.alert_enabled,
        )
    with col2:
        trigger_opts = ["Verdict changes", "Score changes by ≥ 5 pts", "Both"]
        st.session_state.alert_trigger = st.selectbox(
            "Alert trigger", trigger_opts,
            index=trigger_opts.index(st.session_state.alert_trigger),
            disabled=not st.session_state.alert_enabled,
        )
        if st.button("✉️ Send Test Email", key="send_test_email_evaluator", disabled=not st.session_state.alert_enabled):
            ok, err = send_alert_email(
                st.session_state.alert_email,
                "LEAPS Command Center — Test Alert",
                "<html><body><h2>Test alert ✅</h2><p>Email alerts are configured.</p></body></html>",
            )
            if ok:
                st.success("Test email sent successfully!")
            else:
                st.error(f"Email failed: {err}")

    with st.expander("Required secrets for evaluator email"):
        st.code("""
# Add to .streamlit/secrets.toml
# Either key name works (GMAIL_SENDER is preferred):
GMAIL_SENDER          = "your-gmail@gmail.com"
GMAIL_APP_PASSWORD    = "your-16-char-app-password"
ALERT_RECIPIENT_EMAIL = "recipient@example.com"  # optional, defaults to sender

# Legacy key names (also supported as fallback):
# ALERT_EMAIL_FROM = "your-gmail@gmail.com"
# ALERT_EMAIL_PASS = "your-app-password"
        """, language="toml")

    st.divider()

    # ── Earnings & News API Keys ───────────────────────────────────────────────
    st.subheader("Earnings Call & News Analysis")

    _finnhub_key  = _cfg("FINNHUB_API_KEY")
    _aletheia_key = _cfg("ALETHEIA_API_KEY")
    _gemini_key   = _cfg("GEMINI_API_KEY")

    c1, c2, c3 = st.columns(3)
    c1.metric("GEMINI_API_KEY",   "✅ Set" if _gemini_key  else "❌ Missing",
              help="Required for earnings call analysis (LLM)")
    c2.metric("FINNHUB_API_KEY",  "✅ Set" if _finnhub_key  else "⚪ Optional",
              help="Free tier — adds EPS beat/miss context to earnings analysis. finnhub.io")
    c3.metric("ALETHEIA_API_KEY", "✅ Set" if _aletheia_key else "⚪ Optional",
              help="Free tier — pre-computed earnings call sentiment. aletheia-api.com")

    with st.expander("Earnings analysis source pipeline", expanded=False):
        st.markdown("""
**Source priority for each earnings call:**

| # | Source | Key needed | What it provides |
|---|--------|-----------|-----------------|
| 1 | **Aletheia API** | `ALETHEIA_API_KEY` | Pre-computed call sentiment & guidance |
| 2 | **SEC EDGAR 8-K** | None (free) | Actual management text from official filing |
| 2b| **Finnhub EPS** | `FINNHUB_API_KEY` | Beat/miss %, prior quarter comparison |
| 3 | **Gemini + Google Search** | `GEMINI_API_KEY` | Fallback: LLM searches for transcript |

Sources 1 & 2 give Gemini **actual text to analyze** rather than guessing from training data.
SEC EDGAR 8-K is always available for free — no API key required.
        """)

    with st.expander("Add optional API keys to secrets.toml"):
        st.code("""
# Finnhub — free tier at finnhub.io (60 calls/min)
# Provides: EPS actual vs estimate, surprise %, analyst recommendations
FINNHUB_API_KEY = "your_finnhub_key"

# Aletheia — free tier at aletheia-api.com
# Provides: earnings call sentiment analysis, insider trading, congressional trades
ALETHEIA_API_KEY = "your_aletheia_key"
        """, language="toml")

    st.divider()

    # ── Alert thresholds ──────────────────────────────────────────────────────
    st.subheader("Exit / Entry Thresholds")
    st.markdown(
        """
        | Signal | Threshold | Action |
        |---|---|---|
        | **Stop loss** | −60% | Exit — statistical recovery rare |
        | **First trim** | +100% (2x) | Sell 20-25% — recover initial cost |
        | **Trim & Roll** | +300% (4x) | Sell 50%; roll remainder if DTE > 90 |
        | **Trim hard** | +600% (7x) | Sell 75%; trail 25% toward 10x |
        | **Full exit** | +900% (10x) | Exit everything — target hit |
        | **Roll delta** | Δ > 0.90 | Leverage exhausted — roll higher |
        | **Delta warn** | Δ < 0.10 | Option near worthless — reassess |
        | **DTE roll** | < 270 days | Roll if profitable |
        | **DTE urgent** | < 90 days | Exit losers; take profits |
        | **DTE hard exit** | < 60 days | Exit regardless of P&L |
        """
    )

    st.divider()

    # ── Monitoring schedule ───────────────────────────────────────────────────
    st.subheader("Monitoring Schedule")
    st.markdown(
        """
        - Active position checks: **every 30 minutes** (Mon–Fri, 9:30–16:00 ET)
        - Watchlist entry checks: **every hour** (Mon–Fri, 9:30–16:00 ET)
        - Daily portfolio summary email: **9:35 AM ET** (Mon–Fri)
        - Thesis auto-refresh: **6:00 AM ET** (Mon–Fri) — 30-day cycle, 7-day in earnings months
        """
    )

    st.divider()

    # ── Closed / Rolled positions ─────────────────────────────────────────────
    st.subheader("Closed / Rolled Positions")
    try:
        closed = db.get_positions(mode="CLOSED") + db.get_positions(mode="ROLLED")
    except Exception:
        closed = []

    if not closed:
        st.caption("No closed or rolled positions.")
    else:
        st.caption(f"{len(closed)} closed/rolled positions")
        for pos in closed:
            c1, c2, c3 = st.columns([3, 2, 1])
            c1.write(f"{pos.get('ticker')}  {pos.get('expiration_date')} "
                     f"${pos.get('strike')}C  — {pos.get('mode')}")
            c2.write(f"Entry: ${pos.get('entry_price')}  ×{pos.get('quantity')}")
            with c3:
                if st.button("Delete", key=f"del_{pos.get('id')}"):
                    db.delete_position(str(pos.get("id")))
                    st.rerun()
