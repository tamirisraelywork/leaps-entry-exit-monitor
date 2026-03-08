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
from recommender import recommend_options, format_recommendation
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


def _live_market(pos: dict) -> dict:
    ticker   = pos.get("ticker", "")
    contract = _resolve_contract(pos)
    snapshot = options_data.get_option_snapshot(ticker, contract) if contract else {}
    snapshot = snapshot or {}
    # Reuse IV from snapshot to skip the option-chain round-trip in iv_rank
    iv_pct = (snapshot.get("implied_volatility") or 0) * 100 or None
    return {
        "mid":          snapshot.get("mid"),
        "bid":          snapshot.get("bid"),
        "ask":          snapshot.get("ask"),
        "delta":        snapshot.get("delta"),
        "dte":          snapshot.get("dte"),
        "iv_rank":      _get_iv_rank_cached(ticker, iv_pct),
        "thesis_score": None,
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
            time.sleep(5)
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
            time.sleep(5)
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
            _DEPRECATED_METRICS = {
                "cash burn", "capital structure", "operating leverage", "(dol)",
                "total insider ownership", "iv rank",
            }
            def _is_deprecated(name):
                n = str(name).lower()
                return any(d in n for d in _DEPRECATED_METRICS)

            # Build clean display table, skip deprecated (0/0) rows
            disp_rows = []
            for r in report:
                mn = r.get("Metric Name", "")
                if _is_deprecated(mn):
                    continue
                obt = r.get("Obtained points", "")
                tot = r.get("Total points", "")
                # Show blank row if metric contributes nothing (no pts defined)
                if str(obt) == "" and str(tot) == "":
                    continue
                disp_rows.append({
                    "Metric":  mn,
                    "Source":  r.get("Source", ""),
                    "Value":   r.get("Value", ""),
                    "Score":   str(obt),
                    "Max":     str(tot) if str(tot) not in ("", "0") else "",
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

            bar_p1 = min(int(p1 / 35 * 100), 100)
            bar_p2 = min(int(p2 / 65 * 100), 100)
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

        if df.empty:
            st.info("No analyses match the current filters.")
        else:
            hdr = st.columns([1, 2.5, 2.2, 1.5, 2, 1, 1])
            for txt, c in zip(
                ["**#**","**Ticker**","**Date**","**Score**","**Verdict**","",""],
                hdr,
            ):
                c.write(txt)
            st.divider()

            for row_num, row in df.iterrows():
                ticker = row["Ticker"]
                vc     = _verdict_color(row["Verdict"])
                rc     = st.columns([1, 2.5, 2.2, 1.5, 2, 1, 1])
                rc[0].write(row_num + 1)
                rc[1].write(f"**{ticker}**")
                rc[2].write(str(row.get("date", "N/A")))
                rc[3].write(str(row["Score"]))
                rc[4].markdown(
                    f'<span style="color:{vc};font-weight:700">{row["Verdict"]}</span>',
                    unsafe_allow_html=True,
                )
                if rc[5].button("👁️", key=f"view_{row_num}_{ticker}"):
                    st.session_state.past_selected = ticker
                    st.session_state.past_view     = "detail"
                    st.rerun()
                if rc[6].button("🗑️", key=f"del_{row_num}_{ticker}"):
                    if delete_eval_ticker(ticker):
                        st.toast(f"Deleted {ticker}")
                        st.rerun()

    # ── Detail view ──────────────────────────────────────────────────────────
    elif st.session_state.past_view == "detail":
        ticker = st.session_state.past_selected

        btn_c1, btn_c2, btn_c3, _ = st.columns([1.2, 1.8, 1.8, 4])
        if btn_c1.button("← Back"):
            st.session_state.past_view    = "history"
            st.session_state.past_selected = None
            st.rerun()

        # Add to watchlist button
        existing_pos     = db.get_positions()
        existing_tickers = {p.get("ticker","").upper() for p in existing_pos}
        if ticker in existing_tickers:
            if btn_c2.button("✅ In Watchlist — Remove", key="detail_rm"):
                pos_to_rm = next((p for p in existing_pos if p.get("ticker","").upper() == ticker), None)
                if pos_to_rm:
                    db.delete_position(str(pos_to_rm["id"]))
                    st.toast(f"Removed {ticker} from watchlist.")
                    st.rerun()
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
                st.toast(f"Added {ticker} to watchlist.")
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
            _DEPRECATED_KW = {"cash burn","capital structure","operating leverage","(dol)",
                               "total insider ownership","iv rank","net debt\x00","ebitda\x00"}

            def _is_dep(name):
                n = str(name).lower()
                return any(d in n for d in
                           ("cash burn","capital structure","operating leverage","(dol)",
                            "total insider ownership"))

            metrics_df = df[~df[m_col].isin(QUAL)].copy()
            # Remove deprecated and zero-total rows from display
            if t_col in metrics_df.columns:
                metrics_df = metrics_df[
                    ~metrics_df[m_col].apply(_is_dep) &
                    (metrics_df[t_col].apply(safe_float) > 0)
                ].copy()

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

            bar_p1 = min(int(p1 / 35 * 100), 100)
            bar_p2 = min(int(p2 / 65 * 100), 100)

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

    col_refresh, col_spacer = st.columns([1, 5])
    with col_refresh:
        force_check = st.button("🔄 Run Check Now", use_container_width=True)

    try:
        positions = db.get_positions()
    except Exception as e:
        st.error(f"Could not load positions: {e}")
        positions = []

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
        for pos in active:
            ticker   = pos.get("ticker", "?")
            strike   = pos.get("strike", "?")
            exp      = pos.get("expiration_date", "?")
            qty      = pos.get("quantity", "?")
            ep       = pos.get("entry_price")
            notes    = pos.get("notes", "")
            pos_id   = str(pos.get("id", ""))
            cost_basis = (ep or 0) * (qty or 0) * 100

            with st.spinner(f"Loading {ticker}..."):
                if force_check:
                    mkt = _live_market(pos)
                    time.sleep(12)
                    polygon_error = None
                else:
                    contract = _resolve_contract(pos)
                    # Persist auto-built contract so future loads don't need to recompute it
                    if contract and not (pos.get("contract") or "").strip():
                        try:
                            db.update_position(pos_id, {"contract": contract})
                        except Exception:
                            pass
                    snap = options_data.get_option_snapshot(ticker, contract) if contract else {}
                    snap = snap or {}
                    polygon_error = snap.get("_error")
                    dte_fallback = None
                    try:
                        raw_exp = pos.get("expiration_date")
                        if raw_exp:
                            exp_d = raw_exp if isinstance(raw_exp, date) else date.fromisoformat(str(raw_exp))
                            dte_fallback = (exp_d - date.today()).days
                    except Exception:
                        pass
                    iv_pct_dash = (snap.get("implied_volatility") or 0) * 100 or None
                    mkt = {
                        "mid":          snap.get("mid"),
                        "bid":          snap.get("bid"),
                        "ask":          snap.get("ask"),
                        "delta":        snap.get("delta"),
                        "dte":          snap.get("dte") or dte_fallback,
                        "iv_rank":      _get_iv_rank_cached(ticker, iv_pct_dash),
                        "thesis_score": None,
                    }

            mid      = mkt.get("mid")
            delta    = mkt.get("delta")
            dte_days = mkt.get("dte")
            pnl      = _pnl_pct(ep, mid)

            score, score_age = db.get_leaps_monitor_score_with_age(ticker)

            if score is None:
                with st.spinner(f"First-time thesis scoring for {ticker} (~30s)..."):
                    _s, _v = score_thesis.compute_and_save_score(ticker)
                if _s is not None:
                    score, score_age = _s, 0

            mkt["thesis_score"] = score
            position_alerts = evaluate(pos, mkt)

            if position_alerts:
                worst_alert   = max(position_alerts, key=_alert_priority)
                severity      = worst_alert.severity
                posture_label = _POSTURE_LABEL.get(severity, "HOLD")
            else:
                worst_alert   = None
                severity      = "GREEN"
                posture_label = "HOLD"

            # Auto-email EXIT/ROLL signals (once per session per alert key)
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

            color = _SEVERITY_COLOR.get(severity, "#21C55D")
            emoji = _POSTURE_EMOJI.get(severity, "🟢")

            qty_trimmed   = int(pos.get("quantity_trimmed") or 0)
            proceeds      = float(pos.get("proceeds_from_trims") or 0.0)
            qty_remaining = int(qty or 0) - qty_trimmed

            # P&L breakdown
            _orig_cost        = (ep or 0) * int(qty or 0) * 100
            _cost_of_trimmed  = (ep or 0) * qty_trimmed * 100
            _realized_pnl     = proceeds - _cost_of_trimmed
            _unrealized_pnl   = ((mid - (ep or 0)) * qty_remaining * 100) if (mid and ep) else None
            _total_pnl        = (_realized_pnl + _unrealized_pnl) if _unrealized_pnl is not None else _realized_pnl
            _cost_recovery    = (proceeds / _orig_cost * 100) if _orig_cost > 0 else 0.0
            _house_money      = _cost_recovery >= 100.0

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

                m1, m2, m3, m4, m5, m6 = st.columns(6)
                m1.metric("Avg. Price",   f"${ep:.2f}"   if ep       else "N/A")
                m2.metric("Current Mid",  f"${mid:.2f}"  if mid      else "N/A")
                m3.metric("Unrlzd P&L",
                          f"{pnl:+.1f}%" if pnl is not None else "N/A",
                          delta_color="normal" if pnl is None else ("normal" if pnl >= 0 else "inverse"),
                          help="Unrealized P&L % on remaining contracts vs avg entry price")
                m4.metric("Delta",    f"{delta:.2f}" if delta    else "N/A")
                m5.metric("DTE",      f"{dte_days}d" if dte_days else "N/A")
                if score is not None:
                    age_label = f" ({score_age}d ago)" if score_age is not None else ""
                    stale     = score_age is not None and score_age > 30
                    m6.metric("Thesis Score", f"{score}/100{age_label}",
                              delta="⚠️ stale" if stale else None,
                              delta_color="inverse" if stale else "normal")
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

                if polygon_error:
                    st.warning(f"⚠️ Live data unavailable: {polygon_error}")

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
        st.subheader("Watchlist — Waiting for Entry Signal")
        for pos in watchlist:
            ticker = pos.get("ticker", "?")
            pos_id = str(pos.get("id", ""))

            stock_data = get_price_and_range(ticker)
            price      = stock_data.get("price")
            pfl        = stock_data.get("pct_from_low")

            score = db.get_leaps_monitor_score(ticker)
            if score is None:
                with st.spinner(f"First-time scoring for {ticker}..."):
                    _s, _v = score_thesis.compute_and_save_score(ticker)
                    score  = _s
            score_str = f"{score}/100" if score else "N/A"

            with st.container(border=True):
                c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 1])
                c1.metric("Ticker",        ticker)
                c2.metric("Current Price", f"${price:.2f}" if price else "N/A")
                c3.metric("52w Position",  f"{pfl*100:.0f}% from low" if pfl is not None else "N/A")
                c4.metric("Thesis Score",  score_str)
                with c5:
                    if st.button("Move to Active", key=f"activate_{pos_id}"):
                        db.update_position_mode(pos_id, "ACTIVE")
                        st.rerun()
                    if st.button("Remove", key=f"rm_watch_{pos_id}"):
                        db.delete_position(pos_id)
                        st.rerun()
            st.caption(pos.get("notes", ""))


# ===========================================================================
# PAGE: ADD POSITION
# ===========================================================================

elif page == "➕ Add Position":
    st.title("Add Position")

    tab_new, tab_existing = st.tabs([
        "🔍 New Entry — Recommend Options",
        "📋 Existing Position — Already Bought",
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
                snap  = options_data.get_option_snapshot(ticker_ex, contract_sym) or {}
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

    # Auto-trigger fast rescore once per session when settings page first loads
    if not st.session_state.get("_auto_rescore_done"):
        st.session_state["_auto_rescore_done"] = True
        _auto_master = get_eval_master_data()
        if not _auto_master.empty:
            _auto_tickers = _auto_master["Ticker"].tolist()
            _auto_results = []
            _prog = st.progress(0.0, text="Auto-rescoring all analyses with updated rules…")
            for _i, _tk in enumerate(_auto_tickers):
                _os, _ns, _ov, _nv = rescore_ticker_in_bq(_tk)
                _auto_results.append({
                    "Ticker": _tk, "Old Score": _os, "New Score": _ns,
                    "Score Δ": _ns - _os, "Old Verdict": _ov, "New Verdict": _nv,
                    "Changed": "✅" if (_os != _ns or _ov != _nv) else "—",
                })
                _prog.progress((_i + 1) / len(_auto_tickers))
            st.session_state.rescore_results = _auto_results
            st.cache_data.clear()
            _prog.empty()
            _changed_count  = sum(1 for r in _auto_results if r["Changed"] == "✅")
            _error_count    = sum(1 for r in _auto_results if "⚠️ Error" in str(r.get("Old Verdict", "")))
            if _changed_count:
                st.success(f"Auto-rescored {len(_auto_tickers)} tickers — {_changed_count} updated with new scoring rules.")
            else:
                st.info(f"Auto-rescored {len(_auto_tickers)} tickers — all scores are already up to date.")
            if _error_count:
                st.warning(f"{_error_count} ticker(s) had errors — see results table below for details.")

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

    # ── IBKR Data Sync ────────────────────────────────────────────────────────
    st.subheader("📥 Sync IBKR Position Data")
    st.caption(
        "Updates all BigQuery positions with exact quantities, average entry prices, "
        "trim amounts, and proceeds from the IBKR activity statement (Aug 2025 – Mar 6, 2026). "
        "Closed positions (EVGO, FLNC 13C, RUM) will be marked CLOSED."
    )

    _IBKR_SYNC_DATA = [
        # (ticker, strike, expiry, opt_type, qty, avg_px, qty_trimmed, proceeds, mode)
        ("AEHR",   40.00, "2027-01-15", "C",  25, 3.200,   10,  8400.00, "ACTIVE"),
        ("DLO",    24.47, "2027-01-15", "C",  45, 1.900,    0,     0.00, "ACTIVE"),
        ("DLO",    25.00, "2027-12-17", "C",  15, 1.900,    0,     0.00, "ACTIVE"),
        ("EH",     35.00, "2027-01-15", "C",  70, 1.500,    0,     0.00, "ACTIVE"),
        ("ENVX",   20.00, "2027-01-15", "C",  52, 2.119,    0,     0.00, "ACTIVE"),
        ("EVGO",    5.00, "2027-01-15", "C",  80, 1.050,   80,  9600.00, "CLOSED"),
        ("FIG",   100.00, "2028-01-21", "C",  22, 5.545,    0,     0.00, "ACTIVE"),
        ("FLNC",   13.00, "2027-01-15", "C",  64, 1.700,   64, 87020.00, "CLOSED"),
        ("FLNC",   37.00, "2028-01-21", "C",  12, 8.000,    0,     0.00, "ACTIVE"),
        ("IWM",   195.00, "2027-12-17", "P",   7,11.050,    0,     0.00, "ACTIVE"),
        ("JMIA",   12.00, "2027-01-15", "C",  60, 1.700,   40, 24000.00, "ACTIVE"),
        ("LAC",     4.00, "2027-01-15", "C", 150, 0.740,  120, 36000.00, "ACTIVE"),
        ("MVST",    4.50, "2027-01-15", "C", 100, 0.800,    0,     0.00, "ACTIVE"),
        ("OKTA",  180.00, "2027-12-17", "C",  14, 4.000,    0,     0.00, "ACTIVE"),
        ("OPRA",   25.00, "2027-12-17", "C",  22, 3.100,    0,     0.00, "ACTIVE"),
        ("PACB",    3.00, "2027-01-15", "C", 290, 0.338,  100,  8000.00, "ACTIVE"),
        ("REAL",   15.00, "2027-01-15", "C",  90, 1.244,   70, 37800.00, "ACTIVE"),
        ("RUM",    15.00, "2027-01-15", "C",  60, 1.439,   60,  8400.00, "CLOSED"),
        ("SHLS",   12.00, "2027-01-15", "C", 119, 0.871,   60, 16800.00, "ACTIVE"),
        ("SILJ",   35.00, "2027-01-15", "C",  35, 2.550,   20, 18000.00, "ACTIVE"),
        ("XPOF",   15.00, "2026-12-18", "C",  80, 1.200,    0,     0.00, "ACTIVE"),
    ]

    if st.button("📥 Apply IBKR Sync", type="primary"):
        _sync_positions = db.get_positions()
        _sync_log = []
        _sync_updated = 0

        def _ibkr_find(positions, ticker, strike, expiry):
            candidates = [
                p for p in positions
                if p.get("ticker", "").upper() == ticker.upper()
                and abs(float(p.get("strike") or 0) - strike) <= 0.5
                and str(p.get("expiration_date", ""))[:10] == expiry
            ]
            if candidates:
                return candidates[0]
            by_ticker = [p for p in positions if p.get("ticker","").upper() == ticker.upper()]
            return by_ticker[0] if len(by_ticker) == 1 else None

        with st.spinner("Syncing positions…"):
            for (tkr, stk, exp, otype, qty, px, trimmed, proceeds, mode) in _IBKR_SYNC_DATA:
                pos = _ibkr_find(_sync_positions, tkr, stk, exp)
                if pos is None:
                    _sync_log.append({"Ticker": tkr, "Status": "NOT FOUND", "Changes": ""})
                    continue
                fields = {
                    "quantity":            qty,
                    "entry_price":         round(px, 4),
                    "quantity_trimmed":    trimmed,
                    "proceeds_from_trims": round(proceeds, 2),
                    "mode":                mode,
                }
                old_fields = {
                    "quantity":            pos.get("quantity"),
                    "entry_price":         pos.get("entry_price"),
                    "quantity_trimmed":    pos.get("quantity_trimmed"),
                    "proceeds_from_trims": pos.get("proceeds_from_trims"),
                    "mode":                pos.get("mode"),
                }
                changed = {k: v for k, v in fields.items() if str(old_fields.get(k)) != str(v)}
                if changed:
                    db.update_position(str(pos["id"]), changed)
                    _sync_updated += 1
                    _sync_log.append({
                        "Ticker": tkr,
                        "Status": "UPDATED",
                        "Changes": " | ".join(f"{k}: {old_fields[k]} → {v}" for k, v in changed.items()),
                    })
                else:
                    _sync_log.append({"Ticker": tkr, "Status": "OK", "Changes": "no changes"})

        # ── Populate entry_thesis_score for positions that don't have it ──
        _baseline_set = 0
        try:
            _master_df = get_eval_master_data()
            _master_scores = {
                row["Ticker"].upper(): int(row["Score"])
                for _, row in _master_df.iterrows()
                if row.get("Score") is not None
            }
            # Re-fetch positions with fresh data after the updates above
            _refreshed = db.get_positions()
            for _rp in _refreshed:
                if _rp.get("entry_thesis_score"):
                    continue          # already has a baseline
                _rt = (_rp.get("ticker") or "").upper()
                if _rt in _master_scores:
                    db.update_position(str(_rp["id"]), {"entry_thesis_score": _master_scores[_rt]})
                    _baseline_set += 1
                    _sync_log.append({
                        "Ticker": _rt,
                        "Status": "BASELINE SET",
                        "Changes": f"entry_thesis_score → {_master_scores[_rt]}/100 (today's score as baseline)",
                    })
        except Exception as _be:
            st.warning(f"Could not set entry thesis baselines: {_be}")

        st.success(
            f"Sync complete — {_sync_updated} position(s) updated, "
            f"{_baseline_set} thesis baseline(s) set."
        )
        st.cache_data.clear()
        st.dataframe(pd.DataFrame(_sync_log), use_container_width=True, hide_index=True)
        if _baseline_set > 0:
            st.info(
                "Entry thesis baselines have been set using today's scores. "
                "Future re-analyses will show the Δ gap vs this baseline, "
                "letting you track how the thesis evolves from this point forward."
            )

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

    if st.button("Send Test Exit-Alert Email"):
        with st.spinner("Sending..."):
            ok, err = email_alerts.send_test_email()
        if ok:
            st.success("Test email sent successfully!")
        else:
            st.error(f"Test email failed: {err}")

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
        if st.button("✉️ Send Test Email", disabled=not st.session_state.alert_enabled):
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
