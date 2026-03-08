"""
score_thesis.py — Auto-compute LEAPS thesis scores inside the exit agent.

Uses the EXACT same 7-source pipeline as the LEAPS Evaluator:
  1. yahoo_finance     — financial survival & growth metrics (proxy + AV fallbacks)
  2. finviz            — institutional ownership, short float, insider activity
  3. gurufocus_moat    — GuruFocus Moat Score (BigQuery Tier1 → Gemini Tier3)
  4. LLM               — CEO ownership %, business model, company description (Gemini + Google Search)
  5. simply_wall_street — Simply Wall St risks & rewards (Gemini + Google Search)
  6. iv_rank           — IV Rank (yfinance-based proxy)
  7. EPS_growth        — Forward EPS growth (Alpha Vantage)

All 7 tasks run in parallel via asyncio.gather() — same pattern as the LEAPS Evaluator.
Results are upserted into the shared BigQuery master_table so both apps see the same score.

Expert-recommended refresh schedule
────────────────────────────────────
  Normal months:   re-score if last score is > 30 days old
  Earnings months  re-score if last score is >  7 days old
  (Feb/May/Aug/Nov) — most earnings releases happen in these months
"""

import asyncio
import json
import re
import threading
import datetime
import logging
from datetime import date

from google.cloud import bigquery
from google.oauth2 import service_account

from shared.config import cfg, cfg_dict

# Import the exact same analysis modules as the LEAPS Evaluator
from yahoo_finance import run_comprehensive_analysis
from finviz import scrape_finviz
from gurufocus_moat import get_moat_score
from LLM import analyze_ticker
from simply_wall_street import scrape_risk_rewards
from iv_rank import get_iv_rank_advanced
from EPS_growth import get_forward_eps_growth

logger = logging.getLogger(__name__)

# Thread-local storage for net_debt / ebitda scratch (same pattern as LEAPS Evaluator)
_thread_local = threading.local()


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def _bq_client() -> bigquery.Client:
    sa = cfg_dict("SERVICE_ACCOUNT_JSON")
    if not sa:
        raw = cfg("SERVICE_ACCOUNT_JSON")
        sa = json.loads(raw) if raw else {}
    sa["private_key"] = sa.get("private_key", "").replace("\\n", "\n")
    creds = service_account.Credentials.from_service_account_info(sa)
    return bigquery.Client(credentials=creds, project=sa["project_id"])


def _master_table_path(client: bigquery.Client) -> str:
    raw = cfg("DATASET_ID") or cfg("LEAPS_MONITOR_DATASET", "leaps_monitor")
    if "." in str(raw):
        return f"{raw}.master_table"
    return f"{client.project}.{raw}.master_table"


def needs_refresh(ticker: str) -> bool:
    """Return True if the thesis score is missing or stale."""
    try:
        client = _bq_client()
        path = _master_table_path(client)
        rows = list(client.query(
            f"SELECT date FROM `{path}` WHERE UPPER(Ticker) = UPPER(@t) "
            f"ORDER BY date DESC LIMIT 1",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("t", "STRING", ticker)
            ]),
        ).result())
        if not rows:
            return True   # never scored
        last = date.fromisoformat(str(rows[0]["date"])[:10])
        age = (date.today() - last).days
        threshold = 7 if date.today().month in (2, 5, 8, 11) else 30
        return age >= threshold
    except Exception:
        return True   # assume stale on any error


def _upsert_score(ticker: str, score: int, verdict: str):
    """Upsert the score into the shared BigQuery master_table."""
    client = _bq_client()
    path = _master_table_path(client)
    today = date.today().isoformat()
    params = [
        bigquery.ScalarQueryParameter("ticker",  "STRING", ticker),
        bigquery.ScalarQueryParameter("score",   "INT64",  int(score)),
        bigquery.ScalarQueryParameter("verdict", "STRING", verdict),
        bigquery.ScalarQueryParameter("date",    "STRING", today),
    ]
    exists = list(client.query(
        f"SELECT Ticker FROM `{path}` WHERE UPPER(Ticker) = UPPER(@ticker) LIMIT 1",
        job_config=bigquery.QueryJobConfig(query_parameters=[params[0]]),
    ).result())
    if exists:
        sql = (
            f"UPDATE `{path}` "
            f"SET Score=@score, Verdict=@verdict, date=@date "
            f"WHERE UPPER(Ticker) = UPPER(@ticker)"
        )
    else:
        sql = (
            f"INSERT INTO `{path}` (Ticker, Score, Verdict, date) "
            f"VALUES (@ticker, @score, @verdict, @date)"
        )
    client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


# ---------------------------------------------------------------------------
# Scoring rules — EXACT copy of calculate_scoring() from the LEAPS Evaluator
# ---------------------------------------------------------------------------

_NA = {"n/a", "none", "error", ""}


def safe_float(val):
    if val is None or str(val).lower() in ("n/a", "none", "rejected", "", "nan"):
        return 0.0
    try:
        return float(re.sub(r"[^\d.-]", "", str(val)) or "0")
    except Exception:
        return 0.0


def calculate_scoring(metric_name, value):
    """Two-pillar LEAPS scoring system (100 pts total).

    PILLAR 1 — SUSTAINABILITY (35 pts):
      Cash Runway(10), Assets/Liab(5), Net Debt/EBITDA(7),
      Share Count Growth(4), Gross Margin(5), Expiration Date(4)

    PILLAR 2 — UPSIDE POTENTIAL (70 pts):
      Revenue Growth(16), Growth-to-Valuation(12), EPS Growth(8),
      Market Cap(7), Business Model/GF Moat(10), CEO Ownership(4),
      Net Insider Buying(4), Institutional Ownership(5), Short Float(3),
      Degree of Operating Leverage(5)

    Returns (obtained_points, total_points, is_rejected).
    """
    obtained = 0
    total = 0
    is_rejected = False
    val_num = safe_float(value)
    name = str(metric_name).lower()
    val_str = str(value).lower().strip()

    # ── Deprecated metrics — contribute 0 pts, never reject ──────────────────
    if ("burn" in name or "capital structure" in name
            or "total insider ownership" in name):
        return (0, 0, False)

    # IV Rank — display only
    if "iv rank" in name:
        return (0, 0, False)

    # ── PILLAR 1: SUSTAINABILITY ──────────────────────────────────────────────

    if "runway" in name:
        total = 10
        if val_str in _NA:
            obtained = 3
        elif any(p in val_str for p in ("positive", "no cash burn", "no burn", "profitable")):
            obtained = 10
        elif val_num >= 24:
            obtained = 10
        elif val_num >= 18:
            obtained = 8
        elif val_num >= 12:
            obtained = 6
        elif val_num >= 6:
            obtained = 3
        elif val_num >= 3:
            obtained = 1
        elif val_num > 0 and "month" not in val_str:
            obtained = 10   # raw value without "months" — treat as years
        else:
            is_rejected = True   # < 3 months cash — company likely won't survive

    elif "assets" in name and "liabilities" in name:
        total = 5
        if val_str in _NA:
            obtained = 3
        elif val_num >= 2.5:
            obtained = 5
        elif val_num >= 1.5:
            obtained = 4
        elif val_num >= 1.0:
            obtained = 2
        else:
            obtained = 1

    elif "net debt / ebitda" in name:
        total = 7
        net_debt = safe_float(getattr(_thread_local, "net_debt_val", None))
        ebitda   = safe_float(getattr(_thread_local, "ebitda_val",   None))
        if net_debt < 0:
            obtained = 7   # net cash position — no debt risk
        elif ebitda <= 0:
            # Pre-profitable — grade on reported ratio or raw debt level
            if val_num == 0 or val_str in _NA:
                obtained = 3
            elif val_num <= 5:
                obtained = 2
            elif val_num <= 8:
                obtained = 1
            # > 8: 0 pts, no reject
        elif val_num == 0 or val_str in _NA:
            obtained = 7
        elif val_num <= 1.5:
            obtained = 6
        elif val_num <= 3:
            obtained = 5
        elif val_num <= 5:
            obtained = 3
        elif val_num <= 8:
            obtained = 1
        # > 8: 0 pts, no reject

    elif "share count" in name:
        total = 4
        if val_str in _NA:
            obtained = 1
        elif val_num <= 0:
            obtained = 4
        elif val_num <= 5:
            obtained = 3
        elif val_num <= 15:
            obtained = 2
        elif val_num <= 30:
            obtained = 1
        elif val_num <= 50:
            obtained = 0
        else:
            is_rejected = True   # > 50% — extreme dilution destroys per-share LEAPS returns

    elif "gross margin" in name:
        total = 5
        if val_str in _NA:
            obtained = 2
        else:
            try:
                pct = float(val_str.replace("%", ""))
                if pct >= 60:
                    obtained = 5
                elif pct >= 40:
                    obtained = 4
                elif pct >= 20:
                    obtained = 2
                elif pct >= 0:
                    obtained = 1
                # negative: 0 pts
            except Exception:
                obtained = 2

    elif "expiration" in name:
        total = 4
        obtained = ""
        if val_str in _NA:
            obtained = 0   # No expiration specified — skip, don't reject.
            # Only reject if a real date was provided but is too near-term.
        else:
            try:
                import pandas as pd
                diff_months = (
                    (pd.to_datetime(value).date() - datetime.date.today()).days // 30
                )
                if diff_months < 18:
                    is_rejected = True
                elif diff_months >= 36:
                    obtained = 4
                elif diff_months >= 24:
                    obtained = 3
                else:   # 18-23 months
                    obtained = 1
            except Exception:
                obtained = 0   # Can't parse — skip, don't reject

    # ── PILLAR 2: UPSIDE POTENTIAL ────────────────────────────────────────────

    elif "operating leverage" in name or "(dol)" in name:
        total = 5
        if val_str in _NA:
            obtained = 1   # unknown — neutral
        elif val_num >= 3:
            obtained = 5   # very high leverage — earnings explode with revenue growth
        elif val_num >= 2:
            obtained = 4   # high leverage — strong amplification
        elif val_num >= 1:
            obtained = 2   # moderate — some amplification
        elif val_num > 0:
            obtained = 1   # low leverage
        # ≤ 0 (declining or negative): 0 pts

    elif "revenue growth" in name:
        total = 16
        if val_str not in _NA:
            try:
                pct = float(val_str.replace("%", ""))
                if pct >= 50:
                    obtained = 16
                elif pct >= 30:
                    obtained = 12
                elif pct >= 20:
                    obtained = 8
                elif pct >= 10:
                    obtained = 4
                elif pct > 0:
                    obtained = 1
                # negative or 0: 0 pts
            except Exception:
                pass

    elif "growth-to-valuation" in name:
        total = 12
        if val_str not in _NA:
            try:
                gtv = float(val_str)
                if gtv >= 15:
                    obtained = 12
                elif gtv >= 8:
                    obtained = 9
                elif gtv >= 5:
                    obtained = 6
                elif gtv >= 3:
                    obtained = 3
                elif gtv >= 1:
                    obtained = 1
            except Exception:
                pass

    elif "eps growth" in name:
        total = 8
        if val_str in _NA:
            obtained = 3   # neutral N/A — don't penalise missing data
        else:
            try:
                pct = float(val_str.replace("%", ""))
                if pct >= 50:
                    obtained = 8
                elif pct >= 30:
                    obtained = 6
                elif pct >= 15:
                    obtained = 4
                elif pct >= 5:
                    obtained = 2
                elif pct > 0:
                    obtained = 1
                # negative: 0 pts
            except Exception:
                obtained = 3

    elif "market cap" in name:
        total = 7
        billions = val_num
        if "t" in val_str:
            billions = val_num * 1000
        elif "m" in val_str and "b" not in val_str:
            billions = val_num / 1000
        if billions <= 1:
            obtained = 7   # micro/small cap — maximum asymmetry
        elif billions <= 3:
            obtained = 6
        elif billions <= 10:
            obtained = 4
        elif billions <= 30:
            obtained = 2
        elif billions <= 100:
            obtained = 1
        # > $100B: 0 pts, no reject — large cap LEAPS still valid if thesis is strong

    elif "moat score" in name:
        # Feeds into Business Model scoring via thread-local cache; no standalone pts
        try:
            _thread_local.gf_score = float(val_num)
        except Exception:
            _thread_local.gf_score = 0.0
        return (0, 0, False)

    elif "business model" in name:
        total = 10
        gf = getattr(_thread_local, "gf_score", 0.0)
        text = val_str
        monopoly_kw = ["mission-critical", "infrastructure", "monopoly", "dominant", "network effect"]
        saas_kw     = ["saas", "platform", "subscription", "marketplace", "high switching"]
        growth_kw   = ["high-growth", "disruptive", "emerging", "hypergrowth"]
        commodity_kw = ["commodity", "cyclical", "oil", "mining", "brick", "retail"]
        if gf >= 3.0 or any(k in text for k in monopoly_kw):
            obtained = 10
        elif gf >= 2.0 or any(k in text for k in saas_kw):
            obtained = 7
        elif gf >= 1.0 or any(k in text for k in growth_kw):
            obtained = 4
        elif any(k in text for k in commodity_kw):
            obtained = 1
        else:
            obtained = 3   # N/A / unknown fallback

    elif "ceo ownership" in name:
        total = 4
        if "not disclosed" in val_str or val_str in _NA:
            obtained = 1
        elif val_num >= 5:
            obtained = 4
        elif val_num >= 2:
            obtained = 3
        elif val_num >= 1:
            obtained = 2
        else:
            obtained = 1

    elif "buying vs selling" in name or "insider buying" in name:
        total = 4
        if val_str in _NA:
            obtained = 1   # unknown — neutral
        elif val_num >= 5:
            obtained = 4   # strong insider conviction (≥5% net buying)
        elif val_num >= 1:
            obtained = 3   # meaningful net buying
        elif val_num > 0:
            obtained = 2   # any net buying is a positive signal
        elif val_num == 0:
            obtained = 1   # neutral
        # net selling: 0 pts

    elif "institutional ownership" in name:
        total = 5
        if val_str in _NA:
            obtained = 2
        elif val_num < 10:
            obtained = 5   # undiscovered gem — massive upside when institutions enter
        elif val_num < 20:
            obtained = 4   # early discovery phase — still room to run
        elif val_num <= 50:
            obtained = 3   # moderate institutional presence
        elif val_num <= 70:
            obtained = 2   # mainstream — limited further institutional buying
        else:
            obtained = 1   # over-owned — little room for new institutional demand

    elif "short float" in name:
        total = 3
        if val_str not in _NA:
            if 10 <= val_num <= 25:
                obtained = 3   # moderate short interest = potential squeeze catalyst
            elif 25 < val_num <= 40:
                obtained = 2
            elif 5 <= val_num < 10:
                obtained = 2
            elif val_num > 40:
                obtained = 1   # excessive short pressure — elevated risk
            # < 5%: 0 pts (no short squeeze potential)

    return obtained, total, is_rejected


# ---------------------------------------------------------------------------
# LLM response parser — exact copy from LEAPS Evaluator app.py
# ---------------------------------------------------------------------------

def parse_llm_response(text):
    data = {
        "description": "N/A", "value_proposition": "N/A",
        "moat": "N/A", "ceo_ownership": "N/A", "classification": "N/A",
    }
    if not text or not isinstance(text, str):
        return data

    _H = r"(?:[\d]+\.\s*)?(?:[*#]+\s*)?"
    _T = r"[*:]*\s*"

    def _extract(primary, fallback):
        m = re.search(primary, text, re.DOTALL | re.IGNORECASE)
        if not m:
            m = re.search(fallback, text, re.DOTALL | re.IGNORECASE)
        return m.group(1).strip() if m else "N/A"

    data["description"] = _extract(
        rf"{_H}Company Description{_T}(.*?)(?=\n\s*{_H}(?:Value Proposition|Moat|Economic Moat|CEO|Final|$))",
        rf"{_H}Company Description{_T}(.*?)(?=\n\n|\Z)",
    )
    data["value_proposition"] = _extract(
        rf"{_H}Value Proposition{_T}(.*?)(?=\n\s*{_H}(?:Moat|Economic Moat|CEO|Final|$))",
        rf"{_H}Value Proposition{_T}(.*?)(?=\n\n|\Z)",
    )
    data["moat"] = _extract(
        rf"{_H}(?:Moat Analysis|Economic Moat){_T}(.*?)(?=\n\s*{_H}(?:CEO|Ownership|Final|$))",
        rf"{_H}(?:Moat Analysis|Economic Moat){_T}(.*?)(?=\n\n|\Z)",
    )

    own = re.search(r"Ownership Percentage[:\s*]*([0-9]+\.?[0-9]*\s*%)", text, re.IGNORECASE)
    if own:
        data["ceo_ownership"] = own.group(1).strip()
    else:
        own = re.search(r"(?:CEO|ownership)[^\n]{0,150}?([0-9]+\.?[0-9]*\s*%)", text, re.IGNORECASE)
        if own:
            data["ceo_ownership"] = own.group(1).strip()

    cls = re.search(r"Category[*:]*\s*([^\n*]+)", text, re.IGNORECASE)
    if cls:
        data["classification"] = cls.group(1).strip()
    return data


# ---------------------------------------------------------------------------
# Parallel analysis — EXACT same 7-task gather as the LEAPS Evaluator
# ---------------------------------------------------------------------------

async def _run_parallel_analysis(ticker: str):
    """Run all 7 analysis tasks in parallel — same as _run_parallel_analysis in LEAPS Evaluator."""
    key = cfg("ALPHA_VANTAGE_API_KEY_1") or cfg("ALPHA_VANTAGE_API_KEY_2")
    return await asyncio.gather(
        asyncio.to_thread(run_comprehensive_analysis, ticker),
        asyncio.to_thread(scrape_finviz, ticker),
        asyncio.to_thread(get_moat_score, ticker),
        asyncio.to_thread(analyze_ticker, ticker),
        asyncio.to_thread(scrape_risk_rewards, ticker),
        asyncio.to_thread(get_iv_rank_advanced, ticker),
        asyncio.to_thread(get_forward_eps_growth, ticker, key),
        return_exceptions=True,
    )


def _run_analysis(ticker: str):
    """Run the full scraper pipeline in a fresh event loop. Safe from any thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(_run_parallel_analysis(ticker))
    finally:
        loop.close()
        asyncio.set_event_loop(None)


# ---------------------------------------------------------------------------
# Report builder — EXACT same logic as _build_report() in the LEAPS Evaluator
# ---------------------------------------------------------------------------

def _build_report(ticker: str, results):
    """
    Parse the 7-tuple from _run_parallel_analysis into (score, verdict).
    Returns (score, verdict, table_rows, llm, sws) or raises on critical failure.
    """
    analysis, finviz_data, moat_score, raw_llm, sws_data, iv_rank_result, eps_val = results

    # Reset thread-local scoring cache for this ticker
    _thread_local.gf_score    = 0.0
    _thread_local.net_debt_val = None
    _thread_local.ebitda_val   = None

    if isinstance(raw_llm, Exception):
        raw_llm = ""
    llm = parse_llm_response(raw_llm)

    if isinstance(sws_data, Exception):
        sws_data = {"rewards": [], "risks": []}

    if isinstance(analysis, Exception):
        raise RuntimeError(f"yahoo_finance failed: {analysis}")
    if analysis.get("status") != "success":
        raise RuntimeError(analysis.get("error", "Analysis returned non-success status"))

    table_rows = []

    for _section, metrics in analysis["data"].items():
        for metric_name, value in metrics.items():
            ml = metric_name.lower()
            if ml == "net debt":
                _thread_local.net_debt_val = value
            elif ml == "ebitda":
                _thread_local.ebitda_val = value
            pts, total_pts, rejected = calculate_scoring(metric_name, value)
            table_rows.append({
                "Metric Name": metric_name,
                "Source": "Yahoo Finance",
                "Value": str(value),
                "Obtained points": "rejected" if rejected else (str(pts) if total_pts > 0 else ""),
                "Total points": str(total_pts) if total_pts > 0 else "",
            })

    if finviz_data and not isinstance(finviz_data, Exception):
        for mk in ("Net Insider Buying vs Selling (%)",
                   "Institutional Ownership (%)", "Short Float (%)"):
            if mk in finviz_data:
                val = str(finviz_data[mk])
                p, tp, r = calculate_scoring(mk, val)
                table_rows.append({
                    "Metric Name": mk, "Source": "Finviz", "Value": val,
                    "Obtained points": "rejected" if r else str(p),
                    "Total points": str(tp),
                })

    eps_str = (
        f"{eps_val:.2f}%"
        if eps_val is not None and not isinstance(eps_val, Exception)
        else "N/A"
    )
    iv_str = (
        iv_rank_result.split(":")[-1].strip()
        if "Success!" in str(iv_rank_result)
        else "N/A"
    )

    for name, source, val in [
        ("GuruFocus Moat Score",               "GuruFocus",    str(moat_score)),
        ("Forward EPS Growth (%)",             "Alpha Vantage", eps_str),
        ("IV Rank",                            "Unusual Whales", iv_str),
        ("CEO Ownership %",                    "Perplexity",   llm["ceo_ownership"]),
        ("Business Model & Value Proposition", "Perplexity",   llm["classification"]),
    ]:
        p, tp, r = calculate_scoring(name, val)
        table_rows.append({
            "Metric Name": name, "Source": source, "Value": val,
            "Obtained points": "rejected" if r else str(p),
            "Total points": str(tp),
        })

    score = sum(
        float(r["Obtained points"])
        for r in table_rows
        if r["Obtained points"] not in ("rejected", "")
    )
    is_rejected = any(r["Obtained points"] == "rejected" for r in table_rows)
    verdict = (
        "Rejected"              if is_rejected else
        "Elite LEAPS Candidate" if score >= 82 else
        "Qualified"             if score >= 65 else
        "Watchlist"             if score >= 49 else
        "Rejected"
    )

    return int(score), verdict, table_rows, llm, sws_data


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_and_save_score(ticker: str) -> tuple[int | None, str | None]:
    """
    Run the full 7-source parallel analysis (same as LEAPS Evaluator),
    compute the score using the exact same rules, and upsert into the shared
    BigQuery master_table.

    Returns (score, verdict) on success, (None, None) on failure.
    """
    logger.info(f"Thesis scoring (full pipeline): {ticker}")
    try:
        raw = _run_analysis(ticker)
        score, verdict, table_rows, llm, sws = _build_report(ticker, raw)
        _upsert_score(ticker, score, verdict)
        logger.info(f"Thesis score saved: {ticker} → {score} ({verdict})")
        return score, verdict
    except Exception as e:
        logger.error(f"Thesis scoring failed for {ticker}: {e}")
        return None, None
