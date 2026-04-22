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
from __future__ import annotations


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

    PILLAR 1 — SURVIVAL GATE (36 pts):
      Cash Runway(16), Net Debt/EBITDA(9), Assets/Liab(5),
      Share Count Growth(4), Expiration Date(2)

    PILLAR 2 — EXPLOSIVE POTENTIAL (64 pts):
      [Cash Quality — backtest r=0.11–0.19, crisis-confirmed]
        OCF Per Share(16), EBITDA Margin(8), Operating ROA(6)
      [Size — more room to run]
        Market Cap(7)
      [Amplification]
        DOL(7)
      [Secondary signals — weak but directional]
        Revenue Growth(4), Gross Margin(4)

    MOAT (3 pts + hard reject gate):
      GF Moat Score: ≥3→3pts, ≥2→2pts, ≥1→1pt, unknown→1pt, 0→0pts
      HARD REJECT if GF=0 AND OCF/Share < -5.0 (value trap: no moat + severe burn)

    DISPLAY-ONLY (0 pts — context, not score):
      52-Week Position (PHASE 2 TIMING TRIGGER — see below),
      Business Model narrative, EPS Growth, Growth-to-Valuation,
      Revenue Growth, Gross Margin

    PHASE 2 — ENTRY TIMING (use AFTER stock is on watchlist):
      52-Week Position is the primary buy trigger.
      BUY ZONE: position ≤ 0.25 (stock in bottom quarter of its annual range).
      WAIT ZONE: 0.25–0.50 — on watchlist, not yet acting.
      AVOID NOW: > 0.50 — wait for a pullback.
      Data: r=-0.117 overall / -0.146 (2020 crisis) / -0.245 (2021 bull).
      Belongs here, not in Phase 1 — a stock at its 52wk high can be a great
      candidate; scoring it down would filter out companies you should be watching.

    DATA BASIS — 46,219 (symbol, date) pairs, 2016–2021, winner_threshold=+100%:
      OCF Per Share:     Spearman r=+0.139 overall / +0.156 (2018) / +0.152 (2020)
      52wk Position:     r=-0.117 overall / -0.146 (2020) / -0.245 (2021)
      EBITDA Margin:     r=+0.109 (2018) / +0.140 (2020) — crisis-consistent
      Revenue Growth:    r=+0.013 overall / 0.000 in 2018+2020 — bull-only noise
      INFLECTION THESIS: near-breakeven OCF (approaching zero from below) has
      highest doubler rate — market hasn't re-rated yet, turnaround catalyst imminent.

    Verdict thresholds: Elite ≥75 | Qualified ≥60 | Watchlist ≥45 | <45 Rejected

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
        total = 16
        if val_str in _NA:
            obtained = 5
        elif any(p in val_str for p in ("positive", "no cash burn", "no burn", "profitable")):
            obtained = 16
        elif val_num >= 24:
            obtained = 16
        elif val_num >= 18:
            obtained = 13
        elif val_num >= 12:
            obtained = 10
        elif val_num >= 6:
            obtained = 5
        elif val_num >= 3:
            obtained = 2
        elif val_num > 0 and "month" not in val_str:
            obtained = 16   # raw value without "months" — treat as years
        else:
            is_rejected = True   # < 3 months cash — company likely won't survive

    elif "assets" in name and "liabilities" in name:
        total = 4
        if val_str in _NA:
            obtained = 2
        elif val_num >= 3.0:
            obtained = 4
        elif val_num >= 2.0:
            obtained = 3
        elif val_num >= 1.5:
            obtained = 2
        elif val_num >= 1.0:
            obtained = 1
        else:
            obtained = 0

    elif "net debt / ebitda" in name:
        total = 9
        net_debt = safe_float(getattr(_thread_local, "net_debt_val", None))
        ebitda   = safe_float(getattr(_thread_local, "ebitda_val",   None))
        if net_debt < 0:
            obtained = 9   # net cash position — no debt risk
        elif ebitda <= 0:
            # Pre-profitable — grade on reported ratio or raw debt level
            if val_num == 0 or val_str in _NA:
                obtained = 4
            elif val_num <= 5:
                obtained = 3
            elif val_num <= 8:
                obtained = 1
            # > 8: 0 pts, no reject
        elif val_num == 0 or val_str in _NA:
            obtained = 9
        elif val_num <= 1.5:
            obtained = 8
        elif val_num <= 3:
            obtained = 6
        elif val_num <= 5:
            obtained = 4
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
        # r=+0.024 overall — too weak to score. Display-only for business quality context.
        return (0, 0, False)
        if val_str in _NA:
            obtained = 2
        else:
            try:
                pct = float(val_str.replace("%", ""))
                if pct >= 60:
                    obtained = 4
                elif pct >= 40:
                    obtained = 3
                elif pct >= 20:
                    obtained = 2
                elif pct >= 0:
                    obtained = 1
                # negative: 0 pts
            except Exception:
                obtained = 2

    elif "expiration" in name:
        # REJECTION RULE: any expiration < 18 months is a hard reject.
        # Primary purpose is the gate — bonus pts are minor (max 2).
        # N/A = contract not yet specified → skip (can't reject the unknown).
        total = 2
        obtained = 0
        if val_str not in _NA:
            try:
                import pandas as pd
                diff_months = (
                    (pd.to_datetime(value).date() - datetime.date.today()).days // 30
                )
                if diff_months < 18:
                    is_rejected = True   # hard reject — not enough time for thesis to play out
                elif diff_months >= 36:
                    obtained = 2         # 3+ years: maximum time premium
                elif diff_months >= 24:
                    obtained = 1         # 2–3 years: solid LEAPS window
                # 18–23 months: passes gate, 0 bonus pts
            except Exception:
                obtained = 0   # Can't parse date — skip, don't reject

    # ── CASH QUALITY (backtest r=0.11–0.19, crisis-confirmed) ─────────────────

    elif "ocf per share" in name:
        # #1 predictor (r=+0.139 overall, +0.156 in 2018 bear, +0.152 in 2020 crisis).
        # Inflection-point thesis: near-zero OCF (approaching breakeven from below)
        # has the highest LEAPS doubler rate — market hasn't re-rated yet.
        # Sweet spot: slightly negative to just-positive. Deep negative = bankruptcy risk.
        # Cache raw value so the moat block can run its value-trap gate.
        _thread_local.ocf_per_share_val = val_num
        total = 20
        if val_str in _NA:
            obtained = 7   # unknown — neutral
        else:
            try:
                ocf_ps = float(val_num)
                if ocf_ps >= 0:
                    # Profitable — cash generative. Good but potentially priced in.
                    if ocf_ps >= 1.0:
                        obtained = 15   # solidly profitable — strong but may be discovered
                    else:
                        obtained = 18   # just turned profitable — still early inflection
                elif ocf_ps >= -0.50:
                    obtained = 20   # near-zero negative — SWEET SPOT (approaching breakeven)
                elif ocf_ps >= -2.0:
                    obtained = 12   # moderate burn — watchable
                elif ocf_ps >= -5.0:
                    obtained = 6    # heavy burn — elevated risk
                else:
                    obtained = 2    # severe burn — near-bankruptcy risk
            except Exception:
                obtained = 8

    elif "ebitda margin" in name:
        # r=+0.109 (2018 bear) / +0.140 (2020 crisis) — consistent crisis signal.
        # Profitability quality: higher margin = more resilient under stress.
        total = 10
        if val_str in _NA:
            obtained = 4
        else:
            try:
                pct = float(val_str.replace("%", ""))
                if pct >= 20:
                    obtained = 10
                elif pct >= 10:
                    obtained = 8
                elif pct >= 5:
                    obtained = 7
                elif pct >= 0:
                    obtained = 5   # near EBITDA-positive — inflection zone
                elif pct >= -10:
                    obtained = 2   # moderate EBITDA loss
                else:
                    obtained = 1   # deeply EBITDA-negative
            except Exception:
                obtained = 4

    elif "operating roa" in name:
        # r=+0.121 (2018) / +0.112 (2020) — capital efficiency under stress.
        # How much operating profit per dollar of assets? Higher = leaner, more explosive.
        total = 8
        if val_str in _NA:
            obtained = 3
        else:
            try:
                pct = float(val_str.replace("%", ""))
                if pct >= 20:
                    obtained = 8
                elif pct >= 10:
                    obtained = 7
                elif pct >= 3:
                    obtained = 5
                elif pct >= 0:
                    obtained = 4   # barely positive — approaching efficiency
                elif pct >= -10:
                    obtained = 1
                else:
                    obtained = 0
            except Exception:
                obtained = 3

    # ── MEAN REVERSION SETUP ──────────────────────────────────────────────────

    elif "52-week position" in name or ("52" in name and "position" in name):
        # PHASE 2 TIMING TRIGGER — display-only in Phase 1 (watchlist qualification).
        # A great company at its 52wk high is still a great candidate — score it now,
        # time the entry later when it pulls back into the buy zone (≤0.25).
        # BUY ZONE ≤0.25 | WAIT 0.25–0.50 | AVOID NOW >0.50
        # Data: r=-0.117 overall / -0.146 (2020 crisis) / -0.245 (2021 bull)
        return (0, 0, False)

    # ── PILLAR 2: UPSIDE POTENTIAL ────────────────────────────────────────────

    elif "operating leverage" in name or "(dol)" in name:
        total = 7
        if val_str in _NA:
            obtained = 2   # unknown — neutral
        elif val_num >= 3:
            obtained = 7   # very high leverage — earnings explode with revenue growth
        elif val_num >= 2:
            obtained = 5   # high leverage — strong amplification
        elif val_num >= 1:
            obtained = 3   # moderate — some amplification
        elif val_num > 0:
            obtained = 1   # low leverage
        # ≤ 0 (declining or negative): 0 pts

    elif "revenue growth" in name:
        # r=+0.013 (46K samples), r=0.000 in crisis years 2018/2020 — display-only.
        # Shown for context but contributes 0 pts to avoid rewarding bull-only noise.
        return (0, 0, False)
        if val_str not in _NA:
            try:
                pct = float(val_str.replace("%", ""))
                if pct >= 30:
                    obtained = 4
                elif pct >= 15:
                    obtained = 3
                elif pct >= 5:
                    obtained = 2
                elif pct > 0:
                    obtained = 1
                # negative or 0: 0 pts
            except Exception:
                pass

    elif "growth-to-valuation" in name:
        # Display-only: built on revenue growth (r=0.013) — no empirical scoring value.
        return (0, 0, False)

    elif "eps growth" in name:
        # Display-only: forward analyst estimates have poor track record for small-caps.
        # Directionally useful context but not scored to avoid rewarding analyst optimism bias.
        return (0, 0, False)

    elif "market cap" in name:
        total = 7
        billions = val_num
        if "t" in val_str:
            billions = val_num * 1000
        elif "m" in val_str and "b" not in val_str:
            billions = val_num / 1000
        if billions <= 0.3:
            obtained = 7   # micro-cap ≤$300M — maximum asymmetry, most explosive
        elif billions <= 1:
            obtained = 6   # small cap ≤$1B
        elif billions <= 3:
            obtained = 5
        elif billions <= 10:
            obtained = 3
        elif billions <= 30:
            obtained = 2
        elif billions <= 100:
            obtained = 1
        # > $100B: 0 pts, no reject — large cap LEAPS still valid if thesis is strong

    elif "moat score" in name:
        # Moat acts as two things simultaneously:
        #   1. Value-trap hard reject: no moat (GF=0) + deeply cash-burning = reject.
        #      A company losing cash with no competitive protection is unlikely to
        #      survive 24 months for the LEAPS thesis to play out.
        #   2. Durability bonus (3 pts): strong moat (GF≥3) = recovery sticks.
        #      Not scored higher because high-moat small-caps are often already discovered
        #      and priced for quality — the explosive setup requires the moat to be
        #      unrecognised, which we can't quantify from GF score alone.
        total = 3
        try:
            gf = float(val_num)
            _thread_local.gf_score = gf
        except Exception:
            gf = 0.0
            _thread_local.gf_score = 0.0

        # Value-trap gate: GF=0 means GuruFocus explicitly found no moat.
        # Only reject if OCF is also deeply negative (severe cash burn).
        # If moat is unknown (N/A) we give benefit of the doubt — don't reject.
        ocf_ps = safe_float(getattr(_thread_local, "ocf_per_share_val", None))
        no_moat = (val_str not in _NA and gf == 0.0)
        deep_burn = (ocf_ps is not None and ocf_ps < -5.0)
        if no_moat and deep_burn:
            is_rejected = True   # value trap: no moat + severe cash burn
        elif val_str in _NA:
            obtained = 1   # unknown — neutral, small benefit of doubt
        elif gf >= 3.0:
            obtained = 3   # strong moat — recovery durability confirmed
        elif gf >= 2.0:
            obtained = 2   # moderate moat — some protection
        elif gf >= 1.0:
            obtained = 1   # narrow moat — better than none
        # gf == 0 but not deep-burn: 0 pts, no reject

    elif "business model" in name:
        # Display-only: qualitative LLM narrative — important context but moat
        # scoring is already handled by GuruFocus Moat Score above.
        return (0, 0, False)

    elif "ceo ownership" in name:
        # 3 pts — skin in game. High CEO ownership = incentive alignment.
        total = 3
        if "not disclosed" in val_str or val_str in _NA:
            obtained = 1
        elif val_num >= 5:
            obtained = 3
        elif val_num >= 2:
            obtained = 2
        elif val_num >= 1:
            obtained = 1

    elif "buying vs selling" in name or "insider buying" in name:
        # 3 pts — directional conviction signal. Net buying > net selling = bullish.
        total = 3
        if val_str in _NA:
            obtained = 1   # unknown — neutral
        elif val_num >= 5:
            obtained = 3
        elif val_num >= 1:
            obtained = 2
        elif val_num > 0:
            obtained = 1
        elif val_num == 0:
            obtained = 1
        # net selling: 0 pts

    elif "institutional ownership" in name:
        # 4 pts — low ownership = undiscovered. High = over-owned, limited new demand.
        total = 4
        if val_str in _NA:
            obtained = 2
        elif val_num < 10:
            obtained = 4   # undiscovered gem
        elif val_num < 20:
            obtained = 3
        elif val_num <= 50:
            obtained = 2
        elif val_num <= 70:
            obtained = 1
        else:
            obtained = 0   # over-owned

    elif "short float" in name:
        # Display-only: squeeze is a catalyst mechanism, not a fundamental predictor.
        # Shown as context (high short float = binary event risk) but not scored.
        return (0, 0, False)

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
    _thread_local.gf_score        = 0.0
    _thread_local.net_debt_val    = None
    _thread_local.ebitda_val      = None
    _thread_local.ocf_per_share_val = None

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

    finviz_ok = finviz_data and not isinstance(finviz_data, Exception)
    for mk in ("Net Insider Buying vs Selling (%)",
               "Institutional Ownership (%)", "Short Float (%)"):
        val = str(finviz_data[mk]) if finviz_ok and mk in finviz_data else "N/A"
        p, tp, r = calculate_scoring(mk, val)
        table_rows.append({
            "Metric Name": mk, "Source": "Finviz", "Value": val,
            "Obtained points": "rejected" if r else (str(p) if tp > 0 else ""),
            "Total points": str(tp) if tp > 0 else "",
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
        "Elite LEAPS Candidate" if score >= 75 else
        "Qualified"             if score >= 60 else
        "Watchlist"             if score >= 45 else
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
