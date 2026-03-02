"""
score_thesis.py — Auto-compute LEAPS thesis scores inside the exit agent.

Fetches fundamental metrics via yfinance, asks Gemini for moat + business
model classification, then applies the exact same scoring rules used by the
LEAPS Evaluator (leaps-evaluator.streamlit.app).

Results are upserted into the shared BigQuery master_table so both apps read
the same data.

Expert-recommended refresh schedule
────────────────────────────────────
LEAPS thesis is driven by business fundamentals that change on quarters,
not days.  The right balance between freshness and API cost:

  Normal months:          re-score if last score is > 30 days old
  Earnings months         re-score if last score is >  7 days old
  (Feb / May / Aug / Nov) ↑ most earnings releases happen in these months

The daily staleness-check job in monitor.py calls needs_refresh() for each
active ticker and only fires compute_and_save_score() when warranted.
This gives you a thesis score that is:
  • Never more than 30 days stale in quiet months
  • Never more than 7 days stale around earnings
  • Always available (no more N/A) for any ticker in your portfolio
"""

import json
import re
import logging
from datetime import date

import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def _bq_client() -> bigquery.Client:
    raw = st.secrets["SERVICE_ACCOUNT_JSON"]
    sa = json.loads(raw) if isinstance(raw, str) else dict(raw)
    sa["private_key"] = sa.get("private_key", "").replace("\\n", "\n")
    creds = service_account.Credentials.from_service_account_info(sa)
    return bigquery.Client(credentials=creds, project=sa["project_id"])


def _master_table_path(client: bigquery.Client) -> str:
    raw = (
        st.secrets.get("DATASET_ID")
        or st.secrets.get("LEAPS_MONITOR_DATASET", "leaps_monitor")
    )
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
        # Tighter window in earnings months
        threshold = 7 if date.today().month in (2, 5, 8, 11) else 30
        return age >= threshold
    except Exception:
        return True   # assume stale on any error


def _upsert_score(client: bigquery.Client, ticker: str, score: int, verdict: str):
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
# Data fetching — yfinance
# ---------------------------------------------------------------------------

def _sf(v) -> float:
    """Safe float conversion."""
    if v is None or str(v).lower() in ("n/a", "none", "rejected", "", "nan"):
        return 0.0
    try:
        return float(re.sub(r"[^\d.-]", "", str(v)) or "0")
    except Exception:
        return 0.0


def _get_df(df, keys) -> float | None:
    """Return first non-null float from a DataFrame indexed by row names."""
    if df is None:
        return None
    try:
        if df.empty:
            return None
    except Exception:
        return None
    import pandas as pd
    for k in keys:
        if k in df.index:
            try:
                v = df.loc[k].iloc[0]
                if pd.notnull(v):
                    return float(v)
            except Exception:
                continue
    return None


def _fetch_metrics(ticker: str) -> dict:
    """Fetch all scoring metrics using yfinance (no proxy needed for background jobs)."""
    import yfinance as yf
    import pandas as pd

    t = yf.Ticker(ticker)
    info = {}
    try:
        info = t.info or {}
    except Exception:
        pass

    bs_q = bs_a = cf_q = inc_a = None
    try: bs_q = t.quarterly_balance_sheet
    except Exception: pass
    try: bs_a = t.balance_sheet
    except Exception: pass
    try: cf_q = t.quarterly_cashflow
    except Exception: pass
    try: inc_a = t.financials
    except Exception: pass

    mc = info.get("marketCap")

    def _fmt_mc(v):
        if not v:
            return "N/A"
        return f"{v / 1e9:.2f} Billion"

    # Revenue growth
    rev_g = info.get("revenueGrowth")
    rev_g_str = f"{rev_g * 100:.2f}%" if rev_g is not None else "N/A"
    # Tier 2: from annual financials
    if rev_g is None and inc_a is not None and "Total Revenue" in inc_a.index and inc_a.shape[1] >= 2:
        try:
            row = inc_a.loc["Total Revenue"]
            if pd.notnull(row.iloc[0]) and pd.notnull(row.iloc[1]) and row.iloc[1] != 0:
                rev_g_str = f"{(row.iloc[0] - row.iloc[1]) / abs(row.iloc[1]) * 100:.2f}%"
        except Exception:
            pass

    # Gross margin
    gm = info.get("grossMargins")
    gm_str = f"{gm * 100:.2f}%" if gm is not None else "N/A"

    # Insider / institutional
    insider = info.get("heldPercentInsiders")
    insider_str = f"{insider * 100:.2f}%" if insider is not None else "N/A"
    inst = info.get("heldPercentInstitutions")
    inst_str = f"{inst * 100:.2f}%" if inst is not None else "N/A"

    # Short float
    short_f = info.get("shortPercentOfFloat")
    short_str = f"{short_f * 100:.2f}%" if short_f is not None else "N/A"

    # EPS growth
    eps_g = info.get("earningsGrowth") or info.get("earningsQuarterlyGrowth")
    eps_str = f"{eps_g * 100:.2f}%" if eps_g is not None else "N/A"

    # Assets / Liabilities
    total_assets = _get_df(bs_q, ["Total Assets"])
    total_liab = _get_df(bs_q, [
        "Total Liabilities Net Minor Interest", "Total Liab", "Total Liabilities"
    ])
    al_ratio = (
        f"{total_assets / total_liab:.2f}"
        if total_assets and total_liab and total_liab > 0
        else "N/A"
    )

    # Runway
    cash = _get_df(bs_q, [
        "Cash And Cash Equivalents",
        "Cash Cash Equivalents And Short Term Investments",
    ])
    ocf = _get_df(cf_q, ["Operating Cash Flow"])
    if cash is not None and ocf is not None:
        if ocf < 0:
            burn = abs(ocf) / 3
            runway_str = f"{cash / burn:.2f} Months" if burn > 0 else "N/A"
        else:
            runway_str = "Positive OCF (No Burn)"
    else:
        runway_str = "N/A"

    # Net Debt / EBITDA
    ebitda_val = _get_df(inc_a, ["EBITDA", "Normalized EBITDA"])
    net_debt_val = _get_df(bs_a, ["Net Debt"])
    if net_debt_val is None:
        td = _get_df(bs_a, ["Total Debt"])
        ca = _get_df(bs_a, ["Cash And Cash Equivalents"])
        if td is not None and ca is not None:
            net_debt_val = td - ca
    if ebitda_val and ebitda_val != 0 and net_debt_val is not None:
        nd_ebitda_str = f"{net_debt_val / ebitda_val:.2f}"
    else:
        nd_ebitda_str = "N/A"

    # Cash burn severity
    fcf = None
    if cf_q is not None and "Free Cash Flow" in cf_q.index:
        try:
            fcf = float(cf_q.loc["Free Cash Flow"].iloc[:4].sum())
        except Exception:
            pass
    if mc and fcf is not None:
        burn_sev = (
            f"{abs(fcf) / mc * 100:.2f}%"
            if fcf < 0
            else "0.00% (Positive FCF)"
        )
    else:
        burn_sev = "N/A"

    # Share count growth (YoY from annual balance sheet)
    share_growth = "N/A"
    if bs_a is not None:
        for skey in ["Ordinary Shares Number", "Share Issued"]:
            if skey in bs_a.index:
                try:
                    row = bs_a.loc[skey]
                    if (len(row) >= 2
                            and pd.notnull(row.iloc[0])
                            and pd.notnull(row.iloc[1])
                            and row.iloc[1] > 0):
                        chg = (row.iloc[0] - row.iloc[1]) / row.iloc[1] * 100
                        share_growth = f"{chg:.2f}%"
                        break
                except Exception:
                    pass

    # Degree of Operating Leverage
    dol = "N/A"
    if inc_a is not None and "Total Revenue" in inc_a.index and inc_a.shape[1] >= 2:
        try:
            sales = inc_a.loc["Total Revenue"]
            for k in ["EBIT", "Operating Income"]:
                if k in inc_a.index:
                    ebit_row = inc_a.loc[k]
                    s1, s2 = float(sales.iloc[0]), float(sales.iloc[1])
                    e1, e2 = float(ebit_row.iloc[0]), float(ebit_row.iloc[1])
                    if s2 != 0 and e2 != 0:
                        ps_ = (s1 - s2) / abs(s2)
                        pe_ = (e1 - e2) / abs(e2)
                        if ps_ != 0:
                            dol = f"{pe_ / ps_:.2f}"
                    break
        except Exception:
            pass

    # Growth-to-Valuation (Revenue Growth % / P-S Ratio)
    ps = info.get("priceToSalesTrailingTwelveMonths")
    gtv = "N/A"
    if ps and ps > 0 and rev_g and rev_g > 0:
        gtv = f"{(rev_g * 100) / ps:.2f}"

    # Capital structure
    dte = info.get("debtToEquity") or 0
    csp = "Heavy converts / ATM" if dte > 300 else "No converts"

    # Latest options expiry
    latest_exp = "N/A"
    try:
        exps = t.options
        if exps:
            latest_exp = exps[-1]
    except Exception:
        pass

    return {
        "Market cap":                   _fmt_mc(mc),
        "Revenue Growth YoY (%)":       rev_g_str,
        "Gross Margin (%)":             gm_str,
        "Total insider ownership %":    insider_str,
        "Short Float":                  short_str,
        "Institutional Ownership":      inst_str,
        "EPS Growth (Forward %)":       eps_str,
        "Assets / Liabilities Ratio":   al_ratio,
        "Runway":                       runway_str,
        "Net Debt / EBITDA":            nd_ebitda_str,
        "Cash Burn Severity":           burn_sev,
        "Share Count Growth":           share_growth,
        "Degree of Operating Leverage": dol,
        "Growth-to-Valuation Score":    gtv,
        "Capital Structure Pressure":   csp,
        "latest expiration date":       latest_exp,
        # Raw values needed for Net Debt / EBITDA scoring branch
        "_net_debt_raw":    net_debt_val,
        "_ebitda_raw":      ebitda_val,
    }


# ---------------------------------------------------------------------------
# Gemini — moat + business model
# ---------------------------------------------------------------------------

def _gemini_moat_biz(ticker: str) -> tuple[str, str]:
    """Ask Gemini for GuruFocus moat score (0-5) and business model category."""
    try:
        import requests as req
        key = st.secrets.get("GEMINI_API_KEY", "")
        if not key:
            return "N/A", "N/A"
        prompt = (
            f"Ticker: {ticker}\n"
            "Answer ONLY these two questions, no explanation:\n"
            "1. GuruFocus moat score — integer 0 to 5 (0=no moat, 5=wide moat). "
            "Be conservative.\n"
            "2. Business model — pick exactly one:\n"
            "   Mission-critical/infrastructure\n"
            "   SaaS/platform/high switching costs\n"
            "   High-growth/disruptive/emerging\n"
            "   Commodity/retail\n"
            "Format EXACTLY as:\nMOAT:X\nBUSINESS:category name"
        )
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-2.0-flash:generateContent?key={key}"
        )
        resp = req.post(
            url,
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=30,
        )
        text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        moat_m = re.search(r"MOAT:\s*([0-5])", text)
        biz_m  = re.search(r"BUSINESS:\s*(.+)",  text, re.IGNORECASE)
        return (
            moat_m.group(1) if moat_m else "N/A",
            biz_m.group(1).strip() if biz_m else "N/A",
        )
    except Exception:
        return "N/A", "N/A"


# ---------------------------------------------------------------------------
# Scoring — identical rules to the LEAPS Evaluator's calculate_scoring()
# ---------------------------------------------------------------------------

def _score(m: dict) -> tuple[int, str]:
    """Apply the LEAPS Evaluator's exact scoring rules. Returns (score, verdict)."""
    total = 0
    rejected = False

    nd_raw = m.get("_net_debt_raw")   # float | None
    eb_raw = m.get("_ebitda_raw")     # float | None

    # ── Financial Survival & Balance Sheet (21 pts) ──────────────────────────

    # Runway (10 pts, < 6 months = rejected)
    runway = str(m.get("Runway", "N/A")).lower()
    if "positive" in runway or "no burn" in runway or "profitable" in runway:
        total += 10
    elif runway == "n/a":
        total += 3
    else:
        rnum = _sf(runway.split()[0])
        if rnum >= 24:   total += 10
        elif rnum >= 12: total += 7
        elif rnum >= 6:  total += 3
        else:            rejected = True

    # Net Debt / EBITDA (3 pts, > 3× = rejected)
    nd_num = _sf(m.get("Net Debt / EBITDA", "N/A"))
    if nd_raw is not None and nd_raw < 0:
        total += 3            # net cash
    elif eb_raw is not None and eb_raw <= 0:
        total += 0            # pre-profitable — no rejection
    elif nd_num == 0:
        total += 3
    elif nd_num <= 1.5:  total += 2
    elif nd_num <= 3.0:  total += 1
    elif nd_num  > 3.0:  rejected = True

    # Assets / Liabilities (1 pt)
    if _sf(m.get("Assets / Liabilities Ratio", "0")) >= 1.0:
        total += 1

    # Cash Burn Severity (2 pts)
    burn = m.get("Cash Burn Severity", "N/A")
    burn_num = _sf(str(burn).replace("%", ""))
    if "positive" in str(burn).lower() or burn_num == 0 or burn_num < 10:
        total += 2
    elif burn_num <= 20:
        total += 1

    # Share Count Growth (5 pts, > 30% = rejected)
    sg_str = str(m.get("Share Count Growth", "N/A")).lower()
    sg_num = _sf(sg_str.replace("%", ""))
    if sg_str == "n/a":      total += 1
    elif sg_num <= 0:        total += 5
    elif sg_num <  5:        total += 4
    elif sg_num < 10:        total += 2
    elif sg_num <= 30:       total += 0
    else:                    rejected = True

    # Capital Structure (1 pt; heavy converts = rejected)
    csp = str(m.get("Capital Structure Pressure", "")).lower()
    if "heavy" in csp or "atm" in csp:
        rejected = True
    else:
        total += 1

    # Options expiry gate (no points, < 18 months = rejected)
    le = m.get("latest expiration date", "N/A")
    if le and le != "N/A":
        try:
            exp_d = date.fromisoformat(str(le)[:10])
            if (exp_d - date.today()).days < 540:
                rejected = True
        except Exception:
            rejected = True
    else:
        rejected = True

    # ── Growth & Asymmetric Upside (35 pts) ──────────────────────────────────

    # Market Cap (4 pts, ≥ $8B = rejected)
    mc_str = str(m.get("Market cap", "N/A")).lower()
    mc_num = _sf(mc_str)
    if "trillion" in mc_str:  mc_b = mc_num * 1000
    elif "million" in mc_str: mc_b = mc_num / 1000
    else:                     mc_b = mc_num
    if   mc_b >= 8: rejected = True
    elif mc_b <  1: total += 4
    elif mc_b <  3: total += 3
    elif mc_b <  8: total += 2

    # Revenue Growth YoY (13 pts)
    rg = _sf(str(m.get("Revenue Growth YoY (%)", "N/A")).replace("%", ""))
    if   rg >= 50: total += 13
    elif rg >= 30: total += 10
    elif rg >= 20: total += 7
    elif rg >= 10: total += 4

    # Gross Margin (6 pts)
    gm = _sf(str(m.get("Gross Margin (%)", "N/A")).replace("%", ""))
    if   gm >= 50: total += 6
    elif gm >= 30: total += 4
    elif gm >= 15: total += 2
    elif gm >= 0:  total += 1

    # Forward EPS Growth (3 pts)
    eps = _sf(str(m.get("EPS Growth (Forward %)", "N/A")).replace("%", ""))
    if   eps >= 30: total += 3
    elif eps >= 20: total += 2
    elif eps >= 10: total += 1

    # Degree of Operating Leverage (3 pts)
    dol = _sf(m.get("Degree of Operating Leverage", "N/A"))
    if   dol >= 3:   total += 3
    elif dol >= 2:   total += 2
    elif dol >= 1.5: total += 1

    # Short Float (3 pts)
    sf_num = _sf(str(m.get("Short Float", "N/A")).replace("%", ""))
    if   10 <= sf_num <= 30: total += 3
    elif sf_num >= 5:        total += 2
    elif sf_num >  30:       total += 1

    # Institutional Ownership (3 pts)
    inst = _sf(str(m.get("Institutional Ownership", "N/A")).replace("%", ""))
    if   inst < 20: total += 3
    elif inst < 40: total += 2
    elif inst < 60: total += 1

    # ── Insider Alignment & Behavior (11 pts) ────────────────────────────────

    # Total Insider Ownership (5 pts)
    ins_str = str(m.get("Total insider ownership %", "N/A")).lower()
    ins_num = _sf(ins_str.replace("%", ""))
    if ins_str == "n/a":     total += 1
    elif 5 <= ins_num <= 30: total += 5
    elif ins_num >= 2:       total += 3
    elif ins_num >= 1:       total += 2

    # CEO Ownership + Net Insider Buying (6 pts combined)
    # yfinance doesn't easily surface CEO-specific ownership or net-buy ratio.
    # Award 2 floor pts to avoid penalising tickers where data is unavailable.
    total += 2

    # ── Moat & Qualitative Conviction (32 pts) ───────────────────────────────

    # GuruFocus Moat Score (10 pts)
    moat = _sf(m.get("Moat Score", "N/A"))
    if   moat >= 4: total += 10
    elif moat >= 3: total += 6
    elif moat >= 2: total += 3

    # Business Model Classification (15 pts)
    biz = str(m.get("Business Model", "N/A")).lower()
    if   "mission-critical" in biz or "infrastructure" in biz: total += 15
    elif "saas" in biz or "platform" in biz or "switching"  in biz: total += 10
    elif "high-growth" in biz or "disruptive" in biz or "emerging" in biz: total += 6
    elif "commodity" in biz or "retail" in biz: total += 5

    # Growth-to-Valuation Score (7 pts)
    gtv = _sf(m.get("Growth-to-Valuation Score", "N/A"))
    if   gtv >= 15: total += 7
    elif gtv >= 8:  total += 5
    elif gtv >= 4:  total += 3
    elif gtv >= 2:  total += 1

    total = min(total, 100)
    if rejected:              verdict = "Rejected"
    elif total >= 80:         verdict = "Elite LEAPS Candidate"
    elif total >= 70:         verdict = "Qualified"
    elif total >= 60:         verdict = "Watchlist"
    else:                     verdict = "Rejected"

    return total, verdict


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_and_save_score(ticker: str) -> tuple[int | None, str | None]:
    """
    Compute thesis score for a ticker using yfinance + Gemini, then upsert
    into the shared BigQuery master_table.

    Returns (score, verdict) on success, (None, None) on failure.
    """
    logger.info(f"Thesis scoring: {ticker}")
    try:
        metrics = _fetch_metrics(ticker)
        moat, biz = _gemini_moat_biz(ticker)
        metrics["Moat Score"]      = moat
        metrics["Business Model"]  = biz

        score, verdict = _score(metrics)

        client = _bq_client()
        _upsert_score(client, ticker, score, verdict)
        logger.info(f"Thesis score saved: {ticker} → {score} ({verdict})")
        return score, verdict
    except Exception as e:
        logger.error(f"Thesis scoring failed for {ticker}: {e}")
        return None, None
