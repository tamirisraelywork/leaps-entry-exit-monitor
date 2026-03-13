"""
rescore_all.py — Re-apply current scoring rules to all stored BigQuery entries.

Reads existing metric VALUES from BigQuery (no API calls to Yahoo Finance / Gemini etc.)
and re-calculates scores using the current calculate_scoring() rules.

Run from the leaps-exit-agent/ root:
    python -m scripts.rescore_all

Requires these env vars (or a populated .streamlit/secrets.toml on Streamlit Cloud):
    SERVICE_ACCOUNT_JSON  — full service account JSON string
    DATASET_ID            — e.g. "leaps_monitor"  (the evaluator master dataset)
"""

import sys
import os

# Ensure leaps-exit-agent root is on the path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from google.cloud import bigquery
from google.oauth2 import service_account
import json
import re
import pandas as pd

import score_thesis
from shared.config import cfg, cfg_dict


def safe_float(val):
    if val is None or str(val).lower() in ("n/a", "none", "rejected", "", "nan"):
        return 0.0
    try:
        return float(re.sub(r"[^\d.-]", "", str(val)) or "0")
    except Exception:
        return 0.0


def _verdict(score, is_rejected):
    if is_rejected:
        return "❌ Rejected"
    if score >= 75:
        return "🔥 Elite LEAPS Candidate"
    if score >= 60:
        return "✅ Qualified"
    if score >= 45:
        return "⚠️ Watchlist"
    return "❌ Rejected"


def get_client():
    sa = cfg_dict("SERVICE_ACCOUNT_JSON")
    if not sa:
        raw = cfg("SERVICE_ACCOUNT_JSON")
        sa = json.loads(raw) if raw else {}
    if not sa:
        raise RuntimeError(
            "SERVICE_ACCOUNT_JSON not found. "
            "Set it as an environment variable before running this script."
        )
    sa["private_key"] = sa.get("private_key", "").replace("\\n", "\n")
    creds = service_account.Credentials.from_service_account_info(sa)
    return bigquery.Client(credentials=creds, project=sa["project_id"])


def get_all_tickers(client, master_path):
    rows = list(client.query(f"SELECT DISTINCT Ticker FROM `{master_path}`").result())
    return [r["Ticker"] for r in rows if r["Ticker"]]


def ticker_table_name(ticker):
    return re.sub(r"[^a-zA-Z0-9_]", "_", ticker.upper())


def rescore_ticker(client, project, dataset, ticker):
    master_path = f"`{project}.{dataset}.master_table`"
    detail_table = f"{project}.{dataset}.{ticker_table_name(ticker)}"
    detail_path  = f"`{detail_table}`"

    # Read current score
    old_df = client.query(
        f"SELECT Score, Verdict FROM {master_path} WHERE Ticker = @t",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("t", "STRING", ticker)
        ]),
    ).to_dataframe()
    old_score   = int(safe_float(old_df["Score"].iloc[0]))   if not old_df.empty else 0
    old_verdict = str(old_df["Verdict"].iloc[0])              if not old_df.empty else "N/A"

    # Read detail rows
    try:
        detail_df = client.query(f"SELECT * FROM {detail_path}").to_dataframe()
    except Exception as e:
        if "404" in str(e) or "not found" in str(e).lower():
            return old_score, old_score, old_verdict, old_verdict, "no detail table"
        raise

    if detail_df.empty:
        return old_score, old_score, old_verdict, old_verdict, "empty"

    m_col = "Matric name"    if "Matric name"    in detail_df.columns else "Metric Name"
    s_col = "Obtained Score" if "Obtained Score" in detail_df.columns else "Obtained points"
    t_col = "Total score"    if "Total score"    in detail_df.columns else "Total points"

    QUAL         = {"Risks", "Rewards", "Company Description", "Value Proposition", "Moat Analysis", "DATE"}
    SKIP_RESCORE = {"latest expiration date", "expiration date"}

    # Pre-seed thread-local values
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
            try:
                tl.gf_score = float(safe_float(val))
            except Exception:
                pass

    new_rows    = []
    total_score = 0.0
    is_rejected = False

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

        value               = str(r.get("Value", "N/A"))
        pts, total_pts, rej = score_thesis.calculate_scoring(metric_name, value)
        if rej:
            is_rejected  = True
            r[s_col]     = "rejected"
            r[t_col]     = str(total_pts) if total_pts > 0 else ""
        else:
            r[s_col] = str(pts)       if total_pts > 0 else ""
            r[t_col] = str(total_pts) if total_pts > 0 else ""
            if total_pts > 0:
                total_score += pts
        new_rows.append(r)

    # Write updated detail rows back
    rows_df = pd.DataFrame(new_rows).where(pd.notnull(pd.DataFrame(new_rows)), None)
    client.load_table_from_dataframe(
        rows_df, detail_table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    ).result()

    new_score   = int(round(total_score))
    new_verdict = _verdict(new_score, is_rejected)

    client.query(
        f"UPDATE {master_path} SET Score=@score, Verdict=@verdict WHERE Ticker=@ticker",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("score",   "INT64",  new_score),
            bigquery.ScalarQueryParameter("verdict", "STRING", new_verdict),
            bigquery.ScalarQueryParameter("ticker",  "STRING", ticker),
        ]),
    ).result()

    return old_score, new_score, old_verdict, new_verdict, "ok"


def main():
    print("Connecting to BigQuery...")
    try:
        client = get_client()
    except RuntimeError as e:
        print(f"\n❌ {e}")
        print("\nAlternative: go to the app → Settings → '🔄 Rescore All' button.")
        sys.exit(1)

    raw_dataset = cfg("DATASET_ID") or cfg("LEAPS_MONITOR_DATASET", "leaps_monitor")
    dataset     = raw_dataset.split(".")[-1]   # strip project prefix if present
    master_path = f"`{client.project}.{dataset}.master_table`"
    print(f"Dataset: {client.project}.{dataset}\n")

    tickers = get_all_tickers(client, master_path)
    if not tickers:
        print("No tickers found in master_table.")
        sys.exit(0)

    print(f"Rescoring {len(tickers)} tickers with 100pt rules...\n")
    print(f"{'Ticker':<8} {'Old':>5} {'New':>5}  {'Change':>8}  Result")
    print("─" * 55)

    changed = 0
    errors  = 0
    for ticker in sorted(tickers):
        try:
            old, new, old_v, new_v, status = rescore_ticker(client, client.project, dataset, ticker)
            delta    = new - old
            delta_s  = f"{delta:+d}" if delta != 0 else "  ="
            changed += 1 if delta != 0 else 0
            tag      = f"  {new_v}" if delta != 0 else ""
            print(f"{ticker:<8} {old:>5} {new:>5}  {delta_s:>8}  {status}{tag}")
        except Exception as e:
            print(f"{ticker:<8} ERROR: {e}")
            errors += 1

    print("─" * 55)
    print(f"\nDone. {changed} scores changed, {errors} errors.")
    print("Verdicts: Elite ≥75 | Qualified ≥60 | Watchlist ≥45 | <45 Rejected")


if __name__ == "__main__":
    main()
