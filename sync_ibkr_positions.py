"""
One-time script: sync BigQuery positions with IBKR activity statement (Aug 2025 – Mar 2026).
Reads credentials directly from .streamlit/secrets.toml — no Streamlit runtime needed.

Usage:
    python3 sync_ibkr_positions.py          # dry-run (print changes only)
    python3 sync_ibkr_positions.py --apply  # write to BigQuery
"""

import sys
import json
import os
from datetime import date

# ── Load secrets from .streamlit/secrets.toml without Streamlit ──────────────
def _load_secrets():
    toml_path = os.path.join(os.path.dirname(__file__), ".streamlit", "secrets.toml")
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            # Python < 3.11 fallback: manual parse for SERVICE_ACCOUNT_JSON
            tomllib = None

    if tomllib:
        with open(toml_path, "rb") as f:
            return tomllib.load(f)
    else:
        # Minimal fallback: extract SERVICE_ACCOUNT_JSON line manually
        secrets = {}
        with open(toml_path, "r") as f:
            content = f.read()
        import re
        m = re.search(r"SERVICE_ACCOUNT_JSON\s*=\s*'(.*?)'", content, re.DOTALL)
        if not m:
            m = re.search(r'SERVICE_ACCOUNT_JSON\s*=\s*"(.*?)"', content, re.DOTALL)
        if m:
            secrets["SERVICE_ACCOUNT_JSON"] = m.group(1)
        return secrets


def _get_bq_client():
    from google.cloud import bigquery
    from google.oauth2 import service_account

    secrets = _load_secrets()
    raw = secrets.get("SERVICE_ACCOUNT_JSON") or secrets.get("service_account_json")
    if not raw:
        raise RuntimeError("SERVICE_ACCOUNT_JSON not found in .streamlit/secrets.toml")
    sa_info = json.loads(raw) if isinstance(raw, str) else dict(raw)
    if "private_key" in sa_info:
        sa_info["private_key"] = sa_info["private_key"].replace("\\n", "\n")
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    return bigquery.Client(credentials=creds, project=sa_info["project_id"])


DATASET = "leaps_exit_agent"

# ─────────────────────────────────────────────────────────────────────────────
# Ground truth extracted from IBKR activity statement (Aug 2025 – Mar 6, 2026)
# Fields: (ticker, strike, expiry, opt_type, total_qty, avg_entry_price,
#          qty_trimmed, proceeds_from_trims, target_mode)
#
# avg_entry_price = weighted average of all BUY fills ($/share, × 100 = $/contract)
# proceeds_from_trims = total $ received from SELL fills (closing trades only)
# target_mode: "ACTIVE" (remaining > 0) or "CLOSED" (fully exited)
# ─────────────────────────────────────────────────────────────────────────────
IBKR_DATA = [
    # ticker     strike   expiry        type  qty  avg_px   trimmed  proceeds    mode
    ("AEHR",     40.00,  "2027-01-15",  "C",  25,  3.200,    10,    8_400.00, "ACTIVE"),
    ("DLO",      24.47,  "2027-01-15",  "C",  45,  1.900,     0,        0.00, "ACTIVE"),
    ("DLO",      25.00,  "2027-12-17",  "C",  15,  1.900,     0,        0.00, "ACTIVE"),
    ("EH",       35.00,  "2027-01-15",  "C",  70,  1.500,     0,        0.00, "ACTIVE"),
    ("ENVX",     20.00,  "2027-01-15",  "C",  52,  2.119,     0,        0.00, "ACTIVE"),
    ("EVGO",      5.00,  "2027-01-15",  "C",  80,  1.050,    80,    9_600.00, "CLOSED"),
    ("FIG",     100.00,  "2028-01-21",  "C",  22,  5.545,     0,        0.00, "ACTIVE"),
    ("FLNC",     13.00,  "2027-01-15",  "C",  64,  1.700,    64,   87_020.00, "CLOSED"),
    ("FLNC",     37.00,  "2028-01-21",  "C",  12,  8.000,     0,        0.00, "ACTIVE"),
    ("IWM",     195.00,  "2027-12-17",  "P",   7, 11.050,     0,        0.00, "ACTIVE"),
    ("JMIA",     12.00,  "2027-01-15",  "C",  60,  1.700,    40,   24_000.00, "ACTIVE"),
    ("LAC",       4.00,  "2027-01-15",  "C", 150,  0.740,   120,   36_000.00, "ACTIVE"),
    ("MVST",      4.50,  "2027-01-15",  "C", 100,  0.800,     0,        0.00, "ACTIVE"),
    ("OKTA",    180.00,  "2027-12-17",  "C",  14,  4.000,     0,        0.00, "ACTIVE"),
    ("OPRA",     25.00,  "2027-12-17",  "C",  22,  3.100,     0,        0.00, "ACTIVE"),
    ("PACB",      3.00,  "2027-01-15",  "C", 290,  0.338,   100,    8_000.00, "ACTIVE"),
    ("REAL",     15.00,  "2027-01-15",  "C",  90,  1.244,    70,   37_800.00, "ACTIVE"),
    ("RUM",      15.00,  "2027-01-15",  "C",  60,  1.439,    60,    8_400.00, "CLOSED"),
    ("SHLS",     12.00,  "2027-01-15",  "C", 119,  0.871,    60,   16_800.00, "ACTIVE"),
    ("SILJ",     35.00,  "2027-01-15",  "C",  35,  2.550,    20,   18_000.00, "ACTIVE"),
    ("XPOF",     15.00,  "2026-12-18",  "C",  80,  1.200,     0,        0.00, "ACTIVE"),
]

DRY_RUN = "--apply" not in sys.argv


def _strike_match(db_strike, target, tol=0.5):
    try:
        return abs(float(db_strike or 0) - target) <= tol
    except Exception:
        return False


def _expiry_match(db_exp, target):
    try:
        return str(db_exp)[:10] == target
    except Exception:
        return False


def find_position(positions, ticker, strike, expiry, opt_type):
    candidates = [
        p for p in positions
        if p["ticker"].upper() == ticker.upper()
        and _strike_match(p.get("strike"), strike)
        and _expiry_match(p.get("expiration_date"), expiry)
    ]
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        typed = [c for c in candidates if (c.get("option_type") or "CALL")[0].upper() == opt_type]
        return typed[0] if typed else candidates[0]
    # Fallback: ticker only
    by_ticker = [p for p in positions if p["ticker"].upper() == ticker.upper()]
    if len(by_ticker) == 1:
        print(f"  [WARN] {ticker}: matched by ticker only — verify manually")
        return by_ticker[0]
    return None


def main():
    from google.cloud import bigquery

    client = _get_bq_client()
    table  = f"`{client.project}.{DATASET}.positions`"

    print("Loading positions from BigQuery...")
    rows = list(client.query(f"SELECT * FROM {table} ORDER BY created_at DESC").result())
    positions = [dict(r) for r in rows]
    print(f"  Found {len(positions)} positions\n")

    matched   = 0
    not_found = []

    for (ticker, strike, expiry, opt_type, qty, avg_px,
         qty_trimmed, proceeds, target_mode) in IBKR_DATA:

        pos = find_position(positions, ticker, strike, expiry, opt_type)

        if pos is None:
            not_found.append(f"{ticker} {expiry} ${strike}{opt_type}")
            continue

        pos_id = str(pos["id"])

        old = {
            "qty":      pos.get("quantity"),
            "price":    pos.get("entry_price"),
            "trimmed":  pos.get("quantity_trimmed"),
            "proceeds": pos.get("proceeds_from_trims"),
            "mode":     pos.get("mode"),
        }
        new = {
            "qty":      qty,
            "price":    round(avg_px, 4),
            "trimmed":  qty_trimmed,
            "proceeds": round(proceeds, 2),
            "mode":     target_mode,
        }

        changed = [k for k in old if str(old[k]) != str(new[k])]
        tag = f"{ticker:6s} {expiry} ${strike:<8.2f}"

        if not changed:
            print(f"  {'OK':10s} {tag} — no changes")
            matched += 1
            continue

        diff_str = "  |  ".join(f"{k}: {old[k]} -> {new[k]}" for k in changed)
        status = "[DRY RUN]" if DRY_RUN else "[APPLIED]"
        print(f"  {status} {tag} | {diff_str}")

        if not DRY_RUN:
            params = [bigquery.ScalarQueryParameter("id", "STRING", pos_id)]
            set_parts = []
            for i, k in enumerate(changed):
                pname = f"p{i}"
                field = {
                    "qty": "quantity", "price": "entry_price",
                    "trimmed": "quantity_trimmed", "proceeds": "proceeds_from_trims",
                    "mode": "mode",
                }[k]
                bqtype = {
                    "qty": "INT64", "price": "FLOAT64",
                    "trimmed": "INT64", "proceeds": "FLOAT64",
                    "mode": "STRING",
                }[k]
                params.append(bigquery.ScalarQueryParameter(pname, bqtype, new[k]))
                set_parts.append(f"{field} = @{pname}")

            q = f"UPDATE {table} SET {', '.join(set_parts)} WHERE id = @id"
            client.query(q, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

        matched += 1

    print(f"\nMatched: {matched} / {len(IBKR_DATA)}")
    if not_found:
        print(f"\nNOT FOUND in BigQuery ({len(not_found)}):")
        for nf in not_found:
            print(f"  - {nf}")
    if DRY_RUN:
        print("\nRun with --apply to write changes to BigQuery.")
    else:
        print("\nDone.")


if __name__ == "__main__":
    main()
