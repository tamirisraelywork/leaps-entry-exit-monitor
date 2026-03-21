"""
BigQuery operations for the LEAPS Exit Agent.
Manages two tables: positions and alerts.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, date
from google.cloud import bigquery
from google.oauth2 import service_account

from shared.config import cfg, cfg_dict

DATASET = "leaps_exit_agent"

# Module-level singleton — works in both Streamlit and standalone service contexts.
_client: bigquery.Client | None = None


def get_client() -> bigquery.Client:
    global _client
    if _client is None:
        sa_info = cfg_dict("SERVICE_ACCOUNT_JSON")
        if not sa_info:
            raw = cfg("SERVICE_ACCOUNT_JSON")
            sa_info = json.loads(raw) if raw else {}
        # Fix private key newlines
        if "private_key" in sa_info:
            sa_info["private_key"] = sa_info["private_key"].replace("\\n", "\n")
        credentials = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        _client = bigquery.Client(credentials=credentials, project=sa_info["project_id"])
    return _client


def _project() -> str:
    return get_client().project


def ensure_tables():
    """Create the dataset and both tables if they don't already exist."""
    client = get_client()
    dataset_ref = bigquery.DatasetReference(client.project, DATASET)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(bigquery.Dataset(dataset_ref))

    positions_schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("contract", "STRING"),
        bigquery.SchemaField("option_type", "STRING"),
        bigquery.SchemaField("strike", "FLOAT64"),
        bigquery.SchemaField("expiration_date", "DATE"),
        bigquery.SchemaField("entry_date", "DATE"),
        bigquery.SchemaField("entry_price", "FLOAT64"),
        bigquery.SchemaField("quantity", "INT64"),
        bigquery.SchemaField("entry_delta", "FLOAT64"),
        bigquery.SchemaField("entry_iv_rank", "FLOAT64"),
        bigquery.SchemaField("entry_thesis_score", "INT64"),
        bigquery.SchemaField("position_type", "STRING"),   # MOONSHOT / CORE / TACTICAL
        bigquery.SchemaField("target_return", "STRING"),   # "10x" / "3-5x" / "tactical"
        bigquery.SchemaField("mode", "STRING"),            # WATCHLIST / ACTIVE / CLOSED / ROLLED
        bigquery.SchemaField("notes", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ]

    alerts_schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("position_id", "STRING"),
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("alert_type", "STRING"),
        bigquery.SchemaField("severity", "STRING"),
        bigquery.SchemaField("subject", "STRING"),
        bigquery.SchemaField("body", "STRING"),
        bigquery.SchemaField("current_delta", "FLOAT64"),
        bigquery.SchemaField("current_dte", "INT64"),
        bigquery.SchemaField("current_pnl_pct", "FLOAT64"),
        bigquery.SchemaField("current_iv_rank", "FLOAT64"),
        bigquery.SchemaField("current_thesis_score", "INT64"),
        bigquery.SchemaField("triggered_at", "TIMESTAMP"),
        bigquery.SchemaField("email_sent", "BOOL"),
    ]

    earnings_calls_schema = [
        bigquery.SchemaField("id",               "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ticker",            "STRING"),
        bigquery.SchemaField("quarter",           "STRING"),
        bigquery.SchemaField("tone_score",        "FLOAT64"),
        bigquery.SchemaField("tone_label",        "STRING"),
        bigquery.SchemaField("forward_guidance",  "STRING"),
        bigquery.SchemaField("analyst_tone",      "STRING"),
        bigquery.SchemaField("thesis_impact",     "STRING"),
        bigquery.SchemaField("summary",           "STRING"),
        bigquery.SchemaField("key_bullish",       "STRING"),   # JSON array
        bigquery.SchemaField("key_bearish",       "STRING"),   # JSON array
        bigquery.SchemaField("analyzed_at",       "DATE"),
    ]

    for tbl_id, schema in [
        ("positions",      positions_schema),
        ("alerts",         alerts_schema),
        ("earnings_calls", earnings_calls_schema),
    ]:
        tbl_ref = dataset_ref.table(tbl_id)
        try:
            client.get_table(tbl_ref)
        except Exception:
            client.create_table(bigquery.Table(tbl_ref, schema=schema))

    # ── Column migrations (add new columns to existing tables) ──────────────
    # BigQuery ALTER TABLE ADD COLUMN IF NOT EXISTS is idempotent — safe to
    # run on every startup.
    _migrate_cols = [
        ("quantity_trimmed",       "INT64"),
        ("proceeds_from_trims",    "FLOAT64"),
        ("earnings_date",          "DATE"),       # next expected earnings date
        ("last_posture",           "STRING"),     # last known recommendation: HOLD/WATCH/ROLL/EXIT
        ("last_posture_changed_at","TIMESTAMP"),  # when posture last changed (for daily diff)
    ]
    pos_tbl_ref = f"`{client.project}.{DATASET}.positions`"
    for col_name, col_type in _migrate_cols:
        try:
            client.query(
                f"ALTER TABLE {pos_tbl_ref} "
                f"ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
            ).result()
        except Exception:
            pass   # column already exists or DDL not supported — ignore


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------

def get_positions(mode: str = None) -> list[dict]:
    client = get_client()
    q = f"SELECT * FROM `{client.project}.{DATASET}.positions`"
    if mode:
        q += f" WHERE mode = @mode"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("mode", "STRING", mode)]
        )
    else:
        job_config = bigquery.QueryJobConfig()
    q += " ORDER BY created_at DESC"
    rows = client.query(q, job_config=job_config).result()
    return [dict(r) for r in rows]


def get_position_by_id(position_id: str) -> dict | None:
    client = get_client()
    q = f"SELECT * FROM `{client.project}.{DATASET}.positions` WHERE id = @id LIMIT 1"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", position_id)]
    )
    rows = list(client.query(q, job_config=job_config).result())
    return dict(rows[0]) if rows else None


def save_position(pos: dict) -> str:
    """Insert a new position using DML INSERT (not streaming).

    IMPORTANT: insert_rows_json uses the streaming buffer which blocks UPDATE/DELETE
    for up to 90 minutes. DML INSERT commits immediately so the row can be edited
    right away via update_position().
    """
    client = get_client()
    pos = dict(pos)
    pos["id"] = str(uuid.uuid4())
    pos["created_at"] = datetime.utcnow()   # datetime object for TIMESTAMP param

    # Convert date objects to ISO strings for DATE parameters
    for k in ("expiration_date", "entry_date"):
        if isinstance(pos.get(k), date):
            pos[k] = pos[k].isoformat()

    # BigQuery parameter type for each column
    _TYPE_MAP = {
        "id": "STRING", "ticker": "STRING", "contract": "STRING",
        "option_type": "STRING", "position_type": "STRING",
        "target_return": "STRING", "mode": "STRING", "notes": "STRING",
        "strike": "FLOAT64", "entry_price": "FLOAT64",
        "entry_delta": "FLOAT64", "entry_iv_rank": "FLOAT64",
        "expiration_date": "DATE", "entry_date": "DATE",
        "created_at": "TIMESTAMP",
        "quantity": "INT64", "entry_thesis_score": "INT64",
    }

    # Only include columns that have a value and are in the schema
    col_names = [c for c in pos if c in _TYPE_MAP and pos[c] is not None]
    params = [
        bigquery.ScalarQueryParameter(c, _TYPE_MAP[c], pos[c])
        for c in col_names
    ]

    table_ref = f"`{client.project}.{DATASET}.positions`"
    cols_sql  = ", ".join(col_names)
    vals_sql  = ", ".join(f"@{c}" for c in col_names)
    q = f"INSERT INTO {table_ref} ({cols_sql}) VALUES ({vals_sql})"

    client.query(q, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    return pos["id"]


def update_position(position_id: str, fields: dict):
    """
    Update editable fields on an existing position.
    Only updates the keys provided in `fields`.
    Allowed fields: entry_price, quantity, entry_date, expiration_date,
                    strike, mode, notes, contract.
    """
    client = get_client()
    allowed = {"entry_price", "quantity", "entry_date", "expiration_date",
               "strike", "mode", "notes", "contract",
               "quantity_trimmed", "proceeds_from_trims", "entry_thesis_score",
               "entry_delta", "entry_iv_rank", "position_type"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return

    _INT64_COLS   = {"quantity", "quantity_trimmed", "entry_thesis_score"}
    _FLOAT64_COLS = {"entry_price", "strike", "proceeds_from_trims", "entry_delta", "entry_iv_rank"}
    _DATE_COLS    = {"entry_date", "expiration_date"}

    set_clauses = []
    params = [bigquery.ScalarQueryParameter("id", "STRING", position_id)]
    for i, (col, val) in enumerate(updates.items()):
        param_name = f"p{i}"
        if col in _DATE_COLS:
            if isinstance(val, date):
                val = val.isoformat()
            bq_type = "DATE"
        elif col in _FLOAT64_COLS:
            bq_type = "FLOAT64"
        elif col in _INT64_COLS:
            bq_type = "INT64"
        else:
            bq_type = "STRING"
        set_clauses.append(f"{col} = @{param_name}")
        params.append(bigquery.ScalarQueryParameter(param_name, bq_type, val))

    q = f"""
        UPDATE `{client.project}.{DATASET}.positions`
        SET {', '.join(set_clauses)}
        WHERE id = @id
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    client.query(q, job_config=job_config).result()


def update_position_mode(position_id: str, mode: str):
    client = get_client()
    q = f"""
        UPDATE `{client.project}.{DATASET}.positions`
        SET mode = @mode
        WHERE id = @id
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("mode", "STRING", mode),
        bigquery.ScalarQueryParameter("id", "STRING", position_id),
    ])
    client.query(q, job_config=job_config).result()


def update_position_posture(position_id: str, posture: str):
    """
    Store the current recommendation posture on the position row.
    Only writes if the posture changed so last_posture_changed_at is meaningful.
    """
    client = get_client()
    q = f"""
        UPDATE `{client.project}.{DATASET}.positions`
        SET last_posture = @posture,
            last_posture_changed_at = CURRENT_TIMESTAMP()
        WHERE id = @id
          AND (last_posture IS NULL OR last_posture != @posture)
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("posture", "STRING", posture),
        bigquery.ScalarQueryParameter("id",      "STRING", position_id),
    ])
    client.query(q, job_config=job_config).result()


def get_recent_posture_changes(hours: int = 26) -> list[dict]:
    """
    Return positions whose posture changed within the last `hours` hours.
    Used by the morning summary to highlight what changed overnight.
    """
    client = get_client()
    q = f"""
        SELECT id, ticker, contract, last_posture, last_posture_changed_at
        FROM `{client.project}.{DATASET}.positions`
        WHERE mode = 'ACTIVE'
          AND last_posture_changed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
        ORDER BY last_posture_changed_at DESC
    """
    rows = list(client.query(q).result())
    return [dict(r) for r in rows]


def delete_position(position_id: str):
    client = get_client()
    q = f"DELETE FROM `{client.project}.{DATASET}.positions` WHERE id = @id"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", position_id)]
    )
    client.query(q, job_config=job_config).result()


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------

def save_alert(alert: dict):
    client = get_client()
    alert = dict(alert)
    alert["id"] = str(uuid.uuid4())
    alert["triggered_at"] = datetime.utcnow().isoformat()
    table_id = f"{client.project}.{DATASET}.alerts"
    errors = client.insert_rows_json(table_id, [alert])
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")


def already_sent_today(alert_type: str, position_id: str) -> bool:
    """Return True if this alert type was already emailed today for this position."""
    client = get_client()
    q = f"""
        SELECT COUNT(*) AS cnt
        FROM `{client.project}.{DATASET}.alerts`
        WHERE position_id = @pos_id
          AND alert_type  = @atype
          AND DATE(triggered_at) = CURRENT_DATE()
          AND email_sent = TRUE
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("pos_id", "STRING", position_id),
        bigquery.ScalarQueryParameter("atype", "STRING", alert_type),
    ])
    rows = list(client.query(q, job_config=job_config).result())
    return rows[0]["cnt"] > 0 if rows else False


def get_alerts(ticker: str = None, limit: int = 200) -> list[dict]:
    client = get_client()
    q = f"SELECT * FROM `{client.project}.{DATASET}.alerts`"
    params = []
    if ticker:
        q += " WHERE UPPER(ticker) = UPPER(@ticker)"
        params.append(bigquery.ScalarQueryParameter("ticker", "STRING", ticker))
    q += f" ORDER BY triggered_at DESC LIMIT {limit}"
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return [dict(r) for r in client.query(q, job_config=job_config).result()]


# ---------------------------------------------------------------------------
# LEAPS Monitor integration (read-only)
# ---------------------------------------------------------------------------

def get_leaps_monitor_score(ticker: str) -> int | None:
    """Pull latest thesis score from the existing LEAPS Monitor BigQuery table.

    Tries DATASET_ID first (the secret name used by the LEAPS Monitor itself),
    then falls back to LEAPS_MONITOR_DATASET. Handles dataset IDs that already
    include a project prefix (e.g. "myproject.leaps_monitor").
    """
    try:
        client = get_client()
        # DATASET_ID matches the LEAPS Monitor's own secret naming convention
        raw_dataset = (
            cfg("DATASET_ID")
            or cfg("LEAPS_MONITOR_DATASET", "leaps_monitor")
        )
        table = cfg("LEAPS_MONITOR_TABLE", "master_table")

        # If DATASET_ID already includes a project prefix (e.g. "proj.dataset"),
        # use it as-is; otherwise prepend the current project.
        if "." in str(raw_dataset):
            full_table = f"`{raw_dataset}.{table}`"
        else:
            full_table = f"`{client.project}.{raw_dataset}.{table}`"

        q = f"""
            SELECT Score
            FROM {full_table}
            WHERE UPPER(Ticker) = UPPER(@ticker)
            ORDER BY date DESC
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
        )
        rows = list(client.query(q, job_config=job_config).result())
        if rows:
            return int(float(rows[0]["Score"]))
    except Exception:
        pass
    return None


def get_all_leaps_scores_with_age(tickers: list[str]) -> dict[str, tuple[int | None, int | None]]:
    """
    Batch fetch the latest thesis score + age for multiple tickers in ONE BigQuery query.
    Returns {TICKER_UPPER: (score, days_old)}.
    Replaces N individual get_leaps_monitor_score_with_age() calls on the dashboard.
    """
    if not tickers:
        return {}
    try:
        client = get_client()
        raw_dataset = cfg("DATASET_ID") or cfg("LEAPS_MONITOR_DATASET", "leaps_monitor")
        table = cfg("LEAPS_MONITOR_TABLE", "master_table")
        if "." in str(raw_dataset):
            full_table = f"`{raw_dataset}.{table}`"
        else:
            full_table = f"`{client.project}.{raw_dataset}.{table}`"

        q = f"""
            SELECT UPPER(Ticker) AS ticker, Score, date
            FROM {full_table}
            WHERE UPPER(Ticker) IN UNNEST(@tickers)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(Ticker) ORDER BY date DESC) = 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("tickers", "STRING", [t.upper() for t in tickers])
            ]
        )
        rows = list(client.query(q, job_config=job_config).result())
        result = {}
        for row in rows:
            score = int(float(row["Score"]))
            try:
                days_old = (date.today() - date.fromisoformat(str(row["date"])[:10])).days
            except Exception:
                days_old = None
            result[str(row["ticker"]).upper()] = (score, days_old)
        return result
    except Exception:
        return {}


def get_leaps_monitor_score_with_age(ticker: str) -> tuple[int | None, int | None]:
    """Return (score, days_since_last_scored) or (None, None) if not found."""
    try:
        client = get_client()
        raw_dataset = (
            cfg("DATASET_ID")
            or cfg("LEAPS_MONITOR_DATASET", "leaps_monitor")
        )
        table = cfg("LEAPS_MONITOR_TABLE", "master_table")
        if "." in str(raw_dataset):
            full_table = f"`{raw_dataset}.{table}`"
        else:
            full_table = f"`{client.project}.{raw_dataset}.{table}`"
        q = f"""
            SELECT Score, date
            FROM {full_table}
            WHERE UPPER(Ticker) = UPPER(@ticker)
            ORDER BY date DESC
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
        )
        rows = list(client.query(q, job_config=job_config).result())
        if rows:
            score = int(float(rows[0]["Score"]))
            d = rows[0]["date"]
            try:
                last_date = date.fromisoformat(str(d)[:10])
                days_old = (date.today() - last_date).days
            except Exception:
                days_old = None
            return score, days_old
    except Exception:
        pass
    return None, None


# ---------------------------------------------------------------------------
# Earnings calls (new)
# ---------------------------------------------------------------------------

def save_earnings_call(ticker: str, data: dict):
    """Persist earnings call analysis row to the earnings_calls table."""
    import json as _json
    client = get_client()
    row = {
        "id":              str(uuid.uuid4()),
        "ticker":          ticker.upper(),
        "quarter":         data.get("quarter", ""),
        "tone_score":      float(data.get("tone_score", 0)),
        "tone_label":      data.get("overall_tone", "NEUTRAL"),
        "forward_guidance": data.get("forward_guidance", "NOT_PROVIDED"),
        "analyst_tone":    data.get("analyst_tone", ""),
        "thesis_impact":   data.get("thesis_impact", "UNCHANGED"),
        "summary":         data.get("summary", ""),
        "key_bullish":     _json.dumps(data.get("key_bullish_signals", [])[:5]),
        "key_bearish":     _json.dumps(data.get("key_bearish_signals", [])[:5]),
        "analyzed_at":     datetime.utcnow().date().isoformat(),
    }
    client.insert_rows_json(
        f"{client.project}.{DATASET}.earnings_calls", [row]
    )


def get_latest_earnings_call(ticker: str) -> dict | None:
    """Return the most recent earnings call row for a ticker, or None."""
    try:
        client = get_client()
        q = f"""
            SELECT * FROM `{client.project}.{DATASET}.earnings_calls`
            WHERE UPPER(ticker) = UPPER(@ticker)
            ORDER BY analyzed_at DESC
            LIMIT 1
        """
        rows = list(client.query(
            q,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
            ),
        ).result())
        return dict(rows[0]) if rows else None
    except Exception:
        return None


def get_earnings_calls(ticker: str, limit: int = 4) -> list[dict]:
    """Return recent earnings call rows for tone delta analysis."""
    try:
        client = get_client()
        q = f"""
            SELECT * FROM `{client.project}.{DATASET}.earnings_calls`
            WHERE UPPER(ticker) = UPPER(@ticker)
            ORDER BY analyzed_at DESC
            LIMIT {limit}
        """
        rows = list(client.query(
            q,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
            ),
        ).result())
        return [dict(r) for r in rows]
    except Exception:
        return []
