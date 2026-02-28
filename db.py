"""
BigQuery operations for the LEAPS Exit Agent.
Manages two tables: positions and alerts.
"""

import json
import uuid
import streamlit as st
from datetime import datetime, date
from google.cloud import bigquery
from google.oauth2 import service_account

DATASET = "leaps_exit_agent"


@st.cache_resource
def get_client() -> bigquery.Client:
    raw = st.secrets["SERVICE_ACCOUNT_JSON"]
    sa_info = json.loads(raw) if isinstance(raw, str) else dict(raw)
    credentials = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(credentials=credentials, project=sa_info["project_id"])


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

    for tbl_id, schema in [("positions", positions_schema), ("alerts", alerts_schema)]:
        tbl_ref = dataset_ref.table(tbl_id)
        try:
            client.get_table(tbl_ref)
        except Exception:
            client.create_table(bigquery.Table(tbl_ref, schema=schema))


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
               "strike", "mode", "notes", "contract"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return

    set_clauses = []
    params = [bigquery.ScalarQueryParameter("id", "STRING", position_id)]
    for i, (col, val) in enumerate(updates.items()):
        param_name = f"p{i}"
        if col in ("entry_date", "expiration_date"):
            if isinstance(val, date):
                val = val.isoformat()
            bq_type = "DATE"
        elif col in ("entry_price", "strike"):
            bq_type = "FLOAT64"
        elif col == "quantity":
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
    """Pull latest thesis score from the existing LEAPS Monitor BigQuery table."""
    try:
        client = get_client()
        dataset = st.secrets.get("LEAPS_MONITOR_DATASET", "leaps_monitor")
        table = st.secrets.get("LEAPS_MONITOR_TABLE", "master_table")
        q = f"""
            SELECT Score
            FROM `{client.project}.{dataset}.{table}`
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
