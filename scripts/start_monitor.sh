#!/bin/bash
# =============================================================================
# LEAPS Monitor Engine — Start Script
# =============================================================================
# Loads secrets from .streamlit/secrets.toml and launches the monitor engine.
#
# Usage:
#   ./scripts/start_monitor.sh            # foreground (Ctrl+C to stop)
#   ./scripts/start_monitor.sh --daemon   # background, logs to monitor.log
#   ./scripts/start_monitor.sh --check    # verify secrets without starting
#   ./scripts/start_monitor.sh --stop     # stop a running daemon
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SECRETS_FILE="$PROJECT_DIR/.streamlit/secrets.toml"
LOG_FILE="$PROJECT_DIR/monitor.log"
PID_FILE="$PROJECT_DIR/monitor.pid"

# ── Python detection ──────────────────────────────────────────────────────────
find_python() {
    for candidate in \
        "/Library/Developer/CommandLineTools/usr/bin/python3" \
        "/usr/local/bin/python3" \
        "/usr/bin/python3" \
        "$(command -v python3 2>/dev/null)"; do
        if [ -x "$candidate" ] 2>/dev/null; then
            if "$candidate" -c "import apscheduler, google.cloud.bigquery" 2>/dev/null; then
                echo "$candidate"
                return 0
            fi
        fi
    done
    return 1
}

PYTHON="$(find_python 2>/dev/null)"
if [ -z "$PYTHON" ]; then
    echo "❌  No Python found with required packages."
    echo ""
    echo "Install monitor dependencies:"
    echo "  python3 -m pip install --user apscheduler google-cloud-bigquery google-auth db-dtypes"
    exit 1
fi

# ── --stop ────────────────────────────────────────────────────────────────────
if [ "${1:-}" = "--stop" ]; then
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID" && rm -f "$PID_FILE"
            echo "✅  Monitor stopped (PID $PID)."
        else
            echo "PID $PID not running. Cleaning up."
            rm -f "$PID_FILE"
        fi
    else
        pkill -f "monitor_engine.main" 2>/dev/null && echo "✅  Monitor stopped." || echo "No running monitor found."
    fi
    exit 0
fi

# ── Load secrets from .streamlit/secrets.toml ────────────────────────────────
if [ ! -f "$SECRETS_FILE" ]; then
    echo "❌  Secrets file not found: $SECRETS_FILE"
    exit 1
fi

# Use Python (no external toml package needed) to export simple secrets as env vars.
# SERVICE_ACCOUNT_JSON is intentionally skipped here — cfg_dict() reads it directly
# from the TOML file to avoid JSON corruption from shell escaping.
_load_secrets() {
    "$PYTHON" - "$SECRETS_FILE" <<'EOF'
import re, sys

secrets_file = sys.argv[1]
with open(secrets_file, "r") as f:
    content = f.read()

# Keys whose values are JSON objects — skip env export to avoid shell escaping
# corrupting the JSON (especially \n sequences in the private_key field).
# cfg_dict() reads these directly from the TOML file instead.
SKIP_JSON_KEYS = {"SERVICE_ACCOUNT_JSON"}

exported = set()

# KEY = "value"
for m in re.finditer(r'^([A-Za-z_]\w*)\s*=\s*"([^"]*)"', content, re.MULTILINE):
    k, v = m.group(1), m.group(2)
    exported.add(k)
    if k in SKIP_JSON_KEYS:
        continue
    # Do NOT replace \n — preserve escape sequences exactly as stored
    safe_v = v.replace("'", "'\\''")
    print(f"export {k}='{safe_v}'")

# KEY = 'value'
for m in re.finditer(r"^([A-Za-z_]\w*)\s*=\s*'([^']*)'", content, re.MULTILINE):
    k, v = m.group(1), m.group(2)
    if k not in exported:
        exported.add(k)
        if k in SKIP_JSON_KEYS:
            continue
        safe_v = v.replace("'", "'\\''")
        print(f"export {k}='{safe_v}'")

# KEY = """..."""
for m in re.finditer(r'^([A-Za-z_]\w*)\s*=\s*"""(.*?)"""', content, re.MULTILINE | re.DOTALL):
    k, v = m.group(1), m.group(2).strip()
    if k not in exported:
        exported.add(k)
        if k in SKIP_JSON_KEYS:
            continue
        safe_v = v.replace("'", "'\\''")
        print(f"export {k}='{safe_v}'")
EOF
}

eval "$(_load_secrets)"

# ── --check ───────────────────────────────────────────────────────────────────
if [ "${1:-}" = "--check" ]; then
    echo ""
    echo "=== LEAPS Monitor Engine — Configuration Check ==="
    echo "Python  : $PYTHON"
    echo "Project : $PROJECT_DIR"
    echo "Logs    : $LOG_FILE"
    echo ""
    "$PYTHON" - <<'EOF'
import os
checks = [
    ("GMAIL_SENDER",           os.environ.get("GMAIL_SENDER") or os.environ.get("ALERT_EMAIL_FROM")),
    ("GMAIL_APP_PASSWORD",     os.environ.get("GMAIL_APP_PASSWORD") or os.environ.get("ALERT_EMAIL_PASS")),
    ("ALERT_RECIPIENT_EMAIL",  os.environ.get("ALERT_RECIPIENT_EMAIL")),
    ("SERVICE_ACCOUNT_JSON",   os.environ.get("SERVICE_ACCOUNT_JSON", "")[:12] or None),
    ("MARKETDATA_TOKEN",       os.environ.get("MARKETDATA_TOKEN")),
    ("ALPHA_VANTAGE_API_KEY_1",os.environ.get("ALPHA_VANTAGE_API_KEY_1")),
    ("GEMINI_API_KEY",         os.environ.get("GEMINI_API_KEY")),
]
any_missing = False
for k, v in checks:
    status = "✅ set     " if v else "❌ MISSING "
    if not v:
        any_missing = True
    print(f"  {status} {k}")

print()
email_ok = (
    (os.environ.get("GMAIL_SENDER") or os.environ.get("ALERT_EMAIL_FROM")) and
    (os.environ.get("GMAIL_APP_PASSWORD") or os.environ.get("ALERT_EMAIL_PASS"))
)
bq_ok = bool(os.environ.get("SERVICE_ACCOUNT_JSON"))

if not email_ok:
    print("⚠️   Email not configured — daily summary emails will NOT be sent.")
if not bq_ok:
    print("❌  SERVICE_ACCOUNT_JSON missing — BigQuery will fail on startup.")
if not any_missing and email_ok and bq_ok:
    print("✅  All required secrets configured. Ready to start.")
EOF
    exit 0
fi

# ── Launch ────────────────────────────────────────────────────────────────────
export PYTHONPATH="$PROJECT_DIR${PYTHONPATH:+:$PYTHONPATH}"
cd "$PROJECT_DIR"

if [ "${1:-}" = "--daemon" ]; then
    # Stop existing daemon if running
    if [ -f "$PID_FILE" ]; then
        OLD_PID=$(cat "$PID_FILE")
        kill "$OLD_PID" 2>/dev/null || true
        rm -f "$PID_FILE"
        sleep 1
    fi

    nohup "$PYTHON" -m monitor_engine.main >> "$LOG_FILE" 2>&1 &
    MPID=$!
    echo "$MPID" > "$PID_FILE"
    echo "✅  LEAPS Monitor Engine started (PID $MPID)."
    echo "   Logs  :  tail -f $LOG_FILE"
    echo "   Stop  :  $0 --stop"
    echo "   Check :  $0 --check"
else
    echo "=== LEAPS Monitor Engine (foreground) ==="
    echo "Python : $PYTHON"
    echo "Ctrl+C to stop."
    echo ""
    exec "$PYTHON" -m monitor_engine.main
fi
