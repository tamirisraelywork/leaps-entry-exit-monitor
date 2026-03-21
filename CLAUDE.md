# CLAUDE.md — LEAPS Exit Agent

## Architecture

Two-process system. They communicate ONLY through BigQuery — never import one from the other.

1. **Streamlit UI** — `streamlit run app.py`  
   Reads BigQuery for positions/alerts. No scheduler. Deployed to Streamlit Cloud.

2. **Monitor Engine** — `bash scripts/start_monitor.sh` (or `--daemon`)  
   Standalone Python process. Reads positions, writes alerts/scores. Runs locally on macOS.  
   Auto-starts via launchd: `~/Library/LaunchAgents/com.leaps.monitor.plist`  
   Check config: `bash scripts/start_monitor.sh --check`  
   Logs: `tail -f monitor.log`

## File Map

| File | Purpose | Caution |
|------|---------|--------|
| `app.py` | 6-page Streamlit UI (~3700 lines) | Large file — read section before editing |
| `exit_engine.py` | 5-pillar exit + entry signal logic | Critical for correctness |
| `monitor_engine/monitor_service.py` | APScheduler standalone service | 7 scheduled jobs |
| `monitor_engine/main.py` | Entry point for monitor process | Only runs signal handlers + scheduler |
| `db.py` | BigQuery read/write | Always use DML INSERT, not streaming |
| `options_data.py` | Option snapshots (marketdata.app + yfinance fallback) | Thread-safe module cache |
| `email_alerts.py` | Gmail SMTP delivery | Needs GMAIL_SENDER + GMAIL_APP_PASSWORD |
| `shared/config.py` | Secrets abstraction (`cfg(KEY)`) | Used everywhere for env/st.secrets |
| `score_thesis.py` | 7-source thesis scoring pipeline | Slow (~30s), always run in background |
| `technical.py` | yfinance price/RSI | Has semaphore `_yf_sem` — do not bypass |
| `iv_rank.py` | IV rank via yfinance | Has built-in retry — do not add more sleep |
| `exit_engine.py` | Pillar 5 earnings/news alerts | Reads `earnings_state`, `news_sentiment_score` from mkt dict |

## Critical Invariants — Never Break

1. `monitor_engine/` has **zero Streamlit imports**. Keep it that way.
2. All secrets use `shared/config.cfg(KEY)` — never `st.secrets[KEY]` outside Streamlit pages.
3. BigQuery writes use **DML INSERT** (`db.save_position`). Never use `insert_rows_json` (blocks UPDATE for 90 min).
4. Dashboard: all API calls happen in `_prefetch_active_data()` via `ThreadPoolExecutor` **before** the render loop. Never add API calls inside the `for pos in active:` loop.
5. Missing scores are pre-fetched with `ThreadPoolExecutor` **before** the render loop (after `get_all_leaps_scores_with_age()`).
6. `options_data._yf_throttle()` serializes yfinance calls — always use cached chain helpers.
7. `technical.py` has `_yf_sem` semaphore — always wrap yfinance calls with it.

## Performance Patterns

- **Dashboard market data**: `_prefetch_active_data()` — parallel fetch via `ThreadPoolExecutor(max_workers=8)`
- **Thesis scores**: `db.get_all_leaps_scores_with_age(tickers)` — single BigQuery query for all
- **Positions**: `_get_positions_cached()` — 60s session-state TTL prevents BQ spam on reruns
- **Monitor email**: `send_morning_summary()` — parallel active + watchlist fetch via `ThreadPoolExecutor`
- **Cache TTLs**: 5 min for live prices/IV (`@st.cache_data(ttl=300)`), 1 hr for earnings/LLM, `∞` for stateful containers

## What Never to Do

- Do NOT add `sleep()` in the Streamlit render path — blocks the UI
- Do NOT start APScheduler inside `app.py` — it lives in `monitor_engine/main.py` only
- Do NOT use `@st.cache_resource` for the scheduler — use module-level singleton `_scheduler`
- Do NOT use `insert_rows_json` in `db.py` — use DML `INSERT`/`UPDATE`/`DELETE`
- Do NOT import `monitor_engine` from `app.py` — breaks process isolation
- Do NOT call `get_leaps_monitor_score()` in a loop — use `get_all_leaps_scores_with_age()`

## Scheduled Jobs (monitor_engine/monitor_service.py)

| Job | When | Purpose |
|-----|------|---------|
| `run_active_checks` | Every 30 min, Mon-Fri 9:30–4 PM ET | Exit/trim/roll signal evaluation |
| `run_watchlist_checks` | Every hour, Mon-Fri 9–4 PM ET | Entry signal evaluation |
| `send_morning_summary` | 5:00 PM ET Mon-Fri | Daily portfolio email |
| `run_thesis_refresh` | 6:00 AM ET Mon-Fri | Rescore stale theses |
| `run_earnings_refresh` | 6:10 AM ET Mon-Fri | Update earnings dates from yfinance |
| `run_post_earnings_analysis` | 10:00 AM ET Mon-Fri | Earnings call tone analysis (Gemini + EDGAR) |
| `run_news_checks` | 9 AM, 1 PM, 5 PM, 9 PM ET Mon-Fri | Alpha Vantage news sentiment |

## Secrets

**For Streamlit UI** — `.streamlit/secrets.toml`  
**For Monitor Engine** — same file, loaded by `scripts/start_monitor.sh` into env vars

Required: `GMAIL_SENDER`, `GMAIL_APP_PASSWORD`, `ALERT_RECIPIENT_EMAIL`, `SERVICE_ACCOUNT_JSON`, `MARKETDATA_TOKEN`  
Optional: `ALPHA_VANTAGE_API_KEY_1`, `GEMINI_API_KEY`, `FINNHUB_API_KEY`, `ALETHEIA_API_KEY`  
BigQuery: `DATASET_ID` (LEAPS Monitor master_table dataset), `LEAPS_MONITOR_DATASET` (same)

## How to Test Changes

```bash
# Syntax check
python3 -c "import ast; ast.parse(open('app.py').read()); print('OK')"

# Exit engine
python3 -c "from exit_engine import evaluate, evaluate_entry; print('exit_engine OK')"

# Monitor engine start check  
bash scripts/start_monitor.sh --check

# Test email delivery
python3 -c "
import sys; sys.path.insert(0, '.')
import subprocess, os
result = subprocess.run(['bash', 'scripts/start_monitor.sh', '--check'], capture_output=True, text=True)
print(result.stdout)
"

# Full monitor test (runs indefinitely — Ctrl+C to stop)
bash scripts/start_monitor.sh
```

## BigQuery Tables (leaps_exit_agent dataset)

| Table | Purpose |
|-------|---------|
| `positions` | All positions (ACTIVE / WATCHLIST / CLOSED / ROLLED) |
| `alerts` | All alerts ever sent |
| `earnings_calls` | Earnings call tone analysis results |
