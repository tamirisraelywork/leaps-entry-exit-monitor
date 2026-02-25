# LEAPS Position Manager

The **execution layer** for your LEAPS strategy. Once you've identified stocks in the LEAPS Monitor, this app tracks your positions and sends email alerts when to enter, roll, scale out, or exit.

## What It Does

**Two input modes:**
- **New Entry** — Enter a ticker; get LEAPS contract recommendations optimised for your 10x (Moonshot) and 3-5x (Core) strategy
- **Existing Position** — Add an option you've already bought; app monitors it and alerts you

**5-pillar exit engine (runs every 30 min during market hours):**
1. **Fundamental** — Is the thesis still intact? (thesis score from LEAPS Monitor)
2. **Greeks** — Is the leverage still working? (delta threshold per position type)
3. **Time** — Is theta becoming a threat? (context-aware roll vs exit decision)
4. **Profit** — Are you capturing the asymmetric return? (scaling ladders per role)
5. **Entry** — Are conditions optimal to buy? (watchlist: RSI + IV Rank + 52w range)

**Alert calibration by role:**

| Role | Target | Stop Loss | First Profit Signal | Roll Delta |
|---|---|---|---|---|
| MOONSHOT | 10x | -70% | +200% (3x) | Δ > 0.50 |
| CORE | 3-5x | -50% | +50% | Δ > 0.90 |
| TACTICAL | 1-2x | -40% | +50% | Δ > 0.90 |

**Portfolio strategy support:** 2 Moonshots + 4 Core + 4 Tactical = 10 position portfolio

## Setup

### 1. Secrets
Copy `.streamlit/secrets.toml` and fill in your values:
- `SERVICE_ACCOUNT_JSON` — same BigQuery service account as LEAPS Monitor
- `POLYGON_API_KEY_1` / `POLYGON_API_KEY_2` — same Polygon keys
- `GMAIL_SENDER` + `GMAIL_APP_PASSWORD` — Gmail with App Password enabled
- `LEAPS_MONITOR_DATASET` — your existing LEAPS Monitor BigQuery dataset name

**Gmail App Password:** Google Account → Security → 2-Step Verification → App Passwords → create for "Mail"

### 2. Deploy to Streamlit Community Cloud
1. Push this repo to GitHub
2. Go to share.streamlit.io → New App → select this repo
3. Add all secrets in the Streamlit Cloud dashboard
4. Deploy — BigQuery tables are created automatically on first run

### 3. First Use
1. Settings page → "Send Test Email" → verify email arrives
2. Add Position → New Entry → enter a ticker → review recommendations
3. Add Position → Existing Position → enter your actual contract details
4. Dashboard → "Run Check Now" → see live position health

## Files

```
app.py              Streamlit UI (all 4 pages)
monitor.py          APScheduler background loop
exit_engine.py      5-pillar alert logic
recommender.py      Option recommendation engine
options_data.py     Polygon.io Greeks fetcher
technical.py        RSI + 52-week range (yfinance)
db.py               BigQuery operations
email_alerts.py     Gmail SMTP alert delivery
iv_rank.py          IV Rank (Unusual Whales → Gemini fallback)
```
