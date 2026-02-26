"""
LEAPS Position Manager — Streamlit App

Pages:
  1. Dashboard      — live position cards with posture badges
  2. Add Position   — new entry (recommend options) or existing contract
  3. Alert History  — full log of alerts with filters
  4. Settings       — email config, threshold overrides, test email
"""

import re
import time
import streamlit as st
from datetime import date, datetime, timedelta

import db
import options_data
import email_alerts
from recommender import recommend_options, format_recommendation
from technical import get_price_and_range, get_weekly_rsi
from exit_engine import evaluate, evaluate_entry
from monitor import start_scheduler

# ---------------------------------------------------------------------------
# Page config & startup
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="LEAPS Position Manager",
    page_icon="📈",
    layout="wide",
)

# Ensure BigQuery tables exist and start background scheduler
try:
    db.ensure_tables()
except Exception as e:
    st.error(f"BigQuery setup error: {e}")

start_scheduler()   # no-op on subsequent reruns (cached resource)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SEVERITY_COLOR = {
    "RED":   "#FF4B4B",
    "BLUE":  "#1E90FF",
    "AMBER": "#FFA500",
    "GREEN": "#21C55D",
}

_POSTURE_LABEL = {
    "RED":   "EXIT / STOP",
    "BLUE":  "ROLL",
    "AMBER": "WATCH",
    "GREEN": "HOLD",
}

_POSTURE_EMOJI = {
    "RED":   "🔴",
    "BLUE":  "🔵",
    "AMBER": "⚠️",
    "GREEN": "🟢",
}


def _live_market(pos: dict) -> dict:
    """Fetch live market data for a single position (with IV Rank)."""
    ticker   = pos.get("ticker", "")
    contract = pos.get("contract", "")
    snapshot = {}
    if contract:
        snapshot = options_data.get_option_snapshot(ticker, contract) or {}

    iv_rank = None
    try:
        from iv_rank import get_iv_rank_advanced
        result = get_iv_rank_advanced(ticker)
        if result and "Success" in result:
            m = re.search(r"([\d.]+)", result.split("is:")[-1])
            iv_rank = float(m.group(1)) if m else None
    except Exception:
        pass

    return {
        "mid":          snapshot.get("mid"),
        "delta":        snapshot.get("delta"),
        "dte":          snapshot.get("dte"),
        "iv_rank":      iv_rank,
        "thesis_score": db.get_leaps_monitor_score(ticker),
    }


def _pnl_pct(entry_price, mid):
    if entry_price and mid and entry_price > 0:
        return round((mid - entry_price) / entry_price * 100, 1)
    return None


def _posture(position: dict, mkt: dict) -> tuple[str, str]:
    """Return (severity, posture_label) for a position."""
    if position.get("mode") != "ACTIVE":
        return "GREEN", "WATCHLIST"
    alerts = evaluate(position, mkt)
    if not alerts:
        return "GREEN", "HOLD"
    worst = max(alerts, key=lambda a: {"RED": 3, "BLUE": 2, "AMBER": 1, "GREEN": 0}.get(a.severity, 0))
    return worst.severity, _POSTURE_LABEL.get(worst.severity, "HOLD")


# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------

st.sidebar.title("📈 LEAPS Manager")
page = st.sidebar.radio(
    "Navigate",
    ["Dashboard", "Add Position", "Alert History", "Settings"],
    label_visibility="collapsed",
)

st.sidebar.markdown("---")
st.sidebar.caption("Monitoring active during US market hours. Daily summary at 9:35 AM ET.")


# ===========================================================================
# PAGE 1: DASHBOARD
# ===========================================================================

if page == "Dashboard":
    st.title("Portfolio Dashboard")

    col_refresh, col_spacer = st.columns([1, 5])
    with col_refresh:
        force_check = st.button("🔄 Run Check Now", use_container_width=True)

    try:
        positions = db.get_positions()
    except Exception as e:
        st.error(f"Could not load positions: {e}")
        positions = []

    active    = [p for p in positions if p.get("mode") == "ACTIVE"]
    watchlist = [p for p in positions if p.get("mode") == "WATCHLIST"]

    if not positions:
        st.info("No positions yet. Go to **Add Position** to add your first ticker or existing LEAPS.")
        st.stop()

    # Portfolio summary strip
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Active Positions",   len(active))
    s2.metric("Watchlist",          len(watchlist))
    # Portfolio cost basis
    total_cost = sum(
        (p.get("entry_price") or 0) * (p.get("quantity") or 0) * 100
        for p in active
    )
    s3.metric("Portfolio Cost Basis", f"${total_cost:,.0f}" if total_cost else "N/A")
    s4.metric("Avg Contracts/Position", f"{sum(p.get('quantity') or 0 for p in active) / len(active):.1f}" if active else "N/A")

    st.markdown("---")

    # -----------------------------------------------------------------------
    # Active position cards
    # -----------------------------------------------------------------------
    if active:
        st.subheader("Active Positions")
        for pos in active:
            ticker   = pos.get("ticker", "?")
            strike   = pos.get("strike", "?")
            exp      = pos.get("expiration_date", "?")
            qty      = pos.get("quantity", "?")
            ep       = pos.get("entry_price")
            notes    = pos.get("notes", "")
            pos_id   = str(pos.get("id", ""))
            cost_basis = (ep or 0) * (qty or 0) * 100

            with st.spinner(f"Loading {ticker}..."):
                if force_check:
                    mkt = _live_market(pos)
                    time.sleep(12)     # respect Polygon rate limit
                else:
                    # Quick fetch without IV rank (faster for dashboard)
                    contract = pos.get("contract", "")
                    snap = options_data.get_option_snapshot(ticker, contract) or {} if contract else {}
                    mkt = {
                        "mid":          snap.get("mid"),
                        "delta":        snap.get("delta"),
                        "dte":          snap.get("dte"),
                        "iv_rank":      None,
                        "thesis_score": db.get_leaps_monitor_score(ticker),
                    }

            mid       = mkt.get("mid")
            delta     = mkt.get("delta")
            dte_days  = mkt.get("dte")
            iv_rank   = mkt.get("iv_rank")
            score     = mkt.get("thesis_score")
            pnl       = _pnl_pct(ep, mid)

            severity, posture_label = _posture(pos, mkt)
            color = _SEVERITY_COLOR.get(severity, "#21C55D")
            emoji = _POSTURE_EMOJI.get(severity, "🟢")

            with st.container(border=True):
                h1, h2 = st.columns([5, 1])
                with h1:
                    cb_str = f"  ·  Cost Basis ${cost_basis:,.0f}" if cost_basis else ""
                    st.markdown(
                        f"### {ticker} — {exp} ${strike} Call  ·  {qty} contract{'s' if int(qty or 1) > 1 else ''}{cb_str}"
                    )
                with h2:
                    st.markdown(
                        f"<div style='background:{color};color:white;padding:8px 12px;"
                        f"border-radius:8px;text-align:center;font-weight:bold;font-size:1.1em'>"
                        f"{emoji} {posture_label}</div>",
                        unsafe_allow_html=True,
                    )

                m1, m2, m3, m4, m5, m6 = st.columns(6)
                m1.metric("Entry Price",  f"${ep:.2f}"   if ep       else "N/A")
                m2.metric("Current Mid",  f"${mid:.2f}"  if mid      else "N/A")
                m3.metric("P&L",
                          f"{pnl:+.1f}%" if pnl is not None else "N/A",
                          delta_color="normal" if pnl is None else ("normal" if pnl >= 0 else "inverse"))
                m4.metric("Delta",        f"{delta:.2f}" if delta     else "N/A")
                m5.metric("DTE",          f"{dte_days}d" if dte_days  else "N/A")
                m6.metric("Thesis Score", f"{score}/100" if score     else "N/A")

                # Show latest triggered signal (if any)
                if force_check:
                    alerts = evaluate(pos, mkt)
                    if alerts:
                        worst = max(alerts, key=lambda a: {"RED":3,"BLUE":2,"AMBER":1,"GREEN":0}.get(a.severity,0))
                        st.info(f"**Signal:** {worst.subject}")
                    else:
                        st.success("All pillars clear — HOLD.")

                # Action buttons
                b1, b2, b3 = st.columns([1, 1, 4])
                with b1:
                    if st.button("Mark Closed", key=f"close_{pos_id}"):
                        db.update_position_mode(pos_id, "CLOSED")
                        st.rerun()
                with b2:
                    if st.button("Mark Rolled", key=f"roll_{pos_id}"):
                        db.update_position_mode(pos_id, "ROLLED")
                        st.rerun()

                if notes:
                    st.caption(f"Notes: {notes}")

    # -----------------------------------------------------------------------
    # Watchlist
    # -----------------------------------------------------------------------
    if watchlist:
        st.markdown("---")
        st.subheader("Watchlist — Waiting for Entry Signal")
        for pos in watchlist:
            ticker = pos.get("ticker", "?")
            pos_id = str(pos.get("id", ""))

            stock_data = get_price_and_range(ticker)
            price       = stock_data.get("price")
            pfl         = stock_data.get("pct_from_low")

            score = db.get_leaps_monitor_score(ticker)
            score_str = f"{score}/100" if score else "N/A"

            with st.container(border=True):
                c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 1])
                c1.metric("Ticker",       ticker)
                c2.metric("Current Price", f"${price:.2f}" if price else "N/A")
                c3.metric("52w Position", f"{pfl*100:.0f}% from low" if pfl is not None else "N/A")
                c4.metric("Thesis Score", score_str)
                with c5:
                    if st.button("Move to Active", key=f"activate_{pos_id}"):
                        db.update_position_mode(pos_id, "ACTIVE")
                        st.rerun()
                    if st.button("Remove", key=f"rm_watch_{pos_id}"):
                        db.delete_position(pos_id)
                        st.rerun()

            st.caption(pos.get("notes", ""))


# ===========================================================================
# PAGE 2: ADD POSITION
# ===========================================================================

elif page == "Add Position":
    st.title("Add Position")

    tab_new, tab_existing = st.tabs([
        "🔍 New Entry — Recommend Options",
        "📋 Existing Position — Already Bought",
    ])

    # -----------------------------------------------------------------------
    # Tab 1: Recommend options for a ticker
    # -----------------------------------------------------------------------
    with tab_new:
        st.markdown(
            "Enter a stock ticker you've identified and approved in the LEAPS Monitor. "
            "The app will fetch available LEAPS and recommend the best contracts for your "
            "10x (Moonshot) and 3-5x (Core) strategy."
        )

        ticker_new = st.text_input("Stock Ticker", placeholder="e.g. NVDA", max_chars=10).upper().strip()

        if st.button("Get Recommendations", type="primary", disabled=not ticker_new):
            with st.spinner(f"Fetching LEAPS chain for {ticker_new}..."):
                recs = recommend_options(ticker_new)

            if recs.get("error"):
                st.error(recs["error"])
            else:
                stock_price = recs["stock_price"]
                st.success(f"Current price: **${stock_price:.2f}**")

                moonshots = recs["MOONSHOT"]
                cores     = recs["CORE"]

                if not moonshots and not cores:
                    st.warning("No qualifying LEAPS found. The ticker may have limited options liquidity.")
                else:
                    col_m, col_c = st.columns(2)

                    with col_m:
                        st.markdown("### Moonshot Picks (10x target)")
                        st.caption("Far OTM · Low delta · Big stock move needed · High risk / High reward")
                        for i, c in enumerate(moonshots):
                            with st.expander(
                                f"Option {i+1}:  ${c.get('strike')}C  {c.get('expiration_date')}  "
                                f"|  Mid ${c.get('mid','?')}  |  Δ {c.get('delta','?')}",
                                expanded=(i == 0),
                            ):
                                st.code(format_recommendation(c, stock_price))
                                if st.button("Add to Watchlist as Moonshot", key=f"add_moon_{i}"):
                                    exp_date = c.get("expiration_date")
                                    pos_id = db.save_position({
                                        "ticker":          ticker_new,
                                        "contract":        c.get("contract", ""),
                                        "option_type":     "CALL",
                                        "strike":          c.get("strike"),
                                        "expiration_date": exp_date,
                                        "entry_date":      None,
                                        "entry_price":     None,
                                        "quantity":        None,
                                        "entry_delta":     c.get("delta"),
                                        "entry_iv_rank":   None,
                                        "entry_thesis_score": db.get_leaps_monitor_score(ticker_new),
                                        "position_type":   "MOONSHOT",
                                        "target_return":   "10x",
                                        "mode":            "WATCHLIST",
                                        "notes":           f"Recommended by engine. Δ={c.get('delta')}, move needed for 10x: {c.get('10x_required_move_pct','?')}%",
                                    })
                                    st.success(f"Added {ticker_new} moonshot to watchlist!")

                    with col_c:
                        st.markdown("### Core Picks (3-5x target)")
                        st.caption("Moderate OTM · Medium delta · Realistic stock move · Balanced risk / reward")
                        for i, c in enumerate(cores):
                            with st.expander(
                                f"Option {i+1}:  ${c.get('strike')}C  {c.get('expiration_date')}  "
                                f"|  Mid ${c.get('mid','?')}  |  Δ {c.get('delta','?')}",
                                expanded=(i == 0),
                            ):
                                st.code(format_recommendation(c, stock_price))
                                if st.button("Add to Watchlist as Core", key=f"add_core_{i}"):
                                    exp_date = c.get("expiration_date")
                                    pos_id = db.save_position({
                                        "ticker":          ticker_new,
                                        "contract":        c.get("contract", ""),
                                        "option_type":     "CALL",
                                        "strike":          c.get("strike"),
                                        "expiration_date": exp_date,
                                        "entry_date":      None,
                                        "entry_price":     None,
                                        "quantity":        None,
                                        "entry_delta":     c.get("delta"),
                                        "entry_iv_rank":   None,
                                        "entry_thesis_score": db.get_leaps_monitor_score(ticker_new),
                                        "position_type":   "CORE",
                                        "target_return":   "3-5x",
                                        "mode":            "WATCHLIST",
                                        "notes":           f"Recommended by engine. Δ={c.get('delta')}, move needed for 3x: {c.get('3x_required_move_pct','?')}%",
                                    })
                                    st.success(f"Added {ticker_new} core to watchlist!")

    # -----------------------------------------------------------------------
    # Tab 2: Add existing position (already purchased)
    # -----------------------------------------------------------------------
    with tab_existing:
        st.markdown(
            "You've already bought this option. Enter the details and the app "
            "will immediately preview your current position health."
        )

        with st.form("add_existing_form"):
            c1, c2 = st.columns(2)
            with c1:
                ticker_ex  = st.text_input("Stock Ticker *", placeholder="NVDA").upper().strip()
                strike_ex  = st.number_input("Strike Price *", min_value=0.0, step=0.5, format="%.2f")
                exp_date   = st.date_input("Expiration Date *", min_value=date.today() + timedelta(days=30))
                opt_type   = st.selectbox("Option Type", ["CALL", "PUT"])
            with c2:
                entry_price_ex = st.number_input(
                    "Avg. Price $/share *",
                    min_value=0.01, step=0.01, format="%.2f",
                    help="From IBKR 'Avg. Price' column — the premium you paid per share (× 100 = cost per contract)"
                )
                qty_ex     = st.number_input(
                    "Pos (number of contracts) *",
                    min_value=1, step=1, value=1,
                    help="From IBKR 'Pos' column"
                )
                entry_date = st.date_input("Entry Date *", max_value=date.today())
                # Cost Basis computed display (read-only)
                cb_display = entry_price_ex * qty_ex * 100 if entry_price_ex and qty_ex else 0
                st.metric("Cost Basis (computed)", f"${cb_display:,.2f}",
                          help="Pos × 100 × Avg. Price — verify this matches IBKR 'Cost Basis'")

            mode_ex    = st.radio("Mode", ["ACTIVE", "WATCHLIST"], horizontal=True,
                                  help="ACTIVE = monitoring with exit alerts | WATCHLIST = watching for entry signals")
            notes_ex   = st.text_area("Notes (optional)", placeholder="e.g. Bought on earnings dip")

            submitted = st.form_submit_button("Save Position", type="primary")

        if submitted and ticker_ex and strike_ex and exp_date and entry_price_ex:
            # Build OCC symbol
            contract_sym = options_data.to_occ(ticker_ex, exp_date, opt_type[0], strike_ex)

            with st.spinner("Fetching live data and saving..."):
                snap  = options_data.get_option_snapshot(ticker_ex, contract_sym) or {}
                score = db.get_leaps_monitor_score(ticker_ex)

            mid   = snap.get("mid")
            delta = snap.get("delta")
            dte_d = snap.get("dte")
            pnl   = _pnl_pct(entry_price_ex, mid)

            # Save immediately (no second click needed)
            db.save_position({
                "ticker":             ticker_ex,
                "contract":           contract_sym,
                "option_type":        opt_type,
                "strike":             strike_ex,
                "expiration_date":    exp_date,
                "entry_date":         entry_date,
                "entry_price":        entry_price_ex,
                "quantity":           int(qty_ex),
                "entry_delta":        delta,
                "entry_iv_rank":      None,
                "entry_thesis_score": score,
                "position_type":      "STANDARD",
                "target_return":      "5-10x",
                "mode":               mode_ex,
                "notes":              notes_ex,
            })

            st.success(f"✅ Position saved! {ticker_ex} {exp_date} ${strike_ex}C — go to **Dashboard** to monitor it.")

            # Show snapshot after saving
            st.markdown("---")
            st.subheader("Position Snapshot")
            pr1, pr2, pr3, pr4, pr5 = st.columns(5)
            pr1.metric("Current Mid",  f"${mid:.2f}"  if mid    else "N/A")
            pr2.metric("P&L",          f"{pnl:+.1f}%" if pnl is not None else "N/A")
            pr3.metric("Delta",        f"{delta:.2f}" if delta   else "N/A")
            pr4.metric("DTE",          f"{dte_d}d"    if dte_d   else "N/A")
            pr5.metric("Thesis Score", f"{score}/100" if score   else "N/A")

            # Run a quick pillar check
            dummy_pos = {
                "ticker":          ticker_ex,
                "entry_price":     entry_price_ex,
                "expiration_date": exp_date,
                "mode":            mode_ex,
            }
            dummy_mkt = {"mid": mid, "delta": delta, "dte": dte_d, "iv_rank": None, "thesis_score": score}
            preview_alerts = evaluate(dummy_pos, dummy_mkt) if mode_ex == "ACTIVE" else []

            if preview_alerts:
                worst = max(preview_alerts, key=lambda a: {"RED":3,"BLUE":2,"AMBER":1}.get(a.severity,0))
                st.warning(f"Initial signal: {worst.subject}")
            else:
                st.info("All pillars clear — position looks healthy.")


# ===========================================================================
# PAGE 3: ALERT HISTORY
# ===========================================================================

elif page == "Alert History":
    st.title("Alert History")

    # Filters
    fc1, fc2, fc3 = st.columns([1.5, 2, 2])
    with fc1:
        filter_ticker = st.text_input("Filter ticker", placeholder="NVDA").upper().strip()
    with fc2:
        filter_severity = st.multiselect(
            "Severity",
            ["RED", "BLUE", "AMBER", "GREEN"],
            placeholder="All severities",
        )
    with fc3:
        filter_type = st.multiselect(
            "Alert type",
            ["EXIT_THESIS", "EXIT_STOP", "EXIT_TIME_URGENT", "EXIT_TIME_WARNING",
             "ROLL_DELTA", "ROLL_TIME", "PROFIT_50", "PROFIT_100", "PROFIT_200",
             "PROFIT_300", "PROFIT_500", "PROFIT_900", "ENTRY_SIGNAL", "ENTRY_WATCH",
             "DAILY_SUMMARY", "DELTA_WARN"],
            placeholder="All types",
        )

    try:
        alerts = db.get_alerts(ticker=filter_ticker or None, limit=300)
    except Exception as e:
        st.error(f"Could not load alerts: {e}")
        alerts = []

    if filter_severity:
        alerts = [a for a in alerts if a.get("severity") in filter_severity]
    if filter_type:
        alerts = [a for a in alerts if a.get("alert_type") in filter_type]

    st.caption(f"Showing {len(alerts)} alerts")

    if not alerts:
        st.info("No alerts yet. Alerts appear here once the monitoring loop runs.")
    else:
        for a in alerts:
            sev   = a.get("severity", "GREEN")
            color = _SEVERITY_COLOR.get(sev, "#21C55D")
            emoji = _POSTURE_EMOJI.get(sev, "🟢")
            ts    = a.get("triggered_at", "")
            if isinstance(ts, datetime):
                ts = ts.strftime("%b %d, %Y  %H:%M")

            with st.expander(
                f"{emoji}  {ts}  |  {a.get('ticker', '')}  |  {a.get('alert_type', '')}",
                expanded=False,
            ):
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("P&L at Alert",  f"{a.get('current_pnl_pct'):+.1f}%" if a.get("current_pnl_pct") is not None else "N/A")
                c2.metric("Delta",         f"{a.get('current_delta'):.2f}" if a.get("current_delta") else "N/A")
                c3.metric("DTE",           f"{a.get('current_dte')}d" if a.get("current_dte") else "N/A")
                c4.metric("Thesis Score",  f"{a.get('current_thesis_score')}/100" if a.get("current_thesis_score") else "N/A")
                st.code(a.get("body", ""), language=None)
                sent_str = "✅ Email sent" if a.get("email_sent") else "❌ Email failed"
                st.caption(sent_str)


# ===========================================================================
# PAGE 4: SETTINGS
# ===========================================================================

elif page == "Settings":
    st.title("Settings")

    st.subheader("Email Configuration")
    try:
        sender    = st.secrets.get("GMAIL_SENDER", "not configured")
        recipient = st.secrets.get("ALERT_RECIPIENT_EMAIL", sender)
        st.info(f"Alerts sent from **{sender}** to **{recipient}**\nChange via secrets.toml in Streamlit Cloud.")
    except Exception:
        st.warning("Gmail secrets not configured. Add GMAIL_SENDER, GMAIL_APP_PASSWORD, ALERT_RECIPIENT_EMAIL to secrets.")

    if st.button("Send Test Email"):
        with st.spinner("Sending test email..."):
            ok = email_alerts.send_test_email()
        if ok:
            st.success("Test email sent successfully. Check your inbox.")
        else:
            st.error("Test email failed. Check your Gmail secrets configuration.")

    st.markdown("---")
    st.subheader("Alert Thresholds")
    st.info(
        "Default thresholds are set based on professional LEAPS management rules. "
        "Custom overrides coming in a future update."
    )

    st.markdown(
        """
        All positions use a single unified threshold set targeting **5-10x returns**.

        | Signal | Threshold | Action |
        |---|---|---|
        | **Stop loss** | -60% | Exit — recovery from here is statistically rare |
        | **First trim** | +100% (2x) | Sell 20-25% — recover initial cost basis |
        | **Trim & Roll** | +300% (4x) | Sell 50% to lock gains; roll remainder if DTE > 90 |
        | **Trim hard** | +600% (7x) | Sell 75%; trail 25% toward 10x |
        | **Full exit** | +900% (10x) | Exit everything — target hit |
        | **Roll delta** | Δ > 0.90 | Leverage exhausted — roll to higher strike |
        | **Delta warn** | Δ < 0.10 | Option near worthless — reassess |
        | **DTE roll window** | < 270 days | Roll if profitable |
        | **DTE urgent** | < 90 days | Exit losers; take profits if positive |
        | **DTE hard exit** | < 60 days | Exit regardless of P&L |
        """
    )

    st.markdown("---")
    st.subheader("Monitoring Schedule")
    st.markdown(
        """
        - Active position checks: **every 30 minutes** (Mon–Fri, 9:30–16:00 ET)
        - Watchlist entry checks: **every hour** (Mon–Fri, 9:30–16:00 ET)
        - Daily portfolio summary email: **9:35 AM ET** (Mon–Fri)
        - Deduplication: each alert type fires **at most once per day** per position
        """
    )

    st.markdown("---")
    st.subheader("Closed / Rolled Positions")
    try:
        closed = db.get_positions(mode="CLOSED") + db.get_positions(mode="ROLLED")
    except Exception:
        closed = []

    if not closed:
        st.caption("No closed or rolled positions.")
    else:
        st.caption(f"{len(closed)} closed/rolled positions")
        for pos in closed:
            c1, c2, c3 = st.columns([3, 2, 1])
            c1.write(f"{pos.get('ticker')}  {pos.get('expiration_date')} ${pos.get('strike')}C  — {pos.get('mode')}")
            c2.write(f"Entry: ${pos.get('entry_price')}  ×{pos.get('quantity')}")
            with c3:
                if st.button("Delete", key=f"del_{pos.get('id')}"):
                    db.delete_position(str(pos.get("id")))
                    st.rerun()
