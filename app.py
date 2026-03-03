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
import score_thesis
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


@st.cache_data(ttl=3600)
def _get_iv_rank_cached(ticker: str) -> float | None:
    """Fetch IV rank for a ticker and cache for 1 hour (slow API call)."""
    try:
        from iv_rank import get_iv_rank_advanced
        result = get_iv_rank_advanced(ticker)
        if result and "Success" in result:
            m = re.search(r"([\d.]+)", result.split("is:")[-1])
            return float(m.group(1)) if m else None
    except Exception:
        pass
    return None


def _live_market(pos: dict) -> dict:
    """Fetch live market data for a single position (with IV Rank)."""
    ticker   = pos.get("ticker", "")
    contract = pos.get("contract", "")
    snapshot = {}
    if contract:
        snapshot = options_data.get_option_snapshot(ticker, contract) or {}

    return {
        "mid":     snapshot.get("mid"),
        "bid":     snapshot.get("bid"),
        "ask":     snapshot.get("ask"),
        "delta":   snapshot.get("delta"),
        "dte":     snapshot.get("dte"),
        "iv_rank": _get_iv_rank_cached(ticker),
        # thesis_score injected separately in the dashboard loop
        # so we can also show the score age
        "thesis_score": None,
    }


def _pnl_pct(entry_price, mid):
    if entry_price and mid and entry_price > 0:
        return round((mid - entry_price) / entry_price * 100, 1)
    return None


def _alert_priority(a) -> int:
    return {"RED": 3, "BLUE": 2, "AMBER": 1, "GREEN": 0}.get(a.severity, 0)


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
                    polygon_error = None
                else:
                    # Quick fetch without IV rank (faster for dashboard)
                    contract = pos.get("contract", "")
                    snap = options_data.get_option_snapshot(ticker, contract) if contract else {}
                    snap = snap or {}
                    polygon_error = snap.get("_error")
                    # DTE fallback: compute from stored expiration_date when live data fails
                    dte_fallback = None
                    try:
                        raw_exp = pos.get("expiration_date")
                        if raw_exp:
                            exp_d = raw_exp if isinstance(raw_exp, date) else date.fromisoformat(str(raw_exp))
                            dte_fallback = (exp_d - date.today()).days
                    except Exception:
                        pass
                    mkt = {
                        "mid":          snap.get("mid"),
                        "bid":          snap.get("bid"),
                        "ask":          snap.get("ask"),
                        "delta":        snap.get("delta"),
                        "dte":          snap.get("dte") or dte_fallback,
                        "iv_rank":      _get_iv_rank_cached(ticker),
                        "thesis_score": None,  # injected below from get_leaps_monitor_score_with_age
                    }

            mid      = mkt.get("mid")
            delta    = mkt.get("delta")
            dte_days = mkt.get("dte")
            pnl      = _pnl_pct(ep, mid)

            # Thesis score + age (for staleness display)
            score, score_age = db.get_leaps_monitor_score_with_age(ticker)

            # ── Auto-score if thesis is missing from BigQuery ────────────────
            # Happens the first time a ticker is tracked, or if the shared
            # master_table has no entry yet.  Runs synchronously (~30s) so the
            # score is immediately available for Pillar 1 evaluation this run.
            if score is None:
                with st.spinner(f"First-time thesis scoring for {ticker} (~30s)..."):
                    _s, _v = score_thesis.compute_and_save_score(ticker)
                if _s is not None:
                    score, score_age = _s, 0

            # Inject thesis score into mkt so evaluate() sees it
            mkt["thesis_score"] = score

            # Run evaluation once — drives both badge AND inline recommendation
            position_alerts = evaluate(pos, mkt)
            if position_alerts:
                worst_alert   = max(position_alerts, key=_alert_priority)
                severity      = worst_alert.severity
                posture_label = _POSTURE_LABEL.get(severity, "HOLD")
            else:
                worst_alert   = None
                severity      = "GREEN"
                posture_label = "HOLD"

            color = _SEVERITY_COLOR.get(severity, "#21C55D")
            emoji = _POSTURE_EMOJI.get(severity, "🟢")

            qty_trimmed = int(pos.get("quantity_trimmed") or 0)
            proceeds    = float(pos.get("proceeds_from_trims") or 0.0)
            qty_remaining = int(qty or 0) - qty_trimmed

            with st.container(border=True):
                h1, h2 = st.columns([5, 1])
                with h1:
                    cb_str = f"  ·  Cost Basis ${cost_basis:,.0f}" if cost_basis else ""
                    trim_str = ""
                    if qty_trimmed > 0:
                        trim_str = (
                            f"  ·  {qty_remaining} remaining ({qty_trimmed} trimmed"
                            + (f", ${proceeds:,.0f} recovered" if proceeds else "")
                            + ")"
                        )
                    st.markdown(
                        f"### {ticker} — {exp} ${strike} Call  ·  "
                        f"{qty} contract{'s' if int(qty or 1) > 1 else ''}"
                        f"{cb_str}{trim_str}"
                    )
                with h2:
                    st.markdown(
                        f"<div style='background:{color};color:white;padding:8px 12px;"
                        f"border-radius:8px;text-align:center;font-weight:bold;font-size:1.1em'>"
                        f"{emoji} {posture_label}</div>",
                        unsafe_allow_html=True,
                    )

                m1, m2, m3, m4, m5, m6 = st.columns(6)
                m1.metric("Avg. Price",   f"${ep:.2f}"   if ep       else "N/A")
                m2.metric("Current Mid",  f"${mid:.2f}"  if mid      else "N/A")
                m3.metric("P&L",
                          f"{pnl:+.1f}%" if pnl is not None else "N/A",
                          delta_color="normal" if pnl is None else ("normal" if pnl >= 0 else "inverse"))
                m4.metric("Delta",        f"{delta:.2f}" if delta     else "N/A")
                m5.metric("DTE",          f"{dte_days}d" if dte_days  else "N/A")
                # Thesis score with age indicator
                if score is not None:
                    age_label = f" ({score_age}d ago)" if score_age is not None else ""
                    stale = score_age is not None and score_age > 30
                    m6.metric("Thesis Score", f"{score}/100{age_label}",
                              delta="⚠️ stale" if stale else None,
                              delta_color="inverse" if stale else "normal")
                else:
                    m6.metric("Thesis Score", "N/A")
                with m6:
                    if st.button("↻ Re-score", key=f"rescore_{pos_id}",
                                 help="Re-compute thesis score now using yfinance + Gemini"):
                        with st.spinner(f"Scoring {ticker} thesis (~30s)..."):
                            new_score, new_verdict = score_thesis.compute_and_save_score(ticker)
                        if new_score is not None:
                            st.success(f"Score updated: {new_score}/100 ({new_verdict})")
                            st.rerun()
                        else:
                            st.error("Scoring failed — check logs")

                if polygon_error:
                    st.warning(f"⚠️ Live data unavailable: {polygon_error}")

                # ── Active signal — always visible on the card ──────────────
                if worst_alert:
                    sig_color = _SEVERITY_COLOR.get(worst_alert.severity, "#FFA500")
                    st.markdown(
                        f"<div style='background:{sig_color}22;border-left:4px solid {sig_color};"
                        f"padding:10px 14px;border-radius:4px;margin:8px 0'>"
                        f"<strong>Active Signal:</strong>&nbsp; {worst_alert.subject}</div>",
                        unsafe_allow_html=True,
                    )
                    with st.expander("📋 View recommendation & trade instruction"):
                        st.code(worst_alert.body, language=None)
                        # If multiple alerts fired, show the others too
                        others = [a for a in position_alerts if a is not worst_alert]
                        if others:
                            st.caption(f"Additional signals ({len(others)}):")
                            for a in sorted(others, key=_alert_priority, reverse=True):
                                st.markdown(f"- {_POSTURE_EMOJI.get(a.severity,'•')} {a.subject}")
                else:
                    st.success("✅ All pillars clear — HOLD")

                # Action buttons
                b1, b2, b3, b4, b5 = st.columns([1, 1, 1, 1, 1])
                with b1:
                    if st.button("Mark Closed", key=f"close_{pos_id}"):
                        db.update_position_mode(pos_id, "CLOSED")
                        st.rerun()
                with b2:
                    if st.button("Mark Rolled", key=f"roll_{pos_id}"):
                        db.update_position_mode(pos_id, "ROLLED")
                        st.rerun()
                with b3:
                    if st.button("✏️ Edit", key=f"edit_{pos_id}"):
                        st.session_state["editing_pos_id"] = pos_id
                        st.session_state.pop("trimming_pos_id", None)
                with b4:
                    if st.button("✂️ Record Trim", key=f"trim_{pos_id}",
                                 help="Record contracts you've sold from this position"):
                        st.session_state["trimming_pos_id"] = pos_id
                        st.session_state.pop("editing_pos_id", None)

                if notes:
                    st.caption(f"Notes: {notes}")

            # ---- Inline Edit Panel ----
            if st.session_state.get("editing_pos_id") == pos_id:
                with st.container(border=True):
                    st.markdown("#### Edit Position")
                    st.caption("Tip: Avg. Price = option premium per share from IBKR (NOT the stock price, NOT the strike)")
                    ec1, ec2, ec3 = st.columns(3)
                    with ec1:
                        new_ep  = st.number_input(
                            "Avg. Price $/share",
                            value=float(ep or 0), min_value=0.01, step=0.01, format="%.2f",
                            help="Option premium per share you paid (IBKR 'Avg. Price'). ×100 = cost per contract.",
                            key=f"ep_{pos_id}")
                        new_qty = st.number_input(
                            "Pos (contracts)",
                            value=int(qty or 1), min_value=1, step=1,
                            help="Number of contracts (IBKR 'Pos')",
                            key=f"qty_{pos_id}")
                    with ec2:
                        # Parse stored strike for default
                        _strike_default = float(pos.get("strike") or 0)
                        new_strike = st.number_input(
                            "Strike Price",
                            value=_strike_default, min_value=0.01, step=0.5, format="%.2f",
                            help="The strike price of the option contract (e.g. $15, $150)",
                            key=f"strike_{pos_id}")
                        # Parse stored expiry for default
                        _exp_raw = pos.get("expiration_date")
                        try:
                            _exp_default = _exp_raw if isinstance(_exp_raw, date) else date.fromisoformat(str(_exp_raw))
                        except Exception:
                            _exp_default = date.today() + timedelta(days=540)
                        new_exp = st.date_input(
                            "Expiration Date",
                            value=_exp_default,
                            min_value=date.today(),
                            key=f"exp_{pos_id}")
                    with ec3:
                        new_mode = st.selectbox(
                            "Mode", ["ACTIVE", "WATCHLIST"],
                            index=0 if pos.get("mode") == "ACTIVE" else 1,
                            key=f"mode_{pos_id}")
                        # Live cost basis preview
                        cb_preview = new_ep * new_qty * 100
                        st.metric("Cost Basis (preview)", f"${cb_preview:,.0f}",
                                  help="Avg. Price × Pos × 100 — verify against IBKR")

                    # Trim tracking row
                    st.markdown("**Trim tracking** — record contracts you've already sold")
                    tr1, tr2 = st.columns(2)
                    with tr1:
                        new_qty_trimmed = st.number_input(
                            "Contracts sold so far (trimmed)",
                            value=int(pos.get("quantity_trimmed") or 0),
                            min_value=0, max_value=int(new_qty), step=1,
                            help="How many contracts you've already sold from this position",
                            key=f"qtrim_{pos_id}")
                    with tr2:
                        new_proceeds = st.number_input(
                            "Total proceeds from trims ($)",
                            value=float(pos.get("proceeds_from_trims") or 0.0),
                            min_value=0.0, step=100.0, format="%.0f",
                            help="Total dollar amount received from all trim sales",
                            key=f"proc_{pos_id}")
                    # House money indicator
                    if new_qty_trimmed > 0 and new_proceeds > 0:
                        remaining_cost = new_ep * (new_qty - new_qty_trimmed) * 100
                        if new_proceeds >= new_ep * new_qty * 100:
                            st.success(f"HOUSE MONEY — cost fully recovered. "
                                       f"Remaining {int(new_qty) - new_qty_trimmed} contracts cost $0 net.")
                        elif new_proceeds > 0:
                            net_cost = max(0, remaining_cost - (new_proceeds - new_ep * new_qty_trimmed * 100))
                            st.info(f"Cost recovered: ${new_proceeds:,.0f}  |  "
                                    f"Remaining net cost: ~${remaining_cost:,.0f}  |  "
                                    f"Trimmed: {new_qty_trimmed}/{int(new_qty)} contracts")

                    new_notes = st.text_area("Notes", value=notes or "", key=f"notes_{pos_id}")
                    sv1, sv2 = st.columns([1, 5])
                    with sv1:
                        if st.button("Save Changes", type="primary", key=f"save_{pos_id}"):
                            # Rebuild OCC contract symbol whenever strike or expiry changes
                            opt_type_char = (pos.get("option_type") or "CALL")[0].upper()
                            new_contract = options_data.to_occ(ticker, new_exp, opt_type_char, new_strike)
                            updates = {
                                "entry_price":        new_ep,
                                "quantity":           int(new_qty),
                                "strike":             new_strike,
                                "expiration_date":    new_exp,
                                "contract":           new_contract,
                                "mode":               new_mode,
                                "notes":              new_notes,
                                "quantity_trimmed":   int(new_qty_trimmed),
                                "proceeds_from_trims": float(new_proceeds),
                            }
                            try:
                                db.update_position(pos_id, updates)
                                del st.session_state["editing_pos_id"]
                                st.rerun()
                            except Exception as e:
                                st.error(f"Save failed: {e}")
                    with sv2:
                        if st.button("Cancel", key=f"cancel_{pos_id}"):
                            del st.session_state["editing_pos_id"]
                            st.rerun()

            # ---- Inline Trim Panel ----
            if st.session_state.get("trimming_pos_id") == pos_id:
                with st.container(border=True):
                    st.markdown("#### ✂️ Record Trim")
                    st.caption(
                        "Enter how many contracts you sold in this trim event "
                        "and the price you received per share (from IBKR 'Avg. Price' on the sell). "
                        "This is additive — each trim adds to the running total."
                    )
                    tr1, tr2 = st.columns(2)
                    with tr1:
                        max_trim = max(1, qty_remaining)
                        trim_qty_now = st.number_input(
                            "Contracts sold in this trim",
                            min_value=1, max_value=max_trim, step=1,
                            key=f"tnq_{pos_id}",
                            help=f"You have {qty_remaining} contracts remaining"
                        )
                    with tr2:
                        trim_price_now = st.number_input(
                            "Sale price per share $/share",
                            min_value=0.01, step=0.01, format="%.2f",
                            key=f"tnp_{pos_id}",
                            help="From IBKR 'Avg. Price' on the closing trade"
                        )

                    this_proceeds = trim_qty_now * trim_price_now * 100
                    new_total_trimmed  = qty_trimmed + trim_qty_now
                    new_total_proceeds = proceeds + this_proceeds
                    original_cost      = (ep or 0) * (int(qty or 0)) * 100

                    st.info(
                        f"**This trim:** {trim_qty_now} contracts × ${trim_price_now:.2f}/share × 100 = "
                        f"**${this_proceeds:,.0f}** proceeds"
                    )

                    # Running cumulative after this trim
                    pnl_on_trim = None
                    if ep and trim_price_now and ep > 0:
                        pnl_on_trim = round((trim_price_now - ep) / ep * 100, 1)

                    pnl_str = f"  ·  P&L on trim: {pnl_on_trim:+.1f}%" if pnl_on_trim is not None else ""
                    contracts_left = int(qty or 0) - new_total_trimmed

                    if original_cost > 0 and new_total_proceeds >= original_cost:
                        st.success(
                            f"HOUSE MONEY after this trim!  "
                            f"${new_total_proceeds:,.0f} recovered ≥ ${original_cost:,.0f} cost basis.  "
                            f"{contracts_left} contracts left cost $0 net.{pnl_str}"
                        )
                    else:
                        net_remaining_cost = max(0, original_cost - new_total_proceeds)
                        st.markdown(
                            f"After this trim: **{new_total_trimmed}/{int(qty or 0)}** contracts sold  ·  "
                            f"**${new_total_proceeds:,.0f}** recovered  ·  "
                            f"**${net_remaining_cost:,.0f}** still at risk  ·  "
                            f"**{contracts_left}** contracts remaining{pnl_str}"
                        )

                    tc1, tc2 = st.columns([1, 5])
                    with tc1:
                        if st.button("Save Trim", type="primary", key=f"rtrim_{pos_id}"):
                            try:
                                db.update_position(pos_id, {
                                    "quantity_trimmed":    new_total_trimmed,
                                    "proceeds_from_trims": new_total_proceeds,
                                })
                                del st.session_state["trimming_pos_id"]
                                st.rerun()
                            except Exception as e:
                                st.error(f"Trim save failed: {e}")
                    with tc2:
                        if st.button("Cancel", key=f"tcancel_{pos_id}"):
                            del st.session_state["trimming_pos_id"]
                            st.rerun()

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
            if score is None:
                with st.spinner(f"First-time thesis scoring for {ticker}..."):
                    _s, _v = score_thesis.compute_and_save_score(ticker)
                    score = _s
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

        st.info(
            "**Field guide (from IBKR):**  "
            "Strike = the option's strike price (e.g. $15)  ·  "
            "Avg. Price = option premium per share you paid (e.g. $3.20)  ·  "
            "Pos = number of contracts  ·  "
            "Cost Basis = Avg. Price × Pos × 100"
        )
        with st.form("add_existing_form"):
            c1, c2 = st.columns(2)
            with c1:
                ticker_ex  = st.text_input("Stock Ticker *", placeholder="NVDA").upper().strip()
                strike_ex  = st.number_input(
                    "Strike Price * — the option's strike (e.g. $15, $150)",
                    min_value=0.0, step=0.5, format="%.2f",
                    help="This is NOT your avg price. It's the price level the option contract is written on.")
                exp_date   = st.date_input("Expiration Date *", min_value=date.today() + timedelta(days=30))
                opt_type   = st.selectbox("Option Type", ["CALL", "PUT"])
            with c2:
                entry_price_ex = st.number_input(
                    "Avg. Price $/share * — option premium you paid per share",
                    min_value=0.01, step=0.01, format="%.2f",
                    help="From IBKR 'Avg. Price' column. NOT the stock price, NOT the strike. × 100 = cost per contract."
                )
                qty_ex     = st.number_input(
                    "Pos (number of contracts) *",
                    min_value=1, step=1, value=1,
                    help="From IBKR 'Pos' column"
                )
                # Cost Basis computed display (read-only)
                cb_display = entry_price_ex * qty_ex * 100 if entry_price_ex and qty_ex else 0
                st.metric("Cost Basis (computed)", f"${cb_display:,.2f}",
                          help="Avg. Price × Pos × 100 — verify this matches IBKR 'Cost Basis'")

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

            snap_error = snap.get("_error")
            mid   = snap.get("mid")
            delta = snap.get("delta")
            dte_d = snap.get("dte") or (exp_date - date.today()).days
            pnl   = _pnl_pct(entry_price_ex, mid)

            # Save immediately (no second click needed)
            db.save_position({
                "ticker":             ticker_ex,
                "contract":           contract_sym,
                "option_type":        opt_type,
                "strike":             strike_ex,
                "expiration_date":    exp_date,
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

            if snap_error:
                st.warning(f"⚠️ Live data unavailable: {snap_error}  (DTE computed from expiration date)")

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
            [
                # Threshold-based (Pillars 1-5)
                "EXIT_THESIS", "EXIT_STOP", "EXIT_TIME_URGENT", "EXIT_TIME_WARNING",
                "ROLL_DELTA", "ROLL_TIME",
                "PROFIT_100", "PROFIT_300", "PROFIT_600", "PROFIT_900",
                "ENTRY_SIGNAL", "ENTRY_WATCH", "DELTA_WARN",
                # IV Timing (Pillar 6) — fires when IV window is optimal
                "IV_EXIT_NOW", "IV_TRIM_NOW",
                "IV_ROLL_SELL_NOW", "IV_ROLL_BUY_NOW",
                "IV_ENTRY_OPTIMAL",
                # System
                "DAILY_SUMMARY",
            ],
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
    st.subheader("Thesis Score Management")
    st.caption(
        "Scores are fetched from the shared BigQuery master_table (same data as the LEAPS Evaluator). "
        "When a score is missing, the app auto-scores using yfinance + Gemini with Google Search. "
        "Use the buttons below to force a full rescore for all active positions."
    )

    rs1, rs2 = st.columns([1, 3])
    with rs1:
        rescore_all = st.button("↻ Rescore All Active", type="primary",
                                help="Re-score every active position using the full pipeline (Gemini + Google Search). ~30s per ticker.")
    with rs2:
        st.caption("Runs the same Gemini + Google Search pipeline used by the LEAPS Evaluator to get accurate moat scores, CEO ownership, and business model classification.")

    if rescore_all:
        try:
            active_tickers = list({p["ticker"] for p in db.get_positions(mode="ACTIVE") if p.get("ticker")})
        except Exception as e:
            st.error(f"Could not fetch positions: {e}")
            active_tickers = []

        if not active_tickers:
            st.info("No active positions to rescore.")
        else:
            results = []
            prog = st.progress(0.0, text=f"Rescoring 0/{len(active_tickers)}...")
            for i, tk in enumerate(active_tickers):
                prog.progress((i) / len(active_tickers), text=f"Scoring {tk} ({i+1}/{len(active_tickers)})...")
                s, v = score_thesis.compute_and_save_score(tk)
                results.append({"Ticker": tk, "Score": s, "Verdict": v, "Status": "✅" if s else "❌"})
            prog.progress(1.0, text="Done!")

            import pandas as pd
            st.dataframe(
                pd.DataFrame(results),
                use_container_width=True,
                hide_index=True,
            )
            st.success(f"Rescored {len([r for r in results if r['Score']])} of {len(active_tickers)} tickers successfully.")
            st.cache_data.clear()

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
