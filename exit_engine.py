"""
The 5-Pillar Exit Engine.

For each active position, evaluates five independent pillars and returns
a list of Alert objects. Alerts are calibrated to the position's role:
  MOONSHOT  — 10x target (far OTM, higher loss tolerance, different profit ladders)
  CORE      — 3-5x target (standard LEAPS management)
  TACTICAL  — smaller positions, faster profit-taking

Each alert has:
  type     — string code (e.g. ROLL_DELTA, PROFIT_100, EXIT_THESIS)
  severity — RED / AMBER / BLUE / GREEN
  subject  — short email subject
  body     — full formatted email body
  context  — dict of the market data snapshot that triggered it
"""

from dataclasses import dataclass, field
from datetime import date
from options_data import get_roll_contract_price


# ---------------------------------------------------------------------------
# Alert data class
# ---------------------------------------------------------------------------

@dataclass
class Alert:
    type:     str
    severity: str                    # RED / AMBER / BLUE / GREEN
    subject:  str
    body:     str
    context:  dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Thresholds (tunable per role)
# ---------------------------------------------------------------------------

_THRESHOLDS = {
    "MOONSHOT": {
        "stop_loss":         -70,   # -70% before exit alert (lottery tickets need room)
        "profit_first":      200,   # +200% = 3x → first scale-out signal
        "profit_second":     500,   # +500% = 6x → aggressive trim
        "profit_final":      900,   # +900% ≈ 10x → full exit
        "delta_high":        0.50,  # moonshots shouldn't go deep ITM
        "delta_low":         0.07,  # if delta this low, nearly worthless
        "dte_review":        270,   # suggest roll if profitable and time shrinking
        "dte_urgent":         90,   # exit all losers
        "dte_hard_stop":      60,   # exit everything
    },
    "CORE": {
        "stop_loss":         -50,
        "profit_first":       50,
        "profit_second":     100,
        "profit_third":      200,
        "profit_final":      300,
        "delta_high":        0.90,  # roll when delta this high
        "delta_low":         0.25,  # check thesis if delta this low
        "dte_review":        270,
        "dte_urgent":         90,
        "dte_hard_stop":      60,
    },
    "TACTICAL": {
        "stop_loss":         -40,
        "profit_first":       50,
        "profit_final":      100,
        "delta_high":        0.90,
        "delta_low":         0.25,
        "dte_review":        270,
        "dte_urgent":         90,
        "dte_hard_stop":      60,
    },
}


def _t(role: str, key: str):
    """Get threshold for a role, falling back to CORE if role unknown."""
    return _THRESHOLDS.get(role, _THRESHOLDS["CORE"]).get(key)


# ---------------------------------------------------------------------------
# Pillar helpers
# ---------------------------------------------------------------------------

def _pnl_pct(entry_price: float, current_mid: float) -> float | None:
    if not (entry_price and current_mid and entry_price > 0):
        return None
    return round((current_mid - entry_price) / entry_price * 100, 1)


def _dte(expiration_date) -> int | None:
    if not expiration_date:
        return None
    if isinstance(expiration_date, str):
        try:
            expiration_date = date.fromisoformat(expiration_date)
        except Exception:
            return None
    return (expiration_date - date.today()).days


def _header(pos: dict, market: dict) -> str:
    """Shared email header block."""
    ticker   = pos.get("ticker", "?")
    strike   = pos.get("strike", "?")
    exp      = pos.get("expiration_date", "?")
    qty      = pos.get("quantity", "?")
    entry_p  = pos.get("entry_price", "?")
    mid      = market.get("mid")
    pnl      = market.get("pnl_pct")
    delta    = market.get("delta")
    dte_days = market.get("dte")
    iv_rank  = market.get("iv_rank")
    score    = market.get("thesis_score")
    ptype    = pos.get("position_type", "CORE")
    target   = pos.get("target_return", "")

    pnl_str  = f"{pnl:+.1f}%" if pnl is not None else "N/A"
    mid_str  = f"${mid:.2f}" if mid else "N/A"

    score_emoji = "✅" if score and score >= 70 else ("⚠️" if score and score >= 60 else "❌")

    return (
        f"{'─'*50}\n"
        f"POSITION\n"
        f"{'─'*50}\n"
        f"  Stock:        {ticker}\n"
        f"  Contract:     {exp} ${strike} Call  |  {qty} contracts\n"
        f"  Role:         {ptype}  ({target} target)\n"
        f"  Entry price:  ${entry_p}/share\n"
        f"  Current mid:  {mid_str}\n"
        f"  P&L:          {pnl_str}\n"
        f"{'─'*50}\n"
        f"MARKET SNAPSHOT\n"
        f"{'─'*50}\n"
        f"  Delta:        {delta if delta is not None else 'N/A'}\n"
        f"  DTE:          {dte_days if dte_days is not None else 'N/A'} days\n"
        f"  IV Rank:      {f'{iv_rank:.1f}%' if iv_rank is not None else 'N/A'}\n"
        f"  Thesis Score: {score if score is not None else 'N/A'} {score_emoji}\n"
        f"{'─'*50}\n"
    )


# ---------------------------------------------------------------------------
# Main evaluation function
# ---------------------------------------------------------------------------

def evaluate(position: dict, market: dict) -> list[Alert]:
    """
    Run all 5 pillars against a single position.

    Args:
        position: row from the positions table
        market:   live data dict with keys:
                    mid, delta, dte, iv_rank, thesis_score
                  (missing keys are handled gracefully)

    Returns: list of Alert objects (zero or more).
    """
    alerts: list[Alert] = []
    role = position.get("position_type", "CORE")

    entry_price = position.get("entry_price")
    ticker      = position.get("ticker", "?")
    exp_date    = position.get("expiration_date")

    mid         = market.get("mid")
    delta       = market.get("delta")
    dte_days    = market.get("dte") or _dte(exp_date)
    iv_rank     = market.get("iv_rank")
    thesis_score= market.get("thesis_score")

    pnl         = _pnl_pct(entry_price, mid)
    hdr         = lambda: _header(position, {**market, "pnl_pct": pnl})

    # -----------------------------------------------------------------------
    # PILLAR 1 — Fundamental: Is the thesis still intact?
    # -----------------------------------------------------------------------
    if thesis_score is not None:
        if thesis_score < 60:
            alerts.append(Alert(
                type="EXIT_THESIS", severity="RED",
                subject=f"🔴 EXIT — Thesis Broken: {ticker}  (Score: {thesis_score}/100)",
                body=(
                    hdr()
                    + "SIGNAL: EXIT — THESIS BROKEN\n"
                    + f"{'─'*50}\n"
                    + f"  Thesis score has fallen to {thesis_score}/100 (below 60 threshold).\n\n"
                    + "  The fundamental reason for holding this LEAPS is gone.\n"
                    + "  Even if the option is currently profitable, a broken thesis\n"
                    + "  means the asymmetric upside that justified this position\n"
                    + "  no longer exists. Exit now.\n\n"
                    + "  ACTION: Close the position at market open.\n"
                ),
                context=market,
            ))

    # -----------------------------------------------------------------------
    # PILLAR 2 — Greeks: Is the leverage still working?
    # -----------------------------------------------------------------------
    if delta is not None:
        delta_high = _t(role, "delta_high")
        delta_low  = _t(role, "delta_low")

        if role == "MOONSHOT":
            # For moonshots, a RISING delta means the option went deep ITM —
            # the lottery profile is gone and it now behaves like a stock.
            if delta > delta_high:
                action = (
                    "Your moonshot has gone deep ITM (delta > 0.50).\n"
                    "  It no longer has the asymmetric leverage profile of a lottery ticket.\n\n"
                    f"  If the stock continues rising: the remaining upside is limited.\n"
                    "  Consider: rolling to a higher, further-OTM strike to restore the 10x profile.\n"
                    "  Or: take profits if P&L > 200% and redeploy."
                )
                alerts.append(Alert(
                    type="ROLL_DELTA", severity="BLUE",
                    subject=f"🔵 ROLL — Moonshot Went Deep ITM: {ticker}  Delta: {delta:.2f}",
                    body=hdr() + "SIGNAL: ROLL — LEVERAGE PROFILE CHANGED\n" + f"{'─'*50}\n  {action}\n",
                    context=market,
                ))

            elif delta < delta_low:
                alerts.append(Alert(
                    type="DELTA_WARN", severity="AMBER",
                    subject=f"⚠️ WATCH — {ticker} Moonshot Going OTM  Delta: {delta:.2f}",
                    body=(
                        hdr()
                        + "SIGNAL: WATCH — DELTA VERY LOW\n"
                        + f"{'─'*50}\n"
                        + f"  Delta has dropped to {delta:.2f}. The option is becoming very far OTM.\n"
                        + "  This is normal for moonshots during stock pullbacks — but review\n"
                        + "  whether the thesis is still intact and whether enough time remains.\n"
                    ),
                    context=market,
                ))

        else:  # CORE or TACTICAL
            if delta > delta_high:
                if pnl is not None and pnl < 0:
                    # Underwater AND deep ITM — the worst outcome for a long option
                    alerts.append(Alert(
                        type="ROLL_DELTA", severity="RED",
                        subject=f"🔴 EXIT — Leverage Lost & Underwater: {ticker}  Delta: {delta:.2f}  P&L: {pnl:+.1f}%",
                        body=(
                            hdr()
                            + "SIGNAL: EXIT — LEVERAGE LOST AND UNDERWATER\n"
                            + f"{'─'*50}\n"
                            + f"  Delta is {delta:.2f} (above 0.90) AND the position is at {pnl:+.1f}%.\n"
                            + "  This is the worst combination for a long option:\n"
                            + "  you have stock-like risk but option-level decay, and you're losing.\n\n"
                            + "  Rolling this position amplifies the loss. Exit now.\n"
                            + "  Re-evaluate the thesis before re-entering.\n"
                        ),
                        context=market,
                    ))
                else:
                    alerts.append(Alert(
                        type="ROLL_DELTA", severity="BLUE",
                        subject=f"🔵 ROLL — Leverage Exhausted: {ticker}  Delta: {delta:.2f}",
                        body=(
                            hdr()
                            + "SIGNAL: ROLL — DELTA TOO HIGH\n"
                            + f"{'─'*50}\n"
                            + f"  Delta is {delta:.2f}. At this level you move 1-for-1 with the stock\n"
                            + "  but still carry option time decay. The leverage benefit is gone.\n\n"
                            + "  RECOMMENDED ACTION:\n"
                            + "  → Sell current contract\n"
                            + "  → Buy same expiry, higher strike (target delta ~0.70)\n"
                            + "  → This resets your leverage and reduces your capital at risk.\n\n"
                            + "  Note: Only roll if the debit required is < 20% of your\n"
                            + "  original premium paid. Otherwise, consider a full exit.\n"
                        ),
                        context=market,
                    ))

    # -----------------------------------------------------------------------
    # PILLAR 3 — Time: Is theta becoming a threat?
    # -----------------------------------------------------------------------
    if dte_days is not None:
        hard_stop  = _t(role, "dte_hard_stop")   # 60
        urgent     = _t(role, "dte_urgent")       # 90
        review     = _t(role, "dte_review")       # 270

        if dte_days < hard_stop:
            alerts.append(Alert(
                type="EXIT_TIME_URGENT", severity="RED",
                subject=f"🔴 EMERGENCY EXIT — DTE Critical: {ticker}  {dte_days} days left",
                body=(
                    hdr()
                    + "SIGNAL: EMERGENCY EXIT — TIME CRITICAL\n"
                    + f"{'─'*50}\n"
                    + f"  Only {dte_days} days remain to expiration.\n"
                    + "  Theta decay in the final 60 days is exponential and devastating\n"
                    + "  for long options. Exit immediately regardless of P&L.\n\n"
                    + "  If you still believe in the thesis, wait for the position to close,\n"
                    + "  then re-enter with fresh LEAPS (>= 18 months out).\n\n"
                    + "  ACTION: Close position at market open. No exceptions.\n"
                ),
                context=market,
            ))

        elif dte_days < urgent:
            if pnl is not None and pnl < 0:
                alerts.append(Alert(
                    type="EXIT_TIME_URGENT", severity="RED",
                    subject=f"🔴 EXIT — Losing & Time Running Out: {ticker}  DTE: {dte_days}  P&L: {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: EXIT — THETA DANGER ZONE\n"
                        + f"{'─'*50}\n"
                        + f"  {dte_days} days left and the position is at {pnl:+.1f}%.\n"
                        + "  Inside 90 days, theta acceleration will compound losses rapidly.\n"
                        + "  There is not enough time left for a meaningful recovery.\n\n"
                        + "  ACTION: Exit to preserve remaining capital.\n"
                    ),
                    context=market,
                ))
            else:
                # Profitable with < 90 days — take the gain, don't roll inside 3 months
                alerts.append(Alert(
                    type="EXIT_TIME_URGENT", severity="RED",
                    subject=f"🔴 TAKE PROFIT — DTE < 90 Days: {ticker}  P&L: {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: TAKE PROFIT — DTE < 90 DAYS\n"
                        + f"{'─'*50}\n"
                        + f"  {dte_days} days left with a {pnl:+.1f}% gain.\n"
                        + "  Inside 90 days, rolling is rarely cost-effective because the\n"
                        + "  new contract will immediately experience the same theta decay.\n\n"
                        + "  ACTION: Take the profit. Close the position now.\n"
                        + "  If thesis is still strong, re-enter with fresh LEAPS.\n"
                    ),
                    context=market,
                ))

        elif dte_days < review:
            if pnl is not None and pnl > 0:
                # Profitable + 90-270 days → ideal roll window
                alerts.append(Alert(
                    type="ROLL_TIME", severity="BLUE",
                    subject=f"🔵 ROLL — Time Running Down: {ticker}  DTE: {dte_days}  P&L: {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: ROLL — TIME MANAGEMENT\n"
                        + f"{'─'*50}\n"
                        + f"  {dte_days} days remain and you are profitable at {pnl:+.1f}%.\n"
                        + "  This is the ideal window to roll: you have time to act without\n"
                        + "  urgency, and rolling while profitable locks in intrinsic gains.\n\n"
                        + "  RECOMMENDED ACTION:\n"
                        + "  → Sell current contract\n"
                        + "  → Buy same strike, + 12 months expiration\n"
                        + "  → This extends your runway and preserves the trade thesis.\n\n"
                        + "  ROLL CHECK: Only proceed if the roll debit is < 20% of your\n"
                        + "  original premium paid. Otherwise exit and redeploy.\n"
                    ),
                    context=market,
                ))
            elif pnl is not None and pnl < -20:
                alerts.append(Alert(
                    type="EXIT_TIME_WARNING", severity="AMBER",
                    subject=f"⚠️ WATCH — DTE Shrinking & Losing: {ticker}  DTE: {dte_days}  P&L: {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: WATCH — TIME + LOSS WARNING\n"
                        + f"{'─'*50}\n"
                        + f"  {dte_days} days left and position is at {pnl:+.1f}%.\n"
                        + "  Do not roll a losing position — it amplifies losses.\n"
                        + "  Monitor closely. If the thesis is still intact, hold.\n"
                        + "  If thesis is weakening, plan your exit before DTE < 90.\n"
                    ),
                    context=market,
                ))

    # -----------------------------------------------------------------------
    # PILLAR 4 — Profit: Are you capturing the asymmetric return?
    # -----------------------------------------------------------------------
    if pnl is not None:
        stop_loss = _t(role, "stop_loss")

        if pnl <= stop_loss:
            alerts.append(Alert(
                type="EXIT_STOP", severity="RED",
                subject=f"🔴 STOP LOSS HIT: {ticker}  P&L: {pnl:+.1f}%  ({role})",
                body=(
                    hdr()
                    + "SIGNAL: STOP LOSS\n"
                    + f"{'─'*50}\n"
                    + f"  Position is at {pnl:+.1f}% (stop loss: {stop_loss}%).\n"
                    + "  Continued holding risks further capital destruction.\n\n"
                    + "  ACTION: Exit the position. Preserve remaining capital.\n"
                    + "  You can re-enter later if the thesis recovers.\n"
                ),
                context=market,
            ))

        elif role == "MOONSHOT":
            if pnl >= 900:
                alerts.append(Alert(
                    type="PROFIT_900", severity="RED",
                    subject=f"🔴 SELL — Near 10x: {ticker}  P&L: {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: 10x TARGET REACHED — SELL\n"
                        + f"{'─'*50}\n"
                        + f"  Your moonshot is up {pnl:+.1f}% — near the 10x target!\n"
                        + (f"  IV Rank is {iv_rank:.1f}% — market is hyping this stock now.\n" if iv_rank else "")
                        + "  This is your exit window. Sell to someone else's FOMO.\n\n"
                        + "  ACTION: Exit the full position.\n"
                    ),
                    context=market,
                ))
            elif pnl >= 500:
                alerts.append(Alert(
                    type="PROFIT_500", severity="BLUE",
                    subject=f"🔵 SCALE OUT — {ticker} Up 6x: {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: SCALE OUT — UP 6x\n"
                        + f"{'─'*50}\n"
                        + f"  Your moonshot is up {pnl:+.1f}%. This is a 6x return.\n"
                        + "  Recommendation: Sell 50% of position now.\n"
                        + "  Hold the remaining 50% for the 10x target.\n"
                    ),
                    context=market,
                ))
            elif pnl >= 200:
                alerts.append(Alert(
                    type="PROFIT_200", severity="AMBER",
                    subject=f"⚠️ SCALE — {ticker} Moonshot Up 3x: {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: SCALE OUT — UP 3x (FIRST TRIM)\n"
                        + f"{'─'*50}\n"
                        + f"  Your moonshot is up {pnl:+.1f}% — a 3x return.\n"
                        + "  Recommendation: Sell 25% to recover part of your initial cost.\n"
                        + "  Let the remaining 75% ride toward the 10x target.\n\n"
                        + "  This locks in profit while keeping most exposure intact.\n"
                    ),
                    context=market,
                ))

        elif role == "CORE":
            if pnl >= 300:
                iv_note = f"  IV Rank is {iv_rank:.1f}% — market FOMO is your buyer.\n" if iv_rank and iv_rank > 65 else ""
                alerts.append(Alert(
                    type="PROFIT_300", severity="RED" if (iv_rank and iv_rank > 65) else "BLUE",
                    subject=f"{'🔴 SELL ALL' if (iv_rank and iv_rank > 65) else '🔵 CONSIDER EXIT'} — {ticker} Core Up {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: 4x RETURN — TARGET ZONE\n"
                        + f"{'─'*50}\n"
                        + f"  Core position is up {pnl:+.1f}%.\n"
                        + iv_note
                        + "  This is beyond your 3-5x target range.\n"
                        + "  Recommendation: Exit the full position.\n"
                    ),
                    context=market,
                ))
            elif pnl >= 200:
                alerts.append(Alert(
                    type="PROFIT_200", severity="BLUE",
                    subject=f"🔵 TRIM HARD — {ticker} Core Up {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: 3x RETURN — AGGRESSIVE TRIM\n"
                        + f"{'─'*50}\n"
                        + f"  Core position is up {pnl:+.1f}% — a 3x return.\n"
                        + "  Recommendation: Sell 75% of position. Trail the last 25%\n"
                        + "  toward the 5x target.\n"
                    ),
                    context=market,
                ))
            elif pnl >= 100:
                alerts.append(Alert(
                    type="PROFIT_100", severity="AMBER",
                    subject=f"⚠️ SCALE — {ticker} Core Up {pnl:+.1f}%  (2x)",
                    body=(
                        hdr()
                        + "SIGNAL: 2x RETURN — SCALE OUT\n"
                        + f"{'─'*50}\n"
                        + f"  Core position is up {pnl:+.1f}%.\n"
                        + "  Recommendation: Sell another 25% (you should have\n"
                        + "  already sold 25-33% at the 50% mark).\n"
                        + "  Let the remaining position run toward 3-5x.\n"
                    ),
                    context=market,
                ))
            elif pnl >= 50:
                alerts.append(Alert(
                    type="PROFIT_50", severity="AMBER",
                    subject=f"⚠️ FIRST SCALE — {ticker} Core Up {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: FIRST PROFIT TARGET HIT\n"
                        + f"{'─'*50}\n"
                        + f"  Core position is up {pnl:+.1f}%.\n"
                        + "  Recommendation: Sell 25-33% of position to lock in gains.\n"
                        + "  Keep the majority invested — your target is 3-5x.\n"
                    ),
                    context=market,
                ))

        elif role == "TACTICAL":
            if pnl >= 100:
                alerts.append(Alert(
                    type="PROFIT_100", severity="RED",
                    subject=f"🔴 EXIT — Tactical Target Hit: {ticker}  {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: TACTICAL EXIT TARGET\n"
                        + f"{'─'*50}\n"
                        + f"  Tactical position is up {pnl:+.1f}%.\n"
                        + "  Recommendation: Exit the full position. A 2x on a tactical\n"
                        + "  position is the primary target. Don't get greedy.\n"
                    ),
                    context=market,
                ))
            elif pnl >= 50:
                alerts.append(Alert(
                    type="PROFIT_50", severity="AMBER",
                    subject=f"⚠️ SCALE — Tactical: {ticker}  Up {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: SCALE TACTICAL POSITION\n"
                        + f"{'─'*50}\n"
                        + f"  Tactical position is up {pnl:+.1f}%.\n"
                        + "  Recommendation: Take 50% off the table now.\n"
                    ),
                    context=market,
                ))

    return alerts


# ---------------------------------------------------------------------------
# Entry score (Pillar 5 — Watchlist only)
# ---------------------------------------------------------------------------

def evaluate_entry(position: dict, stock_data: dict, iv_rank: float | None) -> Alert | None:
    """
    Compute a composite entry readiness score for a WATCHLIST position.
    Returns an Alert if conditions are sufficiently aligned, else None.

    stock_data: output of technical.get_price_and_range()
    iv_rank:    float (0-100) or None
    """
    ticker       = position.get("ticker", "?")
    weekly_rsi   = stock_data.get("weekly_rsi")
    pct_from_low = stock_data.get("pct_from_low")
    price        = stock_data.get("price")

    score = 0
    reasons = []

    # RSI signal (weekly chart)
    if weekly_rsi is not None:
        if weekly_rsi < 30:
            score += 40
            reasons.append(f"Weekly RSI = {weekly_rsi:.1f} (oversold — strong buy signal)")
        elif weekly_rsi < 40:
            score += 20
            reasons.append(f"Weekly RSI = {weekly_rsi:.1f} (approaching oversold)")

    # IV Rank signal (cheap options)
    if iv_rank is not None:
        if iv_rank < 25:
            score += 30
            reasons.append(f"IV Rank = {iv_rank:.1f}% (options are cheap — good to buy premium)")
        elif iv_rank < 35:
            score += 15
            reasons.append(f"IV Rank = {iv_rank:.1f}% (options reasonably priced)")

    # Price vs 52-week range
    if pct_from_low is not None:
        if pct_from_low < 0.15:
            score += 30
            reasons.append(f"Stock is near 52-week low ({pct_from_low*100:.0f}% from low)")
        elif pct_from_low < 0.30:
            score += 15
            reasons.append(f"Stock is in lower third of 52-week range ({pct_from_low*100:.0f}% from low)")

    if score < 40:
        return None  # not close enough to optimal entry

    severity = "GREEN" if score >= 60 else "AMBER"
    action   = "BUY LEAPS NOW" if score >= 60 else "WATCH — ENTRY IMPROVING"

    body = (
        f"{'─'*50}\n"
        f"WATCHLIST TICKER:  {ticker}\n"
        f"Current Price:     ${price:.2f}\n" if price else ""
        f"Entry Score:       {score}/100\n"
        f"{'─'*50}\n"
        f"SIGNALS\n"
        + "\n".join(f"  ✓ {r}" for r in reasons)
        + f"\n{'─'*50}\n"
        f"RECOMMENDATION:  {action}\n\n"
        + (
            "  When buying, target:\n"
            "  • Delta:    0.70-0.75  (for CORE)  |  0.10-0.20  (for MOONSHOT)\n"
            "  • Strike:   20-25% OTM from current price\n"
            "  • Expiry:   Furthest available >= 18 months\n"
            if score >= 60 else
            "  Keep monitoring. Alert again when score reaches 60+.\n"
        )
    )

    emoji = "🟢" if score >= 60 else "🟡"
    return Alert(
        type     = "ENTRY_SIGNAL" if score >= 60 else "ENTRY_WATCH",
        severity = severity,
        subject  = f"{emoji} {action}: {ticker}  Entry Score {score}/100",
        body     = body,
        context  = {"entry_score": score, "weekly_rsi": weekly_rsi,
                    "iv_rank": iv_rank, "pct_from_low": pct_from_low},
    )
