"""
The 5-Pillar Exit Engine — Unified 5-10x Strategy.

All positions target 5-10x asymmetric returns. No role categories.

Strategy philosophy:
  • Never exit 100% early — you bought LEAPS for the asymmetric tail.
  • Graduated scaling locks in gains while keeping core exposure running.
  • At 3-5x (the first milestone), trim AND roll: sell some contracts to lock
    in gains, roll the remainder forward in time to stay in the trade.
  • Inside 90 days DTE, rolling is rarely cost-effective — just take the gain.
  • Hard stop at -60%: LEAPS can go from -60% to 0, but rarely from -60% to 10x.

Each alert has:
  type     — string code (e.g. ROLL_DELTA, PROFIT_300, EXIT_THESIS)
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
# Unified thresholds — single set for all positions
# ---------------------------------------------------------------------------

_T = {
    # Risk management
    "stop_loss":      -60,   # -60% max pain. Below here recovery is statistically rare.

    # Profit scaling ladder (graduated — never sell all at once)
    "profit_100":    100,   # 2x: recover initial cost basis. Sell ~20%.
    "profit_300":    300,   # 4x: first major milestone. Trim 30% + consider rolling rest.
    "profit_600":    600,   # 7x: deep in the money on the trade. Trim hard, trail rest.
    "profit_900":    900,   # 10x: target hit. Exit everything remaining.

    # Greeks
    "delta_high":   0.90,   # above this = 1:1 stock exposure, option decay still hurts
    "delta_low":    0.10,   # below this = option is near-worthless, reassess

    # Time (DTE)
    "dte_review":   270,    # 9 months: roll window opens when profitable
    "dte_urgent":    90,    # 3 months: exit losers; profitable = take gains / roll now
    "dte_hard_stop": 60,    # 2 months: emergency exit regardless of P&L
}


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

    pnl_str  = f"{pnl:+.1f}%" if pnl is not None else "N/A"
    mid_str  = f"${mid:.2f}" if mid else "N/A"
    cost_basis = ""
    try:
        cb = float(entry_p) * float(qty) * 100
        cost_basis = f"  Cost Basis:   ${cb:,.0f}\n"
    except Exception:
        pass

    score_emoji = "✅" if score and score >= 70 else ("⚠️" if score and score >= 60 else "❌")

    return (
        f"{'─'*50}\n"
        f"POSITION\n"
        f"{'─'*50}\n"
        f"  Stock:        {ticker}\n"
        f"  Contract:     {exp} ${strike} Call  |  {qty} contracts\n"
        f"  Avg. Price:   ${entry_p}/share\n"
        + cost_basis +
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

    entry_price  = position.get("entry_price")
    ticker       = position.get("ticker", "?")
    exp_date     = position.get("expiration_date")
    qty          = position.get("quantity", 1)

    mid          = market.get("mid")
    delta        = market.get("delta")
    dte_days     = market.get("dte") or _dte(exp_date)
    iv_rank      = market.get("iv_rank")
    thesis_score = market.get("thesis_score")

    pnl  = _pnl_pct(entry_price, mid)
    hdr  = lambda: _header(position, {**market, "pnl_pct": pnl})

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
                    + f"  Thesis score has fallen to {thesis_score}/100 (threshold: 60).\n\n"
                    + "  The fundamental reason for holding this LEAPS is gone.\n"
                    + "  A broken thesis means the asymmetric upside that justified\n"
                    + "  this position no longer exists. P&L at entry time is irrelevant —\n"
                    + "  exit before the market prices in the deterioration fully.\n\n"
                    + "  ACTION: Close the position at market open.\n"
                ),
                context=market,
            ))

    # -----------------------------------------------------------------------
    # PILLAR 2 — Greeks: Is the leverage still working?
    # -----------------------------------------------------------------------
    if delta is not None:
        if delta > _T["delta_high"]:
            if pnl is not None and pnl < 0:
                # Deep ITM AND underwater — worst of both worlds
                alerts.append(Alert(
                    type="ROLL_DELTA", severity="RED",
                    subject=f"🔴 EXIT — Leverage Lost & Underwater: {ticker}  Δ {delta:.2f}  P&L: {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: EXIT — LEVERAGE LOST AND UNDERWATER\n"
                        + f"{'─'*50}\n"
                        + f"  Delta is {delta:.2f} (above 0.90) AND the position is at {pnl:+.1f}%.\n"
                        + "  This is the worst combination for a long option:\n"
                        + "  • You have stock-like downside exposure\n"
                        + "  • You're still paying option-level time decay\n"
                        + "  • You're already losing money\n\n"
                        + "  Rolling this position locks in losses AND starts the decay\n"
                        + "  clock again on the new contract. Do not roll.\n\n"
                        + "  ACTION: Exit now. Re-evaluate thesis before re-entering.\n"
                    ),
                    context=market,
                ))
            else:
                # Deep ITM but profitable — roll to restore leverage
                alerts.append(Alert(
                    type="ROLL_DELTA", severity="BLUE",
                    subject=f"🔵 ROLL — Leverage Exhausted: {ticker}  Δ {delta:.2f}  P&L: {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: ROLL — DELTA TOO HIGH\n"
                        + f"{'─'*50}\n"
                        + f"  Delta is {delta:.2f}. Above 0.90 you move 1-for-1 with the stock\n"
                        + "  but still carry option time decay. The leverage is gone.\n\n"
                        + "  RECOMMENDED ACTION:\n"
                        + "  → Sell current contract (lock in gains)\n"
                        + "  → Buy same expiry, higher strike (target Δ ~0.70)\n"
                        + "  → This resets leverage and reduces capital at risk\n\n"
                        + "  ROLL CHECK: Only roll if debit required is < 20% of your\n"
                        + "  original premium paid. Otherwise take the full profit.\n"
                    ),
                    context=market,
                ))

        elif delta < _T["delta_low"]:
            alerts.append(Alert(
                type="DELTA_WARN", severity="AMBER",
                subject=f"⚠️ WATCH — {ticker} Far OTM  Δ {delta:.2f}",
                body=(
                    hdr()
                    + "SIGNAL: WATCH — DELTA VERY LOW\n"
                    + f"{'─'*50}\n"
                    + f"  Delta has dropped to {delta:.2f}. The option is deep out-of-the-money\n"
                    + "  and approaching near-worthless territory.\n\n"
                    + "  Review:\n"
                    + "  • Is the thesis still intact? (thesis score above)\n"
                    + "  • How much DTE is left? (if < 180d and deep OTM, consider exiting)\n"
                    + "  • Would the premium be better deployed in a fresh position?\n"
                ),
                context=market,
            ))

    # -----------------------------------------------------------------------
    # PILLAR 3 — Time: Is theta becoming a threat?
    # -----------------------------------------------------------------------
    if dte_days is not None:
        if dte_days < _T["dte_hard_stop"]:
            alerts.append(Alert(
                type="EXIT_TIME_URGENT", severity="RED",
                subject=f"🔴 EMERGENCY EXIT — DTE Critical: {ticker}  {dte_days} days left",
                body=(
                    hdr()
                    + "SIGNAL: EMERGENCY EXIT — TIME CRITICAL\n"
                    + f"{'─'*50}\n"
                    + f"  Only {dte_days} days remain to expiration.\n"
                    + "  Theta decay in the final 60 days is exponential and destroys\n"
                    + "  premium regardless of underlying movement.\n\n"
                    + "  ACTION: Exit immediately regardless of P&L. No exceptions.\n"
                    + "  If thesis is still valid, re-enter with new LEAPS (>= 18 months).\n"
                ),
                context=market,
            ))

        elif dte_days < _T["dte_urgent"]:
            if pnl is not None and pnl < 0:
                alerts.append(Alert(
                    type="EXIT_TIME_URGENT", severity="RED",
                    subject=f"🔴 EXIT — Losing & Time Running Out: {ticker}  DTE: {dte_days}  P&L: {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: EXIT — THETA DANGER ZONE\n"
                        + f"{'─'*50}\n"
                        + f"  {dte_days} days left and the position is at {pnl:+.1f}%.\n"
                        + "  Inside 90 days, theta acceleration compounds losses rapidly.\n"
                        + "  There is not enough time left for a meaningful recovery.\n\n"
                        + "  Do NOT roll a losing position inside 90 days — it amplifies losses.\n\n"
                        + "  ACTION: Exit to preserve remaining capital.\n"
                    ),
                    context=market,
                ))
            elif pnl is not None:
                # Profitable with < 90 days — take the gain, don't roll inside 3 months
                alerts.append(Alert(
                    type="EXIT_TIME_URGENT", severity="RED",
                    subject=f"🔴 TAKE PROFIT — DTE < 90 Days: {ticker}  P&L: {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: TAKE PROFIT — DTE < 90 DAYS\n"
                        + f"{'─'*50}\n"
                        + f"  {dte_days} days left with a {pnl:+.1f}% gain.\n"
                        + "  Inside 90 days, rolling is rarely cost-effective: the new\n"
                        + "  contract starts decaying at the same accelerated rate.\n\n"
                        + "  ACTION: Take the full profit. Close now.\n"
                        + "  If thesis is still strong, re-enter fresh LEAPS (>= 18 months).\n"
                    ),
                    context=market,
                ))

        elif dte_days < _T["dte_review"]:
            # 90-270 days — the roll window
            if pnl is not None and pnl > 0:
                alerts.append(Alert(
                    type="ROLL_TIME", severity="BLUE",
                    subject=f"🔵 ROLL — Time Running Down: {ticker}  DTE: {dte_days}  P&L: {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: ROLL — TIME MANAGEMENT\n"
                        + f"{'─'*50}\n"
                        + f"  {dte_days} days remain and you are profitable at {pnl:+.1f}%.\n"
                        + "  This is the ideal window to roll: enough time to act without\n"
                        + "  urgency, and rolling while profitable locks in intrinsic gains.\n\n"
                        + "  RECOMMENDED ACTION:\n"
                        + "  → Sell current contract\n"
                        + "  → Buy same strike, + 12 months expiration\n"
                        + "  → Extends your runway toward the 5-10x target\n\n"
                        + "  ROLL CHECK: Only proceed if the roll debit is < 20% of your\n"
                        + "  original Avg. Price. If the debit is higher, exit and redeploy.\n"
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
                        + "  Do NOT roll a losing position — it amplifies losses.\n"
                        + "  Monitor closely. If thesis is intact, hold and let it recover.\n"
                        + "  If thesis is weakening, plan your exit before DTE hits 90.\n"
                    ),
                    context=market,
                ))

    # -----------------------------------------------------------------------
    # PILLAR 4 — Profit: Are you capturing the asymmetric return?
    # -----------------------------------------------------------------------
    if pnl is not None:

        if pnl <= _T["stop_loss"]:
            alerts.append(Alert(
                type="EXIT_STOP", severity="RED",
                subject=f"🔴 STOP LOSS HIT: {ticker}  P&L: {pnl:+.1f}%",
                body=(
                    hdr()
                    + "SIGNAL: STOP LOSS\n"
                    + f"{'─'*50}\n"
                    + f"  Position is at {pnl:+.1f}% (stop: {_T['stop_loss']}%).\n"
                    + "  Below -60%, statistical recovery to breakeven is very unlikely\n"
                    + "  within a reasonable timeframe.\n\n"
                    + "  ACTION: Exit the position. Preserve remaining capital.\n"
                    + "  Thesis may still be valid — re-enter fresh LEAPS if it is.\n"
                ),
                context=market,
            ))

        elif pnl >= _T["profit_900"]:
            alerts.append(Alert(
                type="PROFIT_900", severity="RED",
                subject=f"🔴 10x TARGET HIT — EXIT ALL: {ticker}  P&L: {pnl:+.1f}%",
                body=(
                    hdr()
                    + "SIGNAL: 10x TARGET REACHED — FULL EXIT\n"
                    + f"{'─'*50}\n"
                    + f"  Your position is up {pnl:+.1f}% — you've hit the 10x target!\n"
                    + (f"  IV Rank is {iv_rank:.1f}% — market euphoria is at your back.\n" if iv_rank else "")
                    + "\n"
                    + "  This is your full exit. You are selling to someone else's FOMO.\n"
                    + "  Do not wait for more — the final move from here has lower\n"
                    + "  probability and you've already captured the asymmetric return.\n\n"
                    + "  ACTION: Exit the entire remaining position.\n"
                ),
                context=market,
            ))

        elif pnl >= _T["profit_600"]:
            roll_note = (
                "\n  TIME NOTE: You still have enough DTE to roll the trailing\n"
                "  25% to a further-dated contract if you want to stay in the trade.\n"
                if dte_days and dte_days > _T["dte_urgent"] else
                "\n  TIME NOTE: DTE is short — exit the full position rather than rolling.\n"
            )
            alerts.append(Alert(
                type="PROFIT_600", severity="BLUE",
                subject=f"🔵 TRIM HARD — {ticker} Up 7x: {pnl:+.1f}%",
                body=(
                    hdr()
                    + "SIGNAL: TRIM HARD — UP 7x\n"
                    + f"{'─'*50}\n"
                    + f"  Position is up {pnl:+.1f}% — a 7x return. Outstanding.\n\n"
                    + "  RECOMMENDED ACTION:\n"
                    + "  → Sell 75% of position now to lock in the 7x gains\n"
                    + "  → Keep 25% trailing toward the 10x target\n"
                    + "  → Set a mental stop on the trailing 25%: if it gives back\n"
                    + "    50% of gains, exit the remainder\n"
                    + roll_note
                ),
                context=market,
            ))

        elif pnl >= _T["profit_300"]:
            # 4x milestone — the 3-5x zone the user specifically mentioned
            # Trim AND recommend rolling the remainder for maximum return capture
            if dte_days and dte_days > _T["dte_urgent"]:
                roll_action = (
                    "  → For the remaining 50%:\n"
                    f"    DTE = {dte_days} days. If < 270 days remain, consider rolling:\n"
                    "    Sell current contract, buy same strike + 12 months.\n"
                    "    This keeps you in the trade at reduced cost basis while\n"
                    "    targeting the 7-10x level.\n"
                    "    Roll check: debit must be < 20% of original Avg. Price.\n"
                )
            else:
                roll_action = (
                    "  → For the remaining 50%:\n"
                    f"    DTE = {dte_days} days — too short to roll cost-effectively.\n"
                    "    Exit the remainder and redeploy into fresh LEAPS if\n"
                    "    thesis is still intact and you want more upside.\n"
                )

            alerts.append(Alert(
                type="PROFIT_300", severity="AMBER",
                subject=f"⚠️ TRIM & ROLL — {ticker} Up 4x: {pnl:+.1f}%",
                body=(
                    hdr()
                    + "SIGNAL: TRIM & CONSIDER ROLLING — UP 4x\n"
                    + f"{'─'*50}\n"
                    + f"  Position is up {pnl:+.1f}% — a 4x return. You've hit your\n"
                    + "  first major profit milestone (3-5x target zone).\n\n"
                    + "  Strategy: Take serious profits while keeping upside exposure.\n\n"
                    + "  RECOMMENDED ACTION:\n"
                    + "  → Sell 50% of contracts now (lock in the 4x on half)\n"
                    + roll_action
                    + "\n"
                    + "  This approach:\n"
                    + "  ✓ Secures substantial gains that cannot be taken away\n"
                    + "  ✓ Keeps exposure to the 7-10x move if thesis plays out\n"
                    + "  ✓ Reduces capital at risk significantly\n"
                ),
                context=market,
            ))

        elif pnl >= _T["profit_100"]:
            alerts.append(Alert(
                type="PROFIT_100", severity="AMBER",
                subject=f"⚠️ FIRST TRIM — {ticker} Up 2x: {pnl:+.1f}%",
                body=(
                    hdr()
                    + "SIGNAL: FIRST PROFIT MILESTONE — UP 2x\n"
                    + f"{'─'*50}\n"
                    + f"  Position is up {pnl:+.1f}% — you've doubled your money.\n\n"
                    + "  RECOMMENDED ACTION:\n"
                    + "  → Sell 20-25% of position\n"
                    + "  → This recovers your initial cost basis on those contracts\n"
                    + "  → The majority stays on — your target is 5-10x, not 2x\n\n"
                    + "  Do NOT sell the full position here. Selling at 2x is the\n"
                    + "  most common mistake in LEAPS trading — it sacrifices the\n"
                    + "  asymmetric return that justifies buying options in the first place.\n"
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
        + (f"Current Price:     ${price:.2f}\n" if price else "")
        + f"Entry Score:       {score}/100\n"
        f"{'─'*50}\n"
        f"SIGNALS\n"
        + "\n".join(f"  ✓ {r}" for r in reasons)
        + f"\n{'─'*50}\n"
        f"RECOMMENDATION:  {action}\n\n"
        + (
            "  When buying, target:\n"
            "  • Delta:   0.25-0.40  (moderate OTM — balanced leverage / cost)\n"
            "  • Strike:  15-25% OTM from current price\n"
            "  • Expiry:  Furthest available >= 18 months\n"
            "  • Target return: 5-10x from this entry\n"
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
