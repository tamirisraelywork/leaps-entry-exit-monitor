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

IV Rank is woven into EVERY alert because it changes the urgency of everything:
  > 70%  Options are expensive — SELL NOW, buyers are paying up (FOMO pricing)
  50-70% IV elevated — good timing to execute exits and trims
  25-50% Neutral range — other pillars drive the decision
  < 25%  Options are CHEAP — great time to buy/roll new contracts;
         if selling, consider waiting for IV expansion to maximize proceeds

Each alert has:
  type     — string code (e.g. ROLL_DELTA, PROFIT_300, EXIT_THESIS)
  severity — RED / AMBER / BLUE / GREEN
  subject  — short email subject
  body     — full formatted email body
  context  — dict of the market data snapshot that triggered it
"""

from __future__ import annotations

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
    "profit_300":    300,   # 4x: first major milestone. Trim 50% + consider rolling rest.
    "profit_600":    600,   # 7x: deep in the money on the trade. Trim 75%, trail rest.
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
# IV Rank helpers  (the most underused edge in LEAPS timing)
# ---------------------------------------------------------------------------

def _iv_sell_context(iv_rank: float | None) -> str:
    """
    Context block for SELL / EXIT / TRIM alerts.
    High IV = get out NOW, you're selling expensive paper.
    Low IV  = consider waiting for IV expansion before selling.
    """
    if iv_rank is None:
        return "  IV Rank:  N/A (run full check for IV context)\n"
    if iv_rank >= 70:
        return (
            f"  IV Rank:  {iv_rank:.0f}%  ★ PRIME TIME TO SELL ★\n"
            f"  Options are in the top 30% of their historical volatility range.\n"
            f"  Buyers are paying a fear/FOMO premium on top of intrinsic value.\n"
            f"  This premium evaporates when IV normalizes — exit NOW to capture it.\n"
        )
    elif iv_rank >= 50:
        return (
            f"  IV Rank:  {iv_rank:.0f}%  — IV elevated, good timing to sell.\n"
            f"  You're getting above-average premium from buyers. Execute the trade.\n"
        )
    elif iv_rank >= 25:
        return (
            f"  IV Rank:  {iv_rank:.0f}%  — neutral range. Timing is acceptable.\n"
            f"  Not a particularly good or bad time from an IV perspective.\n"
        )
    else:
        return (
            f"  IV Rank:  {iv_rank:.0f}%  — OPTIONS ARE CHEAP right now.\n"
            f"  Buyers are NOT paying up — your extrinsic value is compressed.\n"
            f"  If DTE allows, consider waiting for IV to rise before selling.\n"
            f"  A spike to IV Rank 50%+ can add 10-30% to your option value.\n"
        )


def _iv_roll_context(iv_rank: float | None) -> str:
    """
    Context block for ROLL alerts.
    Rolling = sell old contract (want high IV) + buy new contract (want low IV).
    These are in tension. Net guidance:
      - High IV: exit current position, re-enter later when IV drops
      - Low IV:  ideal time to roll (buying the new contract cheaply)
    """
    if iv_rank is None:
        return "  IV Rank:  N/A (run full check for IV timing context)\n"
    if iv_rank >= 60:
        return (
            f"  IV Rank:  {iv_rank:.0f}%  — ROLLING IS EXPENSIVE RIGHT NOW.\n"
            f"  The new contract you'd be buying is priced at inflated IV.\n"
            f"  Better path: SELL the current position (collect the high IV premium)\n"
            f"  and wait for IV to drop below 35% before buying a new LEAPS.\n"
            f"  This gets you the best of both: high IV exit + low IV entry.\n"
        )
    elif iv_rank >= 35:
        return (
            f"  IV Rank:  {iv_rank:.0f}%  — acceptable rolling conditions.\n"
            f"  Not ideal but not expensive either. Proceed if the roll debit\n"
            f"  is within 20% of your original Avg. Price.\n"
        )
    else:
        return (
            f"  IV Rank:  {iv_rank:.0f}%  ★ IDEAL TIME TO ROLL ★\n"
            f"  Options are cheap — the new contract you're buying is underpriced.\n"
            f"  Rolling now locks in cheap forward exposure. Execute promptly.\n"
        )


def _iv_entry_context(iv_rank: float | None) -> str:
    """IV context specifically for entry/re-entry guidance."""
    if iv_rank is None:
        return ""
    if iv_rank >= 50:
        return (
            f"  ⚠️  IV Rank = {iv_rank:.0f}% — options are EXPENSIVE to buy right now.\n"
            f"  Wait for IV to drop below 35% before entering a new LEAPS position.\n"
            f"  Buying high-IV options means you pay extra for time premium that will\n"
            f"  likely crush your position even if the stock moves in your favor.\n"
        )
    elif iv_rank >= 25:
        return f"  IV Rank = {iv_rank:.0f}% — acceptable entry timing (neutral IV).\n"
    else:
        return (
            f"  IV Rank = {iv_rank:.0f}% ★ — GREAT TIME TO BUY options.\n"
            f"  You're paying minimal extrinsic value. Even if the stock stalls,\n"
            f"  an IV expansion later will boost your option value independently.\n"
        )


# ---------------------------------------------------------------------------
# Other helpers
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
    qty_total_h  = pos.get("quantity", "?")
    qty_trimmed_h = int(pos.get("quantity_trimmed") or 0)
    qty_remaining_h = (int(qty_total_h) - qty_trimmed_h) if str(qty_total_h).isdigit() else qty_total_h
    entry_p  = pos.get("entry_price", "?")
    mid      = market.get("mid")
    pnl      = market.get("pnl_pct")
    delta    = market.get("delta")
    dte_days = market.get("dte")
    iv_rank  = market.get("iv_rank")
    score    = market.get("thesis_score")

    pnl_str = f"{pnl:+.1f}%" if pnl is not None else "N/A"
    mid_str = f"${mid:.2f}" if mid else "N/A"
    iv_str  = f"{iv_rank:.0f}%" if iv_rank is not None else "N/A"
    score_emoji = "✅" if score and score >= 70 else ("⚠️" if score and score >= 60 else "❌")

    # Cost-basis / trim summary
    cost_line = ""
    try:
        cb   = float(entry_p) * float(qty_total_h) * 100
        prec = float(pos.get("proceeds_from_trims") or 0)
        if qty_trimmed_h > 0:
            recovery_pct = (prec / cb * 100) if cb > 0 else 0
            house_tag = " ★ HOUSE MONEY" if recovery_pct >= 100 else ""
            cost_line = (
                f"  Cost Basis:   ${cb:,.0f} original  "
                f"({qty_trimmed_h} trimmed, ${prec:,.0f} recovered = {recovery_pct:.0f}%{house_tag})\n"
            )
        else:
            cost_line = f"  Cost Basis:   ${cb:,.0f}\n"
    except Exception:
        pass

    # Thesis trend: entry score vs current
    entry_thesis_score = pos.get("entry_thesis_score")
    thesis_trend_line = ""
    if entry_thesis_score and score is not None:
        gap = score - int(entry_thesis_score)
        arrow = "↑" if gap > 0 else ("↓" if gap < 0 else "→")
        thesis_trend_line = (
            f"  Thesis Trend: {arrow} {gap:+d} pts  "
            f"(entry baseline: {entry_thesis_score}/100  →  now: {score}/100)\n"
        )
    elif not entry_thesis_score and score is not None:
        thesis_trend_line = (
            f"  Thesis Trend: legacy position — no entry baseline\n"
            f"  (current score: {score}/100; cost recovery is the primary risk signal)\n"
        )

    # Days held
    days_held_line = ""
    try:
        ed_raw = pos.get("entry_date")
        if ed_raw:
            ed = ed_raw if isinstance(ed_raw, date) else date.fromisoformat(str(ed_raw))
            days_held_line = f"  Held:         {(date.today() - ed).days} days  (entered {ed.strftime('%b %d, %Y')})\n"
    except Exception:
        pass

    return (
        f"{'─'*50}\n"
        f"POSITION\n"
        f"{'─'*50}\n"
        f"  Stock:        {ticker}\n"
        f"  Contract:     {exp} ${strike} Call  |  {qty_remaining_h}/{qty_total_h} contracts remaining\n"
        f"  Avg. Price:   ${entry_p}/share\n"
        + cost_line
        + days_held_line +
        f"  Current mid:  {mid_str}\n"
        f"  P&L:          {pnl_str}  (on remaining {qty_remaining_h} contracts vs avg. entry)\n"
        f"{'─'*50}\n"
        f"THESIS & MARKET\n"
        f"{'─'*50}\n"
        + thesis_trend_line +
        f"  Delta:        {delta if delta is not None else 'N/A'}\n"
        f"  DTE:          {dte_days if dte_days is not None else 'N/A'} days\n"
        f"  IV Rank:      {iv_str}\n"
        f"  Thesis Score: {score if score is not None else 'N/A'} {score_emoji}\n"
        f"{'─'*50}\n"
    )


def _divider(title: str = "") -> str:
    return f"\n{'─'*50}\n{title + chr(10) if title else ''}{'─'*50}\n"


# ---------------------------------------------------------------------------
# Main evaluation function
# ---------------------------------------------------------------------------

def evaluate(position: dict, market: dict) -> list[Alert]:
    """
    Run all 5 pillars against a single position.

    Args:
        position: row from the positions table
        market:   live data dict with keys:
                    mid, bid, delta, dte, iv_rank, thesis_score
                  (missing keys are handled gracefully)

    Returns: list of Alert objects (zero or more).
    """
    alerts: list[Alert] = []

    entry_price  = position.get("entry_price")
    ticker       = position.get("ticker", "?")
    exp_date     = position.get("expiration_date")
    strike       = position.get("strike")

    # Total original contracts purchased
    qty_total    = int(position.get("quantity") or 1)
    # Contracts already sold/trimmed
    qty_trimmed  = int(position.get("quantity_trimmed") or 0)
    # Effective quantity for ALL order calculations going forward
    qty          = max(1, qty_total - qty_trimmed)
    # Dollar proceeds already recovered from trims
    proceeds_recovered = float(position.get("proceeds_from_trims") or 0.0)

    # ── Cost recovery & house money analysis ─────────────────────────────────
    original_cost     = float(entry_price or 0) * qty_total * 100
    cost_recovery_pct = (proceeds_recovered / original_cost * 100) if original_cost > 0 else 0.0
    house_money       = cost_recovery_pct >= 100.0   # trims already returned full investment

    # Thesis score gap: current vs score at entry (gap analysis)
    # Positive = thesis improved since entry; negative = thesis deteriorated
    _ets = position.get("entry_thesis_score")
    entry_thesis = int(_ets) if _ets is not None else None
    thesis_gap   = (market.get("thesis_score") - entry_thesis
                    if (market.get("thesis_score") is not None and entry_thesis is not None)
                    else None)

    # Dynamic exit threshold — relaxed as more profit is locked in.
    # Key insight: these positions were bought with a different scoring system.
    # Performance and cost recovery are the primary signals; thesis is secondary.
    # If thesis has IMPROVED since entry, add 5 pts patience.
    # If thesis has DECLINED a lot, tighten by 5 pts.
    if house_money:
        _thesis_exit = 40     # only exit on near-total collapse when fully covered
    elif cost_recovery_pct >= 70:
        _thesis_exit = 50
    elif cost_recovery_pct >= 30:
        _thesis_exit = 53
    else:
        _thesis_exit = 60     # standard (100pt max)

    if thesis_gap is not None:
        if thesis_gap >= 10:       # thesis improved → more patient
            _thesis_exit = max(30, _thesis_exit - 5)
        elif thesis_gap <= -20:    # thesis deteriorated a lot → tighter
            _thesis_exit = min(65, _thesis_exit + 10)
        elif thesis_gap <= -10:    # thesis somewhat worse
            _thesis_exit = min(65, _thesis_exit + 5)

    # Earnings tone deterioration → tighten thesis exit threshold (must happen
    # BEFORE any pillar evaluates against _thesis_exit, not after)
    _earnings_tone_delta_pre = market.get("earnings_tone_delta")
    if _earnings_tone_delta_pre == "DETERIORATING":
        _thesis_exit = min(65, _thesis_exit + 5)

    mid          = market.get("mid")
    bid          = market.get("bid")
    delta        = market.get("delta")
    dte_days     = market.get("dte") or _dte(exp_date)
    iv_rank      = market.get("iv_rank")
    thesis_score = market.get("thesis_score")

    # Concrete trade execution helpers
    # Limit price: use bid (conservative fill) or 1% below mid if no bid
    limit_px = (round(bid, 2) if bid and bid > 0
                else round(mid * 0.99, 2) if mid
                else None)

    def _exit_order(n_contracts: int) -> str:
        """Return a formatted SELL order line with limit price and estimated proceeds."""
        if limit_px and bid and mid:
            proceeds = limit_px * n_contracts * 100
            return (
                f"  ORDER: SELL {n_contracts} contract{'s' if n_contracts > 1 else ''} "
                f"of {ticker} — limit ${limit_px:.2f}/share\n"
                f"  Estimated proceeds: ~${proceeds:,.0f}  "
                f"(bid ${bid:.2f} · mid ${mid:.2f})\n"
            )
        elif limit_px:
            proceeds = limit_px * n_contracts * 100
            return (
                f"  ORDER: SELL {n_contracts} contract{'s' if n_contracts > 1 else ''} "
                f"of {ticker} — limit ${limit_px:.2f}/share\n"
                f"  Estimated proceeds: ~${proceeds:,.0f}\n"
            )
        return f"  ORDER: SELL {n_contracts} contract{'s' if n_contracts > 1 else ''} of {ticker} at market\n"

    def _roll_target_exp() -> str:
        """Return the roll target expiry string (stored exp + ~12 months)."""
        if not exp_date:
            return "next Jan LEAPS"
        try:
            exp = exp_date if isinstance(exp_date, date) else date.fromisoformat(str(exp_date))
            roll_exp = exp.replace(year=exp.year + 1)
            return roll_exp.strftime("%b %Y")
        except Exception:
            return "next Jan LEAPS"

    pnl  = _pnl_pct(entry_price, mid)
    hdr  = lambda: _header(position, {**market, "pnl_pct": pnl})

    # -----------------------------------------------------------------------
    # PILLAR 1 — Fundamental: Is the thesis still intact?
    # -----------------------------------------------------------------------
    if thesis_score is not None:
        if thesis_score < _thesis_exit:
            # IV context: if IV is high, extra urgency to exit before IV crushes AND price
            iv_extra = ""
            if iv_rank and iv_rank >= 60:
                iv_extra = (
                    f"\n  ★ IV Rank = {iv_rank:.0f}% — DOUBLY URGENT:\n"
                    f"  Options are expensive AND the thesis is weakening. Exit NOW to\n"
                    f"  capture the inflated IV premium before both price AND IV drop.\n"
                )

            # Cost recovery context — explains why threshold may differ from standard 60
            recovery_ctx = ""
            if house_money:
                recovery_ctx = (
                    f"\n  POSITION STATUS: HOUSE MONEY ({cost_recovery_pct:.0f}% cost recovered via trims)\n"
                    f"  Your original capital is already safe. The remaining {qty} contracts\n"
                    f"  represent pure profit. Threshold relaxed to {_thesis_exit}/100 (standard: 60).\n"
                    f"  Exiting only because thesis has reached near-total collapse.\n"
                )
            elif cost_recovery_pct >= 30:
                recovery_ctx = (
                    f"\n  COST RECOVERY: {cost_recovery_pct:.0f}% recovered via trims.\n"
                    f"  Thesis exit threshold relaxed to {_thesis_exit}/100 (standard: 60).\n"
                )

            # Thesis gap context
            gap_ctx = ""
            if thesis_gap is not None:
                direction = "improved" if thesis_gap >= 0 else "deteriorated"
                gap_ctx = (
                    f"\n  THESIS TREND: Score {direction} by {abs(thesis_gap)} pts since entry\n"
                    f"  (entry: {entry_thesis}/100  →  current: {thesis_score}/100)\n"
                )
            elif entry_thesis is None:
                gap_ctx = (
                    f"\n  NOTE: Position predates current scoring system. Cost recovery\n"
                    f"  ({cost_recovery_pct:.0f}%) is the primary risk signal for this position.\n"
                )

            severity = "AMBER" if house_money else "RED"
            subject_prefix = "⚠️ REVIEW" if house_money else "🔴 EXIT"

            alerts.append(Alert(
                type="EXIT_THESIS", severity=severity,
                subject=f"{subject_prefix} — Thesis Weakening: {ticker}  (Score: {thesis_score}/100  threshold: {_thesis_exit})",
                body=(
                    hdr()
                    + f"SIGNAL: {'REVIEW' if house_money else 'EXIT'} — THESIS BELOW THRESHOLD\n"
                    + f"{'─'*50}\n"
                    + f"  Thesis score: {thesis_score}/100  |  Exit threshold: {_thesis_exit}/100\n"
                    + recovery_ctx
                    + gap_ctx
                    + "\n"
                    + (
                        "  HOUSE MONEY GUIDANCE:\n"
                        "  You've already locked in full cost recovery. The remaining contracts\n"
                        "  are playing with profits. Consider trimming rather than full exit.\n"
                        "  Only exit fully if you have NO confidence in the thesis recovering.\n"
                        if house_money else
                        "  The fundamental reason for holding this LEAPS is weakening.\n"
                        "  A deteriorating thesis means the asymmetric upside is shrinking.\n"
                        "  Exit before the market prices in the deterioration fully.\n"
                    )
                    + iv_extra
                    + _divider("IV TIMING")
                    + _iv_sell_context(iv_rank)
                    + _divider("TRADE INSTRUCTION")
                    + (
                        f"  OPTION A — Full exit ({qty} contracts):\n"
                        + _exit_order(qty)
                        + f"\n  OPTION B — Trim only (if you still have some thesis conviction):\n"
                        + _exit_order(max(1, qty // 2))
                        if house_money else
                        _exit_order(qty)
                        + "  Execute at market open tomorrow.\n"
                    )
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
                        + "  Rolling locks in losses AND restarts the decay clock. Do not roll.\n"
                        + _divider("IV TIMING")
                        + _iv_sell_context(iv_rank)
                        + _divider("TRADE INSTRUCTION")
                        + _exit_order(qty)
                    ),
                    context=market,
                ))
            else:
                # Deep ITM but profitable — roll to restore leverage (IV timing is crucial here)
                roll_higher_lo = f"${strike * 1.10:.0f}" if strike else "~10%"
                roll_higher_hi = f"${strike * 1.15:.0f}" if strike else "~15%"
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
                        + "  → This resets leverage and reduces capital at risk\n"
                        + _divider("IV TIMING — CRITICAL FOR ROLLS")
                        + _iv_roll_context(iv_rank)
                        + _divider("TRADE INSTRUCTION")
                        + "  STEP 1 — SELL (close current position):\n"
                        + _exit_order(qty)
                        + "\n"
                        + f"  STEP 2 — BUY (roll to higher strike, same expiry):\n"
                        + f"  ORDER: BUY {qty} contract{'s' if qty > 1 else ''} of {ticker}\n"
                        + f"  Target strike: {roll_higher_lo}–{roll_higher_hi}  (~10-15% above current ${strike})\n"
                        + "  Target delta on new contract: ~0.70\n"
                        + "  Same expiry as current contract.\n\n"
                        + "  ROLL CHECK: Only proceed if net debit (Step 2 − Step 1 proceeds)\n"
                        + "  is < 20% of your original Avg. Price. If higher, take full profit instead.\n"
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
                    + _divider("IV CONTEXT")
                    + _iv_sell_context(iv_rank)
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
                    + _divider("IV TIMING")
                    + _iv_sell_context(iv_rank)
                    + (
                        f"\n  Note on re-entry: wait for IV to drop before buying the new LEAPS.\n"
                        if iv_rank and iv_rank >= 50 else ""
                    )
                    + _divider("TRADE INSTRUCTION")
                    + _exit_order(qty)
                    + "  Execute as soon as market opens. Do not wait.\n"
                ),
                context=market,
            ))

        elif dte_days < _T["dte_urgent"]:
            if pnl is not None and pnl < 0:
                # Losing + < 90 days: still urgent regardless of house money
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
                        + "  Do NOT roll a losing position inside 90 days — it amplifies losses.\n"
                        + (
                            f"\n  HOUSE MONEY NOTE: {cost_recovery_pct:.0f}% cost was already recovered\n"
                            f"  via prior trims, so your net position loss is limited.\n"
                            f"  Still: exit the remaining contracts — theta will destroy them.\n"
                            if house_money else ""
                        )
                        + _divider("IV TIMING")
                        + _iv_sell_context(iv_rank)
                        + _divider("TRADE INSTRUCTION")
                        + _exit_order(qty)
                        + "  Exit to preserve remaining capital.\n"
                    ),
                    context=market,
                ))
            elif pnl is not None:
                # Profitable with < 90 days
                if house_money:
                    # House money: downgrade urgency — remaining contracts are pure profit
                    alerts.append(Alert(
                        type="EXIT_TIME_URGENT", severity="AMBER",
                        subject=f"⚠️ PROTECT GAINS — DTE < 90 Days: {ticker}  DTE: {dte_days}  P&L: {pnl:+.1f}%",
                        body=(
                            hdr()
                            + "SIGNAL: PROTECT GAINS — TIME RUNNING DOWN\n"
                            + f"{'─'*50}\n"
                            + f"  {dte_days} days left. Position is at {pnl:+.1f}% on remaining contracts.\n\n"
                            + f"  HOUSE MONEY STATUS: {cost_recovery_pct:.0f}% of original cost already\n"
                            + f"  recovered via trims. The remaining {qty} contracts are pure profit.\n\n"
                            + "  Urgency is AMBER (not RED) because your original capital is already safe.\n"
                            + "  However, theta will accelerate — protect your remaining gains:\n\n"
                            + "  RECOMMENDED ACTION:\n"
                            + f"  → Sell at least {max(1, qty // 2)} contracts now to lock in gains\n"
                            + f"  → Decide: exit the rest or let the final {qty - max(1, qty // 2)} ride to expiry\n"
                            + "  → If thesis is still strong, re-enter fresh LEAPS after closing\n"
                            + _divider("IV TIMING")
                            + _iv_sell_context(iv_rank)
                            + _divider("TRADE INSTRUCTION")
                            + _exit_order(max(1, qty // 2))
                        ),
                        context=market,
                    ))
                else:
                    # Not house money — standard RED take profit signal
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
                            + _divider("IV TIMING")
                            + _iv_sell_context(iv_rank)
                            + (
                                f"\n  Re-entry note: IV Rank is {iv_rank:.0f}% — wait for IV to drop\n"
                                f"  below 35% before buying the new LEAPS position.\n"
                                if iv_rank and iv_rank >= 50 else
                                f"\n  Re-entry note: IV Rank is {iv_rank:.0f}% — good conditions\n"
                                f"  to buy back in after closing this position.\n"
                                if iv_rank else ""
                            )
                            + _divider("TRADE INSTRUCTION")
                            + _exit_order(qty)
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
                        + "  → Extends your runway toward the 5-10x target\n"
                        + _divider("IV TIMING — CRITICAL FOR ROLLS")
                        + _iv_roll_context(iv_rank)
                        + _divider("TRADE INSTRUCTION")
                        + "  STEP 1 — SELL (close current position):\n"
                        + _exit_order(qty)
                        + "\n"
                        + f"  STEP 2 — BUY (roll forward in time, same strike):\n"
                        + f"  ORDER: BUY {qty} contract{'s' if qty > 1 else ''} of {ticker}\n"
                        + f"  Same strike: ${strike}\n"
                        + f"  Target expiry: {_roll_target_exp()}  (current expiry + 12 months)\n\n"
                        + "  ROLL CHECK: Only proceed if net debit (Step 2 − Step 1 proceeds)\n"
                        + "  is < 20% of your original Avg. Price. If debit is higher, exit and redeploy.\n"
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
                        + _divider("IV CONTEXT")
                        + _iv_sell_context(iv_rank)
                    ),
                    context=market,
                ))

    # -----------------------------------------------------------------------
    # PILLAR 4 — Profit: Are you capturing the asymmetric return?
    # -----------------------------------------------------------------------
    if pnl is not None:

        if pnl <= _T["stop_loss"]:
            _thesis_strong = thesis_score is not None and thesis_score >= 65
            if _thesis_strong:
                # Thesis is intact (≥65 = Qualified) — wrong strike/timing, not wrong thesis.
                # Recommend rolling down to a higher-delta strike rather than exiting.
                alerts.append(Alert(
                    type="ROLL_STOP", severity="AMBER",
                    subject=f"⚠️ ROLL DOWN SIGNAL: {ticker}  P&L: {pnl:+.1f}%  Thesis: {thesis_score}/100",
                    body=(
                        hdr()
                        + "SIGNAL: ROLL DOWN — STOP LOSS HIT BUT THESIS INTACT\n"
                        + f"{'─'*50}\n"
                        + f"  Position is at {pnl:+.1f}% — below the -60% stop level.\n"
                        + f"  However, thesis score is {thesis_score}/100 (≥65 = Qualified) — the story is still valid.\n"
                        + "  This is a strike/timing problem, NOT a thesis problem.\n\n"
                        + "  RECOMMENDED ACTION: ROLL DOWN\n"
                        + "  • Close current position (limit order at mid)\n"
                        + "  • Re-enter same ticker, same expiration, lower strike\n"
                        + "  • Target delta 0.50–0.65 (deeper ITM for higher delta, less time decay)\n"
                        + "  • Use proceeds + additional capital if conviction warrants it\n\n"
                        + "  WHY ROLL INSTEAD OF EXIT:\n"
                        + "  Your current strike is too far OTM given where price is now.\n"
                        + "  A lower strike gives you a better delta and costs less to rebuild.\n"
                        + "  Rolling preserves the thesis exposure at a more efficient entry.\n"
                        + _divider("IV TIMING")
                        + _iv_sell_context(iv_rank)
                        + (
                            f"\n  ★ IV is elevated ({iv_rank:.0f}%) — roll when IV normalizes\n"
                            f"  to get better pricing on the new strike.\n"
                            if iv_rank and iv_rank >= 50 else ""
                        )
                        + _divider("TRADE INSTRUCTION")
                        + _exit_order(qty)
                        + f"  Then re-enter: buy {qty} calls, same expiry, strike ~10–20% lower.\n"
                    ),
                    context=market,
                ))
            else:
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
                        + "  Thesis may still be valid — re-enter fresh LEAPS if it is.\n"
                        + _divider("IV TIMING")
                        + _iv_sell_context(iv_rank)
                        + (
                            f"\n  Re-entry: IV is {iv_rank:.0f}% — wait for IV to normalize\n"
                            f"  before buying back in.\n"
                            if iv_rank and iv_rank >= 50 else ""
                        )
                        + _divider("TRADE INSTRUCTION")
                        + _exit_order(qty)
                        + "  Exit the full position. Preserve remaining capital.\n"
                    ),
                    context=market,
                ))

        elif pnl >= _T["profit_900"]:
            iv_urgency = ""
            if iv_rank and iv_rank >= 65:
                iv_urgency = (
                    f"\n  ★ IV Rank = {iv_rank:.0f}% — FOMO buyers are paying inflated prices.\n"
                    f"  This is the optimal exit: 10x target + high IV = maximum proceeds.\n"
                    f"  This combination rarely lasts. Execute today.\n"
                )
            alerts.append(Alert(
                type="PROFIT_900", severity="RED",
                subject=f"🔴 10x TARGET HIT — EXIT ALL: {ticker}  P&L: {pnl:+.1f}%",
                body=(
                    hdr()
                    + "SIGNAL: 10x TARGET REACHED — FULL EXIT\n"
                    + f"{'─'*50}\n"
                    + f"  Your position is up {pnl:+.1f}% — you've hit the 10x target!\n\n"
                    + "  This is your full exit. You are selling to someone else's FOMO.\n"
                    + "  Do not wait for more — the final move from here has lower\n"
                    + "  probability and you've already captured the asymmetric return.\n"
                    + iv_urgency
                    + _divider("IV TIMING")
                    + _iv_sell_context(iv_rank)
                    + _divider("TRADE INSTRUCTION")
                    + _exit_order(qty)
                    + "  Exit the entire remaining position.\n"
                ),
                context=market,
            ))

        elif pnl >= _T["profit_600"]:
            n_trim = max(1, round(qty * 0.75))
            n_hold = qty - n_trim
            roll_note = (
                "\n  TIME NOTE: You still have enough DTE to roll the trailing\n"
                f"  {n_hold} contract{'s' if n_hold != 1 else ''} to a further-dated contract if you want to stay in the trade.\n"
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
                    + f"  → Sell {n_trim} contract{'s' if n_trim > 1 else ''} (75%) now to lock in the 7x gains\n"
                    + (f"  → Keep {n_hold} contract{'s' if n_hold > 1 else ''} (25%) trailing toward the 10x target\n" if n_hold > 0 else "")
                    + "  → Set a mental stop on the trailing position: if it gives back\n"
                    + "    50% of gains, exit the remainder\n"
                    + roll_note
                    + _divider("IV TIMING")
                    + _iv_sell_context(iv_rank)
                    + _divider(f"TRADE INSTRUCTION  ({n_trim}/{qty} contracts — 75% trim)")
                    + _exit_order(n_trim)
                    + (f"  Keep remaining: {n_hold} contract{'s' if n_hold != 1 else ''} — trailing to 10x\n" if n_hold > 0 else "")
                ),
                context=market,
            ))

        elif pnl >= _T["profit_300"]:
            # For house money: prefer smaller trim — remaining contracts are zero-net-cost
            n_trim = max(1, round(qty * (0.35 if house_money else 0.50)))
            n_hold = qty - n_trim

            # IV-adjusted roll guidance for the held portion
            if dte_days and dte_days > _T["dte_urgent"]:
                if iv_rank and iv_rank >= 60:
                    roll_action = (
                        f"  → For the remaining {n_hold} contract{'s' if n_hold != 1 else ''}:\n"
                        f"    ⚠️  IV Rank = {iv_rank:.0f}% — rolling is expensive right now.\n"
                        f"    Better: exit the remaining contracts too (collect high IV premium)\n"
                        f"    and re-enter a fresh LEAPS when IV drops below 35%.\n"
                    )
                elif iv_rank and iv_rank < 30:
                    roll_action = (
                        f"  → For the remaining {n_hold} contract{'s' if n_hold != 1 else ''}:\n"
                        f"    ★ IV Rank = {iv_rank:.0f}% — IDEAL TIME TO ROLL (cheap new contract).\n"
                        f"    Roll: sell current, buy same strike + {_roll_target_exp()}.\n"
                        f"    Roll check: debit < 20% of original Avg. Price.\n"
                    )
                else:
                    roll_action = (
                        f"  → For the remaining {n_hold} contract{'s' if n_hold != 1 else ''}:\n"
                        f"    DTE = {dte_days} days. If < 270 days remain, consider rolling:\n"
                        f"    Sell current, buy same strike + {_roll_target_exp()}.\n"
                        f"    Roll check: debit < 20% of original Avg. Price.\n"
                    )
            else:
                roll_action = (
                    f"  → For the remaining {n_hold} contract{'s' if n_hold != 1 else ''}:\n"
                    f"    DTE = {dte_days} days — too short to roll cost-effectively.\n"
                    f"    Exit the remainder and redeploy into fresh LEAPS if\n"
                    f"    thesis is still intact and you want more upside.\n"
                )

            trim_pct_str = "35%" if house_money else "50%"
            house_money_context = ""
            if house_money:
                remaining_val = round(mid * qty * 100) if mid else None
                val_str = f"${remaining_val:,.0f}" if remaining_val else "N/A"
                house_money_context = (
                    f"\n  ★ HOUSE MONEY: Original cost fully recovered via prior trims.\n"
                    f"  Remaining {qty} contracts = {val_str} of PURE BONUS PROFIT.\n"
                    f"  Trim suggestion reduced to {trim_pct_str} (vs 50% standard) because\n"
                    f"  your risk is zero — let more of the position ride.\n\n"
                )
            elif cost_recovery_pct >= 20:
                house_money_context = (
                    f"\n  RECOVERY NOTE: {cost_recovery_pct:.0f}% of original cost already recovered.\n"
                    f"  Net capital at risk is significantly reduced.\n\n"
                )

            alerts.append(Alert(
                type="PROFIT_300", severity="AMBER",
                subject=f"⚠️ TRIM & ROLL — {ticker} Up 4x: {pnl:+.1f}%",
                body=(
                    hdr()
                    + "SIGNAL: TRIM & CONSIDER ROLLING — UP 4x\n"
                    + f"{'─'*50}\n"
                    + f"  Position is up {pnl:+.1f}% — a 4x return on remaining contracts.\n"
                    + house_money_context
                    + (
                        "  Strategy: Take serious profits while keeping upside exposure.\n\n"
                        if not house_money else
                        "  Strategy: Protect bonus profit while keeping exposure to 7-10x.\n\n"
                    )
                    + "  RECOMMENDED ACTION:\n"
                    + f"  → Sell {n_trim} contract{'s' if n_trim > 1 else ''} ({trim_pct_str}) now — lock in the 4x\n"
                    + roll_action
                    + "\n"
                    + "  This approach:\n"
                    + "  ✓ Secures substantial gains that cannot be taken away\n"
                    + "  ✓ Keeps exposure to the 7-10x move if thesis plays out\n"
                    + ("  ✓ Zero net capital at risk on remaining contracts\n" if house_money else
                       "  ✓ Reduces capital at risk significantly\n")
                    + _divider("IV TIMING")
                    + _iv_sell_context(iv_rank)
                    + _divider(f"TRADE INSTRUCTION  ({n_trim}/{qty} contracts — {trim_pct_str} trim)")
                    + _exit_order(n_trim)
                    + (f"  Keep remaining: {n_hold} contract{'s' if n_hold != 1 else ''} — targeting 7-10x\n" if n_hold > 0 else "")
                ),
                context=market,
            ))

        elif pnl >= _T["profit_100"]:
            # IV adjusts urgency
            iv_note = ""
            if iv_rank and iv_rank >= 65:
                iv_note = (
                    f"\n  ★ IV Rank = {iv_rank:.0f}% — this is a great time to take the trim.\n"
                    f"  You're selling expensive paper. Execute now.\n"
                )
            elif iv_rank and iv_rank < 25:
                iv_note = (
                    f"\n  IV Rank = {iv_rank:.0f}% — options are cheap right now.\n"
                    f"  If DTE > 270, consider waiting for an IV spike before trimming.\n"
                    f"  If DTE < 270, take the trim regardless.\n"
                )

            if house_money:
                # User already recovered full original cost via prior trims.
                # Remaining contracts cost $0 net — any proceeds are bonus profit.
                # Don't pressure a 20% trim on top of already-executed trimming.
                remaining_val = round(mid * qty * 100) if mid else None
                val_str = f"  Current value of {qty} remaining contracts: ${remaining_val:,.0f}\n" if remaining_val else ""
                alerts.append(Alert(
                    type="PROFIT_100", severity="GREEN",
                    subject=f"🟢 BONUS PROFIT — {ticker} Up 2x on Remaining Contracts: {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: BONUS PROFIT — REMAINING CONTRACTS AT 2x\n"
                        + f"{'─'*50}\n"
                        + f"  Position is up {pnl:+.1f}% on your remaining {qty} contracts.\n\n"
                        + "  ★ HOUSE MONEY CONTEXT:\n"
                        + f"  Your original investment is FULLY RECOVERED via prior trims.\n"
                        + val_str
                        + "  Every dollar you realize from here is pure additional profit.\n\n"
                        + "  No action required — you've already done the hard part.\n"
                        + "  Options:\n"
                        + f"  → Let all {qty} contracts ride toward the 5-10x target\n"
                        + f"  → OR trim a few more to lock in this bonus profit\n"
                        + iv_note
                        + _divider("IV TIMING")
                        + _iv_sell_context(iv_rank)
                    ),
                    context=market,
                ))
            else:
                n_trim = max(1, round(qty * 0.20))
                n_hold = qty - n_trim
                alerts.append(Alert(
                    type="PROFIT_100", severity="AMBER",
                    subject=f"⚠️ FIRST TRIM — {ticker} Up 2x: {pnl:+.1f}%",
                    body=(
                        hdr()
                        + "SIGNAL: FIRST PROFIT MILESTONE — UP 2x\n"
                        + f"{'─'*50}\n"
                        + f"  Position is up {pnl:+.1f}% — you've doubled your money.\n\n"
                        + (
                            f"  PARTIAL RECOVERY NOTE: {cost_recovery_pct:.0f}% of original cost already\n"
                            f"  recovered via prior trims. Net cost at risk is reduced.\n\n"
                            if cost_recovery_pct >= 20 else ""
                        )
                        + "  RECOMMENDED ACTION:\n"
                        + f"  → Sell {n_trim} contract{'s' if n_trim > 1 else ''} (~20%) to recover initial cost basis\n"
                        + (f"  → Keep {n_hold} contract{'s' if n_hold > 1 else ''} running — your target is 5-10x, not 2x\n\n" if n_hold > 0 else "\n")
                        + "  Do NOT sell the full position here. Selling at 2x sacrifices\n"
                        + "  the asymmetric return that justifies buying LEAPS in the first place.\n"
                        + iv_note
                        + _divider("IV TIMING")
                        + _iv_sell_context(iv_rank)
                        + _divider(f"TRADE INSTRUCTION  ({n_trim}/{qty} contracts — ~20% trim)")
                        + _exit_order(n_trim)
                        + (f"  Keep remaining: {n_hold} contract{'s' if n_hold != 1 else ''} — keep riding\n" if n_hold > 0 else "")
                    ),
                    context=market,
                ))

    # -----------------------------------------------------------------------
    # PILLAR 5 — Earnings & Catalyst: Pre/post earnings + news sentiment
    # -----------------------------------------------------------------------
    # Fires on earnings calendar state and earnings call tone.
    # These are the highest-signal events for a 12–24 month LEAPS thesis.

    earnings_state           = market.get("earnings_state")
    earnings_tone_score      = market.get("earnings_tone_score")
    earnings_guidance_change = market.get("earnings_guidance_change")
    earnings_tone_delta      = market.get("earnings_tone_delta")
    news_sentiment_score     = market.get("news_sentiment_score")

    # ── PRE-EARNINGS: IV expanded + profitable → optimal sell window ─────────
    if earnings_state in ("imminent", "week_of") and pnl is not None and pnl > 0:
        iv_val = iv_rank or 0
        if iv_val >= 50:
            days_label = {"imminent": "7–14 days", "week_of": "< 7 days"}.get(earnings_state, "")
            alerts.append(Alert(
                type="PRE_EARNINGS_SELL", severity="BLUE",
                subject=(
                    f"🔵 PRE-EARNINGS SELL WINDOW: {ticker}  "
                    f"IV {iv_val:.0f}%  P&L: {pnl:+.1f}%  ({days_label} to earnings)"
                ),
                body=(
                    hdr()
                    + _divider("PILLAR 5 — PRE-EARNINGS IV SELL WINDOW")
                    + f"  Earnings in: {days_label}\n"
                    + f"  Current IV Rank: {iv_val:.0f}% — IV elevated before earnings\n"
                    + f"  Current P&L: {pnl:+.1f}% — position profitable\n\n"
                    + "  This is the optimal window to capture extrinsic premium:\n"
                    + "  • IV is inflated pre-earnings (buyers paying fear premium)\n"
                    + "  • After the announcement, IV will crush 30-60% overnight\n"
                    + "  • Your contract is worth more TODAY than it will be post-earnings\n\n"
                    + "  OPTIONS:\n"
                    + "  A) SELL NOW (recommended if thesis has played out)\n"
                    + f"     → Capture elevated premium before IV crush\n"
                    + "  B) HOLD if you have strong conviction on the earnings move direction\n"
                    + "     → Risk: IV crush can offset even a correct directional move\n\n"
                    + _divider("TRADE INSTRUCTION  (if executing A)")
                    + _exit_order(qty)
                    + _iv_sell_context(iv_val)
                ),
                context=market,
            ))

    # ── EARNINGS DAY: Decide hold vs sell before close ────────────────────────
    elif earnings_state == "day_of":
        alerts.append(Alert(
            type="EARNINGS_DAY", severity="AMBER",
            subject=f"⚠️ EARNINGS TODAY: {ticker} — decide HOLD or SELL before close",
            body=(
                hdr()
                + _divider("PILLAR 5 — EARNINGS DAY DECISION")
                + "  Earnings are reported TODAY.\n\n"
                + "  SELL BEFORE CLOSE if:\n"
                + "  • IV Rank is high (≥ 50%) — you'll lose premium to IV crush\n"
                + "  • Thesis has already played out (position profitable)\n"
                + "  • You're uncertain about the earnings direction\n\n"
                + "  HOLD THROUGH if:\n"
                + "  • You have high conviction on the earnings beat magnitude\n"
                + "  • Position is a small % of portfolio (risk defined)\n"
                + "  • IV Rank is already low (little crush risk)\n\n"
                + (f"  Current IV Rank: {iv_rank:.0f}%\n" if iv_rank else "")
                + (f"  Current P&L: {pnl:+.1f}%\n" if pnl is not None else "")
            ),
            context=market,
        ))

    # ── POST-EARNINGS: IV crushed, reassess thesis and rolling ───────────────
    elif earnings_state == "post":
        alerts.append(Alert(
            type="POST_EARNINGS_REASSESS", severity="GREEN",
            subject=f"🟢 POST-EARNINGS: {ticker} — IV crushed, reassess thesis & roll economics",
            body=(
                hdr()
                + _divider("PILLAR 5 — POST-EARNINGS REASSESSMENT")
                + "  Earnings were reported in the last 1–3 days.\n"
                + "  IV has likely crushed 30–60% — options are now CHEAPER.\n\n"
                + "  ACTION ITEMS:\n"
                + "  1. Check earnings call tone — did management raise or cut guidance?\n"
                + "  2. Re-assess thesis: is the 12-24 month LEAPS case still intact?\n"
                + "  3. If planning to ROLL: NOW is the best time to buy the new contract\n"
                + "     (IV crush = cheap extrinsic on the new position)\n"
                + "  4. If thesis broke (guidance cut, tone shift) → EXIT per Pillar 1\n\n"
                + (f"  Current IV Rank: {iv_rank:.0f}% (post-crush baseline)\n" if iv_rank else "")
                + _iv_roll_context(iv_rank or 0)
            ),
            context=market,
        ))

    # ── EARNINGS THESIS BREAK: Tone very negative + thesis weakened ──────────
    if (
        earnings_tone_score is not None
        and earnings_tone_score < -0.4
        and market.get("thesis_impact") == "WEAKENED"
    ):
        alerts.append(Alert(
            type="EARNINGS_THESIS_BREAK", severity="RED",
            subject=(
                f"🔴 EARNINGS THESIS BREAK: {ticker} — "
                f"call tone {earnings_tone_score:+.2f} + guidance weakened"
            ),
            body=(
                hdr()
                + _divider("PILLAR 5 — EARNINGS CALL THESIS BREAK")
                + f"  Earnings Call Tone Score: {earnings_tone_score:+.2f}  (threshold: -0.40)\n"
                + f"  Thesis Impact: WEAKENED\n"
                + (f"  Guidance Change: {earnings_guidance_change}\n" if earnings_guidance_change else "")
                + (f"  Tone Trend: {earnings_tone_delta}\n" if earnings_tone_delta else "")
                + "\n  Management language signals thesis deterioration.\n"
                + "  Language detected: vague guidance, headwinds, elongated cycles,\n"
                + "  or management confidence declining vs prior quarters.\n\n"
                + "  RECOMMENDATION: EXIT this position.\n"
                + "  The LEAPS thesis depends on 12-24 month trajectory.\n"
                + "  Management's own language suggests that trajectory has weakened.\n\n"
                + _divider("TRADE INSTRUCTION")
                + _exit_order(qty)
                + _iv_sell_context(iv_rank or 0)
            ),
            context=market,
        ))

    # ── EARNINGS GUIDANCE CUT: Management lowered forward guidance ────────────
    if earnings_guidance_change == "LOWERED":
        alerts.append(Alert(
            type="EARNINGS_GUIDANCE_CUT", severity="RED",
            subject=f"🔴 GUIDANCE CUT: {ticker} — management lowered forward guidance",
            body=(
                hdr()
                + _divider("PILLAR 5 — FORWARD GUIDANCE LOWERED")
                + "  Management has LOWERED forward guidance this quarter.\n"
                + "  This is a direct LEAPS thesis impact — your 12-24 month bet\n"
                + "  is predicated on the company hitting its growth trajectory.\n\n"
                + "  RECOMMENDATION: Trigger full thesis re-score immediately.\n"
                + "  If new score < exit threshold → EXIT per Pillar 1.\n\n"
                + _divider("TRADE INSTRUCTION  (if re-score confirms exit)")
                + _exit_order(qty)
            ),
            context=market,
        ))

    # ── EARNINGS BULLISH: Guidance raised ────────────────────────────────────
    elif earnings_guidance_change == "RAISED" and earnings_tone_score and earnings_tone_score > 0.3:
        alerts.append(Alert(
            type="EARNINGS_BULLISH", severity="GREEN",
            subject=f"🟢 EARNINGS BULLISH: {ticker} — guidance RAISED, thesis strengthened",
            body=(
                hdr()
                + _divider("PILLAR 5 — EARNINGS CALL BULLISH SIGNAL")
                + f"  Guidance Change: RAISED\n"
                + f"  Tone Score: {earnings_tone_score:+.2f}\n"
                + (f"  Tone Trend: {earnings_tone_delta}\n" if earnings_tone_delta else "")
                + "\n  Management raised forward guidance — thesis strengthened.\n"
                + "  HOLD and consider adding on dips if IV permits.\n"
                + "  Review roll economics if DTE < 270 days.\n"
            ),
            context=market,
        ))

    # ── NEWS SENTIMENT: Breaking bearish news ────────────────────────────────
    if news_sentiment_score is not None:
        if news_sentiment_score < -0.35:
            alerts.append(Alert(
                type="NEWS_VERY_BEARISH", severity="RED",
                subject=(
                    f"🔴 VERY BEARISH NEWS: {ticker} — "
                    f"sentiment {news_sentiment_score:+.2f}"
                ),
                body=(
                    hdr()
                    + _divider("PILLAR 5 — VERY BEARISH NEWS SIGNAL")
                    + f"  News Sentiment Score: {news_sentiment_score:+.3f}  (VERY BEARISH)\n\n"
                    + "  Major negative news detected. Potential thesis impacts:\n"
                    + "  • FDA rejection / clinical trial failure\n"
                    + "  • Guidance withdrawal or profit warning\n"
                    + "  • M&A deal collapse or regulatory block\n"
                    + "  • CEO/CFO unexpected departure\n\n"
                    + "  RECOMMENDATION: Verify news and re-assess thesis IMMEDIATELY.\n"
                    + "  If thesis is broken → EXIT before further decline.\n\n"
                    + _divider("CONTINGENCY TRADE INSTRUCTION")
                    + _exit_order(qty)
                ),
                context=market,
            ))
        elif news_sentiment_score < -0.15:
            alerts.append(Alert(
                type="NEWS_BEARISH", severity="AMBER",
                subject=(
                    f"⚠️ BEARISH NEWS: {ticker} — "
                    f"sentiment {news_sentiment_score:+.2f}"
                ),
                body=(
                    hdr()
                    + _divider("PILLAR 5 — BEARISH NEWS SIGNAL")
                    + f"  News Sentiment Score: {news_sentiment_score:+.3f}  (BEARISH)\n\n"
                    + "  Negative news sentiment detected. Monitor closely.\n"
                    + "  Verify whether this news affects your LEAPS thesis fundamentals.\n"
                    + "  If temporary noise → document and continue monitoring.\n"
                    + "  If thesis-relevant → re-score and apply Pillar 1 thresholds.\n"
                ),
                context=market,
            ))

    # -----------------------------------------------------------------------
    # PILLAR 6 — IV Timing: Strike while the iron is hot
    # -----------------------------------------------------------------------
    # These fire ON TOP OF Pillars 1-5 when IV rank creates an especially
    # favorable execution window. Each has its own alert type for independent
    # deduplication — so you may get both ROLL_DELTA and IV_ROLL_SELL_NOW on
    # the same day. They answer: "I know I need to act — but is TODAY optimal?"
    if iv_rank is not None:
        posture_types = {a.type for a in alerts}
        has_exit = bool({"EXIT_THESIS", "EXIT_STOP", "EXIT_TIME_URGENT"} & posture_types)
        has_roll = bool({"ROLL_DELTA", "ROLL_TIME"} & posture_types)
        has_trim = bool({"PROFIT_100", "PROFIT_300", "PROFIT_600", "PROFIT_900"} & posture_types)

        # ── IV HIGH (≥ 65%): prime selling window ───────────────────────────
        if iv_rank >= 65:

            if has_exit and pnl is not None:
                alerts.append(Alert(
                    type="IV_EXIT_NOW", severity="RED",
                    subject=(
                        f"🔴 IV PRIME — EXIT NOW: {ticker}  "
                        f"IV Rank {iv_rank:.0f}%  P&L: {pnl:+.1f}%"
                    ),
                    body=(
                        hdr()
                        + "SIGNAL: IV PRIME — EXECUTE YOUR EXIT TODAY\n"
                        + f"{'─'*50}\n"
                        + f"  IV Rank = {iv_rank:.0f}%. Options are in the top "
                        + f"{100 - iv_rank:.0f}% of their\n"
                        + "  historical volatility range. Buyers are paying a\n"
                        + "  fear/FOMO premium on top of every contract right now.\n\n"
                        + "  You already have an exit signal from the position analysis.\n"
                        + "  IV at this level maximizes your proceeds. This window is\n"
                        + "  temporary — IV normalizes fast when sentiment shifts.\n"
                        + "  Execute today. Do not wait for a 'better' price.\n"
                        + _divider("TRADE INSTRUCTION")
                        + _exit_order(qty)
                        + "  Set limit at bid or 1% below mid. You will get filled.\n"
                    ),
                    context=market,
                ))

            if has_roll and not has_exit:
                alerts.append(Alert(
                    type="IV_ROLL_SELL_NOW", severity="BLUE",
                    subject=(
                        f"🔵 IV HIGH — SELL CURRENT CONTRACT NOW: {ticker}  "
                        f"IV {iv_rank:.0f}%  |  Wait for IV < 35% to buy new"
                    ),
                    body=(
                        hdr()
                        + "SIGNAL: IV HIGH — EXECUTE THE SELL LEG OF YOUR ROLL NOW\n"
                        + f"{'─'*50}\n"
                        + f"  IV Rank = {iv_rank:.0f}%. Your position needs a roll.\n"
                        + "  With IV this elevated, your current contract is near its\n"
                        + "  maximum extrinsic value. Selling now collects that premium.\n\n"
                        + "  ⚡ DO NOT buy the new contract yet.\n"
                        + "  Wait until IV drops below 35%, then buy the new LEAPS\n"
                        + "  cheaply. You will receive an IV_ROLL_BUY_NOW alert when\n"
                        + "  that window opens.\n\n"
                        + "  Split execution typically saves 15-25% on the cost of\n"
                        + "  the new contract vs. rolling in one session.\n"
                        + _divider("STEP 1 — SELL CURRENT CONTRACT (execute today)")
                        + _exit_order(qty)
                        + "\n"
                        + f"  STEP 2 — BUY NEW CONTRACT (execute LATER when IV < 35%):\n"
                        + f"  ORDER: BUY {qty} contract{'s' if qty > 1 else ''} of {ticker}\n"
                        + f"  Strike: ${strike}  |  Target expiry: {_roll_target_exp()}\n"
                        + "  Target delta: ~0.70  |  Wait for IV_ROLL_BUY_NOW alert\n"
                    ),
                    context=market,
                ))

            if has_trim and not has_exit and pnl is not None:
                # Match trim size to whichever profit level is active
                if "PROFIT_600" in posture_types or "PROFIT_900" in posture_types:
                    iv_n_trim = max(1, round(qty * 0.75))
                elif "PROFIT_300" in posture_types:
                    iv_n_trim = max(1, round(qty * 0.50))
                else:   # PROFIT_100
                    iv_n_trim = max(1, round(qty * 0.20))
                iv_n_hold = qty - iv_n_trim
                alerts.append(Alert(
                    type="IV_TRIM_NOW", severity="BLUE",
                    subject=(
                        f"🔵 IV PRIME — TRIM NOW: {ticker}  "
                        f"IV {iv_rank:.0f}%  P&L: {pnl:+.1f}%"
                    ),
                    body=(
                        hdr()
                        + "SIGNAL: IV PRIME — BEST MOMENT TO EXECUTE YOUR TRIM\n"
                        + f"{'─'*50}\n"
                        + f"  IV Rank = {iv_rank:.0f}%. You have a profit milestone\n"
                        + "  AND options are priced at an elevated volatility premium.\n\n"
                        + "  This combination is rare:\n"
                        + "  ✓ P&L target has been hit (profit alert already sent)\n"
                        + "  ✓ High IV adds extra extrinsic premium on top of gains\n"
                        + "  ✓ Buyers are paying a fear/FOMO premium right now\n\n"
                        + "  Executing today captures intrinsic + inflated extrinsic.\n"
                        + "  When IV normalizes, that extrinsic premium disappears.\n"
                        + _divider(f"TRADE INSTRUCTION  ({iv_n_trim}/{qty} contracts)")
                        + _exit_order(iv_n_trim)
                        + (
                            f"  Keep remaining: {iv_n_hold} contract"
                            + f"{'s' if iv_n_hold != 1 else ''} — trailing to target\n"
                            if iv_n_hold > 0 else ""
                        )
                    ),
                    context=market,
                ))

        # ── IV LOW (≤ 30%): prime buying window for the new contract ────────
        if iv_rank <= 30 and has_roll and not has_exit:
            alerts.append(Alert(
                type="IV_ROLL_BUY_NOW", severity="GREEN",
                subject=(
                    f"🟢 IV LOW — BUY NEW LEAPS NOW: {ticker}  "
                    f"IV {iv_rank:.0f}%  (roll step 2 / re-entry)"
                ),
                body=(
                    hdr()
                    + "SIGNAL: IV LOW — OPTIMAL WINDOW TO ENTER THE NEW CONTRACT\n"
                    + f"{'─'*50}\n"
                    + f"  IV Rank = {iv_rank:.0f}%. Options are CHEAP right now.\n"
                    + "  This is the optimal window to buy your new LEAPS contract.\n\n"
                    + "  SCENARIO A — You already sold your current contract:\n"
                    + "  → BUY NOW. IV this low means you pay minimal extrinsic value.\n"
                    + "    Your new position starts with a structural cost advantage.\n"
                    + "    Any future IV expansion will boost your value immediately.\n\n"
                    + "  SCENARIO B — You still hold the current contract:\n"
                    + f"  → IV at {iv_rank:.0f}% is not ideal for the sell side,\n"
                    + "    but the buy discount may offset it. Consider rolling today:\n"
                    + "    Net roll debit < 20% of original Avg. Price → proceed.\n"
                    + "    Net roll debit ≥ 20% → wait for IV to rise, sell first.\n"
                    + _divider("BUY INSTRUCTION (the new contract)")
                    + f"  ORDER: BUY {qty} contract{'s' if qty > 1 else ''} of {ticker}\n"
                    + f"  Strike: ${strike}  (same as current)\n"
                    + f"  Target expiry: {_roll_target_exp()}  (current expiry + 12 months)\n"
                    + "  Target delta on new contract: ~0.70\n\n"
                    + "  Set limit at ask or 1% above mid. Fill promptly.\n"
                    + (
                        f"  Roll check: net debit should be < "
                        f"${entry_price * 0.20:.2f}/share "
                        f"(20% of your ${entry_price}/share Avg. Price).\n"
                        if entry_price else
                        "  Roll check: net debit < 20% of your original Avg. Price.\n"
                    )
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
    ticker            = position.get("ticker", "?")
    weekly_rsi        = stock_data.get("weekly_rsi")
    pct_from_low      = stock_data.get("pct_from_low")
    price             = stock_data.get("price")
    above_ma50        = stock_data.get("above_ma50")
    above_ma200       = stock_data.get("above_ma200")
    ma50_above_ma200  = stock_data.get("ma50_above_ma200")
    ma_50             = stock_data.get("ma_50")
    ma_200            = stock_data.get("ma_200")

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
        elif weekly_rsi < 50:
            score += 5
            reasons.append(f"Weekly RSI = {weekly_rsi:.1f} (neutral — no RSI tailwind yet)")

    # IV Rank signal — the most important timing factor for option buyers
    if iv_rank is not None:
        if iv_rank < 20:
            score += 35   # Extra weight: exceptionally cheap options
            reasons.append(f"IV Rank = {iv_rank:.1f}% ★ — options are VERY cheap (top timing signal)")
        elif iv_rank < 30:
            score += 25
            reasons.append(f"IV Rank = {iv_rank:.1f}% — options are cheap, good time to buy premium")
        elif iv_rank < 40:
            score += 10
            reasons.append(f"IV Rank = {iv_rank:.1f}% — options are reasonably priced")
        elif iv_rank >= 60:
            score -= 20   # Penalty: buying expensive options is a drag on LEAPS returns
            reasons.append(f"IV Rank = {iv_rank:.1f}% ⚠️ — options are expensive, reduces entry quality")

    # Price vs 52-week range
    if pct_from_low is not None:
        if pct_from_low < 0.15:
            score += 30
            reasons.append(f"Stock near 52-week low ({pct_from_low*100:.0f}% from low) — maximum asymmetry")
        elif pct_from_low < 0.30:
            score += 15
            reasons.append(f"Stock in lower third of 52-week range ({pct_from_low*100:.0f}% from low)")
        elif pct_from_low > 0.80:
            score -= 10
            reasons.append(f"Stock near 52-week high ({pct_from_low*100:.0f}% from low) — limited upside room")

    # Trend structure signal (MA50 / MA200)
    # For LEAPS, the ideal scenario is beaten-down stock starting to recover.
    # We reward early recovery (price reclaims MA50) and penalise unconfirmed downtrends.
    if above_ma50 is not None and above_ma200 is not None:
        if above_ma200 and above_ma50 and ma50_above_ma200:
            # Clean uptrend with pullback — highest conviction entry
            score += 15
            reasons.append(
                f"Uptrend intact (price > MA50 ${ma_50:.2f} > MA200 ${ma_200:.2f}) — "
                "buying into strength with trend behind you"
            )
        elif above_ma50 and not above_ma200:
            # Price reclaimed MA50 but still below MA200 — early recovery signal
            score += 10
            reasons.append(
                f"Early recovery: price crossed above MA50 (${ma_50:.2f}) "
                f"but still below MA200 (${ma_200:.2f}) — watch for MA200 reclaim"
            )
        elif not above_ma50 and not above_ma200:
            if weekly_rsi is not None and weekly_rsi < 35:
                # Downtrend but deeply oversold — high-risk / high-reward entry
                score += 5
                reasons.append(
                    f"Price below MA50 & MA200 — downtrend, but RSI oversold "
                    "suggests capitulation may be near"
                )
            else:
                # Pure downtrend, no oversold cushion — reduce score
                score -= 10
                reasons.append(
                    f"Price below MA50 (${ma_50:.2f}) & MA200 (${ma_200:.2f}) "
                    "— downtrend not yet exhausted; wait for recovery signal"
                )

    if score < 40:
        # Still fire if IV is at floor — cheap options is a standalone signal
        # even when RSI and price position aren't fully aligned yet.
        if iv_rank is not None and iv_rank <= 25:
            return Alert(
                type="IV_ENTRY_OPTIMAL", severity="GREEN",
                subject=f"🟢 IV AT FLOOR — BUY LEAPS CHEAP: {ticker}  IV Rank {iv_rank:.0f}%",
                body=(
                    f"{'─'*50}\n"
                    f"WATCHLIST TICKER:  {ticker}\n"
                    + (f"Current Price:     ${price:.2f}\n" if price else "")
                    + f"IV Rank:           {iv_rank:.0f}% ★ FLOOR LEVEL\n"
                    + f"Entry Score:       {score}/100  (other signals not yet aligned)\n"
                    + f"{'─'*50}\n"
                    + "SIGNAL: OPTIONS ARE AT THEIR CHEAPEST\n\n"
                    + f"  IV Rank = {iv_rank:.0f}% means options on {ticker} are cheaper\n"
                    + "  than they've been during 75%+ of the past year.\n\n"
                    + "  Other entry signals (RSI, 52-week position) aren't fully\n"
                    + "  aligned yet — but IV this cheap is a standalone reason to\n"
                    + "  consider entering. Even if the stock stalls for weeks, an\n"
                    + "  IV expansion alone can boost your option value 15-30%.\n\n"
                    + "  WHEN TO ACT:\n"
                    + "  → Buy now if you have conviction in the thesis.\n"
                    + "  → Target delta:  0.25-0.40\n"
                    + "  → Strike:        15-25% OTM from current price\n"
                    + "  → Expiry:        Furthest available >= 18 months\n\n"
                    + "  This IV floor window typically lasts 1-5 trading days.\n"
                    + "  Act promptly once you decide — IV can spike overnight.\n"
                    + (f"\n  Current price: ${price:.2f}\n" if price else "")
                ),
                context={"iv_rank": iv_rank, "entry_score": score, "ticker": ticker},
            )
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
            "  • Target return: 5-10x from this entry\n\n"
            + _iv_entry_context(iv_rank)
            if score >= 60 else
            "  Keep monitoring. Alert again when score reaches 60+.\n\n"
            + _iv_entry_context(iv_rank)
        )
    )

    emoji = "🟢" if score >= 60 else "🟡"
    return Alert(
        type     = "ENTRY_SIGNAL" if score >= 60 else "ENTRY_WATCH",
        severity = severity,
        subject  = f"{emoji} {action}: {ticker}  Entry Score {score}/100"
                   + (f"  IV Rank {iv_rank:.0f}%" if iv_rank is not None else ""),
        body     = body,
        context  = {"entry_score": score, "weekly_rsi": weekly_rsi,
                    "iv_rank": iv_rank, "pct_from_low": pct_from_low,
                    "above_ma50": above_ma50, "above_ma200": above_ma200},
    )
