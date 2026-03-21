"""
Option Recommendation Engine.

Given a stock ticker the user is interested in, this module:
1. Fetches all available LEAPS call contracts (>= 18 months out) from Polygon.io
2. For each contract, calculates the required stock move to achieve 3x, 5x, and 10x
3. Classifies each contract into one of three portfolio roles:
     MOONSHOT  — targeting ~10x (far OTM, delta 0.10-0.25)
     CORE      — targeting 3-5x (moderate OTM, delta 0.30-0.55)
     SKIP      — either too cheap (no realistic path) or too expensive (stock-like)
4. Returns the top 2 candidates per role, sorted by "quality score"

Portfolio strategy this engine is designed for:
  - 10 total LEAPS positions
  - 2 MOONSHOT positions (accept ~80% chance of full loss, targeting 10x on winners)
  - 4 CORE positions    (moderate risk, targeting 3-5x)
  - 4 remaining         (tactical / exit-managed)
"""
from __future__ import annotations


from datetime import date
from options_data import get_leaps_chain, get_stock_price


# ---------------------------------------------------------------------------
# Return calculation
# ---------------------------------------------------------------------------

def calculate_return_targets(stock_price: float, strike: float, premium: float) -> dict:
    """
    At expiration, call option value ≈ max(stock_price_at_expiry - strike, 0).
    For a given return multiple N, we need:
        stock_at_expiry = strike + (N × premium)
    Required percentage move from today:
        move = (stock_at_expiry / stock_price - 1) × 100
    """
    if not (stock_price and strike and premium and premium > 0):
        return {}

    targets = {}
    for multiple in (3, 5, 10):
        required_price = strike + (multiple * premium)
        move_pct = ((required_price / stock_price) - 1) * 100
        targets[f"{multiple}x_required_move_pct"] = round(move_pct, 1)
        targets[f"{multiple}x_required_price"]    = round(required_price, 2)

    return targets


# ---------------------------------------------------------------------------
# Contract scoring
# ---------------------------------------------------------------------------

def _quality_score(contract: dict, role: str) -> float:
    """
    Score a contract within its role (higher = better recommendation).

    For MOONSHOT: reward far OTM but achievable (not insane) moves.
                  Penalise very low open interest (illiquid).
    For CORE:     reward good delta range and reasonable move needed for 3x.
    """
    delta = abs(contract.get("delta") or 0)
    oi    = contract.get("open_interest") or 0
    move3 = contract.get("3x_required_move_pct", 999)
    move10= contract.get("10x_required_move_pct", 999)
    dte   = contract.get("dte", 0)

    score = 0.0

    if role == "MOONSHOT":
        # Ideal: 10x requires 50-80% stock move, delta 0.10-0.20
        if 40 <= move10 <= 90:    score += 40
        elif 30 <= move10 <= 120: score += 20
        if 0.10 <= delta <= 0.20: score += 30
        elif 0.20 < delta <= 0.25:score += 15
        if dte >= 540:            score += 20   # prefer 18+ months
        if oi  >= 500:            score += 10   # liquidity bonus

    elif role == "CORE":
        # Ideal: 3x requires 30-55% move, delta 0.35-0.50
        if 25 <= move3 <= 55:     score += 40
        elif 20 <= move3 <= 70:   score += 20
        if 0.35 <= delta <= 0.50: score += 30
        elif 0.30 <= delta < 0.35:score += 15
        elif 0.50 < delta <= 0.55:score += 15
        if dte >= 540:            score += 20
        if oi  >= 500:            score += 10

    return score


# ---------------------------------------------------------------------------
# Main recommendation function
# ---------------------------------------------------------------------------

def recommend_options(ticker: str) -> dict:
    """
    Fetch LEAPS chain and return the best MOONSHOT and CORE contracts.

    Returns:
        {
          "stock_price": float,
          "MOONSHOT": [list of top contracts, best first],
          "CORE":     [list of top contracts, best first],
          "error":    str or None,
        }
    """
    stock_price = get_stock_price(ticker)
    if not stock_price:
        return {"error": f"Could not fetch stock price for {ticker}. Check the ticker.", "MOONSHOT": [], "CORE": []}

    chain = get_leaps_chain(ticker, min_dte=540)  # tries 18m → 12m → 9m fallback
    if not chain:
        return {
            "stock_price": stock_price,
            "error": f"No LEAPS options found for {ticker} (tried 18, 12, and 9 months out). "
                     "This ticker may not have long-dated options available.",
            "MOONSHOT": [],
            "CORE": [],
        }

    # Note the actual min DTE in the chain so the UI can show the right context
    actual_min_dte = min(c.get("dte", 0) for c in chain)

    moonshots = []
    cores     = []

    for c in chain:
        delta   = abs(c.get("delta") or 0)
        mid     = c.get("mid")
        strike  = c.get("strike")

        if not (mid and mid > 0 and strike):
            continue  # skip contracts with no price

        # Calculate return targets
        targets = calculate_return_targets(stock_price, strike, mid)
        c.update(targets)

        move10 = c.get("10x_required_move_pct", 999)
        move3  = c.get("3x_required_move_pct", 999)

        # Classify into role
        is_moonshot = (0.08 <= delta <= 0.28) and (30 <= move10 <= 120)
        is_core     = (0.28 <= delta <= 0.58) and (20 <= move3  <= 80)

        if is_moonshot:
            c["role"]  = "MOONSHOT"
            c["score"] = _quality_score(c, "MOONSHOT")
            moonshots.append(c)

        elif is_core:
            c["role"]  = "CORE"
            c["score"] = _quality_score(c, "CORE")
            cores.append(c)

    moonshots.sort(key=lambda x: x["score"], reverse=True)
    cores.sort(    key=lambda x: x["score"], reverse=True)

    return {
        "stock_price":   stock_price,
        "error":         None,
        "actual_min_dte": actual_min_dte,
        "MOONSHOT":      moonshots[:3],
        "CORE":          cores[:3],
    }


def recommend_asymmetric(ticker: str) -> dict:
    """
    Recommend the best LEAPS contracts for an asymmetric entry strategy:
      - Strike target: 40–50% above current stock price
      - Expiry: furthest available ≥ 18 months (maximize time value)
      - Strategy: accept high OTM for leveraged upside; premium stays low

    Returns:
        {
          "stock_price":    float,
          "target_strike_lo": float,   # 40% OTM
          "target_strike_hi": float,   # 50% OTM
          "contracts":      [list, best first],
          "error":          str | None,
        }
    """
    stock_price = get_stock_price(ticker)
    if not stock_price:
        return {"error": f"Could not fetch stock price for {ticker}.", "contracts": []}

    target_lo = round(stock_price * 1.40, 2)
    target_hi = round(stock_price * 1.50, 2)

    chain = get_leaps_chain(ticker, min_dte=540)  # tries 18m → 12m → 9m fallback
    if not chain:
        return {
            "stock_price":      stock_price,
            "target_strike_lo": target_lo,
            "target_strike_hi": target_hi,
            "error": f"No LEAPS options found for {ticker} (tried 18, 12, and 9 months out).",
            "contracts": [],
        }

    candidates = []
    # Widen tolerance: accept strikes 35-60% OTM so we always return something
    for c in chain:
        strike = c.get("strike")
        mid    = c.get("mid")
        if not (strike and mid and mid > 0):
            continue
        otm_pct = (strike / stock_price - 1) * 100
        if not (35 <= otm_pct <= 65):
            continue

        targets = calculate_return_targets(stock_price, strike, mid)
        c.update(targets)

        # Quality score: prefer strikes closer to 40-50% band + longer expiry + liquidity
        closeness = max(0, 10 - abs(otm_pct - 45))        # 0-10: perfect = 45% OTM
        dte_bonus  = 15 if c.get("dte", 0) >= 600 else 5  # prefer 20+ months
        oi_bonus   = 8  if (c.get("open_interest") or 0) >= 500 else 0
        iv_penalty = -5 if (c.get("implied_volatility") or 0) > 0.80 else 0
        c["otm_pct"]      = round(otm_pct, 1)
        c["rec_score"]    = closeness + dte_bonus + oi_bonus + iv_penalty
        c["role"]         = "ASYMMETRIC"
        candidates.append(c)

    candidates.sort(key=lambda x: x["rec_score"], reverse=True)

    return {
        "stock_price":      stock_price,
        "target_strike_lo": target_lo,
        "target_strike_hi": target_hi,
        "error":            None if candidates else f"No contracts found in 35-65% OTM range for {ticker}.",
        "contracts":        candidates[:3],
    }


def format_recommendation(c: dict, stock_price: float) -> str:
    """Format a single contract recommendation as a human-readable string."""
    expiry  = c.get("expiration_date", "")
    strike  = c.get("strike", "?")
    mid     = c.get("mid", "?")
    delta   = c.get("delta", "?")
    dte     = c.get("dte", "?")
    move3   = c.get("3x_required_move_pct",  "?")
    move10  = c.get("10x_required_move_pct", "?")
    role    = c.get("role", "")

    if role == "MOONSHOT":
        target_line = f"10x needs stock at +{move10}%  (${c.get('10x_required_price','?')})"
    else:
        target_line = (
            f"3x needs +{move3}%  "
            f"5x needs +{c.get('5x_required_move_pct','?')}%  "
            f"10x needs +{move10}%"
        )

    return (
        f"{expiry}  ${strike} Call  |  Premium: ${mid}/share\n"
        f"Delta: {delta}  |  DTE: {dte} days\n"
        f"{target_line}"
    )
