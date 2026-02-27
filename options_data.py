"""
Options data — Polygon.io primary, yfinance fallback.

get_option_snapshot() tries Polygon first. If Polygon fails (free tier,
no access, etc.) it automatically falls back to yfinance option chains
with Black-Scholes delta computed from IV + stock price.

This means the app works fully without a paid Polygon subscription.
"""

import math
import time
import requests
import streamlit as st
from datetime import date, datetime, timedelta


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _api_key() -> str:
    return st.secrets.get("POLYGON_API_KEY_1") or st.secrets.get("POLYGON_API_KEY_2", "")


def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erf — no scipy needed."""
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


def _bs_delta(S: float, K: float, T: float, r: float, sigma: float, option_type: str = "C") -> float | None:
    """
    Black-Scholes delta for a European option.

    S     = current stock price
    K     = strike price
    T     = time to expiry in years
    r     = risk-free rate (e.g. 0.045 for 4.5%)
    sigma = implied volatility (e.g. 0.35 for 35%)
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return None
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        if option_type.upper() == "C":
            return round(_norm_cdf(d1), 4)
        else:
            return round(_norm_cdf(d1) - 1.0, 4)
    except Exception:
        return None


def _parse_occ(contract: str, ticker: str) -> dict | None:
    """
    Parse an OCC symbol back to (expiry, strike, option_type).
    e.g. 'O:NVDA270115C00150000' → {expiry: date(2027,1,15), strike: 150.0, option_type: 'C'}
    """
    try:
        s = contract
        if s.startswith("O:"):
            s = s[2:]
        ticker_upper = ticker.upper()
        if not s.startswith(ticker_upper):
            return None
        s = s[len(ticker_upper):]
        exp_date = datetime.strptime(s[:6], "%y%m%d").date()
        opt_type = s[6]   # 'C' or 'P'
        strike   = int(s[7:]) / 1000.0
        return {"expiry": exp_date, "strike": strike, "option_type": opt_type}
    except Exception:
        return None


def to_occ(ticker: str, expiry: date, option_type: str, strike: float) -> str:
    """
    Build an OCC option symbol from human-readable inputs.
    Example: NVDA, 2027-01-15, 'C', 150.0  →  O:NVDA270115C00150000
    """
    exp = expiry.strftime("%y%m%d")
    strike_str = f"{int(round(strike * 1000)):08d}"
    return f"O:{ticker.upper()}{exp}{option_type.upper()}{strike_str}"


# ---------------------------------------------------------------------------
# yfinance option snapshot (free, always available)
# ---------------------------------------------------------------------------

def _snapshot_via_yfinance(ticker: str, expiry: date, strike: float, option_type: str = "C") -> dict:
    """
    Fetch option bid/ask via yfinance and compute delta via Black-Scholes.

    Returns same keys as Polygon snapshot (mid, delta, dte, implied_volatility…)
    or {"_error": "..."} on failure.
    """
    try:
        import yfinance as yf

        t = yf.Ticker(ticker)

        # Find the closest available expiry
        try:
            available = t.options   # tuple of expiry date strings e.g. "2027-01-15"
        except Exception as e:
            return {"_error": f"yfinance could not fetch option chain: {e}"}

        if not available:
            return {"_error": "yfinance returned no expiry dates for this ticker"}

        target_str = expiry.strftime("%Y-%m-%d")
        if target_str not in available:
            # Use closest available expiry
            closest = min(
                available,
                key=lambda d: abs((date.fromisoformat(d) - expiry).days)
            )
            target_str = closest

        actual_expiry = date.fromisoformat(target_str)

        # Fetch option chain
        try:
            chain = t.option_chain(target_str)
        except Exception as e:
            return {"_error": f"yfinance option_chain failed: {e}"}

        df = chain.calls if option_type.upper() == "C" else chain.puts

        # Match exact strike; fall back to nearest
        exact = df[df["strike"] == float(strike)]
        if exact.empty:
            exact = df.iloc[(df["strike"] - float(strike)).abs().argsort()[:1]]
        if exact.empty:
            return {"_error": f"yfinance: no option near strike {strike} for {ticker} {target_str}"}

        row = exact.iloc[0]
        bid = float(row.get("bid") or 0)
        ask = float(row.get("ask") or 0)
        last = float(row.get("lastPrice") or 0)

        if bid > 0 and ask > 0:
            mid = round((bid + ask) / 2, 2)
        elif last > 0:
            mid = last
        else:
            mid = None

        iv = float(row.get("impliedVolatility") or 0) or None

        # Black-Scholes delta
        delta = None
        try:
            fi = t.fast_info
            S = fi.last_price or fi.previous_close
            if S and iv:
                T = max((actual_expiry - date.today()).days, 1) / 365.0
                delta = _bs_delta(float(S), float(strike), T, 0.045, iv, option_type)
        except Exception:
            pass

        dte = (actual_expiry - date.today()).days

        return {
            "delta":              delta,
            "gamma":              None,
            "theta":              None,
            "vega":               None,
            "implied_volatility": iv,
            "bid":                bid,
            "ask":                ask,
            "mid":                mid,
            "expiration_date":    actual_expiry,
            "dte":                dte,
            "strike":             float(row.get("strike", strike)),
            "open_interest":      int(row.get("openInterest") or 0) or None,
            "_source":            "yfinance",
        }

    except Exception as e:
        return {"_error": f"yfinance snapshot failed: {e}"}


# ---------------------------------------------------------------------------
# Polygon option snapshot
# ---------------------------------------------------------------------------

def _snapshot_via_polygon(ticker: str, contract: str, key: str) -> dict:
    """Attempt to fetch snapshot from Polygon. Returns _error key on failure."""
    url = f"https://api.polygon.io/v3/snapshot/options/{ticker.upper()}/{contract}"
    try:
        resp = requests.get(url, params={"apiKey": key}, timeout=15)
        if resp.status_code == 403:
            return {"_error": "Polygon: options access requires upgrade"}
        if resp.status_code == 404:
            return {"_error": f"Polygon: contract not found ({contract})"}
        if resp.status_code != 200:
            return {"_error": f"Polygon: HTTP {resp.status_code}"}

        body = resp.json()
        data = body.get("results", {})
        if not data:
            status = body.get("status", "")
            return {"_error": f"Polygon: empty results (status={status})"}

        greeks  = data.get("greeks") or {}
        quote   = data.get("last_quote") or {}
        details = data.get("details") or {}

        ask = quote.get("ask") or 0.0
        bid = quote.get("bid") or 0.0
        mid = round((ask + bid) / 2, 2) if (ask and bid) else None

        exp_str = details.get("expiration_date", "")
        try:
            expiry = date.fromisoformat(exp_str)
            dte = (expiry - date.today()).days
        except Exception:
            expiry = None
            dte = None

        return {
            "delta":              greeks.get("delta"),
            "gamma":              greeks.get("gamma"),
            "theta":              greeks.get("theta"),
            "vega":               greeks.get("vega"),
            "implied_volatility": data.get("implied_volatility"),
            "bid":                bid,
            "ask":                ask,
            "mid":                mid,
            "expiration_date":    expiry,
            "dte":                dte,
            "strike":             details.get("strike_price"),
            "open_interest":      data.get("open_interest"),
            "_source":            "polygon",
        }
    except Exception as e:
        return {"_error": f"Polygon: {e}"}


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def get_option_snapshot(ticker: str, contract: str) -> dict:
    """
    Fetch live option data: bid/ask/mid, delta, DTE, IV.

    Tries Polygon first (if API key present). Falls back to yfinance
    automatically — so this works even without a paid Polygon plan.

    Returns a dict. Check '_error' key for failures; '_source' key
    indicates where data came from ('polygon' or 'yfinance').
    """
    key = _api_key()

    # --- Try Polygon ---
    if key:
        result = _snapshot_via_polygon(ticker, contract, key)
        if "_error" not in result:
            return result
        polygon_err = result["_error"]
    else:
        polygon_err = "no API key"

    # --- Fall back to yfinance ---
    parsed = _parse_occ(contract, ticker)
    if not parsed:
        return {"_error": f"Could not parse contract symbol '{contract}'"}

    yf_result = _snapshot_via_yfinance(
        ticker,
        parsed["expiry"],
        parsed["strike"],
        parsed["option_type"],
    )

    if "_error" not in yf_result:
        return yf_result   # success via yfinance

    # Both failed
    return {
        "_error": f"All sources failed — Polygon: {polygon_err} | yfinance: {yf_result.get('_error', 'unknown')}"
    }


def get_leaps_chain(ticker: str, min_dte: int = 540) -> list[dict]:
    """
    Fetch all LEAPS call contracts for a ticker with at least min_dte days
    to expiration, including their greeks.

    Tries Polygon bulk endpoint first; falls back to yfinance option chain.
    """
    # --- Try Polygon ---
    key = _api_key()
    if key:
        min_date = (date.today() + timedelta(days=min_dte)).isoformat()
        url = f"https://api.polygon.io/v3/snapshot/options/{ticker.upper()}"
        params = {
            "contract_type":        "call",
            "expiration_date.gte":  min_date,
            "limit":                250,
            "apiKey":               key,
        }
        try:
            resp = requests.get(url, params=params, timeout=20)
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    chain = []
                    for item in results:
                        details = item.get("details") or {}
                        greeks  = item.get("greeks") or {}
                        quote   = item.get("last_quote") or {}
                        exp_str = details.get("expiration_date", "")
                        try:
                            expiry = date.fromisoformat(exp_str)
                            dte = (expiry - date.today()).days
                        except Exception:
                            continue
                        ask = quote.get("ask") or 0.0
                        bid = quote.get("bid") or 0.0
                        mid = round((ask + bid) / 2, 2) if (ask and bid) else None
                        chain.append({
                            "contract":           details.get("ticker", ""),
                            "strike":             details.get("strike_price"),
                            "expiration_date":    expiry,
                            "dte":                dte,
                            "delta":              greeks.get("delta"),
                            "implied_volatility": item.get("implied_volatility"),
                            "bid":                bid,
                            "ask":                ask,
                            "mid":                mid,
                            "open_interest":      item.get("open_interest"),
                        })
                    return chain
        except Exception:
            pass

    # --- Fall back to yfinance ---
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        available = t.options
        if not available:
            return []

        min_expiry = date.today() + timedelta(days=min_dte)
        leaps_dates = [d for d in available if date.fromisoformat(d) >= min_expiry]
        if not leaps_dates:
            return []

        # Get stock price once
        try:
            fi = t.fast_info
            S = fi.last_price or fi.previous_close
        except Exception:
            S = None

        chain = []
        for exp_str in leaps_dates:
            try:
                expiry = date.fromisoformat(exp_str)
                dte = (expiry - date.today()).days
                calls = t.option_chain(exp_str).calls
                time.sleep(0.5)  # gentle rate limiting

                for _, row in calls.iterrows():
                    strike = float(row.get("strike", 0))
                    bid    = float(row.get("bid") or 0)
                    ask    = float(row.get("ask") or 0)
                    last   = float(row.get("lastPrice") or 0)
                    iv     = float(row.get("impliedVolatility") or 0) or None
                    mid    = round((bid + ask) / 2, 2) if (bid > 0 and ask > 0) else (last or None)

                    delta = None
                    if S and iv:
                        T = max(dte, 1) / 365.0
                        delta = _bs_delta(S, strike, T, 0.045, iv, "C")

                    contract = to_occ(ticker, expiry, "C", strike)
                    chain.append({
                        "contract":           contract,
                        "strike":             strike,
                        "expiration_date":    expiry,
                        "dte":                dte,
                        "delta":              delta,
                        "implied_volatility": iv,
                        "bid":                bid,
                        "ask":                ask,
                        "mid":                mid,
                        "open_interest":      int(row.get("openInterest") or 0) or None,
                    })
            except Exception:
                continue

        return chain

    except Exception:
        return []


def get_stock_price(ticker: str) -> float | None:
    """
    Get latest stock price. Tries Polygon first, then yfinance.
    """
    key = _api_key()
    if key:
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker.upper()}"
        try:
            resp = requests.get(url, params={"apiKey": key}, timeout=10)
            if resp.status_code == 200:
                data = resp.json().get("ticker", {})
                price = (
                    (data.get("lastTrade") or {}).get("p")
                    or (data.get("day") or {}).get("c")
                    or (data.get("prevDay") or {}).get("c")
                )
                if price:
                    return float(price)
        except Exception:
            pass

    try:
        import yfinance as yf
        fi = yf.Ticker(ticker).fast_info
        return fi.last_price or fi.previous_close
    except Exception:
        return None


def get_roll_contract_price(ticker: str, strike: float, expiry: date, option_type: str = "C") -> float | None:
    """
    Get the current mid-price for a potential roll target contract.
    Used by exit_engine to determine if a roll is cost-effective.
    """
    contract = to_occ(ticker, expiry, option_type, strike)
    snapshot = get_option_snapshot(ticker, contract)
    return snapshot.get("mid") if snapshot and "_error" not in snapshot else None
