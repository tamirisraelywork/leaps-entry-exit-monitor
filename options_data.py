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


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _bs_greeks(S: float, K: float, T: float, r: float, sigma: float,
               option_type: str = "C") -> dict:
    """
    Full Black-Scholes Greeks for a European option.
    Returns dict with delta, gamma, theta (per day), vega (per 1% IV move).
    Returns empty dict on bad inputs.
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return {}
    try:
        d1    = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2    = d1 - sigma * math.sqrt(T)
        pdf1  = _norm_pdf(d1)
        is_call = option_type.upper() == "C"

        delta = _norm_cdf(d1) if is_call else (_norm_cdf(d1) - 1.0)
        gamma = pdf1 / (S * sigma * math.sqrt(T))
        vega  = S * pdf1 * math.sqrt(T) * 0.01          # per 1% change in IV
        theta_raw = (
            -(S * pdf1 * sigma) / (2 * math.sqrt(T))
            - r * K * math.exp(-r * T) * (_norm_cdf(d2) if is_call else _norm_cdf(-d2))
        )
        theta = theta_raw / 365.0                         # per calendar day

        # Sanity-check delta
        if not (0.001 <= abs(delta) <= 0.999):
            delta = None

        return {
            "delta": round(delta, 4) if delta is not None else None,
            "gamma": round(gamma, 6),
            "theta": round(theta, 4),
            "vega":  round(vega,  4),
        }
    except Exception:
        return {}


def _bs_delta(S: float, K: float, T: float, r: float, sigma: float, option_type: str = "C") -> float | None:
    """Black-Scholes delta only (kept for backward compatibility)."""
    return _bs_greeks(S, K, T, r, sigma, option_type).get("delta")


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
    Fetch option bid/ask/IV via yfinance and compute all Greeks via Black-Scholes.
    Retries up to 3 times on rate-limit errors with exponential back-off.
    """
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)

        # Fetch available expiries — retry on rate-limit (429)
        available = None
        last_err  = ""
        for _wait in (0, 3, 8):
            if _wait:
                time.sleep(_wait)
            try:
                available = t.options
                break
            except Exception as e:
                last_err = str(e)
                if not any(x in last_err for x in ("Too Many Requests", "rate limit", "429")):
                    return {"_error": f"yfinance could not fetch option chain: {e}"}
        if available is None:
            return {"_error": f"yfinance rate-limited after retries: {last_err}"}
        if not available:
            return {"_error": "yfinance returned no expiry dates for this ticker"}

        target_str = expiry.strftime("%Y-%m-%d")
        if target_str not in available:
            target_str = min(available, key=lambda d: abs((date.fromisoformat(d) - expiry).days))
        actual_expiry = date.fromisoformat(target_str)

        # Fetch option chain — also retry on rate-limit
        chain = None
        for _wait in (0, 3, 8):
            if _wait:
                time.sleep(_wait)
            try:
                chain = t.option_chain(target_str)
                break
            except Exception as e:
                last_err = str(e)
                if not any(x in last_err for x in ("Too Many Requests", "rate limit", "429")):
                    return {"_error": f"yfinance option_chain failed: {e}"}
        if chain is None:
            return {"_error": f"yfinance rate-limited after retries: {last_err}"}

        df = chain.calls if option_type.upper() == "C" else chain.puts
        exact = df[df["strike"] == float(strike)]
        if exact.empty:
            exact = df.iloc[(df["strike"] - float(strike)).abs().argsort()[:1]]
        if exact.empty:
            return {"_error": f"yfinance: no option near strike {strike} for {ticker} {target_str}"}

        row  = exact.iloc[0]
        bid  = float(row.get("bid")       or 0)
        ask  = float(row.get("ask")       or 0)
        last = float(row.get("lastPrice") or 0)
        mid  = round((bid + ask) / 2, 2) if (bid > 0 and ask > 0) else (last or None)

        # IV — cap at 200% to avoid BS degeneration on illiquid options
        raw_iv = float(row.get("impliedVolatility") or 0)
        iv = min(raw_iv, 2.0) if raw_iv > 0 else None

        # Full Black-Scholes Greeks (delta, gamma, theta, vega)
        greeks = {}
        try:
            fi = t.fast_info
            S  = fi.last_price or fi.previous_close
            if S and iv and iv > 0.01:
                T      = max((actual_expiry - date.today()).days, 1) / 365.0
                greeks = _bs_greeks(float(S), float(strike), T, 0.045, iv, option_type)
        except Exception:
            pass

        return {
            "delta":              greeks.get("delta"),
            "gamma":              greeks.get("gamma"),
            "theta":              greeks.get("theta"),
            "vega":               greeks.get("vega"),
            "implied_volatility": iv,
            "bid":                bid,
            "ask":                ask,
            "mid":                mid,
            "expiration_date":    actual_expiry,
            "dte":                (actual_expiry - date.today()).days,
            "strike":             float(row.get("strike", strike)),
            "open_interest":      int(row.get("openInterest") or 0) or None,
            "_source":            "yfinance+BS",
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

@st.cache_data(ttl=600, show_spinner=False)
def get_option_snapshot(ticker: str, contract: str) -> dict:
    """
    Fetch live option data: bid/ask/mid, Greeks (delta/gamma/theta/vega), DTE, IV.

    Tries Polygon first (if API key present). Falls back to yfinance + Black-Scholes
    automatically — full Greeks available with no paid subscription.

    Results cached for 10 minutes to avoid rate-limiting when the dashboard
    loads multiple positions in quick succession.

    Returns a dict. Check '_error' key for failures; '_source' key
    indicates where data came from ('polygon' or 'yfinance+BS').
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
