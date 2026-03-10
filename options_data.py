"""
Options data — Polygon.io primary, yfinance fallback.

get_option_snapshot() tries Polygon first. If Polygon fails (free tier,
no access, etc.) it automatically falls back to yfinance option chains
with Black-Scholes delta computed from IV + stock price.

This means the app works fully without a paid Polygon subscription.
"""

import math
import time
import threading
import requests
from datetime import date, datetime, timedelta

from shared.config import cfg
import tradier_data

# ---------------------------------------------------------------------------
# Module-level TTL cache — avoids re-hitting yfinance on every page reload
# ---------------------------------------------------------------------------
_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 600   # 10 minutes

# ---------------------------------------------------------------------------
# Global yfinance rate limiter — serializes ALL yfinance network calls
# so 16 simultaneous dashboard loads don't burst Yahoo Finance.
# Ensures ≥1.5 s between consecutive yfinance requests across all threads.
# ---------------------------------------------------------------------------
_yf_gate = threading.Lock()
_yf_last_call_ts: float = 0.0
_YF_CALL_GAP = 1.5   # seconds between yfinance network calls


def _yf_throttle():
    """Acquire the rate gate and wait if needed before making a yfinance call."""
    global _yf_last_call_ts
    with _yf_gate:
        gap = _YF_CALL_GAP - (time.time() - _yf_last_call_ts)
        if gap > 0:
            time.sleep(gap)
        _yf_last_call_ts = time.time()


def _cache_get(key: str):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (time.time() - entry[0]) < _CACHE_TTL:
            return entry[1]
    return None


def _cache_set(key: str, value):
    with _cache_lock:
        _cache[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _api_key() -> str:
    return cfg("POLYGON_API_KEY_1") or cfg("POLYGON_API_KEY_2")


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

        # Sanity-check delta — 0.0001 lower bound to preserve deeply OTM LEAPS deltas
        if not (0.0001 <= abs(delta) <= 0.9999):
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

        # Fetch available expiries — global rate gate prevents burst across threads
        _yf_throttle()
        try:
            available = t.options
        except Exception as e:
            last_err = str(e)
            if any(x in last_err for x in ("Too Many Requests", "rate limit", "429")):
                # One retry after a longer wait
                time.sleep(10)
                try:
                    _yf_throttle()
                    available = t.options
                except Exception as e2:
                    return {"_error": f"yfinance rate-limited after retries: {e2}"}
            else:
                return {"_error": f"yfinance could not fetch option chain: {e}"}
        if not available:
            return {"_error": "yfinance returned no expiry dates for this ticker"}

        target_str = expiry.strftime("%Y-%m-%d")
        if target_str not in available:
            target_str = min(available, key=lambda d: abs((date.fromisoformat(d) - expiry).days))
        actual_expiry = date.fromisoformat(target_str)

        # Fetch option chain — throttle before this call too
        _yf_throttle()
        try:
            chain = t.option_chain(target_str)
        except Exception as e:
            last_err = str(e)
            if any(x in last_err for x in ("Too Many Requests", "rate limit", "429")):
                time.sleep(10)
                try:
                    _yf_throttle()
                    chain = t.option_chain(target_str)
                except Exception as e2:
                    return {"_error": f"yfinance rate-limited after retries: {e2}"}
            else:
                return {"_error": f"yfinance option_chain failed: {e}"}
        last_err = ""

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
        mid  = round((bid + ask) / 2, 2) if (bid > 0 and ask > 0) else None

        # IV — cap at 200% to avoid BS degeneration on illiquid options
        raw_iv = float(row.get("impliedVolatility") or 0)
        iv = min(raw_iv, 2.0) if raw_iv > 0 else None

        # Black-Scholes Greeks (delta, gamma, theta, vega) — computed from IV + stock price
        greeks = {}
        mid_source = "market"
        try:
            _yf_throttle()
            fi = t.fast_info
            S  = fi.last_price or fi.previous_close
            if S and iv and iv > 0.01:
                T      = max((actual_expiry - date.today()).days, 1) / 365.0
                greeks = _bs_greeks(float(S), float(strike), T, 0.045, iv, option_type)
        except Exception:
            pass

        # For illiquid options with no live bid/ask, fall back to last traded price.
        # Mark it as stale — callers must NOT use this for P&L or stop-loss decisions.
        mid_is_live = (bid > 0 and ask > 0)
        if mid is None:
            if last > 0:
                mid = last
                mid_source = "last trade (stale)"

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
            "_mid_source":        mid_source,
            "_mid_is_live":       mid_is_live,
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
        mid_is_live = bool(ask and bid)

        exp_str = details.get("expiration_date", "")
        try:
            expiry = date.fromisoformat(exp_str)
            dte = max(0, (expiry - date.today()).days)
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
            "_mid_is_live":       mid_is_live,
        }
    except Exception as e:
        return {"_error": f"Polygon: {e}"}


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def _historical_vol(ticker: str, days: int = 30) -> float | None:
    """
    Compute annualized historical volatility from the last N trading days.
    Used as an IV proxy for delta calculation when option IV is unavailable.
    """
    try:
        import yfinance as yf
        import math as _math
        _yf_throttle()
        hist = yf.Ticker(ticker).history(period=f"{days + 10}d")
        if len(hist) < 5:
            return None
        closes = hist["Close"].dropna()
        returns = [_math.log(closes.iloc[i] / closes.iloc[i - 1])
                   for i in range(1, min(days + 1, len(closes)))]
        if len(returns) < 5:
            return None
        mean = sum(returns) / len(returns)
        variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
        return (_math.sqrt(variance) * _math.sqrt(252))
    except Exception:
        return None


def _fill_missing_delta(result: dict, ticker: str, parsed: dict | None) -> dict:
    """
    If delta is missing (illiquid option, no IV), compute it using the stock's
    historical volatility as an IV proxy. Marks result as 'estimated'.
    """
    if result.get("delta") is not None:
        return result
    if not parsed:
        return result
    try:
        import yfinance as yf
        _yf_throttle()
        S = yf.Ticker(ticker).fast_info.last_price
        if not S:
            return result
        expiry = parsed["expiry"]
        strike = parsed["strike"]
        opt_type = parsed["option_type"]
        T = max((expiry - date.today()).days, 1) / 365.0
        hv = _historical_vol(ticker)
        if not hv:
            return result
        greeks = _bs_greeks(float(S), float(strike), T, 0.045, hv, opt_type)
        if greeks.get("delta") is not None:
            result["delta"] = greeks["delta"]
            result["gamma"] = greeks.get("gamma")
            result["theta"] = greeks.get("theta")
            result["vega"]  = greeks.get("vega")
            result["_delta_source"] = "historical_vol_proxy"
    except Exception:
        pass
    return result


def get_option_snapshot(ticker: str, contract: str) -> dict:
    """
    Fetch live option data: bid/ask/mid, Greeks (delta/gamma/theta/vega), DTE, IV.

    Source priority (tries each in order, stops at first success):
      1. Tradier   — real-time NBBO for ALL listed options (free with Tradier account)
                     Requires TRADIER_TOKEN secret. Most reliable for illiquid LEAPS.
      2. Polygon   — real-time snapshots (requires paid plan ≥$29/mo)
      3. yfinance  — free but unreliable for illiquid options (may show stale lastPrice)

    If delta is missing (no IV for illiquid options), falls back to computing
    delta from the underlying stock's 30-day historical volatility.

    Results cached 10 minutes. Errors cached 1 minute.
    """
    cache_key = f"{ticker}::{contract}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    parsed = _parse_occ(contract, ticker)
    errors = {}

    # --- 1. Try Tradier (real-time NBBO, best for illiquid options) ---
    tradier_result = tradier_data.get_option_quote(ticker, contract)
    if "_error" not in tradier_result:
        tradier_result = _fill_missing_delta(tradier_result, ticker, parsed)
        _cache_set(cache_key, tradier_result)
        return tradier_result
    errors["tradier"] = tradier_result["_error"]

    # --- 2. Try Polygon (real-time, requires paid plan) ---
    poly_key = _api_key()
    if poly_key:
        poly_result = _snapshot_via_polygon(ticker, contract, poly_key)
        if "_error" not in poly_result:
            poly_result = _fill_missing_delta(poly_result, ticker, parsed)
            _cache_set(cache_key, poly_result)
            return poly_result
        errors["polygon"] = poly_result["_error"]
    else:
        errors["polygon"] = "no API key"

    # --- 3. Fall back to yfinance ---
    if not parsed:
        err_result = {"_error": f"Could not parse contract symbol '{contract}'"}
        with _cache_lock:
            _cache[cache_key] = (time.time() - _CACHE_TTL + 60, err_result)
        return err_result

    yf_result = _snapshot_via_yfinance(
        ticker,
        parsed["expiry"],
        parsed["strike"],
        parsed["option_type"],
    )

    if "_error" not in yf_result:
        yf_result = _fill_missing_delta(yf_result, ticker, parsed)
        _cache_set(cache_key, yf_result)
        return yf_result
    errors["yfinance"] = yf_result.get("_error", "unknown")

    # All sources failed — cache briefly, surface clear error
    err_msg = " | ".join(f"{k}: {v}" for k, v in errors.items())
    err_result = {"_error": f"All sources failed — {err_msg}"}
    with _cache_lock:
        _cache[cache_key] = (time.time() - _CACHE_TTL + 60, err_result)
    return err_result


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
