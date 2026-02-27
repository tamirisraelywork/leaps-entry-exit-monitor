"""
Polygon.io integration — fetch live option Greeks, snapshots, and options chains.
All data is sourced from Polygon.io v3 APIs.
"""

import time
import requests
import streamlit as st
from datetime import date, timedelta


def _api_key() -> str:
    return st.secrets.get("POLYGON_API_KEY_1") or st.secrets.get("POLYGON_API_KEY_2", "")


def to_occ(ticker: str, expiry: date, option_type: str, strike: float) -> str:
    """
    Build an OCC option symbol from human-readable inputs.

    Example: NVDA, 2027-01-15, 'C', 150.0  →  O:NVDA270115C00150000
    """
    exp = expiry.strftime("%y%m%d")
    strike_str = f"{int(round(strike * 1000)):08d}"
    return f"O:{ticker.upper()}{exp}{option_type.upper()}{strike_str}"


def get_option_snapshot(ticker: str, contract: str) -> dict | None:
    """
    Fetch live greeks + bid/ask for a specific option contract.

    Args:
        ticker:   Underlying stock ticker (e.g. 'NVDA')
        contract: OCC symbol (e.g. 'O:NVDA270115C00150000')

    Returns dict with: delta, gamma, theta, vega, implied_volatility,
                       bid, ask, mid, dte, expiration_date, strike,
                       open_interest, _error (on failure).
    """
    key = _api_key()
    if not key:
        return {"_error": "No Polygon API key configured"}

    url = f"https://api.polygon.io/v3/snapshot/options/{ticker.upper()}/{contract}"
    try:
        resp = requests.get(url, params={"apiKey": key}, timeout=15)
        if resp.status_code == 403:
            return {"_error": "Polygon API key does not have options access (upgrade required)"}
        if resp.status_code == 404:
            return {"_error": f"Contract not found on Polygon: {contract}"}
        if resp.status_code != 200:
            return {"_error": f"Polygon returned HTTP {resp.status_code}"}

        body = resp.json()
        data = body.get("results", {})
        if not data:
            # status field sometimes explains an empty result
            status = body.get("status", "")
            return {"_error": f"Polygon returned empty results (status: {status})"}

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
        }
    except Exception as e:
        return {"_error": str(e)}


def get_leaps_chain(ticker: str, min_dte: int = 540) -> list[dict]:
    """
    Fetch all LEAPS call contracts for a ticker with at least min_dte days
    to expiration, including their greeks.

    Uses the /v3/snapshot/options/{ticker} bulk endpoint so a single API call
    returns all options + greeks (no per-contract calls needed).

    Returns a list of dicts, each containing contract OCC symbol + snapshot data.
    """
    min_date = (date.today() + timedelta(days=min_dte)).isoformat()
    url = f"https://api.polygon.io/v3/snapshot/options/{ticker.upper()}"
    params = {
        "contract_type":        "call",
        "expiration_date.gte":  min_date,
        "limit":                250,
        "apiKey":               _api_key(),
    }
    try:
        resp = requests.get(url, params=params, timeout=20)
        if resp.status_code != 200:
            return []
        results = resp.json().get("results", [])
    except Exception:
        return []

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

        contract_ticker = item.get("details", {}).get("ticker") or ""

        chain.append({
            "contract":           contract_ticker,
            "strike":             details.get("strike_price"),
            "expiration_date":    expiry,
            "dte":                dte,
            "delta":              greeks.get("delta"),
            "gamma":              greeks.get("gamma"),
            "theta":              greeks.get("theta"),
            "vega":               greeks.get("vega"),
            "implied_volatility": item.get("implied_volatility"),
            "bid":                bid,
            "ask":                ask,
            "mid":                mid,
            "open_interest":      item.get("open_interest"),
        })

    return chain


def get_stock_price(ticker: str) -> float | None:
    """
    Get latest stock price from Polygon.io.
    Falls back to previous close if real-time is unavailable.
    """
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker.upper()}"
    try:
        resp = requests.get(url, params={"apiKey": _api_key()}, timeout=10)
        if resp.status_code == 200:
            data = resp.json().get("ticker", {})
            # Try real-time last trade first, then day close, then prev close
            price = (
                (data.get("lastTrade") or {}).get("p")
                or (data.get("day") or {}).get("c")
                or (data.get("prevDay") or {}).get("c")
            )
            if price:
                return float(price)
    except Exception:
        pass
    return None


def get_roll_contract_price(ticker: str, strike: float, expiry: date, option_type: str = "C") -> float | None:
    """
    Get the current mid-price for a potential roll target contract.
    Used by exit_engine to determine if a roll is cost-effective.
    """
    contract = to_occ(ticker, expiry, option_type, strike)
    snapshot = get_option_snapshot(ticker, contract)
    return snapshot["mid"] if snapshot else None
