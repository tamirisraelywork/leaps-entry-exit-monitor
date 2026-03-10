"""
Tradier Markets API — real-time option quotes for all listed options.

Tradier provides NBBO (National Best Bid/Offer) data from OPRA for every
listed option contract, regardless of whether it has traded recently. This
solves the yfinance limitation where illiquid LEAPS show bid=ask=0.

Setup (one-time, free):
  1. Go to https://developer.tradier.com — sign up for a free account
  2. Get your API access token from the developer dashboard
  3. Add to secrets.toml: TRADIER_TOKEN = "your_token_here"
  4. For real-time data (not delayed), use a live Tradier brokerage account
     — paper trading accounts get real-time data too, just open one free.

The free sandbox gives 15-min delayed data.
The paper/live brokerage endpoint gives real-time data.
"""

import requests
import time
import threading
from datetime import date

from shared.config import cfg

# ---------------------------------------------------------------------------
# Rate limiter — Tradier free tier: 200 req/min, well within our usage
# ---------------------------------------------------------------------------
_lock = threading.Lock()
_last_call: float = 0.0
_MIN_GAP = 0.1  # 100ms between calls (far under 200 req/min limit)


def _throttle():
    global _last_call
    with _lock:
        gap = _MIN_GAP - (time.time() - _last_call)
        if gap > 0:
            time.sleep(gap)
        _last_call = time.time()


def _token() -> str | None:
    return cfg("TRADIER_TOKEN") or None


def _base_url() -> str:
    """Use production endpoint (real-time). Falls back to sandbox (delayed)."""
    return "https://api.tradier.com/v1"


def _occ_to_tradier(contract: str) -> str:
    """
    Convert our OCC symbol format to Tradier's format.
    'O:XPOF261218C00015000' → 'XPOF261218C00015000'
    """
    return contract.lstrip("O:") if contract.startswith("O:") else contract


def get_option_quote(ticker: str, contract: str) -> dict:
    """
    Fetch live bid/ask/greeks for a single option contract from Tradier.

    Returns a dict compatible with options_data.get_option_snapshot():
      bid, ask, mid, delta, gamma, theta, vega, implied_volatility,
      dte, expiration_date, _source, _mid_is_live

    Returns {"_error": "..."} on failure.
    """
    token = _token()
    if not token:
        return {"_error": "Tradier: no TRADIER_TOKEN configured"}

    symbol = _occ_to_tradier(contract)
    _throttle()

    try:
        resp = requests.get(
            f"{_base_url()}/markets/quotes",
            params={"symbols": symbol, "greeks": "true"},
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            },
            timeout=10,
        )
    except Exception as e:
        return {"_error": f"Tradier: request failed: {e}"}

    if resp.status_code == 401:
        return {"_error": "Tradier: invalid token — check TRADIER_TOKEN secret"}
    if resp.status_code == 403:
        return {"_error": "Tradier: account not authorized for market data"}
    if resp.status_code != 200:
        return {"_error": f"Tradier: HTTP {resp.status_code}"}

    try:
        body = resp.json()
        quotes = body.get("quotes") or {}
        quote  = quotes.get("quote")

        if not quote:
            return {"_error": f"Tradier: no quote returned for {symbol}"}

        # Tradier returns a list if multiple symbols, a dict if single
        if isinstance(quote, list):
            quote = quote[0] if quote else {}

        bid  = float(quote.get("bid")  or 0)
        ask  = float(quote.get("ask")  or 0)
        last = float(quote.get("last") or 0)

        mid_is_live = (bid > 0 and ask > 0)
        mid = round((bid + ask) / 2, 2) if mid_is_live else (last if last > 0 else None)
        mid_source = "market" if mid_is_live else ("last trade (stale)" if last > 0 else None)

        greeks_raw = quote.get("greeks") or {}
        delta = _safe_float(greeks_raw.get("delta"))
        gamma = _safe_float(greeks_raw.get("gamma"))
        theta = _safe_float(greeks_raw.get("theta"))
        vega  = _safe_float(greeks_raw.get("vega"))
        iv    = _safe_float(greeks_raw.get("mid_iv"))  # Tradier calls it mid_iv

        # Parse expiry and DTE
        exp_date = None
        dte      = None
        exp_str  = quote.get("expiration_date") or ""
        if exp_str:
            try:
                exp_date = date.fromisoformat(exp_str)
                dte = max(0, (exp_date - date.today()).days)
            except Exception:
                pass

        strike = _safe_float(quote.get("strike"))

        return {
            "bid":                bid,
            "ask":                ask,
            "mid":                mid,
            "delta":              delta,
            "gamma":              gamma,
            "theta":              theta,
            "vega":               vega,
            "implied_volatility": iv,
            "expiration_date":    exp_date,
            "dte":                dte,
            "strike":             strike,
            "open_interest":      int(quote.get("open_interest") or 0) or None,
            "_source":            "tradier",
            "_mid_source":        mid_source,
            "_mid_is_live":       mid_is_live,
        }

    except Exception as e:
        return {"_error": f"Tradier: failed to parse response: {e}"}


def _safe_float(val) -> float | None:
    try:
        f = float(val)
        return f if f == f else None  # NaN check
    except (TypeError, ValueError):
        return None
