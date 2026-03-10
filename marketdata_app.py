"""
MarketData.app API — option quotes with full greeks.

Endpoint: GET https://api.marketdata.app/v1/options/quotes/{symbol}/
Auth:     Authorization: Bearer {MARKETDATA_TOKEN}
Symbol:   Standard OCC format without "O:" prefix
          e.g. XPOF261218C00015000

Response fields (all arrays, index 0 for single contract):
  bid, ask, mid, last, iv, delta, gamma, theta, vega, openInterest, updated

Add to .streamlit/secrets.toml:
  MARKETDATA_TOKEN = "your_token_here"
"""

import requests
import time
import threading
from datetime import date

from shared.config import cfg

_lock = threading.Lock()
_last_call: float = 0.0
_MIN_GAP = 0.2   # 200ms between calls


def _throttle():
    global _last_call
    with _lock:
        gap = _MIN_GAP - (time.time() - _last_call)
        if gap > 0:
            time.sleep(gap)
        _last_call = time.time()


def _token() -> str | None:
    return cfg("MARKETDATA_TOKEN") or None


def _to_symbol(contract: str) -> str:
    """Strip our internal 'O:' prefix — marketdata.app uses plain OCC."""
    return contract[2:] if contract.startswith("O:") else contract


def _safe(val):
    try:
        f = float(val)
        return None if f != f else f   # NaN guard
    except (TypeError, ValueError):
        return None


def get_option_quote(ticker: str, contract: str) -> dict:
    """
    Fetch option bid/ask/mid/greeks/IV from marketdata.app.

    Returns a dict with: bid, ask, mid, delta, gamma, theta, vega,
    implied_volatility, dte, expiration_date, strike, open_interest,
    _source, _mid_source, _mid_is_live.

    Returns {"_error": "..."} on failure.
    """
    token = _token()
    if not token:
        return {"_error": "marketdata.app: no MARKETDATA_TOKEN configured"}

    symbol = _to_symbol(contract)
    _throttle()

    try:
        resp = requests.get(
            f"https://api.marketdata.app/v1/options/quotes/{symbol}/",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            },
            timeout=10,
        )
    except Exception as e:
        return {"_error": f"marketdata.app: request failed: {e}"}

    if resp.status_code == 401:
        return {"_error": "marketdata.app: invalid token — check MARKETDATA_TOKEN secret"}
    if resp.status_code == 403:
        return {"_error": "marketdata.app: account not authorized"}
    if resp.status_code == 404:
        return {"_error": f"marketdata.app: contract not found ({symbol})"}
    if resp.status_code != 200:
        return {"_error": f"marketdata.app: HTTP {resp.status_code}"}

    try:
        body = resp.json()
        if body.get("s") != "ok":
            msg = body.get("errmsg") or body.get("s") or "unknown"
            return {"_error": f"marketdata.app: {msg}"}

        # All values are single-element arrays
        def _get(key):
            arr = body.get(key)
            return arr[0] if isinstance(arr, list) and arr else None

        bid  = _safe(_get("bid"))  or 0.0
        ask  = _safe(_get("ask"))  or 0.0
        last = _safe(_get("last")) or 0.0

        mid_is_live = (bid > 0 and ask > 0)
        if mid_is_live:
            mid        = round((bid + ask) / 2, 2)
            mid_source = "market"
        elif _safe(_get("mid")):
            mid        = _safe(_get("mid"))
            mid_source = "market"
        elif last > 0:
            mid        = last
            mid_source = "last trade (stale)"
        else:
            mid        = None
            mid_source = None

        iv    = _safe(_get("iv"))
        delta = _safe(_get("delta"))
        gamma = _safe(_get("gamma"))
        theta = _safe(_get("theta"))
        vega  = _safe(_get("vega"))

        # Parse expiry and DTE from the OCC symbol (6 chars after ticker)
        exp_date = None
        dte      = None
        try:
            from datetime import datetime as _dt
            # symbol = e.g. XPOF261218C00015000
            # find the date part: scan for the 6-digit block before C/P
            import re
            m = re.search(r'(\d{6})[CP]', symbol)
            if m:
                exp_date = _dt.strptime(m.group(1), "%y%m%d").date()
                dte = max(0, (exp_date - date.today()).days)
        except Exception:
            pass

        # Parse strike from symbol
        strike = None
        try:
            import re
            m = re.search(r'[CP](\d{8})', symbol)
            if m:
                strike = int(m.group(1)) / 1000.0
        except Exception:
            pass

        if mid is None:
            return {"_error": "marketdata.app: no price data (bid=ask=last=0)"}

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
            "open_interest":      int(_safe(_get("openInterest")) or 0) or None,
            "_source":            "marketdata.app",
            "_mid_source":        mid_source,
            "_mid_is_live":       mid_is_live,
        }

    except Exception as e:
        return {"_error": f"marketdata.app: failed to parse response: {e}"}
