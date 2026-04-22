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
from __future__ import annotations


import requests
import time
import threading
from datetime import date, timedelta

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
    if not (200 <= resp.status_code < 300):
        return {"_error": f"marketdata.app: HTTP {resp.status_code}"}

    try:
        body = resp.json()
        status = body.get("s")
        if status == "no_data":
            # marketdata.app returns this when there are genuinely no quotes right now
            # (market closed, illiquid, or after-hours). Soft miss — fall through to yfinance.
            return {"_error": "marketdata.app: no_data"}
        if status != "ok":
            msg = body.get("errmsg") or status or "unknown"
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
            "_mid_reliable":      True,   # marketdata.app prices (even last-trade) come from exchange feed — reliable for P&L
        }

    except Exception as e:
        return {"_error": f"marketdata.app: failed to parse response: {e}"}


def get_options_chain(
    ticker: str,
    min_dte: int = 270,
    side: str = "call",
) -> list[dict]:
    """
    Fetch options chain from marketdata.app /v1/options/chain/{symbol}/.

    Returns a list of contract dicts in the same format as options_data.get_leaps_chain().
    IV is returned as decimal by marketdata.app (0.35 = 35%).

    Used as fallback when yfinance has no options data for a ticker.
    """
    token = _token()
    if not token:
        return []

    min_expiry = (date.today() + timedelta(days=min_dte)).isoformat()
    _throttle()

    try:
        resp = requests.get(
            f"https://api.marketdata.app/v1/options/chain/{ticker.upper()}/",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            params={"side": side, "minExpiry": min_expiry},
            timeout=20,
        )
    except Exception:
        return []

    if not (200 <= resp.status_code < 300):
        return []

    try:
        body = resp.json()
        if body.get("s") not in ("ok",):
            return []

        def _col(key, idx):
            arr = body.get(key, [])
            try:
                v = arr[idx]
                return None if v is None else v
            except IndexError:
                return None

        symbols      = body.get("optionSymbol", [])
        expirations  = body.get("expiration", [])   # Unix timestamps

        contracts = []
        for i in range(len(symbols)):
            bid = _safe(_col("bid", i)) or 0.0
            ask = _safe(_col("ask", i)) or 0.0
            raw_mid = _safe(_col("mid", i))
            mid = raw_mid if raw_mid else (round((bid + ask) / 2, 2) if bid > 0 and ask > 0 else None)

            exp_ts = _col("expiration", i)
            try:
                from datetime import datetime as _dtt
                exp_date = _dtt.utcfromtimestamp(float(exp_ts)).date() if exp_ts else None
            except Exception:
                exp_date = None

            raw_dte = _col("dte", i)
            dte = int(raw_dte) if raw_dte is not None else (
                max(0, (exp_date - date.today()).days) if exp_date else None
            )

            contracts.append({
                "contract":           _col("optionSymbol", i),
                "strike":             _safe(_col("strike", i)),
                "expiration_date":    exp_date,
                "dte":                dte,
                "delta":              _safe(_col("delta", i)),
                "implied_volatility": _safe(_col("iv", i)),   # decimal, e.g. 0.35
                "bid":                _safe(bid),
                "ask":                _safe(ask),
                "mid":                mid,
                "open_interest":      int(_safe(_col("openInterest", i)) or 0) or None,
                "_source":            "marketdata.app",
            })

        return [c for c in contracts if c.get("mid") is not None]

    except Exception:
        return []
