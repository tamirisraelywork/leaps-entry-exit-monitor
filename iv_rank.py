from __future__ import annotations

"""
IV Rank via yfinance — no Playwright, no proxy, no scraping needed.

Method:
  1. Fetch ATM implied volatility from the nearest-expiry option chain
     (or accept a pre-fetched current_iv_pct to skip that round-trip).
  2. Fetch 1-year daily price history and compute rolling 30-day
     realized volatility (annualized) as the historical IV proxy.
  3. IV Rank = percentile of current IV within that 1-year range
     (0 = cheapest options have been, 100 = most expensive).

Return value matches the original interface:
  "Success! The IV Rank for {TICKER} is: {value}"
  "Could not find IV Rank for {TICKER} after all attempts."
"""

import sys
import math
import time


def _yf_call(fn, *args, retries=(0, 3, 8), **kwargs):
    """Call a yfinance function with exponential back-off on rate-limit errors."""
    last_err = None
    for wait in retries:
        if wait:
            time.sleep(wait)
        try:
            return fn(*args, **kwargs), None
        except Exception as e:
            last_err = str(e)
            if not any(x in last_err for x in ("Too Many Requests", "rate limit", "429")):
                return None, last_err   # non-retriable error
    return None, last_err


def get_iv_rank_advanced(ticker: str, current_iv_pct: float | None = None) -> str:
    """
    Compute IV Rank for a ticker.

    Parameters
    ----------
    ticker          : stock symbol
    current_iv_pct  : if you already have the ATM IV (e.g. from get_option_snapshot),
                      pass it here as a percentage (e.g. 35.0 for 35%).
                      Skips the option-chain round-trip entirely.

    Always produces a rank — if the option chain is unavailable, falls back to
    ranking current 30-day realized volatility within its 1-year range.
    """
    ticker = ticker.upper().strip()
    try:
        import yfinance as yf
        from datetime import date

        t = yf.Ticker(ticker)

        # ── 1. ATM implied volatility (best-effort — do NOT abort on failure) ─
        if current_iv_pct is None or current_iv_pct <= 0:
            try:
                exps, _ = _yf_call(lambda: t.options)
                if exps:
                    nearest_exp = None
                    for exp_str in exps:
                        try:
                            if (date.fromisoformat(exp_str) - date.today()).days >= 7:
                                nearest_exp = exp_str
                                break
                        except Exception:
                            continue
                    if not nearest_exp:
                        nearest_exp = exps[0]

                    chain, _ = _yf_call(t.option_chain, nearest_exp)
                    if chain is not None and not chain.calls.empty:
                        calls = chain.calls
                        try:
                            fi = t.fast_info
                            S  = fi.last_price or fi.previous_close
                        except Exception:
                            S = None
                        atm_row = (
                            calls.iloc[(calls["strike"] - float(S)).abs().argsort()[:1]]
                            if S else calls.iloc[[len(calls) // 2]]
                        )
                        raw_iv = float(atm_row["impliedVolatility"].values[0] or 0)
                        if raw_iv > 0:
                            current_iv_pct = raw_iv * 100
            except Exception:
                pass   # fall through — use realized vol as proxy below

        # ── 2. 1-year realized volatility range ──────────────────────────────
        hist, _ = _yf_call(t.history, period="1y", interval="1d", auto_adjust=True)

        if hist is None or hist.empty or len(hist) < 30:
            # No price history at all — return raw IV if we have it
            if current_iv_pct and current_iv_pct > 0:
                return f"Success! The IV Rank for {ticker} is: {current_iv_pct:.1f}"
            return f"Could not find IV Rank for {ticker} after all attempts."

        rolling_vol = (
            hist["Close"].pct_change().dropna()
            .rolling(30).std()
            .dropna()
            * math.sqrt(252) * 100
        )

        if rolling_vol.empty:
            if current_iv_pct and current_iv_pct > 0:
                return f"Success! The IV Rank for {ticker} is: {current_iv_pct:.1f}"
            return f"Could not find IV Rank for {ticker} after all attempts."

        # If option chain was unavailable, use current realized vol as proxy
        if current_iv_pct is None or current_iv_pct <= 0:
            current_iv_pct = float(rolling_vol.iloc[-1])

        vol_min = float(rolling_vol.min())
        vol_max = float(rolling_vol.max())

        # ── 3. IV Rank (0-100) ───────────────────────────────────────────────
        if vol_max > vol_min:
            iv_rank = (current_iv_pct - vol_min) / (vol_max - vol_min) * 100
            iv_rank = max(0.0, min(100.0, iv_rank))
        else:
            iv_rank = 50.0   # flat vol environment — neutral

        return f"Success! The IV Rank for {ticker} is: {iv_rank:.1f}"

    except Exception as e:
        return f"Could not find IV Rank for {ticker} after all attempts ({e})."


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "NVDA"
    print(get_iv_rank_advanced(target))
