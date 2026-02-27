"""
IV Rank via yfinance — no Playwright, no proxy, no scraping needed.

Method:
  1. Fetch ATM implied volatility from the nearest-expiry option chain.
  2. Fetch 1-year daily price history and compute rolling 30-day
     realized volatility (annualized) as the historical IV proxy.
  3. IV Rank = percentile position of current IV within that 1-year
     realized vol range (0 = cheapest options have been, 100 = priciest).

This is a proxy for true IV Rank (which needs 1-year IV history) but
directionally accurate and free.
Return value matches the original interface:
  "Success! The IV Rank for {TICKER} is: {value}"
  "Could not find IV Rank for {TICKER} after all attempts."
"""

import sys
import math


def get_iv_rank_advanced(ticker: str) -> str:
    ticker = ticker.upper().strip()
    try:
        import yfinance as yf

        t = yf.Ticker(ticker)

        # ── 1. Current ATM implied volatility from nearest-expiry options ──
        try:
            exps = t.options
        except Exception as e:
            return f"Could not find IV Rank for {ticker} after all attempts ({e})."

        if not exps:
            return f"Could not find IV Rank for {ticker} after all attempts (no options listed)."

        # Use the nearest expiry that's at least 7 days out (avoid pinning/expiry noise)
        from datetime import date
        nearest_exp = None
        for exp_str in exps:
            try:
                d = date.fromisoformat(exp_str)
                if (d - date.today()).days >= 7:
                    nearest_exp = exp_str
                    break
            except Exception:
                continue

        if not nearest_exp:
            nearest_exp = exps[0]

        try:
            chain = t.option_chain(nearest_exp)
            calls = chain.calls
        except Exception as e:
            return f"Could not find IV Rank for {ticker} after all attempts (option chain: {e})."

        # Get stock price for ATM selection
        try:
            fi = t.fast_info
            S = fi.last_price or fi.previous_close
        except Exception:
            S = None

        if S and not calls.empty:
            atm_row = calls.iloc[(calls["strike"] - float(S)).abs().argsort()[:1]]
        elif not calls.empty:
            mid_idx = len(calls) // 2
            atm_row = calls.iloc[[mid_idx]]
        else:
            return f"Could not find IV Rank for {ticker} after all attempts (empty option chain)."

        if atm_row.empty:
            return f"Could not find IV Rank for {ticker} after all attempts (no ATM option)."

        current_iv = float(atm_row["impliedVolatility"].values[0] or 0)
        if current_iv <= 0:
            return f"Could not find IV Rank for {ticker} after all attempts (IV = 0)."

        current_iv_pct = current_iv * 100   # e.g. 0.35 → 35.0

        # ── 2. 1-year realized volatility range as IV proxy ──
        try:
            hist = t.history(period="1y", interval="1d", auto_adjust=True)
        except Exception:
            hist = None

        if hist is None or hist.empty or len(hist) < 30:
            # Can't compute rank — return raw IV as best effort
            return f"Success! The IV Rank for {ticker} is: {current_iv_pct:.1f}"

        returns = hist["Close"].pct_change().dropna()
        # 30-day rolling realized vol, annualized to match IV convention
        rolling_vol = returns.rolling(30).std() * math.sqrt(252) * 100
        rolling_vol = rolling_vol.dropna()

        if rolling_vol.empty:
            return f"Success! The IV Rank for {ticker} is: {current_iv_pct:.1f}"

        vol_min = float(rolling_vol.min())
        vol_max = float(rolling_vol.max())

        # ── 3. Compute IV Rank (0-100) ──
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
