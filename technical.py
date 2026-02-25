"""
Technical analysis helpers using yfinance.
Used for:
  - Weekly RSI (entry scoring for watchlist)
  - 52-week high/low position (entry scoring)
  - Current stock price fallback
"""

import yfinance as yf
import pandas as pd


def get_weekly_rsi(ticker: str, period: int = 14) -> float | None:
    """
    Compute 14-period RSI on the weekly chart.
    A reading below 30 is considered technically oversold (good LEAPS entry timing).
    """
    try:
        data = yf.download(ticker, period="2y", interval="1wk",
                           auto_adjust=True, progress=False)
        if data.empty or len(data) < period + 1:
            return None
        close = data["Close"].squeeze()
        delta = close.diff()
        gain  = delta.clip(lower=0).rolling(window=period).mean()
        loss  = (-delta.clip(upper=0)).rolling(window=period).mean()
        rs    = gain / loss.replace(0, float("nan"))
        rsi   = 100 - (100 / (1 + rs))
        val   = rsi.iloc[-1]
        return round(float(val), 1) if pd.notna(val) else None
    except Exception:
        return None


def get_price_and_range(ticker: str) -> dict:
    """
    Return current price, 52-week low, 52-week high, and how far (0-1)
    the current price sits within the 52-week range.
    0.0 = at the 52-week low, 1.0 = at the 52-week high.
    """
    result = {
        "price":          None,
        "low_52w":        None,
        "high_52w":       None,
        "pct_from_low":   None,   # 0.0 = at low, 1.0 = at high
    }
    try:
        info = yf.Ticker(ticker).info
        price  = info.get("currentPrice") or info.get("regularMarketPrice")
        low52  = info.get("fiftyTwoWeekLow")
        high52 = info.get("fiftyTwoWeekHigh")
        result["price"]   = float(price)  if price  else None
        result["low_52w"] = float(low52)  if low52  else None
        result["high_52w"]= float(high52) if high52 else None
        if price and low52 and high52 and (high52 - low52) > 0:
            result["pct_from_low"] = round((price - low52) / (high52 - low52), 3)
    except Exception:
        pass
    return result
