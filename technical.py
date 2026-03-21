"""
Technical analysis helpers using yfinance.
Used for:
  - Weekly RSI (entry scoring for watchlist)
  - 52-week high/low position (entry scoring)
  - Current stock price fallback
"""

from __future__ import annotations

import threading
import time

import yfinance as yf
import pandas as pd

# Shared rate gate — prevents concurrent yfinance calls from bursting Yahoo Finance.
# Max 2 simultaneous calls; each call waits at least 0.5 s after the previous one.
_yf_sem   = threading.Semaphore(2)
_yf_last  = 0.0
_yf_lock  = threading.Lock()
_YF_GAP   = 0.5   # seconds between calls through this module


def _technical_yf_throttle():
    global _yf_last
    with _yf_lock:
        gap = _YF_GAP - (time.time() - _yf_last)
        if gap > 0:
            time.sleep(gap)
        _yf_last = time.time()


def get_weekly_rsi(ticker: str, period: int = 14) -> float | None:
    """
    Compute 14-period RSI on the weekly chart.
    A reading below 30 is considered technically oversold (good LEAPS entry timing).
    """
    try:
        with _yf_sem:
            _technical_yf_throttle()
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
    Return current price, 52-week low/high, range position, and MA50/MA200
    trend data — all used for entry signal scoring.

    pct_from_low:  0.0 = at 52-week low, 1.0 = at 52-week high
    above_ma50:    True if price > 50-day MA (short-term trend)
    above_ma200:   True if price > 200-day MA (long-term trend)
    ma50_above_ma200: True if MA50 > MA200 (golden-cross structure)
    """
    result = {
        "price":            None,
        "low_52w":          None,
        "high_52w":         None,
        "pct_from_low":     None,   # 0.0 = at low, 1.0 = at high
        "ma_50":            None,
        "ma_200":           None,
        "above_ma50":       None,
        "above_ma200":      None,
        "ma50_above_ma200": None,
    }
    try:
        with _yf_sem:
            _technical_yf_throttle()
            info   = yf.Ticker(ticker).info
        price  = info.get("currentPrice") or info.get("regularMarketPrice")
        low52  = info.get("fiftyTwoWeekLow")
        high52 = info.get("fiftyTwoWeekHigh")
        ma50   = info.get("fiftyDayAverage")
        ma200  = info.get("twoHundredDayAverage")

        result["price"]   = float(price)  if price  else None
        result["low_52w"] = float(low52)  if low52  else None
        result["high_52w"]= float(high52) if high52 else None
        result["ma_50"]   = float(ma50)   if ma50   else None
        result["ma_200"]  = float(ma200)  if ma200  else None

        if price and low52 and high52 and (high52 - low52) > 0:
            result["pct_from_low"] = round((price - low52) / (high52 - low52), 3)
        if price and ma50:
            result["above_ma50"]  = float(price) > float(ma50)
        if price and ma200:
            result["above_ma200"] = float(price) > float(ma200)
        if ma50 and ma200:
            result["ma50_above_ma200"] = float(ma50) > float(ma200)
    except Exception:
        pass
    return result
