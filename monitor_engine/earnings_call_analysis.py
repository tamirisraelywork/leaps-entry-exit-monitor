"""
Earnings Call Analysis — Perplexity/Gemini-powered transcript tone + guidance analysis.

Why this matters for LEAPS:
  LEAPS are 12–24 month thesis bets. The earnings call is the single highest-density
  information event each quarter. Management language deterioration
  (vague guidance, "elongated sales cycles", "challenging macro") appears
  QUARTERS before the financial numbers break down.

What this module does:
  1. Day after earnings: run Gemini+GoogleSearch query on the most recent transcript
  2. Extract: tone_score (-1.0 to +1.0), guidance_change, analyst_tone, thesis_impact,
     key bullish/bearish language signals, 3-sentence trader summary
  3. Store results in BigQuery earnings_calls table
  4. Provide get_tone_delta() to compare consecutive quarters → IMPROVING/STABLE/DETERIORATING

Called from:
  monitor_engine/monitor_service.py → run_post_earnings_analysis() (10 AM daily)
  monitor_engine/monitor_service.py → _fetch_market_data() (reads latest cached call data)
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import date, timedelta
from typing import Optional

import requests

from shared.config import cfg

logger = logging.getLogger(__name__)

_GEMINI_MODELS = [
    "gemini-2.0-flash",
    "gemini-1.5-flash",
]

_ANALYSIS_PROMPT = """
You are an expert options trader specializing in LEAPS (Long-term Equity Anticipation Securities).
Analyze the MOST RECENT earnings call for {ticker} ({company_name}).

Use Google Search to find the transcript, management commentary, and analyst Q&A from the most
recent quarterly earnings call. Focus on language that reveals confidence or concern about
the company's outlook over the NEXT 12–24 MONTHS (the LEAPS thesis horizon).

Return ONLY valid JSON (no markdown, no explanation):
{{
  "overall_tone": "BULLISH" | "NEUTRAL" | "BEARISH",
  "tone_score": <float from -1.0 (very bearish) to +1.0 (very bullish)>,
  "forward_guidance": "RAISED" | "MAINTAINED" | "LOWERED" | "WITHDRAWN" | "NOT_PROVIDED",
  "analyst_tone": "CONFIDENT" | "PROBING" | "SKEPTICAL",
  "thesis_impact": "STRENGTHENED" | "UNCHANGED" | "WEAKENED",
  "quarter": "<e.g. Q1 2025>",
  "key_bullish_signals": ["<quote or theme>", ...],
  "key_bearish_signals": ["<quote or theme>", ...],
  "summary": "<3-sentence plain-language summary for a LEAPS trader>"
}}

Key signals to watch for:
BEARISH language flags: "headwinds", "elongated sales cycles", "cautious", "challenging macro",
  "more measured", "uncertainty", "delayed decisions", vague guidance replacing specific numbers,
  management refusing to give forward guidance, hostile analyst Q&A.
BULLISH language flags: "accelerating", "record pipeline", "multiple catalysts", "expanding margins",
  "raised guidance", specific upbeat numeric targets, management confident in raised outlook,
  easy analyst Q&A with satisfaction.

Be strict — if management is hedging heavily or guidance was cut, that is BEARISH regardless
of short-term beat vs estimates. We care about the NEXT 12–24 MONTH TRAJECTORY.
"""

_SYSTEM_PROMPT = (
    "You are a LEAPS options trading analyst. You MUST use Google Search to find real-time "
    "earnings call transcripts. Your output MUST be valid JSON only — no markdown fences, "
    "no preamble."
)


def _get_company_name(ticker: str) -> str:
    """Quick name lookup via Polygon (best effort)."""
    try:
        key = cfg("POLYGON_API_KEY_1") or cfg("POLYGON_API_KEY_2")
        if not key:
            return ticker
        r = requests.get(
            f"https://api.polygon.io/v3/reference/tickers/{ticker.upper()}?apiKey={key}",
            timeout=5,
        )
        if r.status_code == 200:
            return r.json().get("results", {}).get("name", ticker)
    except Exception:
        pass
    return ticker


def _call_gemini(ticker: str, company_name: str) -> Optional[dict]:
    """Call Gemini with Google Search grounding and return parsed JSON dict."""
    api_key = cfg("GEMINI_API_KEY")
    if not api_key:
        logger.error("GEMINI_API_KEY not set — cannot analyze earnings call")
        return None

    prompt = _ANALYSIS_PROMPT.format(ticker=ticker, company_name=company_name)
    payload = {
        "contents":          [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": _SYSTEM_PROMPT}]},
        "tools":             [{"google_search": {}}],
    }
    headers = {"Content-Type": "application/json"}

    for model in _GEMINI_MODELS:
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{model}:generateContent?key={api_key}"
        )
        for attempt in range(3):
            try:
                resp = requests.post(url, json=payload, headers=headers, timeout=60)
                if resp.status_code == 200:
                    parts = (
                        resp.json()
                        .get("candidates", [{}])[0]
                        .get("content", {})
                        .get("parts", [])
                    )
                    raw = "".join(
                        p.get("text", "")
                        for p in parts
                        if p.get("text") and not p.get("thought", False)
                    )
                    # Strip markdown fences if model added them
                    raw = re.sub(r"```(?:json)?", "", raw).strip().strip("`")
                    return json.loads(raw)

                elif resp.status_code == 429:
                    wait = 30 * (attempt + 1)
                    logger.warning(f"Rate limited on {model} — waiting {wait}s")
                    time.sleep(wait)
                else:
                    logger.warning(f"Gemini {model} returned {resp.status_code}")
                    break
            except json.JSONDecodeError as e:
                logger.warning(f"JSON parse error from {model}: {e}")
                break
            except Exception as e:
                logger.warning(f"Gemini {model} attempt {attempt+1} failed: {e}")
                time.sleep(5)

    return None


def analyze_earnings_call(ticker: str) -> Optional[dict]:
    """
    Run earnings call analysis for a ticker via Gemini.
    Returns parsed dict with tone_score, guidance_change, summary, etc.
    Returns None on failure.
    """
    ticker = ticker.upper().strip()
    logger.info(f"Analyzing earnings call for {ticker}…")
    company_name = _get_company_name(ticker)
    result = _call_gemini(ticker, company_name)
    if result:
        result["ticker"] = ticker
        result["analyzed_at"] = date.today().isoformat()
    return result


def save_call_analysis(ticker: str, data: dict):
    """
    Persist earnings call analysis to BigQuery earnings_calls table.
    Creates the table if it doesn't exist.
    """
    try:
        import db
        db.save_earnings_call(ticker, data)
        logger.info(f"Saved earnings call analysis for {ticker}: {data.get('overall_tone')} / {data.get('forward_guidance')}")
    except Exception as e:
        logger.error(f"Failed to save earnings call for {ticker}: {e}")


def get_latest_call_data(ticker: str) -> Optional[dict]:
    """
    Return the most recent earnings call analysis row from BigQuery, or None.
    """
    try:
        import db
        return db.get_latest_earnings_call(ticker)
    except Exception as e:
        logger.debug(f"get_latest_call_data({ticker}): {e}")
        return None


def get_tone_delta(ticker: str) -> str:
    """
    Compare the two most recent quarters' tone scores.
    Returns: 'IMPROVING', 'STABLE', or 'DETERIORATING'.
    """
    try:
        import db
        rows = db.get_earnings_calls(ticker, limit=2)
        if len(rows) < 2:
            return "STABLE"
        current  = float(rows[0].get("tone_score", 0))
        previous = float(rows[1].get("tone_score", 0))
        delta = current - previous
        if delta >= 0.15:
            return "IMPROVING"
        elif delta <= -0.15:
            return "DETERIORATING"
        return "STABLE"
    except Exception as e:
        logger.debug(f"get_tone_delta({ticker}): {e}")
        return "STABLE"


def run_post_earnings_analysis_job():
    """
    For all positions whose earnings_date was yesterday,
    run analysis and save to BigQuery.
    Called daily at 10:00 AM ET (gives time for transcript to be published).
    """
    import db

    yesterday = date.today() - timedelta(days=1)
    logger.info(f"Post-earnings job: checking for earnings on {yesterday}…")

    try:
        positions = db.get_positions()
    except Exception as e:
        logger.error(f"Post-earnings job: could not fetch positions: {e}")
        return

    seen: set[str] = set()
    analyzed = 0

    for pos in positions:
        ticker = (pos.get("ticker") or "").upper()
        if not ticker or ticker in seen:
            continue

        ed_raw = pos.get("earnings_date")
        if not ed_raw:
            continue

        try:
            from monitor_engine.earnings_calendar import _to_date
            ed = _to_date(ed_raw)
        except Exception:
            continue

        if ed != yesterday:
            continue

        seen.add(ticker)

        try:
            result = analyze_earnings_call(ticker)
            if result:
                save_call_analysis(ticker, result)
                analyzed += 1

                # If guidance was cut or tone was very bearish, fire an immediate rescore
                if (
                    result.get("forward_guidance") == "LOWERED"
                    or result.get("tone_score", 0) < -0.5
                ):
                    logger.info(f"  {ticker}: negative earnings signal — triggering thesis rescore")
                    try:
                        import score_thesis
                        score_thesis.compute_and_save_score(ticker)
                    except Exception as se:
                        logger.warning(f"  Rescore failed for {ticker}: {se}")

            time.sleep(15)  # Rate limit Gemini calls

        except Exception as e:
            logger.error(f"Post-earnings analysis failed for {ticker}: {e}")

    logger.info(f"Post-earnings job complete — {analyzed} tickers analyzed.")
