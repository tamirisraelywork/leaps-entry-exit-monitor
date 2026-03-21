"""
Earnings Call Analysis — multi-source transcript pipeline.

Source priority (best → fallback):
  1. Aletheia API        — structured earnings call analysis (free tier, apiKey)
  2. SEC EDGAR 8-K       — actual management text from official press release (free, no key)
                           + Finnhub EPS surprise data (free tier, apiKey)
                           → both fed as context to Gemini Flash for structured analysis
  3. Gemini + Google Search — last resort: Gemini web-searches for the transcript itself

Why the multi-source approach is more accurate than the old single-Perplexity approach:
  - Sources 1 & 2 give Gemini REAL TEXT to analyze, not LLM memory
  - SEC EDGAR 8-K is always available, officially filed within 4 business days of earnings
  - Finnhub EPS surprise adds quantitative context (beat by X%, guidance delta)
  - Aletheia provides pre-computed call sentiment on free tier
  - Gemini Google Search is the safety net for tickers not covered by the others
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

# ---------------------------------------------------------------------------
# Analysis prompt — used when we have actual transcript/press-release text
# ---------------------------------------------------------------------------

_ANALYSIS_PROMPT_WITH_TEXT = """
You are an expert options trader specializing in LEAPS (Long-term Equity Anticipation Securities).

Below is the actual earnings call press release / transcript for {ticker} ({company_name}).
{eps_context}

EARNINGS CALL TEXT:
\"\"\"
{text}
\"\"\"

Analyze this text and return ONLY valid JSON (no markdown, no explanation):
{{
  "overall_tone": "BULLISH" | "NEUTRAL" | "BEARISH",
  "tone_score": <float from -1.0 (very bearish) to +1.0 (very bullish)>,
  "forward_guidance": "RAISED" | "MAINTAINED" | "LOWERED" | "WITHDRAWN" | "NOT_PROVIDED",
  "analyst_tone": "CONFIDENT" | "PROBING" | "SKEPTICAL",
  "thesis_impact": "STRENGTHENED" | "UNCHANGED" | "WEAKENED",
  "quarter": "<e.g. Q1 2025>",
  "key_bullish_signals": ["<exact quote or theme>", ...],
  "key_bearish_signals": ["<exact quote or theme>", ...],
  "summary": "<3-sentence plain-language summary for a LEAPS trader>"
}}

BEARISH language flags (mark BEARISH if multiple present):
  "headwinds", "elongated sales cycles", "cautious", "challenging macro",
  "more measured", "uncertainty", "delayed decisions", vague guidance replacing
  specific numbers, guidance withdrawal, hostile analyst Q&A.

BULLISH language flags:
  "accelerating", "record pipeline", "multiple catalysts", "expanding margins",
  "raised guidance", specific upbeat numeric targets, easy analyst Q&A.

Focus on the NEXT 12–24 MONTH TRAJECTORY, not just the past quarter beat/miss.
If guidance was cut or withdrawn, that is BEARISH regardless of current results.
"""

# ---------------------------------------------------------------------------
# Fallback prompt — used when we only have ticker name (Gemini must search)
# ---------------------------------------------------------------------------

_ANALYSIS_PROMPT_SEARCH_ONLY = """
You are an expert options trader specializing in LEAPS.
Use Google Search to find the MOST RECENT earnings call transcript for {ticker} ({company_name}).

Return ONLY valid JSON (no markdown, no explanation):
{{
  "overall_tone": "BULLISH" | "NEUTRAL" | "BEARISH",
  "tone_score": <float from -1.0 to +1.0>,
  "forward_guidance": "RAISED" | "MAINTAINED" | "LOWERED" | "WITHDRAWN" | "NOT_PROVIDED",
  "analyst_tone": "CONFIDENT" | "PROBING" | "SKEPTICAL",
  "thesis_impact": "STRENGTHENED" | "UNCHANGED" | "WEAKENED",
  "quarter": "<e.g. Q1 2025>",
  "key_bullish_signals": ["<quote or theme>"],
  "key_bearish_signals": ["<quote or theme>"],
  "summary": "<3-sentence plain-language summary for a LEAPS trader>"
}}

Focus on the NEXT 12–24 MONTH TRAJECTORY for a LEAPS position.
"""

_SYSTEM_PROMPT = (
    "You are a LEAPS options trading analyst. Your output MUST be valid JSON only — "
    "no markdown fences, no preamble, no explanation."
)


# ---------------------------------------------------------------------------
# Source 1: Aletheia API
# ---------------------------------------------------------------------------

def _fetch_aletheia_analysis(ticker: str) -> Optional[dict]:
    """
    Fetch pre-computed earnings call analysis from Aletheia API (free tier).
    Returns structured dict or None.
    Aletheia: https://aletheia-api.com — free tier, apiKey required.
    """
    api_key = cfg("ALETHEIA_API_KEY")
    if not api_key:
        return None
    try:
        # Aletheia earnings call analysis endpoint
        resp = requests.get(
            f"https://api.aletheia.com/v1/earning-call/{ticker.upper()}",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            # Normalize to our expected schema
            return _normalize_aletheia(data)
        elif resp.status_code == 404:
            logger.debug(f"Aletheia: no transcript found for {ticker}")
        else:
            logger.debug(f"Aletheia: HTTP {resp.status_code} for {ticker}")
    except Exception as e:
        logger.debug(f"Aletheia fetch failed for {ticker}: {e}")
    return None


def _normalize_aletheia(data: dict) -> Optional[dict]:
    """Map Aletheia response fields to our standard schema."""
    if not data:
        return None
    try:
        tone_score = float(data.get("sentiment_score") or data.get("tone_score") or 0)
        if abs(tone_score) > 1:   # normalize if on a different scale (e.g. -100 to 100)
            tone_score = tone_score / 100.0
        tone_score = max(-1.0, min(1.0, tone_score))

        if tone_score > 0.2:
            overall_tone = "BULLISH"
        elif tone_score < -0.2:
            overall_tone = "BEARISH"
        else:
            overall_tone = "NEUTRAL"

        guidance = (data.get("guidance_change") or data.get("forward_guidance") or "NOT_PROVIDED").upper()
        if guidance not in ("RAISED", "MAINTAINED", "LOWERED", "WITHDRAWN", "NOT_PROVIDED"):
            guidance = "NOT_PROVIDED"

        return {
            "overall_tone":       overall_tone,
            "tone_score":         round(tone_score, 3),
            "forward_guidance":   guidance,
            "analyst_tone":       data.get("analyst_tone", "CONFIDENT").upper(),
            "thesis_impact":      data.get("thesis_impact", "UNCHANGED").upper(),
            "quarter":            data.get("quarter", ""),
            "key_bullish_signals": data.get("bullish_signals") or data.get("key_bullish_signals") or [],
            "key_bearish_signals": data.get("bearish_signals") or data.get("key_bearish_signals") or [],
            "summary":             data.get("summary") or data.get("analysis") or "",
            "_source":             "aletheia",
        }
    except Exception as e:
        logger.debug(f"Aletheia normalize failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Source 2a: SEC EDGAR 8-K (completely free, no API key)
# ---------------------------------------------------------------------------

_EDGAR_HEADERS = {
    "User-Agent": "LEAPS Monitor leaps-monitor@gmail.com",   # EDGAR requires user-agent
    "Accept-Encoding": "gzip, deflate",
    "Host": "data.sec.gov",
}


def _get_cik(ticker: str) -> Optional[str]:
    """
    Look up the SEC CIK for a ticker using the EDGAR company_tickers.json mapping.
    This file is ~200KB and maps every public company's ticker → CIK.
    """
    try:
        resp = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": "LEAPS Monitor leaps-monitor@gmail.com"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        mapping = resp.json()
        ticker_upper = ticker.upper()
        for entry in mapping.values():
            if entry.get("ticker", "").upper() == ticker_upper:
                return str(entry["cik_str"]).zfill(10)
    except Exception as e:
        logger.debug(f"CIK lookup failed for {ticker}: {e}")
    return None


def _fetch_edgar_8k(ticker: str, lookback_days: int = 90) -> Optional[str]:
    """
    Fetch the most recent 8-K earnings press release for a ticker from SEC EDGAR.

    Returns the extracted text content (up to 12,000 chars) or None.
    8-K Item 2.02 = Results of Operations (earnings press release).
    Filed within 4 business days of earnings — always available, completely free.
    """
    cik = _get_cik(ticker)
    if not cik:
        logger.debug(f"EDGAR: CIK not found for {ticker}")
        return None

    try:
        # Get the filing history for this company
        submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        resp = requests.get(
            submissions_url,
            headers={"User-Agent": "LEAPS Monitor leaps-monitor@gmail.com"},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.debug(f"EDGAR submissions: HTTP {resp.status_code} for {ticker}")
            return None

        data = resp.json()
        recent = data.get("filings", {}).get("recent", {})

        forms        = recent.get("form", [])
        dates        = recent.get("filingDate", [])
        accessions   = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])

        cutoff = date.today() - timedelta(days=lookback_days)

        # Find the most recent 8-K within lookback window
        for i, (form, filing_date, acc, doc) in enumerate(
            zip(forms, dates, accessions, primary_docs)
        ):
            if form != "8-K":
                continue
            try:
                fd = date.fromisoformat(filing_date)
            except Exception:
                continue
            if fd < cutoff:
                break   # filings are in reverse-chronological order

            # Fetch the actual document
            acc_clean = acc.replace("-", "")
            doc_url = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{int(cik)}/{acc_clean}/{doc}"
            )
            try:
                doc_resp = requests.get(
                    doc_url,
                    headers={"User-Agent": "LEAPS Monitor leaps-monitor@gmail.com"},
                    timeout=15,
                )
                if doc_resp.status_code != 200:
                    continue

                raw_text = doc_resp.text

                # Strip HTML tags if present
                if "<html" in raw_text.lower() or "<body" in raw_text.lower():
                    raw_text = re.sub(r"<[^>]+>", " ", raw_text)
                    raw_text = re.sub(r"&nbsp;", " ", raw_text)
                    raw_text = re.sub(r"&amp;", "&", raw_text)
                    raw_text = re.sub(r"&lt;", "<", raw_text)
                    raw_text = re.sub(r"&gt;", ">", raw_text)

                # Collapse whitespace
                raw_text = re.sub(r"\s{3,}", "\n\n", raw_text)
                raw_text = raw_text.strip()

                # Filter: must mention earnings-related keywords to be an earnings 8-K
                earnings_keywords = (
                    "revenue", "earnings per share", "net income", "quarterly results",
                    "results of operations", "fiscal", "guidance", "outlook"
                )
                text_lower = raw_text.lower()
                if not any(kw in text_lower for kw in earnings_keywords):
                    continue

                logger.info(f"EDGAR: found 8-K for {ticker} filed {filing_date}")
                # Return first 12,000 chars — enough for Gemini context, not too large
                return raw_text[:12_000]

            except Exception as doc_err:
                logger.debug(f"EDGAR doc fetch error: {doc_err}")
                continue

    except Exception as e:
        logger.debug(f"EDGAR 8-K fetch failed for {ticker}: {e}")

    return None


# ---------------------------------------------------------------------------
# Source 2b: Finnhub EPS surprise (free tier, ~60 calls/min)
# ---------------------------------------------------------------------------

def _fetch_finnhub_surprise(ticker: str) -> Optional[str]:
    """
    Fetch the last 2 quarters of EPS actuals vs estimates from Finnhub (free tier).
    Returns a short text context string like:
      "Q1 2026: EPS actual $0.45 vs estimate $0.38 (+18.4% beat).
       Q4 2025: EPS actual $0.41 vs estimate $0.43 (-4.7% miss)."
    Returns None if Finnhub key not configured or request fails.
    """
    api_key = cfg("FINNHUB_API_KEY")
    if not api_key:
        return None
    try:
        resp = requests.get(
            f"https://finnhub.io/api/v1/stock/earnings",
            params={"symbol": ticker.upper(), "limit": 3},
            headers={"X-Finnhub-Token": api_key},
            timeout=8,
        )
        if resp.status_code != 200:
            return None
        rows = resp.json()
        if not rows:
            return None

        lines = []
        for row in rows[:2]:
            period  = row.get("period", "?")
            actual  = row.get("actual")
            est     = row.get("estimate")
            if actual is None or est is None:
                continue
            try:
                surprise_pct = ((actual - est) / abs(est)) * 100 if est != 0 else 0
                direction    = "beat" if surprise_pct >= 0 else "miss"
                lines.append(
                    f"{period}: EPS actual ${actual:.2f} vs estimate ${est:.2f} "
                    f"({surprise_pct:+.1f}% {direction})"
                )
            except Exception:
                continue

        return "EPS HISTORY: " + " | ".join(lines) if lines else None
    except Exception as e:
        logger.debug(f"Finnhub EPS fetch failed for {ticker}: {e}")
        return None


# ---------------------------------------------------------------------------
# Gemini caller — with real text context
# ---------------------------------------------------------------------------

def _call_gemini_with_context(
    ticker: str,
    company_name: str,
    transcript_text: str,
    eps_context: str = "",
) -> Optional[dict]:
    """
    Call Gemini with actual earnings text (8-K press release + EPS data).
    Uses standard text generation — no Google Search grounding needed since
    we're providing the actual content directly.
    """
    api_key = cfg("GEMINI_API_KEY")
    if not api_key:
        logger.error("GEMINI_API_KEY not set")
        return None

    prompt = _ANALYSIS_PROMPT_WITH_TEXT.format(
        ticker=ticker,
        company_name=company_name,
        text=transcript_text[:10_000],   # cap to stay within token budget
        eps_context=f"\n{eps_context}\n" if eps_context else "",
    )
    payload = {
        "contents":          [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": _SYSTEM_PROMPT}]},
        # No tools needed — we're providing the actual text
        "generationConfig":  {"temperature": 0.1},
    }
    headers = {"Content-Type": "application/json"}

    return _call_gemini_raw(payload, headers, source_label="EDGAR+Gemini")


def _call_gemini_search_only(ticker: str, company_name: str) -> Optional[dict]:
    """
    Last-resort: Gemini with Google Search grounding (searches for the transcript itself).
    Less accurate than providing actual text, but always available.
    """
    api_key = cfg("GEMINI_API_KEY")
    if not api_key:
        return None

    prompt = _ANALYSIS_PROMPT_SEARCH_ONLY.format(
        ticker=ticker, company_name=company_name
    )
    payload = {
        "contents":          [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": _SYSTEM_PROMPT}]},
        "tools":             [{"google_search": {}}],
    }
    headers = {"Content-Type": "application/json"}

    return _call_gemini_raw(payload, headers, source_label="Gemini+Search")


def _call_gemini_raw(payload: dict, headers: dict, source_label: str) -> Optional[dict]:
    """Shared Gemini HTTP call logic with model fallback and rate-limit retry."""
    api_key = cfg("GEMINI_API_KEY")
    if not api_key:
        return None

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
                    raw = re.sub(r"```(?:json)?", "", raw).strip().strip("`")
                    result = json.loads(raw)
                    result["_source"] = source_label
                    return result

                elif resp.status_code == 429:
                    wait = 30 * (attempt + 1)
                    logger.warning(f"Gemini {model} rate-limited — waiting {wait}s")
                    time.sleep(wait)
                else:
                    logger.warning(f"Gemini {model} returned {resp.status_code}")
                    break
            except json.JSONDecodeError as e:
                logger.warning(f"JSON parse error from {model} ({source_label}): {e}")
                break
            except Exception as e:
                logger.warning(f"Gemini {model} attempt {attempt+1} failed: {e}")
                time.sleep(5)

    return None


# ---------------------------------------------------------------------------
# Company name helper
# ---------------------------------------------------------------------------

def _get_company_name(ticker: str) -> str:
    """Quick company name lookup: Polygon first, yfinance fallback."""
    try:
        key = cfg("POLYGON_API_KEY_1") or cfg("POLYGON_API_KEY_2")
        if key:
            r = requests.get(
                f"https://api.polygon.io/v3/reference/tickers/{ticker.upper()}?apiKey={key}",
                timeout=5,
            )
            if r.status_code == 200:
                name = r.json().get("results", {}).get("name", "")
                if name:
                    return name
    except Exception:
        pass
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        return info.get("longName") or info.get("shortName") or ticker
    except Exception:
        return ticker


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def analyze_earnings_call(ticker: str) -> Optional[dict]:
    """
    Run earnings call analysis for a ticker using the best available source.

    Pipeline:
      1. Aletheia API         — pre-computed call analysis (if ALETHEIA_API_KEY set)
      2. SEC EDGAR 8-K        — actual press release text → Gemini analysis
         + Finnhub EPS data   — quantitative context (beat/miss, prior quarter)
      3. Gemini + Google Search — fallback: Gemini searches for the transcript itself

    Returns parsed dict with: tone_score, forward_guidance, overall_tone,
    analyst_tone, thesis_impact, quarter, key_bullish_signals,
    key_bearish_signals, summary, _source.
    Returns None on complete failure.
    """
    ticker = ticker.upper().strip()
    logger.info(f"Analyzing earnings call for {ticker}…")
    company_name = _get_company_name(ticker)

    # ── Source 1: Aletheia ────────────────────────────────────────────────
    aletheia = _fetch_aletheia_analysis(ticker)
    if aletheia:
        logger.info(f"  {ticker}: analysis from Aletheia (tone={aletheia.get('tone_score')})")
        aletheia["ticker"] = ticker
        aletheia["analyzed_at"] = date.today().isoformat()
        return aletheia

    # ── Source 2: SEC EDGAR 8-K + Finnhub EPS → Gemini ───────────────────
    edgar_text = _fetch_edgar_8k(ticker)
    eps_context = _fetch_finnhub_surprise(ticker) or ""

    if edgar_text:
        logger.info(f"  {ticker}: analyzing SEC EDGAR 8-K text ({len(edgar_text)} chars) + Gemini")
        result = _call_gemini_with_context(ticker, company_name, edgar_text, eps_context)
        if result:
            result["ticker"] = ticker
            result["analyzed_at"] = date.today().isoformat()
            logger.info(
                f"  {ticker}: EDGAR+Gemini analysis complete — "
                f"tone={result.get('tone_score')}, guidance={result.get('forward_guidance')}"
            )
            return result
    elif eps_context:
        # We have EPS data but no press release — still useful for Gemini context
        logger.info(f"  {ticker}: no 8-K found, using EPS context + Gemini Search")
        # Fall through to Gemini Search with EPS context appended to prompt
    else:
        logger.info(f"  {ticker}: no EDGAR or Finnhub data — falling back to Gemini Search")

    # ── Source 3: Gemini + Google Search (last resort) ────────────────────
    result = _call_gemini_search_only(ticker, company_name)
    if result:
        # Annotate that this came from search (less reliable)
        result["ticker"] = ticker
        result["analyzed_at"] = date.today().isoformat()
        result["_source"] = "Gemini+Search"
        # Slightly reduce confidence for search-only results (no actual text verified)
        raw_score = result.get("tone_score", 0)
        if isinstance(raw_score, (int, float)):
            result["tone_score"] = round(raw_score * 0.85, 3)   # 15% confidence discount
        logger.info(f"  {ticker}: Gemini+Search analysis complete (tone={result.get('tone_score')})")
        return result

    logger.error(f"  {ticker}: all analysis sources failed")
    return None


# ---------------------------------------------------------------------------
# BigQuery persistence
# ---------------------------------------------------------------------------

def save_call_analysis(ticker: str, data: dict):
    """Persist earnings call analysis to BigQuery earnings_calls table."""
    try:
        import db
        db.save_earnings_call(ticker, data)
        logger.info(
            f"Saved earnings call for {ticker}: "
            f"{data.get('overall_tone')} / {data.get('forward_guidance')} "
            f"(source: {data.get('_source', '?')})"
        )
    except Exception as e:
        logger.error(f"Failed to save earnings call for {ticker}: {e}")


def get_latest_call_data(ticker: str) -> Optional[dict]:
    """Return the most recent earnings call analysis row from BigQuery, or None."""
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


# ---------------------------------------------------------------------------
# Scheduled job
# ---------------------------------------------------------------------------

def run_post_earnings_analysis_job():
    """
    For all positions whose earnings_date was yesterday,
    run analysis and save to BigQuery.
    Called daily at 10:00 AM ET (gives time for 8-K and transcript to be published).
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

                # Guidance cut or very bearish tone → trigger immediate thesis rescore
                if (
                    result.get("forward_guidance") == "LOWERED"
                    or float(result.get("tone_score") or 0) < -0.5
                ):
                    logger.info(f"  {ticker}: negative earnings signal → triggering thesis rescore")
                    try:
                        import score_thesis
                        score_thesis.compute_and_save_score(ticker)
                    except Exception as se:
                        logger.warning(f"  Rescore failed for {ticker}: {se}")

            time.sleep(12)   # Stagger Gemini calls

        except Exception as e:
            logger.error(f"Post-earnings analysis failed for {ticker}: {e}")

    logger.info(f"Post-earnings job complete — {analyzed} tickers analyzed.")
