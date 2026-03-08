"""
News Sentiment — Alpha Vantage News & Sentiments API.

Why this matters for LEAPS:
  Major news (FDA rejection, M&A breakup, guidance withdrawal, CEO resignation)
  can destroy a LEAPS thesis overnight. This module provides early warning
  before the next scheduled position check.

API:
  Alpha Vantage NEWS_SENTIMENT endpoint (free tier: 25 req/day)
  Returns articles with overall_sentiment_score (-1 to +1) and relevance_score.

Caching:
  Results are cached in-memory for 4 hours per ticker to stay within free tier limits.
  The job runs every 4 hours, so effectively one fetch per ticker per job run.

Thresholds:
  score > +0.25  → BULLISH
  -0.25 to +0.25 → NEUTRAL
  -0.35 to -0.25 → BEARISH (AMBER alert)
  < -0.35        → VERY BEARISH (RED alert)
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import requests

from shared.config import cfg

logger = logging.getLogger(__name__)

# In-memory cache: {ticker: (fetched_at, result_dict)}
_cache: dict[str, tuple[datetime, dict]] = {}
_CACHE_TTL_HOURS = 4


def get_news_sentiment(ticker: str) -> Optional[dict]:
    """
    Fetch news sentiment for a ticker from Alpha Vantage.

    Returns:
    {
        'score':         float,   # avg weighted sentiment -1.0 to +1.0
        'signal':        str,     # VERY_BULLISH / BULLISH / NEUTRAL / BEARISH / VERY_BEARISH
        'top_headline':  str,     # most relevant recent headline
        'article_count': int,     # articles found
        'top_articles':  list,    # [{headline, url, source, sentiment_score}]
    }
    Returns None if API call fails.
    """
    api_key = cfg("ALPHA_VANTAGE_API_KEY_1") or cfg("ALPHA_VANTAGE_API_KEY_3")
    if not api_key:
        logger.warning("No ALPHA_VANTAGE_API_KEY set — news sentiment unavailable")
        return None

    url = (
        f"https://www.alphavantage.co/query"
        f"?function=NEWS_SENTIMENT"
        f"&tickers={ticker.upper()}"
        f"&limit=20"
        f"&apikey={api_key}"
    )

    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"Alpha Vantage news API returned {resp.status_code} for {ticker}")
            return None

        data = resp.json()

        if "Note" in data or "Information" in data:
            logger.warning(f"Alpha Vantage rate limit hit for {ticker}")
            return None

        feed = data.get("feed", [])
        if not feed:
            return {
                "score": 0.0,
                "signal": "NEUTRAL",
                "top_headline": "No recent news found",
                "article_count": 0,
                "top_articles": [],
            }

        # Compute weighted average sentiment (weight = relevance_score for this ticker)
        total_weight = 0.0
        weighted_score = 0.0
        top_articles = []

        for article in feed[:20]:
            # Find the sentiment score for our specific ticker
            ticker_sentiment = None
            for ts in article.get("ticker_sentiment", []):
                if ts.get("ticker", "").upper() == ticker.upper():
                    try:
                        rel   = float(ts.get("relevance_score", 0))
                        score = float(ts.get("ticker_sentiment_score", 0))
                        ticker_sentiment = (rel, score)
                    except Exception:
                        pass
                    break

            if ticker_sentiment:
                rel, score = ticker_sentiment
                weighted_score += score * rel
                total_weight   += rel

                top_articles.append({
                    "headline":        article.get("title", ""),
                    "url":             article.get("url", ""),
                    "source":          article.get("source", ""),
                    "sentiment_score": round(score, 3),
                    "relevance":       round(rel, 3),
                    "published":       article.get("time_published", "")[:8],
                })

        if total_weight == 0:
            avg_score = 0.0
        else:
            avg_score = weighted_score / total_weight

        signal = _score_to_signal(avg_score)
        top_articles.sort(key=lambda x: x["relevance"], reverse=True)
        top_headline = top_articles[0]["headline"] if top_articles else "N/A"

        return {
            "score":         round(avg_score, 3),
            "signal":        signal,
            "top_headline":  top_headline,
            "article_count": len(top_articles),
            "top_articles":  top_articles[:5],
        }

    except Exception as e:
        logger.error(f"News sentiment fetch failed for {ticker}: {e}")
        return None


def _score_to_signal(score: float) -> str:
    if score >= 0.35:
        return "VERY_BULLISH"
    elif score >= 0.15:
        return "BULLISH"
    elif score <= -0.35:
        return "VERY_BEARISH"
    elif score <= -0.15:
        return "BEARISH"
    return "NEUTRAL"


def get_news_sentiment_cached(ticker: str) -> Optional[dict]:
    """
    Returns cached sentiment (up to 4 hours old) or fetches fresh data.
    Used during active position checks to avoid hitting rate limits.
    """
    ticker = ticker.upper()
    now = datetime.utcnow()

    if ticker in _cache:
        fetched_at, result = _cache[ticker]
        if now - fetched_at < timedelta(hours=_CACHE_TTL_HOURS):
            return result

    result = get_news_sentiment(ticker)
    if result is not None:
        _cache[ticker] = (now, result)
    return result


def run_news_check_job():
    """
    Fetch news sentiment for all active positions and send alerts for bearish signals.
    Called every 4 hours on weekdays.
    """
    import db
    import email_alerts

    logger.info("Running news sentiment check…")

    try:
        positions = db.get_positions(mode="ACTIVE")
    except Exception as e:
        logger.error(f"News check: could not load positions: {e}")
        return

    seen: set[str] = set()
    alerts_sent = 0

    for pos in positions:
        ticker = (pos.get("ticker") or "").upper()
        pos_id = str(pos.get("id", ""))
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)

        try:
            news = get_news_sentiment(ticker)
            if not news:
                time.sleep(2)
                continue

            score  = news["score"]
            signal = news["signal"]

            if score < -0.35:
                severity = "RED"
                alert_type = "NEWS_VERY_BEARISH"
                subject = f"🔴 VERY BEARISH NEWS: {ticker} — sentiment {score:.2f}"
            elif score < -0.15:
                severity = "AMBER"
                alert_type = "NEWS_BEARISH"
                subject = f"⚠️ BEARISH NEWS: {ticker} — sentiment {score:.2f}"
            else:
                time.sleep(2)
                continue

            if db.already_sent_today(alert_type, pos_id):
                continue

            body = (
                f"NEWS SENTIMENT ALERT — {ticker}\n"
                f"{'=' * 50}\n\n"
                f"Sentiment Score : {score:+.3f}  ({signal})\n"
                f"Articles        : {news['article_count']}\n\n"
                f"TOP HEADLINE:\n  {news['top_headline']}\n\n"
                f"RECENT ARTICLES:\n"
            )
            for art in news.get("top_articles", [])[:3]:
                body += f"  [{art['sentiment_score']:+.2f}] {art['headline']}\n"
                body += f"           Source: {art['source']}  ·  {art['published']}\n"

            body += (
                "\n⚠️  ACTION: Verify if this news invalidates your LEAPS thesis.\n"
                "  If thesis is broken → EXIT per Pillar 1 thresholds.\n"
                "  If temporary noise → document and monitor closely.\n"
            )

            sent = email_alerts.send_alert(subject, body)
            db.save_alert({
                "position_id":          pos_id,
                "ticker":               ticker,
                "alert_type":           alert_type,
                "severity":             severity,
                "subject":              subject,
                "body":                 body,
                "current_delta":        None,
                "current_dte":          None,
                "current_pnl_pct":      None,
                "current_iv_rank":      None,
                "current_thesis_score": db.get_leaps_monitor_score(ticker),
                "email_sent":           sent,
            })
            alerts_sent += 1

        except Exception as e:
            logger.error(f"News check error for {ticker}: {e}")

        time.sleep(2)  # Be kind to Alpha Vantage free tier

    logger.info(f"News check complete — {alerts_sent} alerts sent.")
