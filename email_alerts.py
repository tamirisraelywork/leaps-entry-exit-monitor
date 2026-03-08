"""
Gmail SMTP alert delivery.

Sends formatted alert emails using Gmail with an App Password.
Required secrets:
  GMAIL_SENDER          — the Gmail address you send FROM
  GMAIL_APP_PASSWORD    — 16-char Google App Password (not your regular password)
  ALERT_RECIPIENT_EMAIL — address to send alerts TO (can be same as sender)
"""

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

from shared.config import cfg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Severity → emoji prefix for subject line
# ---------------------------------------------------------------------------

_SEVERITY_EMOJI = {
    "RED":   "🔴",
    "AMBER": "⚠️",
    "BLUE":  "🔵",
    "GREEN": "🟢",
}


def _make_message(subject: str, body: str) -> MIMEMultipart:
    """Construct a MIME email message."""
    sender    = cfg("GMAIL_SENDER")
    recipient = cfg("ALERT_RECIPIENT_EMAIL") or sender

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"LEAPS Monitor <{sender}>"
    msg["To"]      = recipient

    # Plain-text version (primary — no HTML needed for alert emails)
    footer = (
        "\n\n"
        + "─" * 50 + "\n"
        + f"LEAPS Position Manager · {datetime.now().strftime('%b %d, %Y  %H:%M')} UTC\n"
        + "Automated alert — do not reply.\n"
    )
    msg.attach(MIMEText(body + footer, "plain"))
    return msg


def send_alert(subject: str, body: str) -> tuple[bool, str]:
    """
    Send a single alert email via Gmail SMTP.

    Returns (success, error_message).
    Never raises — failures are logged so they never crash the monitoring loop.
    """
    # Support both GMAIL_SENDER (monitor) and ALERT_EMAIL_FROM (evaluator) key names
    sender   = cfg("GMAIL_SENDER") or cfg("ALERT_EMAIL_FROM")
    password = cfg("GMAIL_APP_PASSWORD") or cfg("ALERT_EMAIL_PASS")
    recipient = cfg("ALERT_RECIPIENT_EMAIL") or sender

    if not sender or not password:
        msg = (
            "Email not configured. Add GMAIL_SENDER and GMAIL_APP_PASSWORD to secrets "
            "(Streamlit Cloud → App settings → Secrets)."
        )
        logger.warning(msg)
        return False, msg

    try:
        msg_obj = _make_message(subject, body)

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as server:
            server.login(sender, password)
            server.sendmail(sender, recipient, msg_obj.as_string())

        logger.info(f"Alert email sent: {subject}")
        return True, ""

    except smtplib.SMTPAuthenticationError:
        err = (
            "Gmail authentication failed. Make sure you are using a 16-character "
            "App Password (not your regular Google password). "
            "Generate one at myaccount.google.com → Security → App Passwords."
        )
        logger.error(err)
        return False, err
    except Exception as e:
        err = str(e)
        logger.error(f"Email delivery failed: {err}")
        return False, err


def send_test_email() -> tuple[bool, str]:
    """Send a test email to verify Gmail SMTP is configured correctly.
    Returns (success, error_message)."""
    subject = "✅ LEAPS Position Manager — Test Email"
    body = (
        "This is a test email from your LEAPS Position Manager.\n\n"
        "If you received this, Gmail SMTP is configured correctly.\n"
        "You will receive alerts at this address when your positions\n"
        "trigger any of the 5-pillar exit/roll/entry signals.\n"
    )
    ok, err = send_alert(subject, body)
    return ok, err


def send_daily_summary(positions: list[dict], market_snapshots: dict) -> tuple[bool, str]:
    """
    Build and send the daily portfolio summary email.

    Args:
        positions:        list of position dicts (from db.get_positions)
        market_snapshots: dict keyed by position id → market data dict
    """
    today = datetime.now().strftime("%B %d, %Y")
    subject = f"📊 LEAPS Daily Summary — {today}"

    active_lines   = []
    watchlist_lines = []

    for pos in positions:
        ticker = pos.get("ticker", "?")
        strike = pos.get("strike", "?")
        exp    = pos.get("expiration_date", "?")
        role   = pos.get("position_type", "CORE")
        mode   = pos.get("mode", "ACTIVE")
        qty    = pos.get("quantity", "?")

        mkt = market_snapshots.get(str(pos.get("id")), {})
        mid = mkt.get("mid")
        delta = mkt.get("delta")
        dte   = mkt.get("dte")
        score = mkt.get("thesis_score")
        iv_rank = mkt.get("iv_rank")

        entry_price = pos.get("entry_price")
        pnl_pct = None
        if entry_price and mid:
            pnl_pct = round((mid - entry_price) / entry_price * 100, 1)

        # Determine posture for this position
        from exit_engine import evaluate
        alerts = evaluate(pos, mkt) if mode == "ACTIVE" else []
        if not alerts:
            posture = "HOLD"
            emoji   = "🟢"
        else:
            worst = max(alerts, key=lambda a: {"RED": 3, "BLUE": 2, "AMBER": 1, "GREEN": 0}.get(a.severity, 0))
            severity_map = {"RED": ("EXIT/STOP", "🔴"), "BLUE": ("ROLL", "🔵"),
                            "AMBER": ("WATCH", "⚠️"), "GREEN": ("HOLD", "🟢")}
            posture, emoji = severity_map.get(worst.severity, ("HOLD", "🟢"))

        pnl_str   = f"{pnl_pct:+.1f}%" if pnl_pct is not None else "N/A"
        delta_str = f"Δ {delta:.2f}"   if delta   is not None else "Δ N/A"
        dte_str   = f"DTE {dte}d"      if dte     is not None else "DTE N/A"
        score_str = f"Score {score}"   if score   is not None else "Score N/A"

        line = (
            f"  {emoji} {ticker:<6} {exp} ${strike}C  {qty}x  "
            f"| P&L {pnl_str:<8} | {delta_str:<10} | {dte_str:<10} | {score_str:<10} | {role}"
            f"  → {posture}"
        )

        if mode == "WATCHLIST":
            watchlist_lines.append(line)
        else:
            active_lines.append(line)

    # Compute portfolio P&L
    pnl_values = []
    for pos in positions:
        if pos.get("mode") != "ACTIVE":
            continue
        mkt = market_snapshots.get(str(pos.get("id")), {})
        ep  = pos.get("entry_price")
        mid = mkt.get("mid")
        if ep and mid and ep > 0:
            pnl_values.append((mid - ep) / ep * 100)

    avg_pnl_str = f"{sum(pnl_values) / len(pnl_values):+.1f}%" if pnl_values else "N/A"

    body = (
        f"LEAPS DAILY SUMMARY\n"
        f"{'─'*60}\n"
        f"  {today}   |   Active: {len(active_lines)}   Watchlist: {len(watchlist_lines)}\n"
        f"{'─'*60}\n\n"
        + (
            "ACTIVE POSITIONS\n"
            + "─" * 60 + "\n"
            + "\n".join(active_lines) + "\n"
            + f"\n  Portfolio Average P&L: {avg_pnl_str}\n\n"
            if active_lines else ""
        )
        + (
            "WATCHLIST (waiting for entry)\n"
            + "─" * 60 + "\n"
            + "\n".join(watchlist_lines) + "\n"
            if watchlist_lines else ""
        )
    )

    return send_alert(subject, body)
