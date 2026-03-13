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


def send_daily_summary(
    positions: list[dict],
    market_snapshots: dict,
    posture_changes: dict | None = None,
    watchlist_signals: dict | None = None,
    earnings_section: str = "",
) -> tuple[bool, str]:
    """
    Build and send the 5 PM daily portfolio summary email.

    Args:
        positions:         list of position dicts (from db.get_positions)
        market_snapshots:  dict keyed by position id → market data dict (active only)
        posture_changes:   dict keyed by position id → new posture (changed since yesterday)
        watchlist_signals: dict keyed by position id → {entry_alert, thesis_score, iv_rank, price, rsi,
                                                         rec_strike, rec_expiry, rec_premium, rec_delta, rec_otm_pct}
        earnings_section:  optional pre-built earnings calendar text block
    """
    from exit_engine import evaluate

    posture_changes   = posture_changes   or {}
    watchlist_signals = watchlist_signals or {}
    today   = datetime.now().strftime("%B %d, %Y")
    subject = f"📊 LEAPS Daily — {today}"

    _SEVERITY      = {"RED": 4, "BLUE": 3, "AMBER": 2, "GREEN": 1}
    _POSTURE_EMOJI = {"EXIT": "🔴", "ROLL": "🔵", "WATCH": "⚠️", "HOLD": "🟢"}

    # ── SECTION 1: Active positions ─────────────────────────────────────────
    active_lines  = []
    changed_lines = []

    for pos in positions:
        if pos.get("mode") != "ACTIVE":
            continue
        ticker = pos.get("ticker", "?")
        strike = pos.get("strike", "?")
        exp    = pos.get("expiration_date", "?")
        role   = pos.get("position_type", "CORE")
        qty    = pos.get("quantity", "?")
        pos_id = str(pos.get("id", ""))

        mkt         = market_snapshots.get(pos_id, {})
        mid         = mkt.get("mid")
        delta       = mkt.get("delta")
        dte         = mkt.get("dte")
        score       = mkt.get("thesis_score")
        entry_price = pos.get("entry_price")

        pnl_pct = None
        if entry_price and mid and entry_price > 0:
            pnl_pct = round((mid - entry_price) / entry_price * 100, 1)

        alerts = evaluate(pos, mkt)
        if not alerts:
            posture = "HOLD"
        else:
            worst   = max(alerts, key=lambda a: _SEVERITY.get(a.severity, 0))
            posture = {"RED": "EXIT", "BLUE": "ROLL", "AMBER": "WATCH", "GREEN": "HOLD"}.get(worst.severity, "HOLD")

        emoji     = _POSTURE_EMOJI.get(posture, "🟢")
        pnl_str   = f"{pnl_pct:+.1f}%" if pnl_pct   is not None else "N/A"
        delta_str = f"Δ {delta:.2f}"    if delta     is not None else "Δ N/A"
        dte_str   = f"DTE {dte}d"       if dte       is not None else "DTE N/A"
        score_str = f"Score {score}/100" if score    is not None else "Score N/A"
        changed   = "  ← CHANGED" if pos_id in posture_changes else ""

        line = (
            f"  {emoji} {ticker:<6} {exp} ${strike}C  {qty}x"
            f"  | P&L {pnl_str:<8} | {delta_str:<10} | {dte_str:<10} | {score_str:<14}"
            f"| {role}  → {posture}{changed}"
        )
        active_lines.append(line)

        if pos_id in posture_changes:
            changed_lines.append(f"  {emoji} {ticker:<6} ${strike}C  → {posture}")

    # Portfolio average P&L
    pnl_values = [
        (market_snapshots.get(str(p.get("id")), {}).get("mid") - p.get("entry_price"))
        / p.get("entry_price") * 100
        for p in positions
        if p.get("mode") == "ACTIVE"
        and market_snapshots.get(str(p.get("id")), {}).get("mid") is not None
        and p.get("entry_price") and p.get("entry_price") > 0
    ]
    avg_pnl_str = f"{sum(pnl_values) / len(pnl_values):+.1f}%" if pnl_values else "N/A"

    # ── SECTION 2: Watchlist positions ──────────────────────────────────────
    _ENTRY_EMOJI = {"GREEN": "🟢", "AMBER": "⚠️", "RED": "🔴"}
    watchlist_lines = []

    for pos in positions:
        if pos.get("mode") != "WATCHLIST":
            continue
        ticker = pos.get("ticker", "?")
        pos_id = str(pos.get("id", ""))
        sig    = watchlist_signals.get(pos_id, {})

        score       = sig.get("thesis_score")
        iv_rank     = sig.get("iv_rank")
        price       = sig.get("price")
        rsi         = sig.get("rsi")
        entry_alert = sig.get("entry_alert")

        score_str  = f"Score {score}/100" if score    is not None else "Score N/A"
        ivr_str    = f"IVR {iv_rank:.0f}%" if iv_rank is not None else "IVR N/A"
        price_str  = f"${price:.2f}"       if price   is not None else "N/A"
        rsi_str    = f"RSI {rsi:.0f}"      if rsi     is not None else ""

        if entry_alert:
            e_emoji = _ENTRY_EMOJI.get(entry_alert.severity, "⚪")
            if entry_alert.severity == "GREEN":
                signal_str = f"🟢 BUY NOW — {entry_alert.type}"
            elif entry_alert.severity == "AMBER":
                signal_str = f"🟡 IMPROVING — {entry_alert.type}"
            else:
                signal_str = f"{e_emoji} {entry_alert.type}"
        else:
            signal_str = "⚪ Waiting — entry conditions not met yet"

        # Contract recommendation (if available)
        rec_strike  = sig.get("rec_strike")
        rec_expiry  = sig.get("rec_expiry")
        rec_premium = sig.get("rec_premium")
        rec_delta   = sig.get("rec_delta")
        rec_otm     = sig.get("rec_otm_pct")
        if rec_strike and rec_expiry:
            rec_str = (
                f"  → Rec contract: ${rec_strike}C {rec_expiry} | "
                f"${rec_premium:.2f}/share | Δ {rec_delta} | {rec_otm}% OTM"
            ) if rec_premium else f"  → Rec contract: ${rec_strike}C {rec_expiry}"
        else:
            rec_str = ""

        line = (
            f"  {ticker:<6}  {price_str:<8} | {score_str:<14} | {ivr_str:<10}"
            f"| {rsi_str:<8}| {signal_str}"
        )
        if rec_str:
            line += f"\n{rec_str}"
        watchlist_lines.append(line)

    # ── Build email body ─────────────────────────────────────────────────────
    active_count    = len(active_lines)
    watchlist_count = len(watchlist_lines)

    body = (
        f"LEAPS DAILY BRIEFING — 5:00 PM ET\n"
        f"{'─'*60}\n"
        f"  {today}   |   Active: {active_count}   Watchlist: {watchlist_count}\n"
        f"{'─'*60}\n\n"
    )

    # Changes callout (top of email)
    if changed_lines:
        body += (
            "🔔 RECOMMENDATION CHANGES SINCE YESTERDAY\n"
            + "─" * 60 + "\n"
            + "\n".join(changed_lines) + "\n\n"
        )
    else:
        body += "  ✓ No recommendation changes since yesterday.\n\n"

    # Section 1
    body += (
        "━" * 60 + "\n"
        "  SECTION 1 — ACTIVE POSITIONS (Portfolio Management)\n"
        + "━" * 60 + "\n"
    )
    if active_lines:
        body += (
            "\n".join(active_lines) + "\n"
            + f"\n  Portfolio Average P&L: {avg_pnl_str}\n\n"
        )
    else:
        body += "  No active positions.\n\n"

    # Section 2
    body += (
        "━" * 60 + "\n"
        "  SECTION 2 — WATCHLIST (New Position Candidates)\n"
        + "━" * 60 + "\n"
    )
    if watchlist_lines:
        body += "\n".join(watchlist_lines) + "\n"
    else:
        body += "  No watchlist positions.\n"

    # Optional earnings calendar section
    if earnings_section:
        body += (
            "\n" + "━" * 60 + "\n"
            "  EARNINGS CALENDAR\n"
            + "━" * 60 + "\n"
            + earnings_section + "\n"
        )

    return send_alert(subject, body)
