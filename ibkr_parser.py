"""
IBKR Activity Statement Parser.

Parses Interactive Brokers Activity Statement exports to extract option trades
and determine per-contract net actions (NEW, UPDATE/TRIM, CLOSED, ROLLED).

Supported input formats:
  1. PDF  — default Activity Statement from Account Management > Reports
  2. CSV  — any CSV export with at least the columns described below

IBKR option symbol format:  TICKER DDMONYY STRIKE C/P
  Example: NVDA 16JAN27 150 C  |  AEHR 15JAN27 40 C  |  ENVX 16JAN26 7 C
  Asset category in CSV: "Equity and Index Options"

Trade rows have:
  Symbol | Date/Time | Quantity | T. Price | Proceeds | Comm/Fee | Code
  Quantity: positive = BUY, negative = SELL (in contracts, not shares)
  T. Price: option premium per share
  Code: O = opening, C = closing, A = assignment, Ex = exercise, Ep = expiration
"""
from __future__ import annotations


import re
import io
import csv
from datetime import date, datetime
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

_MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3,  "APR": 4,  "MAY": 5,  "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9,  "OCT": 10, "NOV": 11, "DEC": 12,
}

_OPT_SYM_RE = re.compile(
    r'^([A-Z]{1,6})\s+(\d{1,2})([A-Z]{3})(\d{2})\s+([\d.]+)\s+([CP])$'
)


@dataclass
class IBKRTrade:
    """One filled option trade from IBKR."""
    ticker:      str
    strike:      float
    expiry:      date
    option_type: str        # 'C' or 'P'
    quantity:    int        # positive = BUY, negative = SELL
    price:       float      # premium per share (T. Price)
    proceeds:    float      # total cash (negative = cost, positive = received)
    trade_date:  date
    code:        str        # O / C / A / Ex / Ep / blank
    raw_symbol:  str        # original IBKR symbol string


@dataclass
class PositionSummary:
    """Aggregated net view of one contract across all trades."""
    ticker:      str
    strike:      float
    expiry:      date
    option_type: str

    total_bought:   int   = 0
    avg_buy_price:  float = 0.0
    total_sold:     int   = 0
    avg_sell_price: float = 0.0
    net_qty:        int   = 0    # positive = still holding
    gross_proceeds: float = 0.0  # money received from sells (positive)
    total_cost:     float = 0.0  # money paid for buys (positive)

    action:         str   = ""   # NEW / TRIM / CLOSED / ROLLED / COVERED
    rolled_into:    Optional["PositionSummary"] = field(default=None, repr=False)

    def contract_label(self) -> str:
        mon = [k for k, v in _MONTH_MAP.items() if v == self.expiry.month][0]
        return f"{self.ticker} {self.expiry.day:02d}{mon}{str(self.expiry.year)[2:]} {self.strike} {self.option_type}"


# ---------------------------------------------------------------------------
# Symbol parser
# ---------------------------------------------------------------------------

def parse_ibkr_symbol(sym: str) -> Optional[dict]:
    """
    Parse 'NVDA 16JAN27 150 C' → {ticker, strike, expiry, option_type}.
    Returns None on failure.
    """
    sym = sym.strip().upper()
    m = _OPT_SYM_RE.match(sym)
    if not m:
        return None
    ticker, day_str, mon_str, yr_str, strike_str, opt_type = m.groups()
    try:
        exp = date(2000 + int(yr_str), _MONTH_MAP[mon_str], int(day_str))
        return {
            "ticker":      ticker,
            "strike":      float(strike_str),
            "expiry":      exp,
            "option_type": opt_type,
        }
    except Exception:
        return None


def _parse_date(s: str) -> Optional[date]:
    """Parse IBKR date strings: '2026-01-15', '2026-01-15, 10:30:15', '01/15/2026'."""
    s = s.strip().split(",")[0].strip()   # strip time component
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _safe_float(s) -> float:
    if s is None:
        return 0.0
    try:
        return float(str(s).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# CSV parser
# ---------------------------------------------------------------------------

# Column name aliases — IBKR uses slightly different names across report types
_COL_ALIASES = {
    "symbol":    ["symbol", "sym", "description"],
    "date":      ["date/time", "datetime", "date", "trade date"],
    "quantity":  ["quantity", "qty"],
    "price":     ["t. price", "t.price", "price", "trade price"],
    "proceeds":  ["proceeds"],
    "comm":      ["comm/fee", "comm", "fee", "commission"],
    "code":      ["code", "open/close", "notes"],
    "asset":     ["asset category", "asset class", "type"],
}


def _find_col(headers: list[str], key: str) -> int:
    """Return index of the first matching header alias, or -1."""
    lowers = [h.lower().strip() for h in headers]
    for alias in _COL_ALIASES.get(key, [key]):
        if alias in lowers:
            return lowers.index(alias)
    return -1


def _parse_csv_content(content: str) -> list[IBKRTrade]:
    """
    Parse a CSV export from IBKR.

    Handles two layouts:
    A) Flat CSV (one section, already filtered): all rows are trades
    B) Multi-section CSV (full activity statement): rows include section headers;
       we look for "Trades" section + "Options" asset category
    """
    trades: list[IBKRTrade] = []

    reader = csv.reader(io.StringIO(content))
    rows = list(reader)
    if not rows:
        return trades

    # ── Layout B: full activity statement CSV ─────────────────────────────
    # Rows look like: SectionName, DataType, col1, col2, ...
    # e.g.: "Trades","Header","Asset Category","Symbol","Date/Time",...
    #       "Trades","Data","Equity and Index Options","AEHR 15JAN27 40 C","2026-01-15, 09:30",...
    #       "Trades","SubTotal",...
    # Asset category is "Equity and Index Options" (not just "Options").

    # Scan ALL rows for section names (case-insensitive) — "Trades" may appear far into the file
    first_col_lower = {r[0].strip().lower() for r in rows if r}
    is_multi = bool(first_col_lower & {"trades", "open positions", "closed positions"})

    if is_multi:
        headers = None
        for row in rows:
            if len(row) < 3:
                continue
            section  = row[0].strip()
            row_type = row[1].strip() if len(row) > 1 else ""
            if section.lower() != "trades":
                headers = None
                continue
            if row_type.lower() == "header":
                headers = [c.strip() for c in row[2:]]
                continue
            if row_type.lower() != "data" or headers is None:
                continue
            data = row[2:]
            # Asset category is "Equity and Index Options" — filter out non-option rows
            asset_idx = _find_col(headers, "asset")
            if asset_idx >= 0 and asset_idx < len(data):
                if "option" not in data[asset_idx].lower():
                    continue
            trade = _row_to_trade(headers, data)
            if trade:
                trades.append(trade)
        if trades:
            return trades
        # Fall through to Layout A if multi-section parse found no trades

    # ── Layout A: flat CSV ─────────────────────────────────────────────────
    # First non-empty row is the header
    header_row = None
    for row in rows:
        if any(c.strip() for c in row):
            header_row = row
            break
    if header_row is None:
        return trades

    headers = [c.strip() for c in header_row]
    for row in rows[rows.index(header_row) + 1:]:
        if not any(c.strip() for c in row):
            continue
        # Skip sub-total / total rows
        first = row[0].strip().lower() if row else ""
        if any(x in first for x in ("subtotal", "total", "summary")):
            continue
        trade = _row_to_trade(headers, row)
        if trade:
            trades.append(trade)

    return trades


def _row_to_trade(headers: list[str], data: list[str]) -> Optional[IBKRTrade]:
    """Map a data row to an IBKRTrade using column name lookup."""
    def get(key):
        idx = _find_col(headers, key)
        return data[idx].strip() if (idx >= 0 and idx < len(data)) else ""

    sym_raw = get("symbol")
    parsed  = parse_ibkr_symbol(sym_raw)
    if not parsed:
        return None

    qty_str  = get("quantity")
    qty_sign = -1 if qty_str.startswith("-") else 1
    qty      = int(abs(_safe_float(qty_str)))
    if qty == 0:
        return None

    dt = _parse_date(get("date")) or date.today()

    return IBKRTrade(
        ticker=      parsed["ticker"],
        strike=      parsed["strike"],
        expiry=      parsed["expiry"],
        option_type= parsed["option_type"],
        quantity=    qty * qty_sign,
        price=       abs(_safe_float(get("price"))),
        proceeds=    _safe_float(get("proceeds")),
        trade_date=  dt,
        code=        get("code"),
        raw_symbol=  sym_raw,
    )


# ---------------------------------------------------------------------------
# PDF parser
# ---------------------------------------------------------------------------

def _parse_pdf_content(pdf_bytes: bytes) -> list[IBKRTrade]:
    """
    Parse an IBKR Activity Statement PDF using pdfplumber.

    Strategy:
      1. Try structured table extraction — most reliable for machine-generated PDFs.
      2. Fall back to line-by-line text parsing.
    """
    try:
        import pdfplumber
    except ImportError:
        raise ImportError(
            "pdfplumber is required for PDF parsing. "
            "Install it with: pip install pdfplumber"
        )

    trades: list[IBKRTrade] = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        # ── Strategy 1: extract structured tables ─────────────────────────
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 2:
                    continue
                # Find rows with option data
                header_idx = None
                for i, row in enumerate(table):
                    if not row:
                        continue
                    row_lower = " ".join(str(c).lower() for c in row if c)
                    if ("symbol" in row_lower or "quantity" in row_lower) and "price" in row_lower:
                        header_idx = i
                        break
                if header_idx is None:
                    continue

                headers = [str(c).strip() if c else "" for c in table[header_idx]]
                for row in table[header_idx + 1:]:
                    if not row or not any(row):
                        continue
                    data = [str(c).strip() if c else "" for c in row]
                    trade = _row_to_trade(headers, data)
                    if trade:
                        trades.append(trade)

        if trades:
            return trades

        # ── Strategy 2: text line scanning ────────────────────────────────
        # IBKR PDF text has lines that look like:
        #   "Options  NVDA 16JAN27 150 C  2026-01-15, 10:30:15  10  3.50  ..."
        # or the symbol appears alone followed by trade detail columns

        full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
        in_trades_section = False
        pending_symbol: Optional[dict] = None
        date_re = re.compile(r'\d{4}-\d{2}-\d{2}')
        number_re = re.compile(r'^-?[\d,]+\.?\d*$')

        for line in full_text.splitlines():
            line = line.strip()
            if not line:
                continue

            # Section detection
            if re.match(r'^Trades\b', line, re.IGNORECASE):
                in_trades_section = True
                continue
            if re.match(r'^(Account Information|Open Positions|Closed Positions|'
                        r'Financial Instrument Information|Dividends|Deposits)', line, re.IGNORECASE):
                in_trades_section = False
                pending_symbol = None
                continue

            if not in_trades_section:
                continue

            # Try parsing as a full trade row: symbol + date + qty + price + ...
            # Some PDFs put symbol and data on same line
            sym_match = _OPT_SYM_RE.search(line)
            if not sym_match:
                continue

            sym_str = sym_match.group(0)
            parsed  = parse_ibkr_symbol(sym_str)
            if not parsed:
                continue

            # Find date, quantity, price in rest of line
            rest = line[sym_match.end():].strip()
            tokens = rest.split()

            # Find date token
            dt = None
            price_idx = None
            qty = None
            price_val = None
            for i, tok in enumerate(tokens):
                if date_re.match(tok.split(",")[0]):
                    dt = _parse_date(tok)
                    continue
                if number_re.match(tok) and dt is not None and qty is None:
                    try:
                        q = int(tok.replace(",", ""))
                        if abs(q) > 0:
                            qty = q
                            price_idx = i + 1
                    except ValueError:
                        pass
                    continue
                if price_idx is not None and i == price_idx and number_re.match(tok):
                    try:
                        price_val = abs(float(tok.replace(",", "")))
                    except ValueError:
                        pass
                    break

            if qty is None or price_val is None:
                continue

            trades.append(IBKRTrade(
                ticker=      parsed["ticker"],
                strike=      parsed["strike"],
                expiry=      parsed["expiry"],
                option_type= parsed["option_type"],
                quantity=    qty,
                price=       price_val,
                proceeds=    -(qty * price_val * 100) if qty > 0 else abs(qty) * price_val * 100,
                trade_date=  dt or date.today(),
                code=        "",
                raw_symbol=  sym_str,
            ))

    return trades


# ---------------------------------------------------------------------------
# Aggregation into PositionSummary
# ---------------------------------------------------------------------------

def _contract_key(ticker, strike, expiry, option_type) -> tuple:
    return (ticker.upper(), round(float(strike), 2), expiry, option_type.upper())


def aggregate_trades(trades: list[IBKRTrade]) -> list[PositionSummary]:
    """
    Aggregate a list of individual trades into per-contract PositionSummary objects.
    """
    buckets: dict[tuple, list[IBKRTrade]] = {}
    for t in trades:
        key = _contract_key(t.ticker, t.strike, t.expiry, t.option_type)
        buckets.setdefault(key, []).append(t)

    summaries: list[PositionSummary] = []

    for key, contract_trades in buckets.items():
        ticker, strike, expiry, option_type = key

        total_bought   = sum(t.quantity for t in contract_trades if t.quantity > 0)
        total_sold     = sum(abs(t.quantity) for t in contract_trades if t.quantity < 0)
        net_qty        = total_bought - total_sold
        gross_proceeds = sum(abs(t.proceeds) for t in contract_trades if t.quantity < 0 and t.proceeds > 0)
        total_cost     = sum(abs(t.proceeds) for t in contract_trades if t.quantity > 0)

        buy_trades  = [t for t in contract_trades if t.quantity > 0]
        sell_trades = [t for t in contract_trades if t.quantity < 0]

        avg_buy  = (sum(t.price * t.quantity       for t in buy_trades)  / total_bought
                    if total_bought  else 0.0)
        avg_sell = (sum(t.price * abs(t.quantity)  for t in sell_trades) / total_sold
                    if total_sold else 0.0)

        ps = PositionSummary(
            ticker=        ticker,
            strike=        strike,
            expiry=        expiry,
            option_type=   option_type,
            total_bought=  total_bought,
            avg_buy_price= round(avg_buy,  4),
            total_sold=    total_sold,
            avg_sell_price=round(avg_sell, 4),
            net_qty=       net_qty,
            gross_proceeds=round(gross_proceeds, 2),
            total_cost=    round(total_cost, 2),
        )
        summaries.append(ps)

    return summaries


def classify_actions(summaries: list[PositionSummary]) -> list[PositionSummary]:
    """
    Assign an action label to each PositionSummary.

    Actions:
      NEW      — only buys, no prior context expected in BQ
      TRIM     — partial sell; net_qty > 0
      CLOSED   — net_qty == 0 (fully sold or expired)
      ROLLED   — this ticker has a same-day close + open of different expiry
      COVERED  — net_qty < 0 (oversold — assignment / covered call)
    """
    # Group by ticker for roll detection
    by_ticker: dict[str, list[PositionSummary]] = {}
    for ps in summaries:
        by_ticker.setdefault(ps.ticker, []).append(ps)

    for ps in summaries:
        if ps.total_bought > 0 and ps.total_sold == 0:
            ps.action = "NEW"
        elif ps.net_qty == 0 and ps.total_sold > 0:
            ps.action = "CLOSED"
        elif ps.net_qty < 0:
            ps.action = "COVERED"
        elif ps.total_sold > 0 and ps.net_qty > 0:
            ps.action = "TRIM"
        else:
            ps.action = "NEW"

    # Roll detection: ticker has CLOSED + NEW positions
    for ticker, ticker_sums in by_ticker.items():
        closed = [ps for ps in ticker_sums if ps.action == "CLOSED"]
        new    = [ps for ps in ticker_sums if ps.action == "NEW"]
        if closed and new:
            for c in closed:
                c.action = "ROLLED"
            for n in new:
                n.action = "ROLLED_INTO"

    return summaries


# ---------------------------------------------------------------------------
# Top-level entry points
# ---------------------------------------------------------------------------

def parse_pdf(pdf_bytes: bytes) -> tuple[list[IBKRTrade], list[PositionSummary]]:
    """Parse an IBKR PDF activity statement. Returns (raw_trades, position_summaries)."""
    trades = _parse_pdf_content(pdf_bytes)
    summaries = classify_actions(aggregate_trades(trades))
    return trades, summaries


def parse_csv(csv_content: str) -> tuple[list[IBKRTrade], list[PositionSummary]]:
    """Parse an IBKR CSV activity statement. Returns (raw_trades, position_summaries)."""
    trades = _parse_csv_content(csv_content)
    summaries = classify_actions(aggregate_trades(trades))
    return trades, summaries


def diagnose_csv(file_bytes: bytes) -> dict:
    """
    Return a diagnostic dict describing what was found in a CSV upload.
    Used to show a helpful error when no option trades are detected.

    Returns keys: sections, has_trades_section, sample_rows, asset_categories, symbol_samples
    """
    content = file_bytes.decode("utf-8-sig", errors="replace")
    reader  = csv.reader(io.StringIO(content))
    rows    = list(reader)

    sections: set[str]  = set()
    asset_cats: set[str] = set()
    symbol_samples: list[str] = []
    sample_rows: list[str] = []

    for row in rows[:5]:
        if row:
            sample_rows.append(", ".join(row[:6]))

    for row in rows:
        if not row:
            continue
        sections.add(row[0].strip())
        # Collect asset categories from multi-section format
        if len(row) >= 3 and row[1].strip().lower() == "data":
            asset_cats.add(row[2].strip())
        # Collect candidate symbol values (anything matching option pattern)
        for cell in row:
            cell = cell.strip()
            if _OPT_SYM_RE.match(cell.upper()):
                if cell not in symbol_samples:
                    symbol_samples.append(cell)
                    if len(symbol_samples) >= 5:
                        break

    return {
        "sections":          sorted(sections)[:20],
        "has_trades_section": any(s.lower() == "trades" for s in sections),
        "asset_categories":  sorted(asset_cats)[:20],
        "symbol_samples":    symbol_samples,
        "first_rows":        sample_rows,
    }


def parse_file(file_bytes: bytes, filename: str) -> tuple[list[IBKRTrade], list[PositionSummary]]:
    """Detect format from filename and route to the correct parser."""
    ext = filename.lower().rsplit(".", 1)[-1]
    if ext == "pdf":
        return parse_pdf(file_bytes)
    elif ext in ("csv", "txt"):
        return parse_csv(file_bytes.decode("utf-8-sig", errors="replace"))  # utf-8-sig strips BOM
    else:
        raise ValueError(f"Unsupported file type: .{ext}. Upload a PDF or CSV.")


# ---------------------------------------------------------------------------
# BQ reconciliation helper
# ---------------------------------------------------------------------------

def reconcile_with_bq(
    summaries: list[PositionSummary],
    bq_positions: list[dict],
) -> list[dict]:
    """
    Compare parsed IBKR summaries against existing BigQuery positions.

    Returns a list of change dicts, one per summary:
    {
      "summary":    PositionSummary,
      "bq_pos":     dict | None,        # matching BQ record (None = no match)
      "bq_action":  str,                # CREATE / UPDATE / CLOSE / SKIP
      "changes":    dict,               # fields to write to BQ
      "conflict":   str,                # warning message if data looks inconsistent
    }
    """
    result = []

    def _match(s: PositionSummary) -> Optional[dict]:
        """Find the closest matching BQ position for a summary."""
        for p in bq_positions:
            t = str(p.get("ticker", "")).upper()
            if t != s.ticker:
                continue
            try:
                bq_strike = float(p.get("strike") or 0)
                if abs(bq_strike - s.strike) > 0.5:
                    continue
            except Exception:
                continue
            try:
                raw_exp = p.get("expiration_date")
                if raw_exp:
                    bq_exp = raw_exp if isinstance(raw_exp, date) else date.fromisoformat(str(raw_exp)[:10])
                    if abs((bq_exp - s.expiry).days) > 5:
                        continue
            except Exception:
                continue
            return p
        return None

    for s in summaries:
        bq = _match(s)
        changes: dict = {}
        conflict = ""
        bq_action = "SKIP"

        if s.action in ("NEW", "ROLLED_INTO"):
            if bq is None:
                # New position not yet in BQ — create it
                bq_action = "CREATE"
                changes = {
                    "ticker":          s.ticker,
                    "strike":          s.strike,
                    "expiration_date": s.expiry,
                    "option_type":     "CALL" if s.option_type == "C" else "PUT",
                    "entry_price":     s.avg_buy_price,
                    "quantity":        s.net_qty,
                    "mode":            "ACTIVE",
                    "quantity_trimmed":    s.total_sold,
                    "proceeds_from_trims": s.gross_proceeds,
                }
            else:
                # Exists — update qty and price if different
                bq_action = "UPDATE"
                bq_qty   = int(bq.get("quantity") or 0)
                bq_price = float(bq.get("entry_price") or 0)
                if abs(bq_qty - s.net_qty) > 0:
                    changes["quantity"] = s.net_qty
                if bq_price == 0 or abs(bq_price - s.avg_buy_price) > 0.05:
                    changes["entry_price"] = s.avg_buy_price
                trim_delta = s.total_sold - int(bq.get("quantity_trimmed") or 0)
                if trim_delta > 0:
                    changes["quantity_trimmed"]    = s.total_sold
                    changes["proceeds_from_trims"] = round(
                        float(bq.get("proceeds_from_trims") or 0) + s.gross_proceeds, 2
                    )
                if not changes:
                    bq_action = "SKIP"

        elif s.action in ("TRIM",):
            if bq is None:
                conflict = "Trim found but no matching BQ position"
                bq_action = "SKIP"
            else:
                bq_action = "UPDATE"
                bq_trimmed = int(bq.get("quantity_trimmed") or 0)
                bq_proceeds = float(bq.get("proceeds_from_trims") or 0)
                new_trimmed  = max(bq_trimmed, s.total_sold)
                new_proceeds = bq_proceeds + s.gross_proceeds if s.gross_proceeds > 0 else bq_proceeds
                new_qty = int(bq.get("quantity") or 0) - (new_trimmed - bq_trimmed)
                changes = {
                    "quantity":            max(new_qty, 0),
                    "quantity_trimmed":    new_trimmed,
                    "proceeds_from_trims": round(new_proceeds, 2),
                }

        elif s.action in ("CLOSED", "ROLLED"):
            if bq is not None:
                bq_action = "CLOSE"
                changes = {
                    "mode":                "CLOSED",
                    "quantity_trimmed":    s.total_sold,
                    "proceeds_from_trims": round(s.gross_proceeds, 2),
                }
            # else: already closed or never in BQ — skip

        result.append({
            "summary":   s,
            "bq_pos":    bq,
            "bq_action": bq_action,
            "changes":   changes,
            "conflict":  conflict,
        })

    return result
