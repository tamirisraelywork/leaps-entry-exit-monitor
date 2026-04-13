import yfinance as yf
import pandas as pd
from datetime import datetime
import time
import random
import logging
import requests
from curl_cffi import requests as crequests
import sys

from shared.config import cfg

# --- API Key from Secrets (try key 3 first, fall back to 1 or 2) ---
ALPHA_VANTAGE_KEY = (
    cfg("ALPHA_VANTAGE_API_KEY_3")
    or cfg("ALPHA_VANTAGE_API_KEY_1")
    or cfg("ALPHA_VANTAGE_API_KEY_2")
)

# --- Configured logging to track errors and retries ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def format_large_number(num):
    """
    Converts numbers to strings in Millions, Billions, or Trillions.
    """
    if num is None or not isinstance(num, (int, float)):
        return "N/A"

    abs_num = abs(num)
    if abs_num >= 1_000_000_000_000:
        return f"{num / 1_000_000_000_000:.2f} Trillion"
    elif abs_num >= 1_000_000_000:
        return f"{num / 1_000_000_000:.2f} Billion"
    elif abs_num >= 1_000_000:
        return f"{num / 1_000_000:.2f} Million"
    else:
        return f"{num:.2f}"

def get_latest_metric(df, possible_keys):
    """
    Searches for the first matching key in the dataframe and returns
    the value from the most recent period.
    """
    if df is None or df.empty:
        return None, None
    for key in possible_keys:
        if key in df.index:
            try:
                val = df.loc[key].iloc[0]
                if pd.notnull(val):
                    return val, key
            except (IndexError, AttributeError):
                continue
    return None, None

def run_comprehensive_analysis(ticker_symbol):
    # Proxy Configuration from first code
    PROXY_USER = cfg("PROXY_USER")
    PROXY_PASS = cfg("PROXY_PASS")
    PROXY_HOST = "gw.dataimpulse.com"
    PROXY_PORT = "823"
    proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"

    # Proxy dictionary for requests (Alpha Vantage)
    proxies = {
        "http": proxy_url,
        "https": proxy_url
    }

    max_retries = 5
    retry_count = 0
    # Cycle through different Chrome versions per attempt — each has a distinct TLS
    # fingerprint, making it harder for Yahoo to fingerprint-block consecutive retries.
    _impersonations = ["chrome", "chrome110", "chrome107", "chrome104", "chrome101"]

    results = {"ticker": ticker_symbol, "status": "success", "data": {}, "error": None}

    # Initialize variables for the final report
    current_price = None
    market_cap = None
    low_52 = None
    high_52 = None
    latest_expiry = "N/A"
    insider_val = "N/A"
    total_assets = None
    total_liabilities = None
    al_ratio = None
    runway_val = "N/A"
    ebitda = None
    net_debt_raw = None
    nd_ebitda_val = "N/A"
    severity_val = "N/A"
    share_growth_val = "N/A"
    dol_val = "N/A"
    csp_status = "No converts"
    shares_outstanding = None
    revenue_growth_val = "N/A"
    gross_margin_val = "N/A"
    growth_to_val_score = "N/A"

    # Helper to clean Alpha Vantage string values from second code
    def av_clean(val):
        try:
            return float(val) if val and str(val).lower() != "none" else 0.0
        except (ValueError, TypeError):
            return 0.0

    while retry_count < max_retries:
        try:
            # Each attempt uses a rotating proxy IP + a different Chrome version.
            # Varying the TLS fingerprint per retry makes pattern-based blocking harder.
            _impersonate = _impersonations[retry_count % len(_impersonations)]
            logging.info(f"Attempt {retry_count + 1}/{max_retries} for {ticker_symbol} — proxy ({_impersonate})")
            _session = crequests.Session(proxies=proxies, impersonate=_impersonate)
            ticker = yf.Ticker(ticker_symbol, session=_session)

            # Fetching Info — handle rate-limit gracefully without forcing a full retry.
            # When Yahoo is throttled it either raises "Too Many Requests" or returns
            # a near-empty dict.  In both cases we switch to AV-only mode so the
            # analysis still completes via the per-field Alpha Vantage fallbacks below.
            _yf_rate_limited = False
            info = {}
            try:
                _raw_info = ticker.info or {}
                if not _raw_info or len(_raw_info) < 10 or not any(
                    _raw_info.get(k) for k in ("currentPrice", "regularMarketPrice", "marketCap", "longName")
                ):
                    _yf_rate_limited = True
                    logging.warning(
                        f"Yahoo Finance returned minimal data for {ticker_symbol} "
                        f"({len(_raw_info)} keys) — switching to AV-only mode."
                    )
                else:
                    info = _raw_info
            except Exception as _info_exc:
                _es = str(_info_exc).lower()
                if "too many requests" in _es or "rate limit" in _es or "429" in _es:
                    _yf_rate_limited = True
                    logging.warning(
                        f"Yahoo Finance rate-limited for {ticker_symbol} — switching to AV-only mode."
                    )
                else:
                    raise  # propagate genuine errors (network timeout, bad ticker, etc.)

            # Financial DataFrames — skip all YF calls when rate-limited (they'd fail too).
            if _yf_rate_limited:
                q_balance_sheet = None
                a_balance_sheet = None
                q_cash_flow = None
                a_financials = None
            else:
                try:
                    q_balance_sheet = ticker.quarterly_balance_sheet
                except Exception:
                    q_balance_sheet = None
                try:
                    a_balance_sheet = ticker.balance_sheet
                except Exception:
                    a_balance_sheet = None
                try:
                    q_cash_flow = ticker.quarterly_cashflow
                except Exception:
                    q_cash_flow = None
                try:
                    a_financials = ticker.financials
                except Exception:
                    a_financials = None

            # 1. Price, Low, High, Market Cap (YFinance primary)
            current_price = info.get('currentPrice') or info.get('regularMarketPrice')
            market_cap = info.get('marketCap')
            shares_outstanding = info.get('sharesOutstanding')
            low_52 = info.get('fiftyTwoWeekLow')
            high_52 = info.get('fiftyTwoWeekHigh')

            # --- Alpha Vantage Backup for Price/Cap/Shares/Range (from second code) ---
            if not current_price or not market_cap:
                logging.info(f"Price/Cap missing in YF for {ticker_symbol}. Checking Alpha Vantage...")
                try:
                    ov_url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    ov_resp = requests.get(ov_url, proxies=proxies, timeout=15)
                    ov_data = ov_resp.json()

                    if not current_price:
                        gq_url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                        gq_resp = requests.get(gq_url, proxies=proxies, timeout=15)
                        gq_data = gq_resp.json().get("Global Quote", {})
                        current_price = av_clean(gq_data.get("05. price"))

                    if not market_cap:
                        market_cap = av_clean(ov_data.get("MarketCapitalization"))
                    if not shares_outstanding:
                        shares_outstanding = av_clean(ov_data.get("SharesOutstanding"))
                    if not low_52:
                        low_52 = av_clean(ov_data.get("52WeekLow"))
                    if not high_52:
                        high_52 = av_clean(ov_data.get("52WeekHigh"))
                except Exception as e:
                    logging.error(f"AV Price Backup Error for {ticker_symbol}: {e}")

            # 52-week position: 0.0 = at 52wk low, 1.0 = at 52wk high
            wk52_position_val = "N/A"
            if current_price and low_52 and high_52 and high_52 > low_52:
                wk52_position_val = round((current_price - low_52) / (high_52 - low_52), 4)

            # 4. Latest expiration date — Yahoo first, Polygon.io as fallback
            try:
                if _yf_rate_limited:
                    raise Exception("skipped — YF rate-limited")
                options = ticker.options
                latest_expiry = options[-1] if options else "N/A"
            except Exception:
                latest_expiry = "N/A"

            # Polygon.io fallback for options expiration date
            if latest_expiry == "N/A":
                try:
                    polygon_key = cfg("POLYGON_API_KEY_2")
                    if polygon_key:
                        today_str = datetime.now().strftime("%Y-%m-%d")
                        poly_url = (
                            f"https://api.polygon.io/v3/reference/options/contracts"
                            f"?underlying_ticker={ticker_symbol.upper()}"
                            f"&expiration_date.gte={today_str}"
                            f"&sort=expiration_date&order=desc&limit=1"
                            f"&apiKey={polygon_key}"
                        )
                        poly_resp = requests.get(poly_url, timeout=15)
                        poly_results = poly_resp.json().get("results", [])
                        if poly_results:
                            latest_expiry = poly_results[0].get("expiration_date", "N/A")
                except Exception:
                    pass

            # 5. Total insider ownership % (YF primary, AV Backup from second code)
            insider_own_pct = info.get('heldPercentInsiders')
            if insider_own_pct is None:
                try:
                    ov_url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    ov_resp = requests.get(ov_url, proxies=proxies, timeout=15)
                    ov_data = ov_resp.json()
                    insider_own_pct = av_clean(ov_data.get("PercentInsiders")) / 100.0 if ov_data.get("PercentInsiders") else None
                except:
                    pass
            insider_val = f"{insider_own_pct * 100:.2f}%" if insider_own_pct is not None else "N/A"

            # 6. Total Assets & Liabilities (YF primary)
            total_assets, _ = get_latest_metric(q_balance_sheet, ['Total Assets'])
            total_liabilities, _ = get_latest_metric(q_balance_sheet, [
                'Total Liabilities Net Minor Interest', 'Total Liab', 'Total Liabilities'
            ])

            # Fallback for Liabilities (YFinance specific logic from code 1)
            if total_liabilities is None:
                curr_l, _ = get_latest_metric(q_balance_sheet, ['Current Liabilities', 'Total Current Liabilities'])
                non_curr_l, _ = get_latest_metric(q_balance_sheet, [
                    'Total Non Current Liabilities Net Minority Interest', 'Non Current Liabilities'
                ])
                if curr_l is not None or non_curr_l is not None:
                    total_liabilities = (curr_l or 0) + (non_curr_l or 0)

            # --- Alpha Vantage Fallback for Assets/Liabilities (from second code) ---
            if total_assets is None or total_liabilities is None:
                try:
                    av_bs_url = f"https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    av_bs_resp = requests.get(av_bs_url, proxies=proxies, timeout=15)
                    av_bs_data = av_bs_resp.json()
                    reports = av_bs_data.get("quarterlyReports", [])
                    if reports:
                        if total_assets is None:
                            total_assets = av_clean(reports[0].get("totalAssets"))
                        if total_liabilities is None:
                            total_liabilities = av_clean(reports[0].get("totalLiabilities"))
                except:
                    pass

            # 7. Assets / Liabilities Ratio
            if total_assets and total_liabilities and total_liabilities != 0:
                al_ratio = round(total_assets / total_liabilities, 2)

            # 8. Runway (Cash + ST Investments) / TTM Monthly Burn
            # Use combined cash+short-term investments first (more complete picture),
            # then fall back to cash-only.
            current_cash, _ = get_latest_metric(q_balance_sheet, [
                'Cash Cash Equivalents And Short Term Investments',
                'Cash And Cash Equivalents',
            ])

            # TTM OCF: sum of last 4 quarters — more stable than a single quarter
            # which can be distorted by one-time payments or timing differences.
            ttm_ocf = None
            if q_cash_flow is not None and 'Operating Cash Flow' in q_cash_flow.index:
                try:
                    ocf_series = q_cash_flow.loc['Operating Cash Flow'].dropna()
                    if len(ocf_series) >= 4:
                        ttm_ocf = float(ocf_series.iloc[:4].sum())
                    elif len(ocf_series) >= 1:
                        # Annualise from however many quarters we have
                        n = len(ocf_series)
                        ttm_ocf = float(ocf_series.iloc[:n].sum() / n * 4)
                except Exception:
                    ttm_ocf = None

            # --- Alpha Vantage Fallback for Runway ---
            if current_cash is None or ttm_ocf is None:
                try:
                    av_bs_url = f"https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    av_cf_url = f"https://www.alphavantage.co/query?function=CASH_FLOW&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    if current_cash is None:
                        av_bs_resp = requests.get(av_bs_url, proxies=proxies, timeout=15)
                        av_rep = av_bs_resp.json().get("quarterlyReports", [{}])[0]
                        # Prefer combined cash+ST investments
                        av_cash_st = av_clean(av_rep.get("cashAndShortTermInvestments"))
                        av_cash    = av_clean(av_rep.get("cashAndCashEquivalentsAtCarryingValue"))
                        current_cash = av_cash_st if av_cash_st else av_cash
                    if ttm_ocf is None:
                        av_cf_resp = requests.get(av_cf_url, proxies=proxies, timeout=15)
                        # Use last 4 quarterly reports for TTM
                        q_reports = av_cf_resp.json().get("quarterlyReports", [])
                        ocf_vals = [av_clean(r.get("operatingCashflow")) for r in q_reports[:4]]
                        ocf_vals = [v for v in ocf_vals if v is not None]
                        if len(ocf_vals) >= 2:
                            ttm_ocf = sum(ocf_vals) / len(ocf_vals) * 4
                        elif len(ocf_vals) == 1:
                            ttm_ocf = ocf_vals[0] * 4
                except Exception:
                    pass

            if current_cash is not None and ttm_ocf is not None:
                if ttm_ocf < 0:
                    monthly_burn = abs(ttm_ocf) / 12
                    runway_months = current_cash / monthly_burn
                    runway_val = f"{runway_months:.1f} Months"
                else:
                    runway_val = "Positive OCF (No Burn)"

            # OCF Per Share (TTM OCF / shares outstanding)
            ocf_per_share_val = "N/A"
            if ttm_ocf is not None and shares_outstanding and shares_outstanding > 0:
                ocf_per_share_val = round(ttm_ocf / shares_outstanding, 4)

            # --- Pre-fetch AV Income Statement ONCE — reused for EBITDA fallback,
            #     Share Count Tier 2, DOL, Revenue Growth, and Gross Margin ---
            av_income_reports = []
            try:
                av_inc_url = f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                inc_resp = requests.get(av_inc_url, proxies=proxies, timeout=15)
                av_income_reports = inc_resp.json().get("annualReports", [])
            except Exception:
                av_income_reports = []

            # 9. Net Debt / EBITDA
            ebitda, _ = get_latest_metric(a_financials, ['EBITDA', 'Normalized EBITDA'])
            net_debt_raw, _ = get_latest_metric(a_balance_sheet, ['Net Debt'])

            if net_debt_raw is None:
                total_debt, _ = get_latest_metric(a_balance_sheet, ['Total Debt'])
                cash_comp, _ = get_latest_metric(a_balance_sheet, ['Cash And Cash Equivalents'])
                if total_debt is not None and cash_comp is not None:
                    net_debt_raw = total_debt - cash_comp

            # --- AV fallback for EBITDA/Net Debt — reuses pre-fetched av_income_reports ---
            if ebitda is None or net_debt_raw is None:
                logging.info(f"EBITDA/Net Debt missing for {ticker_symbol} in Yahoo. Using Alpha Vantage...")
                try:
                    if ebitda is None and av_income_reports:
                        ebitda = av_clean(av_income_reports[0].get("ebitda"))

                    if net_debt_raw is None:
                        av_bs_url = f"https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                        bs_resp = requests.get(av_bs_url, proxies=proxies, timeout=15)
                        report = bs_resp.json().get("annualReports", [{}])[0]
                        av_cash = av_clean(report.get("cashAndCashEquivalentsAtCarryingValue"))
                        av_st_debt = av_clean(report.get("shortTermDebt"))
                        av_lt_debt = av_clean(report.get("longTermDebt"))
                        net_debt_raw = (av_st_debt + av_lt_debt) - av_cash
                except Exception as av_err:
                    print(f"Alpha Vantage Debt/EBITDA backup failed for {ticker_symbol}: {str(av_err)}", file=sys.stderr)

            if ebitda is not None and ebitda != 0 and net_debt_raw is not None:
                nd_ebitda_val = round(net_debt_raw / ebitda, 2)

            # 10. Cash Burn Severity
            fcf_ttm = None
            if q_cash_flow is not None and 'Free Cash Flow' in q_cash_flow.index:
                fcf_ttm = q_cash_flow.loc['Free Cash Flow'].iloc[:4].sum()

            # --- AV Fallback for FCF ---
            if fcf_ttm is None:
                try:
                    av_cf_url = f"https://www.alphavantage.co/query?function=CASH_FLOW&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    cf_resp = requests.get(av_cf_url, proxies=proxies, timeout=15)
                    q_reports = cf_resp.json().get("quarterlyReports", [])[:4]
                    if q_reports:
                        fcf_ttm = sum([av_clean(r.get("operatingCashflow")) - av_clean(r.get("capitalExpenditures")) for r in q_reports])
                except Exception:
                    pass

            if market_cap and fcf_ttm is not None and fcf_ttm < 0:
                severity_val = f"{(abs(fcf_ttm) / market_cap) * 100:.2f}%"
            elif fcf_ttm is not None and fcf_ttm >= 0:
                severity_val = "0.00% (Positive FCF)"

            # 11. Share Count Growth
            # Tier 1: yfinance full share history (best granularity) — skip if rate-limited
            try:
                if _yf_rate_limited:
                    raise Exception("skipped — YF rate-limited")
                shares_data = ticker.get_shares_full(start=datetime.now() - pd.DateOffset(years=5))
                if shares_data is not None and not shares_data.empty:
                    shares_data = shares_data.sort_index()
                    shares_data = shares_data[~shares_data.index.duplicated(keep='last')]
                    if len(shares_data) > 1:
                        latest_idx = -1
                        target_date = shares_data.index[latest_idx] - pd.DateOffset(years=3)
                        idx_3y = shares_data.index.get_indexer([target_date], method='nearest')[0]
                        if idx_3y != -1 and idx_3y < (len(shares_data) + latest_idx):
                            latest_s = shares_data.iloc[latest_idx]
                            hist_s = shares_data.iloc[idx_3y]
                            years_diff = (shares_data.index[latest_idx] - shares_data.index[idx_3y]).days / 365.25
                            if (pd.notnull(latest_s) and pd.notnull(hist_s) and
                                    hist_s > 0 and latest_s > 0 and years_diff > 0):
                                cagr = ((latest_s / hist_s) ** (1 / years_diff)) - 1
                                share_growth_val = f"{cagr * 100:.2f}%"
            except Exception:
                share_growth_val = "N/A"

            # Tier 2: yfinance info sharesOutstanding vs AV annual balance sheet shares
            if share_growth_val == "N/A":
                try:
                    current_shares = info.get('sharesOutstanding')
                    if current_shares and len(av_income_reports) >= 2:
                        av_bs_url2 = f"https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                        av_bs_resp2 = requests.get(av_bs_url2, proxies=proxies, timeout=15)
                        ann_reports = av_bs_resp2.json().get("annualReports", [])
                        if len(ann_reports) >= 2:
                            old_shares = av_clean(ann_reports[1].get("commonStockSharesOutstanding"))
                            if old_shares and old_shares > 0:
                                annual_growth = ((current_shares - old_shares) / old_shares) * 100
                                share_growth_val = f"{annual_growth:.2f}%"
                except Exception:
                    pass

            # 12. Degree of Operating Leverage (DOL)
            if a_financials is not None and a_financials.shape[1] >= 2 and 'Total Revenue' in a_financials.index:
                sales = a_financials.loc['Total Revenue']
                ebit_v, ebit_k = get_latest_metric(a_financials, ['EBIT', 'Operating Income'])
                if ebit_v is not None:
                    ebit_row = a_financials.loc[ebit_k]
                    pct_sales = (sales.iloc[0] - sales.iloc[1]) / abs(sales.iloc[1]) if sales.iloc[1] != 0 else 0
                    pct_ebit = (ebit_row.iloc[0] - ebit_row.iloc[1]) / abs(ebit_row.iloc[1]) if ebit_row.iloc[1] != 0 else 0
                    if pct_sales != 0:
                        dol_val = round(pct_ebit / pct_sales, 2)

            if dol_val == "N/A" and len(av_income_reports) >= 2:
                try:
                    s1_d, s2_d = av_clean(av_income_reports[0].get("totalRevenue")), av_clean(av_income_reports[1].get("totalRevenue"))
                    e1_d, e2_d = av_clean(av_income_reports[0].get("operatingIncome")), av_clean(av_income_reports[1].get("operatingIncome"))
                    p_sales = (s1_d - s2_d) / abs(s2_d) if s2_d != 0 else 0
                    p_ebit = (e1_d - e2_d) / abs(e2_d) if e2_d != 0 else 0
                    if p_sales != 0:
                        dol_val = round(p_ebit / p_sales, 2)
                except Exception:
                    pass

            # 14. Revenue Growth YoY
            # Tier 1: info dict (already fetched — most reliable for TTM growth)
            try:
                rev_growth_info = info.get('revenueGrowth')  # decimal, e.g. 0.35 = 35%
                if rev_growth_info is not None:
                    revenue_growth_val = f"{rev_growth_info * 100:.2f}%"
            except Exception:
                pass

            # Tier 2: annual financials from yfinance
            if revenue_growth_val == "N/A":
                try:
                    if a_financials is not None and 'Total Revenue' in a_financials.index:
                        rev_row = a_financials.loc['Total Revenue']
                        if len(rev_row) >= 2:
                            rev_current = rev_row.iloc[0]
                            rev_prior = rev_row.iloc[1]
                            if pd.notnull(rev_current) and pd.notnull(rev_prior) and rev_prior != 0:
                                rev_growth = ((rev_current - rev_prior) / abs(rev_prior)) * 100
                                revenue_growth_val = f"{rev_growth:.2f}%"
                except Exception:
                    pass

            # Tier 3: Alpha Vantage (cached)
            if revenue_growth_val == "N/A" and len(av_income_reports) >= 2:
                try:
                    s1_r = av_clean(av_income_reports[0].get("totalRevenue"))
                    s2_r = av_clean(av_income_reports[1].get("totalRevenue"))
                    if s2_r != 0:
                        revenue_growth_val = f"{((s1_r - s2_r) / abs(s2_r)) * 100:.2f}%"
                except Exception:
                    pass

            # 15. Gross Margin %
            # Tier 1: info dict (most reliable, already fetched)
            try:
                gm_info = info.get('grossMargins')  # decimal, e.g. 0.65 = 65%
                if gm_info is not None:
                    gross_margin_val = f"{gm_info * 100:.2f}%"
            except Exception:
                pass

            # Tier 2: annual financials from yfinance
            if gross_margin_val == "N/A":
                try:
                    if a_financials is not None:
                        gross_profit, _ = get_latest_metric(a_financials, ['Gross Profit', 'GrossProfit'])
                        total_rev, _ = get_latest_metric(a_financials, ['Total Revenue'])
                        # Fallback: compute as Revenue - COGS when Gross Profit line is absent
                        if gross_profit is None and total_rev is not None:
                            cogs, _ = get_latest_metric(a_financials, ['Cost Of Revenue', 'Cost Of Goods Sold', 'CostOfRevenue'])
                            if cogs is not None:
                                gross_profit = total_rev - cogs
                        if gross_profit is not None and total_rev is not None and total_rev != 0:
                            gross_margin_val = f"{(gross_profit / total_rev) * 100:.2f}%"
                except Exception:
                    pass

            # Tier 3: Alpha Vantage (cached)
            if gross_margin_val == "N/A" and av_income_reports:
                try:
                    raw_gp = av_income_reports[0].get("grossProfit")
                    raw_tr = av_income_reports[0].get("totalRevenue")
                    raw_cogs = av_income_reports[0].get("costOfRevenue")
                    if raw_tr and str(raw_tr).lower() not in ("none", "0", ""):
                        tr = av_clean(raw_tr)
                        if tr > 0:
                            if raw_gp and str(raw_gp).lower() not in ("none", "0", ""):
                                gp = av_clean(raw_gp)
                                if gp != 0:
                                    gross_margin_val = f"{(gp / tr) * 100:.2f}%"
                            if gross_margin_val == "N/A" and raw_cogs and str(raw_cogs).lower() not in ("none", "0", ""):
                                cogs = av_clean(raw_cogs)
                                gross_margin_val = f"{((tr - cogs) / tr) * 100:.2f}%"
                except Exception:
                    pass

            # Annual revenue (for EBITDA Margin) — reuse what's already loaded
            annual_revenue_raw = None
            if a_financials is not None and 'Total Revenue' in a_financials.index:
                try:
                    annual_revenue_raw = float(a_financials.loc['Total Revenue'].dropna().iloc[0])
                except Exception:
                    pass
            if annual_revenue_raw is None and av_income_reports:
                try:
                    raw_rev = av_income_reports[0].get("totalRevenue")
                    if raw_rev and str(raw_rev).lower() not in ("none", "0", ""):
                        annual_revenue_raw = av_clean(raw_rev)
                except Exception:
                    pass

            # EBITDA Margin
            ebitda_margin_val = "N/A"
            if ebitda is not None and annual_revenue_raw and annual_revenue_raw > 0:
                ebitda_margin_val = f"{(ebitda / annual_revenue_raw) * 100:.2f}%"

            # Operating ROA (operating income / total assets)
            operating_income_raw = None
            if a_financials is not None:
                try:
                    operating_income_raw, _ = get_latest_metric(
                        a_financials, ['EBIT', 'Operating Income']
                    )
                except Exception:
                    pass
            if operating_income_raw is None and av_income_reports:
                try:
                    v = av_clean(av_income_reports[0].get("operatingIncome"))
                    if v != 0.0:
                        operating_income_raw = v
                except Exception:
                    pass

            operating_roa_val = "N/A"
            if operating_income_raw is not None and total_assets and total_assets > 0:
                operating_roa_val = f"{(operating_income_raw / total_assets) * 100:.2f}%"

            # 16. Growth-to-Valuation Score (Revenue Growth YoY / P-S Ratio)
            try:
                ps_ratio = info.get('priceToSalesTrailingTwelveMonths')
                # Fallback: compute P/S from market_cap / annual revenue when not in info
                if (not ps_ratio or ps_ratio <= 0) and market_cap and market_cap > 0:
                    ann_rev = None
                    if a_financials is not None:
                        ann_rev, _ = get_latest_metric(a_financials, ['Total Revenue'])
                    if ann_rev is None and av_income_reports:
                        raw_rev = av_income_reports[0].get("totalRevenue")
                        if raw_rev and str(raw_rev).lower() not in ("none", "0", ""):
                            ann_rev = av_clean(raw_rev)
                    if ann_rev and ann_rev > 0:
                        ps_ratio = market_cap / ann_rev
                if ps_ratio and ps_ratio > 0 and revenue_growth_val != "N/A":
                    rev_growth_pct = float(revenue_growth_val.replace('%', ''))
                    if rev_growth_pct > 0:
                        ratio = rev_growth_pct / ps_ratio
                        growth_to_val_score = f"{ratio:.2f}"
            except Exception:
                pass

            # 13. Capital Structure Pressure (CSP)
            debt_to_equity = info.get('debtToEquity', 0)

            # --- Alpha Vantage Fallback for DebtToEquity (from second code) ---
            if not debt_to_equity:
                try:
                    ov_url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}"
                    ov_resp = requests.get(ov_url, proxies=proxies, timeout=15)
                    debt_to_equity = av_clean(ov_resp.json().get("DebtToEquityRatio")) * 100
                except:
                    pass

            convert_labels = []
            if a_balance_sheet is not None:
                convert_labels = [idx for idx in a_balance_sheet.index if 'convertible' in str(idx).lower()]

            has_converts = len(convert_labels) > 0
            convert_val = a_balance_sheet.loc[convert_labels[0]].iloc[0] if has_converts else 0

            if (debt_to_equity and debt_to_equity > 300):
                csp_status = "Heavy converts / ATM"
            elif has_converts:
                dilution_overhang = (convert_val / market_cap) if market_cap and market_cap > 0 else 0
                if dilution_overhang > 0.05 or (debt_to_equity and debt_to_equity > 150):
                    csp_status = "Heavy converts / ATM"
                else:
                    csp_status = "Minor converts"
            elif debt_to_equity and debt_to_equity > 100:
                csp_status = "Heavy converts / ATM"

            # Formatting results as per final_metrics dictionary in both codes
            final_metrics = {
                "Current stock price": f"{current_price:.2f}" if current_price else "N/A",
                "Market cap": format_large_number(market_cap),
                "Shares Outstanding": format_large_number(shares_outstanding),
                "52 week low": f"{low_52:.2f}" if low_52 else "N/A",
                "52 weeks high": f"{high_52:.2f}" if high_52 else "N/A",
                "latest expiration date": latest_expiry,
                "Total insider ownership %": insider_val,
                "Total Assets": format_large_number(total_assets),
                "Total Liabilities": format_large_number(total_liabilities),
                "Assets / Liabilities Ratio": al_ratio if al_ratio is not None else "N/A",
                "Runway": runway_val,
                "Net Debt": format_large_number(net_debt_raw),
                "EBITDA": format_large_number(ebitda),
                "Net Debt / EBITDA": nd_ebitda_val,
                "Cash Burn Severity": severity_val,
                "Share Count Growth": share_growth_val,
                "Degree of Operating Leverage": dol_val,
                "Capital Structure Pressure": csp_status,
                "Revenue Growth YoY (%)": revenue_growth_val,
                "Gross Margin (%)": gross_margin_val,
                "Growth-to-Valuation Score": growth_to_val_score,
                "OCF Per Share": ocf_per_share_val,
                "52-Week Position (0=low,1=high)": wk52_position_val,
                "EBITDA Margin (%)": ebitda_margin_val,
                "Operating ROA (%)": operating_roa_val
            }

            results["data"] = {"Summary": final_metrics}
            return results

        except Exception as e:
            retry_count += 1
            err_str = str(e).lower()
            is_rate_limit = "too many requests" in err_str or "rate limit" in err_str or "429" in err_str
            logging.error(f"Error on attempt {retry_count} for {ticker_symbol}: {str(e)}")

            if retry_count < max_retries:
                # Rate-limit: long cooldown + random jitter so consecutive retries
                # don't hit the same server-side time window.
                # Other errors: fast exponential backoff.
                if is_rate_limit:
                    wait = 50 * retry_count + random.uniform(5, 15)
                else:
                    wait = 5 * (2 ** (retry_count - 1))
                logging.info(
                    f"{'Rate-limit detected — ' if is_rate_limit else ''}"
                    f"Retrying in {wait:.0f}s (attempt {retry_count + 1}/{max_retries})…"
                )
                time.sleep(wait)
            else:
                results["status"] = "error"
                results["error"] = f"Final failure for {ticker_symbol} after {max_retries} attempts via proxy: {str(e)}"
                return results
