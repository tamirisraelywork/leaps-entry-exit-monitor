import requests

from shared.config import cfg


def get_forward_eps_growth(symbol, api_key):
    """
    Fetches the Forward EPS Growth for a given ticker
    using the Alpha Vantage Fundamental Data (OVERVIEW) endpoint.
    Returns only the numerical growth percentage value or None if an error occurs.
    """

    url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={api_key}'

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "Note" not in data and data and "Symbol" in data:
            growth_raw = data.get("QuarterlyEarningsGrowthYOY", "0")
            try:
                growth_pct = float(growth_raw) * 100
                return growth_pct
            except (ValueError, TypeError):
                pass  # Proceed to secondary if parsing fails
    except Exception:
        pass  # Proceed to secondary on request failure

    # Build proxy from PROXY_USER/PROXY_PASS (or use pre-built proxies dict if set)
    proxy_url = f"http://{cfg('PROXY_USER')}:{cfg('PROXY_PASS')}@gw.dataimpulse.com:823"
    proxies = {"http": proxy_url, "https": proxy_url}

    try:
        response = requests.get(url, proxies=proxies, timeout=15)
        response.raise_for_status()
        data = response.json()

        if "Note" in data:
            return None

        if not data or "Symbol" not in data:
            return None

        growth_raw = data.get("QuarterlyEarningsGrowthYOY", "0")

        try:
            growth_pct = float(growth_raw) * 100
            return growth_pct
        except (ValueError, TypeError):
            return None

    except requests.exceptions.RequestException:
        return None


if __name__ == "__main__":
    MY_API_KEY = cfg("ALPHA_VANTAGE_API_KEY_1")
    MY_API_KEY2 = cfg("ALPHA_VANTAGE_API_KEY_2")
    ticker = input("Enter Stock Ticker (e.g., NVDA, AAPL): ").strip().upper()

    if ticker:

        result = get_forward_eps_growth(ticker, MY_API_KEY)

        if result is None:
            if MY_API_KEY2 and MY_API_KEY2.strip():
                result = get_forward_eps_growth(ticker, MY_API_KEY2)

        print(result)
