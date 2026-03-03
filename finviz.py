
import requests
import time
import random
from bs4 import BeautifulSoup
import logging
import streamlit as st


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def interpret_insider_activity(value):
    """
    Convert Insider Trans % into Buying / Selling signal.
    """
    if value == "N/A":
        return "N/A"

    try:

        percent = float(value.replace("%", ""))
        if percent > 0:
            return "Net Insider Buying"
        elif percent < 0:
            return "Net Insider Selling"
        else:
            return "Neutral"
    except (ValueError, AttributeError):
        return "N/A"


def scrape_finviz(ticker):
    """
    Scrapes stock data from Finviz and returns the results as a dictionary.
    """
    ticker = ticker.upper()
    url = f"https://finviz.com/quote.ashx?t={ticker}"


    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    # Build proxy URL from PROXY_USER/PROXY_PASS (or use proxy_url directly if set)
    proxy_url = (
        st.secrets.get("proxy_url")
        or f"http://{st.secrets['PROXY_USER']}:{st.secrets['PROXY_PASS']}@gw.dataimpulse.com:823"
    )

    proxies = {
        "http": proxy_url,
        "https": proxy_url
    }

    response = None
    last_error = ""


    for attempt in range(1, 3):
        try:
            logging.info(f"Attempt {attempt}: Fetching {ticker} via Proxy http://gw.dataimpulse.com:823..")

            response = requests.get(url, headers=headers, proxies=proxies, timeout=15)

            if response.status_code == 200:
                logging.info(f"Successfully fetched {ticker} on attempt {attempt}")
                break  # Success, exit retry loop
            else:
                last_error = f"Status Code: {response.status_code}"
                logging.warning(f"Attempt {attempt} failed for {ticker}: {last_error}")
                if attempt < 2:
                    time.sleep(random.uniform(3, 8))

        except Exception as e:
            last_error = str(e)
            logging.error(f"Attempt {attempt} connection error for {ticker}: {last_error}")
            if attempt < 2:
                time.sleep(random.uniform(3, 8))

    # Final check after 2 retries
    if response is None or response.status_code != 200:
        logging.critical(f"All retries failed for {ticker}. Final Error: {last_error}")
        return {"error": f"Failed to fetch Finviz page after 2 tries. Last error: {last_error}"}


    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", class_="snapshot-table2")

    if not table:
        return {"error": "Finviz data table not found. Invalid ticker or layout change."}


    finviz_data = {}
    for row in table.find_all("tr"):
        cols = row.find_all("td")
        for i in range(0, len(cols), 2):
            key = cols[i].text.strip()
            value = cols[i + 1].text.strip()
            finviz_data[key] = value

    # Extract specific metrics
    insider_trans = finviz_data.get("Insider Trans", "N/A")
    insider_activity = interpret_insider_activity(insider_trans)
    company_name = finviz_data.get("Company", "N/A").split("\n")[0]

    extracted_data = {
        "Ticker": ticker,
        "Company": company_name,
        "Net Insider Buying vs Selling (%)": insider_trans,
        "Net Insider Activity": insider_activity,
        "Institutional Ownership (%)": finviz_data.get("Inst Own", "N/A"),
        "Short Float (%)": finviz_data.get("Short Float", "N/A")
    }

    return extracted_data
