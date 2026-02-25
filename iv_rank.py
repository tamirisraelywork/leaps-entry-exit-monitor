import sys
import re
import time
import requests
from playwright.sync_api import sync_playwright
import streamlit as st



def get_iv_rank_advanced(ticker):
    ticker = ticker.upper().strip()
    unusual_whales_url = f"https://unusualwhales.com/stock/{ticker}/volatility"

    # --- Configuration ---
    proxy_config = dict(st.secrets["proxy_config"])
    apiKey = st.secrets["GEMINI_API_KEY"]

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    timeout_ms = 20000

    # ---------------------------------------------------------
    # ATTEMPT 1 : Unusual Whales (1 Try)
    # ---------------------------------------------------------
    for try_num in range(1, 2):
        sys.stderr.write(f"INFO: Attempt {try_num} - Unusual Whales via dataimpulse")
        with sync_playwright() as p:
            try:
                # Launch browser
                browser = p.chromium.launch(
                    headless=True,
                    proxy=proxy_config,
                    args=[
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--ignore-certificate-errors"
                    ]
                )

                context = browser.new_context(
                    user_agent=user_agent,
                    ignore_https_errors=True
                )
                page = context.new_page()

                # Step 1: Verify Proxy IP
                sys.stderr.write(f"DEBUG: Try {try_num} - Verifying Proxy Connection... ")
                try:
                    page.goto("https://httpbin.org/ip", timeout=30000)
                    sys.stderr.write("SUCCESS!\n")
                except Exception as e:
                    sys.stderr.write(f"FAILED (Proxy Handshake). Error: {str(e)}\n")
                    browser.close()
                    continue  # Try the next attempt

                # Step 2: Navigate and Extract
                sys.stderr.write(f"INFO: Navigating to Unusual Whales for {ticker}...\n")
                page.goto(unusual_whales_url, wait_until="load", timeout=timeout_ms)

                iv_rank = None
                # Polling for dynamic JS content
                for _ in range(20):
                    content = page.content()
                    # Regex logic for "IV Rank"
                    match = re.search(r"IV Rank\s*[:]?\s*([\d\.]+)%?", content, re.IGNORECASE)
                    if match:
                        iv_rank = match.group(1)
                        break


                    try:
                        iv_locator = page.get_by_text("IV Rank", exact=False).first
                        if iv_locator.is_visible():
                            parent_text = iv_locator.evaluate("el => el.closest('div').innerText")
                            val_match = re.search(r"(\d+\.\d+|\d+)", parent_text)
                            if val_match:
                                iv_rank = val_match.group(1)
                                break
                    except:
                        pass
                    page.wait_for_timeout(2000)

                if iv_rank:
                    browser.close()
                    return f"Success! The IV Rank for {ticker} is: {iv_rank}"

                browser.close()
            except Exception as e:
                sys.stderr.write(f"ERROR: Attempt {try_num} Failed: {str(e)}\n")

    #
    sys.stderr.write(f"INFO: Final Fallback - Gemini Search for optionscharts.io data\n")

    def call_gemini_with_search(query):
        prompt_text = (
            f"Find the Implied Volatility (IV) Rank for the stock ticker {ticker}.\n\n"
            f"Perform a Google Search for: \"{ticker} stock IV rank optionscharts.io barchart.com\"\n\n"
            "Prioritize data from optionscharts.io or barchart.com. "
            "Look for specific phrasing like \"IV Rank of X%\", \"IV Rank: X\", or \"Rank: X%\".\n\n"
            "CRITICAL INSTRUCTIONS:\n"
            "1. Extract the specific numeric IV Rank value.\n"
            "2. Output ONLY the number (e.g. 17.69, 23.5, 56).\n"
            "3. Do not include \"%\" or words.\n\n"
            "If the text says \"IV Rank of 17.69%\", return: 17.69"
        )

        payload = {
            "contents": [{
                "parts": [{"text": prompt_text}]
            }],
            "tools": [{"google_search": {}}],
            "systemInstruction": {
                "parts": [{
                    "text": (
                        "You are a financial analyst. Your goal is to find the current 'IV Rank' for the given stock ticker. You must use the Google Search tool to query optionscharts.io specifically."
                        "Extract the value ONLY from 'optionscharts.io'."
                        "Return ONLY the numerical value (e.g., 45.2)"
                    )
                }]
            }
        }

        gemini_models = [
            "gemini-2.5-flash-preview-09-2025",
            "gemini-2.0-flash",
            "gemini-1.5-flash",
        ]

        for model_name in gemini_models:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={apiKey}"
            for delay in [1, 2, 4, 8]:
                try:
                    response = requests.post(url, json=payload, timeout=30)
                    if response.status_code == 200:
                        result = response.json()
                        parts = result.get('candidates', [{}])[0].get('content', {}).get('parts', [])
                        text = "".join(
                            p.get('text', '') for p in parts
                            if p.get('text') and not p.get('thought', False)
                        )
                        return text
                    elif response.status_code == 429:  # Rate limit
                        time.sleep(delay)
                    elif response.status_code in (404, 400):
                        # Model unavailable — try next
                        break
                    else:
                        break  # Other error codes — try next model
                except Exception:
                    time.sleep(delay)
        return None


    search_query = f"What is the current IV Rank for ticker {ticker}? Check optionscharts.io specifically."
    gemini_res = call_gemini_with_search(search_query)

    if gemini_res and "NOT_FOUND" not in gemini_res.upper():
        # Extract the numerical value from the Gemini response
        val_match = re.search(r"(\d+\.\d+|\d+)", gemini_res)
        if val_match:
            return f"Success! The IV Rank for {ticker} is: {val_match.group(1)}"

    return f"Could not find IV Rank for {ticker} after all attempts (Unusual Whales and Gemini Search)."


if __name__ == "__main__":

    target = sys.argv[1] if len(sys.argv) > 1 else "meta"
    print(get_iv_rank_advanced(target))