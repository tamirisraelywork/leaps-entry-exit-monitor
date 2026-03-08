import re
import time
import sys
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

from shared.config import cfg, cfg_dict

PROXY_HOST = "gw.dataimpulse.com"
PROXY_PORT = "823"
PROXY_USER = cfg("PROXY_USER")
PROXY_PASS = cfg("PROXY_PASS")
GEMINI_API_KEY = cfg("GEMINI_API_KEY")


def get_moat_score(ticker: str):
    ticker = ticker.upper().strip()
    result = "N/A"

    # --- TIER 1: BIGQUERY LOOKUP ---
    TABLE_ID = cfg("TABLE_ID") or cfg("MOAT_TABLE_ID")
    if TABLE_ID:
        sys.stderr.write(f"INFO: TIER 1 - Initializing bigquery connection for {ticker}\n")
        try:
            service_info = cfg_dict("SERVICE_ACCOUNT_JSON")
            if not service_info:
                sys.stderr.write("ERROR: 'SERVICE_ACCOUNT_JSON' not found in config\n")
            else:

                if "private_key" in service_info:
                    service_info["private_key"] = service_info["private_key"].replace("\\n", "\n")
                else:
                    sys.stderr.write("ERROR: 'private_key' missing from service account info\n")

                try:
                    credentials = service_account.Credentials.from_service_account_info(service_info)
                    client = bigquery.Client(credentials=credentials, project=service_info.get("project_id"))
                except Exception as e:
                    sys.stderr.write(f"ERROR: BigQuery Authentication failed: {e}\n")
                    client = None

                if client:
                    query = f"""
                            SELECT moat_number
                            FROM `{TABLE_ID}`
                            WHERE UPPER(ticker) = @ticker
                        """
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("ticker", "STRING", ticker)
                        ]
                    )

                    query_job = client.query(query, job_config=job_config)
                    results = query_job.result()

                    for row in results:
                        if row.moat_number is not None:
                            result = str(row.moat_number)
                            print(f"[SUCCESS] Moat Score found in BigQuery for {ticker}: {result}")
                            return result

        except Exception as e:
            print(f"[ERROR] BigQuery access failed: {e}")

    print(f"[INFO] Ticker {ticker} not found in database. Escalating to Tier 3 (Gemini LLM)...")

    # --- TIER 3: GEMINI LLM WITH SEARCH ---
    # (Tier 2 Playwright scraping is skipped — not reliable in cloud environments)
    print(f"[INFO] TIER 3: Initiating Gemini Search-Grounding for {ticker}...")
    system_prompt = (
        "You are a financial data extraction agent. You must search GuruFocus to find the 'Moat Score'. "
        "Strictly retrieve the score from GuruFocus. Return ONLY the integer value. "
        "If multiple values are found, return the most recent summary score. If not found, return 'N/A'."
    )
    user_query = f"What is the current GuruFocus Moat Score for the ticker {ticker}?"

    payload = {
        "contents": [{"parts": [{"text": user_query}]}],
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "tools": [{"google_search": {}}]
    }

    gemini_models = [
        "gemini-2.5-flash-preview-09-2025",
        "gemini-2.0-flash",
        "gemini-1.5-flash",
    ]

    for model_name in gemini_models:
        api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
        for i in range(3):
            try:
                resp = requests.post(api_url, json=payload, timeout=30)
                if resp.status_code == 200:
                    resp_json = resp.json()
                    parts = resp_json.get('candidates', [{}])[0].get('content', {}).get('parts', [])
                    text = "".join(
                        p.get('text', '') for p in parts
                        if p.get('text') and not p.get('thought', False)
                    ) or 'N/A'

                    # Extract digits from LLM response
                    match = re.search(r"(\d+)", text)
                    if match:
                        result = match.group(1)
                        print(f"[SUCCESS] Moat score via {model_name} for {ticker}: {result}")
                        return result
                    break
                elif resp.status_code == 429:
                    print(f"[WARN] Gemini API Rate Limited. Backing off...")
                    time.sleep(2 ** i)
                elif resp.status_code in (404, 400):
                    print(f"[WARN] Model {model_name} unavailable ({resp.status_code}). Trying next model.")
                    break
                else:
                    print(f"[ERROR] Gemini API returned status {resp.status_code}")
                    break
            except Exception as api_err:
                print(f"[ERROR] API request error: {api_err}")
                time.sleep(2 ** i)

    print(f"[FINAL] All methods exhausted for {ticker}. Returning N/A.")
    return "N/A"
