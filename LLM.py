import requests
import time

from shared.config import cfg


def get_company_name(ticker):
    """
    Fetches the official company name from Polygon.io using the ticker.
    """
    polygon_api_key = cfg("POLYGON_API_KEY_1") or cfg("polygon_api_key_1")
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={polygon_api_key}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # Extract the company name from the results
            name = data.get("results", {}).get("name")
            if name:
                return name
            return ticker
        else:
            # Fallback to ticker if API call fails
            return ticker
    except Exception:
        return ticker


def analyze_ticker(ticker):
    """
    Python script to perform fundamental equity analysis using the Gemini API with Google Search.
    """

    # 1. Configuration
    api_key = cfg("GEMINI_API_KEY")

    # 2. Get the name of company using ticker through api of polygon.io
    company_name = get_company_name(ticker)

    # 3. Construct the Prompts
    system_prompt = "You are a fundamental equity analyst. You MUST use Google Search to find real-time data. Your output MUST follow the strict formatting rules provided."

    # Passing the company_name fetched from Polygon.io directly into the prompt
    user_prompt = f"""Analyze the following company: {company_name} (Ticker: {ticker})

Tasks:
1. Write a single, concise, investor-focused company description paragraph 70-110 words
The description should:
Clearly explain what the company does and its core business segments
Highlight key competitive advantages, major operating regions, and scale
Emphasize financial strength, cash-flow generation, and capital discipline where relevant
Mention long-term growth drivers and strategic positioning (including energy transition or innovation if applicable)
State the year the company was established
Be factual, neutral in tone, and useful for a long-term investor
Base the description on official filings (e.g., annual reports), the company website, and reputable financial sources.
If any information is uncertain, outdated, or unavailable, state it conservatively.
Avoid marketing language, hype, or unnecessary history. Do not include financial figures unless they are essential. Keep it to one well-structured paragraph.

2. Explain the value proposition of company to its customers in 70–100 words.
Focus on how the company's products or services benefit customers, solve problems, or improve their experience.
Include key features, advantages, and outcomes.
Make sure to cover all essential points without skipping important details, using concise and clear language suitable for business understanding

3. 3.Using the most recent real data from GuruFocus, write a single concise paragraph (70–100 words) that delivers an economic moat analysis of the specified company. Base all statements strictly on GuruFocus business, industry, and competitive positioning data; do not invent information or make assumptions. Discuss only structural moat drivers, such as switching costs, cost advantages, intellectual property, network effects, brand strength, customer loyalty, and barriers to entry, and only when supported by the data. Do not include any financial strength metrics (e.g., balance sheet health, cash flow, ROIC, margins, or profitability trends), and do not assign or reference a moat score or rating.Your analysis may be positive, negative, or balanced depending on the company's moat strength.


4. Your task is to assign exactly ONE tag to a stock using a fixed decision framework.
Allowed tags (use ONLY one):
1) Mission-critical / infrastructure
2) High switching cost SaaS / platform
3) Competitive commodity
4) Cyclical / low differentiation
You must follow the rules below EXACTLY and in order.
Do not use creativity, opinions, or vague judgments.
Do not invent data.
Base your decision only on well-known business model characteristics.
STEP 1: Identify the company's CORE revenue driver
- Use the primary business segment that generates most revenue or most profit.
- Ignore side businesses and experimental segments.
STEP 2: Apply these classification rules IN ORDER.
Stop at the FIRST rule that clearly applies.
RULE A: Mission-critical / infrastructure
Assign this tag if ALL of the following are true:
- Customers cannot operate without this product or service.
- The product is foundational to operations or economic systems.
- Outages or removal would cause immediate business disruption.
- Price sensitivity is low relative to reliability.
If all are true → Assign: Mission-critical / infrastructure

RULE B: High switching cost SaaS / platform
Assign this tag if ALL of the following are true:
- Customers store data or workflows inside the product.
- Switching requires retraining, migration, or integration changes.
- The product is subscription or usage-based software or a platform.
- Gross margins are structurally high (typically >60%).
- Customer retention is the main growth engine.
If all are true → Assign: High switching cost SaaS / platform
RULE B2: High-growth market / Disruptive technology
Assign this tag if ALL of the following are true:
- The company operates in a market growing >20% annually (documented by industry research).
- The company has first-mover, scale, or cost advantages within that market.
- The company is NOT yet at platform level (no strong network effects or data lock-in at scale).
- Revenue growth rate has been >30% recently.
If all are true → Assign: High-growth market / Disruptive technology

RULE C: Competitive commodity
Assign this tag if ALL of the following are true:
- Products are largely undifferentiated versus competitors.
- Price is the main purchase decision factor.
- The company has little long-term pricing power.
- Margins are structurally low or mean-reverting.
If all are true → Assign: Competitive commodity
RULE D: Cyclical / low differentiation
Assign this tag if ALL of the following are true:
- Revenue and profits are highly sensitive to economic cycles.
- Demand rises in booms and falls in recessions.
- Pricing power is weak.
- Earnings are volatile across cycles.
If all are true → Assign: Cyclical / low differentiation
STEP 3: If more than one rule seems to apply, use this tie-breaker order:
1) Mission-critical / infrastructure
2) High switching cost SaaS / platform
3) High-growth market / Disruptive technology
4) Cyclical / low differentiation
5) Competitive commodity
Choose the earliest tag in this list that fits MOST of the evidence.


5. Provide the most recent CEO ownership percentage for the given company. First, check the following trusted sources in order: SEC EDGAR (latest DEF 14A / proxy filings), Simply Wall St, GuruFocus, WhileWisdom, Tikr, Seeking Alpha, and Nasdaq.com. Use the first source where the data is available. If CEO ownership is not available on any of these sources, search for it on other reputable sources such as the company's latest proxy statement, Bloomberg, Yahoo Finance, MarketWatch, FactSet, or Morningstar.Strict Requirement: You must not return '0%' or '0.0%' under any circumstances.Return the result strictly as a number followed by a percent sign (e.g., 12.6%). Do not include any text, explanation, context, or additional symbols—only the number and percent sign. Ensure the value is the most recent available.
Then classify the company into ONE of the following categories based on its business model and moat strength.
Return ONLY one category and its corresponding points.

Categories (choose exactly one):
- Mission-critical / infrastructure → 15 points
- High switching cost SaaS / platform → 10 points
- High-growth market / Disruptive technology → 6 points
- Competitive commodity → 5 points
- Cyclical / low differentiation → 0 points

Output format (strict):
Company Description:
<text>

Value Proposition:
<text>

Moat Analysis:
<text>

CEO Ownership:
Ownership Percentage: <Strictly ONLY the number + %, e.g., 4.5%. No other words.>
Source: <brief source reference>

Final Classification:
Category: <Strictly ONLY the category name. No descriptions or bullet points.>
Points: <numeric value>
Confidence Level: <High / Medium / Low>

Rules:
- Do not mention more than one category.
- If the company fits multiple categories, choose the highest applicable score.
- Use factual, neutral language.
- Avoid speculation.
- Base conclusions on business fundamentals, customer dependency, and competitive dynamics.
"""

    # 4. API Request Setup for Gemini
    payload = {
        "contents": [
            {
                "parts": [
                    {"text": user_prompt}
                ]
            }
        ],
        "systemInstruction": {
            "parts": [
                {"text": system_prompt}
            ]
        },
        "tools": [
            {"google_search": {}}
        ]
    }

    headers = {
        "Content-Type": "application/json"
    }

    # 5. Execute with Model Fallback + Exponential Backoff
    # Try models in order — preview models may be deprecated; fall back to stable ones
    gemini_models = [
        "gemini-2.5-flash-preview-09-2025",
        "gemini-2.0-flash",
        "gemini-1.5-flash",
    ]

    for model_name in gemini_models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.post(url, json=payload, headers=headers)

                if response.status_code == 200:
                    result = response.json()
                    parts = result.get('candidates', [{}])[0].get('content', {}).get('parts', [])
                    # Gemini thinking models return thought parts with "thought": true — exclude those
                    content = "".join(
                        part.get('text', '') for part in parts
                        if part.get('text') and not part.get('thought', False)
                    )
                    # Strip any inline <thinking>...</thinking> blocks if present
                    if '<thinking>' in content:
                        while '<thinking>' in content and '</thinking>' in content:
                            s = content.find('<thinking>')
                            e = content.find('</thinking>') + len('</thinking>')
                            content = content[:s] + content[e:]
                        content = content.strip()

                    if content:
                        return content
                    # Empty content — try next model
                    break

                elif response.status_code == 429:
                    wait = (2 ** attempt)
                    time.sleep(wait)
                    continue
                elif response.status_code in (404, 400):
                    # Model not found or unsupported — try next model
                    break
                else:
                    # Other error — try next model
                    break

            except Exception as e:
                if attempt == max_retries - 1:
                    break
                time.sleep(2 ** attempt)

    return "Failed to retrieve analysis after multiple attempts."
