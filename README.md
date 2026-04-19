# Blue Ocean Platform — Unified Commerce Cockpit

Fully automated platform that runs two parallel workflows from a single Streamlit cockpit:

- **Clone workflow** (Meta / Merivalo-style): scrape a competitor page, translate, generate a Shopify listing, import reviews.
- **Research workflow** (Google / Movanella-style): keyword research → AliExpress sourcing → AI content generation → Shopify listing → Google PMax testing → automated scale/kill decisions.

Part A (this repo) is the Python/Streamlit platform. Part B is a Node page-cloner that runs as a sibling service and is called over HTTP from the clone workflow.

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp .env.example .env
# Edit .env with your API keys (see "Required Credentials" below)
```

### 3. Create Google Sheet

Create a new Google Sheet and add the Sheet ID to your `.env` file. The system will automatically create all required tabs (Keywords, Products, Config, Action Log, Notifications, Feedback) on first run.

### 4. Create Google Ads PMax campaigns

Create two Performance Max campaigns in your Google Ads account:
- **"Blue Ocean - Testing"** — Fixed daily budget for testing new products
- **"Blue Ocean - Winners"** — Higher budget for proven products

Set up listing groups to filter by `custom_label_0`.

### 5. Run the system

```bash
# Full system (scheduler + dashboard)
python run.py

# Dashboard only
streamlit run dashboard/app.py

# Scheduler only (no UI)
python run.py --scheduler-only
```

Open http://localhost:8501 for the dashboard.

## Architecture

```
Research Pipeline → Google Sheet → Content Generation → Shopify → Google Ads
      ↑                                                              ↓
      ↑                                                     Performance Data
      ↑                                                              ↓
      └─── Feedback Loop ←──── Decision Engine ←───────────── Sheet Update
```

### How it works

1. **AI Research** — LLM generates product-intent keywords, validated by Google Keyword Planner
2. **Competition Analysis** — SerpAPI checks Google Shopping for competitors and pricing
3. **Product Matching** — AliExpress API finds matching products with pricing
4. **Agent Sourcing** — System writes product URLs to Sheet; your China agent fills in landed cost
5. **Economic Validation** — Automatically calculates margins, break-even ROAS, max CPC
6. **Content Generation** — AI generates unique product images and SEO-optimized content
7. **Shopify Listing** — Auto-creates basic product pages for testing
8. **Google Ads** — Products enter PMax Testing campaign via label sync
9. **Performance Monitoring** — Pulls per-product metrics every 2 hours
10. **Decision Engine** — Kills bad products, promotes winners, scales budgets

## Dashboard Pages

- **Overview** — Pipeline metrics, recent actions, quick triggers
- **Settings** — All configurable parameters (research, economics, kill/scale rules, etc.)
- **Research** — View AI research results, add manual keywords, trigger pipeline
- **Products** — Full product pipeline with status tracking and manual actions
- **Performance** — Portfolio and per-product performance metrics
- **Logs** — Audit trail, notifications, scheduler status

## Required Credentials

| Service | Credential | How to get it |
|---------|-----------|---------------|
| OpenAI | API Key | https://platform.openai.com/api-keys |
| Google Ads | Developer Token + OAuth2 | https://developers.google.com/google-ads/api/docs/get-started |
| Google Sheets | Service Account JSON | https://console.cloud.google.com → APIs → Credentials |
| Google Merchant Center | Merchant ID | https://merchants.google.com |
| Shopify | Admin API Access Token | Shopify Admin → Settings → Apps → Develop apps |
| AliExpress | Drop Shipping App Key + Secret | https://openservice.aliexpress.com (Portals tracking ID via https://portals.aliexpress.com) |
| SerpAPI | API Key | https://serpapi.com |
| Gemini | API Key | https://aistudio.google.com (bind to your GCP project for billing) |
| Fal.ai | Key ID:Secret | https://fal.ai/dashboard/keys |

## Project Structure

```
blue-ocean-platform/
├── run.py                         # Main entry point
├── requirements.txt
├── .env.example
├── config/defaults.yaml           # Default settings
├── src/
│   ├── core/                      # Models, interfaces, config
│   ├── research/                  # Keyword research pipeline
│   ├── economics/                 # Economic validation
│   ├── content/                   # AI image + content generation
│   ├── shopify/                   # Shopify listing management
│   ├── sheets/                    # Google Sheets integration
│   ├── ads/                       # Google Ads campaign management
│   ├── decisions/                 # Kill/maintain/scale engine
│   ├── monitoring/                # Competitor price + stock checks
│   └── scheduler/                 # Job scheduling
├── dashboard/                     # Streamlit UI
│   ├── app.py
│   ├── components/
│   └── pages/
└── tests/
```

## Running Tests

```bash
pytest tests/ -v
```

## Verifying the stack

After any credential rotation, billing change, or migration, run the unified
smoke test to confirm every external API is live and bound to the right entity:

```bash
python scripts/smoke_test.py           # all checks
python scripts/smoke_test.py -v        # + per-check details
python scripts/smoke_test.py --only google_ads aliexpress
```

Exits 0 only if every check passes.

## License

Proprietary — Blue Ocean Commerce B.V.
