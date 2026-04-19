# Blue Ocean Platform — Google Ads API Design Document

## Overview

Blue Ocean Platform is an internal product research and Google Ads management tool built by Blue Ocean Commerce B.V. It uses the Google Ads API to automate product research and campaign performance monitoring for our own Google Shopping / Performance Max campaigns.

## API Usage

### 1. Keyword Planner (KeywordPlanIdeaService)
- **Purpose:** Retrieve search volume, CPC estimates, and competition data for product-intent keywords
- **Method:** `GenerateKeywordIdeas`
- **Frequency:** ~1-2 times per day, batches of 20 keywords per request
- **Data used:** `avg_monthly_searches`, `competition_index`, `low_top_of_page_bid_micros`, `high_top_of_page_bid_micros`

### 2. Campaign Performance (GoogleAdsService)
- **Purpose:** Pull performance metrics for our Performance Max campaigns
- **Method:** `Search` (GAQL queries)
- **Frequency:** Every 2 hours
- **Metrics retrieved:** clicks, impressions, cost, conversions, conversion_value
- **Segmentation:** By campaign name and date

### 3. Campaign Budget Management (CampaignBudgetService)
- **Purpose:** Adjust daily budgets for winning campaigns based on ROAS performance
- **Method:** `MutateCampaignBudgets`
- **Frequency:** At most once every 3 days per campaign
- **Logic:** Increase budget by 20% when ROAS exceeds target for 2+ consecutive days

## Architecture

- **Language:** Python 3.9+
- **Google Ads Client Library:** google-ads-python v23
- **Authentication:** OAuth2 (Desktop app flow)
- **Data Storage:** Google Sheets (single source of truth)
- **Dashboard:** Streamlit (internal use only)

## Account Structure

- Google Ads Account: 188-465-2074 (Blue Ocean Commerce B.V., login `info@movanella.com`)
- Campaign types: Performance Max (Google Shopping)

## Rate Limits & Compliance

- All API calls respect Google Ads API rate limits
- No automated campaign creation (campaigns are created manually)
- No bulk account management (single advertiser account only)
- No third-party access (internal tool for own accounts)
- Developer token used solely by Blue Ocean Commerce B.V.

## Contact

- Company: Blue Ocean Commerce B.V.
- Website: https://movanella.com
- Email: info@rovelloshop.com
