"""
Google Ads Keyword Planner API integration.
Validates keywords with real search volume, CPC, and competition data.
"""

from __future__ import annotations

import logging
from typing import Optional

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from src.core.config import (
    AppConfig,
    GOOGLE_ADS_DEVELOPER_TOKEN,
    GOOGLE_ADS_CLIENT_ID,
    GOOGLE_ADS_CLIENT_SECRET,
    GOOGLE_ADS_REFRESH_TOKEN,
    GOOGLE_ADS_CUSTOMER_ID,
    GOOGLE_ADS_LOGIN_CUSTOMER_ID,
)

logger = logging.getLogger(__name__)

# Map competition index ranges to labels
COMPETITION_LABELS = {
    (0, 33): "low",
    (34, 66): "medium",
    (67, 100): "high",
}


def _get_google_ads_client() -> GoogleAdsClient:
    """Create and return a Google Ads API client."""
    config = {
        "developer_token": GOOGLE_ADS_DEVELOPER_TOKEN,
        "client_id": GOOGLE_ADS_CLIENT_ID,
        "client_secret": GOOGLE_ADS_CLIENT_SECRET,
        "refresh_token": GOOGLE_ADS_REFRESH_TOKEN,
        "use_proto_plus": True,
    }
    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        config["login_customer_id"] = GOOGLE_ADS_LOGIN_CUSTOMER_ID

    return GoogleAdsClient.load_from_dict(config)


def _competition_label(index: int) -> str:
    """Convert competition index (0-100) to label."""
    for (low, high), label in COMPETITION_LABELS.items():
        if low <= index <= high:
            return label
    return "unknown"


def _get_location_id(country_code: str) -> str:
    """Map country codes to Google Ads geo target constants."""
    country_map = {
        "DE": "2276",   # Germany
        "NL": "2528",   # Netherlands
        "AT": "2040",   # Austria
        "FR": "2250",   # France
        "BE": "2056",   # Belgium
        "CH": "2756",   # Switzerland
        "ES": "2724",   # Spain
        "IT": "2380",   # Italy
        "PL": "2616",   # Poland
        "GB": "2826",   # United Kingdom
        "US": "2840",   # United States
    }
    return country_map.get(country_code, "2276")


def _get_language_id(language: str) -> str:
    """Map language codes to Google Ads language constants."""
    lang_map = {
        "de": "1001",  # German
        "nl": "1010",  # Dutch
        "fr": "1002",  # French
        "es": "1003",  # Spanish
        "it": "1004",  # Italian
        "pl": "1030",  # Polish
        "en": "1000",  # English
    }
    return lang_map.get(language, "1001")


def validate_keywords(
    keywords: list[str],
    country: str = "DE",
    language: str = "de",
    config: AppConfig = None,
) -> list[dict]:
    """
    Validate keywords via Google Ads Keyword Planner API.

    Args:
        keywords: List of keyword strings to validate
        country: Target country code
        language: Target language code
        config: App configuration

    Returns:
        List of dicts with:
            keyword, monthly_search_volume, estimated_cpc,
            competition_level, competition_index
    """
    config = config or AppConfig()

    if not keywords:
        return []

    # Check if Google Ads credentials are configured
    if (
        not GOOGLE_ADS_DEVELOPER_TOKEN
        or GOOGLE_ADS_DEVELOPER_TOKEN.startswith("your_")
        or not GOOGLE_ADS_CUSTOMER_ID
        or GOOGLE_ADS_CUSTOMER_ID.startswith("your_")
    ):
        logger.warning(
            "Google Ads API credentials not configured — skipping Keyword Planner validation. "
            "Keywords will pass through without volume/CPC data."
        )
        # Return keywords as-is with zero metrics so they can still proceed
        return [{"keyword": kw, "monthly_search_volume": 0, "estimated_cpc": 0, "competition_level": "unknown"} for kw in keywords]

    try:
        client = _get_google_ads_client()
        customer_id = GOOGLE_ADS_CUSTOMER_ID.replace("-", "")

        keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")

        # Build the request
        request = client.get_type("GenerateKeywordIdeasRequest")
        request.customer_id = customer_id

        # Set location (country)
        location_id = _get_location_id(country)
        request.geo_target_constants.append(
            f"geoTargetConstants/{location_id}"
        )

        # Set language
        language_id = _get_language_id(language)
        request.language = f"languageConstants/{language_id}"

        # Set the keyword seed
        # Process in batches of 20 (API limit)
        all_results = []
        batch_size = 20

        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]

            request.keyword_seed.keywords.clear()
            for kw in batch:
                request.keyword_seed.keywords.append(kw)

            # Execute the request
            response = keyword_plan_idea_service.generate_keyword_ideas(
                request=request
            )

            for idea in response.results:
                metrics = idea.keyword_idea_metrics

                # Get search volume (monthly average)
                avg_monthly_searches = metrics.avg_monthly_searches or 0

                # Get competition index (0-100)
                competition_index = metrics.competition_index or 0
                competition = _competition_label(competition_index)

                # Get estimated CPC (low and high range)
                low_cpc = (metrics.low_top_of_page_bid_micros or 0) / 1_000_000
                high_cpc = (metrics.high_top_of_page_bid_micros or 0) / 1_000_000
                avg_cpc = (low_cpc + high_cpc) / 2 if high_cpc > 0 else low_cpc

                result = {
                    "keyword": idea.text,
                    "monthly_search_volume": avg_monthly_searches,
                    "estimated_cpc": round(avg_cpc, 2),
                    "competition_level": competition,
                    "competition_index": competition_index,
                    "low_cpc": round(low_cpc, 2),
                    "high_cpc": round(high_cpc, 2),
                }
                all_results.append(result)

            logger.info(
                "Validated batch %d-%d: %d results",
                i, i + len(batch), len(all_results)
            )

        # Match results back to input keywords
        # The API may return additional keyword ideas, so we filter
        input_keywords_lower = {kw.lower() for kw in keywords}
        matched_results = []
        for r in all_results:
            if r["keyword"].lower() in input_keywords_lower:
                matched_results.append(r)

        logger.info(
            "Keyword Planner: %d/%d keywords matched for %s",
            len(matched_results), len(keywords), country
        )
        return matched_results

    except GoogleAdsException as ex:
        logger.error(
            "Google Ads API error: %s",
            ex.failure.errors[0].message if ex.failure.errors else str(ex)
        )
        return []
    except Exception as e:
        logger.error("Keyword Planner validation failed: %s", e)
        return []


def filter_keywords(
    validated_keywords: list[dict],
    config: AppConfig = None,
) -> list[dict]:
    """
    Filter validated keywords based on minimum thresholds.

    Filters:
    - monthly_search_volume >= min_volume
    - Preliminary CPC check (will be refined after economics calculation)
    """
    config = config or AppConfig()
    min_volume = config.min_search_volume

    filtered = []
    for kw in validated_keywords:
        volume = kw.get("monthly_search_volume", 0)

        if volume < min_volume:
            logger.debug(
                "Filtered out '%s': volume %d < %d",
                kw["keyword"], volume, min_volume
            )
            continue

        filtered.append(kw)

    logger.info(
        "Keyword filter: %d/%d passed (min volume: %d)",
        len(filtered), len(validated_keywords), min_volume
    )
    return filtered
