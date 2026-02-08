"""
Google Ads performance data retrieval.
Pulls per-product metrics from PMax campaigns via the Shopping Performance View.
Supports per-country campaign filtering.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
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


def get_product_performance(
    days: int = 30,
    campaign_name: str = None,
    country: str = None,
    config: AppConfig = None,
) -> list[dict]:
    """
    Get per-product performance data from Google Ads PMax campaigns.

    Uses the shopping_performance_view to get product-level metrics.
    When a country is provided (and no explicit campaign_name), the query
    is automatically scoped to that country's Testing and Winners campaigns.

    Args:
        days: Number of days of data to retrieve
        campaign_name: Optional explicit filter by campaign name
        country: Optional country code to scope to that country's campaigns
        config: App configuration

    Returns:
        List of dicts with:
            product_id (offer_id), clicks, impressions, spend, conversions,
            revenue, campaign_name, custom_label_0
    """
    config = config or AppConfig()

    try:
        client = _get_google_ads_client()
        customer_id = GOOGLE_ADS_CUSTOMER_ID.replace("-", "")

        ga_service = client.get_service("GoogleAdsService")

        # Date range
        end_date = datetime.utcnow().strftime("%Y-%m-%d")
        start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

        # GAQL query for shopping performance
        query = f"""
            SELECT
                segments.product_item_id,
                segments.product_custom_attribute0,
                segments.product_title,
                campaign.name,
                metrics.clicks,
                metrics.impressions,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value
            FROM shopping_performance_view
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """

        # Apply campaign filter: explicit name takes priority, then country-based
        if campaign_name:
            query += f" AND campaign.name = '{campaign_name}'"
        elif country:
            from src.ads.campaign_manager import testing_campaign_name, winners_campaign_name
            t_name = testing_campaign_name(country)
            w_name = winners_campaign_name(country)
            query += f" AND campaign.name IN ('{t_name}', '{w_name}')"

        # Execute query
        response = ga_service.search(
            customer_id=customer_id,
            query=query,
        )

        # Aggregate by product
        product_metrics = {}

        for row in response:
            product_id = row.segments.product_item_id
            if not product_id:
                continue

            if product_id not in product_metrics:
                product_metrics[product_id] = {
                    "product_id": product_id,
                    "product_title": row.segments.product_title,
                    "custom_label_0": row.segments.product_custom_attribute0,
                    "campaign_name": row.campaign.name,
                    "clicks": 0,
                    "impressions": 0,
                    "spend": 0.0,
                    "conversions": 0,
                    "revenue": 0.0,
                }

            metrics = product_metrics[product_id]
            metrics["clicks"] += row.metrics.clicks
            metrics["impressions"] += row.metrics.impressions
            metrics["spend"] += row.metrics.cost_micros / 1_000_000
            metrics["conversions"] += int(row.metrics.conversions)
            metrics["revenue"] += row.metrics.conversions_value

        # Calculate ROAS
        results = []
        for product_id, metrics in product_metrics.items():
            metrics["spend"] = round(metrics["spend"], 2)
            metrics["revenue"] = round(metrics["revenue"], 2)
            metrics["roas"] = (
                round(metrics["revenue"] / metrics["spend"], 2)
                if metrics["spend"] > 0
                else 0
            )
            results.append(metrics)

        logger.info(
            "Retrieved performance data for %d products (%d days, country=%s)",
            len(results), days, country or "ALL"
        )
        return results

    except GoogleAdsException as ex:
        logger.error(
            "Google Ads API error: %s",
            ex.failure.errors[0].message if ex.failure.errors else str(ex),
        )
        return []
    except Exception as e:
        logger.error("Failed to get product performance: %s", e)
        return []


def get_daily_performance(
    product_offer_id: str,
    days: int = 14,
    config: AppConfig = None,
) -> list[dict]:
    """
    Get daily performance breakdown for a specific product.

    Returns:
        List of dicts with: date, clicks, impressions, spend, conversions, revenue, roas
    """
    config = config or AppConfig()

    try:
        client = _get_google_ads_client()
        customer_id = GOOGLE_ADS_CUSTOMER_ID.replace("-", "")
        ga_service = client.get_service("GoogleAdsService")

        end_date = datetime.utcnow().strftime("%Y-%m-%d")
        start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

        query = f"""
            SELECT
                segments.date,
                metrics.clicks,
                metrics.impressions,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value
            FROM shopping_performance_view
            WHERE segments.product_item_id = '{product_offer_id}'
                AND segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY segments.date ASC
        """

        response = ga_service.search(
            customer_id=customer_id,
            query=query,
        )

        daily_data = []
        for row in response:
            spend = row.metrics.cost_micros / 1_000_000
            revenue = row.metrics.conversions_value
            daily_data.append({
                "date": row.segments.date,
                "clicks": row.metrics.clicks,
                "impressions": row.metrics.impressions,
                "spend": round(spend, 2),
                "conversions": int(row.metrics.conversions),
                "revenue": round(revenue, 2),
                "roas": round(revenue / spend, 2) if spend > 0 else 0,
            })

        return daily_data

    except Exception as e:
        logger.error("Failed to get daily performance for %s: %s", product_offer_id, e)
        return []


def get_campaign_performance(
    campaign_name: str,
    days: int = 7,
    config: AppConfig = None,
) -> Optional[dict]:
    """
    Get aggregated performance for an entire campaign.

    Returns:
        dict with: campaign_name, clicks, impressions, spend, conversions,
                   revenue, roas, daily_budget
    """
    config = config or AppConfig()

    try:
        client = _get_google_ads_client()
        customer_id = GOOGLE_ADS_CUSTOMER_ID.replace("-", "")
        ga_service = client.get_service("GoogleAdsService")

        end_date = datetime.utcnow().strftime("%Y-%m-%d")
        start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

        query = f"""
            SELECT
                campaign.name,
                campaign.campaign_budget,
                metrics.clicks,
                metrics.impressions,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value
            FROM campaign
            WHERE campaign.name = '{campaign_name}'
                AND segments.date BETWEEN '{start_date}' AND '{end_date}'
        """

        response = ga_service.search(
            customer_id=customer_id,
            query=query,
        )

        total = {
            "campaign_name": campaign_name,
            "clicks": 0,
            "impressions": 0,
            "spend": 0.0,
            "conversions": 0,
            "revenue": 0.0,
        }

        for row in response:
            total["clicks"] += row.metrics.clicks
            total["impressions"] += row.metrics.impressions
            total["spend"] += row.metrics.cost_micros / 1_000_000
            total["conversions"] += int(row.metrics.conversions)
            total["revenue"] += row.metrics.conversions_value

        total["spend"] = round(total["spend"], 2)
        total["revenue"] = round(total["revenue"], 2)
        total["roas"] = (
            round(total["revenue"] / total["spend"], 2)
            if total["spend"] > 0
            else 0
        )

        return total

    except Exception as e:
        logger.error("Failed to get campaign performance for '%s': %s", campaign_name, e)
        return None
