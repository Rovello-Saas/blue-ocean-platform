"""
Google Ads PMax campaign management.
Manages per-country campaign pairs (Testing + Winners) and budget scaling.

Campaign naming convention:
    "Blue Ocean - Testing - {COUNTRY}"   e.g. "Blue Ocean - Testing - DE"
    "Blue Ocean - Winners - {COUNTRY}"   e.g. "Blue Ocean - Winners - DE"
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

# Campaign name templates (country code is appended). These strings become
# the actual campaign names in the Google Ads UI — keep them short and stable.
# Renamed from "Qoveliqo - ..." during the Blue Ocean Platform rebrand; if you
# ever have existing Qoveliqo campaigns in production, they'll need to be
# archived or renamed manually (the Ads API treats the name as the key).
TESTING_CAMPAIGN_PREFIX = "Blue Ocean - Testing"
WINNERS_CAMPAIGN_PREFIX = "Blue Ocean - Winners"


def testing_campaign_name(country: str) -> str:
    """Return the testing campaign name for a given country."""
    return f"{TESTING_CAMPAIGN_PREFIX} - {country.upper()}"


def winners_campaign_name(country: str) -> str:
    """Return the winners campaign name for a given country."""
    return f"{WINNERS_CAMPAIGN_PREFIX} - {country.upper()}"


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


class CampaignManager:
    """
    Manages PMax campaigns for product testing and scaling.

    Campaign structure (per country):
    1. Testing Campaign: Fixed budget, includes ready_to_test + testing products
    2. Winners Campaign: Scalable budget, includes winner products

    Example for DE:
        "Blue Ocean - Testing - DE"
        "Blue Ocean - Winners - DE"
    """

    def __init__(self, config: AppConfig = None):
        self.config = config or AppConfig()
        self.client = None
        self.customer_id = GOOGLE_ADS_CUSTOMER_ID.replace("-", "")

    def _get_client(self) -> GoogleAdsClient:
        if not self.client:
            self.client = _get_google_ads_client()
        return self.client

    def get_campaign_budget(self, campaign_name: str) -> Optional[float]:
        """
        Get the current daily budget for a campaign.

        Args:
            campaign_name: Name of the campaign

        Returns:
            Daily budget in EUR or None
        """
        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    campaign.name,
                    campaign_budget.amount_micros
                FROM campaign
                WHERE campaign.name = '{campaign_name}'
                    AND campaign.status != 'REMOVED'
            """

            response = ga_service.search(
                customer_id=self.customer_id,
                query=query,
            )

            for row in response:
                budget_micros = row.campaign_budget.amount_micros
                return budget_micros / 1_000_000

            logger.warning("Campaign '%s' not found", campaign_name)
            return None

        except GoogleAdsException as ex:
            logger.error("Google Ads API error: %s", ex)
            return None
        except Exception as e:
            logger.error("Failed to get campaign budget: %s", e)
            return None

    def update_campaign_budget(
        self,
        campaign_name: str,
        new_daily_budget: float,
    ) -> bool:
        """
        Update the daily budget of a campaign.

        Args:
            campaign_name: Name of the campaign
            new_daily_budget: New daily budget in EUR

        Returns:
            True if successful
        """
        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")
            campaign_budget_service = client.get_service("CampaignBudgetService")

            # First, find the campaign and its budget resource name
            query = f"""
                SELECT
                    campaign.campaign_budget,
                    campaign.name
                FROM campaign
                WHERE campaign.name = '{campaign_name}'
                    AND campaign.status != 'REMOVED'
            """

            response = ga_service.search(
                customer_id=self.customer_id,
                query=query,
            )

            budget_resource_name = None
            for row in response:
                budget_resource_name = row.campaign.campaign_budget
                break

            if not budget_resource_name:
                logger.error("Budget not found for campaign '%s'", campaign_name)
                return False

            # Update the budget
            budget_operation = client.get_type("CampaignBudgetOperation")
            budget = budget_operation.update
            budget.resource_name = budget_resource_name
            budget.amount_micros = int(new_daily_budget * 1_000_000)

            client.copy_from(
                budget_operation.update_mask,
                client.get_type("FieldMask")(paths=["amount_micros"]),
            )

            campaign_budget_service.mutate_campaign_budgets(
                customer_id=self.customer_id,
                operations=[budget_operation],
            )

            logger.info(
                "Updated budget for '%s': EUR %.2f/day",
                campaign_name, new_daily_budget
            )
            return True

        except GoogleAdsException as ex:
            logger.error(
                "Failed to update budget for '%s': %s",
                campaign_name,
                ex.failure.errors[0].message if ex.failure.errors else str(ex),
            )
            return False
        except Exception as e:
            logger.error("Failed to update campaign budget: %s", e)
            return False

    def scale_winners_budget(
        self,
        country: str = "DE",
        increment_pct: float = None,
        max_budget: float = None,
    ) -> Optional[float]:
        """
        Scale the Winners campaign budget for a specific country by a percentage.

        Args:
            country: Country code (e.g. "DE", "NL")
            increment_pct: Percentage to increase (e.g., 0.20 for 20%)
            max_budget: Maximum daily budget cap

        Returns:
            New budget amount or None if failed/at cap
        """
        increment_pct = increment_pct or self.config.scale_increment_pct
        max_budget = max_budget or self.config.max_daily_budget

        campaign_name = winners_campaign_name(country)
        current_budget = self.get_campaign_budget(campaign_name)
        if current_budget is None:
            return None

        if current_budget >= max_budget:
            logger.info(
                "Winners campaign '%s' already at max budget: EUR %.2f",
                campaign_name, current_budget
            )
            return current_budget

        new_budget = min(current_budget * (1 + increment_pct), max_budget)

        if self.update_campaign_budget(campaign_name, new_budget):
            logger.info(
                "Scaled Winners budget '%s': EUR %.2f -> EUR %.2f (+%.0f%%)",
                campaign_name, current_budget, new_budget, increment_pct * 100
            )
            return new_budget

        return None

    def get_all_campaigns(self) -> list[dict]:
        """Get all active PMax campaigns."""
        try:
            client = self._get_client()
            ga_service = client.get_service("GoogleAdsService")

            query = """
                SELECT
                    campaign.name,
                    campaign.status,
                    campaign.advertising_channel_type,
                    campaign_budget.amount_micros
                FROM campaign
                WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'
                    AND campaign.status != 'REMOVED'
            """

            response = ga_service.search(
                customer_id=self.customer_id,
                query=query,
            )

            campaigns = []
            for row in response:
                campaigns.append({
                    "name": row.campaign.name,
                    "status": row.campaign.status.name,
                    "type": "PERFORMANCE_MAX",
                    "daily_budget": row.campaign_budget.amount_micros / 1_000_000,
                })

            return campaigns

        except Exception as e:
            logger.error("Failed to get campaigns: %s", e)
            return []

    def ensure_campaigns_exist(self, country: str = None) -> bool:
        """
        Verify that Testing and Winners campaigns exist for the given country
        (or all configured countries if none specified).
        Returns True if all required campaigns exist, False otherwise.

        NOTE: Campaign creation should be done manually in Google Ads UI
        as PMax campaigns require asset groups, feed connections, etc.
        """
        campaigns = self.get_all_campaigns()
        names = {c["name"] for c in campaigns}

        if country:
            countries = [country]
        else:
            countries = [
                c.get("code", "DE") if isinstance(c, dict) else str(c)
                for c in self.config.countries
            ]

        all_exist = True
        for cc in countries:
            t_name = testing_campaign_name(cc)
            w_name = winners_campaign_name(cc)

            if t_name not in names:
                logger.warning(
                    "Testing campaign '%s' not found. Please create it in Google Ads.",
                    t_name
                )
                all_exist = False
            if w_name not in names:
                logger.warning(
                    "Winners campaign '%s' not found. Please create it in Google Ads.",
                    w_name
                )
                all_exist = False

        return all_exist
