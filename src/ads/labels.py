"""
Google Merchant Center product label management.
Syncs product statuses from the Sheet to Merchant Center custom labels.

Mapping:
  custom_label_0 = product test_status (ready_to_test, testing, winner, killed, etc.)
  
PMax campaigns use listing group filters based on custom_label_0:
  - Testing campaign: custom_label_0 IN (ready_to_test, testing)
  - Winners campaign: custom_label_0 = winner
"""

from __future__ import annotations

import logging
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build

from src.core.config import (
    AppConfig,
    GOOGLE_MERCHANT_CENTER_ID,
    GOOGLE_SHEETS_CREDENTIALS_PATH,
)
from src.core.models import Product, ProductStatus

logger = logging.getLogger(__name__)

# Statuses that should be active in Google Ads
ACTIVE_STATUSES = {
    ProductStatus.READY_TO_TEST.value,
    ProductStatus.TESTING.value,
    ProductStatus.WINNER.value,
    ProductStatus.SCALING.value,
    ProductStatus.LISTING_CREATED.value,
}


class MerchantCenterLabels:
    """
    Manages custom labels on Google Merchant Center products.
    Uses the Content API for Shopping to update product labels.
    Authenticates via service account (same one used for Google Sheets).
    """

    def __init__(self, config: AppConfig = None):
        self.config = config or AppConfig()
        self.merchant_id = GOOGLE_MERCHANT_CENTER_ID
        self._service = None

    def _get_service(self):
        """Get or create the Merchant Center API service."""
        if self._service:
            return self._service

        creds = service_account.Credentials.from_service_account_file(
            GOOGLE_SHEETS_CREDENTIALS_PATH,
            scopes=["https://www.googleapis.com/auth/content"],
        )

        self._service = build("content", "v2.1", credentials=creds)
        return self._service

    def sync_product_label(
        self,
        shopify_product_id: str,
        test_status: str,
        country: str = "DE",
    ) -> bool:
        """
        Update custom_label_0 on a Merchant Center product.

        Args:
            shopify_product_id: The Shopify product ID (used to find MC product)
            test_status: The product's test_status value
            country: Target country

        Returns:
            True if successful
        """
        try:
            service = self._get_service()

            # Find the product in Merchant Center by Shopify ID
            # Shopify products typically have offer_id = shopify_{country}_{id}
            offer_id = f"shopify_{country}_{shopify_product_id}"
            product_id = f"online:en:{country}:{offer_id}"

            # Update the product with the new custom label
            body = {
                "customLabel0": test_status,
            }

            result = service.products().update(
                merchantId=self.merchant_id,
                productId=product_id,
                body=body,
            ).execute()

            logger.info(
                "Synced label for MC product %s: custom_label_0 = %s",
                offer_id, test_status
            )
            return True

        except Exception as e:
            logger.error(
                "Failed to sync label for product %s: %s",
                shopify_product_id, e
            )
            return False

    def sync_all_product_labels(self, products: list[Product]) -> dict:
        """
        Sync custom_label_0 for all products that have a Shopify listing.

        Returns:
            dict with success_count and error_count
        """
        results = {"success_count": 0, "error_count": 0, "skipped": 0}

        for product in products:
            if not product.shopify_product_id:
                results["skipped"] += 1
                continue

            success = self.sync_product_label(
                shopify_product_id=product.shopify_product_id,
                test_status=product.test_status,
                country=product.country,
            )

            if success:
                results["success_count"] += 1
            else:
                results["error_count"] += 1

        logger.info(
            "Label sync complete: %d success, %d errors, %d skipped",
            results["success_count"], results["error_count"], results["skipped"]
        )
        return results

    def get_product_labels(self, country: str = "DE") -> dict[str, str]:
        """
        Get current custom_label_0 values for all products in Merchant Center.

        Returns:
            dict mapping offer_id -> custom_label_0
        """
        try:
            service = self._get_service()
            labels = {}

            request = service.products().list(merchantId=self.merchant_id)
            while request:
                response = request.execute()
                for product in response.get("resources", []):
                    offer_id = product.get("offerId", "")
                    label = product.get("customLabel0", "")
                    if offer_id:
                        labels[offer_id] = label

                request = service.products().list_next(request, response)

            return labels

        except Exception as e:
            logger.error("Failed to get product labels: %s", e)
            return {}
