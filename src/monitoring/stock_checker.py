"""
Stock/availability monitoring for AliExpress products.
Toggleable feature — disabled by default (agent-based sourcing handles this).
Periodically checks if products are still available on AliExpress.
"""

from __future__ import annotations

import logging
from typing import Optional

from src.core.config import AppConfig
from src.core.interfaces import DataStore
from src.core.models import Product, ProductStatus, ActionLog, ActionType, Notification
from src.research.aliexpress import check_product_availability

logger = logging.getLogger(__name__)


class StockChecker:
    """
    Checks AliExpress product availability for active products.
    Auto-pauses ads if a product becomes unavailable.
    Disabled by default (toggle in dashboard settings).
    """

    def __init__(self, data_store: DataStore, config: AppConfig = None):
        self.store = data_store
        self.config = config or AppConfig()

    def check_all_active_products(self) -> list[dict]:
        """
        Check stock availability for all active products.

        Returns:
            List of alert dicts for out-of-stock products
        """
        if not self.config.get("monitoring.stock_monitoring_enabled", False):
            logger.info("Stock monitoring is disabled")
            return []

        alerts = []

        for status in [ProductStatus.TESTING.value, ProductStatus.WINNER.value]:
            products = self.store.get_products(status=status)
            for product in products:
                alert = self.check_product_stock(product)
                if alert:
                    alerts.append(alert)

        if alerts:
            logger.warning("Found %d out-of-stock products", len(alerts))
        else:
            logger.info("All active products are in stock")

        return alerts

    def check_product_stock(self, product: Product) -> Optional[dict]:
        """
        Check if a specific product is still available on AliExpress.
        Auto-pauses the product if it's unavailable.

        Returns:
            Alert dict if out of stock, None if available
        """
        if not product.aliexpress_url:
            return None

        # Extract product ID from URL
        ali_product_id = self._extract_product_id(product.aliexpress_url)
        if not ali_product_id:
            return None

        # Check availability
        is_available = check_product_availability(ali_product_id)

        if is_available:
            return None

        # Product is unavailable — auto-pause
        alert = {
            "product_id": product.product_id,
            "keyword": product.keyword,
            "aliexpress_url": product.aliexpress_url,
            "status": "out_of_stock",
            "action_taken": "paused",
        }

        # Update product status
        self.store.update_product(product.product_id, {
            "test_status": ProductStatus.PAUSED.value,
            "reason": "Product unavailable on AliExpress — auto-paused",
        })

        # Log
        log = ActionLog(
            product_id=product.product_id,
            action_type=ActionType.STOCK_ALERT.value,
            old_status=product.test_status,
            new_status=ProductStatus.PAUSED.value,
            reason="Product no longer available on AliExpress",
            details=f"URL: {product.aliexpress_url}",
            country=product.country,
        )
        self.store.add_log(log)

        # Notify
        notification = Notification(
            title=f"Stock alert: {product.keyword}",
            message=f"Product is no longer available on AliExpress. "
                    f"Ads have been auto-paused. Check with your agent for alternatives.",
            level="error",
            product_id=product.product_id,
        )
        self.store.add_notification(notification)

        logger.warning(
            "Product '%s' is out of stock on AliExpress — paused",
            product.keyword
        )

        return alert

    @staticmethod
    def _extract_product_id(url: str) -> Optional[str]:
        """Extract AliExpress product ID from URL."""
        try:
            # URLs like: https://www.aliexpress.com/item/1005001234567.html
            parts = url.split("/item/")
            if len(parts) > 1:
                product_id = parts[1].split(".")[0].split("?")[0]
                return product_id

            # URLs like: https://aliexpress.com/item.htm?id=1005001234567
            if "id=" in url:
                for part in url.split("&"):
                    if part.startswith("id=") or "?id=" in part:
                        return part.split("=")[-1]

            return None
        except Exception:
            return None
