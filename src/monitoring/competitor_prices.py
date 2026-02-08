"""
Competitor price monitoring.
Periodically re-checks Google Shopping prices for active/winner products.
Alerts if competitors significantly change their pricing.
"""

from __future__ import annotations

import logging
from typing import Optional

from src.core.config import AppConfig
from src.core.interfaces import DataStore
from src.core.models import Product, ProductStatus, ActionLog, ActionType, Notification
from src.research.competition import analyze_competition

logger = logging.getLogger(__name__)


class CompetitorPriceMonitor:
    """
    Monitors competitor prices for active and winner products.
    Alerts when prices change significantly.
    """

    def __init__(self, data_store: DataStore, config: AppConfig = None):
        self.store = data_store
        self.config = config or AppConfig()

    def check_all_active_products(self) -> list[dict]:
        """
        Check competitor prices for all winner and testing products.

        Returns:
            List of alert dicts for products with significant price changes
        """
        if not self.config.get("monitoring.competitor_price_enabled", True):
            logger.info("Competitor price monitoring is disabled")
            return []

        alerts = []

        # Check winners and testing products
        for status in [ProductStatus.WINNER.value, ProductStatus.TESTING.value]:
            products = self.store.get_products(status=status)
            for product in products:
                alert = self.check_product_price(product)
                if alert:
                    alerts.append(alert)

        if alerts:
            logger.info("Found %d competitor price alerts", len(alerts))
        else:
            logger.info("No significant competitor price changes detected")

        return alerts

    def check_product_price(self, product: Product) -> Optional[dict]:
        """
        Check current competitor prices for a specific product.

        Returns:
            Alert dict if significant change detected, None otherwise
        """
        if not product.keyword or not product.selling_price:
            return None

        # Get current competitor data
        comp_data = analyze_competition(
            keyword=product.keyword,
            country=product.country,
            language=product.language,
            config=self.config,
        )

        if not comp_data:
            return None

        current_median = comp_data.get("median_competitor_price", 0)
        original_selling_price = float(product.selling_price or 0)

        if current_median <= 0 or original_selling_price <= 0:
            return None

        # Calculate price change percentage
        price_change_pct = (current_median - original_selling_price) / original_selling_price
        threshold = float(self.config.get("monitoring.price_change_alert_threshold_pct", 0.10))

        # Only alert on significant changes
        if abs(price_change_pct) < threshold:
            return None

        # Build alert
        direction = "dropped" if price_change_pct < 0 else "increased"
        alert = {
            "product_id": product.product_id,
            "keyword": product.keyword,
            "country": product.country,
            "original_selling_price": original_selling_price,
            "current_competitor_median": current_median,
            "price_change_pct": round(price_change_pct * 100, 1),
            "direction": direction,
            "competitor_count": comp_data.get("competitor_count", 0),
        }

        # Log and notify
        reason = (
            f"Competitor median price {direction} by {abs(price_change_pct):.1%}: "
            f"EUR {original_selling_price:.2f} -> EUR {current_median:.2f}"
        )

        log = ActionLog(
            product_id=product.product_id,
            action_type=ActionType.PRICE_ALERT.value,
            old_status=product.test_status,
            new_status=product.test_status,  # Status doesn't change
            reason=reason,
            details=f"Competitors: {comp_data.get('competitor_count', 0)}, "
                    f"Price range: EUR {comp_data.get('price_range_min', 0):.2f} - EUR {comp_data.get('price_range_max', 0):.2f}",
            country=product.country,
        )
        self.store.add_log(log)

        level = "warning" if price_change_pct < 0 else "info"
        notification = Notification(
            title=f"Price alert: {product.keyword}",
            message=f"Competitor prices have {direction} by {abs(price_change_pct):.1%}. "
                    f"Current median: EUR {current_median:.2f} (was EUR {original_selling_price:.2f}). "
                    f"Review pricing strategy.",
            level=level,
            product_id=product.product_id,
        )
        self.store.add_notification(notification)

        logger.info(
            "Price alert for '%s': %s by %.1f%%",
            product.keyword, direction, abs(price_change_pct) * 100
        )

        return alert
