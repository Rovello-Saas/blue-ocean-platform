"""
Decision Engine.
Evaluates product performance and makes automated decisions:
- Kill: Product is not performing, remove from ads
- Maintain: Product is performing at break-even or slightly above, keep as-is
- Promote to Winner: Product exceeds performance thresholds
- Scale: Winner product is consistently above threshold, increase budget
- Pause: Winner drops below break-even

All thresholds are configurable via the dashboard (AppConfig).
Every decision is logged with a reason.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from src.core.config import AppConfig
from src.core.interfaces import DataStore
from src.core.models import (
    Product, ProductStatus, ActionLog, ActionType, Notification
)
from src.economics.validator import EconomicValidator
from src.ads.campaign_manager import CampaignManager, winners_campaign_name

logger = logging.getLogger(__name__)


class Decision:
    """Represents a decision made by the engine."""

    def __init__(
        self,
        action: str,
        old_status: str,
        new_status: str,
        reason: str,
        details: str = "",
        budget_change: float = 0,
    ):
        self.action = action
        self.old_status = old_status
        self.new_status = new_status
        self.reason = reason
        self.details = details
        self.budget_change = budget_change


class DecisionEngine:
    """
    Evaluates all active products and makes automated decisions.
    Reads all thresholds from AppConfig (dashboard-configurable).
    """

    def __init__(
        self,
        data_store: DataStore,
        config: AppConfig = None,
        campaign_manager: CampaignManager = None,
    ):
        self.store = data_store
        self.config = config or AppConfig()
        self.campaigns = campaign_manager or CampaignManager(self.config)
        self.validator = EconomicValidator(self.config)

    def evaluate_all_products(self, country: str = None) -> list[dict]:
        """
        Evaluate products in testing/winner/scaling status for a specific country.
        If no country is given, evaluates products across all countries.

        Args:
            country: Optional country code to filter by (e.g. "DE", "NL")

        Returns:
            List of decision result dicts
        """
        results = []

        # Get products that need evaluation, filtered by country
        testing_products = self.store.get_products(
            country=country, status=ProductStatus.TESTING.value
        )
        winner_products = self.store.get_products(
            country=country, status=ProductStatus.WINNER.value
        )
        scaling_products = self.store.get_products(
            country=country, status=ProductStatus.SCALING.value
        )

        all_active = testing_products + winner_products + scaling_products

        logger.info(
            "Evaluating %d products (country=%s): %d testing, %d winners, %d scaling",
            len(all_active), country or "ALL", len(testing_products),
            len(winner_products), len(scaling_products)
        )

        for product in all_active:
            decision = self.evaluate_product(product)
            if decision:
                self._apply_decision(product, decision)
                results.append({
                    "product_id": product.product_id,
                    "keyword": product.keyword,
                    "country": product.country,
                    "action": decision.action,
                    "old_status": decision.old_status,
                    "new_status": decision.new_status,
                    "reason": decision.reason,
                })

        logger.info(
            "Decision engine complete (country=%s): %d actions taken",
            country or "ALL", len(results)
        )
        return results

    def evaluate_product(self, product: Product) -> Optional[Decision]:
        """
        Evaluate a single product and return a decision.
        Returns None if no action is needed (maintain).
        """
        status = product.test_status

        if status == ProductStatus.TESTING.value:
            return self._evaluate_testing_product(product)
        elif status in (ProductStatus.WINNER.value, ProductStatus.SCALING.value):
            return self._evaluate_winner_product(product)
        else:
            return None

    def _evaluate_testing_product(self, product: Product) -> Optional[Decision]:
        """Evaluate a product in testing phase."""
        spend = float(product.spend or 0)
        conversions = int(product.conversions or 0)
        revenue = float(product.revenue or 0)
        days_testing = int(product.days_testing or 0)
        kill_threshold = float(product.kill_threshold_spend or 0)
        break_even_roas = float(product.break_even_roas or 0)

        # Calculate current ROAS
        roas = revenue / spend if spend > 0 else 0

        # --- Kill conditions ---

        # Kill condition 1: Spent kill threshold with zero conversions
        if spend >= kill_threshold and conversions == 0:
            return Decision(
                action=ActionType.PRODUCT_KILLED.value,
                old_status=ProductStatus.TESTING.value,
                new_status=ProductStatus.KILLED.value,
                reason=f"Spent EUR {spend:.2f} (>= kill threshold EUR {kill_threshold:.2f}) with 0 conversions",
                details=f"Clicks: {product.clicks}, Impressions: {product.impressions}",
            )

        # Kill condition 2: Below break-even ROAS for too many days
        days_below = int(product.days_below_broas or 0)
        max_days = self.config.max_days_below_broas
        min_duration = self.config.min_test_duration_days

        if (
            days_below >= max_days
            and days_testing >= min_duration
            and spend > 0
            and roas < break_even_roas
        ):
            return Decision(
                action=ActionType.PRODUCT_KILLED.value,
                old_status=ProductStatus.TESTING.value,
                new_status=ProductStatus.KILLED.value,
                reason=f"ROAS {roas:.2f} below break-even {break_even_roas:.2f} for {days_below} consecutive days (max: {max_days})",
                details=f"Spend: EUR {spend:.2f}, Revenue: EUR {revenue:.2f}, Conversions: {conversions}",
            )

        # --- Winner promotion conditions ---

        min_conversions = int(self.config.get("winner_rules.min_conversions", 3))
        min_test_days = int(self.config.get("winner_rules.min_test_duration_days", 3))

        if (
            roas >= break_even_roas
            and conversions >= min_conversions
            and days_testing >= min_test_days
        ):
            return Decision(
                action=ActionType.PRODUCT_WINNER.value,
                old_status=ProductStatus.TESTING.value,
                new_status=ProductStatus.WINNER.value,
                reason=f"ROAS {roas:.2f} >= break-even {break_even_roas:.2f} with {conversions} conversions over {days_testing} days",
                details=f"Spend: EUR {spend:.2f}, Revenue: EUR {revenue:.2f}, Net profit: EUR {self.validator.calculate_net_profit(product):.2f}",
            )

        # --- Maintain (no action) ---
        logger.debug(
            "Product %s: maintain (ROAS: %.2f, spend: EUR %.2f, conv: %d, days: %d)",
            product.product_id, roas, spend, conversions, days_testing
        )
        return None

    def _evaluate_winner_product(self, product: Product) -> Optional[Decision]:
        """Evaluate a winner/scaling product."""
        spend = float(product.spend or 0)
        revenue = float(product.revenue or 0)
        break_even_roas = float(product.break_even_roas or 0)
        roas = revenue / spend if spend > 0 else 0

        days_above = int(product.consecutive_days_above_scale_threshold or 0)
        days_since_scale = int(product.days_since_last_scale or 0)

        scale_threshold_pct = self.config.scale_threshold_pct
        scale_threshold_roas = break_even_roas * (1 + scale_threshold_pct)

        # --- Pause condition ---
        # Winner drops below break-even for too many days
        days_below = int(product.days_below_broas or 0)
        max_days = self.config.max_days_below_broas

        if days_below >= max_days and roas < break_even_roas:
            return Decision(
                action=ActionType.PRODUCT_PAUSED.value,
                old_status=product.test_status,
                new_status=ProductStatus.PAUSED.value,
                reason=f"Winner ROAS {roas:.2f} dropped below break-even {break_even_roas:.2f} for {days_below} days",
                details=f"Spend: EUR {spend:.2f}, Revenue: EUR {revenue:.2f}",
            )

        # --- Scale condition ---
        min_days = self.config.min_days_before_scale
        freq_days = self.config.scale_frequency_days

        if (
            roas >= scale_threshold_roas
            and days_above >= min_days
            and days_since_scale >= freq_days
        ):
            # Calculate new budget for this product's country campaign
            increment = self.config.scale_increment_pct
            max_budget = self.config.max_daily_budget

            country_winners = winners_campaign_name(product.country or "DE")
            current_budget = self.campaigns.get_campaign_budget(country_winners)
            if current_budget and current_budget < max_budget:
                new_budget = min(current_budget * (1 + increment), max_budget)
                budget_change = new_budget - current_budget

                return Decision(
                    action=ActionType.BUDGET_SCALED.value,
                    old_status=product.test_status,
                    new_status=ProductStatus.WINNER.value,
                    reason=f"ROAS {roas:.2f} >= scale threshold {scale_threshold_roas:.2f} for {days_above} consecutive days",
                    details=f"Budget ({country_winners}): EUR {current_budget:.2f} -> EUR {new_budget:.2f} (+{increment:.0%})",
                    budget_change=budget_change,
                )

        # No action needed
        return None

    def _apply_decision(self, product: Product, decision: Decision):
        """Apply a decision: update product, log action, send notification."""
        now = datetime.utcnow().isoformat()

        # Update product status
        updates = {
            "test_status": decision.new_status,
            "reason": decision.reason,
            "last_action_at": now,
        }

        # Reset tracking counters based on decision type
        if decision.action == ActionType.PRODUCT_WINNER.value:
            updates["consecutive_days_above_scale_threshold"] = 0
            updates["days_since_last_scale"] = 0

        if decision.action == ActionType.BUDGET_SCALED.value:
            updates["days_since_last_scale"] = 0
            updates["last_scale_at"] = now
            # Scale the budget for this product's country campaign
            self.campaigns.scale_winners_budget(country=product.country or "DE")

        # Calculate and update net profit
        net_profit = self.validator.calculate_net_profit(product)
        updates["net_profit"] = round(net_profit, 2)

        self.store.update_product(product.product_id, updates)

        # Log the action
        log = ActionLog(
            product_id=product.product_id,
            action_type=decision.action,
            old_status=decision.old_status,
            new_status=decision.new_status,
            reason=decision.reason,
            details=decision.details,
            country=product.country,
        )
        self.store.add_log(log)

        # Send notification
        level = "error" if "killed" in decision.action else (
            "success" if "winner" in decision.action or "scaled" in decision.action else "warning"
        )
        notification = Notification(
            title=f"Product {decision.action.replace('product_', '').replace('budget_', '').title()}",
            message=f"'{product.keyword}': {decision.reason}",
            level=level,
            product_id=product.product_id,
        )
        self.store.add_notification(notification)

        logger.info(
            "Decision applied: %s -> %s (%s) | %s",
            product.product_id, decision.new_status,
            decision.action, decision.reason
        )

    def update_daily_counters(self, country: str = None):
        """
        Update daily tracking counters for active products.
        Should be called once per day. If country is specified, only
        updates products for that country.
        """
        active_statuses = [
            ProductStatus.TESTING.value,
            ProductStatus.WINNER.value,
            ProductStatus.SCALING.value,
        ]

        for status in active_statuses:
            products = self.store.get_products(country=country, status=status)
            for product in products:
                updates = {}
                spend = float(product.spend or 0)
                revenue = float(product.revenue or 0)
                break_even_roas = float(product.break_even_roas or 0)
                roas = revenue / spend if spend > 0 else 0

                # Increment days testing
                updates["days_testing"] = int(product.days_testing or 0) + 1

                # Track days below break-even ROAS
                if spend > 0 and roas < break_even_roas:
                    updates["days_below_broas"] = int(product.days_below_broas or 0) + 1
                else:
                    updates["days_below_broas"] = 0

                # Track consecutive days above scale threshold
                scale_threshold_roas = break_even_roas * (1 + self.config.scale_threshold_pct)
                if roas >= scale_threshold_roas:
                    updates["consecutive_days_above_scale_threshold"] = (
                        int(product.consecutive_days_above_scale_threshold or 0) + 1
                    )
                else:
                    updates["consecutive_days_above_scale_threshold"] = 0

                # Track days since last scale
                updates["days_since_last_scale"] = int(product.days_since_last_scale or 0) + 1

                # Update net profit
                net_profit = self.validator.calculate_net_profit(product)
                updates["net_profit"] = round(net_profit, 2)

                # Update ROAS
                updates["roas"] = round(roas, 2)

                self.store.update_product(product.product_id, updates)

        logger.info("Updated daily counters for all active products")

    def update_research_feedback(self):
        """
        Analyze winners and losers to generate feedback for the LLM.
        Updates the research feedback data in the store.
        """
        winners = self.store.get_products(status=ProductStatus.WINNER.value)
        killed = self.store.get_products(status=ProductStatus.KILLED.value)

        # Extract patterns from winners
        winning_categories = []
        winning_margins = []
        winning_competitions = []

        for w in winners:
            keyword_parts = w.keyword.lower().split()
            if len(keyword_parts) > 1:
                winning_categories.append(" ".join(keyword_parts[:2]))
            if w.net_margin_pct:
                winning_margins.append(float(w.net_margin_pct))
            if w.competitor_count:
                winning_competitions.append(int(w.competitor_count))

        # Extract patterns from killed products
        losing_categories = []
        for k in killed:
            keyword_parts = k.keyword.lower().split()
            if len(keyword_parts) > 1:
                losing_categories.append(" ".join(keyword_parts[:2]))

        # Build feedback
        feedback = {
            "winning_categories": list(set(winning_categories))[:20],
            "losing_categories": list(set(losing_categories))[:20],
            "avg_winning_margin_pct": (
                sum(winning_margins) / len(winning_margins) if winning_margins else 0
            ),
            "avg_winning_competition": (
                int(sum(winning_competitions) / len(winning_competitions))
                if winning_competitions else 0
            ),
            "total_winners": len(winners),
            "total_killed": len(killed),
            "last_updated": datetime.utcnow().isoformat(),
        }

        self.store.save_research_feedback(feedback)
        logger.info(
            "Updated research feedback: %d winners, %d killed analyzed",
            len(winners), len(killed)
        )
