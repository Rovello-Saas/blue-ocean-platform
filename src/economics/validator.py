"""
Economic Validation Engine.
Calculates all derived economics and validates product viability.

Auto-calculated fields:
  gross_margin = selling_price - landed_cost
  gross_margin_pct = gross_margin / selling_price
  transaction_fees = selling_price * (transaction_fee_pct + payment_fee_pct) + payment_fixed_fee
  net_margin = gross_margin - transaction_fees
  net_margin_pct = net_margin / selling_price
  break_even_roas = 1 / net_margin_pct
  target_roas = break_even_roas * safety_factor
  break_even_cpa = net_margin
  max_allowed_cpc = break_even_cpa * assumed_conversion_rate
  test_budget = selling_price * test_budget_multiplier
  kill_threshold_spend = test_budget
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from src.core.config import AppConfig
from src.core.models import Product, ProductStatus, ActionLog, ActionType, Notification

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of economic validation."""
    passed: bool
    reason: str
    economics: dict  # All calculated economic fields


class EconomicValidator:
    """
    Validates product economics and calculates all derived fields.
    All thresholds come from AppConfig (configurable via dashboard).
    """

    def __init__(self, config: AppConfig = None):
        self.config = config or AppConfig()

    def calculate_economics(self, product: Product) -> dict:
        """
        Calculate all economic fields for a product.
        Returns a dict of field_name -> value.
        """
        selling_price = float(product.selling_price or 0)
        landed_cost = float(product.landed_cost or 0)

        if selling_price <= 0:
            return self._empty_economics()

        # Core margin calculation
        gross_margin = selling_price - landed_cost
        gross_margin_pct = gross_margin / selling_price if selling_price > 0 else 0

        # Transaction fees
        transaction_fees = (
            selling_price * (self.config.transaction_fee_pct + self.config.payment_fee_pct)
            + self.config.payment_fixed_fee
        )

        # Net margin (after transaction fees, before ad spend)
        net_margin = gross_margin - transaction_fees
        net_margin_pct = net_margin / selling_price if selling_price > 0 else 0

        # Break-even and target ROAS
        break_even_roas = (1 / net_margin_pct) if net_margin_pct > 0 else 999
        target_roas = break_even_roas * self.config.safety_factor

        # CPA and CPC limits
        break_even_cpa = net_margin
        max_allowed_cpc = break_even_cpa * self.config.assumed_conversion_rate

        # Test budget
        test_budget = selling_price * self.config.test_budget_multiplier
        kill_threshold_spend = test_budget

        return {
            "gross_margin": round(gross_margin, 2),
            "gross_margin_pct": round(gross_margin_pct, 4),
            "transaction_fees": round(transaction_fees, 2),
            "net_margin": round(net_margin, 2),
            "net_margin_pct": round(net_margin_pct, 4),
            "break_even_roas": round(break_even_roas, 2),
            "target_roas": round(target_roas, 2),
            "break_even_cpa": round(break_even_cpa, 2),
            "max_allowed_cpc": round(max_allowed_cpc, 2),
            "test_budget": round(test_budget, 2),
            "kill_threshold_spend": round(kill_threshold_spend, 2),
        }

    def validate(self, product: Product) -> ValidationResult:
        """
        Validate whether a product's economics are viable for testing.
        Returns ValidationResult with pass/fail and reason.
        """
        economics = self.calculate_economics(product)
        reasons = []

        # Check selling price exists
        if float(product.selling_price or 0) <= 0:
            return ValidationResult(
                passed=False,
                reason="No selling price set",
                economics=economics
            )

        # Check landed cost exists
        if float(product.landed_cost or 0) <= 0:
            return ValidationResult(
                passed=False,
                reason="No landed cost from agent yet",
                economics=economics
            )

        # Check minimum search volume
        volume = int(product.monthly_search_volume or 0)
        if volume < self.config.min_search_volume:
            reasons.append(
                f"Search volume {volume} < minimum {self.config.min_search_volume}"
            )

        # Check CPC vs max allowed
        estimated_cpc = float(product.estimated_cpc or 0)
        max_cpc = economics["max_allowed_cpc"]
        if estimated_cpc > 0 and max_cpc > 0 and estimated_cpc > max_cpc:
            reasons.append(
                f"Estimated CPC EUR {estimated_cpc:.2f} > max allowed EUR {max_cpc:.2f}"
            )

        # Check net margin percentage
        if economics["net_margin_pct"] < self.config.min_gross_margin_pct:
            reasons.append(
                f"Net margin {economics['net_margin_pct']:.1%} < minimum {self.config.min_gross_margin_pct:.1%}"
            )

        # Check competitor count
        competitors = int(product.competitor_count or 0)
        if competitors > 0 and competitors > self.config.max_competitors:
            reasons.append(
                f"Competitor count {competitors} > maximum {self.config.max_competitors}"
            )

        # Check differentiation score
        diff_score = float(product.differentiation_score or 0)
        if diff_score > 0 and diff_score < self.config.min_differentiation_score:
            reasons.append(
                f"Differentiation score {diff_score:.0f} < minimum {self.config.min_differentiation_score:.0f}"
            )

        if reasons:
            return ValidationResult(
                passed=False,
                reason="; ".join(reasons),
                economics=economics
            )

        return ValidationResult(
            passed=True,
            reason="All economic criteria met",
            economics=economics
        )

    def calculate_net_profit(self, product: Product) -> float:
        """
        Calculate net profit for an active product.
        net_profit = revenue - (landed_cost * units_sold) - ad_spend - (transaction_fees * units_sold)
        """
        revenue = float(product.revenue or 0)
        spend = float(product.spend or 0)
        conversions = int(product.conversions or 0)  # units sold
        landed_cost = float(product.landed_cost or 0)
        selling_price = float(product.selling_price or 0)

        if conversions <= 0:
            return -spend  # Loss = ad spend with no sales

        # Transaction fees per unit
        transaction_fees_per_unit = (
            selling_price * (self.config.transaction_fee_pct + self.config.payment_fee_pct)
            + self.config.payment_fixed_fee
        )

        # Total costs
        total_product_cost = landed_cost * conversions
        total_transaction_fees = transaction_fees_per_unit * conversions
        total_cost = total_product_cost + total_transaction_fees + spend

        net_profit = revenue - total_cost
        return round(net_profit, 2)

    def process_product_with_cost(self, product: Product, data_store) -> tuple[Product, ActionLog]:
        """
        Process a product that has received a landed cost from the agent.
        Calculates economics, validates, and updates status.
        Returns updated product and action log.
        """
        result = self.validate(product)
        economics = result.economics

        # Update product with calculated economics
        updates = {**economics}

        if result.passed:
            updates["test_status"] = ProductStatus.READY_TO_TEST.value
            updates["reason"] = result.reason
            log = ActionLog(
                product_id=product.product_id,
                action_type=ActionType.ECONOMICS_PASSED.value,
                old_status=product.test_status,
                new_status=ProductStatus.READY_TO_TEST.value,
                reason=result.reason,
                details=f"Margin: {economics['net_margin_pct']:.1%}, Max CPC: EUR {economics['max_allowed_cpc']:.2f}, Test budget: EUR {economics['test_budget']:.2f}",
                country=product.country
            )
            notification = Notification(
                title="Product ready for testing",
                message=f"Product '{product.keyword}' passed economics. Margin: {economics['net_margin_pct']:.1%}, Ready for PMax testing.",
                level="success",
                product_id=product.product_id
            )
        else:
            updates["test_status"] = ProductStatus.REJECTED.value
            updates["reason"] = result.reason
            log = ActionLog(
                product_id=product.product_id,
                action_type=ActionType.ECONOMICS_FAILED.value,
                old_status=product.test_status,
                new_status=ProductStatus.REJECTED.value,
                reason=result.reason,
                details=f"Landed cost: EUR {product.landed_cost}, Selling price: EUR {product.selling_price}",
                country=product.country
            )
            notification = Notification(
                title="Product rejected",
                message=f"Product '{product.keyword}' failed economics: {result.reason}",
                level="warning",
                product_id=product.product_id
            )

        # Persist changes
        data_store.update_product(product.product_id, updates)
        data_store.add_log(log)
        data_store.add_notification(notification)

        logger.info(
            "Product %s: %s - %s",
            product.product_id,
            "PASSED" if result.passed else "REJECTED",
            result.reason
        )

        return product, log

    @staticmethod
    def _empty_economics() -> dict:
        return {
            "gross_margin": 0,
            "gross_margin_pct": 0,
            "transaction_fees": 0,
            "net_margin": 0,
            "net_margin_pct": 0,
            "break_even_roas": 0,
            "target_roas": 0,
            "break_even_cpa": 0,
            "max_allowed_cpc": 0,
            "test_budget": 0,
            "kill_threshold_spend": 0,
        }
