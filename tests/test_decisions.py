"""
Tests for the Decision Engine.
"""

import pytest
from unittest.mock import MagicMock, patch
from src.core.config import AppConfig
from src.core.models import Product, ProductStatus
from src.decisions.engine import DecisionEngine


@pytest.fixture
def config():
    c = AppConfig()
    c.set("kill_rules.kill_spend_multiplier", 3.0)
    c.set("kill_rules.max_days_below_broas", 3)
    c.set("kill_rules.min_test_duration_days", 3)
    c.set("scale_rules.scale_threshold_pct", 0.30)
    c.set("scale_rules.min_days_before_scale", 2)
    c.set("scale_rules.scale_increment_pct", 0.20)
    c.set("scale_rules.scale_frequency_days", 3)
    c.set("scale_rules.max_daily_budget", 100.0)
    c.set("winner_rules.min_conversions", 3)
    c.set("winner_rules.min_test_duration_days", 3)
    c.set("economics.assumed_conversion_rate", 0.01)
    c.set("economics.safety_factor", 1.5)
    c.set("economics.transaction_fee_pct", 0.02)
    c.set("economics.payment_fee_pct", 0.029)
    c.set("economics.payment_fixed_fee", 0.30)
    return c


@pytest.fixture
def mock_store():
    store = MagicMock()
    store.update_product = MagicMock()
    store.add_log = MagicMock()
    store.add_notification = MagicMock()
    return store


@pytest.fixture
def mock_campaigns():
    campaigns = MagicMock()
    campaigns.get_campaign_budget = MagicMock(return_value=50.0)
    campaigns.scale_winners_budget = MagicMock(return_value=60.0)
    return campaigns


@pytest.fixture
def engine(mock_store, config, mock_campaigns):
    return DecisionEngine(mock_store, config, mock_campaigns)


class TestKillDecisions:
    """Test kill decision logic."""

    def test_kill_zero_conversions_over_budget(self, engine):
        """Product should be killed when spend exceeds kill threshold with 0 conversions."""
        product = Product(
            product_id="test1",
            keyword="test product",
            selling_price=50.0,
            landed_cost=15.0,
            test_status=ProductStatus.TESTING.value,
            spend=160.0,  # > kill threshold (50 * 3 = 150)
            conversions=0,
            revenue=0,
            break_even_roas=1.5,
            kill_threshold_spend=150.0,
            days_testing=5,
        )

        decision = engine.evaluate_product(product)

        assert decision is not None
        assert decision.new_status == ProductStatus.KILLED.value
        assert "0 conversions" in decision.reason

    def test_kill_below_broas_too_long(self, engine):
        """Product killed after being below bROAS for too many days."""
        product = Product(
            product_id="test2",
            keyword="test product",
            test_status=ProductStatus.TESTING.value,
            spend=50.0,
            conversions=1,
            revenue=30.0,  # Low revenue
            break_even_roas=3.0,
            kill_threshold_spend=150.0,
            days_testing=5,
            days_below_broas=4,  # > max 3
        )

        decision = engine.evaluate_product(product)

        assert decision is not None
        assert decision.new_status == ProductStatus.KILLED.value
        assert "below break-even" in decision.reason.lower()

    def test_no_kill_before_min_duration(self, engine):
        """Product should not be killed before minimum test duration."""
        product = Product(
            product_id="test3",
            keyword="test product",
            test_status=ProductStatus.TESTING.value,
            spend=50.0,
            conversions=1,
            revenue=30.0,
            break_even_roas=3.0,
            kill_threshold_spend=150.0,
            days_testing=1,  # Only 1 day, min is 3
            days_below_broas=3,
        )

        decision = engine.evaluate_product(product)

        # Should NOT be killed yet (days_testing < min_test_duration)
        assert decision is None

    def test_no_kill_under_threshold(self, engine):
        """Product should not be killed when spend is under threshold."""
        product = Product(
            product_id="test4",
            keyword="test product",
            test_status=ProductStatus.TESTING.value,
            spend=50.0,  # Well under 150 threshold
            conversions=0,
            revenue=0,
            break_even_roas=1.5,
            kill_threshold_spend=150.0,
            days_testing=2,
        )

        decision = engine.evaluate_product(product)
        assert decision is None  # Maintain, not kill


class TestWinnerPromotion:
    """Test winner promotion logic."""

    def test_promote_to_winner(self, engine):
        """Product should be promoted when ROAS exceeds bROAS with enough conversions."""
        product = Product(
            product_id="test5",
            keyword="winner product",
            test_status=ProductStatus.TESTING.value,
            spend=100.0,
            conversions=5,  # >= min 3
            revenue=400.0,  # ROAS = 4.0
            break_even_roas=2.0,
            kill_threshold_spend=150.0,
            days_testing=5,  # >= min 3
            selling_price=80.0,
            landed_cost=25.0,
        )

        decision = engine.evaluate_product(product)

        assert decision is not None
        assert decision.new_status == ProductStatus.WINNER.value

    def test_no_promotion_insufficient_conversions(self, engine):
        """Product should not be promoted with too few conversions."""
        product = Product(
            product_id="test6",
            keyword="almost winner",
            test_status=ProductStatus.TESTING.value,
            spend=100.0,
            conversions=1,  # < min 3
            revenue=400.0,
            break_even_roas=2.0,
            kill_threshold_spend=150.0,
            days_testing=5,
        )

        decision = engine.evaluate_product(product)
        assert decision is None  # Maintain

    def test_no_promotion_insufficient_days(self, engine):
        """Product should not be promoted before min test duration."""
        product = Product(
            product_id="test7",
            keyword="early winner",
            test_status=ProductStatus.TESTING.value,
            spend=50.0,
            conversions=5,
            revenue=200.0,
            break_even_roas=2.0,
            kill_threshold_spend=150.0,
            days_testing=1,  # < min 3
        )

        decision = engine.evaluate_product(product)
        assert decision is None


class TestScaleDecisions:
    """Test budget scaling logic for winners."""

    def test_scale_budget(self, engine):
        """Winner should be scaled when above threshold for enough days."""
        product = Product(
            product_id="test8",
            keyword="scaling product",
            test_status=ProductStatus.WINNER.value,
            spend=200.0,
            revenue=800.0,  # ROAS = 4.0
            break_even_roas=2.0,  # Scale threshold = 2.0 * 1.3 = 2.6
            days_below_broas=0,
            consecutive_days_above_scale_threshold=3,  # >= min 2
            days_since_last_scale=5,  # >= min 3
        )

        decision = engine.evaluate_product(product)

        assert decision is not None
        assert "scaled" in decision.action.lower() or "budget" in decision.action.lower()

    def test_no_scale_too_soon(self, engine):
        """Winner should not scale if scaled too recently."""
        product = Product(
            product_id="test9",
            keyword="recent scale",
            test_status=ProductStatus.WINNER.value,
            spend=200.0,
            revenue=800.0,
            break_even_roas=2.0,
            days_below_broas=0,
            consecutive_days_above_scale_threshold=3,
            days_since_last_scale=1,  # Too soon (< 3)
        )

        decision = engine.evaluate_product(product)
        assert decision is None

    def test_pause_winner_below_broas(self, engine):
        """Winner should be paused when below bROAS for too long."""
        product = Product(
            product_id="test10",
            keyword="declining winner",
            test_status=ProductStatus.WINNER.value,
            spend=300.0,
            revenue=200.0,  # ROAS = 0.67, below bROAS
            break_even_roas=2.0,
            days_below_broas=4,  # > max 3
        )

        decision = engine.evaluate_product(product)

        assert decision is not None
        assert decision.new_status == ProductStatus.PAUSED.value


class TestMaintainDecision:
    """Test that products in acceptable range are maintained (no action)."""

    def test_maintain_decent_roas(self, engine):
        """Product at break-even should be maintained (no kill, no promote)."""
        product = Product(
            product_id="test11",
            keyword="ok product",
            test_status=ProductStatus.TESTING.value,
            spend=50.0,
            conversions=2,  # < min 3 for winner
            revenue=100.0,  # ROAS = 2.0, at bROAS
            break_even_roas=2.0,
            kill_threshold_spend=150.0,
            days_testing=3,
            days_below_broas=0,
        )

        decision = engine.evaluate_product(product)
        assert decision is None  # No action = maintain
