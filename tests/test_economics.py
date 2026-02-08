"""
Tests for the Economic Validation Engine.
"""

import pytest
from src.core.config import AppConfig
from src.core.models import Product
from src.economics.validator import EconomicValidator


@pytest.fixture
def config():
    """Create a test config."""
    c = AppConfig()
    c.set("economics.assumed_conversion_rate", 0.01)
    c.set("economics.safety_factor", 1.5)
    c.set("economics.min_gross_margin_pct", 0.30)
    c.set("economics.test_budget_multiplier", 3.0)
    c.set("economics.transaction_fee_pct", 0.02)
    c.set("economics.payment_fee_pct", 0.029)
    c.set("economics.payment_fixed_fee", 0.30)
    c.set("research.min_monthly_search_volume", 500)
    c.set("research.max_competitors", 10)
    c.set("research.min_differentiation_score", 30)
    return c


@pytest.fixture
def validator(config):
    return EconomicValidator(config)


class TestCalculateEconomics:
    """Test the economic calculation engine."""

    def test_basic_calculation(self, validator):
        product = Product(selling_price=49.90, landed_cost=15.00)
        economics = validator.calculate_economics(product)

        assert economics["gross_margin"] == 34.90
        assert economics["gross_margin_pct"] == pytest.approx(0.6994, abs=0.001)
        assert economics["transaction_fees"] > 0
        assert economics["net_margin"] > 0
        assert economics["net_margin_pct"] > 0
        assert economics["break_even_roas"] > 1
        assert economics["target_roas"] > economics["break_even_roas"]
        assert economics["max_allowed_cpc"] > 0
        assert economics["test_budget"] == pytest.approx(149.70, abs=0.01)

    def test_cheap_product(self, validator):
        """EUR 25 product — CPC should be very low."""
        product = Product(selling_price=25.0, landed_cost=15.0)
        economics = validator.calculate_economics(product)

        # Margin is EUR 10
        assert economics["gross_margin"] == 10.0
        # Max CPC should be small
        assert economics["max_allowed_cpc"] < 0.10

    def test_expensive_product(self, validator):
        """EUR 150 product — CPC can be higher."""
        product = Product(selling_price=150.0, landed_cost=80.0)
        economics = validator.calculate_economics(product)

        # Margin is EUR 70
        assert economics["gross_margin"] == 70.0
        # Max CPC should be higher
        assert economics["max_allowed_cpc"] > 0.50

    def test_zero_selling_price(self, validator):
        product = Product(selling_price=0, landed_cost=10.0)
        economics = validator.calculate_economics(product)
        assert economics["gross_margin"] == 0

    def test_transaction_fees(self, validator):
        product = Product(selling_price=50.0, landed_cost=20.0)
        economics = validator.calculate_economics(product)

        # 50 * (0.02 + 0.029) + 0.30 = 50 * 0.049 + 0.30 = 2.45 + 0.30 = 2.75
        assert economics["transaction_fees"] == pytest.approx(2.75, abs=0.01)


class TestValidation:
    """Test the validation logic."""

    def test_product_passes(self, validator):
        product = Product(
            selling_price=49.90,
            landed_cost=15.0,
            monthly_search_volume=1000,
            estimated_cpc=0.05,
            competitor_count=5,
            differentiation_score=50,
        )
        result = validator.validate(product)
        assert result.passed is True

    def test_product_rejected_low_margin(self, validator):
        product = Product(
            selling_price=25.0,
            landed_cost=20.0,  # Only EUR 5 margin = 20%
            monthly_search_volume=1000,
            estimated_cpc=0.05,
            competitor_count=5,
            differentiation_score=50,
        )
        result = validator.validate(product)
        assert result.passed is False
        assert "margin" in result.reason.lower()

    def test_product_rejected_high_cpc(self, validator):
        product = Product(
            selling_price=25.0,
            landed_cost=10.0,
            monthly_search_volume=1000,
            estimated_cpc=5.0,  # Way too high for EUR 25 product
            competitor_count=5,
            differentiation_score=50,
        )
        result = validator.validate(product)
        assert result.passed is False
        assert "cpc" in result.reason.lower()

    def test_product_rejected_low_volume(self, validator):
        product = Product(
            selling_price=49.90,
            landed_cost=15.0,
            monthly_search_volume=100,  # Too low
            estimated_cpc=0.05,
            competitor_count=5,
            differentiation_score=50,
        )
        result = validator.validate(product)
        assert result.passed is False
        assert "volume" in result.reason.lower()

    def test_product_rejected_too_many_competitors(self, validator):
        product = Product(
            selling_price=49.90,
            landed_cost=15.0,
            monthly_search_volume=1000,
            estimated_cpc=0.05,
            competitor_count=15,  # Too many
            differentiation_score=50,
        )
        result = validator.validate(product)
        assert result.passed is False
        assert "competitor" in result.reason.lower()

    def test_product_no_landed_cost(self, validator):
        product = Product(
            selling_price=49.90,
            landed_cost=0,  # Not filled yet
        )
        result = validator.validate(product)
        assert result.passed is False
        assert "landed cost" in result.reason.lower()

    def test_multiple_rejection_reasons(self, validator):
        """Product that fails multiple criteria should list all reasons."""
        product = Product(
            selling_price=25.0,
            landed_cost=20.0,  # Low margin
            monthly_search_volume=100,  # Low volume
            estimated_cpc=5.0,  # High CPC
            competitor_count=15,  # Too many competitors
            differentiation_score=10,  # Low differentiation
        )
        result = validator.validate(product)
        assert result.passed is False
        # Should contain multiple reasons separated by semicolons
        assert ";" in result.reason


class TestNetProfit:
    """Test net profit calculations."""

    def test_profitable_product(self, validator):
        product = Product(
            selling_price=50.0,
            landed_cost=15.0,
            revenue=500.0,  # 10 sales
            spend=100.0,
            conversions=10,
        )
        profit = validator.calculate_net_profit(product)
        # Revenue: 500
        # Product cost: 15 * 10 = 150
        # Transaction fees: (50 * 0.049 + 0.30) * 10 = 27.5
        # Ad spend: 100
        # Net: 500 - 150 - 27.5 - 100 = 222.50
        assert profit > 0
        assert profit == pytest.approx(222.50, abs=1.0)

    def test_unprofitable_product(self, validator):
        product = Product(
            selling_price=25.0,
            landed_cost=15.0,
            revenue=25.0,  # 1 sale
            spend=75.0,  # Spent 3x selling price
            conversions=1,
        )
        profit = validator.calculate_net_profit(product)
        assert profit < 0

    def test_no_conversions(self, validator):
        product = Product(
            selling_price=50.0,
            landed_cost=15.0,
            revenue=0,
            spend=50.0,
            conversions=0,
        )
        profit = validator.calculate_net_profit(product)
        assert profit == -50.0
