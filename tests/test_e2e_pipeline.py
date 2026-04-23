"""
End-to-end smoke test for the product lifecycle.

Drives a product through the real state machine (SOURCING → READY_TO_TEST
or REJECTED → LISTING_CREATED) using the `InMemoryDataStore`, so all
business logic runs — but no Sheets, no Shopify, no OpenAI/AliExpress
calls. External integrations are replaced with `unittest.mock` doubles
at the call site.

What this covers that unit tests don't:

- The validator's `process_product_with_cost` path, including the
  downstream update + log + notification persistence.
- The `JobScheduler._process_single_product` branch that picks up a
  READY_TO_TEST product, asks for content + images, creates a Shopify
  listing, and flips status to LISTING_CREATED.
- The interplay between `DataStore.update_product` and
  `DataStore.get_products(status=...)` — the same queries the real
  scheduler uses when it polls for work.

Intentionally *not* covered: keyword research, SerpAPI, AliExpress
matching, agent task queue. Those have their own unit tests; wiring
them together would require real network access.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.core.config import AppConfig
from src.core.memory_store import InMemoryDataStore
from src.core.models import (
    ActionType,
    Product,
    ProductStatus,
)
from src.economics.validator import EconomicValidator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    """AppConfig with the economic thresholds the validator checks against."""
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
def store():
    return InMemoryDataStore()


def _sourcing_product(**overrides) -> Product:
    """Build a realistic product that has reached the sourcing stage."""
    base = dict(
        keyword="cooling pillow",
        country="DE",
        language="de",
        selling_price=49.90,
        monthly_search_volume=1200,
        estimated_cpc=0.05,
        competitor_count=5,
        differentiation_score=60,
        aliexpress_url="https://aliexpress.com/item/123.html",
        aliexpress_image_urls="https://cdn.example/a.jpg,https://cdn.example/b.jpg",
        test_status=ProductStatus.SOURCING.value,
    )
    base.update(overrides)
    return Product(**base)


# ---------------------------------------------------------------------------
# Stage 1: agent fills landed_cost → economics validator runs
# ---------------------------------------------------------------------------

class TestEconomicsTransition:
    """Once the agent fills landed_cost, validator must promote or reject."""

    def test_passing_product_moves_to_ready_to_test(self, store, config):
        product = _sourcing_product(landed_cost=15.0)
        store.add_product(product)

        validator = EconomicValidator(config)
        validator.process_product_with_cost(product, store)

        persisted = store.get_product(product.product_id)
        assert persisted.test_status == ProductStatus.READY_TO_TEST.value

        # Economics must be filled in, not left zeroed out
        assert persisted.gross_margin == pytest.approx(34.90, abs=0.01)
        assert persisted.net_margin > 0
        assert persisted.break_even_roas > 1
        assert persisted.max_allowed_cpc > 0
        assert persisted.test_budget == pytest.approx(149.70, abs=0.01)

        # Single log entry with the right action type
        logs = store.get_logs(product_id=product.product_id)
        assert len(logs) == 1
        assert logs[0].action_type == ActionType.ECONOMICS_PASSED.value
        assert logs[0].new_status == ProductStatus.READY_TO_TEST.value

        # Dashboard notification created
        notes = store.get_notifications()
        assert any(n.product_id == product.product_id for n in notes)

    def test_low_margin_product_is_rejected(self, store, config):
        # 25 selling, 20 landed → 20% gross < 30% threshold
        product = _sourcing_product(
            selling_price=25.0,
            landed_cost=20.0,
            keyword="budget pillow",
        )
        store.add_product(product)

        validator = EconomicValidator(config)
        validator.process_product_with_cost(product, store)

        persisted = store.get_product(product.product_id)
        assert persisted.test_status == ProductStatus.REJECTED.value
        assert "margin" in persisted.reason.lower()

        logs = store.get_logs(product_id=product.product_id)
        assert logs[0].action_type == ActionType.ECONOMICS_FAILED.value

    def test_missing_landed_cost_is_rejected(self, store, config):
        """Safety net: validator must not auto-pass a product with 0 cost."""
        product = _sourcing_product(landed_cost=0.0)
        store.add_product(product)

        validator = EconomicValidator(config)
        validator.process_product_with_cost(product, store)

        persisted = store.get_product(product.product_id)
        assert persisted.test_status == ProductStatus.REJECTED.value
        assert "landed cost" in persisted.reason.lower()


# ---------------------------------------------------------------------------
# Stage 2: ready-to-test product picked up by scheduler → listing created
# ---------------------------------------------------------------------------

class TestListingCreation:
    """`JobScheduler._process_single_product` should flip READY_TO_TEST → LISTING_CREATED."""

    def test_ready_product_becomes_listing_created(self, store, config):
        # Seed a product already past economics validation.
        product = _sourcing_product(
            landed_cost=15.0,
            test_status=ProductStatus.READY_TO_TEST.value,
            gross_margin=34.90,
            net_margin=30.0,
            net_margin_pct=0.60,
        )
        store.add_product(product)

        # Mocks for the three external services touched during processing.
        image_gen = MagicMock()
        image_gen.generate_product_images.return_value = [
            {"image_data": b"fake-bytes-1"},
            {"image_data": b"fake-bytes-2"},
        ]

        shopify = MagicMock()
        shopify.create_listing.return_value = {
            "shopify_product_id": "gid://shopify/Product/999",
            "shopify_product_url": "https://example.myshopify.com/products/cooling-pillow",
        }

        fake_content = {
            "title": "CoolRest Pro — cooling pillow",
            "description_html": "<p>Stays cool all night.</p>",
            "meta_title": "CoolRest Pro",
            "meta_description": "Cooling pillow for hot sleepers.",
            "tags": "pillow, cooling",
            "product_type": "Home & Living",
        }

        # Patch the content generator at the exact import site the scheduler uses.
        with patch(
            "src.scheduler.jobs.generate_product_content",
            return_value=fake_content,
        ), patch("src.scheduler.jobs.AIImageGenerator"), patch(
            "src.scheduler.jobs.ShopifyListingManager"
        ):
            # Import locally so the patches are in place before class construction.
            from src.scheduler.jobs import JobScheduler

            scheduler = JobScheduler(store, config)

            # Drive one product rather than the whole cron loop.
            scheduler._process_single_product(product, image_gen, shopify)

        persisted = store.get_product(product.product_id)
        assert persisted.test_status == ProductStatus.LISTING_CREATED.value
        assert persisted.shopify_product_id == "gid://shopify/Product/999"
        assert persisted.shopify_product_url.endswith("cooling-pillow")

        # Content + listing calls happened with the right shape
        image_gen.generate_product_images.assert_called_once()
        shopify.create_listing.assert_called_once()
        _, kwargs = shopify.create_listing.call_args
        assert kwargs["title"] == fake_content["title"]
        assert kwargs["price"] == pytest.approx(49.90, abs=0.01)
        assert len(kwargs["images"]) == 2

        # Lifecycle log entry written
        logs = store.get_logs(product_id=product.product_id)
        assert any(
            l.action_type == ActionType.LISTING_CREATED.value
            and l.new_status == ProductStatus.LISTING_CREATED.value
            for l in logs
        )

    def test_listing_failure_leaves_status_unchanged(self, store, config):
        """If Shopify returns None the product must not be flipped forward."""
        product = _sourcing_product(
            landed_cost=15.0,
            test_status=ProductStatus.READY_TO_TEST.value,
        )
        store.add_product(product)

        image_gen = MagicMock()
        image_gen.generate_product_images.return_value = [{"image_data": b"x"}]
        shopify = MagicMock()
        shopify.create_listing.return_value = None  # simulates upstream failure

        fake_content = {
            "title": "t",
            "description_html": "<p>x</p>",
            "meta_title": "",
            "meta_description": "",
            "tags": "",
            "product_type": "",
        }

        with patch(
            "src.scheduler.jobs.generate_product_content",
            return_value=fake_content,
        ), patch("src.scheduler.jobs.AIImageGenerator"), patch(
            "src.scheduler.jobs.ShopifyListingManager"
        ):
            from src.scheduler.jobs import JobScheduler

            scheduler = JobScheduler(store, config)
            scheduler._process_single_product(product, image_gen, shopify)

        persisted = store.get_product(product.product_id)
        assert persisted.test_status == ProductStatus.READY_TO_TEST.value
        # No LISTING_CREATED log should have been written
        logs = store.get_logs(product_id=product.product_id)
        assert not any(
            l.action_type == ActionType.LISTING_CREATED.value for l in logs
        )


# ---------------------------------------------------------------------------
# Stage 3: full SOURCING → LISTING_CREATED walk
# ---------------------------------------------------------------------------

class TestFullLifecycle:
    """Glue test: walk one product all the way through."""

    def test_product_walks_sourcing_to_listing_created(self, store, config):
        product = _sourcing_product()
        store.add_product(product)
        pid = product.product_id

        # 1. Agent fills in landed cost (would be a write from the tasks tab)
        store.update_product(pid, {"landed_cost": 15.0})

        # 2. Scheduler's cost-polling job picks this up: it reads products
        #    where landed_cost > 0 and status == SOURCING, then runs the
        #    validator on each. Simulate that polling query.
        awaiting = store.get_products_awaiting_cost()
        assert len(awaiting) == 1
        assert awaiting[0].product_id == pid

        validator = EconomicValidator(config)
        validator.process_product_with_cost(awaiting[0], store)

        # Status must now be READY_TO_TEST so the next job sees it.
        assert (
            store.get_product(pid).test_status
            == ProductStatus.READY_TO_TEST.value
        )

        # 3. The "process ready products" job queries by status and
        #    picks this one up for listing creation.
        ready = store.get_products(status=ProductStatus.READY_TO_TEST.value)
        assert len(ready) == 1

        image_gen = MagicMock()
        image_gen.generate_product_images.return_value = [{"image_data": b"img"}]
        shopify = MagicMock()
        shopify.create_listing.return_value = {
            "shopify_product_id": "42",
            "shopify_product_url": "https://example.myshopify.com/products/cooling-pillow",
        }
        fake_content = {
            "title": "Cooling pillow",
            "description_html": "<p>Sleep cool.</p>",
            "meta_title": "",
            "meta_description": "",
            "tags": "",
            "product_type": "",
        }

        with patch(
            "src.scheduler.jobs.generate_product_content",
            return_value=fake_content,
        ), patch("src.scheduler.jobs.AIImageGenerator"), patch(
            "src.scheduler.jobs.ShopifyListingManager"
        ):
            from src.scheduler.jobs import JobScheduler

            scheduler = JobScheduler(store, config)
            scheduler._process_single_product(ready[0], image_gen, shopify)

        # Final state
        final = store.get_product(pid)
        assert final.test_status == ProductStatus.LISTING_CREATED.value
        assert final.shopify_product_id == "42"

        # Full audit trail captured
        log_types = [l.action_type for l in store.get_logs(product_id=pid)]
        assert ActionType.ECONOMICS_PASSED.value in log_types
        assert ActionType.LISTING_CREATED.value in log_types
