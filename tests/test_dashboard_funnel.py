"""
Tests for the home-page pipeline funnel aggregator.

The funnel must be monotone non-increasing: every item in stage N+1 is also
in stage N. If this ever breaks, the visual funnel renders with a bar
taller than the one before it, which is confusing and wrong.
"""

from __future__ import annotations

import pytest

from dashboard.components.widgets import compute_funnel_counts
from src.core.models import KeywordResearch, Product, ProductStatus


def _p(**kw) -> Product:
    defaults = dict(keyword="kw", selling_price=30.0, landed_cost=0.0)
    defaults.update(kw)
    return Product(**defaults)


def _k(i: int) -> KeywordResearch:
    return KeywordResearch(keyword=f"kw-{i}", country="DE")


class TestFunnelCounts:
    def test_empty_input_returns_six_zeros(self):
        stages = compute_funnel_counts([], [])
        assert len(stages) == 6
        assert all(s["count"] == 0 for s in stages)

    def test_keywords_only_populates_first_stage(self):
        keywords = [_k(i) for i in range(17)]
        stages = compute_funnel_counts(keywords, [])
        counts = [s["count"] for s in stages]
        assert counts == [17, 0, 0, 0, 0, 0]

    def test_funnel_is_monotone_non_increasing(self):
        """The core invariant: no stage can exceed the one before it."""
        products = [
            _p(test_status=ProductStatus.DISCOVERED.value),
            _p(test_status=ProductStatus.SOURCING.value, landed_cost=12.0),
            _p(test_status=ProductStatus.READY_TO_TEST.value, landed_cost=10.0),
            _p(test_status=ProductStatus.LISTING_CREATED.value, landed_cost=10.0),
            _p(test_status=ProductStatus.TESTING.value, landed_cost=10.0),
            _p(test_status=ProductStatus.WINNER.value, landed_cost=10.0),
            _p(test_status=ProductStatus.KILLED.value, landed_cost=10.0),
            _p(test_status=ProductStatus.REJECTED.value, landed_cost=9.0),
        ]
        stages = compute_funnel_counts([_k(i) for i in range(20)], products)
        counts = [s["count"] for s in stages]

        for prev, cur in zip(counts, counts[1:]):
            assert cur <= prev, f"funnel widened at step: {counts}"

    def test_stage_membership_semantics(self):
        products = [
            # No cost, sitting in sourcing
            _p(test_status=ProductStatus.SOURCING.value),
            # Has cost but rejected — counts for stage 3 but NOT stage 4
            _p(test_status=ProductStatus.REJECTED.value, landed_cost=15.0),
            # Ready to test: passes economics, no listing yet
            _p(test_status=ProductStatus.READY_TO_TEST.value, landed_cost=15.0),
            # Listing created, not yet in ads
            _p(test_status=ProductStatus.LISTING_CREATED.value, landed_cost=15.0),
            # Fully live
            _p(test_status=ProductStatus.TESTING.value, landed_cost=15.0),
            _p(test_status=ProductStatus.WINNER.value, landed_cost=15.0),
        ]
        stages = compute_funnel_counts([_k(i) for i in range(10)], products)
        counts = [s["count"] for s in stages]

        assert counts == [
            10,  # keywords
            6,   # all products
            5,   # cost received (everyone except the sourcing-only one)
            4,   # profit-validated (ready_to_test + listing + testing + winner)
            3,   # listing created or later
            2,   # in ads
        ]

    def test_rejected_product_with_cost_does_not_count_as_profit_validated(self):
        """A rejected product had its cost filled in but failed validation —
        must stop at stage 3 (cost received), not reach stage 4."""
        products = [
            _p(test_status=ProductStatus.REJECTED.value, landed_cost=22.0)
        ]
        stages = compute_funnel_counts([], products)
        counts = [s["count"] for s in stages]
        assert counts[2] == 1  # cost received
        assert counts[3] == 0  # NOT profit-validated

    def test_stage_labels_stable(self):
        """Labels are part of the dashboard contract; don't rename them silently."""
        stages = compute_funnel_counts([], [])
        labels = [s["label"] for s in stages]
        assert labels == [
            "Keywords researched",
            "Products sourced",
            "Agent cost received",
            "Profit-validated",
            "Pages created",
            "In ads",
        ]
