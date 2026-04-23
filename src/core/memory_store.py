"""
In-memory DataStore implementation.

Implements the same `DataStore` interface as the Google Sheets backend so
business logic can be exercised without any network or credentials. Used
for:

- Unit / integration tests (`tests/test_e2e_pipeline.py`)
- Local smoke runs when you don't want to touch the shared Sheet
- A reference implementation for future SaaS backends (Postgres, etc.)
  so there is a minimal, known-correct comparison point

The store is intentionally simple: dicts keyed by `product_id` /
`keyword_id` / `log_id`, plus a config dict and a feedback dict. No
ordering guarantees beyond insertion order (Python 3.7+ dict semantics).
"""

from __future__ import annotations

import copy
from dataclasses import fields
from typing import Optional

from src.core.interfaces import DataStore
from src.core.models import (
    ActionLog,
    CountryConfig,
    KeywordResearch,
    Notification,
    Product,
    ProductStatus,
)


class InMemoryDataStore(DataStore):
    """Dict-backed DataStore. Everything lives in the process."""

    def __init__(self, countries: Optional[list[CountryConfig]] = None):
        self._keywords: dict[str, KeywordResearch] = {}
        self._products: dict[str, Product] = {}
        self._logs: list[ActionLog] = []
        self._notifications: list[Notification] = []
        self._config: dict = {}
        self._feedback: dict = {}
        self._countries: list[CountryConfig] = countries or [
            CountryConfig(code="DE", name="Germany", language="de", currency="EUR"),
        ]

    # ------------------------------------------------------------------
    # Keywords
    # ------------------------------------------------------------------
    def get_keywords(
        self, country: str = None, status: str = None
    ) -> list[KeywordResearch]:
        # `status` is accepted for interface compatibility; KeywordResearch
        # has no status field, so the filter is ignored here.
        out = list(self._keywords.values())
        if country:
            out = [k for k in out if k.country == country]
        return out

    def add_keyword(self, keyword: KeywordResearch) -> None:
        self._keywords[keyword.keyword_id] = copy.deepcopy(keyword)

    def update_keyword(self, keyword_id: str, updates: dict) -> None:
        kw = self._keywords.get(keyword_id)
        if not kw:
            return
        valid = {f.name for f in fields(KeywordResearch)}
        for k, v in updates.items():
            if k in valid:
                setattr(kw, k, v)

    def keyword_exists(self, keyword: str, country: str) -> bool:
        k_lower = (keyword or "").strip().lower()
        return any(
            (kw.keyword or "").strip().lower() == k_lower and kw.country == country
            for kw in self._keywords.values()
        )

    # ------------------------------------------------------------------
    # Products
    # ------------------------------------------------------------------
    def get_products(
        self, country: str = None, status: str = None
    ) -> list[Product]:
        out = list(self._products.values())
        if country:
            out = [p for p in out if p.country == country]
        if status:
            out = [p for p in out if p.test_status == status]
        return out

    def get_product(self, product_id: str) -> Optional[Product]:
        return self._products.get(product_id)

    def add_product(self, product: Product) -> None:
        self._products[product.product_id] = copy.deepcopy(product)

    def update_product(self, product_id: str, updates: dict) -> None:
        p = self._products.get(product_id)
        if not p:
            return
        valid = {f.name for f in fields(Product)}
        for k, v in updates.items():
            if k in valid:
                setattr(p, k, v)

    def get_products_awaiting_cost(self) -> list[Product]:
        return [
            p
            for p in self._products.values()
            if p.test_status == ProductStatus.SOURCING.value
            and float(p.landed_cost or 0) > 0
        ]

    # ------------------------------------------------------------------
    # Logs
    # ------------------------------------------------------------------
    def add_log(self, log: ActionLog) -> None:
        self._logs.append(copy.deepcopy(log))

    def get_logs(self, product_id: str = None, limit: int = 100) -> list[ActionLog]:
        logs = list(self._logs)
        if product_id:
            logs = [l for l in logs if l.product_id == product_id]
        # Newest first, matching Sheets manager semantics
        logs.reverse()
        return logs[:limit]

    # ------------------------------------------------------------------
    # Notifications
    # ------------------------------------------------------------------
    def add_notification(self, notification: Notification) -> None:
        self._notifications.append(copy.deepcopy(notification))

    def get_notifications(
        self, unread_only: bool = False, limit: int = 50
    ) -> list[Notification]:
        notes = list(self._notifications)
        if unread_only:
            notes = [n for n in notes if not n.read]
        notes.reverse()
        return notes[:limit]

    def mark_notification_read(self, notification_id: str) -> None:
        for n in self._notifications:
            if n.notification_id == notification_id:
                n.read = True
                return

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------
    def get_config(self) -> dict:
        return copy.deepcopy(self._config)

    def save_config(self, config: dict) -> None:
        self._config = copy.deepcopy(config)

    def get_countries(self) -> list[CountryConfig]:
        return list(self._countries)

    # ------------------------------------------------------------------
    # Research feedback
    # ------------------------------------------------------------------
    def get_research_feedback(self) -> dict:
        return copy.deepcopy(self._feedback)

    def save_research_feedback(self, feedback: dict) -> None:
        self._feedback = copy.deepcopy(feedback)
