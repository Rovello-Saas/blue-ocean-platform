"""
Abstract interfaces for SaaS-ready architecture.
All data access goes through these interfaces so the storage backend
can be swapped from Google Sheets to PostgreSQL without changing business logic.
"""

from abc import ABC, abstractmethod
from typing import Optional

from src.core.models import (
    Product, KeywordResearch, ActionLog, Notification, CountryConfig
)


class DataStore(ABC):
    """
    Abstract data store interface.
    Currently implemented by Google Sheets. Can be swapped to PostgreSQL for SaaS.
    """

    # --- Keywords ---
    @abstractmethod
    def get_keywords(self, country: str = None, status: str = None) -> list[KeywordResearch]:
        """Get keyword research results, optionally filtered."""
        pass

    @abstractmethod
    def add_keyword(self, keyword: KeywordResearch) -> None:
        """Add a new keyword research result."""
        pass

    def add_keywords_bulk(self, keywords: list[KeywordResearch]) -> None:
        """Add many keywords in one go. Default falls back to a per-item loop;
        backends can override for a single-round-trip write (e.g. Sheets
        `append_rows`) — important to stay under rate-limit quotas when a
        pipeline run adds dozens of rows at once."""
        for kw in keywords:
            self.add_keyword(kw)

    @abstractmethod
    def update_keyword(self, keyword_id: str, updates: dict) -> None:
        """Update fields on a keyword."""
        pass

    @abstractmethod
    def keyword_exists(self, keyword: str, country: str) -> bool:
        """Check if a keyword already exists for a country (deduplication)."""
        pass

    # --- Products ---
    @abstractmethod
    def get_products(self, country: str = None, status: str = None) -> list[Product]:
        """Get products, optionally filtered by country and/or status."""
        pass

    @abstractmethod
    def get_product(self, product_id: str) -> Optional[Product]:
        """Get a single product by ID."""
        pass

    @abstractmethod
    def add_product(self, product: Product) -> None:
        """Add a new product."""
        pass

    def add_products_bulk(self, products: list[Product]) -> None:
        """Add many products in one go. Default falls back to a per-item
        loop — backends should override with a batched write."""
        for p in products:
            self.add_product(p)

    @abstractmethod
    def update_product(self, product_id: str, updates: dict) -> None:
        """Update fields on a product."""
        pass

    @abstractmethod
    def get_products_awaiting_cost(self) -> list[Product]:
        """Get products in 'sourcing' status where landed_cost has been filled."""
        pass

    # --- Action Log ---
    @abstractmethod
    def add_log(self, log: ActionLog) -> None:
        """Add an action log entry."""
        pass

    def add_logs_bulk(self, logs: list[ActionLog]) -> None:
        """Bulk insert action logs. Default falls back to per-item loop."""
        for log in logs:
            self.add_log(log)

    @abstractmethod
    def get_logs(self, product_id: str = None, limit: int = 100) -> list[ActionLog]:
        """Get action logs, optionally filtered by product."""
        pass

    # --- Notifications ---
    @abstractmethod
    def add_notification(self, notification: Notification) -> None:
        """Add a dashboard notification."""
        pass

    @abstractmethod
    def get_notifications(self, unread_only: bool = False, limit: int = 50) -> list[Notification]:
        """Get notifications."""
        pass

    @abstractmethod
    def mark_notification_read(self, notification_id: str) -> None:
        """Mark a notification as read."""
        pass

    # --- Config ---
    @abstractmethod
    def get_config(self) -> dict:
        """Get the full configuration dictionary."""
        pass

    @abstractmethod
    def save_config(self, config: dict) -> None:
        """Save configuration to the store."""
        pass

    @abstractmethod
    def get_countries(self) -> list[CountryConfig]:
        """Get configured countries."""
        pass

    # --- Research Feedback ---
    @abstractmethod
    def get_research_feedback(self) -> dict:
        """Get accumulated research feedback for LLM improvement."""
        pass

    @abstractmethod
    def save_research_feedback(self, feedback: dict) -> None:
        """Save updated research feedback."""
        pass


class NotificationDispatcher(ABC):
    """
    Abstract notification dispatcher.
    Currently dashboard-only. Can add email/Slack/Telegram handlers for SaaS.
    """

    @abstractmethod
    def send(self, notification: Notification) -> None:
        """Send a notification through this channel."""
        pass


class ImageGenerator(ABC):
    """Abstract interface for image generation."""

    @abstractmethod
    def generate_product_images(
        self,
        reference_image_urls: list[str],
        product_description: str,
        target_language: str = "de",
        num_images: int = 4
    ) -> list[bytes]:
        """Generate unique product images from reference images."""
        pass


class ProductListingService(ABC):
    """Abstract interface for e-commerce product listing creation."""

    @abstractmethod
    def create_listing(
        self,
        product: Product,
        title: str,
        description_html: str,
        images: list[bytes],
        price: float
    ) -> dict:
        """Create a product listing. Returns dict with listing ID and URL."""
        pass

    @abstractmethod
    def update_listing(self, listing_id: str, updates: dict) -> None:
        """Update an existing listing."""
        pass

    @abstractmethod
    def delete_listing(self, listing_id: str) -> None:
        """Delete a product listing."""
        pass
