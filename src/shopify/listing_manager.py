"""
Shopify product listing management via Admin API.
Auto-creates basic product listings for testing, updates/deletes as needed.
"""

from __future__ import annotations

import base64
import logging
import time
from typing import Optional

import requests

from src.core.config import AppConfig, SHOPIFY_SHOP_URL, SHOPIFY_ACCESS_TOKEN
from src.core.interfaces import ProductListingService
from src.core.models import Product

logger = logging.getLogger(__name__)


class ShopifyListingManager(ProductListingService):
    """
    Manages product listings on Shopify via the Admin REST API.
    """

    def __init__(self, config: AppConfig = None):
        self.config = config or AppConfig()
        self.shop_url = SHOPIFY_SHOP_URL
        self.access_token = SHOPIFY_ACCESS_TOKEN
        self.api_version = "2024-10"
        self.base_url = f"https://{self.shop_url}/admin/api/{self.api_version}"

    def _headers(self) -> dict:
        return {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
        }

    def create_listing(
        self,
        product: Product,
        title: str,
        description_html: str,
        images: list[bytes],
        price: float,
        meta_title: str = "",
        meta_description: str = "",
        tags: str = "",
        product_type: str = "",
        key_features_html: str = "",
    ) -> Optional[dict]:
        """
        Create a product listing on Shopify.

        Args:
            product: Product model
            title: Product title
            description_html: HTML product description (body_html)
            images: List of image bytes
            price: Product price
            meta_title: SEO meta title
            meta_description: SEO meta description
            tags: Comma-separated tags
            product_type: Product type/category
            key_features_html: HTML bullet points for above-CTA metafield

        Returns:
            dict with shopify_product_id and shopify_product_url, or None
        """
        try:
            # Build the product payload
            payload = {
                "product": {
                    "title": title,
                    "body_html": description_html,
                    "product_type": product_type,
                    "tags": tags,
                    "status": "draft",  # Created as draft — publish manually when ready
                    "variants": [
                        {
                            "price": str(price),
                            "inventory_management": "shopify",
                            "inventory_quantity": 999,  # High stock for dropshipping
                            "requires_shipping": True,
                            "sku": f"QOV-{product.product_id}",
                        }
                    ],
                    "metafields_global_title_tag": meta_title,
                    "metafields_global_description_tag": meta_description,
                }
            }

            # Add images
            if images:
                payload["product"]["images"] = []
                for i, img_data in enumerate(images):
                    b64_img = base64.b64encode(img_data).decode("utf-8")
                    payload["product"]["images"].append({
                        "attachment": b64_img,
                        "filename": f"{product.product_id}_{i}.png",
                        "position": i + 1,
                    })

            # Create the product
            response = requests.post(
                f"{self.base_url}/products.json",
                json=payload,
                headers=self._headers(),
                timeout=60,
            )
            response.raise_for_status()

            result = response.json()
            shopify_product = result.get("product", {})
            shopify_id = str(shopify_product.get("id", ""))

            # Build the product URL
            handle = shopify_product.get("handle", "")
            product_url = f"https://{self.shop_url.replace('.myshopify.com', '')}.com/products/{handle}"

            logger.info(
                "Created Shopify listing: %s (ID: %s)",
                title[:50], shopify_id
            )

            # Set key_features metafield (displayed above CTA by theme)
            if key_features_html and shopify_id:
                self.set_product_metafield(
                    shopify_id,
                    namespace="custom",
                    key="key_features",
                    value=key_features_html,
                    value_type="multi_line_text_field",
                )

            return {
                "shopify_product_id": shopify_id,
                "shopify_product_url": product_url,
                "handle": handle,
            }

        except requests.exceptions.HTTPError as e:
            logger.error(
                "Shopify API error creating listing: %s - %s",
                e.response.status_code,
                e.response.text[:500] if e.response else "",
            )
            return None
        except Exception as e:
            logger.error("Failed to create Shopify listing: %s", e)
            return None

    def update_listing(self, listing_id: str, updates: dict) -> bool:
        """
        Update an existing Shopify product listing.

        Args:
            listing_id: Shopify product ID
            updates: Dict of fields to update (title, body_html, price, etc.)

        Returns:
            True if successful
        """
        try:
            payload = {"product": {"id": int(listing_id)}}

            # Map common fields
            field_mapping = {
                "title": "title",
                "description_html": "body_html",
                "tags": "tags",
                "product_type": "product_type",
                "status": "status",
            }

            for key, shopify_key in field_mapping.items():
                if key in updates:
                    payload["product"][shopify_key] = updates[key]

            # Handle price update (on variant)
            if "price" in updates:
                # Need to get current variant ID first
                variant_id = self._get_first_variant_id(listing_id)
                if variant_id:
                    self._update_variant_price(variant_id, updates["price"])

            response = requests.put(
                f"{self.base_url}/products/{listing_id}.json",
                json=payload,
                headers=self._headers(),
                timeout=30,
            )
            response.raise_for_status()

            logger.info("Updated Shopify listing: %s", listing_id)
            return True

        except Exception as e:
            logger.error("Failed to update Shopify listing %s: %s", listing_id, e)
            return False

    def delete_listing(self, listing_id: str) -> bool:
        """Delete a Shopify product listing."""
        try:
            response = requests.delete(
                f"{self.base_url}/products/{listing_id}.json",
                headers=self._headers(),
                timeout=30,
            )
            response.raise_for_status()
            logger.info("Deleted Shopify listing: %s", listing_id)
            return True
        except Exception as e:
            logger.error("Failed to delete Shopify listing %s: %s", listing_id, e)
            return False

    def add_images(self, listing_id: str, images: list[bytes]) -> bool:
        """Add images to an existing product listing."""
        try:
            for i, img_data in enumerate(images):
                b64_img = base64.b64encode(img_data).decode("utf-8")
                payload = {
                    "image": {
                        "attachment": b64_img,
                        "filename": f"product_{listing_id}_{i}.png",
                    }
                }

                response = requests.post(
                    f"{self.base_url}/products/{listing_id}/images.json",
                    json=payload,
                    headers=self._headers(),
                    timeout=60,
                )
                response.raise_for_status()
                time.sleep(0.5)  # Rate limiting

            logger.info("Added %d images to listing %s", len(images), listing_id)
            return True

        except Exception as e:
            logger.error("Failed to add images to listing %s: %s", listing_id, e)
            return False

    def set_product_metafield(
        self, listing_id: str, namespace: str, key: str, value: str, value_type: str = "single_line_text_field"
    ) -> bool:
        """Set a metafield on a product (useful for custom_label_0 sync)."""
        try:
            payload = {
                "metafield": {
                    "namespace": namespace,
                    "key": key,
                    "value": value,
                    "type": value_type,
                }
            }

            response = requests.post(
                f"{self.base_url}/products/{listing_id}/metafields.json",
                json=payload,
                headers=self._headers(),
                timeout=30,
            )
            response.raise_for_status()
            return True

        except Exception as e:
            logger.error("Failed to set metafield on %s: %s", listing_id, e)
            return False

    def _get_first_variant_id(self, listing_id: str) -> Optional[str]:
        """Get the first variant ID of a product."""
        try:
            response = requests.get(
                f"{self.base_url}/products/{listing_id}/variants.json",
                headers=self._headers(),
                timeout=30,
            )
            response.raise_for_status()
            variants = response.json().get("variants", [])
            return str(variants[0]["id"]) if variants else None
        except Exception:
            return None

    def _update_variant_price(self, variant_id: str, price: float) -> bool:
        """Update the price of a variant."""
        try:
            payload = {"variant": {"id": int(variant_id), "price": str(price)}}
            response = requests.put(
                f"{self.base_url}/variants/{variant_id}.json",
                json=payload,
                headers=self._headers(),
                timeout=30,
            )
            response.raise_for_status()
            return True
        except Exception:
            return False
