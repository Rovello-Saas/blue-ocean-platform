"""
AliExpress Affiliate API integration.
Searches for matching products and extracts pricing, images, ratings, and URLs.
"""

from __future__ import annotations

import logging
import hashlib
import hmac
import time
import json
from typing import Optional
from urllib.parse import urlencode

import requests

from src.core.config import (
    AppConfig,
    ALIEXPRESS_APP_KEY,
    ALIEXPRESS_APP_SECRET,
    ALIEXPRESS_TRACKING_ID,
)

logger = logging.getLogger(__name__)

# AliExpress API endpoints
API_BASE_URL = "https://api-sg.aliexpress.com/sync"


def _sign_request(params: dict, secret: str) -> str:
    """Generate HMAC-SHA256 signature for AliExpress API request."""
    sorted_params = sorted(params.items())
    sign_str = "".join(f"{k}{v}" for k, v in sorted_params)
    sign_str = secret + sign_str + secret
    return hmac.new(
        secret.encode("utf-8"),
        sign_str.encode("utf-8"),
        hashlib.sha256
    ).hexdigest().upper()


def search_products(
    keyword: str,
    country: str = "DE",
    language: str = "de",
    min_rating: float = 4.5,
    min_orders: int = 500,
    max_results: int = 10,
    config: AppConfig = None,
) -> list[dict]:
    """
    Search AliExpress for products matching a keyword.

    Args:
        keyword: Search keyword
        country: Target country for shipping/pricing
        language: Result language
        min_rating: Minimum product rating filter
        min_orders: Minimum order count filter
        max_results: Maximum number of results to return
        config: App configuration

    Returns:
        List of product dicts with:
            product_id, title, url, price, rating, orders,
            image_urls, shipping_cost
    """
    config = config or AppConfig()

    if not ALIEXPRESS_APP_KEY or ALIEXPRESS_APP_KEY.startswith("your_"):
        logger.warning("AliExpress API credentials not configured — skipping product search")
        return []

    # AliExpress country mapping for shipping
    ship_to_map = {
        "DE": "DE", "NL": "NL", "AT": "AT", "FR": "FR",
        "BE": "BE", "CH": "CH", "ES": "ES", "IT": "IT",
        "PL": "PL", "GB": "GB", "US": "US",
    }

    # Language mapping
    lang_map = {
        "de": "de", "nl": "nl", "fr": "fr", "es": "es",
        "it": "it", "pl": "pl", "en": "en",
    }

    try:
        timestamp = str(int(time.time() * 1000))

        params = {
            "app_key": ALIEXPRESS_APP_KEY,
            "timestamp": timestamp,
            "sign_method": "sha256",
            "method": "aliexpress.affiliate.product.query",
            "keywords": keyword,
            "ship_to_country": ship_to_map.get(country, "DE"),
            "target_language": lang_map.get(language, "en"),
            "target_currency": "EUR",
            "tracking_id": ALIEXPRESS_TRACKING_ID,
            "page_no": "1",
            "page_size": str(min(max_results * 3, 50)),  # Fetch extra, filter later
            "sort": "SALE_PRICE_ASC",
        }

        # Sign the request
        params["sign"] = _sign_request(params, ALIEXPRESS_APP_SECRET)

        response = requests.get(API_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Parse response
        resp_body = data.get("aliexpress_affiliate_product_query_response", {})
        resp_result = resp_body.get("resp_result", {})
        result_data = resp_result.get("result", {})
        products_raw = result_data.get("products", {}).get("product", [])

        if not products_raw:
            logger.info("No AliExpress products found for '%s'", keyword)
            return []

        # Process and filter products
        products = []
        for item in products_raw:
            product = _parse_product(item)
            if not product:
                continue

            # Apply filters
            rating = product.get("rating", 0)
            orders = product.get("orders", 0)

            if rating < min_rating:
                continue
            if orders < min_orders:
                continue

            products.append(product)

            if len(products) >= max_results:
                break

        logger.info(
            "AliExpress search '%s' for %s: %d products found (after filters)",
            keyword, country, len(products)
        )
        return products

    except Exception as e:
        logger.error("AliExpress search failed for '%s': %s", keyword, e)
        return []


def get_product_details(product_id: str) -> Optional[dict]:
    """
    Get detailed information about a specific AliExpress product.

    Args:
        product_id: AliExpress product ID

    Returns:
        Product details dict or None
    """
    try:
        timestamp = str(int(time.time() * 1000))

        params = {
            "app_key": ALIEXPRESS_APP_KEY,
            "timestamp": timestamp,
            "sign_method": "sha256",
            "method": "aliexpress.affiliate.product.detail.get",
            "product_ids": product_id,
            "tracking_id": ALIEXPRESS_TRACKING_ID,
            "target_currency": "EUR",
        }

        params["sign"] = _sign_request(params, ALIEXPRESS_APP_SECRET)

        response = requests.get(API_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        resp_body = data.get("aliexpress_affiliate_product_detail_get_response", {})
        resp_result = resp_body.get("resp_result", {})
        result_data = resp_result.get("result", {})
        products = result_data.get("products", {}).get("product", [])

        if products:
            return _parse_product(products[0])
        return None

    except Exception as e:
        logger.error("AliExpress product detail failed for %s: %s", product_id, e)
        return None


def check_product_availability(product_id: str) -> bool:
    """Check if a product is still available on AliExpress."""
    details = get_product_details(product_id)
    return details is not None


def find_best_match(
    keyword: str,
    estimated_selling_price: float,
    country: str = "DE",
    language: str = "de",
    config: AppConfig = None,
) -> Optional[dict]:
    """
    Find the best matching product for a keyword.
    Considers price viability against the estimated selling price.

    Args:
        keyword: Search keyword
        estimated_selling_price: Expected selling price from competitor analysis
        country: Target country
        language: Target language
        config: App configuration

    Returns:
        Best matching product dict or None
    """
    config = config or AppConfig()

    products = search_products(
        keyword=keyword,
        country=country,
        language=language,
        min_rating=config.min_aliexpress_rating,
        min_orders=config.min_aliexpress_orders,
        max_results=10,
        config=config,
    )

    if not products:
        return None

    # Score products by margin viability
    best_product = None
    best_score = -1

    for product in products:
        ali_price = product.get("price", 0)
        if ali_price <= 0:
            continue

        # Preliminary margin check
        # Agent's landed cost is typically 1.1-1.3x AliExpress price
        estimated_landed_cost = ali_price * 1.2  # 20% buffer for agent markup
        estimated_margin = estimated_selling_price - estimated_landed_cost
        margin_pct = estimated_margin / estimated_selling_price if estimated_selling_price > 0 else 0

        if margin_pct < 0.2:  # Skip if even preliminary margin is too low
            continue

        # Score based on rating, orders, and margin
        rating_score = product.get("rating", 0) / 5.0 * 30  # 0-30
        order_score = min(product.get("orders", 0) / 10000, 1.0) * 30  # 0-30
        margin_score = min(margin_pct, 0.6) / 0.6 * 40  # 0-40

        score = rating_score + order_score + margin_score

        if score > best_score:
            best_score = score
            best_product = product
            best_product["estimated_margin_pct"] = round(margin_pct, 4)
            best_product["match_score"] = round(score, 1)

    if best_product:
        logger.info(
            "Best match for '%s': %s (score: %.1f, margin: %.1%%)",
            keyword, best_product.get("title", "?")[:50],
            best_product.get("match_score", 0),
            best_product.get("estimated_margin_pct", 0) * 100,
        )

    return best_product


def build_search_url(keyword: str) -> dict:
    """
    Build AliExpress and Alibaba search URLs for manual review.

    Returns:
        dict with aliexpress_search_url and alibaba_search_url
    """
    encoded_keyword = keyword.replace(" ", "+")
    return {
        "aliexpress_search_url": f"https://www.aliexpress.com/wholesale?SearchText={encoded_keyword}",
        "alibaba_search_url": f"https://www.alibaba.com/trade/search?SearchText={encoded_keyword}",
    }


def _parse_product(item: dict) -> Optional[dict]:
    """Parse a raw AliExpress API product response into a clean dict."""
    try:
        # Extract price
        price_str = item.get("target_sale_price", item.get("target_original_price", "0"))
        try:
            price = float(str(price_str).replace(",", "."))
        except (ValueError, TypeError):
            price = 0

        # Extract images
        image_url = item.get("product_main_image_url", "")
        small_images = item.get("product_small_image_urls", {})
        image_list = small_images.get("string", []) if isinstance(small_images, dict) else []
        all_images = [image_url] + image_list if image_url else image_list

        # Extract rating
        rating_str = item.get("evaluate_rate", "0")
        try:
            rating = float(str(rating_str).replace("%", "")) / 20  # Convert percentage to 5-star
        except (ValueError, TypeError):
            rating = 0

        # Extract orders
        orders_str = item.get("lastest_volume", item.get("latest_volume", "0"))
        try:
            orders = int(orders_str)
        except (ValueError, TypeError):
            orders = 0

        return {
            "aliexpress_product_id": str(item.get("product_id", "")),
            "title": item.get("product_title", ""),
            "url": item.get("promotion_link", item.get("product_detail_url", "")),
            "price": price,
            "original_price": float(item.get("target_original_price", price) or price),
            "rating": round(rating, 1),
            "orders": orders,
            "image_url": image_url,
            "image_urls": all_images,
            "shipping_cost": float(item.get("target_app_sale_price", "0") or 0),
            "category_id": str(item.get("first_level_category_id", "")),
            "category_name": item.get("first_level_category_name", ""),
        }

    except Exception as e:
        logger.warning("Failed to parse AliExpress product: %s", e)
        return None
