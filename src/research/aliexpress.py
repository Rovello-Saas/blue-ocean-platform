"""
AliExpress product research integration.

This module targets the **Drop Shipping** (`aliexpress.ds.*`) API surface, which
is what the BOC Open Platform app (AppKey 532468) is registered for. See
the long header in .env for why we're on DS and not Affiliate.

What works with signed-only access (no OAuth, verified 2026-04-19):
  - aliexpress.ds.category.get       → full category tree (558 nodes)
  - aliexpress.ds.feedname.get       → promo feed catalog (~131 feeds)
  - aliexpress.ds.recommend.feed.get → products in a feed (title/img/price/
                                       rating/orders/detail URL)

What's BLOCKED and why (do not re-debug, confirmed scope boundary):
  - aliexpress.ds.text.search        → EXCEPTION_TEXT_SEARCH_FOR_DS (free-form
                                       keyword search is Affiliate-only)
  - aliexpress.ds.product.get        → MissingParameter access_token (user
                                       OAuth required)
  - aliexpress.affiliate.*           → InsufficientPermission (wrong profile)

As a result, `search_products` / `find_top3_matches` no longer do true keyword
search against the full AliExpress catalog. They:
  1. Fetch a page from one of the curated bestseller feeds (200k+ products),
  2. Filter client-side by title substring match on the keyword,
  3. Return the top-N after applying rating/orders thresholds.

This gives us a decent proxy for "what bestsellers on AliExpress roughly match
this keyword", which is exactly what the research pipeline needs for margin
estimation. It is NOT exhaustive keyword search — niche keywords may return
nothing. The pipeline already handles empty results by falling back to
manual-review URLs via `build_search_url`.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from typing import Optional

import requests

from src.core.config import (
    AppConfig,
    ALIEXPRESS_APP_KEY,
    ALIEXPRESS_APP_SECRET,
    ALIEXPRESS_TRACKING_ID,
)

logger = logging.getLogger(__name__)

API_BASE_URL = "https://api-sg.aliexpress.com/sync"

# Which feed to pull from for keyword-match fallback. Picked because it's
# global (no country lock), 200k products, and weighted toward bestsellers —
# so title substring matches usually hit popular items, not long-tail junk.
DEFAULT_SOURCING_FEED = "AEB_Droplo_BestsellersItems_20241016"

# Cache feed pages for a short time to avoid N+1 fetches when pipeline
# iterates over many keywords back-to-back. Keyed by (feed, page_no, page_size).
_FEED_CACHE: dict[tuple, tuple[float, list[dict]]] = {}
_FEED_CACHE_TTL_SECONDS = 300  # 5 min


# -----------------------------------------------------------------------------
# Low-level signed request
# -----------------------------------------------------------------------------

def _sign_request(params: dict, secret: str) -> str:
    """Generate HMAC-SHA256 signature for AliExpress TOP API request.

    Algorithm: sort params by key, concatenate as "k1v1k2v2...", HMAC-SHA256
    with the app secret as key, hex digest uppercased. NO secret-wrapper
    (`secret + str + secret`) — that's the deprecated MD5 sign_method, not SHA256.
    Verified working 2026-04-19 against category.get / feedname.get / feed.get.
    """
    sorted_kv = "".join(f"{k}{v}" for k, v in sorted(params.items()))
    return hmac.new(
        secret.encode("utf-8"),
        sorted_kv.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest().upper()


def _call_ds_api(method: str, extra_params: dict, timeout: int = 20) -> dict:
    """
    POST a signed request to the DS API. Returns the parsed JSON response,
    or an empty dict on transport/signing failure (logged).

    Note: POST with form-encoded body is what works — a number of AliExpress
    docs show GET with query-string, but we verified the live endpoint rejects
    GET silently for some methods.
    """
    if not ALIEXPRESS_APP_KEY or ALIEXPRESS_APP_KEY.startswith("your_"):
        logger.warning("AliExpress credentials not configured — skipping %s", method)
        return {}

    params = {
        "app_key": ALIEXPRESS_APP_KEY,
        "method": method,
        "sign_method": "sha256",
        "timestamp": str(int(time.time() * 1000)),
        "format": "json",
        "v": "2.0",
        **{k: str(v) for k, v in extra_params.items() if v is not None and v != ""},
    }
    params["sign"] = _sign_request(params, ALIEXPRESS_APP_SECRET)

    try:
        resp = requests.post(API_BASE_URL, data=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error("AliExpress API %s transport failure: %s", method, e)
        return {}


# -----------------------------------------------------------------------------
# Feed-based primitives (verified working in scope)
# -----------------------------------------------------------------------------

def list_feeds() -> list[dict]:
    """Return the full list of promo feeds with product counts.

    Each entry: {"promo_name": str, "product_num": int, "promo_desc": str}.
    Useful for picking a feed to source products from.
    """
    data = _call_ds_api("aliexpress.ds.feedname.get", {})
    resp = data.get("aliexpress_ds_feedname_get_response", {})
    feeds_raw = (
        resp.get("resp_result", {})
            .get("result", {})
            .get("promos", {})
            .get("promo", [])
    )
    out = []
    for f in feeds_raw:
        try:
            product_num = int(f.get("product_num", 0))
        except (TypeError, ValueError):
            product_num = 0
        out.append({
            "promo_name": f.get("promo_name", ""),
            "product_num": product_num,
            "promo_desc": f.get("promo_desc", ""),
        })
    return out


def list_categories() -> list[dict]:
    """Return the DS category tree."""
    data = _call_ds_api("aliexpress.ds.category.get", {})
    resp = data.get("aliexpress_ds_category_get_response", {})
    cats = (
        resp.get("resp_result", {})
            .get("result", {})
            .get("categories", {})
            .get("category", [])
    )
    return cats if isinstance(cats, list) else []


def browse_feed(
    feed_name: str = DEFAULT_SOURCING_FEED,
    page_no: int = 1,
    page_size: int = 50,
    country: Optional[str] = None,
    currency: str = "USD",
    language: str = "EN",
    tracking_id: Optional[str] = None,
) -> list[dict]:
    """
    Fetch a page of products from a feed. Cached for 5 min by (feed, page, size).

    The global bestseller feeds (DEFAULT_SOURCING_FEED being the biggest) work
    best with no country filter. Country-locked feeds (AEB_UK_*, AEB_US_*)
    need a matching country or return zero.
    """
    cache_key = (feed_name, page_no, page_size, country or "GLOBAL")
    now = time.time()
    cached = _FEED_CACHE.get(cache_key)
    if cached and (now - cached[0]) < _FEED_CACHE_TTL_SECONDS:
        return cached[1]

    extra = {
        "feed_name": feed_name,
        "page_no": page_no,
        "page_size": page_size,
        "target_currency": currency,
        "target_language": language,
        "tracking_id": tracking_id or ALIEXPRESS_TRACKING_ID or "",
    }
    if country:
        extra["country"] = country

    data = _call_ds_api("aliexpress.ds.recommend.feed.get", extra)
    resp = data.get("aliexpress_ds_recommend_feed_get_response", {})
    products_raw = (
        resp.get("result", {})
            .get("products", {})
            .get("traffic_product_d_t_o", [])
    )
    if not isinstance(products_raw, list):
        products_raw = []

    parsed = [p for p in (_parse_product(item) for item in products_raw) if p]
    _FEED_CACHE[cache_key] = (now, parsed)
    return parsed


# -----------------------------------------------------------------------------
# Public API (signatures preserved from pre-migration version)
# -----------------------------------------------------------------------------

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
    Find products matching a keyword using the feed-filter fallback.

    Since `aliexpress.ds.text.search` is blocked for DS-scope apps, this pulls
    from DEFAULT_SOURCING_FEED and filters title-contains-keyword locally.
    Returns the top `max_results` after rating/orders thresholds.

    Args match the pre-migration signature so `pipeline.py` and dashboard code
    don't need updating. `country` / `language` are accepted but currently
    unused (global feed pricing is USD; conversion is approximate).
    """
    config = config or AppConfig()

    if not ALIEXPRESS_APP_KEY or ALIEXPRESS_APP_KEY.startswith("your_"):
        logger.warning("AliExpress API credentials not configured — skipping product search")
        return []

    keyword_norm = (keyword or "").strip().lower()
    if not keyword_norm:
        return []

    # Pull a reasonably large page from the bestseller feed. 50 is the
    # safe upper bound for feed.get. We scan through a few pages if needed
    # to find enough matches.
    matches: list[dict] = []
    max_pages = 3
    for page in range(1, max_pages + 1):
        page_products = browse_feed(
            feed_name=DEFAULT_SOURCING_FEED,
            page_no=page,
            page_size=50,
        )
        if not page_products:
            break

        for p in page_products:
            title = (p.get("title") or "").lower()
            if keyword_norm not in title:
                continue
            if p.get("rating", 0) < min_rating:
                continue
            if p.get("orders", 0) < min_orders:
                continue
            matches.append(p)
            if len(matches) >= max_results:
                break

        if len(matches) >= max_results:
            break

    logger.info(
        "AliExpress feed-filter search '%s' (country=%s): %d matches from %d-page scan",
        keyword, country, len(matches), max_pages,
    )
    return matches


def get_product_details(product_id: str) -> Optional[dict]:
    """
    Return details for a specific product ID by scanning the sourcing feed
    for a match. This is a best-effort lookup — `aliexpress.ds.product.get`
    itself requires OAuth which we don't have.

    If the product isn't in the first few pages of the sourcing feed, we
    return None. For a fuller lookup in the future, wire up DS OAuth.
    """
    if not product_id:
        return None

    target = str(product_id)
    for page in range(1, 5):
        page_products = browse_feed(page_no=page, page_size=50)
        if not page_products:
            break
        for p in page_products:
            if str(p.get("aliexpress_product_id")) == target:
                return p
    return None


def check_product_availability(product_id: str) -> bool:
    """
    Check if a product is still available on AliExpress.

    Without `product.get` (OAuth required) we can't do a real availability
    check. Conservative policy: return True (assume available) unless we have
    positive evidence of removal. The stock checker's purpose is to flag
    *regressions* in what's working, so silent "assume OK" is safer than
    false-positive kill signals on every call.
    """
    if not product_id:
        return True
    # If we happen to find the product in our cached feed pages, we know it's live.
    # If not, we don't know either way, so we defer to "assume live".
    if get_product_details(product_id):
        return True
    return True


def find_best_match(
    keyword: str,
    estimated_selling_price: float,
    country: str = "DE",
    language: str = "de",
    config: AppConfig = None,
) -> Optional[dict]:
    """Find the best-scored product for a keyword given an expected sell price."""
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

    best_product = None
    best_score = -1.0

    for product in products:
        ali_price = product.get("price", 0)
        if ali_price <= 0:
            continue

        estimated_landed_cost = ali_price * 1.2
        estimated_margin = estimated_selling_price - estimated_landed_cost
        margin_pct = (
            estimated_margin / estimated_selling_price
            if estimated_selling_price > 0 else 0
        )
        if margin_pct < 0.2:
            continue

        rating_score = (product.get("rating", 0) / 5.0) * 30
        order_score = min(product.get("orders", 0) / 10000, 1.0) * 30
        margin_score = min(margin_pct, 0.6) / 0.6 * 40
        score = rating_score + order_score + margin_score

        if score > best_score:
            best_score = score
            best_product = dict(product)
            best_product["estimated_margin_pct"] = round(margin_pct, 4)
            best_product["match_score"] = round(score, 1)

    if best_product:
        logger.info(
            "Best match for '%s': %s (score: %.1f, margin: %.1f%%)",
            keyword,
            (best_product.get("title") or "?")[:50],
            best_product.get("match_score", 0),
            best_product.get("estimated_margin_pct", 0) * 100,
        )
    return best_product


def find_top3_matches(
    keyword: str,
    estimated_selling_price: float = 0,
    country: str = "DE",
    language: str = "de",
    config: AppConfig = None,
) -> dict:
    """
    Return the **Top 3** products for a keyword:
      - best_seller  (most orders)
      - best_price   (cheapest)
      - best_rated   (highest rating)

    Uses relaxed filters so we usually get something. Each entry is a product
    dict plus a 'tag' field, or None if nothing was found at all.
    """
    config = config or AppConfig()

    products = search_products(
        keyword=keyword,
        country=country,
        language=language,
        min_rating=0,
        min_orders=0,
        max_results=30,
        config=config,
    )

    result: dict = {
        "best_seller": None,
        "best_price": None,
        "best_rated": None,
        "all_products": products,
    }
    if not products:
        return result

    priced = [p for p in products if p.get("price", 0) > 0]
    if not priced:
        return result

    best_seller = dict(sorted(priced, key=lambda p: p.get("orders", 0), reverse=True)[0])
    best_seller["tag"] = "Best Seller"
    result["best_seller"] = best_seller

    best_price = dict(sorted(priced, key=lambda p: p.get("price", 9999))[0])
    best_price["tag"] = "Best Price"
    result["best_price"] = best_price

    best_rated = dict(sorted(
        priced,
        key=lambda p: (p.get("rating", 0), p.get("orders", 0)),
        reverse=True,
    )[0])
    best_rated["tag"] = "Best Rated"
    result["best_rated"] = best_rated

    if estimated_selling_price > 0:
        for key in ("best_seller", "best_price", "best_rated"):
            p = result[key]
            if p:
                ali_price = p.get("price", 0)
                landed = ali_price * 1.2
                margin = estimated_selling_price - landed
                p["estimated_margin_pct"] = round(
                    margin / estimated_selling_price if estimated_selling_price else 0, 4
                )

    logger.info(
        "Top-3 AliExpress for '%s': seller=%s orders, price=$%.2f, rated=%.1f/5",
        keyword,
        best_seller.get("orders", 0),
        best_price.get("price", 0),
        best_rated.get("rating", 0),
    )
    return result


def build_search_url(keyword: str) -> dict:
    """Build AliExpress and Alibaba search URLs for manual review."""
    encoded_keyword = (keyword or "").replace(" ", "+")
    return {
        "aliexpress_search_url": f"https://www.aliexpress.com/wholesale?SearchText={encoded_keyword}",
        "alibaba_search_url": f"https://www.alibaba.com/trade/search?SearchText={encoded_keyword}",
    }


# -----------------------------------------------------------------------------
# Parsing
# -----------------------------------------------------------------------------

def _parse_product(item: dict) -> Optional[dict]:
    """Parse a raw feed-API product into the canonical shape used downstream.

    DS feed-API response uses `productSmallImageUrl` (camelCase) inside
    `product_small_image_urls` — NOT `string` like the old affiliate API.
    """
    try:
        price_str = item.get("target_sale_price", item.get("target_original_price", "0"))
        try:
            price = float(str(price_str).replace(",", "."))
        except (ValueError, TypeError):
            price = 0.0

        image_url = item.get("product_main_image_url", "")
        small = item.get("product_small_image_urls", {})
        small_list = (
            small.get("productSmallImageUrl", [])
            if isinstance(small, dict) else []
        )
        # De-dup while preserving order
        seen = set()
        all_images = []
        for url in ([image_url] if image_url else []) + small_list:
            if url and url not in seen:
                seen.add(url)
                all_images.append(url)

        # DS API's `evaluate_rate` is a positive-feedback percentage like
        # "97.6%". We map to a nominal 5-star scale (% / 20) for backward
        # compatibility with downstream code that expects 0-5. We also keep
        # the raw percentage in `feedback_rate` for anything more honest.
        rating_str = item.get("evaluate_rate", "0")
        try:
            pct = float(str(rating_str).replace("%", ""))
        except (ValueError, TypeError):
            pct = 0.0
        rating_5 = round(pct / 20, 1) if pct else 0.0

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
            "rating": rating_5,
            "feedback_rate": pct,  # raw % (new field, more honest)
            "orders": orders,
            "image_url": image_url,
            "image_urls": all_images,
            "shipping_cost": 0.0,  # not provided by feed API
            "currency": item.get("target_sale_price_currency", "USD"),
            "category_id": str(item.get("first_level_category_id", "")),
            "category_name": item.get("first_level_category_name", ""),
            "shop_id": str(item.get("shop_id", "")),
            "shop_url": item.get("shop_url", ""),
            "discount_pct_raw": item.get("discount", ""),
        }

    except Exception as e:
        logger.warning("Failed to parse AliExpress product: %s", e)
        return None
