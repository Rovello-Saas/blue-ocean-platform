"""
Google Merchant Center — Best Sellers report fetcher.

Replaces the old "Claude hallucinates 150 keywords from nothing" ideation
step with real demand data from Google Shopping.

What Google gives us per row:
  - product title (real product, not a guess)
  - rank within category × country × week
  - previous_rank (so we can compute velocity)
  - relative_demand: VERY_HIGH / HIGH / MEDIUM / LOW / VERY_LOW
  - relative_demand_change: RISER / FLAT / SINKER
  - category path (l1..l3)
  - price range (micros)
  - inventory_status (IN_STOCK / OUT_OF_STOCK / NOT_IN_INVENTORY)

Docs:
  https://developers.google.com/shopping-content/guides/reports/best-selling-products
  https://cloud.google.com/bigquery/docs/merchant-center-best-sellers-schema

Eligibility (verified 2026-04-21):
  - Country must be one of the 39 supported (DE, US, UK, FR, etc. all OK)
  - MC account exists and has at least some approved products
  - User / service account has the "Performance and insights" role on MC
  - Market Insights Terms of Service accepted in MC UI

Auth: service account (same creds as labels.py), scope
`https://www.googleapis.com/auth/content`.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src.core.config import GOOGLE_MERCHANT_CENTER_ID, get_service_account_credentials

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Default dropship-friendly Google product categories.
#
# IDs come from Google's Product Taxonomy (the same IDs that appear in a MC
# feed's `google_product_category`). We intentionally skip categories that
# are either (a) brand-dominated, (b) regulated, (c) heavy/fragile for
# shipping, or (d) low dropship viability:
#   - Apparel & Accessories (166) — brand-heavy, variant hell
#   - Electronics (222) — Apple/Samsung/Sony dominate top ranks
#   - Software / Media / Books — not physical goods
#   - Vehicles & Parts (783) — fitment complexity
#
# Feel free to override via config `research.best_sellers_categories`.
# ---------------------------------------------------------------------------
DEFAULT_DROPSHIP_CATEGORIES: list[dict] = [
    # Home & Garden — bread and butter for dropshipping
    {"id": 536,  "name": "Home & Garden"},
    {"id": 696,  "name": "Home & Garden > Decor"},
    {"id": 730,  "name": "Home & Garden > Kitchen & Dining"},
    {"id": 985,  "name": "Home & Garden > Household Appliances"},
    {"id": 729,  "name": "Home & Garden > Lawn & Garden"},
    # Sporting Goods — fitness, outdoor, yoga
    {"id": 988,  "name": "Sporting Goods"},
    # Health & Beauty — careful, some regulated; supplements filtered downstream
    {"id": 469,  "name": "Health & Beauty"},
    # Baby & Toddler — stroller accessories, nursery gadgets
    {"id": 537,  "name": "Baby & Toddler"},
    # Pet Supplies
    {"id": 1,    "name": "Animals & Pet Supplies"},
    # Toys & Games
    {"id": 1239, "name": "Toys & Games"},
    # Office products — desk accessories, organisation
    {"id": 950,  "name": "Office Supplies"},
    # Luggage & Bags — travel accessories
    {"id": 5181, "name": "Luggage & Bags"},
]


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class BestSellerProduct:
    """One row from the Best Sellers product cluster view.

    `source_category` is the category we queried under (for downstream
    analytics). The Google-returned `category_l1..l3` is the product's
    taxonomy path, which may be narrower than our seed.
    """
    title: str = ""
    brand: str = ""
    rank: int = 0
    previous_rank: int = 0
    relative_demand: str = ""         # VERY_HIGH / HIGH / MEDIUM / LOW / VERY_LOW
    relative_demand_change: str = ""  # RISER / FLAT / SINKER
    category_l1: str = ""
    category_l2: str = ""
    category_l3: str = ""
    country_code: str = ""
    inventory_status: str = ""        # IN_STOCK / OUT_OF_STOCK / NOT_IN_INVENTORY
    price_min_eur: float = 0.0
    price_max_eur: float = 0.0
    currency: str = ""
    report_date: str = ""
    source_category_id: int = 0
    source_category_name: str = ""
    fetched_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    @property
    def rank_delta(self) -> int:
        """How many spots it climbed (positive) or fell (negative)."""
        if self.rank and self.previous_rank:
            return self.previous_rank - self.rank
        return 0

    @property
    def rank_velocity(self) -> float:
        """Rank improvement as a fraction of current rank.

        `Δrank / current_rank` is the "movers & shakers" primitive: a product
        climbing from rank 100 to 50 (Δ=50, velocity=1.0) is a stronger signal
        than one going from 10 to 5 (Δ=5, velocity=1.0) — same ratio, but the
        former is ascending out of the long tail and the latter is just
        jockeying at the top. We use Δ alone elsewhere as a simpler filter;
        velocity is exposed for anyone wanting to rank across ranks.
        """
        if self.rank and self.previous_rank:
            return (self.previous_rank - self.rank) / self.rank
        return 0.0


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------

class BestSellersClient:
    """Thin wrapper around MC API Reports for the Best Sellers view.

    Caches the authed service per instance. Safe to instantiate on every
    pipeline run; the service account creds are fetched lazily once.
    """

    # Schema aligned with Content API v2.1 as of 2026-04:
    #   - country/category fields dropped the `report_` prefix
    #   - `product_cluster_view.*` renamed to `product_cluster.*`
    #   - `price_range` was removed from this view (pricing lives on
    #     ProductPerformanceView now); our downstream economics gate
    #     therefore relies on DataForSEO-reported product prices and
    #     the SerpAPI-scraped competitor range instead.
    _REPORT_COLUMNS = [
        "best_sellers.report_date",
        "best_sellers.report_granularity",
        "best_sellers.country_code",
        "best_sellers.category_id",
        "best_sellers.rank",
        "best_sellers.previous_rank",
        "best_sellers.relative_demand",
        "best_sellers.previous_relative_demand",
        "best_sellers.relative_demand_change",
        "product_cluster.title",
        "product_cluster.brand",
        "product_cluster.category_l1",
        "product_cluster.category_l2",
        "product_cluster.category_l3",
        "product_cluster.variant_gtins",
        "product_cluster.inventory_status",
        "product_cluster.brand_inventory_status",
    ]

    def __init__(self, merchant_id: Optional[str] = None):
        self.merchant_id = merchant_id or GOOGLE_MERCHANT_CENTER_ID
        if not self.merchant_id:
            raise ValueError("GOOGLE_MERCHANT_CENTER_ID is not configured")
        self._service = None

    def _get_service(self):
        if self._service is not None:
            return self._service
        creds = get_service_account_credentials(
            scopes=["https://www.googleapis.com/auth/content"],
        )
        self._service = build("content", "v2.1", credentials=creds, cache_discovery=False)
        return self._service

    # -----------------------------------------------------------------------
    # Raw query
    # -----------------------------------------------------------------------

    def _run_query(self, query: str) -> list[dict]:
        """Run a GAQL query via the Reports resource. Raises on HTTP errors
        other than "no results" (which we treat as an empty list).
        """
        service = self._get_service()
        try:
            resp = service.reports().search(
                merchantId=self.merchant_id,
                body={"query": query},
            ).execute()
            return resp.get("results", []) or []
        except HttpError as e:
            status = e.resp.status if e.resp else "?"
            body = e.content.decode() if isinstance(e.content, bytes) else str(e.content)
            logger.error(
                "MC Reports query failed (HTTP %s): %s\nquery=\n%s",
                status, body[:500], query.strip(),
            )
            raise

    # -----------------------------------------------------------------------
    # Per-category fetch
    # -----------------------------------------------------------------------

    def fetch_category(
        self,
        country: str,
        category_id: int,
        *,
        limit: int = 20,
        granularity: str = "WEEKLY",
        only_not_in_inventory: bool = True,
        exclude_sinkers: bool = True,
    ) -> list[BestSellerProduct]:
        """Fetch top-N best sellers for one category + country.

        Args:
            country: ISO code (e.g. "DE", "US").
            category_id: Google product taxonomy ID.
            limit: max rows to return (usually 20 per category).
            granularity: "WEEKLY" (default, fresher) or "MONTHLY" (smoother).
            only_not_in_inventory: filter out products we already stock. This
                is the whole point — surface what we DON'T sell yet.
            exclude_sinkers: drop declining products (relative_demand_change =
                SINKER). RISER + FLAT are kept.

        Returns:
            List of BestSellerProduct. Empty list if the category is unpopulated
            for this account/country.
        """
        cols = ", ".join(self._REPORT_COLUMNS)
        where = [
            f"best_sellers.report_granularity = '{granularity}'",
            f"best_sellers.country_code = '{country}'",
            f"best_sellers.category_id = {int(category_id)}",
        ]
        if only_not_in_inventory:
            where.append("product_cluster.inventory_status = 'NOT_IN_INVENTORY'")
        # NOTE: SINKER filtering is done in Python below, not in the WHERE
        # clause. GAQL doesn't accept parenthesised OR, and a plain
        # `relative_demand_change != 'SINKER'` would also eliminate NULL
        # rows (brand-new products with no previous rank) — exactly the
        # fresh entrants we most want to see.

        query = (
            f"SELECT {cols} "
            f"FROM BestSellersProductClusterView "
            f"WHERE {' AND '.join(where)} "
            f"ORDER BY best_sellers.rank "
            f"LIMIT {int(limit)}"
        )

        def _iso_date(d) -> str:
            """`reportDate` comes back as {"year", "month", "day"}; flatten."""
            if isinstance(d, dict):
                y, m, dd = d.get("year"), d.get("month"), d.get("day")
                if y and m and dd:
                    try:
                        return f"{int(y):04d}-{int(m):02d}-{int(dd):02d}"
                    except (TypeError, ValueError):
                        return ""
            return str(d) if d else ""

        raw = self._run_query(query)
        out: list[BestSellerProduct] = []
        for row in raw:
            bs = row.get("bestSellers", {}) or {}
            # Response wrapper is `productCluster` (no longer View).
            pc = row.get("productCluster", {}) or {}
            # Python-side SINKER filter — keeps NULL (brand-new) products.
            if exclude_sinkers and bs.get("relativeDemandChange") == "SINKER":
                continue
            out.append(BestSellerProduct(
                title=pc.get("title") or "",
                brand=pc.get("brand") or "",
                rank=int(bs.get("rank") or 0),
                previous_rank=int(bs.get("previousRank") or 0),
                relative_demand=bs.get("relativeDemand") or "",
                relative_demand_change=bs.get("relativeDemandChange") or "",
                category_l1=pc.get("categoryL1") or "",
                category_l2=pc.get("categoryL2") or "",
                category_l3=pc.get("categoryL3") or "",
                country_code=bs.get("countryCode") or country,
                inventory_status=pc.get("inventoryStatus") or "",
                # price_range was removed from this view in the 2026 API
                # rev; these fields stay on the dataclass (with 0.0
                # defaults) so callers that read them get a safe no-op.
                price_min_eur=0.0,
                price_max_eur=0.0,
                currency="",
                report_date=_iso_date(bs.get("reportDate")),
                source_category_id=int(category_id),
            ))
        return out

    # -----------------------------------------------------------------------
    # Multi-category convenience
    # -----------------------------------------------------------------------

    def fetch_categories(
        self,
        country: str,
        categories: Optional[list[dict]] = None,
        *,
        per_category_limit: int = 20,
        delay_between_calls: float = 0.5,
        **kwargs,
    ) -> list[BestSellerProduct]:
        """Fetch top-N best sellers across multiple categories.

        `categories` is a list of {"id": int, "name": str} dicts. Falls back
        to `DEFAULT_DROPSHIP_CATEGORIES` if omitted.

        The `delay_between_calls` is a small courtesy pause — MC Reports has
        no published rate limit, but hammering 12 calls in a row with no
        spacing occasionally gets throttled. Half a second is invisible in a
        pipeline that's about to make 100× more expensive SerpAPI calls.

        Returns one flat list, de-duped by title (if the same product shows
        up under two overlapping categories, keep the better-ranked copy).
        """
        cats = categories or DEFAULT_DROPSHIP_CATEGORIES
        all_products: dict[str, BestSellerProduct] = {}  # title → best row
        for cat in cats:
            cid = cat.get("id")
            cname = cat.get("name", f"cat_{cid}")
            if not cid:
                continue
            try:
                rows = self.fetch_category(
                    country=country,
                    category_id=int(cid),
                    limit=per_category_limit,
                    **kwargs,
                )
            except HttpError as e:
                logger.warning("Category %s (%s) failed: %s — skipping",
                               cname, cid, e)
                continue
            except Exception as e:
                logger.warning("Category %s (%s) unexpected error: %s — skipping",
                               cname, cid, e)
                continue

            for p in rows:
                p.source_category_name = cname
                key = (p.title or "").strip().lower()
                if not key:
                    continue
                # Keep the row with the best (lowest) rank if we already
                # saw this product from another category.
                existing = all_products.get(key)
                if existing is None or (p.rank and p.rank < (existing.rank or 9999)):
                    all_products[key] = p
            logger.info(
                "Best Sellers: fetched %d rows from '%s' (%s)",
                len(rows), cname, cid,
            )
            if delay_between_calls > 0:
                time.sleep(delay_between_calls)

        return list(all_products.values())


# ---------------------------------------------------------------------------
# Module-level convenience
# ---------------------------------------------------------------------------

def fetch_best_sellers(
    country: str = "DE",
    categories: Optional[list[dict]] = None,
    per_category_limit: int = 20,
    **kwargs,
) -> list[BestSellerProduct]:
    """One-call convenience for the pipeline. See `BestSellersClient.fetch_categories`."""
    try:
        client = BestSellersClient()
    except ValueError as e:
        logger.error("Best Sellers: %s", e)
        return []
    return client.fetch_categories(
        country=country,
        categories=categories,
        per_category_limit=per_category_limit,
        **kwargs,
    )
