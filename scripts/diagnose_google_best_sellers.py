"""
Diagnostic — pull the top 20 Best Sellers from Google Merchant Center for a
country, via the existing service-account + Content API v2.1 setup.

Why this exists: before wiring Best Sellers data into the research pipeline,
we need to confirm empirically that:
  1. Our service account has the "Performance and insights" role on MC
  2. The Market Insights ToS has been accepted on this MC account
  3. The account actually has populated Best Sellers data for the target
     country (empty-state risk for brand-new / small accounts)

Usage:
    python3 scripts/diagnose_google_best_sellers.py            # defaults to DE
    python3 scripts/diagnose_google_best_sellers.py US         # US instead
    python3 scripts/diagnose_google_best_sellers.py DE 50      # top 50
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running from repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src.core.config import GOOGLE_MERCHANT_CENTER_ID, get_service_account_credentials


def _fmt_inventory(s: str) -> str:
    """Short-form inventory status (NOT_IN_INVENTORY → not_in)."""
    if not s:
        return "—"
    return s.replace("_INVENTORY", "").replace("_", " ").lower()


def run(country: str = "DE", limit: int = 20) -> None:
    if not GOOGLE_MERCHANT_CENTER_ID:
        print("✗ GOOGLE_MERCHANT_CENTER_ID is not configured in .env")
        sys.exit(1)

    print(f"Merchant Center ID: {GOOGLE_MERCHANT_CENTER_ID}")
    print(f"Target country: {country}")
    print(f"Limit: {limit}\n")

    creds = get_service_account_credentials(
        scopes=["https://www.googleapis.com/auth/content"],
    )
    service = build("content", "v2.1", credentials=creds, cache_discovery=False)

    # GAQL query — Best Sellers product cluster view, weekly granularity.
    # We don't pin a `report_date` because the latest snapshot varies; filtering
    # on granularity + country and ordering by rank is sufficient for a diagnostic.
    # Schema note (2026-04 Content API v2.1):
    #   - country/category fields dropped the `report_` prefix
    #     (`best_sellers.country_code`, `best_sellers.category_id`)
    #   - `product_cluster_view.*` → `product_cluster.*`
    #   - `price_range` was removed from this view; pricing only lives
    #     on the per-product ProductPerformanceView now.
    query = f"""
        SELECT
          best_sellers.report_date,
          best_sellers.report_granularity,
          best_sellers.country_code,
          best_sellers.category_id,
          best_sellers.rank,
          best_sellers.previous_rank,
          best_sellers.relative_demand,
          best_sellers.previous_relative_demand,
          best_sellers.relative_demand_change,
          product_cluster.title,
          product_cluster.brand,
          product_cluster.category_l1,
          product_cluster.category_l2,
          product_cluster.category_l3,
          product_cluster.variant_gtins,
          product_cluster.inventory_status,
          product_cluster.brand_inventory_status
        FROM BestSellersProductClusterView
        WHERE
          best_sellers.report_granularity = 'WEEKLY'
          AND best_sellers.country_code = '{country}'
        ORDER BY best_sellers.rank
        LIMIT {limit}
    """

    try:
        resp = service.reports().search(
            merchantId=GOOGLE_MERCHANT_CENTER_ID,
            body={"query": query},
        ).execute()
    except HttpError as e:
        status = getattr(e, "status_code", None) or (e.resp.status if e.resp else "?")
        body = e.content.decode() if isinstance(e.content, bytes) else str(e.content)
        print(f"✗ HTTP {status} from Merchant Center API")
        print(body[:800])
        print()
        if status in (403,):
            print("Likely causes:")
            print("  • Service account lacks 'Performance and insights' role on MC")
            print("  • Market Insights Terms of Service not accepted in MC UI")
            print("  • Service account email not added as a user on the MC account")
        elif status == 400:
            print("Query rejected — likely schema drift. Check the Reports API docs:")
            print("  https://developers.google.com/shopping-content/reference/rest/v2.1/reports")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Unexpected error: {type(e).__name__}: {e}")
        sys.exit(1)

    rows = resp.get("results", [])
    if not rows:
        print("✗ API call succeeded but returned 0 rows.")
        print()
        print("Likely causes:")
        print("  • Market Insights ToS not yet accepted (check MC UI → Analytics → Popular products)")
        print("  • MC account too new / not enough approved products to surface data")
        print("  • Target country (%s) not active on this MC account" % country)
        sys.exit(0)

    print(f"✓ Got {len(rows)} rows\n")
    print(f"{'Rank':>5}  {'Δ':>4}  {'Demand':<10}  {'Change':<7}  {'Inventory':<10}  "
          f"{'Brand':<16}  {'Category':<30}  Title")
    print("-" * 160)

    for row in rows:
        bs = row.get("bestSellers", {}) or {}
        pc = row.get("productCluster", {}) or {}
        rank = bs.get("rank") or "—"
        prev = bs.get("previousRank")
        delta = ""
        if isinstance(rank, int) and isinstance(prev, int):
            diff = prev - rank
            delta = f"+{diff}" if diff > 0 else (str(diff) if diff < 0 else "·")
        cat = " > ".join(
            x for x in [pc.get("categoryL1"), pc.get("categoryL2"), pc.get("categoryL3")] if x
        )[:30]
        title = (pc.get("title") or "—")[:60]
        brand = (pc.get("brand") or "—")[:16]
        demand = bs.get("relativeDemand") or "—"
        change = bs.get("relativeDemandChange") or "—"
        inv = _fmt_inventory(pc.get("inventoryStatus") or "")
        print(f"{str(rank):>5}  {delta:>4}  {demand:<10}  {change:<7}  {inv:<10}  "
              f"{brand:<16}  {cat:<30}  {title}")

    # Extra summary — how many risers, demand distribution, categories
    risers = sum(1 for r in rows if (r.get("bestSellers") or {}).get("relativeDemandChange") == "RISER")
    sinkers = sum(1 for r in rows if (r.get("bestSellers") or {}).get("relativeDemandChange") == "SINKER")
    flat = len(rows) - risers - sinkers
    print()
    print(f"Movement: {risers} risers  /  {flat} flat  /  {sinkers} sinkers")
    # reportDate comes back as {"year": 2026, "month": 4, "day": 20} — flatten to ISO.
    def _iso(d):
        if isinstance(d, dict):
            y, m, dd = d.get("year"), d.get("month"), d.get("day")
            if y and m and dd:
                return f"{int(y):04d}-{int(m):02d}-{int(dd):02d}"
        return str(d) if d else ""
    snapshot_dates = {_iso((r.get("bestSellers") or {}).get("reportDate")) for r in rows}
    print(f"Snapshot date(s): {', '.join(sorted(d for d in snapshot_dates if d))}")


if __name__ == "__main__":
    country = sys.argv[1] if len(sys.argv) > 1 else "DE"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    run(country=country, limit=limit)
