"""
Competition analysis using SerpAPI.
Analyzes Google Shopping results to determine:
- Number of competitors
- Same product vs diverse products (differentiation score)
- Competitor pricing data
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import Optional

from serpapi import GoogleSearch

from src.core.config import AppConfig, SERPAPI_KEY
from src.core.cost_tracker import CostTracker

logger = logging.getLogger(__name__)

# Country to Google Shopping domain mapping
COUNTRY_DOMAINS = {
    "DE": "google.de",
    "NL": "google.nl",
    "AT": "google.at",
    "FR": "google.fr",
    "BE": "google.be",
    "CH": "google.ch",
    "ES": "google.es",
    "IT": "google.it",
    "PL": "google.pl",
    "GB": "google.co.uk",
    "US": "google.com",
}

COUNTRY_GL = {
    "DE": "de", "NL": "nl", "AT": "at", "FR": "fr",
    "BE": "be", "CH": "ch", "ES": "es", "IT": "it",
    "PL": "pl", "GB": "uk", "US": "us",
}

LANGUAGE_HL = {
    "de": "de", "nl": "nl", "fr": "fr", "es": "es",
    "it": "it", "pl": "pl", "en": "en",
}


def analyze_competition(
    keyword: str,
    country: str = "DE",
    language: str = "de",
    config: AppConfig = None,
    cost_tracker: Optional[CostTracker] = None,
) -> Optional[dict]:
    """
    Analyze competition for a keyword using Google Shopping results.

    Returns:
        dict with:
            competitor_count: int
            unique_product_count: int
            competition_type: "same_product" | "diverse_products"
            differentiation_score: float (0-100)
            avg_competitor_price: float
            median_competitor_price: float
            sellers: list of seller names
            price_range: (min, max)
    """
    config = config or AppConfig()

    if not SERPAPI_KEY or SERPAPI_KEY.startswith("your_") or len(SERPAPI_KEY) < 20:
        logger.warning("SERPAPI_KEY not configured — skipping competition analysis")
        return None

    try:
        params = {
            "engine": "google_shopping",
            "q": keyword,
            "google_domain": COUNTRY_DOMAINS.get(country, "google.de"),
            "gl": COUNTRY_GL.get(country, "de"),
            "hl": LANGUAGE_HL.get(language, "de"),
            "api_key": SERPAPI_KEY,
            "num": 40,  # Get up to 40 results for thorough analysis
        }

        search = GoogleSearch(params)
        results = search.get_dict()

        # Record SerpAPI cost — SerpAPI doesn't return cost in the response,
        # so we always estimate from the plan per-call rate. Happens whether
        # or not shopping_results came back, because we still got charged.
        if cost_tracker is not None:
            cost_tracker.record_serpapi(
                engine="google_shopping",
                context=f"{keyword[:40]} / {country}",
            )

        shopping_results = results.get("shopping_results", [])

        if not shopping_results:
            logger.info("No Shopping results for '%s' in %s", keyword, country)
            return None

        # Extract data from results
        sellers = []
        product_titles = []
        prices = []
        competitor_pdp_url = ""  # First result's product page URL
        # Thumbnail of the first Shopping result that exposes one. Used by
        # the Research Inbox as a visual preview per row — we always have
        # SerpAPI data for anything that reached this function, so this is
        # more reliable than falling back to AliExpress (which misses ~70%
        # of the time for DE-market niche product keywords).
        competitor_thumbnail_url = ""

        for item in shopping_results:
            # Seller/source
            source = item.get("source", item.get("merchant", {}).get("name", ""))
            if source:
                sellers.append(source.strip().lower())

            # Product title
            title = item.get("title", "")
            if title:
                product_titles.append(title.strip().lower())

            # Price
            price = _extract_price(item)
            if price and price > 0:
                prices.append(price)

            # Capture the top competitor's actual PDP URL (first result with a link)
            if not competitor_pdp_url:
                link = item.get("link") or item.get("product_link") or ""
                if link and link.startswith("http"):
                    competitor_pdp_url = link

            # Capture first thumbnail URL. SerpAPI returns these as
            # `thumbnail` in each shopping_result; some items lack it
            # (sponsored/local inventory ads), so we keep scanning until
            # we find one.
            if not competitor_thumbnail_url:
                thumb = item.get("thumbnail") or ""
                if thumb and thumb.startswith("http"):
                    competitor_thumbnail_url = thumb

        # Calculate metrics
        unique_sellers = set(sellers)
        competitor_count = len(unique_sellers)

        # Calculate product uniqueness using title similarity
        unique_product_count = _count_unique_products(product_titles)

        # Differentiation score: how easy is it to differentiate?
        # High score = many sellers selling the SAME product (easy to stand out)
        # Low score = sellers offer DIVERSE products (harder to differentiate)
        total_results = len(shopping_results)
        if total_results > 0 and unique_product_count > 0:
            differentiation_score = (1 - (unique_product_count / total_results)) * 100
        else:
            differentiation_score = 50  # Default if insufficient data

        # Clamp to 0-100 range
        differentiation_score = max(0, min(100, differentiation_score))

        # Competition type
        if unique_product_count <= total_results * 0.3:
            competition_type = "same_product"  # Most results are the same product
        else:
            competition_type = "diverse_products"  # Many different products

        # Price statistics
        avg_price = sum(prices) / len(prices) if prices else 0
        median_price = _median(prices) if prices else 0
        price_range = (min(prices), max(prices)) if prices else (0, 0)

        # Build a Google Shopping URL for this keyword + country
        domain = COUNTRY_DOMAINS.get(country, "google.de")
        encoded_kw = keyword.replace(" ", "+")
        google_shopping_url = f"https://{domain}/search?tbm=shop&q={encoded_kw}"

        result = {
            "competitor_count": competitor_count,
            "unique_product_count": unique_product_count,
            "competition_type": competition_type,
            "differentiation_score": round(differentiation_score, 1),
            "avg_competitor_price": round(avg_price, 2),
            "median_competitor_price": round(median_price, 2),
            "sellers": list(unique_sellers),
            "price_range_min": round(price_range[0], 2),
            "price_range_max": round(price_range[1], 2),
            "total_results": len(shopping_results),
            "google_shopping_url": google_shopping_url,
            "competitor_pdp_url": competitor_pdp_url,
            "competitor_thumbnail_url": competitor_thumbnail_url,
        }

        logger.info(
            "Competition for '%s' in %s: %d competitors, %d unique products, score: %.0f",
            keyword, country, competitor_count, unique_product_count,
            differentiation_score
        )

        return result

    except Exception as e:
        logger.error("Competition analysis failed for '%s': %s", keyword, e)
        return None


def filter_by_competition(
    keyword_data: list[dict],
    config: AppConfig = None,
) -> list[dict]:
    """
    Filter keywords based on competition analysis results.

    Two modes, selected by `research.use_tiered_filter`:

    1. Portfolio (default, recommended): a candidate passes if (volume,
       competitor_count, differentiation_score, trend_slope) fits any
       strategy in `research.strategies`. Hard floors/ceilings from
       opportunity.py apply regardless. On pass the keyword is annotated
       with:
          - `matched_strategies`: list of strategy names it qualifies for
          - `primary_strategy`: first match (for logging/display; actual
            slot allocation happens later in pipeline.py)
          - `tier`: same as primary_strategy (legacy compat — old code
            paths that read `kw["tier"]` keep working)

       Legacy `research.tiers` config (pre-portfolio schema) is still
       honored if `research.strategies` isn't present.

    2. Binary (legacy): competitor_count <= max_competitors AND
       differentiation_score >= min_differentiation_score. Kept for users
       who want to A/B the old behavior without editing code.
    """
    from src.research import opportunity

    config = config or AppConfig()
    use_tiered = bool(config.get("research.use_tiered_filter", True))

    if use_tiered:
        # Prefer new `strategies` schema; fall back to legacy `tiers`;
        # fall back to hardcoded DEFAULT_STRATEGIES.
        strategies = config.get("research.strategies")
        if not strategies:
            legacy_tiers = config.get("research.tiers")
            if legacy_tiers:
                # Project the legacy 4-tier schema into the portfolio shape
                # so match_strategies() can consume it uniformly.
                strategies = [
                    {
                        "name": t.get("name", "unknown"),
                        "enabled": True,
                        "slots": 1,
                        "score_weight": opportunity.TIER_WEIGHT.get(
                            str(t.get("name", "")).lower(), 0.7,
                        ),
                        "criteria": {
                            "volume_min": t.get("min_volume"),
                            "volume_max": t.get("max_volume"),
                            "comp_max": t.get("max_competitors"),
                            "diff_min": t.get("min_differentiation"),
                        },
                    }
                    for t in legacy_tiers
                ]
            else:
                strategies = opportunity.DEFAULT_STRATEGIES

        filtered: list[dict] = []
        rejections: dict[str, int] = {}
        for kw in keyword_data:
            vol = float(kw.get("monthly_search_volume", 0) or 0)
            comp_count = int(kw.get("competitor_count", 0) or 0)
            diff_score = float(kw.get("differentiation_score", 0) or 0)
            # Use pytrends slope if present, else derive from Best Sellers
            # RISER/FLAT/SINKER flag. Anything we can't parse → 0.0.
            if "trend_slope" in kw and kw.get("trend_slope") is not None:
                trend_slope = float(kw.get("trend_slope") or 0)
            else:
                trend_slope = opportunity.trend_slope_from_rdc(
                    kw.get("relative_demand_change")
                )

            matches = opportunity.match_strategies(
                volume=vol,
                competitors=comp_count,
                diff_score=diff_score,
                trend_slope=trend_slope,
                strategies=strategies,
            )
            if not matches:
                if vol < opportunity.HARD_VOLUME_FLOOR:
                    reason = "below hard volume floor"
                elif comp_count > opportunity.HARD_COMPETITOR_CEILING:
                    reason = "above hard competitor ceiling"
                else:
                    reason = "no strategy matched"
                rejections[reason] = rejections.get(reason, 0) + 1
                logger.debug(
                    "Filtered '%s': %s (vol=%.0f, comp=%d, diff=%.0f, trend=%+.2f)",
                    kw.get("keyword", "?"), reason, vol, comp_count,
                    diff_score, trend_slope,
                )
                continue

            kw["matched_strategies"] = matches
            kw["primary_strategy"] = matches[0]
            kw["tier"] = matches[0]  # legacy-compat alias
            filtered.append(kw)

        coverage = opportunity.summarize_strategy_coverage(filtered)
        primary_hist = opportunity.summarize_strategy_distribution(filtered)
        logger.info(
            "Competition filter (portfolio): %d/%d passed — primary=%s, "
            "coverage=%s, rejections=%s",
            len(filtered), len(keyword_data), primary_hist, coverage, rejections,
        )
        return filtered

    # ─── Legacy binary path ─────────────────────────────────────────────
    max_comp = config.max_competitors
    min_diff = config.min_differentiation_score

    filtered = []
    for kw in keyword_data:
        comp_count = kw.get("competitor_count", 0)
        diff_score = kw.get("differentiation_score", 0)

        if comp_count > max_comp:
            logger.debug(
                "Filtered '%s': %d competitors > max %d",
                kw.get("keyword", "?"), comp_count, max_comp
            )
            continue

        if diff_score < min_diff:
            logger.debug(
                "Filtered '%s': differentiation %.0f < min %.0f",
                kw.get("keyword", "?"), diff_score, min_diff
            )
            continue

        filtered.append(kw)

    logger.info(
        "Competition filter (binary): %d/%d passed (max comp: %d, min diff: %.0f)",
        len(filtered), len(keyword_data), max_comp, min_diff
    )
    return filtered


def _extract_price(item: dict) -> Optional[float]:
    """Extract price from a Shopping result item."""
    # Try extracted_price first (numeric)
    price = item.get("extracted_price")
    if price and isinstance(price, (int, float)):
        return float(price)

    # Try price string
    price_str = item.get("price", "")
    if price_str:
        # Remove currency symbols and parse
        clean = price_str.replace("€", "").replace("$", "").replace("£", "")
        clean = clean.replace(",", ".").strip()
        try:
            return float(clean)
        except ValueError:
            pass

    return None


def _count_unique_products(titles: list[str]) -> int:
    """
    Estimate the number of unique products from product titles.
    Uses simple word overlap to determine if two titles refer to the same product.
    """
    if not titles:
        return 0

    # Tokenize and compare
    unique_groups = []

    for title in titles:
        words = set(title.lower().split())
        # Remove very common words
        stop_words = {"für", "mit", "und", "der", "die", "das", "in", "von",
                      "for", "with", "and", "the", "of", "-", "|", ","}
        words -= stop_words

        found_match = False
        for group in unique_groups:
            # Calculate Jaccard similarity
            overlap = len(words & group) / len(words | group) if (words | group) else 0
            if overlap > 0.5:  # >50% word overlap = same product
                group.update(words)
                found_match = True
                break

        if not found_match:
            unique_groups.append(words)

    return len(unique_groups)


def _median(values: list[float]) -> float:
    """Calculate median of a list."""
    if not values:
        return 0
    sorted_values = sorted(values)
    n = len(sorted_values)
    if n % 2 == 0:
        return (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
    return sorted_values[n // 2]
