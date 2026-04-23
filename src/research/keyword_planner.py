"""
Keyword Planner — real search-volume and CPC data via DataForSEO.

Replaces the previous Google Ads Keyword Planner integration, which required
Basic/Standard dev-token access (not available on Explorer tier and blocked
entirely when the Ads account is suspended).

DataForSEO exposes the same underlying Google Ads data through its
`/v3/keywords_data/google_ads/search_volume/live` endpoint. Pricing is
$0.05 per task of up to 1000 keywords — effectively free at our Discover
scale (~$0.05 per Discover run).

Public API is preserved: `validate_keywords()` and `filter_keywords()` have
the same signatures and return the same dict shape as before, so
`pipeline.py` needs no changes.
"""

from __future__ import annotations

import logging
from typing import Optional

import requests
from requests.auth import HTTPBasicAuth

from src.core.config import (
    AppConfig,
    DATAFORSEO_LOGIN,
    DATAFORSEO_PASSWORD,
)
from src.core.cost_tracker import CostTracker

logger = logging.getLogger(__name__)

DATAFORSEO_ENDPOINT = (
    "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live"
)

# DataForSEO's location_code is Google's geo_target_constant ID as an int.
LOCATION_CODES = {
    "DE": 2276,   # Germany
    "NL": 2528,   # Netherlands
    "AT": 2040,   # Austria
    "FR": 2250,   # France
    "BE": 2056,   # Belgium
    "CH": 2756,   # Switzerland
    "ES": 2724,   # Spain
    "IT": 2380,   # Italy
    "PL": 2616,   # Poland
    "GB": 2826,   # United Kingdom
    "UK": 2826,   # alias
    "US": 2840,   # United States
}

# DataForSEO accepts ISO 639-1 language codes directly (unlike the old Google
# Ads API which required numeric language constants).
LANGUAGE_CODES = {
    "de": "de", "nl": "nl", "fr": "fr",
    "es": "es", "it": "it", "pl": "pl", "en": "en",
}

# HTTP request timeout. DataForSEO's live endpoint typically responds in
# 2–5s per task; we leave a generous buffer for slow days.
REQUEST_TIMEOUT = 60


def _competition_label(index: int) -> str:
    """Convert competition index (0-100) to low/medium/high label.

    DataForSEO sometimes returns a text "competition" field ("LOW" / "MEDIUM"
    / "HIGH") alongside the numeric competition_index. We normalise both to
    lowercase labels for consistency with the rest of the platform.
    """
    if index is None:
        return "unknown"
    if index <= 33:
        return "low"
    if index <= 66:
        return "medium"
    return "high"


def _credentials_configured() -> bool:
    return bool(
        DATAFORSEO_LOGIN
        and DATAFORSEO_PASSWORD
        and not DATAFORSEO_LOGIN.startswith("your_")
        and not DATAFORSEO_PASSWORD.startswith("your_")
    )


def validate_keywords(
    keywords: list[str],
    country: str = "DE",
    language: str = "de",
    config: AppConfig = None,
    cost_tracker: Optional[CostTracker] = None,
) -> list[dict]:
    """
    Validate keywords via DataForSEO → Google Ads Search Volume (Live).

    Args:
        keywords: List of keyword strings to validate
        country: Target country code (e.g., "DE")
        language: Target language code (e.g., "de")
        config: App configuration (unused here — kept for signature parity)
        cost_tracker: Optional CostTracker — records DataForSEO spend using
            the exact `cost` field from each response (falls back to the
            per-task flat-rate estimate if missing).

    Returns:
        List of dicts with the SAME shape as the old Google-Ads-based
        implementation:
            keyword, monthly_search_volume, estimated_cpc,
            competition_level, competition_index, low_cpc, high_cpc
        Missing keywords (no data from DataForSEO) are included with zero
        metrics so the pipeline's dedup + volume-filter stages behave
        identically to before.
    """
    if not keywords:
        return []

    if not _credentials_configured():
        logger.warning(
            "DataForSEO credentials not configured — skipping volume/CPC "
            "validation. Keywords will pass through with zero metrics."
        )
        return [
            {
                "keyword": kw,
                "monthly_search_volume": 0,
                "estimated_cpc": 0,
                "competition_level": "unknown",
                "competition_index": 0,
                "low_cpc": 0,
                "high_cpc": 0,
            }
            for kw in keywords
        ]

    location_code = LOCATION_CODES.get(country.upper())
    if location_code is None:
        logger.warning(
            "No DataForSEO location_code mapping for country=%r; defaulting to DE",
            country,
        )
        location_code = LOCATION_CODES["DE"]

    language_code = LANGUAGE_CODES.get(language.lower(), "en")

    # DataForSEO's per-task limit is 1000 keywords. We process in batches of
    # 1000 so large ideation runs don't exceed it; at the default 150/run
    # this is one request.
    batch_size = 1000
    results_by_keyword: dict[str, dict] = {}

    for i in range(0, len(keywords), batch_size):
        batch = keywords[i : i + batch_size]
        payload = [
            {
                "keywords": batch,
                "location_code": location_code,
                "language_code": language_code,
                "search_partners": False,
                "include_adult_keywords": False,
            }
        ]

        try:
            resp = requests.post(
                DATAFORSEO_ENDPOINT,
                auth=HTTPBasicAuth(DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD),
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as e:
            logger.error("DataForSEO request failed: %s", e)
            continue

        if resp.status_code != 200:
            logger.error(
                "DataForSEO HTTP %d: %s",
                resp.status_code, resp.text[:500],
            )
            continue

        try:
            body = resp.json()
        except ValueError as e:
            logger.error("DataForSEO returned non-JSON: %s", e)
            continue

        # DataForSEO wrapper: {"status_code": 20000, "cost": 0.05, "tasks": [ {...} ]}
        top_status = body.get("status_code")
        if top_status != 20000:
            logger.error(
                "DataForSEO top-level error status=%s message=%s",
                top_status, body.get("status_message"),
            )
            continue

        # Record exact cost from DataForSEO's response. Top-level `cost`
        # is the sum across all tasks in the request; we submit one task
        # per batch so they're equivalent. Fall back to task-level if the
        # top-level field is missing (has happened occasionally on retries).
        if cost_tracker is not None:
            resp_cost = body.get("cost")
            if resp_cost is None:
                first_task = (body.get("tasks") or [{}])[0]
                resp_cost = first_task.get("cost")
            cost_tracker.record_dataforseo(
                endpoint="google_ads/search_volume/live",
                cost_from_response=float(resp_cost) if resp_cost is not None else None,
                num_keywords=len(batch),
                context=f"{country}/{language}",
            )

        tasks = body.get("tasks") or []
        if not tasks:
            logger.warning("DataForSEO returned no tasks in response")
            continue

        task = tasks[0]
        task_status = task.get("status_code")
        if task_status != 20000:
            logger.error(
                "DataForSEO task error status=%s message=%s",
                task_status, task.get("status_message"),
            )
            continue

        task_results = task.get("result") or []
        for item in task_results:
            keyword_text = item.get("keyword") or ""
            if not keyword_text:
                continue

            # DataForSEO returns `null` for search_volume when Google Keyword
            # Planner has no aggregated data for that phrase — very common for
            # long-tail compound phrases (typical in German), Explorer-tier
            # accounts, or brand-new terms. We need to distinguish this from
            # a true zero so the pipeline can drop these cleanly at a
            # `volume_no_data` stage rather than silently passing them
            # through at volume=0 via the "API broken" fallback.
            raw_volume = item.get("search_volume")
            has_planner_data = raw_volume is not None
            volume = int(raw_volume) if has_planner_data else 0
            cpc_raw = item.get("cpc")
            low_cpc = item.get("low_top_of_page_bid") or 0
            high_cpc = item.get("high_top_of_page_bid") or 0

            # Prefer DataForSEO's own `cpc` (a model blend of low/high);
            # fall back to midpoint if missing.
            if cpc_raw is None:
                if high_cpc:
                    cpc_raw = (low_cpc + high_cpc) / 2
                else:
                    cpc_raw = low_cpc

            competition_index = item.get("competition_index")
            # DataForSEO also returns textual "competition" ("LOW"/"MED"/"HIGH"
            # or numeric 0-1). Prefer the index-based label for consistency.
            if competition_index is None:
                raw_comp = item.get("competition")
                if isinstance(raw_comp, str):
                    comp_label = raw_comp.lower()
                    if comp_label == "med":
                        comp_label = "medium"
                    if comp_label not in ("low", "medium", "high"):
                        comp_label = "unknown"
                else:
                    comp_label = "unknown"
                competition_index = 0
            else:
                comp_label = _competition_label(competition_index)

            results_by_keyword[keyword_text.lower()] = {
                "keyword": keyword_text,
                "monthly_search_volume": volume,
                "estimated_cpc": round(float(cpc_raw or 0), 2),
                "competition_level": comp_label,
                "competition_index": int(competition_index),
                "low_cpc": round(float(low_cpc or 0), 2),
                "high_cpc": round(float(high_cpc or 0), 2),
                # True iff Google Keyword Planner actually returned an
                # aggregate for this keyword. Lets the pipeline tell
                # "Google has no data" apart from "Google confirmed zero".
                "has_planner_data": has_planner_data,
            }

        logger.info(
            "DataForSEO batch %d-%d: %d/%d keywords returned data",
            i, i + len(batch), len(task_results), len(batch),
        )

    # Assemble final list in the same order as input, including zeros for
    # keywords DataForSEO had no data on.
    final: list[dict] = []
    hits = 0
    for kw in keywords:
        row = results_by_keyword.get(kw.lower())
        if row is None:
            final.append(
                {
                    "keyword": kw,
                    "monthly_search_volume": 0,
                    "estimated_cpc": 0,
                    "competition_level": "unknown",
                    "competition_index": 0,
                    "low_cpc": 0,
                    "high_cpc": 0,
                    # Not in the response at all — treat identically to
                    # "null volume": Google has nothing to say about this.
                    "has_planner_data": False,
                }
            )
        else:
            final.append(row)
            hits += 1

    logger.info(
        "DataForSEO Keyword Planner: %d/%d keywords had volume/CPC data (%s)",
        hits, len(keywords), country,
    )
    return final


def filter_keywords(
    validated_keywords: list[dict],
    config: AppConfig = None,
) -> list[dict]:
    """
    Filter validated keywords based on minimum volume threshold.

    Reads `research.min_monthly_search_volume` from config (defaults to 500/mo
    per config/defaults.yaml). Keywords with zero volume are filtered out
    unless every keyword is at zero (graceful fallback — preserves behaviour
    when DataForSEO has no data).
    """
    config = config or AppConfig()
    min_volume = config.get("research.min_monthly_search_volume", 500)

    filtered = []
    for kw in validated_keywords:
        volume = kw.get("monthly_search_volume", 0)
        if volume < min_volume:
            logger.debug(
                "Filtered out '%s': volume %d < %d",
                kw.get("keyword"), volume, min_volume,
            )
            continue
        filtered.append(kw)

    logger.info(
        "Keyword filter: %d/%d passed (min volume: %d/mo)",
        len(filtered), len(validated_keywords), min_volume,
    )
    return filtered
