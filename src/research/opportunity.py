"""
Opportunity scoring & strategy-portfolio qualification for Discover candidates.

A single (max_competitors, min_volume) threshold can't express the business
reality that several very different candidate shapes are ALL interesting —
they just need different bets placed on them. The Strategy Portfolio System
lets each shape be a first-class "strategy" with its own criteria and its
own slice of the per-run output budget, so one Discover run produces a
diversified basket instead of 5 products that all look the same.

Default strategies (DE Google Shopping, evidence-grounded):

  premium          : ≥ 3 000/mo,  ≤ 12 comp, ≥ 12 diff
                     Big niche, thin SERP — textbook blue ocean.
  volume           : 1 500-2 999, ≤ 18 comp, ≥ 10 diff
                     Mid niche, tolerable SERP pressure when headroom is big.
  small            : 500-1 499,   ≤ 12 comp, ≥  8 diff
                     Defensible micro-niche; fastest path to first sale.
  micro            : 250-499,     ≤  6 comp, ≥  6 diff
                     Emerging / edge-case; thin SERP mandatory.
  saturated_volume : ≥ 5 000/mo,  ≤ 25 comp, ≥ 12 diff
                     Big-pie bet — accept harder SERP if volume is large
                     enough that even low IS hits conversion minimums.
  rising_niche     : ≥ 500/mo,    ≤ 22 comp, trend_slope ≥ +0.3
                     Trend-momentum bet — advertisers lag Google's data;
                     this captures pre-saturation windows.

A keyword can match MULTIPLE strategies — e.g. (vol=6000, comp=10, diff=14)
qualifies for both `premium` AND `saturated_volume`. Per-run slot allocation
uses the keyword's PRIMARY strategy (the first match in config order) so
each strategy gets its own slice of the output budget.

Hard floors/ceilings still apply regardless of strategy:
  - volume < 250/mo      → reject (can't train PMax smart-bidding)
  - competitors > 25     → reject (CPC inflation eats margin)

All functions in this module are pure — no I/O, no mutation.
"""

from __future__ import annotations

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Defaults
#
# The portfolio schema: a list of strategy dicts, tried in order. Each has:
#   - name (str)                — stable identifier for logging + UI
#   - criteria (dict)           — any subset of {volume_min, volume_max,
#                                                comp_max, diff_min,
#                                                trend_slope_min}
#   - slots (int)               — max products this strategy gets per run
#   - score_weight (float)      — base multiplier in opportunity_score
#   - enabled (bool, opt)       — defaults True; false = silently skipped
#
# `tiers` is preserved as a legacy alias for backward compat — the old
# 4-tier list is a proper subset of the new 6-strategy portfolio.
# ---------------------------------------------------------------------------

DEFAULT_STRATEGIES: list[dict] = [
    {
        "name": "premium",
        "criteria": {
            "volume_min": 3000,
            "comp_max": 12,
            "diff_min": 12,
        },
        "slots": 1,
        "score_weight": 1.0,
        "enabled": True,
    },
    {
        "name": "volume",
        "criteria": {
            "volume_min": 1500,
            "volume_max": 3000,
            "comp_max": 18,
            "diff_min": 10,
        },
        "slots": 1,
        "score_weight": 0.85,
        "enabled": True,
    },
    {
        "name": "small",
        "criteria": {
            "volume_min": 500,
            "volume_max": 1500,
            "comp_max": 12,
            "diff_min": 8,
        },
        "slots": 1,
        "score_weight": 0.70,
        "enabled": True,
    },
    {
        "name": "micro",
        "criteria": {
            "volume_min": 250,
            "volume_max": 500,
            "comp_max": 6,
            "diff_min": 6,
        },
        "slots": 1,
        "score_weight": 0.55,
        "enabled": True,
    },
    {
        "name": "saturated_volume",
        "criteria": {
            "volume_min": 5000,
            "comp_max": 25,
            "diff_min": 12,
        },
        "slots": 1,
        "score_weight": 0.90,
        "enabled": True,
    },
    {
        "name": "rising_niche",
        "criteria": {
            # volume_min matches the HARD FLOOR (250) — rising_niche is the
            # loosest bet by design. Below 500/mo a single keyword can't
            # train PMax alone, but the real-world play is to bundle 3-5
            # low-vol rising keywords into one campaign and train on the
            # aggregate. Dropping to 250 lets those bundleable candidates
            # qualify; the log(vol) component of opportunity_score already
            # down-ranks 250-vol candidates ~30% below 2500-vol ones, so
            # they naturally sink unless there's nothing better this run.
            "volume_min": 250,
            # comp_max matches the hard ceiling (25) — on RISER trends we
            # explicitly WANT the 20-25 comp band because that's the
            # pre-saturation window the thesis targets. DE Shopping
            # structurally clusters here; a tighter cap just kills the bet.
            "comp_max": 25,
            # Minimal diff floor. `rising_niche` has the loosest diff
            # requirement of any strategy because the thesis is trend-
            # driven, not convergence-driven — BUT diff<5 means the SERP is
            # totally SKU-fragmented (39 distinct products across ~25
            # sellers), so there's no dominant product to dropship. Even on
            # a rising trend, that's an un-runnable bet.
            "diff_min": 5,
            "trend_slope_min": 0.3,
        },
        "slots": 1,
        "score_weight": 0.80,
        "enabled": True,
    },
]

# ─── Legacy aliases (kept for back-compat) ──────────────────────────────────
# Old code paths referencing DEFAULT_TIERS / TIER_WEIGHT continue to work by
# projecting the portfolio schema onto the pre-portfolio shape. Remove once
# all callers migrate to DEFAULT_STRATEGIES.
DEFAULT_TIERS: list[dict] = [
    {
        "name": s["name"],
        "min_volume": s["criteria"].get("volume_min", 0),
        "max_volume": s["criteria"].get("volume_max"),
        "max_competitors": s["criteria"].get("comp_max", 999),
        "min_differentiation": s["criteria"].get("diff_min", 0),
    }
    for s in DEFAULT_STRATEGIES
    if s["name"] in {"premium", "volume", "small", "micro"}
]

TIER_WEIGHT: dict[str, float] = {
    s["name"]: float(s["score_weight"])
    for s in DEFAULT_STRATEGIES
}

# Tier A recalibration (2026-04-22): dropped hard volume floor 250 → 150
# to let the `micro` strategy capture cluster-demand opportunities. The
# strategy table is still the primary gate; only `micro` accepts sub-250
# today. Other strategies have their own volume_min that won't drop below
# 500 without an explicit config change.
# TODO: implement cluster-demand aggregation so a lone 150-vol keyword
# doesn't train PMax alone.
HARD_VOLUME_FLOOR = 150
# 2026-04-22 Tier B: raised 25 → 28 after run 2 showed DE Shopping pages
# structurally cluster at 17-28 competitor counts (mean ~22, 90th
# percentile 26). The old ceiling was killing saturated_volume bets
# with 26-28 advertisers — exactly the pre-saturation window the
# `saturated_volume` and `rising_niche` strategies are designed to
# capture. The ceiling is the "CPC inflation eats margin" guard;
# 28 is the observed plateau before SERPs go truly junk.
HARD_COMPETITOR_CEILING = 28


# ---------------------------------------------------------------------------
# Strategy matching
# ---------------------------------------------------------------------------

def _meets_criteria(
    criteria: dict,
    vol: float,
    comp: float,
    diff: float,
    trend_slope: float,
) -> bool:
    """Test a single (vol, comp, diff, trend_slope) tuple against one criteria dict."""
    vmin = criteria.get("volume_min")
    if vmin is not None and vol < float(vmin):
        return False

    vmax = criteria.get("volume_max")
    if vmax is not None and vol >= float(vmax):
        return False

    cmax = criteria.get("comp_max")
    if cmax is not None and comp > float(cmax):
        return False

    dmin = criteria.get("diff_min")
    if dmin is not None and diff < float(dmin):
        return False

    tmin = criteria.get("trend_slope_min")
    if tmin is not None and trend_slope < float(tmin):
        return False

    return True


def match_strategies(
    volume: float,
    competitors: float,
    diff_score: float,
    trend_slope: float = 0.0,
    strategies: Optional[list[dict]] = None,
    hard_floor: int = HARD_VOLUME_FLOOR,
    hard_ceiling: int = HARD_COMPETITOR_CEILING,
) -> list[str]:
    """
    Return every enabled strategy whose criteria match the candidate, in
    config order (so the first element is the "primary" strategy for slot
    allocation purposes). Empty list = reject.

    Rejection order:
      1. volume < hard_floor           → []
      2. competitors > hard_ceiling    → []
      3. no strategy matches           → []

    Strategies with `enabled: false` are silently ignored — useful for the
    Settings UI where a user can toggle individual bets on/off without
    deleting the config.
    """
    strategies = strategies if strategies is not None else DEFAULT_STRATEGIES

    if volume < hard_floor:
        return []
    if competitors > hard_ceiling:
        return []

    matched: list[str] = []
    for strat in strategies:
        if not strat.get("enabled", True):
            continue
        criteria = strat.get("criteria", {}) or {}
        if _meets_criteria(criteria, volume, competitors, diff_score, trend_slope):
            matched.append(str(strat.get("name", "unknown")))

    return matched


def classify_tier(
    volume: float,
    competitors: int,
    diff_score: float,
    tiers: Optional[list[dict]] = None,
    hard_floor: int = HARD_VOLUME_FLOOR,
    hard_ceiling: int = HARD_COMPETITOR_CEILING,
) -> Optional[str]:
    """
    Legacy shim — returns the FIRST matching strategy name or None. Kept so
    old callers (and user-provided config using the pre-portfolio `tiers`
    schema) continue working without modification.

    New code should call `match_strategies()` directly to get the full list.
    """
    # If a legacy `tiers` list was passed in (the old shape with
    # min_volume/max_volume/max_competitors/min_differentiation keys), project
    # it onto the new strategy shape on the fly.
    if tiers is not None:
        projected = []
        for t in tiers:
            projected.append({
                "name": t.get("name", "unknown"),
                "criteria": {
                    "volume_min": t.get("min_volume"),
                    "volume_max": t.get("max_volume"),
                    "comp_max": t.get("max_competitors"),
                    "diff_min": t.get("min_differentiation"),
                },
                "enabled": True,
            })
        strategies = projected
    else:
        strategies = None  # use DEFAULT_STRATEGIES

    matches = match_strategies(
        volume=float(volume),
        competitors=float(competitors),
        diff_score=float(diff_score),
        strategies=strategies,
        hard_floor=hard_floor,
        hard_ceiling=hard_ceiling,
    )
    return matches[0] if matches else None


# ---------------------------------------------------------------------------
# Opportunity scoring
# ---------------------------------------------------------------------------

def _strategy_weight(
    name: str,
    strategies: Optional[list[dict]] = None,
) -> float:
    """Look up a strategy's score_weight by name, default 0.7 if not found."""
    strategies = strategies if strategies is not None else DEFAULT_STRATEGIES
    for s in strategies:
        if str(s.get("name", "")).lower() == name.lower():
            return float(s.get("score_weight", 0.7))
    return 0.7


def opportunity_score(
    kw_data: dict,
    strategies: Optional[list[dict]] = None,
    tier_weights: Optional[dict[str, float]] = None,
) -> float:
    """
    Composite opportunity score for ranking candidates within a strategy's
    slot allocation. Higher is better. Purely a ranking signal — don't
    compare scores across runs; the scale is arbitrary.

    Reads the candidate's `matched_strategies` annotation (list of strategy
    names from `match_strategies()`) and uses the BEST score_weight among
    matches. Falls back to `tier` (legacy) or "small" (default 0.7).

    Formula (all components bounded so no single dimension dominates):

        score = strategy_weight
              × log1p(volume)                      # headroom
              × (diff_score / 50)                  # differentiation
              × (1 / (cpc + 0.5))                  # cheap clicks
              × (1 + 0.3 × trend_slope)            # momentum [-1,+1]
              × saturation_factor                  # SERP duplication

    Negative return (-1.0) means "sink to tail" — used for rows with zero
    volume or no AliExpress match (contract preserved from pre-portfolio
    code to keep the pipeline's sort stable).
    """
    vol = float(kw_data.get("monthly_search_volume", 0) or 0)
    cpc = float(kw_data.get("estimated_cpc", 0) or 0)
    diff = float(kw_data.get("differentiation_score", 0) or 0) or 50.0
    ali_price = float((kw_data.get("aliexpress_match") or {}).get("price", 0) or 0)

    # Tail contract — unchanged.
    if vol <= 0 or ali_price <= 0:
        return -1.0

    # Resolve strategy weight. Prefer the new `matched_strategies` annotation
    # (list). If multiple match, take the max weight — that's the strongest
    # thesis we have on this candidate. Fall back to legacy `tier` or a
    # neutral default.
    matched = kw_data.get("matched_strategies") or []
    if matched:
        if tier_weights is not None:
            weight = max(tier_weights.get(str(m).lower(), 0.7) for m in matched)
        else:
            weight = max(_strategy_weight(str(m), strategies) for m in matched)
    else:
        legacy_tier = (kw_data.get("tier") or "small").lower()
        if tier_weights is not None:
            weight = tier_weights.get(legacy_tier, 0.7)
        else:
            weight = _strategy_weight(legacy_tier, strategies)

    # Trend slope, bounded.
    trend_slope = float(kw_data.get("trend_slope", 0) or 0)
    trend_slope = max(-1.0, min(1.0, trend_slope))

    # Saturation ratio: unique_products / competitor_count. 1.0 = every
    # advertiser has a distinct product. <1 = SERP is the same product
    # repeated. Default 1.0 if the SerpAPI snapshot didn't yield counts.
    comp_count = float(kw_data.get("competitor_count", 0) or 0)
    unique_count = float(kw_data.get("unique_products", 0) or 0)
    if comp_count > 0 and unique_count > 0:
        saturation = unique_count / comp_count
    else:
        saturation = 1.0
    saturation = max(0.3, min(1.2, saturation))

    volume_component = math.log1p(vol)
    diff_component = max(0.2, diff / 50.0)
    cpc_component = 1.0 / (cpc + 0.5)
    trend_component = 1.0 + 0.3 * trend_slope

    return (
        weight
        * volume_component
        * diff_component
        * cpc_component
        * trend_component
        * saturation
    )


# ---------------------------------------------------------------------------
# Portfolio allocation
# ---------------------------------------------------------------------------

def trend_slope_from_rdc(relative_demand_change: Optional[str]) -> float:
    """
    Map Google Best Sellers `relative_demand_change` to a numeric trend slope:
      RISER  → +1.0
      FLAT   →  0.0
      SINKER → -1.0
    Anything else (None, unknown) → 0.0. Used as the trend_slope input to
    `match_strategies()` when pytrends data isn't available.
    """
    if relative_demand_change is None:
        return 0.0
    rdc = str(relative_demand_change).strip().upper()
    if rdc == "RISER":
        return 1.0
    if rdc == "SINKER":
        return -1.0
    return 0.0


def allocate_slots_by_strategy(
    keywords: list[dict],
    max_total: int,
    strategies: Optional[list[dict]] = None,
    score_fn=None,
) -> list[dict]:
    """
    Given a qualified pool of keywords (each annotated with
    `matched_strategies` from `match_strategies()`), pick up to `max_total`
    winners subject to each strategy's `slots` budget.

    Algorithm:
      1. Sort candidates within each strategy by `score_fn(kw)` desc.
      2. Round-robin across strategies, taking the next-best from each,
         skipping strategies that have already hit their slot cap or are
         out of candidates.
      3. Stop when either max_total is reached OR every strategy is empty.
      4. Prevent double-picking — a keyword that matched multiple strategies
         is claimed by whichever strategy picks it first (in round-robin
         order).

    Returns the winners in selection order. Caller is responsible for
    attaching `primary_strategy` to each row if desired.
    """
    strategies = strategies if strategies is not None else DEFAULT_STRATEGIES
    if score_fn is None:
        score_fn = opportunity_score

    # Build enabled-strategy list with slot budgets. Order matters — it's
    # the round-robin order.
    budget: list[tuple[str, int]] = [
        (str(s["name"]), int(s.get("slots", 1)))
        for s in strategies
        if s.get("enabled", True)
    ]

    # Bucket candidates by EVERY strategy they match (so a multi-match
    # candidate can be picked by any qualifying strategy). Sort each bucket
    # by score desc — computed once up front.
    buckets: dict[str, list[tuple[float, dict]]] = {
        name: [] for name, _ in budget
    }
    for kw in keywords:
        matched = kw.get("matched_strategies") or []
        if not matched:
            continue
        score = score_fn(kw)
        for m in matched:
            if m in buckets:
                buckets[m].append((score, kw))
    for name in buckets:
        buckets[name].sort(key=lambda pair: pair[0], reverse=True)

    # Track indices into each bucket and picked identities to prevent
    # double-picking. We use id(kw) because dicts aren't hashable and
    # candidates flow through the pipeline by reference.
    cursor: dict[str, int] = {name: 0 for name, _ in budget}
    taken: set[int] = set()
    picked_per_strategy: dict[str, int] = {name: 0 for name, _ in budget}
    winners: list[dict] = []

    # Round-robin until budgets exhausted or max_total hit.
    while len(winners) < max_total:
        progressed = False
        for name, slots in budget:
            if len(winners) >= max_total:
                break
            if picked_per_strategy[name] >= slots:
                continue

            bucket = buckets[name]
            # Advance this strategy's cursor past anything already taken.
            while cursor[name] < len(bucket):
                score, kw = bucket[cursor[name]]
                if id(kw) in taken:
                    cursor[name] += 1
                    continue
                # Claim it.
                taken.add(id(kw))
                kw["primary_strategy"] = name
                winners.append(kw)
                picked_per_strategy[name] += 1
                cursor[name] += 1
                progressed = True
                break

        if not progressed:
            break  # every strategy is full or out of candidates

    return winners


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------

def summarize_tier_distribution(keywords: list[dict]) -> dict[str, int]:
    """
    Legacy histogram by `tier` field. Kept for pre-portfolio callers.
    New code should prefer `summarize_strategy_distribution`.
    """
    counts: dict[str, int] = {}
    for kw in keywords:
        t = (kw.get("tier") or "unassigned").lower()
        counts[t] = counts.get(t, 0) + 1
    return counts


def summarize_strategy_distribution(keywords: list[dict]) -> dict[str, int]:
    """
    Histogram over `primary_strategy` (if set) else first of
    `matched_strategies` else 'unassigned'. Useful for logging which
    bets the qualified pool covers and which slot budgets would be dry.
    """
    counts: dict[str, int] = {}
    for kw in keywords:
        primary = kw.get("primary_strategy")
        if not primary:
            matched = kw.get("matched_strategies") or []
            primary = matched[0] if matched else "unassigned"
        primary = str(primary).lower()
        counts[primary] = counts.get(primary, 0) + 1
    return counts


def summarize_strategy_coverage(keywords: list[dict]) -> dict[str, int]:
    """
    How many candidates match each strategy (counting each match, so a
    multi-match candidate is counted in every bucket it qualifies for).
    Useful for spotting strategies with empty pools BEFORE slot allocation.
    """
    counts: dict[str, int] = {}
    for kw in keywords:
        for m in (kw.get("matched_strategies") or []):
            key = str(m).lower()
            counts[key] = counts.get(key, 0) + 1
    return counts
