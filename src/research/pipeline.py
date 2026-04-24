"""
Research Pipeline Orchestrator.
Coordinates the full keyword-to-sourcing flow:
1. LLM keyword ideation
2. Keyword Planner validation
3. Competition analysis (SerpAPI)
4. AliExpress product matching
5. Write to Sheet with status = sourcing
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Callable, Optional

from src.core.config import AppConfig
from src.core.cost_tracker import CostTracker
from src.core.interfaces import DataStore
from src.core.models import (
    KeywordResearch, Product, ProductStatus, ActionLog, ActionType,
    Notification, ResearchFeedback, ResearchSource
)
from src.research import (
    llm_ideation,
    keyword_planner,
    competition,
    aliexpress,
    google_best_sellers,
    google_trends,
)

logger = logging.getLogger(__name__)


class ResearchPipeline:
    """
    Orchestrates the full research pipeline from keyword generation to product candidate.
    """

    def __init__(self, data_store: DataStore, config: AppConfig = None):
        self.store = data_store
        self.config = config or AppConfig()

    def run_full_pipeline(
        self,
        country: str = "DE",
        language: str = "de",
        progress_cb: Optional[Callable[[str, int, int], None]] = None,
    ) -> dict:
        """
        Run the full research pipeline for a single country.

        Args:
            progress_cb: Optional callback `(stage_label, current, total)` fired
                at each major pipeline step. `current` / `total` default to 0
                for batched stages; inside the per-keyword loops (competition,
                AliExpress) we emit `(i+1, n)` so callers can render a real
                "3/10 keywords" counter. Exceptions from the callback are
                swallowed — progress reporting must never break the run.

        Returns:
            dict with pipeline statistics + a `dropped_keywords` audit trail so
            callers can answer "why did nothing land in the Sheet?" without
            having to re-read the logs.

        Drop-tracking format:
            stats["dropped_keywords"] = [
                {"keyword": "kabellose Ohrhörer", "stage": "dedup",     "reason": "already exists"},
                {"keyword": "x",                  "stage": "price",     "reason": "€12 < min €25"},
                {"keyword": "y",                  "stage": "aliexpress","reason": "no supplier matched"},
                ...
            ]
        Every keyword that doesn't make it to the Sheet should show up here
        with the exact filter that killed it. This is the number-one fix for
        "I burned SerpAPI credits and got nothing" — you can now see where.
        """
        # Cost tracker — one per country run. Records every paid API hit
        # (Claude ideation, DataForSEO volume, SerpAPI competition) with
        # exact USD cost. Persisted to the API Costs sheet at the end of
        # this method and summarised into `stats['cost_*']` so the UI can
        # display it without re-reading the sheet.
        run_id = f"disc_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{country.lower()}"
        cost_tracker = CostTracker(run_id=run_id, run_type="discover")

        stats = {
            "country": country,
            "run_id": run_id,
            "keywords_generated": 0,
            "keywords_passed_length": 0,   # survivors of the max-words filter
            "keywords_passed_llm_price": 0,  # survivors of the LLM price pre-filter
            "keywords_passed_llm_quality": 0,  # survivors of the LLM competition/sourcing/type pre-filter
            "keywords_passed_llm_qa": 0,   # survivors of the batched Claude QA (Layer 3)
            "keywords_validated": 0,
            "keywords_with_planner_data": 0,  # had real volume/CPC from Google
            "keywords_passed_volume": 0,
            "keywords_passed_cpc": 0,      # survivors of the max-CPC filter
            "keywords_passed_competition": 0,
            "keywords_passed_price_filter": 0,
            "products_matched": 0,
            "keywords_passed_economics": 0,  # survivors of the per-product economics gate (max_allowed_cpc / min_net_margin)
            "keywords_pending_manual_review": 0,  # otherwise-valid keywords with no DS feed match — queued for manual AliExpress lookup
            "products_capped_out": 0,      # dropped by max_products_per_run
            "products_added_to_sourcing": 0,
            "duplicates_skipped": 0,
            "dropped_keywords": [],
            "started_at": datetime.utcnow().isoformat(),
            "finished_at": None,
            "cost_total_usd": 0.0,
            "cost_breakdown": [],
            "cost_summary": "",
        }

        def _drop(keyword: str, stage: str, reason: str) -> None:
            """Record a dropped keyword + log it at INFO so it shows up in the console."""
            stats["dropped_keywords"].append(
                {"keyword": keyword, "stage": stage, "reason": reason}
            )
            logger.info("  ↳ DROP [%s] '%s' — %s", stage, keyword, reason)

        # Ordered list of stage labels we'll emit to `progress_cb`. Built
        # upfront so the callback can format "Step N of M" prefixes even for
        # batched stages that have no per-keyword counter. Layer 3 (Claude
        # QA) is conditional — only appears when `pre_serpapi_cap > 0` —
        # so we resolve that config here and fold it in.
        _pre_cap_configured = int(self.config.get("research.pre_serpapi_cap", 0) or 0) > 0
        _pipeline_stages: list[str] = [
            "Ideating keywords",
            "Fetching search volume",
        ]
        if _pre_cap_configured:
            _pipeline_stages.append("Claude QA pre-SerpAPI")
        _pipeline_stages.extend([
            "Analyzing competition",
            "Matching AliExpress",
            "Writing to Sheet",
        ])
        _total_steps = len(_pipeline_stages)

        def _progress(stage: str, current: int = 0, total: int = 0) -> None:
            """Fire the progress callback with a "Step N/M: stage" prefix so
            the UI always shows where we are, even during batched stages
            that have no per-keyword counter (ideation, volume lookup,
            QA, write). Never raises — the callback is UI glue, not
            business logic, so a faulty one shouldn't cap a $0.30 run."""
            if progress_cb is None:
                return
            # Look up this stage's position. Unknown/ad-hoc labels pass
            # through unprefixed so the callback can still render them.
            try:
                step_idx = _pipeline_stages.index(stage) + 1
                labeled = f"Step {step_idx}/{_total_steps}: {stage}"
            except ValueError:
                labeled = stage
            try:
                progress_cb(labeled, current, total)
            except Exception as e:
                logger.debug("progress_cb raised (ignored): %s", e)

        # Build the "already tried" set FIRST so we can feed it to the LLM
        # as an avoid-list. Without this, the LLM reliably regenerates the
        # same obvious German dropshipping staples every run (LED-Streifen,
        # Luftbefeuchter, Bluetooth-Lautsprecher, …) and the pipeline kills
        # them at the post-ideation dedup step — wasting every output slot.
        # The set is also reused by the downstream dedup filter (step 1b).
        # Fail-open on either read: we'd rather run with a partial blacklist
        # than kill the whole discover run because the sheet hiccuped.
        try:
            existing_keywords = self.store.get_keywords(country=country)
            existing_set = {kw.keyword.lower() for kw in existing_keywords}
        except Exception as e:
            logger.warning("Could not fetch existing keywords for dedup/avoid-list: %s", e)
            existing_keywords = []
            existing_set = set()

        # Historical Research Drops. We split "blacklist for the LLM
        # avoid-list" from "blacklist for the dedup gate" because they
        # serve different purposes:
        #
        # - LLM avoid-list ALWAYS includes drops. No point asking Claude
        #   to re-generate the same idea that failed before; token spend
        #   is the same whether the idea is novel or stale.
        #
        # - Dedup gate is filter-version-sensitive. If the research
        #   filters (volume/comp/diff thresholds, CPC cap, strategy
        #   portfolio) loosen, candidates that died under the old gates
        #   deserve a second look under the new ones. Set
        #   `research.dedup_against_drops: false` after a recalibration
        #   to reset the drop-blacklist for ONE run — failing candidates
        #   just land back in drops, passing ones proceed.
        try:
            dropped_set = self.store.get_dropped_keyword_set(country=country)
        except Exception as e:
            logger.warning("Could not fetch drop history for dedup/avoid-list: %s", e)
            dropped_set = set()

        dedup_against_drops = bool(
            self.config.get("research.dedup_against_drops", True)
        )
        if dedup_against_drops:
            existing_set |= dropped_set
            logger.info(
                "Avoid-list built: %d active keywords + %d historical drops = %d exclusions",
                len(existing_set) - len(dropped_set), len(dropped_set), len(existing_set),
            )
        else:
            logger.info(
                "Avoid-list built: %d active keywords (drop-dedup DISABLED — "
                "%d prior drops will get a second look under current filters)",
                len(existing_set), len(dropped_set),
            )
        # Order matters for the LLM avoid-list (we keep the TAIL under the
        # token cap — see generate_keywords). Put active keywords LAST so
        # the most "currently relevant" failures are the ones Claude sees
        # if we overflow the 400-entry cap. Deterministic order within each
        # bucket (sorted) makes the prompt stable across runs.
        avoid_for_llm = sorted(dropped_set) + sorted(existing_set - dropped_set)

        # Step 1: Keyword Ideation
        # ─────────────────────────────────────────────────────────────
        # Two ideation sources, selected by `research.ideation_source`:
        #   "best_sellers" (default) — fetch real top-selling products
        #       from Google Merchant Center per category, optionally
        #       validate against Google Trends, then ask Claude to
        #       translate the product titles into local search
        #       keywords. Anchors the pipeline in *what is actually
        #       selling on Google Shopping this week* instead of
        #       Claude's training-data priors.
        #   "llm" — legacy behaviour: Claude generates keywords from
        #       scratch with only the avoid-list as context. Kept for
        #       (a) fallback when MC returns an empty result set and
        #       (b) markets where we don't have MC data yet.
        #
        # Either path writes into the same `raw_keywords` list and
        # drops into the shared downstream filter flow below — the
        # pipeline keeps every existing gate (length, price, volume,
        # CPC, competition, economics, AliExpress) untouched.
        target_kw = int(self.config.get("research.keywords_per_run", 150))
        ideation_source = str(
            self.config.get("research.ideation_source", "best_sellers")
        ).lower().strip()
        _progress("Ideating keywords", 0, target_kw)
        logger.info(
            "Step 1: Ideating keywords for %s (%s) — source=%s",
            country, language, ideation_source,
        )

        raw_keywords: list[dict] = []
        feedback = None
        try:
            feedback = self._get_feedback()
        except Exception as e:
            logger.warning("Could not load research feedback: %s", e)

        # Three ideation modes:
        #   "best_sellers" — Best Sellers only (with LLM fallback on empty)
        #   "llm"          — LLM ideation only
        #   "hybrid"       — both sources run in parallel and concatenate.
        #                    The LLM pool covers under-served niches that
        #                    Best Sellers (top-performers-only, by definition)
        #                    can't surface. Best Sellers RISERs carry a
        #                    trend_slope bonus so rising_niche can still
        #                    fire. Downstream dedup handles overlap.
        bs_keywords: list[dict] = []
        llm_keywords: list[dict] = []

        # ── Branch A: Best Sellers + (optional) Trends + LLM translate ──
        if ideation_source in ("best_sellers", "hybrid"):
            try:
                bs_keywords = self._ideate_from_best_sellers(
                    country=country,
                    language=language,
                    target_kw=target_kw,
                    existing_set=existing_set,
                    cost_tracker=cost_tracker,
                    stats=stats,
                )
                logger.info(
                    "Best-Sellers ideation produced %d keywords",
                    len(bs_keywords),
                )
            except Exception as e:
                logger.error(
                    "Best-Sellers ideation failed: %s", e, exc_info=True,
                )
                stats["error_step1_best_sellers"] = str(e)
                bs_keywords = []

            # Empty-state fallback — MC returns 0 rows for new accounts,
            # unaccepted ToS, or disabled APIs. Rather than silently
            # aborting the whole run, fall back to the legacy LLM
            # generator so the pipeline still produces *something* while
            # we fix the Best-Sellers side. Controlled by config so you
            # can force-fail instead (e.g. if you want to catch the
            # regression in CI). Only applies to pure `best_sellers` mode
            # — hybrid already runs LLM so no fallback needed.
            if (
                ideation_source == "best_sellers"
                and not bs_keywords
                and bool(self.config.get(
                    "research.best_sellers_fallback_to_llm", True,
                ))
            ):
                logger.warning(
                    "Best-Sellers returned 0 keywords — falling back to "
                    "legacy LLM ideation"
                )
                stats["best_sellers_fell_back_to_llm"] = True
                ideation_source = "llm"

        # ── Branch B: LLM ideation (fallback, hybrid, or explicit choice) ──
        if ideation_source in ("llm", "hybrid"):
            try:
                category_focus = self.config.get("research.category_focus", [])
                # In hybrid mode we only need the LLM to supply the diversity
                # that Best Sellers can't (under-served niches), so we can
                # run it at minimum-budget to keep the Anthropic spend sane.
                # 2026-04-22: cut hybrid LLM target 30 → 15. Run 2 showed
                # Best Sellers delivers ~210 keywords alone — the hybrid LLM
                # half was burning 9 min of wall time on gateway retries
                # (single 30-keyword request = ~60-90s generation, Anthropic's
                # gateway hard-drops at ~60s). 15 keywords = ~3k output
                # tokens = ~25s generation = well inside the gateway. It's
                # diversity garnish, not the main course.
                llm_target = (
                    15 if ideation_source == "hybrid"
                    else target_kw
                )
                llm_keywords = llm_ideation.generate_keywords(
                    country=country,
                    language=language,
                    num_keywords=llm_target,
                    category_focus=category_focus if category_focus else None,
                    feedback=feedback,
                    config=self.config,
                    cost_tracker=cost_tracker,
                    avoid_keywords=avoid_for_llm,
                )
                logger.info("Generated %d keyword ideas (LLM)", len(llm_keywords))
            except Exception as e:
                logger.error("Step 1 (LLM Ideation) failed: %s", e, exc_info=True)
                llm_keywords = []
                stats["error_step1"] = str(e)

        # Merge the two pools. Best Sellers first so when we dedup
        # downstream the demand-validated candidate wins over the LLM's
        # cold-start equivalent. `_source` is tagged on each row so the
        # funnel log can attribute pass-rate per source if we want.
        for kw in bs_keywords:
            kw.setdefault("_source", "best_sellers")
        for kw in llm_keywords:
            kw.setdefault("_source", "llm")
        raw_keywords = bs_keywords + llm_keywords
        stats["keywords_generated"] = len(raw_keywords)
        stats["keywords_from_best_sellers"] = len(bs_keywords)
        stats["keywords_from_llm"] = len(llm_keywords)
        _progress(
            "Ideating keywords",
            len(raw_keywords),
            max(len(raw_keywords), target_kw),
        )
        if ideation_source == "hybrid":
            logger.info(
                "Hybrid ideation: %d Best Sellers + %d LLM = %d total",
                len(bs_keywords), len(llm_keywords), len(raw_keywords),
            )

        if not raw_keywords:
            logger.warning("No keywords generated, stopping pipeline")
            stats["finished_at"] = datetime.utcnow().isoformat()
            # Early-return paths used to die silently — now they emit the
            # same funnel summary as a successful run so the user always
            # sees where the pipeline stopped.
            self._log_funnel_summary(stats)
            self._finalize_costs(cost_tracker, stats)
            return stats

        # Deduplicate against BOTH the active Keywords tab and the
        # historical Research Drops tab. Two purposes:
        #   1. Don't re-present the same keyword we already have in the
        #      inbox / archive / sourcing queue.
        #   2. Don't spend DataForSEO/SerpAPI/Claude on keywords that
        #      failed a filter in a previous run — volume doesn't
        #      spontaneously appear where there was none, CPC caps don't
        #      loosen retroactively, competition-saturated SERPs don't
        #      become beatable by re-asking. One Sheets read per run
        #      saves recurring API spend.
        #
        # `existing_set` was already built above the ideation step to feed
        # the LLM avoid-list — reusing it here avoids a redundant Sheets
        # read. If that build failed, `existing_set` is just an empty set
        # and dedup becomes a no-op (fail-open).
        keyword_strings = []
        keyword_metadata = {}

        # Length filter — drop keywords with more words than
        # `research.max_keyword_words` before we spend DataForSEO budget on
        # them. Google Ads Keyword Planner (the backing source for
        # DataForSEO) rarely has aggregated volume for phrases of 6+ words,
        # so a long descriptive LLM output is almost guaranteed to come
        # back with volume=null and get passed through the
        # "whole-batch-is-zero" fallback, flooding sourcing with unprovable
        # products. The LLM prompt already targets 2–4 words; this is the
        # hard backstop.
        max_words = int(self.config.get("research.max_keyword_words", 5))
        # Pre-SerpAPI price filter — the LLM ideation step already outputs
        # an `estimated_price_range` per keyword (e.g. "20-40"). If that
        # range doesn't overlap with our economics [min_selling_price,
        # max_selling_price] window, there's no business paying DataForSEO
        # ($0.0015/kw) or SerpAPI ($0.015/kw) to prove what the LLM already
        # told us: this product is priced outside our sell-through band.
        # Conservative semantics: only drop on clear NO-OVERLAP. Missing or
        # unparseable ranges pass through (fail open) so an LLM glitch doesn't
        # nuke the whole batch.
        econ_min = float(self.config.min_selling_price)
        econ_max = float(self.config.max_selling_price)

        def _parse_price_range(raw) -> tuple[float, float] | None:
            """Parse '20-40' / '€20-40' / '20 - 40 EUR' → (20.0, 40.0).
            Returns None on anything we don't recognise — caller treats that
            as 'pass through' so a weird LLM response doesn't cost us drops."""
            if not raw:
                return None
            import re as _re
            nums = _re.findall(r"\d+(?:\.\d+)?", str(raw))
            if len(nums) < 2:
                return None
            try:
                lo, hi = float(nums[0]), float(nums[1])
                if lo > hi:
                    lo, hi = hi, lo
                return (lo, hi)
            except ValueError:
                return None

        for kw in raw_keywords:
            keyword_text = kw.get("keyword", "").strip()
            if not keyword_text:
                continue
            if keyword_text.lower() in existing_set:
                stats["duplicates_skipped"] += 1
                # Record but don't flood the log — dedup drops are usually
                # voluminous and uninteresting unless you're debugging the LLM.
                stats["dropped_keywords"].append(
                    {"keyword": keyword_text, "stage": "dedup",
                     "reason": "already in Keywords sheet"}
                )
                continue
            # Word count by whitespace split — works for every target
            # language we support (hyphenated compounds like "Kinder-Trampolin"
            # count as one word, which is what Google Ads uses anyway).
            word_count = len(keyword_text.split())
            if max_words > 0 and word_count > max_words:
                _drop(
                    keyword_text,
                    "length",
                    f"{word_count} words > max {max_words} (Keyword Planner rarely has data)",
                )
                continue
            # LLM-estimated price pre-filter. Only drop if the LLM's own
            # range is clearly outside the economics window (no overlap).
            if econ_min > 0 or econ_max > 0:
                parsed = _parse_price_range(kw.get("estimated_price_range"))
                if parsed:
                    lo, hi = parsed
                    if (econ_max > 0 and lo > econ_max) or (econ_min > 0 and hi < econ_min):
                        _drop(
                            keyword_text,
                            "llm_price",
                            f"LLM est. €{lo:.0f}-{hi:.0f} outside "
                            f"economics €{econ_min:.0f}-{econ_max:.0f}",
                        )
                        continue
            # LLM-flagged quality pre-filter (Layer 2). The ideation prompt
            # now asks Claude to self-label each keyword with competition
            # signal / sourcing difficulty / product type. If Claude itself
            # says "this is saturated / hard to source / branded / regulated
            # / perishable / counterfeit-risk", we drop it for free before
            # paying a cent on any paid API. Missing/unrecognised values
            # fail OPEN (pass through) so a model glitch doesn't nuke the
            # whole batch.
            #
            # `kill_*` config keys let the user relax these gates (e.g.
            # `kill_high_competition: false` if they want to test saturated
            # niches). Default: all three gates on.
            if bool(self.config.get("research.kill_high_competition", True)):
                if str(kw.get("competition_signal", "")).lower() == "high":
                    _drop(keyword_text, "llm_quality",
                          "LLM flagged competition_signal=high (saturated SERP)")
                    continue
            if bool(self.config.get("research.kill_hard_sourcing", True)):
                if str(kw.get("sourcing_difficulty", "")).lower() == "hard":
                    _drop(keyword_text, "llm_quality",
                          "LLM flagged sourcing_difficulty=hard (thin supplier base)")
                    continue
            blocked_types = {"branded", "regulated", "perishable", "counterfeit_risk"}
            ptype = str(kw.get("product_type", "")).lower()
            if ptype in blocked_types:
                _drop(keyword_text, "llm_quality",
                      f"LLM flagged product_type={ptype} (not dropship-friendly)")
                continue
            keyword_strings.append(keyword_text)
            keyword_metadata[keyword_text.lower()] = kw

        stats["keywords_passed_length"] = len(keyword_strings) + sum(
            1 for d in stats["dropped_keywords"] if d["stage"] in ("llm_price", "llm_quality")
        )
        stats["keywords_passed_llm_price"] = len(keyword_strings) + sum(
            1 for d in stats["dropped_keywords"] if d["stage"] == "llm_quality"
        )
        stats["keywords_passed_llm_quality"] = len(keyword_strings)
        logger.info(
            "%d unique new keywords after dedup + length + LLM-price + LLM-quality filters "
            "(skipped %d duplicates, %d too long, %d outside LLM price range, %d LLM-quality flagged)",
            len(keyword_strings),
            stats["duplicates_skipped"],
            sum(1 for d in stats["dropped_keywords"] if d["stage"] == "length"),
            sum(1 for d in stats["dropped_keywords"] if d["stage"] == "llm_price"),
            sum(1 for d in stats["dropped_keywords"] if d["stage"] == "llm_quality"),
        )

        if not keyword_strings:
            logger.warning("All keywords are duplicates, stopping pipeline")
            stats["finished_at"] = datetime.utcnow().isoformat()
            # Early-return paths used to die silently — now they emit the
            # same funnel summary as a successful run so the user always
            # sees where the pipeline stopped.
            self._log_funnel_summary(stats)
            self._finalize_costs(cost_tracker, stats)
            return stats

        # Step 2: Keyword Planner Validation — batched DataForSEO call.
        # Emit 0/N → N/N bookends so the bar visibly fills when the call
        # returns instead of just flipping stage labels.
        _progress("Fetching search volume", 0, len(keyword_strings))
        logger.info("Step 2: Validating %d keywords via Keyword Planner", len(keyword_strings))
        try:
            validated = keyword_planner.validate_keywords(
                keywords=keyword_strings,
                country=country,
                language=language,
                config=self.config,
                cost_tracker=cost_tracker,
            )
            stats["keywords_validated"] = len(validated)
            _progress("Fetching search volume", len(validated), max(len(validated), len(keyword_strings)))
        except Exception as e:
            logger.error("Step 2 (Keyword Planner) failed: %s", e, exc_info=True)
            validated = []
            stats["error_step2"] = str(e)

        # Fallback: if Keyword Planner returned nothing (common when the Google
        # Ads dev token is at Explorer-access tier, which forbids the
        # generateKeywordIdeas method, or the API simply raised and was swallowed
        # by validate_keywords), pass the raw keywords through with zero volume
        # data so the downstream steps (competition, AliExpress, sheet write)
        # still run.
        if not validated and keyword_strings:
            logger.warning(
                "Keyword Planner returned no results — passing %d raw keywords "
                "through with no volume data (likely Explorer-tier API restriction).",
                len(keyword_strings),
            )
            validated = [
                {"keyword": kw, "monthly_search_volume": 0, "estimated_cpc": 0, "competition_level": "unknown"}
                for kw in keyword_strings
            ]
            stats["keywords_validated"] = len(validated)

        # Step 2a — drop keywords Google Keyword Planner has NO aggregated
        # data for. These are usually long-tail compound phrases (very common
        # in German) or brand-new terms. Without Planner data we have no
        # volume, no CPC, and no way to estimate ad economics — putting them
        # in the sourcing inbox just pollutes it with blank rows (which is
        # what the old "API-unavailable fallback" did every time the LLM
        # produced 4+ word German compounds).
        #
        # We distinguish this from "zero volume" via the `has_planner_data`
        # flag: null volume from DataForSEO → has_planner_data=False → dropped
        # here; actual 0 aggregate → has_planner_data=True → falls through to
        # the min-volume filter below.
        validated_with_data = [
            kw for kw in validated if kw.get("has_planner_data")
        ]
        stats["keywords_with_planner_data"] = len(validated_with_data)
        for kw_data in validated:
            if not kw_data.get("has_planner_data"):
                _drop(
                    kw_data["keyword"],
                    "volume_no_data",
                    "Google Keyword Planner has no aggregated data "
                    "(too long-tail or not yet indexed by Google Ads).",
                )
        if validated and not validated_with_data:
            # Genuine zero-coverage run. Tell the user loudly — every single
            # keyword was dropped before filters even ran. Usually means the
            # LLM prompt generated phrases that are too specific.
            logger.warning(
                "All %d keywords returned null volume from DataForSEO. "
                "This means Google Keyword Planner has no aggregated data "
                "for any of them — likely too long-tail. Consider shortening "
                "the LLM prompt to 2–3 word keywords.",
                len(validated),
            )
            stats["finished_at"] = datetime.utcnow().isoformat()
            self._log_funnel_summary(stats)
            self._finalize_costs(cost_tracker, stats)
            return stats

        # Step 2b — min-volume filter, applied only to keywords that HAVE
        # real Planner data. Record drops so the UI can show
        # "X keywords below the 500/mo threshold".
        min_volume = self.config.get("research.min_monthly_search_volume", 0)
        passed_volume = keyword_planner.filter_keywords(
            validated_with_data, config=self.config
        )
        stats["keywords_passed_volume"] = len(passed_volume)
        logger.info(
            "%d/%d keywords with Planner data passed volume filter (min=%d/mo)",
            len(passed_volume), len(validated_with_data), min_volume,
        )

        passed_volume_keywords = {kw["keyword"].lower() for kw in passed_volume}
        for kw_data in validated_with_data:
            if kw_data["keyword"].lower() not in passed_volume_keywords:
                vol = kw_data.get("monthly_search_volume", 0)
                _drop(
                    kw_data["keyword"],
                    "volume",
                    f"{vol}/mo < min {min_volume}/mo",
                )

        # CPC cap — typical paid-search budget gate. Applied after volume so
        # the funnel can show "X keywords were priced above the CPC cap"
        # independently of the volume filter. 0 disables the filter.
        max_cpc = float(self.config.get("research.max_cpc", 0) or 0)
        if max_cpc > 0 and passed_volume:
            kept_cpc, dropped_cpc = [], []
            for kw in passed_volume:
                cpc = float(kw.get("estimated_cpc") or 0)
                # Pass through rows with no CPC data — we only want to
                # drop keywords DataForSEO priced and they priced too high.
                if cpc > 0 and cpc > max_cpc:
                    dropped_cpc.append(kw)
                else:
                    kept_cpc.append(kw)
            for kw in dropped_cpc:
                _drop(
                    kw["keyword"],
                    "cpc",
                    f"CPC €{float(kw.get('estimated_cpc') or 0):.2f} > max €{max_cpc:.2f}",
                )
            if dropped_cpc:
                logger.info(
                    "CPC filter dropped %d/%d keywords above €%.2f cap",
                    len(dropped_cpc), len(passed_volume), max_cpc,
                )
            passed_volume = kept_cpc
            stats["keywords_passed_cpc"] = len(passed_volume)

        if not passed_volume:
            logger.warning("No keywords to process after volume filter")
            stats["finished_at"] = datetime.utcnow().isoformat()
            # Early-return paths used to die silently — now they emit the
            # same funnel summary as a successful run so the user always
            # sees where the pipeline stopped.
            self._log_funnel_summary(stats)
            self._finalize_costs(cost_tracker, stats)
            return stats

        # Layer 3 — batched Claude QA gate. We send the surviving keywords
        # (now enriched with DataForSEO volume/CPC) to Claude in a single
        # call and ask for the top N. Anything not picked gets dropped at
        # stage `llm_qa` before we spend $0.015/kw on SerpAPI.
        #
        # This is the single biggest cost-lever on Discover runs:
        # 30 → 15 via Layer 3 saves ~$0.23 per run at a $0.02 QA cost.
        # Default cap from config — 0 disables.
        pre_serpapi_cap = int(self.config.get("research.pre_serpapi_cap", 0) or 0)
        if pre_serpapi_cap > 0 and len(passed_volume) > pre_serpapi_cap:
            _progress("Claude QA pre-SerpAPI", 0, len(passed_volume))
            # Re-attach LLM metadata (category, competition_signal, etc.)
            # onto each candidate so Claude can reason about them. DataForSEO
            # rows carry only keyword/volume/cpc/competition_level by default.
            for kw_data in passed_volume:
                meta = keyword_metadata.get(kw_data["keyword"].lower(), {})
                for k in ("category", "estimated_price_range", "competition_signal",
                          "sourcing_difficulty", "product_type", "reasoning"):
                    if k not in kw_data and k in meta:
                        kw_data[k] = meta[k]

            pre_cap_count = len(passed_volume)
            ranked = llm_ideation.rank_keywords_pre_serpapi(
                keywords=passed_volume,
                country=country,
                language=language,
                top_n=pre_serpapi_cap,
                config=self.config,
                cost_tracker=cost_tracker,
            )
            kept_set = {id(kw) for kw in ranked}
            for kw in passed_volume:
                if id(kw) not in kept_set:
                    _drop(
                        kw["keyword"],
                        "llm_qa",
                        f"Layer 3 QA ranked outside top {pre_serpapi_cap} "
                        f"of {pre_cap_count} (competition/sourcing/fit).",
                    )
            stats["keywords_passed_llm_qa"] = len(ranked)
            # Close the 0/N → N/N bookend for the QA bar. Using pre_cap_count
            # as the denominator so the bar reads "X of the input went
            # forward" rather than "X of X".
            _progress("Claude QA pre-SerpAPI", len(ranked), pre_cap_count)
            logger.info(
                "Layer 3 QA: kept %d/%d before SerpAPI (saved ~$%.2f at $0.015/kw)",
                len(ranked), pre_cap_count,
                (pre_cap_count - len(ranked)) * 0.015,
            )
            passed_volume = ranked
        else:
            # Layer 3 not triggered (disabled or already under cap) — record
            # full passthrough so the funnel doesn't show a mystery drop.
            stats["keywords_passed_llm_qa"] = len(passed_volume)

        if not passed_volume:
            logger.warning("Layer 3 QA returned zero candidates — stopping")
            stats["finished_at"] = datetime.utcnow().isoformat()
            self._log_funnel_summary(stats)
            self._finalize_costs(cost_tracker, stats)
            return stats

        # Step 3: Competition Analysis
        logger.info("Step 3: Analyzing competition for %d keywords", len(passed_volume))
        total_comp = len(passed_volume)
        # Emit a 0/N beat BEFORE the first SerpAPI call so the UI flips to
        # the per-keyword view as soon as the stage starts (SerpAPI can take
        # a few seconds on the first call — don't leave the user staring at
        # the previous stage label).
        _progress("Analyzing competition", 0, total_comp)
        enriched_keywords = []
        for i, kw_data in enumerate(passed_volume):
            keyword_text = kw_data["keyword"]
            try:
                comp_data = competition.analyze_competition(
                    keyword=keyword_text,
                    country=country,
                    language=language,
                    config=self.config,
                    cost_tracker=cost_tracker,
                )
                if comp_data:
                    kw_data.update(comp_data)
                    enriched_keywords.append(kw_data)
                else:
                    # No competition data — still include with defaults
                    enriched_keywords.append(kw_data)
            except Exception as e:
                logger.error("Competition analysis failed for '%s': %s", keyword_text, e)
                enriched_keywords.append(kw_data)  # Include anyway
            _progress("Analyzing competition", i + 1, total_comp)

        # Filter by competition (only if competition data was available).
        # Record everyone cut so we can tell the user "8 keywords had too many
        # competitors" instead of a silent count.
        has_competition_data = any(kw.get("competitor_count") for kw in enriched_keywords)
        if has_competition_data:
            passed_competition = competition.filter_by_competition(
                enriched_keywords, config=self.config
            )
            passed_set = {kw["keyword"].lower() for kw in passed_competition}
            use_tiered = bool(self.config.get("research.use_tiered_filter", True))
            max_competitors = self.config.get("research.max_competitors", 0)
            for kw_data in enriched_keywords:
                if kw_data["keyword"].lower() not in passed_set:
                    n_comp = kw_data.get("competitor_count", 0)
                    diff_score = kw_data.get("differentiation_score", 0)
                    vol = kw_data.get("monthly_search_volume", 0)
                    if use_tiered:
                        reason = (
                            f"no tier fit — vol={vol:.0f}/mo, "
                            f"comp={n_comp}, diff={diff_score:.0f}"
                        )
                    else:
                        reason = (
                            f"{n_comp} competitors / diff_score={diff_score:.0f} "
                            f"(max_competitors={max_competitors})"
                        )
                    _drop(kw_data["keyword"], "competition", reason)
        else:
            logger.info("No competition data available — passing all keywords through")
            passed_competition = enriched_keywords

        stats["keywords_passed_competition"] = len(passed_competition)
        logger.info(
            "%d/%d keywords passed competition filter",
            len(passed_competition), len(enriched_keywords),
        )

        if not passed_competition:
            logger.warning("No keywords passed competition filter")
            stats["finished_at"] = datetime.utcnow().isoformat()
            # Early-return paths used to die silently — now they emit the
            # same funnel summary as a successful run so the user always
            # sees where the pipeline stopped.
            self._log_funnel_summary(stats)
            self._finalize_costs(cost_tracker, stats)
            return stats

        # Step 3b: Selling Price Filter (based on competitor median price)
        min_price = self.config.min_selling_price
        max_price = self.config.max_selling_price
        has_price_data = any(kw.get("median_competitor_price", 0) > 0 for kw in passed_competition)

        if has_price_data and (min_price > 0 or max_price > 0):
            price_filtered = []
            for kw_data in passed_competition:
                comp_price = kw_data.get("median_competitor_price", 0)
                if comp_price <= 0:
                    # No price data — pass through
                    price_filtered.append(kw_data)
                    continue
                if min_price > 0 and comp_price < min_price:
                    _drop(
                        kw_data.get("keyword", "?"),
                        "price",
                        f"competitor median €{comp_price:.2f} < min €{min_price:.2f}",
                    )
                    continue
                if max_price > 0 and comp_price > max_price:
                    _drop(
                        kw_data.get("keyword", "?"),
                        "price",
                        f"competitor median €{comp_price:.2f} > max €{max_price:.2f}",
                    )
                    continue
                price_filtered.append(kw_data)

            logger.info("Selling price filter: %d/%d passed (€%.0f – €%.0f range)",
                         len(price_filtered), len(passed_competition), min_price, max_price)
            passed_competition = price_filtered
        else:
            logger.info("No competitor price data — skipping selling price filter")

        stats["keywords_passed_price_filter"] = len(passed_competition)

        if not passed_competition:
            logger.warning("No keywords passed selling price filter")
            stats["finished_at"] = datetime.utcnow().isoformat()
            # Early-return paths used to die silently — now they emit the
            # same funnel summary as a successful run so the user always
            # sees where the pipeline stopped.
            self._log_funnel_summary(stats)
            self._finalize_costs(cost_tracker, stats)
            return stats

        # Step 4: AliExpress Product Matching (Top-3 approach)
        #
        # Policy (2026-04-23): a keyword with no AliExpress match is an
        # INBOX-ONLY lead, not a hard drop and not a separate Product row.
        # Background: the DS feed API only surfaces ~10k bestsellers, so
        # ~50-70% of otherwise viable keywords come back unmatched — but
        # the underlying products almost always exist on AliExpress, just
        # not in the feeds. Hard-dropping leaks real opportunities.
        #
        # Previously (2026-04-21 → 2026-04-23) these were written as
        # Products with test_status=PENDING_MANUAL_REVIEW and surfaced on a
        # dedicated Manual Review page. That page was retired because the
        # Research Inbox already IS the manual-review queue — every
        # candidate gets a human decision there (fill Ali € → Send to
        # Agent, or Kill). A separate Product row just duplicated the same
        # decision. Now unmatched keywords land in the Research Inbox as
        # KeywordResearch rows only (no Product, no log), status blank,
        # Ali fields empty. The user fills Ali € by hand and clicks "Send
        # to Agent" — which fires the economics gate and upserts a
        # Product with status=READY_TO_TEST (or SOURCING if still empty).
        logger.info("Step 4: Matching %d keywords to AliExpress products (top 3)", len(passed_competition))
        total_ali = len(passed_competition)
        _progress("Matching AliExpress", 0, total_ali)
        products_to_write = []
        # Unmatched keywords (no AliExpress DS-feed match) no longer create
        # Product rows — they land in the Research Inbox as keyword-only
        # rows for the user to source by hand. See the unmatched branch
        # in the loop below for the full rationale.
        unmatched_inbox_only: list[dict] = []
        try:
            import json as _json_step4
            for i, kw_data in enumerate(passed_competition):
                keyword_text = kw_data["keyword"]
                selling_price = kw_data.get("median_competitor_price", 0)

                # Pull English search terms + category from the LLM-generated
                # metadata. Note: post-2026-04-21, the primary match is a
                # direct target-language comparison against localised feed
                # titles; `english_search_terms` is only used as a rescue
                # pass in `search_products` when the direct match comes up
                # empty. `category` is a new (2026-04-21) routing hint — the
                # matcher hoists the category-relevant feed to the front of
                # its scan order for a better first-feed hit rate.
                llm_meta = keyword_metadata.get(keyword_text.lower(), {})
                english_terms = llm_meta.get("english_search_terms") or []
                if not isinstance(english_terms, list):
                    english_terms = []
                llm_category = llm_meta.get("category") or ""

                top3 = aliexpress.find_top3_matches(
                    keyword=keyword_text,
                    estimated_selling_price=selling_price,
                    country=country,
                    language=language,
                    config=self.config,
                    english_search_terms=english_terms,
                    category=llm_category,
                )

                best_seller = top3.get("best_seller")
                if not best_seller:
                    # Unmatched lane. The DS feed covers ~10k bestseller SKUs
                    # so a miss here doesn't mean "not on AliExpress" — it
                    # means "not in the feeds we can scan". Keep the keyword
                    # in the Research Inbox (status blank) with Ali fields
                    # empty; the user fills them in by hand from an
                    # AliExpress search and hits "Send to Agent" when ready.
                    #
                    # Previously these rows were written as Products with
                    # status=PENDING_MANUAL_REVIEW and surfaced on a separate
                    # Manual Review page. That page was retired 2026-04-23
                    # because the Research Inbox already IS the manual-review
                    # queue (every candidate gets a human decision now), so
                    # creating a parallel Product row just duplicated the
                    # review step. We still write the KeywordResearch row so
                    # the unmatched candidate is visible in the Inbox — just
                    # no Product is created until the user promotes it.
                    kw_data["aliexpress_match"] = {}
                    kw_data["aliexpress_top3_json"] = ""
                    kw_data["aliexpress_match_meta_json"] = ""
                    kw_data["_unmatched_inbox_only"] = True
                    unmatched_inbox_only.append(kw_data)
                    stats["keywords_unmatched_to_inbox"] = (
                        stats.get("keywords_unmatched_to_inbox", 0) + 1
                    )
                    _progress("Matching AliExpress", i + 1, total_ali)
                    continue

                kw_data["aliexpress_match"] = best_seller
                stats["products_matched"] += 1

                # Capture match diagnostics (which feed, which pass/strategy,
                # what title was used as the needle context) so we can later
                # audit weak matches without re-running the pipeline. The
                # matcher attaches these to every product it returns.
                match_meta = {
                    "match_via": best_seller.get("match_via", ""),
                    "match_feed": best_seller.get("match_feed", ""),
                    "match_pass": best_seller.get("match_pass", ""),
                    "matched_title": (best_seller.get("title") or "")[:200],
                    "keyword": keyword_text,
                    "language": language,
                    "country": country,
                }
                kw_data["aliexpress_match_meta_json"] = _json_step4.dumps(
                    match_meta, ensure_ascii=False
                )

                # Serialize top-3 JSON for storage
                top3_list = []
                for key in ("best_seller", "best_price", "best_rated"):
                    p = top3.get(key)
                    if p:
                        top3_list.append({
                            "tag": p.get("tag", key),
                            "title": (p.get("title") or "")[:120],
                            "url": p.get("url", ""),
                            "price": round(float(p.get("price", 0) or 0), 2),
                            "rating": round(float(p.get("rating", 0) or 0), 1),
                            "orders": int(p.get("orders", 0) or 0),
                            "image_url": p.get("image_url", ""),
                            "margin_pct": round(float(p.get("estimated_margin_pct", 0) or 0), 4),
                        })
                kw_data["aliexpress_top3_json"] = _json_step4.dumps(top3_list, ensure_ascii=False) if top3_list else ""

                # Step 4c — per-keyword economics gate.
                # Recompute max_allowed_cpc and net_margin_pct using the
                # SAME formulas as EconomicValidator.calculate_economics (see
                # src/economics/validator.py), so research-time filtering
                # matches what the downstream economics gate would say once
                # the product lands in sourcing. Without this, keywords
                # priced between research.max_cpc (flat threshold) and the
                # economics-derived ceiling slip into the queue only to be
                # killed later — wasting an SerpAPI call and a sourcing
                # slot. We pass `landed_cost = best_seller.price` as a
                # proxy (AliExpress DS feed prices already include shipping
                # for DE fulfilment); fail-open on zero inputs so genuine
                # data gaps don't mass-kill a run.
                selling_p = float(kw_data.get("median_competitor_price", 0) or 0)
                landed_c = float(best_seller.get("price", 0) or 0)
                est_cpc = float(kw_data.get("estimated_cpc", 0) or 0)
                econ_pass, econ_reason, econ_meta = self._economics_gate(
                    selling_price=selling_p,
                    landed_cost=landed_c,
                    estimated_cpc=est_cpc,
                )
                if not econ_pass:
                    _drop(keyword_text, "economics", econ_reason)
                    _progress("Matching AliExpress", i + 1, total_ali)
                    continue
                # Stash computed economics on the keyword so downstream
                # consumers (sourcing agent, dashboard) don't have to
                # recompute — they see the same numbers research used.
                kw_data["max_allowed_cpc_calc"] = econ_meta["max_allowed_cpc"]
                kw_data["net_margin_pct_calc"] = econ_meta["net_margin_pct"]
                kw_data["break_even_roas_calc"] = econ_meta["break_even_roas"]
                stats["keywords_passed_economics"] += 1

                products_to_write.append(kw_data)
                _progress("Matching AliExpress", i + 1, total_ali)
        except Exception as e:
            logger.error("Step 4 (AliExpress Matching) failed: %s", e, exc_info=True)
            stats["error_step4"] = str(e)
            # Transport/signing failure for the whole step: we can't tell
            # which keywords would have matched, so we hard-drop the
            # un-processed ones with an explanatory reason rather than
            # writing manual-URL placeholders. The partial progress we made
            # before the exception stays in `products_to_write`.
            processed = {id(kw) for kw in products_to_write}
            for kw_data in passed_competition:
                if id(kw_data) not in processed:
                    _drop(
                        kw_data.get("keyword", "?"),
                        "aliexpress",
                        f"step 4 aborted before matching: {type(e).__name__}",
                    )

        logger.info("%d products matched on AliExpress, %d total to write",
                     stats["products_matched"], len(products_to_write))

        # Step 4b: Global top-N cap on sourcing candidates.
        #
        # A human sourcing agent can only process a handful of products per
        # day; dumping 40 candidates at once means the oldest ones sit stale
        # and the dashboard becomes noise. We cap at `max_products_per_run`
        # (default 5). Set to 0 to disable.
        #
        # The cap is a SINGLE global top-N by `opportunity_score` — NOT a
        # per-strategy slot allocation. Strategies (premium / volume / small
        # / micro / saturated_volume / rising_niche — see defaults.yaml) act
        # as a QUALIFICATION gate upstream: to reach this point, a candidate
        # must have matched at least one strategy's thresholds. Beyond that
        # gate, strategies only contribute to ranking via their `score_weight`
        # which `opportunity_score` folds into the composite. Whichever
        # candidates score highest across all strategies win the N slots —
        # no artificial per-strategy quotas that would force us to drop an
        # excellent premium candidate just because "premium already has 1".
        #
        # Ranking uses the composite `opportunity_score()`:
        #   strategy_weight × log(volume) × diff × 1/cpc × trend × saturation
        # Higher = better. Candidates with zero volume OR zero AliExpress
        # price return -1.0 so they sink to the bottom.
        #
        # Cap applies to the agent's sourcing backlog only. Unmatched
        # keywords (no AliExpress match) are handled separately in Step 5
        # — they land in the Research Inbox as keyword-only rows and are
        # never subject to this cap.
        from src.research import opportunity
        max_per_run = int(self.config.get("research.max_products_per_run", 5))
        if max_per_run > 0 and len(products_to_write) > max_per_run:
            ranked = sorted(
                products_to_write,
                key=opportunity.opportunity_score,
                reverse=True,
            )
            kept, dropped = ranked[:max_per_run], ranked[max_per_run:]
            for kwd in dropped:
                strat = kwd.get("primary_strategy") or (
                    (kwd.get("matched_strategies") or ["?"])[0]
                )
                _drop(
                    kwd.get("keyword", "?"),
                    "cap",
                    f"ranked below top {max_per_run}/run (strategy={strat})",
                )
            picked_hist = opportunity.summarize_strategy_distribution(kept)
            logger.info(
                "Global top-N cap applied: keeping %d of %d candidates "
                "(picked=%s, capped %d out)",
                len(kept), len(products_to_write), picked_hist, len(dropped),
            )
            stats["products_capped_out"] = len(dropped)
            products_to_write = kept

        # Step 5: Write to Sheet — BULK. Each keyword previously meant 3+
        # API calls (add_keyword, add_product, add_log) so 71 products =
        # ~213 calls hitting the 60/min write quota mid-run. We now build
        # all three collections in memory and flush each in a single call.
        #
        # Two distinct write paths now exist:
        #   (a) MATCHED candidates in `products_to_write` — get a
        #       KeywordResearch + Product (status=SOURCING) + ActionLog.
        #   (b) UNMATCHED candidates in `unmatched_inbox_only` — get a
        #       KeywordResearch row only, with empty Ali fields and blank
        #       status. No Product is created until the user fills the
        #       Ali price in the Research Inbox and clicks "Send to Agent".
        #       This retired the old PENDING_MANUAL_REVIEW Product lane
        #       (2026-04-23): the Research Inbox already IS the manual
        #       review queue, so a separate Product row just duplicated
        #       the decision.
        write_total = len(products_to_write) + len(unmatched_inbox_only)
        _progress("Writing to Sheet", 0, write_total)
        logger.info(
            "Step 5: Writing %d matched + %d unmatched keywords to Sheet (bulk)",
            len(products_to_write), len(unmatched_inbox_only),
        )
        keywords_batch: list[KeywordResearch] = []
        products_batch: list[Product] = []
        logs_batch: list[ActionLog] = []

        # (a) Matched candidates — full triplet (keyword + product + log)
        for kw_data in products_to_write:
            try:
                kw, product, log = self._build_product_entry(
                    kw_data, country, language,
                    status=ProductStatus.SOURCING.value,
                )
                keywords_batch.append(kw)
                products_batch.append(product)
                logs_batch.append(log)
                stats["products_added_to_sourcing"] += 1
            except Exception as e:
                logger.error("Failed to build product entry '%s': %s", kw_data.get("keyword", "?"), e)

        # (b) Unmatched candidates — keyword-only row, no Product, no log.
        # Reuse _build_product_entry but discard the product/log it emits;
        # the KeywordResearch row already has empty ali_* fields because
        # Step 4's no-match branch stored `kw_data["aliexpress_match"] = {}`.
        inbox_only_written = 0
        for kw_data in unmatched_inbox_only:
            try:
                kw, _product_discarded, _log_discarded = self._build_product_entry(
                    kw_data, country, language,
                    status=ProductStatus.SOURCING.value,  # unused; product discarded
                )
                keywords_batch.append(kw)
                inbox_only_written += 1
            except Exception as e:
                logger.error(
                    "Failed to build keyword-only entry '%s': %s",
                    kw_data.get("keyword", "?"), e,
                )
        stats["keywords_written_to_inbox_only"] = inbox_only_written
        # Final 100% tick so the bar doesn't sit half-full after write.
        _progress(
            "Writing to Sheet",
            stats["products_added_to_sourcing"] + inbox_only_written,
            write_total,
        )

        # Flush in 3 bulk calls (was ~213 serial calls). Each call is
        # individually retried on 429 by the Sheets layer.
        try:
            if keywords_batch:
                self.store.add_keywords_bulk(keywords_batch)
            if products_batch:
                self.store.add_products_bulk(products_batch)
            if logs_batch:
                self.store.add_logs_bulk(logs_batch)
        except Exception as e:
            logger.error("Bulk write to Sheet failed: %s", e, exc_info=True)
            stats["error_step5"] = str(e)

        # Sync to Agent Tasks tab (also single bulk call internally)
        try:
            synced = self.store.sync_all_sourcing_to_agent_tasks()
            if synced:
                logger.info("Synced %d products to Agent Tasks tab", synced)
        except Exception as e:
            logger.error("Failed to sync to Agent Tasks: %s", e)

        # Send summary notification
        try:
            self._send_pipeline_notification(stats)
        except Exception as e:
            logger.error("Failed to send notification: %s", e)

        stats["finished_at"] = datetime.utcnow().isoformat()

        # --- Final funnel summary -----------------------------------------
        # Log the funnel as a block so it's easy to eyeball in the terminal.
        # This is the one log line you'd re-read after a failed run to see
        # "how many keywords died where". Kept separate from the raw stats
        # dump above because that dict is big and hard to scan.
        self._log_funnel_summary(stats)

        # Finalise cost tracking: stamp summary into stats (for the UI),
        # log it to console, and persist all records to the API Costs
        # sheet in one bulk write. Never fail the run if persistence
        # errors — cost log is observability, not pipeline-critical.
        self._finalize_costs(cost_tracker, stats)

        return stats

    def _finalize_costs(self, cost_tracker: CostTracker, stats: dict) -> None:
        """Summarise + persist the tracker's records.

        - Stamps `cost_total_usd`, `cost_breakdown`, `cost_summary` onto `stats`
          so the Streamlit UI can display the cost banner without re-querying.
        - Logs the one-line summary + indented breakdown to the terminal.
        - Writes all records to the API Costs sheet in a single bulk call.
        - Persists `stats["dropped_keywords"]` to the Research Drops tab so
          the funnel is queryable after the run (the in-memory list is
          otherwise lost).

        Safe to call on a tracker with no records — becomes a near-no-op
        (just stamps zeros onto stats).
        """
        if cost_tracker is None:
            return
        stats["cost_total_usd"] = cost_tracker.total_usd()
        stats["cost_breakdown"] = cost_tracker.breakdown()
        stats["cost_summary"] = cost_tracker.summary()

        if cost_tracker.records:
            logger.info("Cost summary:\n%s", cost_tracker.summary())
            try:
                # persist() now handles its own failures internally:
                # drains any previous-run backlog from disk, writes
                # today's batch, and spills to disk if that fails. This
                # outer try/except is defense-in-depth for unexpected
                # exceptions (e.g. a programming error in persist itself
                # after a future refactor) — the pipeline must never
                # crash for an observability side-effect.
                cost_tracker.persist(self.store)
            except Exception as e:
                logger.error(
                    "Unexpected error in cost persist (pipeline-level catch): %s",
                    e, exc_info=True,
                )

        # Persist drops from stats["dropped_keywords"] in the same end-of-run
        # hook. Kept here (not in a separate method) so every early-return
        # path that already calls _finalize_costs also flushes drops — no
        # duplication of the try/except guard and no risk of new return
        # paths forgetting to call it.
        self._persist_drops(stats)

    def _persist_drops(self, stats: dict) -> None:
        """Bulk-write `stats["dropped_keywords"]` to the Research Drops tab.

        In-memory stats carry `{keyword, stage, reason}` per drop; we stamp
        the run-level `run_id` + `country` + a timestamp on each row here
        (rather than at drop-time) so the pipeline code stays unchanged.
        Swallow all sheet errors — drop logging is observability, not
        pipeline-critical.
        """
        drops = stats.get("dropped_keywords") or []
        if not drops:
            return
        run_id = stats.get("run_id") or ""
        country = stats.get("country") or ""
        ts = datetime.utcnow().isoformat(timespec="seconds")
        rows = [
            {
                "timestamp": ts,
                "run_id": run_id,
                "country": country,
                "keyword": d.get("keyword", ""),
                "stage": d.get("stage", ""),
                "reason": d.get("reason", ""),
            }
            for d in drops
        ]
        try:
            self.store.append_drop_records(rows)
            logger.info(
                "Persisted %d drop rows to Research Drops (%s)",
                len(rows), run_id,
            )
        except Exception as e:
            # Loud on failure — this is how you diagnose "run finished but
            # the funnel is empty on the next page load". Silent swallow here
            # was the reason the 10:19 run's 50 drops vanished.
            logger.error(
                "Failed to persist %d drop records for run %s: %s",
                len(rows), run_id, e, exc_info=True,
            )

    def _economics_gate(
        self,
        selling_price: float,
        landed_cost: float,
        estimated_cpc: float,
    ) -> tuple[bool, str, dict]:
        """Research-time economics filter.

        Mirrors EconomicValidator.calculate_economics + two of its validate()
        checks (max_allowed_cpc, min_gross_margin_pct) so we can kill
        un-viable keywords in Step 4 before they pollute the sourcing queue.
        The downstream validator will re-check these once the agent records
        the real landed cost; at research time we use the AliExpress DS
        supplier price as a proxy. Using the same formulas on both sides
        keeps the two filters consistent — if research-time economics pass,
        the post-sourcing validator will too (barring a landed-cost surprise).

        Fail-open on missing inputs: if we don't have both selling + landed,
        we can't compute anything sensible, so we pass the keyword through
        and let the downstream validator make the call. Better than a mass
        false-negative kill on a transient data gap.

        Returns: (passed, reason, economics_dict). Reason is empty on pass.
        """
        # Missing inputs → skip the filter. Pass a neutral meta dict so
        # callers can stash zeros instead of crashing on missing keys.
        if selling_price <= 0 or landed_cost <= 0:
            return True, "", {
                "max_allowed_cpc": 0.0,
                "net_margin_pct": 0.0,
                "break_even_roas": 0.0,
            }

        gross_margin = selling_price - landed_cost
        transaction_fees = (
            selling_price * (self.config.transaction_fee_pct + self.config.payment_fee_pct)
            + self.config.payment_fixed_fee
        )
        net_margin = gross_margin - transaction_fees
        net_margin_pct = net_margin / selling_price if selling_price > 0 else 0.0
        break_even_roas = (1 / net_margin_pct) if net_margin_pct > 0 else 999.0
        max_allowed_cpc = net_margin * self.config.assumed_conversion_rate

        meta = {
            "max_allowed_cpc": round(max_allowed_cpc, 2),
            "net_margin_pct": round(net_margin_pct, 4),
            "break_even_roas": round(break_even_roas, 2),
        }

        # Reject when the product can't support the ad cost. We only drop on
        # CPC when we actually have a CPC from DataForSEO — a zero means
        # "no data", not "free clicks", and we'd rather let those through.
        if estimated_cpc > 0 and max_allowed_cpc > 0 and estimated_cpc > max_allowed_cpc:
            return (
                False,
                f"estimated CPC €{estimated_cpc:.2f} > max allowed €{max_allowed_cpc:.2f} "
                f"(net margin {net_margin_pct:.1%} × CVR {self.config.assumed_conversion_rate:.1%})",
                meta,
            )

        # Reject on thin margins regardless of CPC. If the product only nets
        # 15% after fees, even a break-even ROAS campaign would cannibalise
        # profit the second anything goes slightly off.
        min_margin = self.config.min_gross_margin_pct
        if min_margin > 0 and net_margin_pct < min_margin:
            return (
                False,
                f"net margin {net_margin_pct:.1%} < min {min_margin:.1%} "
                f"(selling €{selling_price:.2f}, landed €{landed_cost:.2f})",
                meta,
            )

        return True, "", meta

    def _log_funnel_summary(self, stats: dict) -> None:
        """
        Emit a multi-line funnel report after a run completes. Grouped drops
        by stage so you can tell at a glance where the pipeline leaks.

        Example output:
            ===== Research funnel (DE) =====
             LLM generated       : 150
             After dedup         : 120  (dropped 30: dedup)
             Passed volume       :  85  (dropped 35: volume)
             Passed competition  :  40  (dropped 45: competition)
             Passed price filter :  18  (dropped 22: price)
             AliExpress matched  :  12  (dropped 6: aliexpress)
             Written to sheet    :  12
            ================================
        """
        # Aggregate the audit trail into per-stage counts so the log stays
        # scannable even when hundreds of keywords get filtered.
        by_stage: dict[str, int] = {}
        for d in stats.get("dropped_keywords", []):
            by_stage[d["stage"]] = by_stage.get(d["stage"], 0) + 1

        def _drop_suffix(stage: str) -> str:
            n = by_stage.get(stage, 0)
            return f"  (dropped {n}: {stage})" if n else ""

        country    = stats.get("country", "?")
        gen        = stats.get("keywords_generated", 0)
        dedup      = gen - stats.get("duplicates_skipped", 0)
        length_p   = stats.get("keywords_passed_length", dedup)
        llm_price  = stats.get("keywords_passed_llm_price", length_p)
        llm_qual   = stats.get("keywords_passed_llm_quality", llm_price)
        planner    = stats.get("keywords_with_planner_data", llm_qual)
        vol        = stats.get("keywords_passed_volume", 0)
        cpc_p      = stats.get("keywords_passed_cpc", vol)
        llm_qa     = stats.get("keywords_passed_llm_qa", cpc_p)
        comp       = stats.get("keywords_passed_competition", 0)
        price      = stats.get("keywords_passed_price_filter", 0)
        matched    = stats.get("products_matched", 0)
        unmatched  = stats.get("keywords_unmatched_to_inbox", 0)
        econ       = stats.get("keywords_passed_economics", matched)
        written    = stats.get("products_added_to_sourcing", 0)
        inbox_only = stats.get("keywords_written_to_inbox_only", 0)

        lines = [
            f"===== Research funnel ({country}) =====",
            f" LLM generated       : {gen}",
            f" After dedup         : {dedup}{_drop_suffix('dedup')}",
            f" Passed length       : {length_p}{_drop_suffix('length')}",
            f" Passed LLM price    : {llm_price}{_drop_suffix('llm_price')}",
            f" Passed LLM quality  : {llm_qual}{_drop_suffix('llm_quality')}",
            f" Has Planner data    : {planner}{_drop_suffix('volume_no_data')}",
            f" Passed volume       : {vol}{_drop_suffix('volume')}",
            f" Passed CPC          : {cpc_p}{_drop_suffix('cpc')}",
            f" Passed Claude QA    : {llm_qa}{_drop_suffix('llm_qa')}",
            f" Passed competition  : {comp}{_drop_suffix('competition')}",
            f" Passed price filter : {price}{_drop_suffix('price')}",
            f" AliExpress matched  : {matched} (+{unmatched} unmatched → inbox-only)",
            f" Passed economics    : {econ}{_drop_suffix('economics')}",
            f" Written to sourcing : {written}",
            f" Written to inbox    : {inbox_only}",
            "=" * 34,
        ]
        # Single logger.info call so the block lands contiguous in the output
        # (individual calls can interleave with other thread's logs).
        logger.info("\n".join(lines))

        # If nothing landed in either lane, shout — this is the case the
        # user was hitting. A WARNING makes it stand out in tools like
        # Streamlit's log viewer. Inbox-only writes still count as
        # "something landed" since they produce dashboard work (a row in
        # the Research Inbox waiting for a human decision).
        if written == 0 and inbox_only == 0:
            if gen == 0:
                logger.warning(
                    "No keywords were generated at all. Check LLM config / API key."
                )
            elif dedup == 0:
                logger.warning(
                    "All %d generated keywords were duplicates of existing rows. "
                    "Clear the Keywords sheet or adjust the LLM prompt to produce "
                    "different ideas.", gen,
                )
            else:
                # The common case: keywords generated, filters killed them all.
                # Point at the biggest killer so the user knows which knob to
                # loosen first.
                biggest_stage = max(by_stage.items(), key=lambda kv: kv[1])[0] if by_stage else "?"
                logger.warning(
                    "Pipeline wrote 0 products — biggest drop at stage '%s' "
                    "(%d keywords). Inspect stats['dropped_keywords'] for details.",
                    biggest_stage, by_stage.get(biggest_stage, 0),
                )

    def run_for_all_countries(self) -> list[dict]:
        """Run the pipeline for all configured countries."""
        countries = self.config.countries
        all_stats = []
        for country_info in countries:
            if isinstance(country_info, dict):
                code = country_info.get("code", "DE")
                lang = country_info.get("language", "de")
            else:
                code = str(country_info)
                lang = "de"
            stats = self.run_full_pipeline(country=code, language=lang)
            all_stats.append(stats)
        return all_stats

    def add_manual_keyword(
        self,
        keyword: str,
        country: str = "DE",
        language: str = "de",
        monthly_search_volume: int = 0,
        estimated_cpc: float = 0.0,
        notes: str = "",
    ) -> KeywordResearch:
        """
        Add a manually researched keyword to the pipeline.
        It still goes through the same validation logic.
        """
        kw = KeywordResearch(
            keyword=keyword,
            country=country,
            language=language,
            monthly_search_volume=monthly_search_volume,
            estimated_cpc=estimated_cpc,
            research_source=ResearchSource.MANUAL.value,
            notes=notes,
        )
        self.store.add_keyword(kw)

        # Create product entry
        product = Product(
            keyword_id=kw.keyword_id,
            keyword=keyword,
            country=country,
            language=language,
            monthly_search_volume=monthly_search_volume,
            estimated_cpc=estimated_cpc,
            test_status=ProductStatus.DISCOVERED.value,
        )
        self.store.add_product(product)

        logger.info("Added manual keyword: %s (%s)", keyword, country)
        return kw

    def enrich_keywords(
        self,
        keyword_ids: list[str],
        run_aliexpress: bool = True,
    ) -> dict:
        """
        Run competition analysis (and optionally AliExpress search) on existing
        keywords. Use this for manually added keywords to fill in competitors,
        differentiation score, prices, and supplier link.

        Args:
            keyword_ids: List of keyword_id values to enrich.
            run_aliexpress: If True, also search AliExpress and fill in supplier data.

        Returns:
            dict with enriched_count, aliexpress_matched_count, errors list.
        """
        # Cost tracker for enrich runs — separate run_type so manual enrich
        # spend is distinguishable from full Discover runs in the log.
        run_id = f"enrich_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        cost_tracker = CostTracker(run_id=run_id, run_type="enrich")

        stats = {
            "enriched_count": 0,
            "aliexpress_matched_count": 0,
            "errors": [],
            "run_id": run_id,
            "cost_total_usd": 0.0,
            "cost_breakdown": [],
            "cost_summary": "",
        }

        if not keyword_ids:
            return stats

        all_keywords = self.store.get_keywords()
        keywords_to_enrich = [kw for kw in all_keywords if kw.keyword_id in keyword_ids]
        if not keywords_to_enrich:
            stats["errors"].append("No matching keywords found for the selected IDs.")
            return stats

        all_products = self.store.get_products()
        product_by_kw_id = {p.keyword_id: p for p in all_products if p.keyword_id}

        for kw in keywords_to_enrich:
            keyword_text = kw.keyword
            country = kw.country or "DE"
            language = kw.language or "de"

            try:
                comp_data = competition.analyze_competition(
                    keyword=keyword_text,
                    country=country,
                    language=language,
                    config=self.config,
                    cost_tracker=cost_tracker,
                )
            except Exception as e:
                logger.exception("Competition analysis failed for '%s': %s", keyword_text, e)
                stats["errors"].append(f"{keyword_text}: {e}")
                continue

            if not comp_data:
                stats["errors"].append(f"{keyword_text}: No competition data returned.")
                continue

            try:
                def _num(v, default=0):
                    if v is None:
                        return default
                    try:
                        return float(v) if v != "" else default
                    except (TypeError, ValueError):
                        return default

                def _str(v, default=""):
                    return str(v).strip() if v is not None and str(v).strip() else default

                median_price = _num(comp_data.get("median_competitor_price"), 0)
                kw_updates = {
                    "competitor_count": int(_num(comp_data.get("competitor_count"), 0)),
                    "unique_product_count": int(_num(comp_data.get("unique_product_count"), 0)),
                    "competition_type": _str(comp_data.get("competition_type"), ""),
                    "differentiation_score": _num(comp_data.get("differentiation_score"), 0),
                    "avg_competitor_price": _num(comp_data.get("avg_competitor_price"), 0),
                    "median_competitor_price": median_price,
                    "estimated_selling_price": median_price,
                    "google_shopping_url": _str(comp_data.get("google_shopping_url"), ""),
                    "competitor_pdp_url": _str(comp_data.get("competitor_pdp_url"), ""),
                    "competitor_thumbnail_url": _str(comp_data.get("competitor_thumbnail_url"), ""),
                }

                # Run AliExpress — fetch Top-3 listings (relaxed filters)
                ali_product = None       # best seller (used for main aliexpress_* fields)
                ali_top3_json = ""       # JSON with all 3 picks
                if run_aliexpress:
                    try:
                        import json as _json
                        selling_price_for_ali = median_price
                        product = product_by_kw_id.get(kw.keyword_id)
                        if product and product.selling_price:
                            selling_price_for_ali = product.selling_price

                        top3 = aliexpress.find_top3_matches(
                            keyword=keyword_text,
                            estimated_selling_price=selling_price_for_ali,
                            country=country,
                            language=language,
                            config=self.config,
                        )

                        # Serialize the top-3 for storage (compact, only the fields we need)
                        top3_list = []
                        for key in ("best_seller", "best_price", "best_rated"):
                            p = top3.get(key)
                            if p:
                                top3_list.append({
                                    "tag": p.get("tag", key),
                                    "title": (p.get("title") or "")[:120],
                                    "url": p.get("url", ""),
                                    "price": round(_num(p.get("price"), 0), 2),
                                    "rating": round(_num(p.get("rating"), 0), 1),
                                    "orders": int(_num(p.get("orders"), 0)),
                                    "image_url": p.get("image_url", ""),
                                    "margin_pct": round(_num(p.get("estimated_margin_pct"), 0), 4),
                                })

                        if top3_list:
                            ali_top3_json = _json.dumps(top3_list, ensure_ascii=False)

                        # Use the best seller as the primary AliExpress match
                        # (keeps backward compat with the single-product fields)
                        ali_product = top3.get("best_seller")
                        if ali_product:
                            kw_updates["aliexpress_url"] = _str(ali_product.get("url"), "")
                            kw_updates["aliexpress_price"] = _num(ali_product.get("price"), 0)
                            kw_updates["aliexpress_rating"] = _num(ali_product.get("rating"), 0)
                            kw_updates["aliexpress_orders"] = int(_num(ali_product.get("orders"), 0))
                            image_urls = ali_product.get("image_urls") or []
                            kw_updates["aliexpress_image_urls"] = ",".join(str(u) for u in image_urls if u) if image_urls else ""
                            stats["aliexpress_matched_count"] += 1

                        if ali_top3_json:
                            kw_updates["aliexpress_top3_json"] = ali_top3_json

                    except Exception as e:
                        logger.warning("AliExpress match failed for '%s': %s", keyword_text, e)
                        stats["errors"].append(f"{keyword_text} (AliExpress): {e}")

                self.store.update_keyword(kw.keyword_id, kw_updates)
                stats["enriched_count"] += 1

                product = product_by_kw_id.get(kw.keyword_id)
                selling_price = median_price or (product.selling_price if product else 0)
                product_updates = {
                    "competitor_count": kw_updates["competitor_count"],
                    "differentiation_score": kw_updates["differentiation_score"],
                    "competition_type": kw_updates["competition_type"],
                    "google_shopping_url": kw_updates["google_shopping_url"],
                    "competitor_pdp_url": kw_updates["competitor_pdp_url"],
                    "selling_price": selling_price,
                    "test_status": ProductStatus.SOURCING.value,
                }
                if ali_product:
                    product_updates["aliexpress_url"] = kw_updates["aliexpress_url"]
                    product_updates["aliexpress_price"] = kw_updates["aliexpress_price"]
                    product_updates["aliexpress_rating"] = kw_updates["aliexpress_rating"]
                    product_updates["aliexpress_orders"] = kw_updates["aliexpress_orders"]
                    product_updates["aliexpress_image_urls"] = kw_updates.get("aliexpress_image_urls", "")
                if ali_top3_json:
                    product_updates["aliexpress_top3_json"] = ali_top3_json

                if product:
                    self.store.update_product(product.product_id, product_updates)
                    synced_product = self.store.get_product(product.product_id)
                    if synced_product:
                        self.store.sync_product_to_agent_tasks(synced_product)
                else:
                    # Create product so manual flow = AI flow (same as after discovery)
                    new_product = Product(
                        keyword_id=kw.keyword_id,
                        keyword=keyword_text,
                        country=country,
                        language=language,
                        monthly_search_volume=kw.monthly_search_volume,
                        estimated_cpc=kw.estimated_cpc,
                        competition_level=kw.competition_level or "",
                        competitor_count=product_updates["competitor_count"],
                        differentiation_score=product_updates["differentiation_score"],
                        competition_type=product_updates["competition_type"],
                        google_shopping_url=product_updates["google_shopping_url"],
                        competitor_pdp_url=product_updates["competitor_pdp_url"],
                        aliexpress_url=product_updates.get("aliexpress_url", ""),
                        aliexpress_price=product_updates.get("aliexpress_price", 0),
                        aliexpress_rating=product_updates.get("aliexpress_rating", 0),
                        aliexpress_orders=product_updates.get("aliexpress_orders", 0),
                        aliexpress_image_urls=product_updates.get("aliexpress_image_urls", ""),
                        aliexpress_top3_json=product_updates.get("aliexpress_top3_json", ""),
                        selling_price=selling_price,
                        test_status=ProductStatus.SOURCING.value,
                    )
                    self.store.add_product(new_product)
                    log = ActionLog(
                        product_id=new_product.product_id,
                        action_type=ActionType.SOURCING_STARTED.value,
                        old_status=ProductStatus.DISCOVERED.value,
                        new_status=ProductStatus.SOURCING.value,
                        reason="Enriched from manual/AI keyword — same process as discovery",
                        details=f"Competitors: {product_updates['competitor_count']}, "
                                f"Diff: {product_updates['differentiation_score']:.0f}, "
                                f"Price: €{selling_price:.2f}",
                        country=country,
                    )
                    self.store.add_log(log)
                    self.store.sync_product_to_agent_tasks(new_product)
            except Exception as e:
                logger.exception("Enrich failed for '%s': %s", keyword_text, e)
                stats["errors"].append(f"{keyword_text}: {e}")

        logger.info(
            "Enrich complete: %d enriched, %d AliExpress matches, %d errors",
            stats["enriched_count"], stats["aliexpress_matched_count"], len(stats["errors"]),
        )

        # Persist enrich-run costs (SerpAPI calls per keyword analysed).
        self._finalize_costs(cost_tracker, stats)

        return stats

    def _build_product_entry(
        self,
        kw_data: dict,
        country: str,
        language: str,
        status: str = ProductStatus.SOURCING.value,
    ) -> tuple[KeywordResearch, Product, ActionLog]:
        """Build (keyword, product, log) objects from enriched keyword data.

        Pure in-memory construction — does NOT write to the store. The caller
        is responsible for batching these into bulk writes (see Step 5 in
        `run_full_pipeline`).

        For MATCHED keywords the caller keeps all three outputs and writes
        them as a triplet (status=SOURCING).

        For UNMATCHED keywords (no AliExpress DS-feed hit, Step 4's
        `unmatched_inbox_only` list) the caller keeps only the
        KeywordResearch and discards the Product + ActionLog — those
        keywords land as keyword-only rows in the Research Inbox and get
        promoted to a Product only when the user fills the Ali price and
        clicks "Send to Agent". The Product/log built here are still
        valid; they're just unused for that branch.
        """
        ali_match = kw_data.get("aliexpress_match", {})
        keyword_text = kw_data.get("keyword", "")

        google_shopping_url = kw_data.get("google_shopping_url", "")
        competitor_pdp_url = kw_data.get("competitor_pdp_url", "")
        competitor_thumbnail_url = kw_data.get("competitor_thumbnail_url", "")
        top3_json = kw_data.get("aliexpress_top3_json", "")
        match_meta_json = kw_data.get("aliexpress_match_meta_json", "")

        kw = KeywordResearch(
            keyword=keyword_text,
            country=country,
            language=language,
            monthly_search_volume=kw_data.get("monthly_search_volume", 0),
            estimated_cpc=kw_data.get("estimated_cpc", 0),
            competition_level=kw_data.get("competition_level", ""),
            research_source=ResearchSource.AI.value,
            competitor_count=kw_data.get("competitor_count", 0),
            unique_product_count=kw_data.get("unique_product_count", 0),
            competition_type=kw_data.get("competition_type", "unknown"),
            differentiation_score=kw_data.get("differentiation_score", 0),
            avg_competitor_price=kw_data.get("avg_competitor_price", 0),
            median_competitor_price=kw_data.get("median_competitor_price", 0),
            estimated_selling_price=kw_data.get("median_competitor_price", 0),
            google_shopping_url=google_shopping_url,
            competitor_pdp_url=competitor_pdp_url,
            competitor_thumbnail_url=competitor_thumbnail_url,
            aliexpress_url=ali_match.get("url", ""),
            aliexpress_price=ali_match.get("price", 0),
            aliexpress_rating=ali_match.get("rating", 0),
            aliexpress_orders=ali_match.get("orders", 0),
            aliexpress_image_urls=",".join(ali_match.get("image_urls", [])),
            aliexpress_top3_json=top3_json,
        )

        product = Product(
            keyword_id=kw.keyword_id,
            keyword=keyword_text,
            country=country,
            language=language,
            monthly_search_volume=kw_data.get("monthly_search_volume", 0),
            estimated_cpc=kw_data.get("estimated_cpc", 0),
            competition_level=kw_data.get("competition_level", ""),
            competitor_count=kw_data.get("competitor_count", 0),
            differentiation_score=kw_data.get("differentiation_score", 0),
            competition_type=kw_data.get("competition_type", "unknown"),
            google_shopping_url=google_shopping_url,
            competitor_pdp_url=competitor_pdp_url,
            aliexpress_url=ali_match.get("url", ""),
            aliexpress_price=ali_match.get("price", 0),
            aliexpress_rating=ali_match.get("rating", 0),
            aliexpress_orders=ali_match.get("orders", 0),
            aliexpress_image_urls=",".join(ali_match.get("image_urls", [])),
            aliexpress_top3_json=top3_json,
            aliexpress_match_meta_json=match_meta_json,
            selling_price=kw_data.get("median_competitor_price", 0),
            test_status=status,
        )

        reason = "Auto-discovered via AI research pipeline"
        details = (
            f"AliExpress price: EUR {ali_match.get('price', 0):.2f}, "
            f"Est. selling price: EUR {kw_data.get('median_competitor_price', 0):.2f}, "
            f"Competitors: {kw_data.get('competitor_count', 0)}, "
            f"Differentiation: {kw_data.get('differentiation_score', 0):.0f}"
        )

        log = ActionLog(
            product_id=product.product_id,
            action_type=ActionType.SOURCING_STARTED.value,
            old_status="",
            new_status=status,
            reason=reason,
            details=details,
            country=country,
        )
        return kw, product, log

    def _ideate_from_best_sellers(
        self,
        *,
        country: str,
        language: str,
        target_kw: int,
        existing_set: set,
        cost_tracker: CostTracker,
        stats: dict,
    ) -> list[dict]:
        """Fetch Best Sellers from Google Merchant Center, optionally
        validate with Google Trends, then translate into local-language
        keywords via Claude.

        Returns a list of keyword dicts in the same shape as
        `llm_ideation.generate_keywords()` so the existing downstream
        filter flow can process them unchanged.

        Any failure in this path is non-fatal — we log, record a stats
        key, and return whatever we have (possibly []). The caller
        decides whether to fall back to legacy LLM ideation.
        """
        # Config
        override_cats = self.config.get("research.best_sellers_categories", [])
        per_category_limit = int(
            self.config.get("research.best_sellers_per_category", 20)
        )
        use_trends = bool(
            self.config.get("research.use_google_trends_validation", True)
        )
        max_kw_per_product = int(
            self.config.get("research.best_sellers_kw_per_product", 3)
        )
        drop_declining = bool(
            self.config.get("research.trends_drop_declining", True)
        )
        # When true, drop FLAT and SINKER products — keep ONLY those
        # Google flagged as RISER (demand growing this week vs last).
        # The thesis: RISERs are niches where demand is climbing but
        # advertisers may not yet have caught up, so SERP competition
        # is thinner. Empirically validated by comparing 2026-04-22 runs
        # (mixed pool) against the first RISERs-only run to see if
        # competitor counts drop below the structural ~20 floor.
        risers_only = bool(
            self.config.get("research.best_sellers_risers_only", False)
        )

        # Normalise the category override — accept either a flat list of
        # IDs [536, 696, ...] or the richer [{id,name}, ...] shape.
        categories = None
        if override_cats:
            categories = []
            for entry in override_cats:
                if isinstance(entry, dict) and entry.get("id"):
                    categories.append(entry)
                elif isinstance(entry, (int, str)):
                    try:
                        categories.append(
                            {"id": int(entry), "name": f"cat_{entry}"}
                        )
                    except (TypeError, ValueError):
                        continue

        # ─── Stage 1: Fetch Best Sellers ───────────────────────────────
        logger.info(
            "Best Sellers: fetching for %s (%d categories, limit=%d each)",
            country,
            len(categories or google_best_sellers.DEFAULT_DROPSHIP_CATEGORIES),
            per_category_limit,
        )
        try:
            best_sellers = google_best_sellers.fetch_best_sellers(
                country=country,
                categories=categories,
                per_category_limit=per_category_limit,
            )
        except Exception as e:
            logger.error("Best Sellers fetch failed: %s", e, exc_info=True)
            stats["best_sellers_error"] = str(e)
            return []

        stats["best_sellers_fetched"] = len(best_sellers)
        if not best_sellers:
            logger.warning(
                "Best Sellers returned 0 rows — likely unaccepted ToS, "
                "disabled API, or new MC account with no approved products"
            )
            return []
        logger.info(
            "Best Sellers: got %d unique products after category dedup",
            len(best_sellers),
        )

        # Pre-filter against economics window — the BestSellers API
        # already returns price_range. If Google says every variant is
        # over €200 or under €25, there's no point paying Claude to
        # translate a title we'll kill at the price gate anyway.
        econ_min = float(self.config.min_selling_price)
        econ_max = float(self.config.max_selling_price)
        if econ_min > 0 or econ_max > 0:
            kept = []
            for p in best_sellers:
                lo, hi = p.price_min_eur, p.price_max_eur
                if not lo and not hi:
                    kept.append(p)  # no price data → pass through (fail open)
                    continue
                # No overlap with economics window → drop
                if (econ_max > 0 and lo and lo > econ_max) or (
                    econ_min > 0 and hi and hi < econ_min
                ):
                    continue
                kept.append(p)
            if len(kept) < len(best_sellers):
                logger.info(
                    "Best Sellers: %d → %d after econ price pre-filter "
                    "(€%.0f–%.0f window)",
                    len(best_sellers), len(kept), econ_min, econ_max,
                )
            best_sellers = kept

        # Pre-dedup against existing keywords — if the BS title in
        # lowercase already matches an active/historical keyword,
        # there's nothing new to translate. This is a weak filter
        # (titles are more verbose than keywords) but catches obvious
        # repeats cheaply.
        if existing_set:
            before = len(best_sellers)
            best_sellers = [
                p for p in best_sellers
                if (p.title or "").strip().lower() not in existing_set
            ]
            if len(best_sellers) < before:
                logger.info(
                    "Best Sellers: %d → %d after dedup against existing keywords",
                    before, len(best_sellers),
                )

        if not best_sellers:
            return []

        # ─── Stage 1.5: RISERs-only filter (optional) ─────────────────
        # Drop products Google flagged as FLAT or SINKER — keep only the
        # ones with week-over-week rising demand. This is the "pre-
        # saturation" signal: advertisers typically lag Google's
        # Best-Sellers data by weeks to months, so RISERs are where we
        # most often find SERPs with thinner competition (<15 advertisers
        # instead of the 20+ structural floor on established niches).
        if risers_only:
            before = len(best_sellers)
            best_sellers = [
                p for p in best_sellers if p.relative_demand_change == "RISER"
            ]
            logger.info(
                "Best Sellers: %d → %d after RISERs-only filter (dropped %d FLAT/SINKER)",
                before, len(best_sellers), before - len(best_sellers),
            )
            stats["best_sellers_risers_only_dropped"] = before - len(best_sellers)
            if not best_sellers:
                logger.warning(
                    "Best Sellers: RISERs-only filter eliminated all products "
                    "— disable `research.best_sellers_risers_only` or widen "
                    "`best_sellers_per_category` to pull a bigger candidate pool"
                )
                return []

        # ─── Stage 2: Google Trends validation (optional, fail-open) ──
        if use_trends:
            logger.info(
                "Trends: validating %d Best Seller titles (fail-open)",
                len(best_sellers),
            )
            # Use the short English product-type words, not the full
            # branded title — full titles rarely have Trends data.
            # Fall back to the raw title if we can't extract a shorter
            # form. The category path is a good proxy.
            def _trend_term(p) -> str:
                # Prefer a concise "[l3 category]" term if we have it;
                # that's roughly "what the product cluster is".
                for fld in (p.category_l3, p.category_l2, p.category_l1, p.title):
                    if fld and len(fld.split()) <= 4:
                        return fld.strip()
                return (p.title or "").strip()

            terms = list({_trend_term(p) for p in best_sellers if _trend_term(p)})
            try:
                trend_results = google_trends.validate_terms(
                    terms=terms, geo=country,
                )
            except Exception as e:
                logger.warning("Trends validation errored (%s) — skipping", e)
                trend_results = {}

            if trend_results and drop_declining:
                lookup = {k.lower(): v for k, v in trend_results.items()}
                before = len(best_sellers)
                survivors = []
                dropped_trend = 0
                for p in best_sellers:
                    term = _trend_term(p).lower()
                    tr = lookup.get(term)
                    if tr and tr.direction == "declining":
                        dropped_trend += 1
                        continue
                    survivors.append(p)
                best_sellers = survivors
                stats["best_sellers_trends_dropped"] = dropped_trend
                logger.info(
                    "Trends: %d → %d (dropped %d declining terms)",
                    before, len(best_sellers), dropped_trend,
                )

        if not best_sellers:
            return []

        # ─── Stage 3: Translate to local keywords via Claude ──────────
        # Cap the input batch — more than ~100 products per translate
        # call bloats the prompt for no gain (we already have more
        # candidates than `target_kw` can accept downstream).
        max_products = max(int(target_kw), 50)
        if len(best_sellers) > max_products:
            # Prefer RISERs over FLAT, and better-ranked over worse-ranked.
            best_sellers.sort(
                key=lambda p: (
                    0 if p.relative_demand_change == "RISER" else 1,
                    p.rank or 9999,
                ),
            )
            best_sellers = best_sellers[:max_products]
            logger.info(
                "Best Sellers: capped to top %d for translation (prefer RISERs)",
                max_products,
            )

        stats["best_sellers_to_translate"] = len(best_sellers)

        # Convert to plain dicts for the translator (it lives in
        # llm_ideation and shouldn't import the dataclass).
        product_payload = [
            {
                "title": p.title,
                "brand": p.brand,
                "category_l1": p.category_l1,
                "category_l2": p.category_l2,
                "category_l3": p.category_l3,
                "price_min_eur": p.price_min_eur,
                "price_max_eur": p.price_max_eur,
                "relative_demand_change": p.relative_demand_change,
            }
            for p in best_sellers
        ]

        keywords = llm_ideation.translate_products_to_keywords(
            products=product_payload,
            country=country,
            language=language,
            config=self.config,
            cost_tracker=cost_tracker,
            max_kw_per_product=max_kw_per_product,
        )
        stats["best_sellers_keywords_translated"] = len(keywords)
        return keywords

    def _get_feedback(self) -> ResearchFeedback:
        """Load research feedback for LLM prompt improvement."""
        try:
            feedback_data = self.store.get_research_feedback()
            return ResearchFeedback(
                winning_categories=feedback_data.get("winning_categories", []),
                losing_categories=feedback_data.get("losing_categories", []),
                avg_winning_margin_pct=feedback_data.get("avg_winning_margin_pct", 0),
                avg_winning_price_range=feedback_data.get("avg_winning_price_range", ""),
                avg_winning_competition=feedback_data.get("avg_winning_competition", 0),
            )
        except Exception:
            return ResearchFeedback()

    def _send_pipeline_notification(self, stats: dict):
        """Send a summary notification after pipeline run."""
        added = stats.get("products_added_to_sourcing", 0)
        inbox_only = stats.get("keywords_written_to_inbox_only", 0)
        country = stats.get("country", "?")

        if added > 0 or inbox_only > 0:
            parts = []
            if added > 0:
                parts.append(f"{added} ready for sourcing")
            if inbox_only > 0:
                parts.append(f"{inbox_only} unmatched in Research Inbox")
            notification = Notification(
                title=f"Research complete: {country}",
                message=f"{' + '.join(parts)} for {country}. "
                        f"Generated: {stats.get('keywords_generated', 0)}, "
                        f"Passed volume: {stats.get('keywords_passed_volume', 0)}, "
                        f"Passed competition: {stats.get('keywords_passed_competition', 0)}, "
                        f"AliExpress matched: {stats.get('products_matched', 0)}.",
                level="success",
            )
        else:
            notification = Notification(
                title=f"Research complete: {country}",
                message=f"No new product candidates found for {country}. "
                        f"Generated: {stats.get('keywords_generated', 0)} keywords, "
                        f"but none passed all filters.",
                level="info",
            )

        self.store.add_notification(notification)
