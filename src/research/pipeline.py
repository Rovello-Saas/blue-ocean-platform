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

from src.core.config import AppConfig
from src.core.interfaces import DataStore
from src.core.models import (
    KeywordResearch, Product, ProductStatus, ActionLog, ActionType,
    Notification, ResearchFeedback, ResearchSource
)
from src.research import llm_ideation, keyword_planner, competition, aliexpress

logger = logging.getLogger(__name__)


class ResearchPipeline:
    """
    Orchestrates the full research pipeline from keyword generation to product candidate.
    """

    def __init__(self, data_store: DataStore, config: AppConfig = None):
        self.store = data_store
        self.config = config or AppConfig()

    def run_full_pipeline(self, country: str = "DE", language: str = "de") -> dict:
        """
        Run the full research pipeline for a single country.

        Returns:
            dict with pipeline statistics
        """
        stats = {
            "country": country,
            "keywords_generated": 0,
            "keywords_validated": 0,
            "keywords_passed_volume": 0,
            "keywords_passed_competition": 0,
            "keywords_passed_price_filter": 0,
            "products_matched": 0,
            "products_added_to_sourcing": 0,
            "duplicates_skipped": 0,
            "started_at": datetime.utcnow().isoformat(),
            "finished_at": None,
        }

        # Step 1: LLM Keyword Ideation
        logger.info("Step 1: Generating keywords for %s (%s)", country, language)
        try:
            feedback = self._get_feedback()
            category_focus = self.config.get("research.category_focus", [])

            raw_keywords = llm_ideation.generate_keywords(
                country=country,
                language=language,
                num_keywords=self.config.get("research.keywords_per_run", 150),
                category_focus=category_focus if category_focus else None,
                feedback=feedback,
                config=self.config,
            )
            stats["keywords_generated"] = len(raw_keywords)
            logger.info("Generated %d keyword ideas", len(raw_keywords))
        except Exception as e:
            logger.error("Step 1 (LLM Ideation) failed: %s", e, exc_info=True)
            raw_keywords = []
            stats["error_step1"] = str(e)

        if not raw_keywords:
            logger.warning("No keywords generated, stopping pipeline")
            stats["finished_at"] = datetime.utcnow().isoformat()
            return stats

        # Deduplicate against existing keywords (fetch once, check in memory)
        keyword_strings = []
        keyword_metadata = {}
        try:
            existing_keywords = self.store.get_keywords(country=country)
            existing_set = {kw.keyword.lower() for kw in existing_keywords}
        except Exception as e:
            logger.warning("Could not fetch existing keywords for dedup: %s — skipping dedup", e)
            existing_set = set()

        for kw in raw_keywords:
            keyword_text = kw.get("keyword", "").strip()
            if not keyword_text:
                continue
            if keyword_text.lower() in existing_set:
                stats["duplicates_skipped"] += 1
                continue
            keyword_strings.append(keyword_text)
            keyword_metadata[keyword_text.lower()] = kw

        logger.info(
            "%d unique new keywords (skipped %d duplicates)",
            len(keyword_strings), stats["duplicates_skipped"]
        )

        if not keyword_strings:
            logger.warning("All keywords are duplicates, stopping pipeline")
            stats["finished_at"] = datetime.utcnow().isoformat()
            return stats

        # Step 2: Keyword Planner Validation
        logger.info("Step 2: Validating %d keywords via Keyword Planner", len(keyword_strings))
        try:
            validated = keyword_planner.validate_keywords(
                keywords=keyword_strings,
                country=country,
                language=language,
                config=self.config,
            )
            stats["keywords_validated"] = len(validated)
        except Exception as e:
            logger.error("Step 2 (Keyword Planner) failed: %s", e, exc_info=True)
            # Fall back to passing all keywords without volume data
            validated = [{"keyword": kw, "monthly_search_volume": 0, "estimated_cpc": 0, "competition_level": "unknown"} for kw in keyword_strings]
            stats["keywords_validated"] = len(validated)
            stats["error_step2"] = str(e)

        # Filter by volume (if volume data is available)
        passed_volume = keyword_planner.filter_keywords(validated, config=self.config)
        stats["keywords_passed_volume"] = len(passed_volume)
        logger.info("%d keywords passed volume filter", len(passed_volume))

        # If no keywords have volume data (Keyword Planner unavailable), pass all through
        if not passed_volume and validated:
            logger.info("No volume data available — passing all %d keywords through", len(validated))
            passed_volume = validated
            stats["keywords_passed_volume"] = len(passed_volume)

        if not passed_volume:
            logger.warning("No keywords to process after volume filter")
            stats["finished_at"] = datetime.utcnow().isoformat()
            return stats

        # Step 3: Competition Analysis
        logger.info("Step 3: Analyzing competition for %d keywords", len(passed_volume))
        enriched_keywords = []
        for kw_data in passed_volume:
            keyword_text = kw_data["keyword"]
            try:
                comp_data = competition.analyze_competition(
                    keyword=keyword_text,
                    country=country,
                    language=language,
                    config=self.config,
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

        # Filter by competition (only if competition data was available)
        has_competition_data = any(kw.get("competitor_count") for kw in enriched_keywords)
        if has_competition_data:
            passed_competition = competition.filter_by_competition(
                enriched_keywords, config=self.config
            )
        else:
            logger.info("No competition data available — passing all keywords through")
            passed_competition = enriched_keywords

        stats["keywords_passed_competition"] = len(passed_competition)
        logger.info("%d keywords passed competition filter", len(passed_competition))

        if not passed_competition:
            logger.warning("No keywords passed competition filter")
            stats["finished_at"] = datetime.utcnow().isoformat()
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
                    logger.debug("Filtered '%s': competitor price €%.2f < min €%.2f",
                                 kw_data.get("keyword", "?"), comp_price, min_price)
                    continue
                if max_price > 0 and comp_price > max_price:
                    logger.debug("Filtered '%s': competitor price €%.2f > max €%.2f",
                                 kw_data.get("keyword", "?"), comp_price, max_price)
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
            return stats

        # Step 4: AliExpress Product Matching (Top-3 approach)
        logger.info("Step 4: Matching %d keywords to AliExpress products (top 3)", len(passed_competition))
        products_to_write = []
        try:
            import json as _json_step4
            for kw_data in passed_competition:
                keyword_text = kw_data["keyword"]
                selling_price = kw_data.get("median_competitor_price", 0)

                top3 = aliexpress.find_top3_matches(
                    keyword=keyword_text,
                    estimated_selling_price=selling_price,
                    country=country,
                    language=language,
                    config=self.config,
                )

                best_seller = top3.get("best_seller")
                if best_seller:
                    kw_data["aliexpress_match"] = best_seller
                    stats["products_matched"] += 1
                else:
                    # No AliExpress results — build a manual search URL
                    search_urls = aliexpress.build_search_url(keyword_text)
                    kw_data["aliexpress_match"] = {
                        "url": search_urls.get("aliexpress_search_url", ""),
                        "price": 0, "rating": 0, "orders": 0, "image_urls": [],
                    }

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

                products_to_write.append(kw_data)
        except Exception as e:
            logger.error("Step 4 (AliExpress Matching) failed: %s", e, exc_info=True)
            stats["error_step4"] = str(e)
            # If AliExpress fails entirely, still write products without matches
            for kw_data in passed_competition:
                if kw_data not in products_to_write:
                    search_urls = aliexpress.build_search_url(kw_data["keyword"])
                    kw_data["aliexpress_match"] = {
                        "url": search_urls.get("aliexpress_search_url", ""),
                        "price": 0, "rating": 0, "orders": 0, "image_urls": [],
                    }
                    kw_data["aliexpress_top3_json"] = ""
                    products_to_write.append(kw_data)

        logger.info("%d products matched on AliExpress, %d total to write",
                     stats["products_matched"], len(products_to_write))

        # Step 5: Write to Sheet
        logger.info("Step 5: Writing %d products to Sheet", len(products_to_write))
        for kw_data in products_to_write:
            try:
                self._create_product_entry(kw_data, country, language)
                stats["products_added_to_sourcing"] += 1
            except Exception as e:
                logger.error("Failed to write product '%s': %s", kw_data.get("keyword", "?"), e)

        # Sync to Agent Tasks tab
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
        logger.info("Pipeline complete for %s: %s", country, stats)
        return stats

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
        stats = {"enriched_count": 0, "aliexpress_matched_count": 0, "errors": []}

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
        return stats

    def _create_product_entry(self, kw_data: dict, country: str, language: str):
        """Create a keyword and product entry from enriched keyword data."""
        ali_match = kw_data.get("aliexpress_match", {})
        meta = {}  # LLM metadata if available
        keyword_text = kw_data.get("keyword", "")

        # Merge LLM metadata
        keyword_lower = keyword_text.lower()

        # Build search URLs
        search_urls = aliexpress.build_search_url(keyword_text)

        # Google Shopping URL (from competition analysis or generated)
        google_shopping_url = kw_data.get("google_shopping_url", "")
        competitor_pdp_url = kw_data.get("competitor_pdp_url", "")

        # Create keyword record
        top3_json = kw_data.get("aliexpress_top3_json", "")
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
            aliexpress_url=ali_match.get("url", ""),
            aliexpress_price=ali_match.get("price", 0),
            aliexpress_rating=ali_match.get("rating", 0),
            aliexpress_orders=ali_match.get("orders", 0),
            aliexpress_image_urls=",".join(ali_match.get("image_urls", [])),
            aliexpress_top3_json=top3_json,
        )
        self.store.add_keyword(kw)

        # Create product record
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
            selling_price=kw_data.get("median_competitor_price", 0),
            test_status=ProductStatus.SOURCING.value,
        )
        self.store.add_product(product)

        # Log the action
        log = ActionLog(
            product_id=product.product_id,
            action_type=ActionType.SOURCING_STARTED.value,
            old_status="",
            new_status=ProductStatus.SOURCING.value,
            reason="Auto-discovered via AI research pipeline",
            details=f"AliExpress price: EUR {ali_match.get('price', 0):.2f}, "
                    f"Est. selling price: EUR {kw_data.get('median_competitor_price', 0):.2f}, "
                    f"Competitors: {kw_data.get('competitor_count', 0)}, "
                    f"Differentiation: {kw_data.get('differentiation_score', 0):.0f}",
            country=country,
        )
        self.store.add_log(log)

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
        country = stats.get("country", "?")

        if added > 0:
            notification = Notification(
                title=f"Research complete: {country}",
                message=f"Found {added} new product candidates for {country}. "
                        f"Generated: {stats.get('keywords_generated', 0)}, "
                        f"Passed volume: {stats.get('keywords_passed_volume', 0)}, "
                        f"Passed competition: {stats.get('keywords_passed_competition', 0)}, "
                        f"AliExpress matched: {stats.get('products_matched', 0)}. "
                        f"Agent action needed for {added} products.",
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
