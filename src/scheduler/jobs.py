"""
Job scheduler.
Orchestrates all recurring automated tasks using APScheduler.

Job schedule:
- Research pipeline: Daily (configurable)
- Agent cost polling: Every 30-60 min (configurable)
- Content generation: On-demand (triggered by status change)
- Performance data pull: Every 1-2 hours
- Decision engine: Every 2 hours (after performance pull)
- Daily counter update: Once per day at midnight
- Competitor price monitoring: Weekly (configurable)
- Stock monitoring: Daily (if enabled)
- Research feedback update: Weekly
"""

from __future__ import annotations

import logging
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.core.config import AppConfig
from src.core.interfaces import DataStore
from src.core.models import ProductStatus, ActionLog, ActionType, Notification
from src.economics.validator import EconomicValidator
from src.research.pipeline import ResearchPipeline
from src.ads.performance import get_product_performance
from src.ads.labels import MerchantCenterLabels
from src.decisions.engine import DecisionEngine
from src.monitoring.competitor_prices import CompetitorPriceMonitor
from src.monitoring.stock_checker import StockChecker
from src.content.image_generator import AIImageGenerator
from src.content.product_content import generate_product_content
from src.shopify.listing_manager import ShopifyListingManager

logger = logging.getLogger(__name__)


class JobScheduler:
    """
    Manages all recurring automation jobs.
    """

    def __init__(self, data_store: DataStore, config: AppConfig = None):
        self.store = data_store
        self.config = config or AppConfig()
        self.scheduler = BackgroundScheduler()
        self._setup_jobs()

    def _setup_jobs(self):
        """Configure all scheduled jobs."""

        # 1. Research pipeline — daily
        research_hours = int(self.config.get("research.research_frequency_hours", 24))
        self.scheduler.add_job(
            self.job_research_pipeline,
            IntervalTrigger(hours=research_hours),
            id="research_pipeline",
            name="AI Research Pipeline",
            replace_existing=True,
        )

        # 2. Poll for agent costs — every 30-60 min
        poll_minutes = int(self.config.polling_interval_minutes)
        self.scheduler.add_job(
            self.job_poll_agent_costs,
            IntervalTrigger(minutes=poll_minutes),
            id="poll_agent_costs",
            name="Poll Agent Costs",
            replace_existing=True,
        )

        # 3. Performance data pull — every 2 hours
        perf_hours = int(self.config.get("ads.performance_check_interval_hours", 2))
        self.scheduler.add_job(
            self.job_pull_performance,
            IntervalTrigger(hours=perf_hours),
            id="pull_performance",
            name="Pull Ad Performance",
            replace_existing=True,
        )

        # 4. Decision engine — every 2 hours (offset by 15 min from performance pull)
        self.scheduler.add_job(
            self.job_run_decisions,
            IntervalTrigger(hours=perf_hours, minutes=15),
            id="run_decisions",
            name="Decision Engine",
            replace_existing=True,
        )

        # 5. Sync labels — every 2 hours
        self.scheduler.add_job(
            self.job_sync_labels,
            IntervalTrigger(hours=2),
            id="sync_labels",
            name="Sync Product Labels",
            replace_existing=True,
        )

        # 6. Daily counter update — midnight
        self.scheduler.add_job(
            self.job_update_daily_counters,
            CronTrigger(hour=0, minute=5),
            id="daily_counters",
            name="Update Daily Counters",
            replace_existing=True,
        )

        # 7. Competitor price monitoring — weekly
        if self.config.get("monitoring.competitor_price_enabled", True):
            freq_days = int(self.config.get("monitoring.competitor_price_frequency_days", 7))
            self.scheduler.add_job(
                self.job_check_competitor_prices,
                IntervalTrigger(days=freq_days),
                id="competitor_prices",
                name="Competitor Price Check",
                replace_existing=True,
            )

        # 8. Stock monitoring — daily (if enabled)
        if self.config.get("monitoring.stock_monitoring_enabled", False):
            self.scheduler.add_job(
                self.job_check_stock,
                CronTrigger(hour=6),  # Check at 6 AM
                id="stock_check",
                name="Stock Availability Check",
                replace_existing=True,
            )

        # 9. Research feedback update — weekly
        self.scheduler.add_job(
            self.job_update_feedback,
            IntervalTrigger(days=7),
            id="update_feedback",
            name="Update Research Feedback",
            replace_existing=True,
        )

        # 10. Process ready-to-test products (content + listing) — every hour
        self.scheduler.add_job(
            self.job_process_ready_products,
            IntervalTrigger(hours=1),
            id="process_ready",
            name="Process Ready Products",
            replace_existing=True,
        )

    def start(self):
        """Start the scheduler."""
        self.scheduler.start()
        logger.info("Job scheduler started with %d jobs", len(self.scheduler.get_jobs()))

    def stop(self):
        """Stop the scheduler."""
        self.scheduler.shutdown()
        logger.info("Job scheduler stopped")

    def get_job_status(self) -> list[dict]:
        """Get status of all scheduled jobs."""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": str(job.next_run_time) if job.next_run_time else "Not scheduled",
                "trigger": str(job.trigger),
            })
        return jobs

    # -----------------------------------------------------------------------
    # Job implementations
    # -----------------------------------------------------------------------

    def job_research_pipeline(self):
        """Run the full research pipeline for all configured countries."""
        logger.info("=== Running Research Pipeline ===")
        try:
            pipeline = ResearchPipeline(self.store, self.config)
            stats = pipeline.run_for_all_countries()
            logger.info("Research pipeline complete: %s", stats)
        except Exception as e:
            logger.error("Research pipeline failed: %s", e, exc_info=True)

    def job_poll_agent_costs(self):
        """
        Two-step agent sync:
        1. Push new sourcing products to the Agent Tasks tab
        2. Check if the agent has filled in any landed costs
        """
        logger.info("Polling for agent cost updates...")
        try:
            # Step 1: Sync any new sourcing products to Agent Tasks tab
            added = self.store.sync_all_sourcing_to_agent_tasks()
            if added:
                logger.info("Synced %d new products to Agent Tasks tab", added)

            # Step 2: Check if agent has filled in costs
            validator = EconomicValidator(self.config)
            products = self.store.get_products_awaiting_cost()

            if not products:
                logger.debug("No new agent costs found")
                return

            logger.info("Found %d products with new agent costs", len(products))

            for product in products:
                validator.process_product_with_cost(product, self.store)
                # Mark the agent task as processed so we don't pick it up again
                self.store.mark_agent_task_processed(product.product_id)

        except Exception as e:
            logger.error("Agent cost polling failed: %s", e, exc_info=True)

    def job_pull_performance(self):
        """Pull performance data from Google Ads per country and update products."""
        logger.info("=== Pulling Performance Data (per country) ===")
        try:
            countries = self.config.countries
            total_updated = 0

            for country_info in countries:
                country = country_info.get("code", "DE") if isinstance(country_info, dict) else str(country_info)
                logger.info("Pulling performance for country: %s", country)

                perf_data = get_product_performance(
                    days=30, country=country, config=self.config
                )

                if not perf_data:
                    logger.info("No performance data for %s", country)
                    continue

                # Match performance data to products for this country
                products = self.store.get_products(country=country)
                product_map = {}
                for p in products:
                    if p.shopify_product_id:
                        offer_id = f"shopify_{p.country}_{p.shopify_product_id}"
                        product_map[offer_id] = p
                    product_map[p.product_id] = p

                updated = 0
                for perf in perf_data:
                    product_id = perf.get("product_id", "")
                    product = product_map.get(product_id)

                    if not product:
                        continue

                    updates = {
                        "clicks": perf.get("clicks", 0),
                        "impressions": perf.get("impressions", 0),
                        "spend": perf.get("spend", 0),
                        "conversions": perf.get("conversions", 0),
                        "revenue": perf.get("revenue", 0),
                        "roas": perf.get("roas", 0),
                    }

                    # Calculate net profit
                    validator = EconomicValidator(self.config)
                    product.spend = updates["spend"]
                    product.revenue = updates["revenue"]
                    product.conversions = updates["conversions"]
                    updates["net_profit"] = validator.calculate_net_profit(product)

                    self.store.update_product(product.product_id, updates)
                    updated += 1

                total_updated += updated
                logger.info("Updated performance for %d products in %s", updated, country)

            logger.info("Total performance updates: %d products", total_updated)

        except Exception as e:
            logger.error("Performance pull failed: %s", e, exc_info=True)

    def job_run_decisions(self):
        """Run the decision engine per country on active products."""
        logger.info("=== Running Decision Engine (per country) ===")
        try:
            engine = DecisionEngine(self.store, self.config)
            countries = self.config.countries
            total_actions = 0

            for country_info in countries:
                country = country_info.get("code", "DE") if isinstance(country_info, dict) else str(country_info)
                results = engine.evaluate_all_products(country=country)
                total_actions += len(results)
                if results:
                    logger.info("Decision engine (%s): %d actions taken", country, len(results))

            logger.info("Decision engine total: %d actions across %d countries", total_actions, len(countries))
        except Exception as e:
            logger.error("Decision engine failed: %s", e, exc_info=True)

    def job_sync_labels(self):
        """Sync product labels to Google Merchant Center."""
        logger.info("Syncing product labels to Merchant Center...")
        try:
            labels = MerchantCenterLabels(self.config)
            products = self.store.get_products()
            results = labels.sync_all_product_labels(products)
            logger.info("Label sync: %s", results)
        except Exception as e:
            logger.error("Label sync failed: %s", e, exc_info=True)

    def job_update_daily_counters(self):
        """Update daily tracking counters per country (days_testing, days_below_broas, etc.)."""
        logger.info("Updating daily counters (per country)...")
        try:
            engine = DecisionEngine(self.store, self.config)
            countries = self.config.countries
            for country_info in countries:
                country = country_info.get("code", "DE") if isinstance(country_info, dict) else str(country_info)
                engine.update_daily_counters(country=country)
                logger.info("Updated daily counters for %s", country)
        except Exception as e:
            logger.error("Daily counter update failed: %s", e, exc_info=True)

    def job_check_competitor_prices(self):
        """Check competitor prices for active products."""
        logger.info("Checking competitor prices...")
        try:
            monitor = CompetitorPriceMonitor(self.store, self.config)
            alerts = monitor.check_all_active_products()
            if alerts:
                logger.info("Found %d price alerts", len(alerts))
        except Exception as e:
            logger.error("Competitor price check failed: %s", e, exc_info=True)

    def job_check_stock(self):
        """Check stock availability for active products."""
        logger.info("Checking stock availability...")
        try:
            checker = StockChecker(self.store, self.config)
            alerts = checker.check_all_active_products()
            if alerts:
                logger.warning("Found %d out-of-stock products", len(alerts))
        except Exception as e:
            logger.error("Stock check failed: %s", e, exc_info=True)

    def job_update_feedback(self):
        """Update research feedback based on winner/loser patterns."""
        logger.info("Updating research feedback...")
        try:
            engine = DecisionEngine(self.store, self.config)
            engine.update_research_feedback()
        except Exception as e:
            logger.error("Feedback update failed: %s", e, exc_info=True)

    def job_process_ready_products(self):
        """
        Process products that are ready_to_test:
        1. Generate AI images
        2. Generate product content
        3. Create Shopify listing
        4. Update status to listing_created
        """
        logger.info("Processing ready-to-test products...")
        try:
            products = self.store.get_products(status=ProductStatus.READY_TO_TEST.value)

            if not products:
                logger.debug("No ready-to-test products")
                return

            image_gen = AIImageGenerator(self.config)
            shopify = ShopifyListingManager(self.config)

            for product in products:
                try:
                    self._process_single_product(product, image_gen, shopify)
                except Exception as e:
                    logger.error(
                        "Failed to process product %s: %s",
                        product.product_id, e
                    )

        except Exception as e:
            logger.error("Product processing failed: %s", e, exc_info=True)

    def _process_single_product(
        self,
        product: Product,
        image_gen: AIImageGenerator,
        shopify: ShopifyListingManager,
    ):
        """Process a single ready-to-test product through content + listing creation."""

        logger.info("Processing product: %s (%s)", product.keyword, product.product_id)

        # 1. Generate AI images
        reference_urls = (
            product.aliexpress_image_urls.split(",")
            if product.aliexpress_image_urls else []
        )
        reference_urls = [url.strip() for url in reference_urls if url.strip()]

        images = []
        if reference_urls:
            generated = image_gen.generate_product_images(
                reference_image_urls=reference_urls,
                product_description=product.keyword,
                target_language=product.language or "de",
                num_images=4,
            )
            images = [g["image_data"] for g in generated if g.get("image_data")]

        # 2. Generate product content
        content = generate_product_content(product, config=self.config)

        if not content:
            logger.warning("Content generation failed for %s, using basic content", product.keyword)
            from src.content.product_content import generate_basic_content
            content = generate_basic_content(
                product.keyword,
                float(product.selling_price),
                product.language or "de",
                product.country or "DE",
            )

        # 3. Create Shopify listing
        result = shopify.create_listing(
            product=product,
            title=content["title"],
            description_html=content["description_html"],
            images=images,
            price=float(product.selling_price),
            meta_title=content.get("meta_title", ""),
            meta_description=content.get("meta_description", ""),
            tags=content.get("tags", ""),
            product_type=content.get("product_type", ""),
        )

        if result:
            # Update product with Shopify data
            updates = {
                "shopify_product_id": result["shopify_product_id"],
                "shopify_product_url": result["shopify_product_url"],
                "test_status": ProductStatus.LISTING_CREATED.value,
                "reason": "Shopify listing created, awaiting Merchant Center sync",
            }
            self.store.update_product(product.product_id, updates)

            # Log
            log = ActionLog(
                product_id=product.product_id,
                action_type=ActionType.LISTING_CREATED.value,
                old_status=ProductStatus.READY_TO_TEST.value,
                new_status=ProductStatus.LISTING_CREATED.value,
                reason="Shopify listing auto-created with AI content and images",
                details=f"Shopify ID: {result['shopify_product_id']}, "
                        f"Images: {len(images)}, Title: {content['title'][:50]}",
                country=product.country,
            )
            self.store.add_log(log)

            # Notification
            self.store.add_notification(Notification(
                title="Listing created",
                message=f"Shopify listing created for '{product.keyword}': {content['title'][:50]}",
                level="success",
                product_id=product.product_id,
            ))

            logger.info(
                "Created Shopify listing for %s: %s",
                product.keyword, result["shopify_product_id"]
            )
        else:
            logger.error("Failed to create Shopify listing for %s", product.keyword)
