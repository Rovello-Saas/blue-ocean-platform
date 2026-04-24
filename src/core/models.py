"""
Data models for the Blue Ocean Platform.
All models use dataclasses for clean serialization and SaaS-ready architecture.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ProductStatus(str, Enum):
    """Product lifecycle statuses.

    Note: there is no intermediate ``cost_received`` status. When the agent
    fills in the landed cost, the economics gate runs immediately and the
    product moves straight to READY_TO_TEST (pass) or REJECTED (fail). The
    log stream still carries a ``cost_received`` ActionType for audit, but
    a product never *lives* in that state.
    """
    DISCOVERED = "discovered"
    PENDING_MANUAL_REVIEW = "pending_manual_review"
    SOURCING = "sourcing"
    READY_TO_TEST = "ready_to_test"
    LISTING_CREATED = "listing_created"
    TESTING = "testing"
    WINNER = "winner"
    SCALING = "scaling"
    PAUSED = "paused"
    KILLED = "killed"
    REJECTED = "rejected"


class CompetitionType(str, Enum):
    """Whether competitors sell the same or different products."""
    SAME_PRODUCT = "same_product"
    DIVERSE_PRODUCTS = "diverse_products"
    UNKNOWN = "unknown"


class ShippingModel(str, Enum):
    FREE = "free"
    PAID = "paid"
    THRESHOLD = "threshold"


class AdsAction(str, Enum):
    ADD_TO_TESTING = "add_to_testing"
    MOVE_TO_WINNERS = "move_to_winners"
    PAUSE_ADS = "pause_ads"
    ENABLE_ADS = "enable_ads"
    KILL_ADS = "kill_ads"
    SCALE_BUDGET = "scale_budget"
    NONE = ""


class ResearchSource(str, Enum):
    AI = "ai"
    MANUAL = "manual"


class ActionType(str, Enum):
    PRODUCT_DISCOVERED = "product_discovered"
    SOURCING_STARTED = "sourcing_started"
    COST_RECEIVED = "cost_received"
    ECONOMICS_PASSED = "economics_passed"
    ECONOMICS_FAILED = "economics_failed"
    LISTING_CREATED = "listing_created"
    TESTING_STARTED = "testing_started"
    PRODUCT_KILLED = "product_killed"
    PRODUCT_PAUSED = "product_paused"
    PRODUCT_WINNER = "product_winner"
    BUDGET_SCALED = "budget_scaled"
    PRODUCT_RETEST = "product_retest"
    # Manual "send this back to the sourcing agent for a fresh cost" —
    # distinct from PRODUCT_RETEST (which resets the whole lifecycle to
    # DISCOVERED) and from SOURCING_STARTED (fired by the pipeline at
    # initial discovery). Emitted by the Reopen-sourcing button on the
    # Products page when the user wants the agent to redo a wrong cost.
    PRODUCT_REOPEN_SOURCING = "product_reopen_sourcing"
    PRICE_ALERT = "price_alert"
    STOCK_ALERT = "stock_alert"


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class KeywordResearch:
    """Result from the keyword research pipeline."""
    keyword_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    keyword: str = ""
    country: str = "DE"
    language: str = "de"
    monthly_search_volume: int = 0
    estimated_cpc: float = 0.0
    competition_level: str = ""  # low / medium / high
    intent_score: int = 0  # 0-100
    research_source: str = ResearchSource.AI.value
    competitor_count: int = 0
    unique_product_count: int = 0
    competition_type: str = CompetitionType.UNKNOWN.value
    differentiation_score: float = 0.0
    avg_competitor_price: float = 0.0
    median_competitor_price: float = 0.0
    estimated_selling_price: float = 0.0
    google_shopping_url: str = ""  # link to Google Shopping results for this keyword
    competitor_pdp_url: str = ""  # top competitor's actual product page URL
    # Thumbnail URL of the first Google Shopping result for this keyword.
    # Sourced from SerpAPI's `shopping_results[0].thumbnail` so we always
    # have a visual even when AliExpress doesn't match — the keyword
    # must have passed the competition filter to reach this field, so
    # there's guaranteed to be a Shopping result.
    competitor_thumbnail_url: str = ""
    aliexpress_url: str = ""
    aliexpress_price: float = 0.0
    aliexpress_rating: float = 0.0
    aliexpress_orders: int = 0
    aliexpress_image_urls: str = ""  # comma-separated
    # Top-3 AliExpress listings stored as JSON string
    # Each is {"title","url","price","rating","orders","image_url","tag"}
    aliexpress_top3_json: str = ""
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    notes: str = ""
    # Inbox decision state. Empty / "active" = needs a decision; "archived"
    # = human said skip; "sent_to_sourcing" = promoted into a Product row.
    # Used by the Research-page keyword inbox to cleanly separate open
    # decisions from already-handled ones without deleting any history.
    status: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "KeywordResearch":
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered)


@dataclass
class Product:
    """Central product model — the main entity in the pipeline."""
    product_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    keyword_id: str = ""

    # Research data
    country: str = "DE"
    language: str = "de"
    keyword: str = ""
    monthly_search_volume: int = 0
    estimated_cpc: float = 0.0
    competition_level: str = ""
    competitor_count: int = 0
    differentiation_score: float = 0.0
    competition_type: str = CompetitionType.UNKNOWN.value

    # Sourcing
    google_shopping_url: str = ""  # link to competitor Google Shopping results
    competitor_pdp_url: str = ""  # top competitor's actual product page URL
    aliexpress_url: str = ""
    aliexpress_price: float = 0.0
    aliexpress_rating: float = 0.0
    aliexpress_orders: int = 0
    aliexpress_image_urls: str = ""  # comma-separated
    aliexpress_top3_json: str = ""   # JSON string with top 3 listings
    # Diagnostics for the AliExpress match — which feed we found it in,
    # which pass/strategy picked it, and the title that was used as the
    # needle. JSON-encoded so we can extend the schema without migrating
    # the sheet. Useful when auditing "why did this keyword pick that
    # weird product", especially since the DS feed coverage is structural
    # (bestsellers only) and false-positive-prone.
    aliexpress_match_meta_json: str = ""

    # Pricing
    selling_price: float = 0.0
    landed_cost: float = 0.0  # Filled by agent (includes shipping to customer)

    # Auto-calculated economics
    gross_margin: float = 0.0
    gross_margin_pct: float = 0.0
    transaction_fees: float = 0.0
    net_margin: float = 0.0
    net_margin_pct: float = 0.0
    break_even_roas: float = 0.0
    target_roas: float = 0.0
    break_even_cpa: float = 0.0
    max_allowed_cpc: float = 0.0
    test_budget: float = 0.0
    kill_threshold_spend: float = 0.0

    # Performance (read from Google Ads)
    clicks: int = 0
    impressions: int = 0
    spend: float = 0.0
    conversions: int = 0
    revenue: float = 0.0
    roas: float = 0.0
    net_profit: float = 0.0

    # Status & control
    test_status: str = ProductStatus.DISCOVERED.value
    ads_action: str = AdsAction.NONE.value
    listing_group_status: str = "not_added"

    # Shopify
    shopify_product_id: str = ""
    shopify_product_url: str = ""

    # Tracking
    days_testing: int = 0
    days_below_broas: int = 0
    consecutive_days_above_scale_threshold: int = 0
    days_since_last_scale: int = 0
    testing_started_at: str = ""
    last_scale_at: str = ""

    # Logging
    reason: str = ""
    last_action_at: str = ""
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    # Request real photos flag
    request_real_photos: bool = False

    # Free-form notes carried through the pipeline — set by the research
    # pipeline (AliExpress match rationale), the agent (sourcing context),
    # or the Manual Review form (whatever the human wrote). Persisted to
    # the Products sheet so nothing gets silently dropped.
    notes: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Product":
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {}
        for k, v in data.items():
            if k in valid_fields:
                # Handle type conversions from sheet data (strings)
                field_type = cls.__dataclass_fields__[k].type
                if v is None or v == "":
                    filtered[k] = cls.__dataclass_fields__[k].default if hasattr(cls.__dataclass_fields__[k], 'default') else v
                    continue
                try:
                    if field_type == "int":
                        filtered[k] = int(float(v)) if v else 0
                    elif field_type == "float":
                        filtered[k] = float(v) if v else 0.0
                    elif field_type == "bool":
                        filtered[k] = str(v).lower() in ("true", "1", "yes")
                    else:
                        filtered[k] = v
                except (ValueError, TypeError):
                    filtered[k] = v
        return cls(**filtered)


@dataclass
class ActionLog:
    """Audit trail entry for every automated action."""
    log_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    product_id: str = ""
    action_type: str = ""
    old_status: str = ""
    new_status: str = ""
    reason: str = ""
    details: str = ""
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    country: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ActionLog":
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered)


@dataclass
class Notification:
    """Dashboard notification."""
    notification_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    title: str = ""
    message: str = ""
    level: str = "info"  # info, warning, success, error
    read: bool = False
    product_id: str = ""
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CountryConfig:
    """Per-country configuration."""
    code: str = "DE"
    name: str = "Germany"
    language: str = "de"
    currency: str = "EUR"
    enabled: bool = True


# ---------------------------------------------------------------------------
# Image Studio Models
# ---------------------------------------------------------------------------

class ImageJobStatus(str, Enum):
    """Status of an image generation job."""
    PENDING = "pending"
    GENERATING = "generating"
    REVIEW = "review"          # waiting for user approval
    APPROVED = "approved"
    REJECTED = "rejected"
    REGENERATING = "regenerating"
    UPLOADED = "uploaded"       # pushed to Shopify / Drive
    FAILED = "failed"
    ARCHIVED = "archived"      # kept for history, replaced by a newer attempt


class ImageGeneratorType(str, Enum):
    """Available image generator backends."""
    OPENAI_GPT_IMAGE = "gpt-image-1"
    GOOGLE_IMAGEN_4 = "google-imagen-4"
    GOOGLE_IMAGEN_4_FAST = "google-imagen-4-fast"
    NANO_BANANA = "nano-banana"
    NANO_BANANA_PRO = "nano-banana-pro"
    FAL_FLUX_PRO = "fal-flux-pro"
    FAL_FLUX_DEV = "fal-flux-dev"
    FAL_FLUX_SCHNELL = "fal-flux-schnell"


@dataclass
class ImageRequest:
    """A single image generation request within a job."""
    request_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    image_index: int = 0           # position in the set (1-based)
    prompt: str = ""
    negative_prompt: str = ""
    text_labels: str = ""          # specific text/labels to include in the image
    text_overlay: bool = False     # if True, use Pillow to overlay text (guaranteed legible)
    generator: str = ImageGeneratorType.OPENAI_GPT_IMAGE.value
    reference_image_url: str = ""  # optional reference image
    status: str = ImageJobStatus.PENDING.value
    image_url: str = ""            # generated image URL or base64 data ref
    image_data: bytes = field(default=b"", repr=False)
    feedback: str = ""             # user feedback when rejected
    retry_count: int = 0
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("image_data", None)  # Don't serialize binary data
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "ImageRequest":
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_fields and k != "image_data"}
        return cls(**filtered)


@dataclass
class ImageJob:
    """
    A batch image generation job for a product.
    Contains multiple ImageRequests (one per image).
    """
    job_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    product_id: str = ""
    product_keyword: str = ""
    num_images: int = 4
    default_generator: str = ImageGeneratorType.OPENAI_GPT_IMAGE.value
    status: str = ImageJobStatus.PENDING.value
    images: list = field(default_factory=list)  # list of ImageRequest dicts
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    notes: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        # images is already a list of dicts from asdict, but remove image_data
        clean_images = []
        for img in d.get("images", []):
            img.pop("image_data", None)
            clean_images.append(img)
        d["images"] = clean_images
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "ImageJob":
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {}
        for k, v in data.items():
            if k in valid_fields:
                if k == "images" and isinstance(v, str):
                    import json as _json
                    try:
                        filtered[k] = _json.loads(v)
                    except (ValueError, TypeError):
                        filtered[k] = []
                else:
                    filtered[k] = v
        return cls(**filtered)


@dataclass
class ResearchFeedback:
    """Feedback data for improving LLM keyword research over time."""
    winning_categories: list = field(default_factory=list)
    losing_categories: list = field(default_factory=list)
    winning_keywords: list = field(default_factory=list)
    losing_keywords: list = field(default_factory=list)
    avg_winning_margin_pct: float = 0.0
    avg_winning_price_range: str = ""
    avg_winning_competition: int = 0
    last_updated: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_summary(self) -> str:
        """Generate a text summary for LLM prompt injection."""
        parts = []
        if self.winning_categories:
            parts.append(f"Winning product categories: {', '.join(self.winning_categories[:10])}")
        if self.losing_categories:
            parts.append(f"Categories to AVOID: {', '.join(self.losing_categories[:10])}")
        if self.avg_winning_margin_pct > 0:
            parts.append(f"Winning products typically have margins above {self.avg_winning_margin_pct:.0%}")
        if self.avg_winning_price_range:
            parts.append(f"Best performing price range: {self.avg_winning_price_range}")
        if self.avg_winning_competition > 0:
            parts.append(f"Best competition level: {self.avg_winning_competition} or fewer competitors")
        if not parts:
            return "No historical data available yet."
        return "\n".join(parts)
