"""
LLM-based keyword ideation using Anthropic Claude Sonnet 4.5.

Generates product-intent keywords for target countries with an optional
feedback loop that learns from past winners/losers.

Why Claude Sonnet 4.5 (not GPT-4o):
- Better instruction-following on structured JSON output — consistently
  returns the requested number of keywords (GPT-4o would bail early around
  50/150 with JSON-format + high-temperature requests).
- Stronger multilingual output quality (natural German / Dutch / French).
- Single-provider stack alignment — Page Cloner already uses Anthropic.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

import anthropic

from src.core.config import AppConfig, ANTHROPIC_API_KEY
from src.core.cost_tracker import CostTracker
from src.core.models import ResearchFeedback

logger = logging.getLogger(__name__)


# Anthropic's model alias for "latest Sonnet 4.5".
CLAUDE_MODEL = "claude-sonnet-4-5"


SYSTEM_PROMPT = """You are an expert e-commerce product researcher specializing in finding profitable dropshipping products for Google Shopping / Performance Max campaigns.

Your task is to generate a list of commercially-viable product keywords that real people actually type into Google when they want to BUY a product.

CRITICAL — keyword shape (this is the single biggest thing to get right):
- Generate HEAD-TERM keywords: **2–4 words**. Short, like how people really search.
- GOOD: "bluetooth kopfhörer", "kabellose kopfhörer", "noise cancelling kopfhörer", "holzuhr herren"
- BAD:  "kabellose Bluetooth Kopfhörer mit Geräuschunterdrückung", "wireless bluetooth earbuds with active noise cancelling"
- Long descriptive phrases look specific but return **zero search volume** from Keyword Planner because no one types them verbatim. The downstream volume filter will kill them.
- If a product has an important modifier (colour, size, material, gender, use case), include it as ONE extra word — don't stack three modifiers into one phrase.
- Get diversity across keywords by spanning **different product categories**, not by adding modifiers to the same product.

Other rules:
1. PRODUCT-INTENT only — keywords for people searching to buy, not to learn or compare.
2. Generic product terms, not brand names (no "Apple", "Samsung", "Nike").
3. Target EUR 20–200 selling price — dropshipping-economic range.
4. Avoid ultra-broad single words that compete with Amazon on raw traffic ("shoes", "watch") — aim for the **head of a specific product niche** instead ("running shoes men", "wooden watch men", "standing desk").
5. Seasonal relevance is fine, but the keyword itself shouldn't be seasonal in phrasing.
6. Output keywords in the TARGET LANGUAGE for the target country (lowercase is natural for most languages — match how people actually type).
7. Deliver EXACTLY the requested number of keywords. Prefer **different products over more modifiers on the same product**.

Output format: Return a JSON object with a single top-level key "keywords" whose value is an array of objects. Each object must have:
- "keyword": the search keyword in the target language. 2–4 words. Head-term shape.
- "english_search_terms": an array of 2-3 short English product terms that someone on AliExpress would use to search for the SAME product. Lowercase, no punctuation, prefer noun-first (e.g. for "holzuhr herren" → ["wooden watch men", "mens wooden watch", "bamboo watch"]). These are used for cross-language product matching against AliExpress's English feeds — accuracy matters.
- "category": product category (in English)
- "estimated_price_range": approximate selling price range in EUR (e.g., "20-40")
- "competition_signal": your honest read on market saturation. One of:
    * "low"    — niche or underserved; few big retailers dominate results
    * "medium" — competitive but beatable with good creative / angle
    * "high"   — saturated; Amazon, category leaders, or big-brand direct-to-consumer own the SERP
- "sourcing_difficulty": how easy this is to ship via AliExpress dropshipping. One of:
    * "easy"   — standard gadget / home / lifestyle good, widely available from multiple suppliers
    * "medium" — specific variants or bulky items, longer shipping, slimmer supplier choice
    * "hard"   — niche, heavy, fragile, or supplier availability is thin
- "product_type": commerce category. One of:
    * "dropshippable"    — normal dropship-friendly physical good
    * "branded"          — requires specific brand (Apple, Nike, etc.) — skip
    * "regulated"        — legal/compliance constraints (supplements, medical, CE-critical electronics, age-gated) — skip
    * "perishable"       — food, fresh, short shelf life — skip
    * "counterfeit_risk" — knockoff-heavy category (luxury bags, designer watches) — skip
- "reasoning": brief note on why this could be profitable (in English), 1 sentence max

BE HONEST — we use these signals to drop weak keywords before spending on paid APIs. A confidently-labelled "high competition" or "hard sourcing" keyword that we skip is cheaper than paying $0.015 to confirm what you already suspected. Don't flatter.

Return ONLY the JSON object. No prose, no markdown fences, no commentary before or after."""


def generate_keywords(
    country: str = "DE",
    language: str = "de",
    num_keywords: int = 150,
    category_focus: list[str] = None,
    feedback: Optional[ResearchFeedback] = None,
    config: AppConfig = None,
    cost_tracker: Optional[CostTracker] = None,
    avoid_keywords: Optional[list[str]] = None,
) -> list[dict]:
    """
    Generate product-intent keywords using Claude Sonnet 4.5.

    Args:
        country: Target country code (e.g., "DE")
        language: Target language (e.g., "de")
        num_keywords: Number of keywords to generate
        category_focus: Optional list of categories to focus on
        feedback: Historical feedback from past winners/losers
        config: App configuration
        cost_tracker: Optional CostTracker — records Anthropic token usage
            for every successful call so the Discover run can show total spend.
        avoid_keywords: List of keyword strings to exclude from generation.
            Typically the union of the Keywords sheet and Research Drops sheet
            for this country. Without this, the LLM reliably regenerates the
            same obvious German dropshipping staples every run (LED-Streifen,
            Luftbefeuchter, Bluetooth-Lautsprecher, …) and they all get
            killed at the dedup stage — wasting the LLM's output slots on
            ideas the pipeline has already processed. Passing the avoid
            list shifts that selection pressure back to the model so it
            uses every output slot on a novel product idea.

    Returns:
        List of keyword dicts with keyword, english_search_terms, category,
        estimated_price_range, reasoning.
    """
    config = config or AppConfig()

    if not ANTHROPIC_API_KEY or ANTHROPIC_API_KEY.startswith("your_") or len(ANTHROPIC_API_KEY) < 20:
        logger.warning("ANTHROPIC_API_KEY not configured — skipping LLM ideation")
        return []

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Country and language name lookups — Claude performs best when given the
    # full human name plus the code, rather than just "DE".
    country_names = {
        "DE": "Germany", "NL": "Netherlands", "AT": "Austria",
        "FR": "France", "BE": "Belgium", "CH": "Switzerland",
        "ES": "Spain", "IT": "Italy", "PL": "Poland",
        "US": "United States", "UK": "United Kingdom", "GB": "United Kingdom",
    }
    country_name = country_names.get(country, country)
    language_names = {
        "de": "German", "nl": "Dutch", "fr": "French",
        "es": "Spanish", "it": "Italian", "pl": "Polish", "en": "English",
    }
    language_name = language_names.get(language, language)

    user_prompt_lines = [
        f"Generate {num_keywords} product-intent keywords for the {country_name} market.",
        "",
        f"Target language: {language_name}",
        f"Target country: {country_name}",
    ]

    if category_focus:
        user_prompt_lines.append("")
        user_prompt_lines.append(f"Focus on these categories: {', '.join(category_focus)}")

    if feedback:
        feedback_text = feedback.to_summary()
        if feedback_text != "No historical data available yet.":
            user_prompt_lines.extend([
                "",
                "IMPORTANT — Historical performance data from our previous products:",
                feedback_text,
                "",
                "Use this data to generate BETTER keywords that are more likely to be profitable.",
                "Focus on categories and price ranges that have worked well.",
                "AVOID categories that have consistently failed.",
            ])

    # Inject the "already tried / already rejected" blacklist. We cap the
    # list at ~400 terms to keep the input-token bill sane (every avoid
    # keyword is ~5–10 tokens, so 400 ≈ 3–4k tokens — an order of
    # magnitude less than the output). Newest-last ordering preserves
    # recency: if the sheet grows huge we prefer to show Claude the recent
    # failures over the oldest ones (older categories may have become
    # interesting again if the market shifted). Callers send us the ordering
    # they want; we treat the list as authoritative and just truncate.
    if avoid_keywords:
        cleaned = [
            (k or "").strip().lower()
            for k in avoid_keywords
            if k and isinstance(k, str) and k.strip()
        ]
        # De-duplicate while preserving order (first occurrence wins).
        seen: set[str] = set()
        deduped: list[str] = []
        for k in cleaned:
            if k not in seen:
                seen.add(k)
                deduped.append(k)
        # Keep the TAIL (most recent) to stay under the soft token cap.
        max_avoid = 400
        if len(deduped) > max_avoid:
            deduped = deduped[-max_avoid:]
        if deduped:
            # Comma-separated is ~2x cheaper on tokens than a bulleted list
            # and Claude handles both formats equally well for this kind
            # of "exclude these strings" constraint.
            blacklist = ", ".join(deduped)
            user_prompt_lines.extend([
                "",
                f"CRITICAL — these {len(deduped)} keywords have ALREADY been "
                f"tried or rejected in previous runs. Do NOT suggest any of "
                f"them, and do NOT suggest close variations (same product "
                f"with a synonym, plural, or reworded modifier — e.g. if "
                f"'led streifen' is on the list, skip 'led strip', 'led "
                f"band', 'led lichtband' etc). Pick genuinely DIFFERENT "
                f"product categories:",
                blacklist,
                "",
                f"Your {num_keywords} keywords must all be novel product "
                f"ideas not in the list above. If you find yourself about "
                f"to suggest something adjacent, pivot to a different "
                f"product niche entirely.",
            ])

    user_prompt_lines.extend([
        "",
        f"Remember:",
        f"- All keywords must be in {language_name}",
        f"- **2–4 words per keyword** — head-term shape, not long descriptive phrases",
        f"- Diversity comes from spanning different product niches, not stacking modifiers",
        f"- EUR 20–200 selling-price range",
        f"- Think about what someone actually types into Google to BUY — short, imperfect, lowercase",
        f"- Deliver EXACTLY {num_keywords} keywords in the JSON array",
        "",
        "Return ONLY a valid JSON object with the shape:",
        '{"keywords": [ {...}, {...}, ... ]}',
    ])
    user_prompt = "\n".join(user_prompt_lines)

    # 2026-04-22: max_tokens scales with num_keywords instead of being a
    # flat 20000. Each keyword entry costs ~180 tokens (keyword +
    # english_search_terms + category + price + reasoning + signals).
    # Budget = num_keywords × 200 with floor 2000 (for tiny requests) and
    # ceiling 8000 (which caps generation at ~60-70s — safely inside
    # Anthropic's gateway timeout). Old 20000 allowed the model to
    # generate until timeout hit, failing every retry. At hybrid's
    # num_keywords=15 this gives max_tokens=3000 ≈ 25s generation.
    max_tokens_budget = min(8000, max(2000, num_keywords * 200))
    last_err: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            # Use a messages.create call with a pre-filled "{" assistant prefix
            # to force JSON output without needing response_format. Claude
            # continues the JSON cleanly from that seed — equivalent to OpenAI's
            # json_object mode but more reliable.
            message = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=max_tokens_budget,
                temperature=0.7,
                system=SYSTEM_PROMPT,
                messages=[
                    {"role": "user", "content": user_prompt},
                    {"role": "assistant", "content": "{"},
                ],
            )

            # Claude's response is a list of content blocks (text).
            if not message.content:
                logger.warning(
                    "Claude returned empty content (stop_reason=%s, attempt %d/3)",
                    message.stop_reason, attempt,
                )
                last_err = RuntimeError(f"empty completion (stop={message.stop_reason})")
                continue

            raw_text = "".join(
                block.text for block in message.content if hasattr(block, "text")
            ).strip()
            # We seeded with "{" so prepend it back.
            json_text = "{" + raw_text
            # Claude occasionally wraps output in ```json fences despite the
            # explicit instruction not to. Strip them defensively.
            if json_text.startswith("{```") or "```json" in json_text[:20]:
                json_text = json_text.replace("```json", "").replace("```", "").strip()

            try:
                result = json.loads(json_text)
            except json.JSONDecodeError as je:
                # Log the first and last 200 chars so we can see whether it
                # was truncated or malformed mid-stream.
                logger.warning(
                    "Attempt %d: JSON parse failed: %s. head=%r tail=%r",
                    attempt, je, json_text[:200], json_text[-200:],
                )
                last_err = je
                continue

            # Accept both {"keywords": [...]} (preferred) and
            # {"results": [...]} (common model drift).
            if isinstance(result, dict):
                keywords = result.get("keywords") or result.get("results") or []
            elif isinstance(result, list):
                keywords = result
            else:
                keywords = []

            if not keywords:
                logger.warning(
                    "Claude returned 0 keywords (shape=%s, attempt %d/3); retrying",
                    type(result).__name__, attempt,
                )
                last_err = RuntimeError("parsed response contained no keywords")
                continue

            # Log if we got fewer than requested, but still return what we
            # got — a short list is better than nothing. The pipeline's
            # volume filter will cull further anyway.
            if len(keywords) < num_keywords:
                logger.info(
                    "Claude returned %d/%d keywords for %s (%s) — shorter than requested",
                    len(keywords), num_keywords, country, language,
                )
            else:
                logger.info(
                    "Generated %d keywords for %s (%s) on attempt %d",
                    len(keywords), country, language, attempt,
                )

            # Record exact cost from Anthropic's usage payload.
            # `message.usage` carries `input_tokens` and `output_tokens` —
            # we multiply by current Sonnet 4.5 pricing in cost_tracker.
            if cost_tracker is not None:
                usage = getattr(message, "usage", None)
                if usage is not None:
                    cost_tracker.record_anthropic(
                        model=CLAUDE_MODEL,
                        input_tokens=getattr(usage, "input_tokens", 0) or 0,
                        output_tokens=getattr(usage, "output_tokens", 0) or 0,
                        context=f"ideation {country}/{language} ({len(keywords)} kws)",
                    )

            return keywords

        except anthropic.APIError as e:
            logger.warning("Attempt %d: Anthropic API error: %s", attempt, e)
            last_err = e
            continue
        except Exception as e:
            logger.warning("Attempt %d: Claude keyword generation failed: %s", attempt, e)
            last_err = e
            continue

    logger.error("Claude keyword generation failed after 3 attempts: %s", last_err)
    return []


TRANSLATE_SYSTEM_PROMPT = """You are a localisation + keyword specialist for Google Shopping / Performance Max dropshipping.

Your input is a list of REAL PRODUCTS that are currently top sellers on Google Shopping. Your job is NOT to invent new products — it's to convert each real product into head-term keywords that a buyer in the target market actually types into Google when they want to buy this kind of thing.

DEFAULT TO GENERATING — skipping is the exception, not the rule.
The input list is already pre-filtered (we've dropped SINKER products, and we pay for every input token). If you skip a product, we waste an input slot. A brand name in the title is NOT a reason to skip — strip the brand and output the generic category.
  Example: "Shark Navigator Professional Vacuum" → KEEP and output "staubsauger kabellos", "beutelloser staubsauger" (NOT skipped as branded).
  Example: "Apple iPhone 15 Pro Max Case" → KEEP and output "handyhülle", "schutzhülle smartphone" (skip ONLY the brand, keep the category).
  Example: "LEGO Star Wars Millennium Falcon" → KEEP and output "klemmbausteine raumschiff", "bausteine star wars alternative" (generic competitors exist).

ONLY skip when a product has NO viable generic alternative that a dropshipper can legally source from AliExpress:
    * regulated        — prescription medical, supplements with active compounds, age-gated (firearms, nicotine)
    * perishable       — food, fresh produce, cut flowers
    * counterfeit_risk — the product CATEGORY is knockoff-saturated (designer handbags, luxury watches, branded sneakers) — NOT just because one input title has a brand
Do NOT use "branded" or "unsuitable_dropship" as skip reasons — if the category is brand-named, output the generic equivalent; if it's heavy/fragile, that's the economics gate's job downstream.

CRITICAL rules:
- Keywords must be in the TARGET LANGUAGE (e.g. German for DE market).
- 2–4 words. Head-term shape. Lowercase is natural for most languages.
- Generic category keyword, NOT the brand-specific product name.
- Output UP TO 5 keywords per product (cap is configurable; the caller will tell you the max). Use all the slots you're given — each keyword should be a genuinely different search intent (category synonym, buyer-side variant, use-case angle), not rewording. If only one obvious term truly fits, return 1, but bias toward filling the slots.

For each keyword you produce, also return:
- "english_search_terms": 2–3 short English product terms an AliExpress search would use for the SAME product. Lowercase, noun-first. Used for cross-language supplier matching.
- "category": product category in English, same cluster as the input.
- "estimated_price_range": approximate selling price range in EUR (e.g., "25-60") — use the input product's price_range as a strong anchor.
- "competition_signal": "low" / "medium" / "high" — your honest read of how saturated this keyword is.
- "sourcing_difficulty": "easy" / "medium" / "hard".
- "product_type": "dropshippable" (default) / "branded" / "regulated" / "perishable" / "counterfeit_risk".
- "reasoning": 1 sentence on why this is a good dropship opportunity (in English).

Be honest. If a product is branded-only and you can't produce a generic keyword for it, skip it — don't force a meaningless translation.

Output format: ONE top-level JSON object:
{
  "translations": [
    {
      "source_title": "<the input product title>",
      "skip_reason": "<empty string if not skipped, else one of: regulated|perishable|counterfeit_risk>",
      "keywords": [
        {
          "keyword": "<in target language>",
          "english_search_terms": ["...", "..."],
          "category": "...",
          "estimated_price_range": "25-60",
          "competition_signal": "low",
          "sourcing_difficulty": "easy",
          "product_type": "dropshippable",
          "reasoning": "..."
        },
        ...
      ]
    },
    ...
  ]
}

Return ONLY the JSON object. No prose, no markdown fences."""


# ---------------------------------------------------------------------------
# Batching the translator — why.
#
# A previous version sent all post-RISERs products in a single `messages.create`
# call with max_tokens=20000. With 99 products that request consistently hung
# ~60s server-side then returned "Connection error" (Anthropic's gateway drops
# requests whose generation runs long enough to exceed its internal timeout).
# The result: three wasted retry cycles, 3 min of wall time, zero keywords.
#
# Batching into ~20 products per call keeps each individual request well inside
# the timeout window (~10-20s per call, <8k tokens of output), lets us succeed-
# partial when some batches fail, and gives us proportional cost visibility.
# We keep the 3-attempt retry logic inside each batch so transient 529s still
# recover.
# ---------------------------------------------------------------------------
# 2026-04-22: dropped from 20 → 8 after iterating on the
# default-to-generating prompt. Each output keyword carries ~180 tokens
# (keyword, english_search_terms, category, price, signals, reasoning),
# so batch × kw × 180 has to fit BOTH max_output_tokens AND Anthropic's
# ~60s gateway timeout for a single request. At batch=8, kw=3 that's
# 24 × 180 ≈ 4.3k output tokens ≈ ~40s generation — comfortable margin
# on both axes. 94-product DE pool → 12 batches, each ~40s = 8 min
# total translation wall time. Reliability > speed here.
TRANSLATE_BATCH_SIZE = 8  # products per Claude call


def translate_products_to_keywords(
    products: list[dict],
    country: str = "DE",
    language: str = "de",
    *,
    config: AppConfig = None,
    cost_tracker: Optional[CostTracker] = None,
    max_kw_per_product: int = 3,
) -> list[dict]:
    """Convert real product titles from Best Sellers into localised search keywords.

    This is the REPLACEMENT for `generate_keywords()` — instead of Claude
    hallucinating products from nothing, it receives a list of real products
    that Google Shopping already proves are selling, and translates them
    into search-keyword shape. Downstream filters (DataForSEO volume,
    SerpAPI competition, AliExpress matching, economics gate) are unchanged.

    The translator batches products into groups of `TRANSLATE_BATCH_SIZE`
    (currently 20) to keep each Claude request inside Anthropic's per-request
    timeout window. Each batch has independent retry logic — a failed batch
    doesn't sink the whole run.

    Args:
        products: list of dicts with at least:
            - "title"              (required — the product name from Best Sellers)
            - "brand"              (optional)
            - "category_l1/l2/l3"  (optional — Google taxonomy path)
            - "price_min_eur"      (optional — Google's reported range low)
            - "price_max_eur"      (optional — Google's reported range high)
            - "relative_demand_change" (optional — RISER / FLAT / SINKER)
        country: target country ISO code
        language: target language ISO code
        config: AppConfig (unused today but threaded for future tuning)
        cost_tracker: records Anthropic token usage
        max_kw_per_product: cap per-product output (3 is sensible; a single
            product rarely needs more alternate search terms than that)

    Returns:
        Flat list of keyword dicts compatible with the downstream pipeline —
        same shape as `generate_keywords()` output. Each dict gains a
        `_source_product_title` field so we can attribute the origin in the
        Keywords sheet / drop analytics.
    """
    config = config or AppConfig()
    if not products:
        return []
    if not ANTHROPIC_API_KEY or ANTHROPIC_API_KEY.startswith("your_") or len(ANTHROPIC_API_KEY) < 20:
        logger.warning("ANTHROPIC_API_KEY not configured — skipping translation")
        return []

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    country_names = {
        "DE": "Germany", "NL": "Netherlands", "AT": "Austria",
        "FR": "France", "BE": "Belgium", "CH": "Switzerland",
        "ES": "Spain", "IT": "Italy", "PL": "Poland",
        "US": "United States", "UK": "United Kingdom", "GB": "United Kingdom",
    }
    language_names = {
        "de": "German", "nl": "Dutch", "fr": "French",
        "es": "Spanish", "it": "Italian", "pl": "Polish", "en": "English",
    }
    country_name = country_names.get(country, country)
    language_name = language_names.get(language, language)

    # Split into batches and process sequentially. We don't parallelise:
    # Anthropic rate-limits by TPM, and sequential calls keep us well under
    # both the RPM and TPM ceilings with room to spare.
    n_products = len(products)
    n_batches = (n_products + TRANSLATE_BATCH_SIZE - 1) // TRANSLATE_BATCH_SIZE
    logger.info(
        "Translation: %d products → %d batches of up to %d",
        n_products, n_batches, TRANSLATE_BATCH_SIZE,
    )

    all_keywords: list[dict] = []
    total_skipped = 0
    failed_batches = 0
    for batch_idx in range(n_batches):
        start = batch_idx * TRANSLATE_BATCH_SIZE
        batch = products[start : start + TRANSLATE_BATCH_SIZE]
        batch_kws, batch_skipped, ok = _translate_batch(
            client=client,
            products=batch,
            country=country,
            country_name=country_name,
            language=language,
            language_name=language_name,
            max_kw_per_product=max_kw_per_product,
            cost_tracker=cost_tracker,
            batch_label=f"batch {batch_idx + 1}/{n_batches}",
        )
        all_keywords.extend(batch_kws)
        total_skipped += batch_skipped
        if not ok:
            failed_batches += 1

    logger.info(
        "Translation complete: %d keywords from %d products (%d skipped, %d/%d batches failed)",
        len(all_keywords), n_products, total_skipped, failed_batches, n_batches,
    )
    return all_keywords


def _translate_batch(
    *,
    client: anthropic.Anthropic,
    products: list[dict],
    country: str,
    country_name: str,
    language: str,
    language_name: str,
    max_kw_per_product: int,
    cost_tracker: Optional[CostTracker],
    batch_label: str,
) -> tuple[list[dict], int, bool]:
    """Translate one batch of products. Returns (keywords, skipped_count, success_flag)."""
    # Strip inline quote characters from free-text fields before sending.
    # Product titles occasionally contain them (e.g. `GFK Pool "Aurora"`),
    # and Claude has a bad habit of emitting them back with broken escaping
    # (producing `"GFK Pool "Aurora\""`), which crashes json.loads on the
    # response. Dropping them pre-send costs nothing — the keyword output
    # is generated from the semantic meaning of the title, not its
    # punctuation — and it eliminates a whole class of parse failures.
    def _clean(s: str) -> str:
        if not s:
            return ""
        return str(s).replace('"', "").replace("“", "").replace("”", "").replace("„", "").strip()

    compact_products = []
    # Build a title → relative_demand_change map so we can re-attach the
    # RISER/FLAT/SINKER flag onto each output keyword after Claude
    # translates them. Without this, the rising_niche strategy is dead
    # code — trend_slope stays 0.0 and the ≥0.3 threshold never fires
    # even in RISERs-only mode (2026-04-22 fix).
    title_to_rdc: dict[str, str] = {}
    for p in products:
        row = {"title": _clean(p.get("title", ""))}
        if p.get("brand"):
            row["brand"] = _clean(p["brand"])
        cat_path = " > ".join(x for x in [
            p.get("category_l1"), p.get("category_l2"), p.get("category_l3")
        ] if x)
        if cat_path:
            row["category"] = cat_path
        lo = float(p.get("price_min_eur") or 0)
        hi = float(p.get("price_max_eur") or 0)
        if lo or hi:
            row["price_eur"] = f"{lo:.0f}-{hi:.0f}" if (lo and hi and lo != hi) else f"{lo or hi:.0f}"
        if p.get("relative_demand_change"):
            row["demand_change"] = p["relative_demand_change"]
            # index by the cleaned title Claude will echo back in source_title
            # (+ a lowercase alias for defensive matching)
            title_to_rdc[row["title"]] = p["relative_demand_change"]
            title_to_rdc[row["title"].lower()] = p["relative_demand_change"]
        compact_products.append(row)

    user_prompt = (
        f"Target market: {country_name} ({country}) / {language_name}.\n\n"
        f"Convert each of these {len(compact_products)} top-selling products "
        f"on Google Shopping into UP TO {max_kw_per_product} head-term "
        f"search keywords a buyer would type. Use all {max_kw_per_product} "
        f"slots when the product has multiple distinct search angles — "
        f"most top-selling physical goods do. Only skip when the entire "
        f"category is regulated, perishable, or counterfeit-saturated "
        f"(see system prompt). A brand name in the title is NOT a skip "
        f"reason — output the generic category instead.\n\n"
        f"Products (JSON):\n{json.dumps(compact_products, ensure_ascii=False)}\n\n"
        f"Return ONLY the JSON object described in the system prompt."
    )

    # max_tokens budget: batch size × keywords/product × ~180 tokens per keyword
    # entry. After the default-to-generating prompt rewrite (2026-04-22) each
    # keyword row carries keyword + english_search_terms (1–3 terms) + category
    # + price_estimate_eur + reasoning; observed output sizes are ~170–190
    # tokens/row. At BATCH_SIZE=8 and kw_per_product=3 that's 24 × 180 ≈ 4.3k
    # output tokens. Floor bumped 2000 → 4500 so even light batches don't clip;
    # cap stays at 8000 because Anthropic's gateway times out past ~60s of
    # generation regardless of max_tokens.
    max_output_tokens = min(8000, max(4500, len(compact_products) * max_kw_per_product * 180))

    last_err: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            message = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=max_output_tokens,
                temperature=0.4,  # Lower than generative ideation — this is
                                  # more of a deterministic translation task.
                system=TRANSLATE_SYSTEM_PROMPT,
                messages=[
                    {"role": "user", "content": user_prompt},
                    {"role": "assistant", "content": "{"},
                ],
            )
            if not message.content:
                logger.warning(
                    "Translation %s: Claude returned empty (stop=%s, attempt %d/3)",
                    batch_label, message.stop_reason, attempt,
                )
                last_err = RuntimeError("empty completion")
                continue

            raw_text = "".join(
                block.text for block in message.content if hasattr(block, "text")
            ).strip()
            json_text = "{" + raw_text
            if json_text.startswith("{```") or "```json" in json_text[:20]:
                json_text = json_text.replace("```json", "").replace("```", "").strip()

            try:
                result = json.loads(json_text)
            except json.JSONDecodeError as je:
                logger.warning(
                    "Translation %s attempt %d: JSON parse failed: %s. head=%r tail=%r",
                    batch_label, attempt, je, json_text[:200], json_text[-200:],
                )
                last_err = je
                continue

            translations = result.get("translations") if isinstance(result, dict) else None
            if not isinstance(translations, list):
                last_err = RuntimeError("no 'translations' array in response")
                continue

            flattened: list[dict] = []
            skipped = 0
            for t in translations:
                if not isinstance(t, dict):
                    continue
                src_title = t.get("source_title", "")
                if t.get("skip_reason"):
                    skipped += 1
                    continue
                kws = t.get("keywords") or []
                for kw in kws[:max_kw_per_product]:
                    if not isinstance(kw, dict):
                        continue
                    if not kw.get("keyword"):
                        continue
                    # Tag origin — helps downstream analytics understand
                    # which Best Seller each keyword came from.
                    kw = dict(kw)
                    kw["_source_product_title"] = src_title
                    # Propagate RISER/FLAT/SINKER from the source Best
                    # Seller product so competition.py can compute
                    # trend_slope and the rising_niche strategy can fire.
                    # Claude echoes source_title per the response schema,
                    # but we normalise case as a defensive belt-and-braces
                    # in case the model lightly reformats the title.
                    cleaned_src = _clean(src_title)
                    rdc = (title_to_rdc.get(cleaned_src)
                           or title_to_rdc.get(cleaned_src.lower()))
                    if rdc:
                        kw["relative_demand_change"] = rdc
                    flattened.append(kw)

            logger.info(
                "Translation %s: %d products → %d keywords (%d skipped) on attempt %d",
                batch_label, len(products), len(flattened), skipped, attempt,
            )

            if cost_tracker is not None:
                usage = getattr(message, "usage", None)
                if usage is not None:
                    cost_tracker.record_anthropic(
                        model=CLAUDE_MODEL,
                        input_tokens=getattr(usage, "input_tokens", 0) or 0,
                        output_tokens=getattr(usage, "output_tokens", 0) or 0,
                        context=f"translate {country}/{language} {batch_label}",
                    )
            return flattened, skipped, True

        except anthropic.APIError as e:
            logger.warning("Translation %s attempt %d: API error: %s", batch_label, attempt, e)
            last_err = e
            continue
        except Exception as e:
            logger.warning(
                "Translation %s attempt %d: %s: %s",
                batch_label, attempt, type(e).__name__, e,
            )
            last_err = e
            continue

    logger.error("Translation %s failed after 3 attempts: %s", batch_label, last_err)
    return [], 0, False


def rank_keywords_pre_serpapi(
    keywords: list[dict],
    country: str,
    language: str,
    top_n: int,
    config: AppConfig = None,
    cost_tracker: Optional[CostTracker] = None,
) -> list[dict]:
    """
    Layer 3 pre-SerpAPI gate — batched Claude QA call.

    Takes the keywords that survived dedup + length + LLM price/quality + volume
    + CPC and asks Claude to rank them holistically. Claude sees the full
    candidate set at once, which is a stronger signal than per-keyword
    individual judgements — it can make RELATIVE calls ("this is better than
    that because…") that single-keyword scoring can't.

    Cost vs. savings: one call (~$0.02) replaces `N - top_n` SerpAPI calls at
    $0.015 each. Break-even at ~2 keywords dropped. At N=50, top_n=15, we
    save $0.015 × 35 - $0.02 ≈ $0.50 per run.

    Fails OPEN: on any error (timeout, JSON parse fail, model refusal), returns
    `keywords` unchanged so the pipeline still runs. We never want a flaky LLM
    to nuke a whole Discover run.

    Args:
        keywords: surviving candidates, each dict includes `keyword`,
            `monthly_search_volume`, `estimated_cpc`, `category`,
            `estimated_price_range`, `competition_signal`,
            `sourcing_difficulty`, `product_type`.
        country: country code (e.g. "DE").
        language: language code (e.g. "de").
        top_n: how many to keep. If >= len(keywords), returns as-is.
        config: optional AppConfig (unused today but threaded for future
            tuning like per-country thresholds).
        cost_tracker: records Anthropic token usage.

    Returns:
        The top-N keywords (same dicts, possibly re-ordered). Empty list
        means "the LLM said none were worth pursuing" — callers should treat
        that as "stop the pipeline" to save downstream cost.
    """
    if not keywords or top_n <= 0 or len(keywords) <= top_n:
        return keywords
    if not ANTHROPIC_API_KEY or ANTHROPIC_API_KEY.startswith("your_") or len(ANTHROPIC_API_KEY) < 20:
        logger.warning("ANTHROPIC_API_KEY not configured — skipping Layer 3 QA")
        return keywords

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Compact per-candidate line to minimise token cost. Keep the JSON
    # structure so the model can ground its answers on the exact keyword
    # strings — free-form lists confuse 50-item rankings.
    compact = []
    for i, kw in enumerate(keywords):
        compact.append({
            "i": i,
            "kw": kw.get("keyword", ""),
            "cat": kw.get("category", ""),
            "vol_mo": int(kw.get("monthly_search_volume", 0) or 0),
            "cpc_eur": round(float(kw.get("estimated_cpc", 0) or 0), 2),
            "price_eur": kw.get("estimated_price_range", ""),
            "competition": kw.get("competition_signal", ""),
            "sourcing": kw.get("sourcing_difficulty", ""),
            "type": kw.get("product_type", ""),
        })

    system = (
        "You are a dropshipping product evaluator. You rank candidate keywords "
        "by REAL-WORLD profitability likelihood for a one-person dropshipping "
        "operation using Google Shopping / Performance Max. You see the whole "
        "candidate set at once, so make RELATIVE judgments — which keywords "
        "are strictly better than others.\n\n"
        "Score each keyword 0–100 on combined profitability. Weight:\n"
        "- Search volume (traffic ceiling) but don't over-index on it — "
        "1,500 searches at €0.30 CPC beats 5,000 at €1.80 CPC for a beginner.\n"
        "- CPC (lower is better for break-even).\n"
        "- Competition signal (low > medium > high).\n"
        "- Sourcing difficulty (easy > medium > hard).\n"
        "- Product type (dropshippable only — penalise anything else).\n"
        "- Price range fit — products in €25–€200 band are easier to test.\n"
        "- Market angle: is there a clear differentiator or USP story for "
        "creative? Bland commodities are harder to scale even if the maths work.\n\n"
        "Be strict. If you can only find 3 good ones out of 30, return 3. "
        "Padding the list with mediocre candidates wastes the operator's "
        "budget on losing tests."
    )

    user_prompt = (
        f"Target market: {country} / {language}.\n\n"
        f"Rank these {len(keywords)} candidate keywords by profitability "
        f"likelihood. Return the TOP {top_n} (or fewer if you think "
        f"fewer are worth pursuing).\n\n"
        f"Candidates (JSON):\n{json.dumps(compact, ensure_ascii=False)}\n\n"
        "Return ONLY a JSON object with this shape:\n"
        '{"ranked": [{"i": <candidate index>, "score": <0-100>, '
        '"reason": "<one short phrase>"}, ...]}\n'
        "Order the array best-first. Include at most "
        f"{top_n} entries. Do not invent new keywords."
    )

    try:
        message = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=6000,
            temperature=0.2,  # lower temp — ranking is not a creative task
            system=system,
            messages=[
                {"role": "user", "content": user_prompt},
                {"role": "assistant", "content": "{"},
            ],
        )
        if cost_tracker is not None:
            usage = getattr(message, "usage", None)
            if usage is not None:
                cost_tracker.record_anthropic(
                    model=CLAUDE_MODEL,
                    input_tokens=getattr(usage, "input_tokens", 0) or 0,
                    output_tokens=getattr(usage, "output_tokens", 0) or 0,
                    context=f"pre-serpapi QA {country}/{language} "
                            f"({len(keywords)}→{top_n})",
                )

        raw_text = "".join(
            block.text for block in (message.content or []) if hasattr(block, "text")
        ).strip()
        json_text = "{" + raw_text
        if json_text.startswith("{```") or "```json" in json_text[:20]:
            json_text = json_text.replace("```json", "").replace("```", "").strip()
        parsed = json.loads(json_text)
        ranked = parsed.get("ranked") if isinstance(parsed, dict) else None
        if not isinstance(ranked, list) or not ranked:
            logger.warning("Layer 3 QA returned no ranked entries; passing through")
            return keywords

        # Map back to the original keyword dicts. The QA call can hallucinate
        # an index or repeat one — defend against that.
        seen: set[int] = set()
        picked: list[dict] = []
        for entry in ranked[:top_n]:
            if not isinstance(entry, dict):
                continue
            idx = entry.get("i")
            if not isinstance(idx, int) or idx in seen:
                continue
            if idx < 0 or idx >= len(keywords):
                continue
            seen.add(idx)
            picked.append(keywords[idx])

        if not picked:
            logger.warning(
                "Layer 3 QA parsed but no valid indices; passing all %d through",
                len(keywords),
            )
            return keywords
        logger.info(
            "Layer 3 QA kept %d/%d candidates (dropped %d before SerpAPI)",
            len(picked), len(keywords), len(keywords) - len(picked),
        )
        return picked

    except Exception as e:
        logger.warning(
            "Layer 3 QA failed (%s: %s) — passing all %d through to SerpAPI",
            type(e).__name__, e, len(keywords),
        )
        return keywords


def generate_keywords_batch(
    countries: list[dict],
    num_per_country: int = 150,
    category_focus: list[str] = None,
    feedback: Optional[ResearchFeedback] = None,
    config: AppConfig = None,
    cost_tracker: Optional[CostTracker] = None,
    avoid_keywords_by_country: Optional[dict[str, list[str]]] = None,
) -> dict[str, list[dict]]:
    """
    Generate keywords for multiple countries.
    Returns {country_code: [keywords]}.

    `avoid_keywords_by_country` lets the caller feed a per-country blacklist
    (active Keywords sheet + historical drops) so the LLM doesn't spend its
    output budget regenerating the same ideas every run. See the docstring
    on `generate_keywords` for why this matters.
    """
    results = {}
    for country_info in countries:
        code = country_info.get("code", "DE")
        lang = country_info.get("language", "de")
        avoid = None
        if avoid_keywords_by_country:
            avoid = avoid_keywords_by_country.get(code)
        results[code] = generate_keywords(
            country=code,
            language=lang,
            num_keywords=num_per_country,
            category_focus=category_focus,
            feedback=feedback,
            config=config,
            cost_tracker=cost_tracker,
            avoid_keywords=avoid,
        )
    return results
