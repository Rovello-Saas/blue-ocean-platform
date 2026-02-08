"""
LLM-based keyword ideation using OpenAI.
Generates product-intent keywords for target countries with feedback loop.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from openai import OpenAI

from src.core.config import AppConfig, OPENAI_API_KEY
from src.core.models import ResearchFeedback

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """You are an expert e-commerce product researcher specializing in finding profitable dropshipping products for Google Shopping / Performance Max campaigns.

Your task is to generate a list of specific, commercially-viable product keywords that people search for when they want to BUY a product. 

Rules:
1. Focus on PRODUCT-INTENT keywords only (people searching to buy, not to learn)
2. Be SPECIFIC — "wireless bluetooth earbuds with noise cancelling" not just "earbuds"
3. Include product variations, colors, sizes when they indicate different purchase intent
4. Think about products with good margins (typically EUR 20-200 selling price for dropshipping)
5. Avoid generic categories — each keyword should map to a specific purchasable product
6. Avoid branded keywords (no "Apple", "Samsung", etc.) — we want generic product searches
7. Consider seasonal relevance
8. Output keywords in the TARGET LANGUAGE for the target country

Output format: Return a JSON array of objects with:
- "keyword": the search keyword in the target language
- "category": product category (in English)
- "estimated_price_range": approximate selling price range in EUR (e.g., "20-40")
- "reasoning": brief note on why this could be profitable (in English)
"""


def generate_keywords(
    country: str = "DE",
    language: str = "de",
    num_keywords: int = 150,
    category_focus: list[str] = None,
    feedback: Optional[ResearchFeedback] = None,
    config: AppConfig = None,
) -> list[dict]:
    """
    Generate product-intent keywords using OpenAI.

    Args:
        country: Target country code (e.g., "DE")
        language: Target language (e.g., "de")
        num_keywords: Number of keywords to generate
        category_focus: Optional list of categories to focus on
        feedback: Historical feedback from past winners/losers
        config: App configuration

    Returns:
        List of keyword dicts with keyword, category, estimated_price_range, reasoning
    """
    config = config or AppConfig()

    if not OPENAI_API_KEY or OPENAI_API_KEY.startswith("your_") or len(OPENAI_API_KEY) < 20:
        logger.warning("OPENAI_API_KEY not configured — skipping LLM ideation")
        return []

    client = OpenAI(api_key=OPENAI_API_KEY)

    # Build the user prompt
    country_names = {
        "DE": "Germany", "NL": "Netherlands", "AT": "Austria",
        "FR": "France", "BE": "Belgium", "CH": "Switzerland",
        "ES": "Spain", "IT": "Italy", "PL": "Poland",
    }
    country_name = country_names.get(country, country)
    language_names = {
        "de": "German", "nl": "Dutch", "fr": "French",
        "es": "Spanish", "it": "Italian", "pl": "Polish", "en": "English",
    }
    language_name = language_names.get(language, language)

    user_prompt = f"""Generate {num_keywords} product-intent keywords for the {country_name} market.

Target language: {language_name}
Target country: {country_name}
"""

    if category_focus:
        user_prompt += f"\nFocus on these categories: {', '.join(category_focus)}\n"

    if feedback:
        feedback_text = feedback.to_summary()
        if feedback_text != "No historical data available yet.":
            user_prompt += f"""
IMPORTANT - Historical performance data from our previous products:
{feedback_text}

Use this data to generate BETTER keywords that are more likely to be profitable.
Focus on categories and price ranges that have worked well.
AVOID categories that have consistently failed.
"""

    user_prompt += f"""
Remember:
- All keywords must be in {language_name}
- Focus on products in the EUR 20-200 price range
- Think about what someone would type into Google when they want to BUY a specific product
- Be creative and explore diverse product categories
- Each keyword should represent a distinct product opportunity

Return ONLY a valid JSON array. No other text."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.8,  # Higher temperature for more creative/diverse ideas
            max_tokens=16000,
            response_format={"type": "json_object"},
        )

        content = response.choices[0].message.content
        result = json.loads(content)

        # Handle both {"keywords": [...]} and direct array format
        if isinstance(result, dict):
            keywords = result.get("keywords", result.get("results", []))
        elif isinstance(result, list):
            keywords = result
        else:
            keywords = []

        logger.info(
            "Generated %d keywords for %s (%s)",
            len(keywords), country, language
        )
        return keywords

    except json.JSONDecodeError as e:
        logger.error("Failed to parse LLM response as JSON: %s", e)
        return []
    except Exception as e:
        logger.error("LLM keyword generation failed: %s", e)
        return []


def generate_keywords_batch(
    countries: list[dict],
    num_per_country: int = 150,
    category_focus: list[str] = None,
    feedback: Optional[ResearchFeedback] = None,
    config: AppConfig = None,
) -> dict[str, list[dict]]:
    """
    Generate keywords for multiple countries.
    Returns {country_code: [keywords]}.
    """
    results = {}
    for country_info in countries:
        code = country_info.get("code", "DE")
        lang = country_info.get("language", "de")
        results[code] = generate_keywords(
            country=code,
            language=lang,
            num_keywords=num_per_country,
            category_focus=category_focus,
            feedback=feedback,
            config=config,
        )
    return results
