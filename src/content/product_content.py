"""
LLM-powered product content generation.
Creates SEO-optimized titles, descriptions, and meta content for Shopify listings.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from openai import OpenAI

from src.core.config import AppConfig, OPENAI_API_KEY
from src.core.models import Product

logger = logging.getLogger(__name__)

LANGUAGE_NAMES = {
    "de": "German", "nl": "Dutch", "fr": "French",
    "es": "Spanish", "it": "Italian", "pl": "Polish", "en": "English",
}

SYSTEM_PROMPT = """You are an expert e-commerce copywriter specializing in creating compelling, SEO-optimized product listings for an online store.

Your copy should:
1. Be written entirely in the specified TARGET LANGUAGE
2. Be compelling and benefit-focused (not just feature lists)
3. Include the main keyword naturally for SEO
4. Feel professional and trustworthy
5. Use clear, scannable formatting with bullet points
6. Avoid exaggerated claims or fake urgency
7. NOT mention brand names (we sell unbranded/white-label products)

You write for a dropshipping store that sells quality products at competitive prices."""


def generate_product_content(
    product: Product,
    reference_description: str = "",
    config: AppConfig = None,
) -> Optional[dict]:
    """
    Generate all product content for a Shopify listing.

    Args:
        product: The product data
        reference_description: Optional description from AliExpress for reference
        config: App configuration

    Returns:
        dict with: title, description_html, meta_title, meta_description,
                   tags, product_type
    """
    config = config or AppConfig()
    client = OpenAI(api_key=OPENAI_API_KEY)

    language = product.language or "de"
    lang_name = LANGUAGE_NAMES.get(language, "German")

    user_prompt = f"""Create a complete product listing for an online store.

Product keyword: {product.keyword}
Target language: {lang_name}
Selling price: EUR {product.selling_price}
Target country: {product.country}

{"Reference description from supplier: " + reference_description[:500] if reference_description else ""}

Generate the following (ALL in {lang_name}):

1. **title**: A compelling, SEO-optimized product title (max 70 characters). Include the main keyword. 
   Do NOT include the price. Do NOT include brand names.

2. **description_html**: Product description in HTML format for a Shopify product page. Include:
   - Opening paragraph (2-3 sentences about the product's main benefit)
   - Key features as a bulleted list (5-7 features)
   - A "Why choose this product?" section (2-3 sentences)
   - Product specifications if applicable (dimensions, material, etc.)
   Use <h2>, <p>, <ul>, <li>, <strong> tags appropriately.
   Keep it concise but informative (200-350 words total).

3. **meta_title**: SEO meta title for the page (max 60 characters)

4. **meta_description**: SEO meta description (max 155 characters)

5. **tags**: Comma-separated product tags for Shopify (5-8 tags, in {lang_name})

6. **product_type**: Single category name for Shopify (in {lang_name})

Return as a JSON object with these exact keys. All values must be strings.
Return ONLY valid JSON, no other text."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.7,
            max_tokens=2000,
            response_format={"type": "json_object"},
        )

        content = json.loads(response.choices[0].message.content)

        # Validate required fields
        required_fields = ["title", "description_html", "meta_title", "meta_description", "tags", "product_type"]
        for field in required_fields:
            if field not in content:
                content[field] = ""

        logger.info(
            "Generated content for '%s': title='%s'",
            product.keyword, content.get("title", "")[:50]
        )

        return content

    except json.JSONDecodeError as e:
        logger.error("Failed to parse content generation response: %s", e)
        return None
    except Exception as e:
        logger.error("Product content generation failed: %s", e)
        return None


def generate_basic_content(
    keyword: str,
    selling_price: float,
    language: str = "de",
    country: str = "DE",
) -> dict:
    """
    Generate minimal product content without LLM (fallback).
    Used when OpenAI API is unavailable.
    """
    lang_titles = {
        "de": f"{keyword.title()} - Jetzt Online Kaufen",
        "nl": f"{keyword.title()} - Nu Online Kopen",
        "fr": f"{keyword.title()} - Acheter en Ligne",
        "en": f"{keyword.title()} - Buy Online",
    }

    lang_desc = {
        "de": f"<p>Entdecken Sie unseren hochwertigen {keyword}. Schneller Versand, einfache Rückgabe.</p>",
        "nl": f"<p>Ontdek onze hoogwaardige {keyword}. Snelle verzending, eenvoudig retourneren.</p>",
        "fr": f"<p>Découvrez notre {keyword} de haute qualité. Livraison rapide, retour facile.</p>",
        "en": f"<p>Discover our high-quality {keyword}. Fast shipping, easy returns.</p>",
    }

    return {
        "title": lang_titles.get(language, keyword.title()),
        "description_html": lang_desc.get(language, f"<p>{keyword}</p>"),
        "meta_title": keyword.title()[:60],
        "meta_description": f"{keyword.title()} - EUR {selling_price:.2f}"[:155],
        "tags": keyword,
        "product_type": keyword.split()[0].title() if keyword else "Product",
    }
