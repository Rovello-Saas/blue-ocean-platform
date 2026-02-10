"""
LLM-powered product content generation (v3 — Refined PDP Template).

All content is generated in **English** — the store's translation app
(e.g. Weglot, Langify, Shopify Translate & Adapt) handles localisation
into German, Dutch, French, etc.

The LLM produces structured JSON (features, benefits, specs, FAQ,
whats_included) and a fixed HTML template renders it into a
high-converting product page with:

- Payment method icons bar
- Trust badges bar
- Key features (single column, max 5)
- Editorial benefit blocks with emoji headers
- Collapsible dropdowns: What's Included, Specifications, FAQ
- Bottom trust icons (3 items, evenly spaced)

Shipping & Returns and Payment & Security are handled by the Shopify
theme natively and are NOT duplicated in the product description.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from openai import OpenAI

from src.core.config import AppConfig, OPENAI_API_KEY
from src.core.models import Product

logger = logging.getLogger(__name__)

# ── LLM system prompt ────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are an expert e-commerce copywriter who creates compelling, \
SEO-optimised product listings in ENGLISH.

Rules:
1. Always write in English — a translation app handles other languages.
2. Be benefit-focused, not just feature lists.
3. Include the main keyword naturally for SEO.
4. Sound professional and trustworthy — no hype or fake urgency.
5. NEVER mention brand names (we sell unbranded / white-label products).
6. Use clear, concise language suitable for a European audience.
"""

# ── Emoji map for benefit sections ───────────────────────────────

_BENEFIT_EMOJIS = ["💡", "🌡️", "🧵", "🎛️", "🔒", "⚡", "🏠", "✨"]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PUBLIC API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def generate_product_content(
    product: Product,
    reference_description: str = "",
    config: AppConfig = None,
) -> Optional[dict]:
    """
    Generate all product content for a Shopify listing.

    The LLM returns structured JSON which is rendered into a rich
    HTML template.  All content is in English.

    Returns dict with:
        title, description_html, meta_title, meta_description,
        tags, product_type, handle
    """
    config = config or AppConfig()
    client = OpenAI(api_key=OPENAI_API_KEY)

    user_prompt = _build_llm_prompt(product, reference_description)

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.7,
            max_tokens=3000,
            response_format={"type": "json_object"},
        )

        structured = json.loads(response.choices[0].message.content)
        logger.info(
            "LLM returned structured content for '%s'",
            product.keyword,
        )

        # Render structured JSON → rich HTML
        description_html = _render_pdp_html(structured)

        # Build key_features HTML for metafield (displayed above CTA by theme)
        features = structured.get("key_features", [])[:5]
        features_html = ""
        if features:
            items = "".join(
                f'<li style="font-size:14px;line-height:1.8;color:#444;">'
                f'✔ {f}</li>'
                for f in features
            )
            features_html = (
                f'<ul style="list-style:none;padding:0;margin:0 0 8px;">'
                f'{items}</ul>'
            )

        result = {
            "title": structured.get("title", product.keyword.title()),
            "description_html": description_html,
            "meta_title": structured.get("meta_title", "")[:60],
            "meta_description": structured.get("meta_description", "")[:155],
            "tags": structured.get("tags", product.keyword),
            "product_type": structured.get("product_type", "Product"),
            "handle": _slugify(structured.get("title", product.keyword)),
            "key_features_html": features_html,
        }

        logger.info(
            "Generated PDP content for '%s': title='%s'",
            product.keyword, result["title"][:50],
        )
        return result

    except json.JSONDecodeError as e:
        logger.error("Failed to parse content generation response: %s", e)
        return None
    except Exception as e:
        logger.error("Product content generation failed: %s", e)
        return None


def generate_basic_content(
    keyword: str,
    selling_price: float,
    language: str = "en",
    country: str = "DE",
) -> dict:
    """
    Generate minimal product content without LLM (fallback).
    Used when OpenAI API is unavailable.  Always English.
    """
    return {
        "title": f"{keyword.title()} — Buy Online",
        "description_html": (
            f"<p>Discover our high-quality {keyword}. "
            f"Fast shipping, easy returns.</p>"
        ),
        "meta_title": keyword.title()[:60],
        "meta_description": (
            f"{keyword.title()} — EUR {selling_price:.2f}"
        )[:155],
        "tags": keyword,
        "product_type": keyword.split()[0].title() if keyword else "Product",
        "handle": _slugify(keyword),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LLM PROMPT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _build_llm_prompt(product: Product, reference_description: str) -> str:
    ref_block = ""
    if reference_description:
        ref_block = (
            f"\nReference description from supplier/competitor "
            f"(for inspiration only):\n"
            f"{reference_description[:800]}\n"
        )

    return f"""\
Create a complete product listing for an online store.

Product keyword: {product.keyword}
Selling price: EUR {product.selling_price}
Target country: {product.country}
{ref_block}
Return a JSON object with these exact keys:

{{
  "title": "Compelling SEO title, max 80 chars. Include keyword. No brand names. No price.",

  "key_features": [
    "Short bullet string (one line each, max 8 words)",
    "... exactly 5 items — the product's strongest selling points"
  ],

  "whats_included": [
    "1x Main product",
    "1x Controller / Remote",
    "... list every item that comes in the box, 3-6 items"
  ],

  "benefits": [
    {{
      "heading": "Short benefit heading (3-6 words)",
      "text": "Benefit paragraph, 2-3 sentences. Engaging, benefit-focused copy."
    }},
    "... exactly 4-5 benefit blocks"
  ],

  "specs": [
    {{"label": "Dimensions", "value": "180 × 130 cm"}},
    "... 6-10 specification rows (dimensions, weight, material, voltage, power, etc.)"
  ],

  "faq": [
    {{"q": "Question?", "a": "Answer, 1-3 sentences."}},
    "... 4-6 relevant FAQs a customer would ask"
  ],

  "meta_title": "SEO page title, max 60 chars",
  "meta_description": "SEO meta description, max 155 chars. Compelling, includes keyword.",
  "tags": "comma, separated, product, tags, 6-10 items",
  "product_type": "Single product category name"
}}

Important:
- ALL text in English.
- Do NOT include brand names anywhere.
- key_features: exactly 5 items, start with the strongest selling point (e.g. cost savings, # of settings).
- whats_included: list every physical item in the box (product, controller, manual, cable, bag, etc.).
- Benefits should feel editorial and engaging, not generic.
- FAQ should answer real customer concerns (safety, washing, electricity cost, etc.).
- Specs should be factual — if you don't know exact values, make reasonable estimates based on the keyword and reference.

Return ONLY valid JSON."""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HTML TEMPLATE RENDERER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _render_pdp_html(data: dict) -> str:
    """
    Render structured product data into high-converting HTML.

    Layout order:
    0. CSS overrides (hide theme accordion to avoid duplication)
    1. Key feature bullets (plain <ul>)
    2. Benefit text blocks (editorial sections with emoji headers)
    3. Dropdowns: What's Included, Specifications, Shipping & Returns, FAQ

    The theme's native USP icons (Free Shipping, Secure Payments, Easy Returns)
    are left untouched — we do NOT hide or duplicate them.
    """
    parts: list[str] = []

    # ── CSS overrides for theme elements ──────────────────────
    parts.append(_theme_css_overrides())

    # ── Key feature bullets (shown under the price) ──────────
    features = data.get("key_features", [])[:5]
    if features:
        items = "\n".join(f"<li>{f}</li>" for f in features)
        parts.append(f"<ul>\n{items}\n</ul>")

    # ── Benefit blocks ────────────────────────────────────────
    benefits = data.get("benefits", [])
    for i, benefit in enumerate(benefits):
        emoji = _BENEFIT_EMOJIS[i % len(_BENEFIT_EMOJIS)]
        heading = benefit.get("heading", "")
        text = benefit.get("text", "")
        parts.append(
            f'<div style="margin-bottom:28px;">\n'
            f'  <h3 style="font-size:18px;color:#222;margin:0 0 8px;">'
            f'{emoji} {heading}</h3>\n'
            f'  <p style="font-size:15px;line-height:1.7;color:#555;'
            f'margin:0;">{text}</p>\n'
            f'</div>'
        )

    # ── Dropdowns (all closed by default) ─────────────────────

    # What's Included
    whats_included = data.get("whats_included", [])
    if whats_included:
        included_items = "\n".join(
            f'<li style="font-size:14px;line-height:1.8;color:#444;">'
            f'{item}</li>'
            for item in whats_included
        )
        included_html = (
            f'<ul style="list-style:disc;padding-left:20px;margin:0;">\n'
            f'{included_items}\n'
            f'</ul>'
        )
        parts.append(
            '<div style="margin-bottom:12px;">\n'
            + _collapsible("What's Included", included_html)
            + '\n</div>'
        )

    # Specifications
    specs = data.get("specs", [])
    if specs:
        rows = []
        for idx, spec in enumerate(specs):
            bg = "background:#fafafa;" if idx % 2 == 1 else ""
            border = "" if idx == len(specs) - 1 else "border-bottom:1px solid #f0f0f0;"
            rows.append(
                f'        <tr style="{border}{bg}">\n'
                f'          <td style="padding:10px 20px;color:#666;width:40%;">'
                f'{spec.get("label", "")}</td>\n'
                f'          <td style="padding:10px 20px;color:#222;font-weight:500;">'
                f'{spec.get("value", "")}</td>\n'
                f'        </tr>'
            )
        parts.append(
            '<div style="margin-bottom:12px;">\n'
            + _collapsible("Specifications", "\n".join(rows), table=True)
            + '\n</div>'
        )

    # Shipping & Returns — exact copy of theme text with policy links
    shipping_html = (
        '<p style="margin:0 0 16px;"><strong>Shipping</strong>: We offer free shipping on '
        'all our products. Orders are processed within 1\u20132 business days and '
        'typically arrive within 5\u20138 business days. For full details, visit our '
        '<a href="/policies/shipping-policy" target="_blank">Shipping Policy</a>.</p>\n'
        '<p style="margin:0;"><strong>Returns</strong>: You may return items within '
        '30 days of delivery. Products must be returned with all original packaging '
        'and accessories included. For full details, check our '
        '<a href="/policies/refund-policy" target="_blank">Refund &amp; Return Policy</a>.</p>'
    )
    parts.append(
        '<div style="margin-bottom:12px;">\n'
        + _collapsible("Shipping & Returns", shipping_html)
        + '\n</div>'
    )

    # FAQ
    faq = data.get("faq", [])
    if faq:
        faq_items = []
        for i, item in enumerate(faq):
            mb = "0" if i == len(faq) - 1 else "16px"
            faq_items.append(
                f'<p style="margin:0 0 4px;"><strong>{item.get("q", "")}'
                f'</strong></p>\n'
                f'<p style="margin:0 0 {mb};">{item.get("a", "")}</p>'
            )
        faq_html = "\n\n".join(faq_items)
        parts.append(
            '<div style="margin-bottom:12px;">\n'
            + _collapsible("Frequently Asked Questions", faq_html)
            + '\n</div>'
        )

    return "\n\n".join(parts)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _theme_css_overrides() -> str:
    """
    Minimal CSS for product description layout only.

    No button colors, fonts, or branding — those are controlled by
    the Shopify theme editor (Theme Settings → Colors, Typography).
    This keeps every product consistent with the store's global brand.

    Only hides the theme's native accordion to avoid duplication with
    our own collapsible sections.
    """
    return (
        '<style>\n'
        '/* Hide theme accordion to avoid duplicate Shipping & Returns */\n'
        '.product__accordion,\n'
        '.product__accordion.accordion {\n'
        '  display: none !important;\n'
        '}\n'
        '</style>'
    )


def _collapsible(
    title: str,
    inner_html: str,
    table: bool = False,
    open_default: bool = False,
) -> str:
    """Render a collapsible <details> section."""
    open_attr = " open" if open_default else ""
    border_bottom = "border-bottom:1px solid #e5e5e5;" if not table else ""

    if table:
        body = (
            f'    <div style="padding:0;">\n'
            f'      <table style="width:100%;border-collapse:collapse;'
            f'font-size:14px;">\n{inner_html}\n'
            f'      </table>\n'
            f'    </div>'
        )
    else:
        body = (
            f'    <div style="padding:16px 20px;font-size:14px;'
            f'line-height:1.7;color:#555;">\n'
            f'      {inner_html}\n'
            f'    </div>'
        )

    return (
        f'  <details{open_attr} style="border:1px solid #e5e5e5;'
        f'border-radius:8px;overflow:hidden;">\n'
        f'    <summary style="font-size:16px;font-weight:700;color:#222;'
        f'padding:14px 20px;cursor:pointer;background:#f9f9f9;'
        f'{border_bottom}list-style:none;display:flex;'
        f'justify-content:space-between;align-items:center;">\n'
        f'      {title}\n'
        f'      <span style="font-size:12px;color:#888;">▼</span>\n'
        f'    </summary>\n'
        f'{body}\n'
        f'  </details>'
    )


def _slugify(text: str) -> str:
    """Create a URL-safe handle from text."""
    import re
    slug = text.lower().strip()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'[\s_]+', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    return slug.strip('-')[:80]
