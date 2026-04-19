"""
Content Studio service — manage product text content for Shopify PDPs.

Stores the structured JSON (title, features, benefits, specs, FAQ, etc.)
per product. Users can edit fields, regenerate sections via AI, change
emojis, preview the final HTML, and push updates to Shopify.
"""

from __future__ import annotations

import json
import logging
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

from openai import OpenAI

from src.core.config import AppConfig, OPENAI_API_KEY
from src.core.models import Product
from src.content.product_content import (
    _render_pdp_html,
    _build_llm_prompt,
    SYSTEM_PROMPT,
    _BENEFIT_EMOJIS,
    _slugify,
)

logger = logging.getLogger(__name__)

# Local cache directory (same pattern as Image Studio)
CACHE_DIR = Path(tempfile.gettempdir()) / "blue_ocean_content_studio"
CACHE_DIR.mkdir(exist_ok=True)

# Available emoji sets for benefit headers
EMOJI_SETS = {
    "default": ["💡", "🌡️", "🧵", "🎛️", "🔒", "⚡", "🏠", "✨"],
    "minimal": ["•", "•", "•", "•", "•", "•", "•", "•"],
    "checkmarks": ["✅", "✅", "✅", "✅", "✅", "✅", "✅", "✅"],
    "stars": ["⭐", "⭐", "⭐", "⭐", "⭐", "⭐", "⭐", "⭐"],
    "arrows": ["→", "→", "→", "→", "→", "→", "→", "→"],
    "numbers": ["①", "②", "③", "④", "⑤", "⑥", "⑦", "⑧"],
    "mixed": ["🔥", "💎", "🛡️", "⚙️", "🌿", "🚀", "💫", "🎯"],
    "none": ["", "", "", "", "", "", "", ""],
}


class ContentStudioService:
    """Manages product text content with editing, AI regeneration, and Shopify push."""

    def __init__(self):
        self._content: dict[str, dict] = {}
        self._load_cache()

    # ── Generate full content for a product ────────────────────

    def generate_content(
        self,
        product: Product,
        reference_description: str = "",
        custom_prompt: str = "",
    ) -> Optional[dict]:
        """
        Generate all product content via GPT-4o.
        Stores the structured JSON for later editing.
        """
        client = OpenAI(api_key=OPENAI_API_KEY)

        if custom_prompt:
            user_prompt = custom_prompt
        else:
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
            logger.info("Generated content for '%s'", product.keyword)

            # Store with metadata
            content_data = {
                "product_id": product.product_id,
                "product_keyword": product.keyword,
                "structured": structured,
                "emoji_set": "default",
                "text_approved": False,
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
            }

            self._content[product.product_id] = content_data
            self._save_cache()
            return content_data

        except Exception as e:
            logger.error("Content generation failed: %s", e)
            return None

    # ── Regenerate a single section via AI ─────────────────────

    def regenerate_section(
        self,
        product_id: str,
        section: str,
        instruction: str = "",
    ) -> Optional[dict]:
        """
        Regenerate a specific section (title, key_features, benefits,
        specs, faq, whats_included, meta_title, meta_description).

        Args:
            product_id: Product to update
            section: JSON key to regenerate
            instruction: Optional custom instruction for the AI
        """
        content = self._content.get(product_id)
        if not content:
            return None

        structured = content["structured"]
        keyword = content.get("product_keyword", "")
        client = OpenAI(api_key=OPENAI_API_KEY)

        # Build a focused prompt for just this section
        current_value = json.dumps(structured.get(section, ""), indent=2)
        section_prompts = {
            "title": "a compelling SEO product title (max 80 chars, include keyword, no brand names)",
            "key_features": "exactly 5 short bullet points (max 8 words each) — the product's strongest selling points",
            "benefits": "4-5 editorial benefit blocks, each with a 'heading' (3-6 words) and 'text' (2-3 engaging sentences)",
            "specs": "6-10 specification rows with 'label' and 'value' (dimensions, weight, material, etc.)",
            "faq": "4-6 FAQs with 'q' and 'a' — real customer concerns",
            "whats_included": "3-6 items that come in the box",
            "meta_title": "an SEO page title (max 60 chars)",
            "meta_description": "an SEO meta description (max 155 chars, compelling, includes keyword)",
        }

        section_desc = section_prompts.get(section, f"the '{section}' section")

        prompt = (
            f"Product: {keyword}\n\n"
            f"Current {section}:\n{current_value}\n\n"
        )
        if instruction:
            prompt += f"User instruction: {instruction}\n\n"
        prompt += (
            f"Generate an improved version of {section_desc}.\n"
            f"Return ONLY valid JSON with the key '{section}' and its new value.\n"
            f"All text in English. No brand names."
        )

        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.7,
                max_tokens=1500,
                response_format={"type": "json_object"},
            )

            result = json.loads(response.choices[0].message.content)
            new_value = result.get(section)

            if new_value is not None:
                structured[section] = new_value
                content["updated_at"] = datetime.utcnow().isoformat()
                self._save_cache()
                logger.info("Regenerated '%s' for product %s", section, product_id)
                return content

        except Exception as e:
            logger.error("Section regeneration failed: %s", e)

        return None

    # ── Manual edits ───────────────────────────────────────────

    def update_field(self, product_id: str, field: str, value) -> bool:
        """Update a single field in the structured content.
        Automatically revokes text approval when content is edited."""
        content = self._content.get(product_id)
        if not content:
            return False

        content["structured"][field] = value
        content["text_approved"] = False  # edits revoke approval
        content["updated_at"] = datetime.utcnow().isoformat()
        self._save_cache()
        return True

    def set_emoji_set(self, product_id: str, emoji_set: str) -> bool:
        """Change the emoji set used for benefit headers."""
        content = self._content.get(product_id)
        if not content or emoji_set not in EMOJI_SETS:
            return False

        content["emoji_set"] = emoji_set
        content["updated_at"] = datetime.utcnow().isoformat()
        self._save_cache()
        return True

    def set_benefit_emoji(self, product_id: str, benefit_index: int, emoji: str) -> bool:
        """Set a custom emoji for a specific benefit."""
        content = self._content.get(product_id)
        if not content:
            return False

        # Store custom emojis in the content
        if "custom_emojis" not in content:
            content["custom_emojis"] = {}
        content["custom_emojis"][str(benefit_index)] = emoji
        content["updated_at"] = datetime.utcnow().isoformat()
        self._save_cache()
        return True

    # ── Approval ────────────────────────────────────────────────

    def approve_text(self, product_id: str) -> bool:
        """Mark the text content as approved and ready for publishing."""
        content = self._content.get(product_id)
        if not content:
            return False
        content["text_approved"] = True
        content["updated_at"] = datetime.utcnow().isoformat()
        self._save_cache()
        logger.info("Text approved for product %s", product_id)
        return True

    def unapprove_text(self, product_id: str) -> bool:
        """Revoke text approval (e.g. after edits)."""
        content = self._content.get(product_id)
        if not content:
            return False
        content["text_approved"] = False
        content["updated_at"] = datetime.utcnow().isoformat()
        self._save_cache()
        return True

    def is_text_approved(self, product_id: str) -> bool:
        """Check if text content is approved."""
        content = self._content.get(product_id)
        return bool(content and content.get("text_approved", False))

    # ── Rendering ──────────────────────────────────────────────

    def render_html(self, product_id: str) -> str:
        """Render the structured content into final PDP HTML."""
        content = self._content.get(product_id)
        if not content:
            return ""

        structured = content["structured"]
        emoji_set_name = content.get("emoji_set", "default")
        custom_emojis = content.get("custom_emojis", {})
        emojis = list(EMOJI_SETS.get(emoji_set_name, EMOJI_SETS["default"]))

        # Apply custom emoji overrides
        for idx_str, emoji in custom_emojis.items():
            idx = int(idx_str)
            if 0 <= idx < len(emojis):
                emojis[idx] = emoji

        # Render with the chosen emoji set
        return _render_pdp_html_with_emojis(structured, emojis)

    def get_shopify_payload(self, product_id: str) -> Optional[dict]:
        """Get the full payload ready to push to Shopify."""
        content = self._content.get(product_id)
        if not content:
            return None

        structured = content["structured"]
        description_html = self.render_html(product_id)

        # Build key_features HTML for metafield
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

        return {
            "title": structured.get("title", ""),
            "description_html": description_html,
            "meta_title": structured.get("meta_title", "")[:60],
            "meta_description": structured.get("meta_description", "")[:155],
            "tags": structured.get("tags", ""),
            "product_type": structured.get("product_type", "Product"),
            "handle": _slugify(structured.get("title", "")),
            "key_features_html": features_html,
        }

    # ── Retrieval ──────────────────────────────────────────────

    def get_content(self, product_id: str) -> Optional[dict]:
        """Get stored content for a product."""
        self._load_cache()
        return self._content.get(product_id)

    def get_all_content(self) -> dict:
        """Get all stored content."""
        self._load_cache()
        return dict(self._content)

    def has_content(self, product_id: str) -> bool:
        """Check if content exists for a product."""
        return product_id in self._content

    def are_images_approved(self, product_id: str) -> bool:
        """Check if images are approved in Image Studio (all images approved/uploaded)."""
        try:
            from src.content.image_studio import ImageStudioService, ImageJobStatus
            studio = ImageStudioService()
            jobs = studio.get_jobs(product_id=product_id)
            active_job = next(
                (j for j in jobs
                 if j.status not in (ImageJobStatus.FAILED.value, ImageJobStatus.ARCHIVED.value)),
                None,
            )
            if not active_job or not active_job.images:
                return False
            approved_statuses = {ImageJobStatus.APPROVED.value, ImageJobStatus.UPLOADED.value}
            return all(img.get("status") in approved_statuses for img in active_job.images)
        except Exception:
            return False

    def is_ready_to_publish(self, product_id: str) -> dict:
        """Check if both text and images are approved.
        Returns dict with text_ok, images_ok, ready booleans."""
        text_ok = self.is_text_approved(product_id)
        images_ok = self.are_images_approved(product_id)
        return {
            "text_ok": text_ok,
            "images_ok": images_ok,
            "ready": text_ok and images_ok,
        }

    def delete_content(self, product_id: str) -> bool:
        """Delete stored content for a product."""
        if product_id in self._content:
            del self._content[product_id]
            self._save_cache()
            return True
        return False

    # ── Push to Shopify ────────────────────────────────────────

    def push_to_shopify(self, product_id: str, shopify_product_id: str) -> bool:
        """Update an existing Shopify product with the current content."""
        payload = self.get_shopify_payload(product_id)
        if not payload:
            return False

        try:
            from src.shopify.listing_manager import ShopifyListingManager
            shopify = ShopifyListingManager()

            updates = {
                "title": payload["title"],
                "description_html": payload["description_html"],
                "tags": payload["tags"],
            }
            success = shopify.update_listing(shopify_product_id, updates)

            # Update SEO metafields
            if success and payload.get("meta_title"):
                shopify.set_product_metafield(
                    shopify_product_id,
                    namespace="global",
                    key="title_tag",
                    value=payload["meta_title"],
                )
            if success and payload.get("meta_description"):
                shopify.set_product_metafield(
                    shopify_product_id,
                    namespace="global",
                    key="description_tag",
                    value=payload["meta_description"],
                )

            return success
        except Exception as e:
            logger.error("Push to Shopify failed: %s", e)
            return False

    # ── Cache ──────────────────────────────────────────────────

    def _save_cache(self) -> None:
        cache_file = CACHE_DIR / "content.json"
        cache_file.write_text(json.dumps(self._content, indent=2, default=str))

    def _load_cache(self) -> None:
        cache_file = CACHE_DIR / "content.json"
        if cache_file.exists():
            try:
                self._content = json.loads(cache_file.read_text())
                logger.info("Loaded %d content entries from cache", len(self._content))
            except Exception as e:
                logger.warning("Failed to load content cache: %s", e)


def _render_pdp_html_with_emojis(data: dict, emojis: list[str]) -> str:
    """Render structured product data into HTML with custom emoji list."""
    from src.content.product_content import _theme_css_overrides, _collapsible

    parts: list[str] = []
    parts.append(_theme_css_overrides())

    # Key feature bullets
    features = data.get("key_features", [])[:5]
    if features:
        items = "\n".join(f"<li>{f}</li>" for f in features)
        parts.append(f"<ul>\n{items}\n</ul>")

    # Benefit blocks with custom emojis
    benefits = data.get("benefits", [])
    for i, benefit in enumerate(benefits):
        emoji = emojis[i % len(emojis)] if emojis else ""
        heading = benefit.get("heading", "")
        text = benefit.get("text", "")
        prefix = f"{emoji} " if emoji else ""
        parts.append(
            f'<div style="margin-bottom:28px;">\n'
            f'  <h3 style="font-size:18px;color:#222;margin:0 0 8px;">'
            f'{prefix}{heading}</h3>\n'
            f'  <p style="font-size:15px;line-height:1.7;color:#555;'
            f'margin:0;">{text}</p>\n'
            f'</div>'
        )

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
            f'{included_items}\n</ul>'
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

    # Shipping & Returns
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
