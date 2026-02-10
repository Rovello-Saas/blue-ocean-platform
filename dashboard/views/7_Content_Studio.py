"""
Content Studio — Edit product text, emojis, and preview PDP content.

Features:
- View and edit all product text sections (title, features, benefits, etc.)
- Regenerate individual sections via AI with custom instructions
- Change emoji set or individual benefit emojis
- Live HTML preview of the final PDP
- Push content updates to Shopify
"""

import json
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import logging

from src.content.content_studio import (
    ContentStudioService,
    EMOJI_SETS,
)

logger = logging.getLogger(__name__)

# Only show products that are validated / ready for the store
READY_STATUSES = {
    "cost_received", "ready_to_test", "listing_created",
    "testing", "winner", "scaling",
}

# ── Example seed content for the electric heated blanket ──────
_BLANKET_SEED = {
    "title": "Premium Electric Heated Blanket — 10 Heat Settings, Timer & Auto Shut-Off",
    "key_features": [
        "10 adjustable heat settings",
        "Auto shut-off timer for safety",
        "Ultra-soft flannel & sherpa fabric",
        "Machine washable (detach controller)",
        "Large 180 × 130 cm size",
    ],
    "benefits": [
        {
            "heading": "Save Up to 60% on Heating Bills",
            "text": "Instead of heating the entire house, warm yourself directly. An electric heated blanket uses a fraction of the energy of central heating — keeping you cosy on the couch while saving significantly on your monthly energy bill.",
        },
        {
            "heading": "10 Heat Levels — Your Perfect Temperature",
            "text": "Whether you prefer a gentle warmth or deep heat, the intuitive controller lets you dial in exactly the right temperature. The blanket heats up in under 5 minutes, so you're never left waiting.",
        },
        {
            "heading": "Ultra-Soft Comfort You Can Feel",
            "text": "Wrapped in premium flannel on one side and plush sherpa on the other, this blanket feels as luxurious as it performs. It drapes beautifully over a sofa and looks great in any living room.",
        },
        {
            "heading": "Built-In Safety You Can Trust",
            "text": "The automatic shut-off timer (1–12 hours) and overheat protection give you peace of mind — fall asleep worry-free knowing the blanket turns itself off.",
        },
        {
            "heading": "Easy Care, Long-Lasting Quality",
            "text": "Simply detach the controller and toss the blanket in the washing machine. The heating wires are engineered to withstand regular washing without losing performance.",
        },
    ],
    "whats_included": [
        "1× Electric heated blanket (180 × 130 cm)",
        "1× Detachable controller with LED display",
        "1× Power cord (2.5 m)",
        "1× User manual (EN/DE/FR/NL)",
    ],
    "specs": [
        {"label": "Dimensions", "value": "180 × 130 cm"},
        {"label": "Weight", "value": "1.8 kg"},
        {"label": "Material (top)", "value": "Flannel"},
        {"label": "Material (bottom)", "value": "Sherpa fleece"},
        {"label": "Heat Settings", "value": "10 levels"},
        {"label": "Timer", "value": "1–12 hours auto shut-off"},
        {"label": "Voltage", "value": "220–240 V / 50 Hz"},
        {"label": "Power", "value": "120 W"},
        {"label": "Cable Length", "value": "2.5 m"},
        {"label": "Washable", "value": "Yes — machine wash (remove controller)"},
    ],
    "faq": [
        {
            "q": "Is it safe to sleep with the heated blanket on?",
            "a": "Yes. The auto shut-off timer and overheat protection ensure the blanket turns off automatically, so you can fall asleep safely.",
        },
        {
            "q": "Can I wash this blanket in the washing machine?",
            "a": "Absolutely. Detach the controller and power cord, then machine wash on a gentle cycle at 30 °C. Air dry or tumble dry on low.",
        },
        {
            "q": "How much electricity does it use?",
            "a": "At 120 W on the highest setting, it costs roughly €0.04 per hour — a fraction of what central heating costs to run.",
        },
        {
            "q": "How quickly does it heat up?",
            "a": "The blanket reaches a comfortable temperature in under 5 minutes. Higher settings warm up even faster.",
        },
        {
            "q": "Is the blanket large enough for two people?",
            "a": "At 180 × 130 cm it's generous for one person and can work for two when cuddling on the sofa, though each person gets more coverage solo.",
        },
    ],
    "meta_title": "Electric Heated Blanket — 10 Settings, Timer, Washable",
    "meta_description": "Stay warm and save on heating bills with this premium electric heated blanket. 10 heat settings, auto shut-off timer, ultra-soft flannel & sherpa. Free shipping.",
    "tags": "heated blanket, electric blanket, throw blanket, winter, cosy, energy saving, washable, timer, sherpa, flannel",
    "product_type": "Heated Blanket",
}


@st.cache_resource
def get_content_service():
    return ContentStudioService()


def get_products_from_sheet():
    try:
        from src.sheets.manager import GoogleSheetsStore
        store = GoogleSheetsStore()
        return store.get_products()
    except Exception as e:
        logger.error("Failed to load products: %s", e)
        return []


def _seed_blanket_if_needed(service, product):
    """Seed example content for the electric heated blanket on first visit."""
    if service.has_content(product.product_id):
        return
    kw = (product.keyword or "").lower()
    if "blanket" in kw or "heated" in kw:
        content_data = {
            "product_id": product.product_id,
            "product_keyword": product.keyword,
            "structured": dict(_BLANKET_SEED),
            "emoji_set": "default",
            "text_approved": False,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }
        service._content[product.product_id] = content_data
        service._save_cache()
        logger.info("Seeded example content for blanket product %s", product.product_id)


def main():
    st.title("Content Studio")
    st.caption("Edit product text, regenerate sections with AI, and push to Shopify")

    service = get_content_service()
    service._load_cache()

    products = get_products_from_sheet()
    if not products:
        st.warning("No products found. Add products via the Research tab first.")
        return

    # Only show validated products (same filter as Image Studio)
    eligible = [
        p for p in products
        if p.keyword and (p.test_status in READY_STATUSES or p.shopify_product_id)
    ]

    if not eligible:
        st.info("No products validated for the store yet. Products appear here once they reach 'cost_received' status or have a Shopify listing.")
        return

    product_options = {
        p.product_id: f"{p.keyword} ({p.country})"
        for p in eligible
    }
    selected_id = st.selectbox(
        "Select product",
        options=list(product_options.keys()),
        format_func=lambda x: product_options[x],
    )

    if not selected_id:
        return

    product = next((p for p in eligible if p.product_id == selected_id), None)
    if not product:
        return

    # Auto-seed blanket example content on first visit
    _seed_blanket_if_needed(service, product)

    content = service.get_content(selected_id)

    # If no content exists, show generate button
    if not content:
        st.info(f"No content generated yet for **{product.keyword}**.")

        # Auto-suggest reference URL: competitor first, AliExpress fallback
        ref_url = product.google_shopping_url or product.aliexpress_url or ""
        ref_source = ""
        if product.google_shopping_url:
            ref_source = "Google Shopping competitor"
        elif product.aliexpress_url:
            ref_source = "AliExpress listing"

        if ref_url:
            st.caption(f"Reference URL ({ref_source}): [{ref_url}]({ref_url})")

        ref_desc = st.text_area(
            "Reference description",
            value=ref_url,
            placeholder="Paste competitor/supplier description or URL for inspiration...",
            height=100,
            help="Competitor descriptions produce better results than AliExpress listings. "
                 "The system pre-fills with the best available reference.",
        )

        if st.button("Generate Content", type="primary"):
            with st.spinner("Generating product content with AI..."):
                result = service.generate_content(product, ref_desc)
                if result:
                    st.success("Content generated!")
                    st.rerun()
                else:
                    st.error("Generation failed. Check logs.")
        return

    # ── Content exists — show editor ──────────────────────────
    structured = content["structured"]

    # Approval status banner
    readiness = service.is_ready_to_publish(selected_id)
    text_approved = readiness["text_ok"]
    images_approved = readiness["images_ok"]

    status_cols = st.columns([2, 2, 2, 2])
    with status_cols[0]:
        st.markdown(f"**Text:** {'✅ Approved' if text_approved else '⏳ Not approved'}")
    with status_cols[1]:
        st.markdown(f"**Images:** {'✅ Approved' if images_approved else '⏳ Not approved'}")
    with status_cols[2]:
        if readiness["ready"]:
            st.markdown("**Ready to publish** ✅")
        else:
            missing = []
            if not text_approved:
                missing.append("text")
            if not images_approved:
                missing.append("images")
            st.markdown(f"**Waiting for:** {', '.join(missing)}")

    # Tabs for different editing modes
    tab_edit, tab_preview, tab_push = st.tabs(["Edit Content", "Preview", "Publish"])

    with tab_edit:
        render_editor(service, selected_id, product, content)

    with tab_preview:
        render_preview(service, selected_id, product)

    with tab_push:
        render_push(service, selected_id, product)


def render_editor(service, product_id, product, content):
    """Render the content editor with all editable sections."""
    structured = content["structured"]

    # ── Reference links ─────────────────────────────────────
    has_ref = product.competitor_pdp_url or product.google_shopping_url or product.aliexpress_url
    if has_ref:
        ref_cols = st.columns([3, 3, 3])
        with ref_cols[0]:
            if product.competitor_pdp_url:
                st.markdown(
                    f"🏆 [Competitor product page]({product.competitor_pdp_url})"
                )
            elif product.google_shopping_url:
                st.markdown(
                    f"🔍 [Search competitors on Google Shopping]({product.google_shopping_url})"
                )
        with ref_cols[1]:
            if product.google_shopping_url and product.competitor_pdp_url:
                st.markdown(
                    f"🔍 [Google Shopping search]({product.google_shopping_url})"
                )
        with ref_cols[2]:
            if product.aliexpress_url:
                st.markdown(
                    f"📦 [AliExpress source listing]({product.aliexpress_url})"
                )
        st.divider()

    # ── Title ─────────────────────────────────────────────────
    st.subheader("Title")
    col1, col2 = st.columns([5, 1])
    with col1:
        new_title = st.text_input(
            "Product title",
            value=structured.get("title", ""),
            key="edit_title",
            label_visibility="collapsed",
        )
        if new_title != structured.get("title", ""):
            service.update_field(product_id, "title", new_title)
    with col2:
        if st.button("Regenerate", key="regen_title"):
            _regenerate_section(service, product_id, "title")

    # ── Key Features ──────────────────────────────────────────
    st.subheader("Key Features (bullet points)")
    features = structured.get("key_features", [])
    changed_features = False
    new_features = []
    for i, feat in enumerate(features):
        new_val = st.text_input(
            f"Feature {i+1}",
            value=feat,
            key=f"feat_{i}",
            label_visibility="collapsed",
        )
        new_features.append(new_val)
        if new_val != feat:
            changed_features = True
    if changed_features:
        service.update_field(product_id, "key_features", new_features)

    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("Regenerate", key="regen_features"):
            _regenerate_section(service, product_id, "key_features")

    # ── Benefits (with emoji picker) ──────────────────────────
    st.subheader("Benefit Sections")

    # Emoji set picker
    emoji_set = content.get("emoji_set", "default")
    col_emoji, col_label = st.columns([2, 4])
    with col_emoji:
        new_emoji_set = st.selectbox(
            "Emoji style",
            options=list(EMOJI_SETS.keys()),
            index=list(EMOJI_SETS.keys()).index(emoji_set) if emoji_set in EMOJI_SETS else 0,
            format_func=lambda x: f"{x.title()} ({' '.join(EMOJI_SETS[x][:4])}...)" if EMOJI_SETS[x][0] else f"{x.title()} (no emojis)",
            key="emoji_set_picker",
        )
        if new_emoji_set != emoji_set:
            service.set_emoji_set(product_id, new_emoji_set)
            st.rerun()

    benefits = structured.get("benefits", [])
    current_emojis = EMOJI_SETS.get(emoji_set, EMOJI_SETS["default"])
    custom_emojis = content.get("custom_emojis", {})

    for i, benefit in enumerate(benefits):
        emoji = custom_emojis.get(str(i), current_emojis[i % len(current_emojis)])
        with st.container():
            col_em, col_head, col_act = st.columns([0.5, 4.5, 1])
            with col_em:
                new_em = st.text_input(
                    "Emoji",
                    value=emoji,
                    key=f"benefit_emoji_{i}",
                    label_visibility="collapsed",
                    max_chars=4,
                )
                if new_em != emoji:
                    service.set_benefit_emoji(product_id, i, new_em)
            with col_head:
                new_heading = st.text_input(
                    f"Heading {i+1}",
                    value=benefit.get("heading", ""),
                    key=f"benefit_heading_{i}",
                    label_visibility="collapsed",
                )
                if new_heading != benefit.get("heading", ""):
                    benefit["heading"] = new_heading
                    service.update_field(product_id, "benefits", benefits)
            with col_act:
                st.write("")

            new_text = st.text_area(
                f"Benefit text {i+1}",
                value=benefit.get("text", ""),
                key=f"benefit_text_{i}",
                height=80,
                label_visibility="collapsed",
            )
            if new_text != benefit.get("text", ""):
                benefit["text"] = new_text
                service.update_field(product_id, "benefits", benefits)
            st.divider()

    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("Regenerate", key="regen_benefits"):
            _regenerate_section(service, product_id, "benefits")

    # ── What's Included ───────────────────────────────────────
    st.subheader("What's Included")
    whats_included = structured.get("whats_included", [])
    changed_included = False
    new_included = []
    for i, item in enumerate(whats_included):
        new_val = st.text_input(
            f"Item {i+1}",
            value=item,
            key=f"included_{i}",
            label_visibility="collapsed",
        )
        new_included.append(new_val)
        if new_val != item:
            changed_included = True
    if changed_included:
        service.update_field(product_id, "whats_included", new_included)

    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("Regenerate", key="regen_included"):
            _regenerate_section(service, product_id, "whats_included")

    # ── Specifications ────────────────────────────────────────
    st.subheader("Specifications")
    specs = structured.get("specs", [])
    changed_specs = False
    new_specs = []
    for i, spec in enumerate(specs):
        col_l, col_v = st.columns(2)
        with col_l:
            new_label = st.text_input(
                "Label",
                value=spec.get("label", ""),
                key=f"spec_label_{i}",
                label_visibility="collapsed",
            )
        with col_v:
            new_value = st.text_input(
                "Value",
                value=spec.get("value", ""),
                key=f"spec_value_{i}",
                label_visibility="collapsed",
            )
        new_specs.append({"label": new_label, "value": new_value})
        if new_label != spec.get("label", "") or new_value != spec.get("value", ""):
            changed_specs = True
    if changed_specs:
        service.update_field(product_id, "specs", new_specs)

    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("Regenerate", key="regen_specs"):
            _regenerate_section(service, product_id, "specs")

    # ── FAQ ────────────────────────────────────────────────────
    st.subheader("FAQ")
    faq = structured.get("faq", [])
    changed_faq = False
    new_faq = []
    for i, item in enumerate(faq):
        new_q = st.text_input(
            f"Q{i+1}",
            value=item.get("q", ""),
            key=f"faq_q_{i}",
            label_visibility="collapsed",
        )
        new_a = st.text_area(
            f"A{i+1}",
            value=item.get("a", ""),
            key=f"faq_a_{i}",
            height=60,
            label_visibility="collapsed",
        )
        new_faq.append({"q": new_q, "a": new_a})
        if new_q != item.get("q", "") or new_a != item.get("a", ""):
            changed_faq = True
        if i < len(faq) - 1:
            st.divider()
    if changed_faq:
        service.update_field(product_id, "faq", new_faq)

    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("Regenerate", key="regen_faq"):
            _regenerate_section(service, product_id, "faq")

    # ── SEO ────────────────────────────────────────────────────
    st.subheader("SEO")
    col1, col2 = st.columns(2)
    with col1:
        new_meta_title = st.text_input(
            "Meta title (max 60 chars)",
            value=structured.get("meta_title", ""),
            key="edit_meta_title",
            max_chars=60,
        )
        if new_meta_title != structured.get("meta_title", ""):
            service.update_field(product_id, "meta_title", new_meta_title)
    with col2:
        new_meta_desc = st.text_input(
            "Meta description (max 155 chars)",
            value=structured.get("meta_description", ""),
            key="edit_meta_desc",
            max_chars=155,
        )
        if new_meta_desc != structured.get("meta_description", ""):
            service.update_field(product_id, "meta_description", new_meta_desc)

    new_tags = st.text_input(
        "Tags (comma-separated)",
        value=structured.get("tags", ""),
        key="edit_tags",
    )
    if new_tags != structured.get("tags", ""):
        service.update_field(product_id, "tags", new_tags)

    # ── Actions ────────────────────────────────────────────────
    st.divider()
    text_approved = content.get("text_approved", False)
    col_approve, col_regen = st.columns([4, 2])
    with col_approve:
        if text_approved:
            st.success("Text content is **approved** ✅")
            if st.button("Revoke approval", key="unapprove_text"):
                service.unapprove_text(product_id)
                st.rerun()
        else:
            st.info("Review the content above, then approve when you're happy with it.")
            if st.button("Approve text content", type="primary", key="approve_text"):
                service.approve_text(product_id)
                st.rerun()
    with col_regen:
        if st.button("Regenerate ALL content", type="secondary", use_container_width=True):
            with st.spinner("Regenerating all content..."):
                result = service.generate_content(product)
                if result:
                    st.success("All content regenerated!")
                    st.rerun()
                else:
                    st.error("Regeneration failed.")


def _regenerate_section(service, product_id, section):
    """Show a dialog to regenerate a specific section."""
    instruction = st.session_state.get(f"regen_instruction_{section}", "")

    @st.dialog(f"Regenerate: {section.replace('_', ' ').title()}")
    def regen_dialog():
        inst = st.text_area(
            "Custom instruction (optional)",
            placeholder="e.g., 'Make it more urgent', 'Highlight the 10 heat settings', 'Add winter theme'...",
            key=f"dialog_instruction_{section}",
            height=80,
        )
        if st.button("Regenerate", type="primary"):
            with st.spinner(f"Regenerating {section}..."):
                result = service.regenerate_section(product_id, section, inst)
                if result:
                    st.success("Done!")
                    st.rerun()
                else:
                    st.error("Failed to regenerate.")

    regen_dialog()


def render_preview(service, product_id, product):
    """Render a live HTML preview of the PDP."""
    html = service.render_html(product_id)
    if not html:
        st.info("No content to preview. Generate content first.")
        return

    st.subheader("PDP Preview")
    st.caption("This is how the product description will appear on Shopify (approximate — fonts and colors come from your theme)")

    # "Preview on Shopify" buttons — admin + storefront
    shopify_id = product.shopify_product_id
    if shopify_id:
        from src.core.config import SHOPIFY_SHOP_URL
        admin_url = f"https://{SHOPIFY_SHOP_URL}/admin/products/{shopify_id}"
        storefront_url = product.shopify_product_url or ""

        col_a, col_b, col_c = st.columns([2, 2, 4])
        with col_a:
            st.link_button("Open in Shopify Admin", admin_url, use_container_width=True)
        with col_b:
            if storefront_url:
                st.link_button("View Storefront Page", storefront_url, use_container_width=True)
        st.caption(
            "**Tip:** In Shopify Admin, click **'View on your online store'** to see the full "
            "preview — this works even for draft/unpublished products."
        )
    else:
        st.caption("No Shopify listing yet — create a listing first to preview on the actual store.")

    # Wrap in a styled container for realistic preview
    preview_html = f"""
    <div style="max-width:700px; margin:0 auto; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; padding:20px; background:#fff; border:1px solid #e5e5e5; border-radius:8px;">
        {html}
    </div>
    """
    st.components.v1.html(preview_html, height=1200, scrolling=True)

    # Show raw HTML toggle
    with st.expander("View raw HTML"):
        st.code(html, language="html")


def render_push(service, product_id, product):
    """Push content to Shopify with clear publish workflow."""
    st.subheader("Publish to Shopify")

    shopify_id = product.shopify_product_id
    shopify_url = product.shopify_product_url

    if not shopify_id:
        st.warning(
            "This product doesn't have a Shopify listing yet. "
            "Create a listing first via the Products tab or the automated pipeline."
        )
        return

    st.info(f"Shopify Product ID: **{shopify_id}**")
    if shopify_url:
        st.markdown(f"[View on Shopify]({shopify_url})")

    # ── Readiness check ──────────────────────────────────────
    readiness = service.is_ready_to_publish(product_id)

    st.markdown("### Checklist")
    col_t, col_i = st.columns(2)
    with col_t:
        if readiness["text_ok"]:
            st.success("Text content approved ✅")
        else:
            st.error("Text not approved yet — go to **Edit Content** tab and approve")
    with col_i:
        if readiness["images_ok"]:
            st.success("Images approved ✅")
        else:
            st.error("Images not approved yet — go to **Image Studio** and approve all images")

    # ── Step 1: Push content (does NOT publish) ──────────────
    st.divider()
    st.markdown("### Step 1: Push content to Shopify")
    st.caption(
        "This updates the product text on Shopify (title, description, SEO, tags). "
        "It replaces the existing HTML, including any old CSS overrides. "
        "**This does NOT make the product live** — it stays in its current status (draft or active)."
    )

    payload = service.get_shopify_payload(product_id)
    if payload:
        with st.expander("What will be updated"):
            st.write(f"**Title:** {payload['title']}")
            st.write(f"**Meta Title:** {payload['meta_title']}")
            st.write(f"**Meta Description:** {payload['meta_description']}")
            st.write(f"**Tags:** {payload['tags']}")
            st.write(f"**Description HTML:** {len(payload['description_html'])} characters")

    if st.button("Push Content to Shopify", use_container_width=True):
        with st.spinner("Updating Shopify listing..."):
            success = service.push_to_shopify(product_id, shopify_id)
            if success:
                st.success("Content pushed to Shopify!")
            else:
                st.error("Failed to push content. Check logs for details.")

    # ── Step 2: Publish / Unpublish ──────────────────────────
    st.divider()
    st.markdown("### Step 2: Publish or unpublish")

    if readiness["ready"]:
        st.success(
            "Both text and images are approved — this product is **ready to publish**."
        )
    else:
        missing = []
        if not readiness["text_ok"]:
            missing.append("approve text in the Edit Content tab")
        if not readiness["images_ok"]:
            missing.append("approve all images in the Image Studio")
        st.warning(
            f"Before publishing, you need to: **{' and '.join(missing)}**."
        )

    col_pub, col_unpub = st.columns(2)
    with col_pub:
        if st.button(
            "Publish (make live)",
            type="primary",
            use_container_width=True,
            disabled=not readiness["ready"],
            help="Both text and images must be approved before publishing. "
                 "Sets the product status to 'active' — visible on your store immediately."
                 if readiness["ready"] else
                 "Disabled — approve text and images first.",
        ):
            with st.spinner("Publishing..."):
                success = _set_shopify_status(shopify_id, "active")
                if success:
                    st.success("Product is now **live** on your store!")
                    st.balloons()
                else:
                    st.error("Failed to publish. Check logs.")
    with col_unpub:
        if st.button(
            "Unpublish (set to draft)",
            type="secondary",
            use_container_width=True,
            help="Sets the product status to 'draft' — it will no longer be visible to customers.",
        ):
            with st.spinner("Unpublishing..."):
                success = _set_shopify_status(shopify_id, "draft")
                if success:
                    st.success("Product is now a **draft** — not visible to customers.")
                else:
                    st.error("Failed to unpublish. Check logs.")


def _set_shopify_status(shopify_product_id: str, status: str) -> bool:
    """Set a Shopify product's status (active/draft/archived)."""
    try:
        from src.shopify.listing_manager import ShopifyListingManager
        shopify = ShopifyListingManager()
        return shopify.update_listing(shopify_product_id, {"status": status})
    except Exception as e:
        logger.error("Failed to set Shopify status: %s", e)
        return False


main()
