"""
Image Studio — Review and approve auto-generated product images.

Features: archive + auto-regenerate on "Start Over", previous attempts
gallery, version history, click-to-enlarge, per-image generator override,
reference image upload for regeneration.
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import logging

from src.content.image_studio import (
    ImageStudioService,
    IMAGE_TYPE_LABELS,
    GENERATOR_INFO,
    get_cost_per_image,
)
from src.core.models import ImageJobStatus, ImageGeneratorType

logger = logging.getLogger(__name__)

_GEN_OPTIONS = list(GENERATOR_INFO.keys())
_GEN_SHORT_LABELS = {
    k: f"{v.get('name', k)} ({v.get('cost_label', '?')})"
    for k, v in GENERATOR_INFO.items()
}


def _format_status(status: str) -> str:
    return status.replace("_", " ").title() if status else "—"


@st.cache_resource
def get_studio_service():
    return ImageStudioService()


def get_products_from_sheet():
    try:
        from src.sheets.manager import GoogleSheetsStore
        store = GoogleSheetsStore()
        return store.get_products()
    except Exception as e:
        logger.warning("Could not load products: %s", e)
        return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    st.title("Image Studio")
    st.caption("Review AI-generated product images. Approve or give feedback to regenerate.")

    studio = get_studio_service()

    products = get_products_from_sheet()
    if not products:
        st.warning("No products found in the pipeline.")
        return

    READY_STATUSES = {"cost_received", "ready_to_test", "listing_created", "testing", "winner", "scaling"}
    eligible = [p for p in products if p.keyword and (p.test_status in READY_STATUSES or p.shopify_product_id)]
    product_options = {f"{p.keyword} ({p.country})": p for p in eligible}

    if not product_options:
        st.info("No products ready for image generation yet.")
        return

    selected_label = st.selectbox("Select product", list(product_options.keys()))
    product = product_options[selected_label]

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(f"**Keyword**\n\n{product.keyword}")
    c2.markdown(f"**Country**\n\n{product.country}")
    c3.markdown(f"**Price**\n\n€{product.selling_price:.2f}" if product.selling_price else "**Price**\n\n—")
    c4.markdown(f"**Status**\n\n{_format_status(product.test_status)}")

    # Reference links
    has_ref = getattr(product, "competitor_pdp_url", "") or product.google_shopping_url or product.aliexpress_url
    if has_ref:
        ref_cols = st.columns([3, 3, 3])
        with ref_cols[0]:
            pdp = getattr(product, "competitor_pdp_url", "")
            if pdp:
                st.markdown(f"🏆 [Competitor product page]({pdp})")
            elif product.google_shopping_url:
                st.markdown(f"🔍 [Search competitors on Google Shopping]({product.google_shopping_url})")
        with ref_cols[1]:
            if product.google_shopping_url and getattr(product, "competitor_pdp_url", ""):
                st.markdown(f"🔍 [Google Shopping search]({product.google_shopping_url})")
        with ref_cols[2]:
            if product.aliexpress_url:
                st.markdown(f"📦 [AliExpress source listing]({product.aliexpress_url})")

    st.divider()

    # ── Active job vs generate form ──────────────────────────────
    existing_jobs = studio.get_jobs(product_id=product.product_id)
    active_job = next(
        (j for j in existing_jobs
         if j.status not in (ImageJobStatus.FAILED.value, ImageJobStatus.ARCHIVED.value)),
        None,
    )

    if active_job and active_job.images:
        render_review_gallery(studio, active_job, product)
    else:
        render_generate_form(studio, product)

    # ── Previous attempts ────────────────────────────────────────
    render_previous_attempts(studio, product)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  GENERATE FORM  (shown only when there's no active job)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def render_generate_form(studio, product):
    st.info("No images generated yet for this product. Provide a reference image to start.")

    ref_urls = []
    if product.aliexpress_image_urls:
        ref_urls = [u.strip() for u in product.aliexpress_image_urls.split(",") if u.strip()]

    default_ref = ref_urls[0] if ref_urls else st.session_state.get("last_ref_url", "")

    ref_url = st.text_input("Reference image URL", value=default_ref, placeholder="Paste a competitor or AliExpress product image URL")
    description = st.text_input("Product description", value=product.keyword)

    # Product instructions — custom guidance injected into every image prompt
    default_instructions = st.session_state.get("last_instructions", "")
    instructions = st.text_area(
        "Product instructions (optional)",
        value=default_instructions,
        height=80,
        placeholder="e.g. Pay close attention to the remote control — it has a digital LED display and 6 buttons in 2 columns...",
        help="These instructions are injected into every image prompt. Use this to guide the AI on specific details like the remote control, fabric pattern, etc. The system also auto-analyzes the reference image for a detailed JSON product spec.",
    )

    from src.core.config import AppConfig
    config = AppConfig()
    default_gen = config.get("image_studio.default_generator", ImageGeneratorType.OPENAI_GPT_IMAGE.value)
    default_idx = _GEN_OPTIONS.index(default_gen) if default_gen in _GEN_OPTIONS else 0

    selected_gen = st.selectbox("Image generator", options=_GEN_OPTIONS, index=default_idx, format_func=lambda x: _GEN_SHORT_LABELS.get(x, x))
    gen_info = GENERATOR_INFO.get(selected_gen, {})
    if gen_info:
        st.caption(gen_info.get("description", ""))

    num_images = int(config.get("image_studio.num_images", 5))
    cost_per = gen_info.get("cost_per_image", 0.06)

    col_btn, col_cost = st.columns([3, 1])
    with col_cost:
        st.metric("Estimated cost", f"${num_images * cost_per:.2f}", help=f"{num_images} images × ${cost_per:.3f}")
    with col_btn:
        if ref_url and description:
            if st.button(f"Generate {num_images} Images", type="primary", use_container_width=True):
                _run_generation(studio, product, ref_url, description, num_images, instructions)
        else:
            st.caption("Paste a reference image URL above to start.")


def _run_generation(studio, product, ref_url, description, num_images, instructions=""):
    """Shared generation logic used by both the form and Start Over."""
    progress = st.progress(0, text="Analyzing reference image...")

    def on_progress(cur, tot, msg):
        progress.progress(cur / max(tot, 1), text=f"({cur}/{tot}) {msg}")

    with st.spinner("Generating — this takes 1-3 minutes (includes AI reference analysis)..."):
        job = studio.auto_generate_job(
            product_id=product.product_id,
            product_keyword=product.keyword,
            reference_image_urls=[ref_url],
            product_description=description,
            target_language=product.language or "en",
            num_images=num_images,
            progress_callback=on_progress,
            product_instructions=instructions,
        )

    progress.progress(1.0, text="Done!")
    if job.status == ImageJobStatus.FAILED.value:
        st.error(f"Generation failed: {job.notes}")
    else:
        st.success(f"Generated {len(job.images)} images!")
        st.rerun()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  REVIEW GALLERY  (active job)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def render_review_gallery(studio, job, product):
    total = len(job.images)
    approved = sum(1 for img in job.images if img["status"] == ImageJobStatus.APPROVED.value)
    review_count = sum(1 for img in job.images if img["status"] == ImageJobStatus.REVIEW.value)
    rejected = sum(1 for img in job.images if img["status"] == ImageJobStatus.REJECTED.value)

    meta = {}
    try:
        meta = json.loads(job.notes) if job.notes else {}
    except (json.JSONDecodeError, TypeError):
        pass

    reference_url = meta.get("reference_url", "")
    cost_initial = meta.get("cost_initial", 0.0)
    cost_regen = meta.get("cost_regenerations", 0.0)
    cost_total = cost_initial + cost_regen
    job_cost_per_image = get_cost_per_image(job.default_generator)

    # ── Summary row ──────────────────────────────────────────────
    cols = st.columns(7)
    cols[0].metric("Total", total)
    cols[1].metric("Approved", f"{approved}/{total}")
    cols[2].metric("To Review", review_count)
    cols[3].metric("Rejected", rejected)
    cols[4].metric("Initial Cost", f"${cost_initial:.2f}")
    cols[5].metric("Retries Cost", f"${cost_regen:.2f}")
    cols[6].metric("Total Cost", f"${cost_total:.2f}")

    # ── Quick actions ────────────────────────────────────────────
    act = st.columns(4)
    with act[0]:
        if review_count > 0 and st.button("Approve All", use_container_width=True):
            studio.approve_all(job.job_id)
            st.rerun()
    with act[1]:
        if approved == total and total > 0:
            if st.button("Upload Images to Shopify", type="primary", use_container_width=True):
                _upload_to_shopify(studio, job)
    with act[2]:
        if st.button("Start Over", use_container_width=True, help="Archive this batch — you can adjust settings and add instructions before generating new images"):
            settings = studio.archive_job(job.job_id)
            if settings:
                # Pre-fill the generate form with values from the archived job
                st.session_state["last_ref_url"] = settings.get("reference_url", "")
                st.session_state["last_instructions"] = settings.get("product_instructions", "")
                st.rerun()
    with act[3]:
        if st.button("Delete", use_container_width=True, help="Permanently delete this batch"):
            studio.delete_job(job.job_id)
            st.rerun()

    st.divider()

    # ── Reference image + instructions ──────────────────────────
    product_instructions = meta.get("product_instructions", "")
    if reference_url:
        with st.expander("Reference Image (competitor)", expanded=True):
            ref_col1, ref_col2 = st.columns([1, 3])
            with ref_col1:
                st.image(reference_url, caption="Competitor reference", width=250)
            with ref_col2:
                st.caption(f"URL: {reference_url}")
                st.caption("Compare each generated image against this to check product accuracy.")
                if product_instructions:
                    st.markdown(f"**Product instructions:** {product_instructions}")

    # ── Image cards ──────────────────────────────────────────────
    for img in job.images:
        _render_image_card(studio, job, img, job_cost_per_image)

    # ── Publish readiness (all images approved) ───────────────
    all_approved = approved == total and total > 0
    if all_approved:
        _render_publish_section(product)


def _render_publish_section(product):
    """Show publish readiness + publish button when all images approved."""
    st.divider()
    st.subheader("Ready to Publish?")

    try:
        from src.content.content_studio import ContentStudioService
        cs = ContentStudioService()
        readiness = cs.is_ready_to_publish(product.product_id)
    except Exception:
        readiness = {"text_ok": False, "images_ok": True, "ready": False}

    col_t, col_i = st.columns(2)
    with col_t:
        if readiness["text_ok"]:
            st.success("Text content approved ✅")
        else:
            st.warning("Text not approved — go to **Content Studio** and approve")
    with col_i:
        st.success("Images approved ✅")

    if readiness["ready"]:
        st.success("Both text and images are approved — **ready to publish!**")

        shopify_id = product.shopify_product_id
        if shopify_id:
            if st.button("Publish (make live)", type="primary", use_container_width=True,
                         key="img_publish",
                         help="Sets the product to 'active' — visible on your store immediately."):
                try:
                    from src.shopify.listing_manager import ShopifyListingManager
                    shopify = ShopifyListingManager()
                    success = shopify.update_listing(shopify_id, {"status": "active"})
                    if success:
                        st.success("Product is now **live** on your store!")
                        st.balloons()
                    else:
                        st.error("Failed to publish.")
                except Exception as e:
                    st.error(f"Failed: {e}")
        else:
            st.info("No Shopify listing yet — create one first via the Products tab.")
    else:
        st.info(
            "All images are approved! Now go to the **Content Studio** to review and approve "
            "the text. Once both are approved, the Publish button will unlock."
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SINGLE IMAGE CARD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_image_card(studio, job, img, job_cost_per_image):
    request_id = img["request_id"]
    status = img["status"]
    image_type = img.get("image_type", "unknown")
    type_label = IMAGE_TYPE_LABELS.get(image_type, image_type)
    retry_count = img.get("retry_count", 0)
    current_gen = img.get("generator", job.default_generator)

    with st.container(border=True):
        col_gen, col_act = st.columns([3, 2])

        with col_gen:
            icon = _status_icon(status)
            retry_tag = f" (retry #{retry_count})" if retry_count else ""
            st.markdown(f"**Image {img['image_index']}: {type_label}** {icon}{retry_tag}")

            # ── Version navigation (clean dots) ─────────────
            if retry_count > 0:
                versions = studio.get_all_versions(request_id, current_generator=current_gen)

                if len(versions) > 1:
                    ver_key = f"ver_{request_id}"
                    if ver_key not in st.session_state:
                        st.session_state[ver_key] = len(versions) - 1

                    dot_cols = st.columns(len(versions) + 2)
                    with dot_cols[0]:
                        if st.button("◀", key=f"prev_{request_id}", use_container_width=True):
                            st.session_state[ver_key] = max(0, st.session_state[ver_key] - 1)
                            st.rerun()
                    for i, ver in enumerate(versions):
                        with dot_cols[i + 1]:
                            is_selected = (i == st.session_state[ver_key])
                            label = f"**v{i+1}**" if is_selected else f"v{i+1}"
                            if st.button(label, key=f"dot_{request_id}_{i}", use_container_width=True):
                                st.session_state[ver_key] = i
                                st.rerun()
                    with dot_cols[-1]:
                        if st.button("▶", key=f"next_{request_id}", use_container_width=True):
                            st.session_state[ver_key] = min(len(versions) - 1, st.session_state[ver_key] + 1)
                            st.rerun()

                    sel = st.session_state[ver_key]
                    display_bytes = versions[sel]["bytes"]
                    ver_gen = versions[sel]["generator"]
                    is_current = versions[sel]["is_current"]
                    st.caption(f"v{sel+1} of {len(versions)} — {ver_gen}{'  (current)' if is_current else ''}")
                else:
                    display_bytes = studio.get_image_bytes(request_id)
            else:
                display_bytes = studio.get_image_bytes(request_id)

            if display_bytes:
                st.image(display_bytes, use_container_width=True)
                if st.button("View full size", key=f"zoom_{request_id}", use_container_width=True):
                    _show_fullsize_dialog(display_bytes, f"Image {img['image_index']}: {type_label}")
            elif status == ImageJobStatus.GENERATING.value:
                st.info("Generating...")
            elif status == ImageJobStatus.FAILED.value:
                st.error(f"Failed: {img.get('feedback', 'Unknown error')}")
            else:
                st.warning("No image available")

        # ── Actions column ──────────────────────────────────
        with col_act:
            if retry_count == 0:
                st.caption(f"Cost per retry: ~${job_cost_per_image:.3f}")
            else:
                spent = retry_count * job_cost_per_image
                st.caption(f"Cost per retry: ~${job_cost_per_image:.3f} (spent ${spent:.2f} on {retry_count} retries)")

            if status == ImageJobStatus.REVIEW.value:
                if st.button("Approve", key=f"ok_{request_id}", type="primary", use_container_width=True):
                    studio.approve_image(job.job_id, request_id)
                    st.rerun()

                st.markdown("---")
                st.caption("Not happy? Describe what to change:")
                feedback = st.text_area(
                    "Feedback", key=f"fb_{request_id}", height=80,
                    placeholder="e.g. Make the remote control match the reference...",
                    label_visibility="collapsed",
                )

                ref_upload = st.file_uploader(
                    "Attach reference image (optional)", type=["png", "jpg", "jpeg", "webp"],
                    key=f"ref_{request_id}",
                    help="Upload a screenshot to guide the AI.",
                )
                ref_bytes_cache = ref_upload.read() if ref_upload else None
                if ref_bytes_cache:
                    st.image(ref_bytes_cache, caption="Your reference", width=120)
                    st.caption(f"Reference image attached ({len(ref_bytes_cache):,} bytes)")

                gen_default_idx = _GEN_OPTIONS.index(current_gen) if current_gen in _GEN_OPTIONS else 0
                regen_gen = st.selectbox(
                    "Generator for retry", options=_GEN_OPTIONS, index=gen_default_idx,
                    format_func=lambda x: _GEN_SHORT_LABELS.get(x, x), key=f"gen_{request_id}",
                )

                if st.button("Reject & Regenerate", key=f"rej_{request_id}", disabled=not feedback, use_container_width=True):
                    studio.reject_image(job.job_id, request_id, feedback)
                    with st.spinner("Regenerating..."):
                        try:
                            studio.regenerate_with_feedback(
                                job.job_id, request_id, feedback,
                                reference_image_bytes=ref_bytes_cache,
                                generator_override=regen_gen,
                            )
                        except Exception as e:
                            st.error(f"Failed: {e}")
                    st.rerun()

            elif status == ImageJobStatus.REJECTED.value:
                st.warning(f"Feedback: *{img.get('feedback', '')}*")
                new_fb = st.text_area("Updated feedback", key=f"rfb_{request_id}", value=img.get("feedback", ""), height=80)

                ref_upload_rej = st.file_uploader(
                    "Attach reference image (optional)", type=["png", "jpg", "jpeg", "webp"],
                    key=f"refr_{request_id}",
                )
                ref_bytes_rej = ref_upload_rej.read() if ref_upload_rej else None
                if ref_bytes_rej:
                    st.image(ref_bytes_rej, caption="Your reference", width=120)
                    st.caption(f"Reference image attached ({len(ref_bytes_rej):,} bytes)")

                gen_default_idx = _GEN_OPTIONS.index(current_gen) if current_gen in _GEN_OPTIONS else 0
                regen_gen_rej = st.selectbox(
                    "Generator for retry", options=_GEN_OPTIONS, index=gen_default_idx,
                    format_func=lambda x: _GEN_SHORT_LABELS.get(x, x), key=f"genr_{request_id}",
                )

                if st.button("Regenerate", key=f"rg_{request_id}", use_container_width=True):
                    with st.spinner("Regenerating..."):
                        try:
                            studio.regenerate_with_feedback(
                                job.job_id, request_id, new_fb,
                                reference_image_bytes=ref_bytes_rej,
                                generator_override=regen_gen_rej,
                            )
                        except Exception as e:
                            st.error(f"Failed: {e}")
                    st.rerun()

            elif status == ImageJobStatus.APPROVED.value:
                st.success("Approved")
                if st.button("Undo", key=f"undo_{request_id}"):
                    img["status"] = ImageJobStatus.REVIEW.value
                    studio._update_job_status(job)
                    studio._save_cache()
                    st.rerun()

            elif status == ImageJobStatus.UPLOADED.value:
                st.success("Uploaded to Shopify")

            elif status == ImageJobStatus.FAILED.value:
                if st.button("Retry", key=f"rt_{request_id}", use_container_width=True):
                    with st.spinner("Retrying..."):
                        try:
                            studio.regenerate_with_feedback(job.job_id, request_id, "Try again with better quality.")
                        except Exception as e:
                            st.error(f"Failed: {e}")
                    st.rerun()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PREVIOUS ATTEMPTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def render_previous_attempts(studio, product):
    """Show archived image batches as a collapsible section."""
    archived = studio.get_archived_jobs(product.product_id)
    if not archived:
        return

    st.divider()
    with st.expander(f"Previous Attempts ({len(archived)})", expanded=False):
        for idx, old_job in enumerate(archived):
            # Parse metadata
            meta = {}
            try:
                meta = json.loads(old_job.notes) if old_job.notes else {}
            except (json.JSONDecodeError, TypeError):
                pass

            cost_init = meta.get("cost_initial", 0.0)
            cost_regen = meta.get("cost_regenerations", 0.0)
            total_cost = cost_init + cost_regen
            created = old_job.created_at[:10] if old_job.created_at else "?"
            num = len(old_job.images)
            approved_count = sum(1 for img in old_job.images if img.get("status") == ImageJobStatus.APPROVED.value)

            st.markdown(f"**Attempt {len(archived) - idx}** — {created} — {num} images ({approved_count} approved) — ${total_cost:.2f} spent")

            # Thumbnail row — clickable for full-size view
            thumb_cols = st.columns(min(num, 5))
            for i, img in enumerate(old_job.images[:5]):
                with thumb_cols[i]:
                    rid = img.get("request_id", "")
                    img_bytes = studio.get_image_bytes(rid)
                    if img_bytes:
                        st.image(img_bytes, use_container_width=True)
                        img_type = IMAGE_TYPE_LABELS.get(img.get("image_type", ""), f"Image {i+1}")
                        if st.button("View", key=f"arch_zoom_{old_job.job_id}_{rid}", use_container_width=True):
                            _show_fullsize_dialog(img_bytes, img_type)
                    else:
                        st.caption("No image")

            # Actions
            btn_cols = st.columns(3)
            with btn_cols[0]:
                if st.button("Restore this batch", key=f"restore_{old_job.job_id}", use_container_width=True,
                             help="Make this the active batch again"):
                    studio.restore_job(old_job.job_id)
                    st.rerun()
            with btn_cols[1]:
                if st.button("Delete permanently", key=f"del_{old_job.job_id}", use_container_width=True):
                    studio.delete_job(old_job.job_id)
                    st.rerun()

            if idx < len(archived) - 1:
                st.markdown("---")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@st.dialog("Full-size image", width="large")
def _show_fullsize_dialog(image_bytes: bytes, title: str):
    st.markdown(f"**{title}**")
    st.image(image_bytes, use_container_width=True)


def _upload_to_shopify(studio, job):
    from src.shopify.listing_manager import ShopifyListingManager
    approved = studio.get_approved_images(job.job_id)
    if not approved:
        st.warning("No approved images.")
        return
    if not job.product_id:
        st.warning("No product ID linked.")
        return
    try:
        manager = ShopifyListingManager()
        manager.add_images(job.product_id, [b for _, b in approved])
        for img in job.images:
            if img["status"] == ImageJobStatus.APPROVED.value:
                img["status"] = ImageJobStatus.UPLOADED.value
        studio._update_job_status(job)
        studio._save_cache()
        st.success(f"Uploaded {len(approved)} image(s) to Shopify!")
    except Exception as e:
        st.error(f"Upload failed: {e}")


def _status_icon(status):
    return {
        ImageJobStatus.REVIEW.value: "👁️",
        ImageJobStatus.APPROVED.value: "✅",
        ImageJobStatus.REJECTED.value: "❌",
        ImageJobStatus.GENERATING.value: "⚡",
        ImageJobStatus.UPLOADED.value: "📤",
        ImageJobStatus.FAILED.value: "💥",
        ImageJobStatus.ARCHIVED.value: "📦",
    }.get(status, "")


main()
