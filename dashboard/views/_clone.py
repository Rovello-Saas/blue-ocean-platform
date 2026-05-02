"""
Clone workflow — "I have a competitor URL".

Hosts the page-cloner handoff. User enters a competitor product URL, picks a
target store + language, and the Node page-cloner runs scrape → AI generate
→ upload → publish. This view just starts the job and polls it to completion;
all the actual cloning logic lives in the Node service.

Live-progress pattern: the active job ID is stored in `st.session_state`.
On each render we GET the job record; if it's still running we sleep briefly
and `st.rerun()` so Streamlit re-renders and pulls fresh state. Once the job
hits a terminal status (done/failed) we stop polling and show the result.
"""

import sys
import time
from pathlib import Path

# Dashboard views each re-insert the project root into sys.path so Streamlit
# can import `src.*` regardless of where it was launched from.
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st

from src.page_cloner import (
    PageClonerClient,
    PageClonerError,
    PageClonerUnavailable,
    PageClonerJobFailed,
    PageClonerTimeout,
)


# Stores the page cloner knows about. Mirrors the files in
# page-cloner/stores/*.json on the Node side. If you add a store there, add it
# here too (or, later, wire this to an endpoint that lists available stores).
STORES = [
    {"id": "movanella", "name": "Movanella", "channel": "Google",   "flag": "🇺🇸"},
    {"id": "merivalo",  "name": "Merivalo",  "channel": "Meta",     "flag": "🇩🇪"},
]

# Common target languages. "Auto" means: use the store's default language
# (configured in stores/<id>.json on the Node side). Extend freely — the Node
# side validates against its own LANGUAGE_NAMES table and silently ignores
# anything it doesn't recognise.
LANGUAGES = [
    ("",   "Auto (store default)"),
    ("en", "English"),
    ("de", "German"),
    ("nl", "Dutch"),
    ("fr", "French"),
    ("es", "Spanish"),
    ("it", "Italian"),
]

# Ordered list of steps the page cloner pipeline emits. Order matches the
# Node-side `runPipeline` in src/routes/api.js — keep in sync if steps change.
# Used to render a progress checklist that's stable regardless of which step
# the job is currently on.
STEP_ORDER = [
    ("scraping",    "Scrape source page"),
    ("generating",  "Generate liquid content"),
    ("creating",    "Create product in Shopify"),
    ("translating", "Translate images"),
    ("pushing",     "Push Horizon template"),
    ("publishing",  "Publish product"),
    ("reviews",     "Import reviews"),
]

# Poll cadence for the live view. 2.5 s is the sweet spot: snappy enough to
# feel live, slow enough to avoid hammering localhost or fighting Streamlit's
# own rerun debounce.
POLL_INTERVAL_SEC = 2.5

# Terminal statuses — once we see one, stop polling.
_TERMINAL = {"done", "failed"}


def _status_badge(status: str) -> str:
    """Map a job step's status to a visual marker for the checklist."""
    return {
        "done":    "✅",
        "running": "⏳",
        "pending": "⚪",
    }.get(status, "⚪")


def _render_progress(job: dict) -> None:
    """Render the step-by-step progress checklist for an in-flight job."""
    steps = job.get("steps", {}) or {}
    # Note: `translating` only appears when targetLanguage is set on the job.
    # We filter it out of the checklist when it's missing so we don't show a
    # perpetually-pending row for stores using the source language as-is.
    visible = [(k, label) for k, label in STEP_ORDER if k in steps or k != "translating"]

    for key, label in visible:
        step = steps.get(key, {"status": "pending"})
        st.markdown(f"{_status_badge(step.get('status'))} {label}")

    progress_pct = int(job.get("progress", 0))
    st.progress(progress_pct / 100.0, text=f"{progress_pct}%")


def _render_result(job: dict) -> None:
    """Render the success card — product links + key metadata from the clone."""
    result = job.get("result") or {}
    meta = result.get("productMeta") or {}

    st.success("Clone complete.")

    # Big, obvious links to the thing they just built.
    col1, col2 = st.columns(2)
    with col1:
        if result.get("productUrl"):
            st.link_button(
                "🌐 View live product",
                result["productUrl"],
                use_container_width=True,
            )
    with col2:
        if result.get("adminUrl"):
            st.link_button(
                "🛠️ Open in Shopify admin",
                result["adminUrl"],
                use_container_width=True,
            )

    # Key facts about what got created. Compact metric row — more readable than
    # a dumped dict.
    st.markdown("#### Details")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Title",    meta.get("title", "—"))
    c2.metric("Price",    f"€{meta.get('price', '—')}")
    c3.metric("Variants", meta.get("variantCount", 0))
    c4.metric("Images",   meta.get("imageCount", 0))

    # Reviews side-car: not every clone produces reviews (Loox scraping can
    # fail; product might have none). Show only when relevant.
    reviews = result.get("reviews")
    if reviews and reviews.get("count"):
        st.caption(
            f"📝 Imported {reviews['count']} reviews "
            f"(of {reviews.get('totalSource', '?')} source reviews)."
        )

    # QA summary — the post-clone sanity checks. We surface errors as a hard
    # warning, warnings as info. "All green" collapses to a one-liner so a
    # clean clone doesn't drown in QA chrome.
    qa = result.get("qa")
    if qa:
        errors = qa.get("errors", []) or []
        warnings = qa.get("warnings", []) or []
        if errors or warnings:
            with st.expander(
                f"QA report — {len(errors)} error(s), {len(warnings)} warning(s)",
                expanded=bool(errors),
            ):
                for e in errors:
                    st.error(e)
                for w in warnings:
                    st.warning(w)
        else:
            st.caption("✅ QA: all checks passed.")


def _render_failure(job: dict) -> None:
    """Render the failure card — the error message from the pipeline."""
    st.error("Clone failed.")
    err = job.get("error") or "(no error message)"
    st.code(err, language="text")
    st.caption(
        "Check the Node page-cloner logs for the full stack trace. "
        "The job record is still available at "
        f"`{st.session_state.get('active_job_id', '?')}` for re-inspection."
    )


def _render_recent_jobs(client: PageClonerClient) -> None:
    """Mini-history of the last few jobs the page cloner has seen."""
    try:
        import requests
        r = requests.get(f"{client.base_url}/api/jobs", timeout=5)
        if not r.ok:
            return
        jobs = r.json()
    except Exception:
        # History is nice-to-have, not load-bearing. If the server blinks,
        # just skip it rather than breaking the page.
        return

    if not jobs:
        return

    st.markdown("#### Recent clones")
    for j in jobs[:5]:
        status_icon = {"done": "✅", "failed": "❌"}.get(j.get("status", ""), "⏳")
        url_short = (j.get("url", "") or "").replace("https://", "")[:60]
        st.markdown(
            f"{status_icon} `{j.get('id')}`  **{j.get('status')}**  — {url_short}"
        )


def main() -> None:
    st.title("Clone a competitor page")
    st.caption(
        "Paste a competitor's product URL and we'll scrape, translate, "
        "and publish it to the selected store."
    )

    # Health check up front — fail fast if the Node service isn't running.
    # Cheaper than waiting for the user to click Start and get a confusing
    # connection error.
    try:
        client = PageClonerClient()
        cloner_ready = client.health_check()
    except Exception as exc:
        st.error(
            "The built-in page cloner could not start.\n\n"
            f"{exc}"
        )
        return

    if not cloner_ready:
        st.error(
            "The built-in page cloner is not responding yet. Refresh this page "
            "in a moment and try again."
        )
        return

    # --- Active job view (polling) -------------------------------------------
    # If a job was started in a previous render, we're now in "watching" mode:
    # render the progress checklist, rerun until terminal, then show the result.
    active_job_id = st.session_state.get("active_job_id")
    if active_job_id:
        try:
            job = client.get_job(active_job_id)
        except PageClonerUnavailable:
            st.error("Lost connection to the page cloner mid-job.")
            if st.button("Clear and retry"):
                st.session_state.pop("active_job_id", None)
                st.rerun()
            return
        except PageClonerError as e:
            # 404 on poll means the in-memory job store was wiped (server
            # restart). Treat as "gone" and let the user start over.
            st.error(f"Could not read job `{active_job_id}`: {e}")
            if st.button("Clear and start a new clone"):
                st.session_state.pop("active_job_id", None)
                st.rerun()
            return

        status = job.get("status", "unknown")

        with st.container(border=True):
            st.markdown(
                f"**Job `{active_job_id}`** — `{job.get('url', '')}`  "
                f"→ store **{job.get('storeId')}**"
            )
            _render_progress(job)

        if status == "done":
            _render_result(job)
        elif status == "failed":
            _render_failure(job)
        else:
            # Still running — sleep briefly and rerun so the UI updates.
            # Streamlit's `st.rerun()` re-invokes this script from the top;
            # on the next render we'll hit `get_job()` again with fresh state.
            time.sleep(POLL_INTERVAL_SEC)
            st.rerun()

        # Terminal — give the user a way out.
        if status in _TERMINAL:
            if st.button("Start another clone", type="primary"):
                st.session_state.pop("active_job_id", None)
                st.rerun()

        return

    # --- Start-new-clone view ------------------------------------------------
    with st.form("clone_form", clear_on_submit=False):
        source_url = st.text_input(
            "Competitor product URL",
            placeholder="https://competitor.com/products/cool-pillow",
            help="The source product page. We'll scrape the full DOM, images, and reviews.",
        )

        col1, col2 = st.columns(2)
        with col1:
            # If the user landed here via a hero CTA on the Home page, that
            # CTA stashes the intended store in session state so we can seed
            # the dropdown to the right value. Pop it (one-shot) — leaving
            # it set would override the user's choice on subsequent renders.
            store_options = [s["id"] for s in STORES]
            preselected = st.session_state.pop("clone_preselected_store", None)
            default_index = (
                store_options.index(preselected)
                if preselected in store_options
                else 0
            )
            store = st.selectbox(
                "Target store",
                options=store_options,
                index=default_index,
                format_func=lambda sid: next(
                    f"{s['flag']} {s['name']} ({s['channel']})"
                    for s in STORES if s["id"] == sid
                ),
            )
        with col2:
            lang_code = st.selectbox(
                "Translate to",
                options=[code for code, _ in LANGUAGES],
                format_func=lambda c: dict(LANGUAGES)[c],
            )

        submitted = st.form_submit_button("Start clone", type="primary", use_container_width=True)

    if submitted:
        if not source_url.strip():
            st.warning("Enter a URL first.")
        else:
            try:
                job_id = client.start_clone(
                    source_url=source_url.strip(),
                    store=store,
                    target_language=lang_code or None,
                )
            except PageClonerUnavailable as e:
                st.error(str(e))
                return
            except PageClonerError as e:
                st.error(f"Page cloner refused the job: {e}")
                return

            st.session_state["active_job_id"] = job_id
            st.rerun()

    st.markdown("---")
    _render_recent_jobs(client)


main()
