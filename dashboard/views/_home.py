"""
Home — unified Blue Ocean Platform cockpit.

Start-here page for both of the stores this platform operates:
  • 🇺🇸 Movanella  — dropshipping pipeline (keyword → AliExpress → Google Ads)
  • 🇩🇪 Merivalo   — page cloner (clone competitor URL → Meta Ads)

These are genuinely different workflows, not two views of the same data, so
forcing them into a single funnel would mislead. Instead: you pick the site
first, and the rest of the page reshapes to show only the stuff that matters
for *that* store. The sidebar nav is always there when you want to dive into
a specific tool (Products, Performance, etc.).

Layout:
  1. Site picker (segmented control, session-state backed).
  2. Contextual hero — the primary CTA for the selected site.
  3. Status pane — funnel + KPIs for Movanella, recent clone jobs for Merivalo.
  4. Blockers — items that need a human decision, regardless of site.
  5. Quick actions — shortcuts for the currently selected site.

Design intent: landing here should answer "what do I do next?" without you
having to hunt across five tabs to find the right entry point.
"""

import os
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st


# Routes target files inside dashboard/views/. Keep in sync with the names
# registered in dashboard/app.py — Streamlit validates these at switch time
# and raises a friendly error if they don't match.
ROUTE_CLONE          = "views/_clone.py"
ROUTE_RESEARCH       = "views/2_Research.py"
ROUTE_PRODUCTS       = "views/3_Products.py"
ROUTE_PERF           = "views/4_Performance.py"
ROUTE_MANUAL_REVIEW  = "views/_manual_review.py"


# Sites the platform supports. Edit here and the picker + routing update in
# one place. `id` is the internal key used in session state + for matching
# against page-cloner storeId and (eventually) future per-site scoping.
SITES = [
    {"id": "all",       "name": "All sites",         "flag": "🌐", "channel": ""},
    {"id": "movanella", "name": "Movanella",         "flag": "🇺🇸", "channel": "Google Ads"},
    {"id": "merivalo",  "name": "Merivalo",          "flag": "🇩🇪", "channel": "Meta Ads"},
]


# ---------------------------------------------------------------------------
# Clone-page availability
# ---------------------------------------------------------------------------

def _cloud_mode_enabled() -> bool:
    return os.getenv("BLUE_OCEAN_CLOUD_MODE", "").strip().lower() in {"1", "true", "yes"}


def _clone_page_enabled() -> bool:
    """
    Local runs can use the laptop Node service. Hosted Streamlit runs need a
    public PAGE_CLONER_URL, otherwise switching to the Clone page would fail.
    """
    from src.core.config import PAGE_CLONER_URL

    if not _cloud_mode_enabled():
        return True

    parsed = urlparse((PAGE_CLONER_URL or "").strip())
    return bool(parsed.hostname and parsed.hostname not in {"localhost", "127.0.0.1", "::1"})


def _clone_unavailable_caption() -> str:
    return (
        "Page cloning is ready in the platform UI, but the cloud app still needs "
        "a public Page Cloner URL before it can run clones."
    )


def _start_clone_for(site: str) -> None:
    if not _clone_page_enabled():
        st.info(_clone_unavailable_caption())
        return
    st.session_state["clone_preselected_store"] = site
    st.switch_page(ROUTE_CLONE)


# ---------------------------------------------------------------------------
# Site picker
# ---------------------------------------------------------------------------

def _site_picker() -> str:
    """
    Top-of-page segmented control. Persists choice in session state so the
    selection survives reruns (metric clicks, quick-action buttons, etc.).
    Returns the active site id: "all" | "movanella" | "merivalo".
    """
    # Initialise once on first render. Default "all" shows the picker in its
    # neutral state — user explicitly picks to narrow the view.
    if "active_site" not in st.session_state:
        st.session_state.active_site = "all"

    labels = {s["id"]: f"{s['flag']} {s['name']}" for s in SITES}
    choice = st.segmented_control(
        "Active site",
        options=[s["id"] for s in SITES],
        format_func=lambda sid: labels[sid],
        default=st.session_state.active_site,
        key="site_picker",
        label_visibility="collapsed",
    )

    # `st.segmented_control` returns None if the user clicks the active pill
    # to deselect. Treat that as a no-op — keep the previous selection so the
    # page doesn't blank out on accidental clicks.
    if choice:
        st.session_state.active_site = choice
    return st.session_state.active_site


# ---------------------------------------------------------------------------
# Contextual heroes — one per site
# ---------------------------------------------------------------------------

def _movanella_hero() -> None:
    """
    Movanella = research-led pipeline. Primary CTA is 'start research', but
    we also surface a secondary 'Clone a page' so the user can skip the
    research workflow when they already have a competitor URL in mind.

    Why both: the research pipeline is the right entry point for "I want to
    find products to sell". But sometimes you've seen a winning ad in the
    wild and just want to ship a Shopify clone of it — no keyword discovery,
    no AliExpress matching needed. Forcing that flow through Research adds
    pointless friction. The page-cloner already supports Movanella as a
    target store (see `_clone.py` → STORES), so wiring the CTA here is
    purely a UI addition.
    """
    with st.container(border=True):
        left, right = st.columns([3, 1], vertical_alignment="center")
        with left:
            st.markdown("#### 🔬 Movanella — research pipeline")
            st.caption(
                "Keyword research → AliExpress sourcing → AI content → Shopify → "
                "Google Shopping / PMax test campaign. Starts here."
            )
        with right:
            if st.button(
                "Start research",
                type="primary",
                use_container_width=True,
                key="hero_movanella",
            ):
                st.switch_page(ROUTE_RESEARCH)

    with st.container(border=True):
        left, right = st.columns([3, 1], vertical_alignment="center")
        with left:
            st.markdown("#### 🔗 Movanella — page cloner")
            st.caption(
                "Paste a competitor product URL and publish a Shopify clone "
                "directly to Movanella — no keyword research step."
            )
        with right:
            if st.button(
                "Clone a page",
                type="primary",
                use_container_width=True,
                key="hero_movanella_clone",
                help="Paste a competitor URL and publish it to Movanella — no research step.",
                disabled=not _clone_page_enabled(),
            ):
                _start_clone_for("movanella")
            if not _clone_page_enabled():
                st.caption(_clone_unavailable_caption())


def _merivalo_hero() -> None:
    """Merivalo = clone-led pipeline. Primary CTA is 'clone a competitor URL'."""
    with st.container(border=True):
        left, right = st.columns([3, 1], vertical_alignment="center")
        with left:
            st.markdown("#### 🔗 Merivalo — page cloner")
            st.caption(
                "Paste a competitor product URL and we scrape, translate to "
                "German (du-form), and publish to Shopify — ready for Meta Ads."
            )
        with right:
            if st.button(
                "Clone a page",
                type="primary",
                use_container_width=True,
                key="hero_merivalo",
                disabled=not _clone_page_enabled(),
            ):
                _start_clone_for("merivalo")
            if not _clone_page_enabled():
                st.caption(_clone_unavailable_caption())


def _all_sites_hero() -> None:
    """Neutral view: both CTAs side-by-side so you can pick either one."""
    left, right = st.columns(2, gap="medium")
    with left:
        _movanella_hero()
    with right:
        _merivalo_hero()


# ---------------------------------------------------------------------------
# Movanella status pane — funnel + KPIs
# ---------------------------------------------------------------------------

def _render_movanella_pipeline(store) -> None:
    """Funnel hero + KPI row for the research/Google-Ads pipeline."""
    try:
        products = store.get_products()
    except Exception as e:
        st.warning(f"Couldn't load products from Sheet: {e}")
        return

    # Keywords are fetched best-effort: the funnel is still readable even if
    # the keywords tab is empty / unreadable. Don't blow up the whole hero.
    try:
        keywords = store.get_keywords()
    except Exception:
        keywords = []

    status_counts: dict[str, int] = {}
    total_revenue = 0.0
    total_spend = 0.0
    total_profit = 0.0
    for p in products:
        status_counts[p.test_status] = status_counts.get(p.test_status, 0) + 1
        total_revenue += float(p.revenue or 0)
        total_spend   += float(p.spend or 0)
        total_profit  += float(p.net_profit or 0)

    # --- Hero: pipeline funnel ----------------------------------------------
    from dashboard.components.widgets import compute_funnel_counts, pipeline_funnel

    st.markdown("### Pipeline funnel")
    st.caption(
        "Left → right: how many keywords became a live ad. Each bar is a "
        "subset of the one before — the drop-off shows where the pipeline leaks."
    )
    pipeline_funnel(compute_funnel_counts(keywords, products))

    # --- KPI row ------------------------------------------------------------
    st.markdown("### Business metrics")
    st.caption("Movanella / Google Ads — rolling totals across all active tests.")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total products", len(products))
    c2.metric("Active testing", status_counts.get("testing", 0))
    c3.metric("Winners",        status_counts.get("winner", 0))
    overall_roas = round(total_revenue / total_spend, 2) if total_spend > 0 else 0
    c4.metric("Overall ROAS",   f"{overall_roas:.2f}")
    c5.metric("Net profit",     f"€{total_profit:,.2f}")

    # --- Status grid (detail, collapsed by default) -------------------------
    with st.expander("Products by status — full breakdown", expanded=False):
        status_info = [
            ("discovered",      "Discovered",      "🔍"),
            ("sourcing",        "Awaiting agent",  "📦"),
            ("ready_to_test",   "Ready to test",   "🚀"),
            ("listing_created", "Listing created", "🛍️"),
            ("testing",         "Testing",         "🧪"),
            ("winner",          "Winners",         "🏆"),
            ("scaling",         "Scaling",         "📈"),
            ("paused",          "Paused",          "⏸️"),
            ("killed",          "Killed",          "💀"),
            ("rejected",        "Rejected",        "❌"),
        ]
        cols = st.columns(5)
        for i, (status, label, icon) in enumerate(status_info):
            with cols[i % 5]:
                st.metric(f"{icon} {label}", status_counts.get(status, 0))


# ---------------------------------------------------------------------------
# Merivalo status pane — recent clone jobs
# ---------------------------------------------------------------------------

def _render_clone_pipeline(site: str) -> None:
    """
    For clone-led flows the 'pipeline' is the list of page-cloner jobs. Show
    health + recent clones inline so the user doesn't have to jump to the
    Clone page just to see status.
    """
    from src.page_cloner import PageClonerClient

    client = PageClonerClient()
    site_name = "Movanella" if site == "movanella" else "Merivalo"

    st.markdown("### Page cloner status")

    if not _clone_page_enabled():
        st.warning(_clone_unavailable_caption())
        return

    if not client.health_check():
        st.error(
            f"Page cloner is unreachable at `{client.base_url}`. "
            "Start it with `cd page-cloner && node server.js`."
        )
        return

    st.caption(f"Connected to `{client.base_url}` ✓")
    if st.button(
        f"Clone a page for {site_name}",
        type="primary",
        key=f"status_clone_{site}",
        use_container_width=True,
    ):
        _start_clone_for(site)

    # Job list is best-effort — the detail page has richer controls.
    try:
        import requests
        r = requests.get(f"{client.base_url}/api/jobs", timeout=5)
        jobs = r.json() if r.ok else []
    except Exception:
        jobs = []

    if site in ("movanella", "merivalo"):
        jobs = [j for j in jobs if j.get("storeId") == site]

    if not jobs:
        st.info("No clone jobs yet. Paste a competitor URL above to start the first one.")
        return

    # Quick totals so you don't have to eyeball the list.
    done    = sum(1 for j in jobs if j.get("status") == "done")
    failed  = sum(1 for j in jobs if j.get("status") == "failed")
    running = sum(1 for j in jobs if j.get("status") not in ("done", "failed"))
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total jobs", len(jobs))
    c2.metric("✅ Done",    done)
    c3.metric("⏳ Running", running)
    c4.metric("❌ Failed",  failed)

    st.markdown("#### Recent clones")
    for j in jobs[:8]:
        status = j.get("status", "")
        icon   = {"done": "✅", "failed": "❌"}.get(status, "⏳")
        url    = (j.get("url") or "").replace("https://", "")
        store  = j.get("storeId", "?")
        # Compact one-liner — full detail is on the Clone page.
        st.markdown(
            f"{icon} **{status}** · `{store}` · "
            f"{url[:70]}{'…' if len(url) > 70 else ''}"
        )


# ---------------------------------------------------------------------------
# Blockers — products that need a human decision
# ---------------------------------------------------------------------------

def _render_blockers(store) -> None:
    """
    Items the pipeline can't advance on its own: agent hasn't sent cost yet,
    or the product is paused waiting on a call.

    When many products share the same blocker (typical: "40 items waiting on
    the sourcing agent"), rendering one card per product buries real signal
    under a wall of identical rows. We instead group by (title, hint) and
    render one summary card per group, with the first few product names
    inlined as a sampler. Singletons keep their original detailed card so
    individual paused / ready-to-list items still get a direct "Open" link
    to the specific product.
    """
    try:
        products = store.get_products()
    except Exception:
        return

    # Each blocker is (product, title, hint, group_key). group_key drives
    # aggregation; we keep it separate from the human-readable title so a
    # future "kill all" or "filter Products tab" CTA can key off it.
    blockers: list[tuple[object, str, str, str]] = []
    for p in products:
        if p.test_status == "pending_manual_review":
            # The AliExpress auto-matcher couldn't find a DS-feed supplier
            # match, so a human (user or partner) needs to look the product
            # up on AliExpress and fill in the real landed cost. Routed to
            # its dedicated Manual Review page, not Products.
            blockers.append(
                (p, "🔎 Needs manual AliExpress lookup",
                 "Search AliExpress, paste the landed cost — economics fires on submit.",
                 "manual_review")
            )
        elif p.test_status == "sourcing" and not float(p.landed_cost or 0):
            blockers.append(
                (p, "⏳ Waiting on cost from sourcing agent",
                 "Agent should fill in landed_cost; pipeline then auto-validates.",
                 "sourcing_no_cost")
            )
        elif p.test_status == "ready_to_test" and not p.shopify_product_id:
            blockers.append(
                (p, "🛍️ Ready to list — not yet in Shopify",
                 "Scheduler will pick this up on the next listing job.",
                 "ready_to_list")
            )
        elif p.test_status == "paused":
            blockers.append(
                (p, "⏸️ Paused — needs a call",
                 "Decide whether to kill, resume, or swap creative.",
                 "paused")
            )

    if not blockers:
        return  # No chrome if nothing to show — keep the page calm.

    # Bucket by group_key, preserving insertion order so the first blocker of
    # each kind keeps its position in the visual stack.
    groups: dict[str, list[tuple[object, str, str, str]]] = {}
    for b in blockers:
        groups.setdefault(b[3], []).append(b)

    st.markdown(f"### ⚠️ Needs attention ({len(blockers)})")

    for group_key, items in groups.items():
        _, title, hint, _ = items[0]
        countries = sorted({(p.country or "—") for p, *_ in items})

        # Collapse: one summary card for the whole group when 2+ items share
        # the same blocker. The typical case on a fresh batch is ALL 40
        # sourcing items — showing 40 near-identical rows adds noise, not
        # information. A sampler of up to 3 names + count is enough to make
        # the group tangible without being a second Products page.
        if len(items) >= 2:
            sample = [(p.keyword or p.product_id) for p, *_ in items[:3]]
            more = len(items) - len(sample)
            sample_line = ", ".join(f"*{s}*" for s in sample)
            if more > 0:
                sample_line += f", +{more} more"

            with st.container(border=True):
                c1, c2 = st.columns([4, 1], vertical_alignment="center")
                with c1:
                    st.markdown(f"**{title} — {len(items)} products**")
                    st.caption(
                        f"{hint}  ·  countries: "
                        f"{', '.join(f'`{c}`' for c in countries)}"
                    )
                    st.caption(sample_line)
                with c2:
                    if st.button(
                        f"Open all ({len(items)})",
                        key=f"blocker_group_{group_key}",
                        use_container_width=True,
                    ):
                        # Manual-review items live on their own page — the
                        # Products kanban shows them too but the form for
                        # entering landed_cost is only on /Manual Review.
                        if group_key == "manual_review":
                            st.switch_page(ROUTE_MANUAL_REVIEW)
                        else:
                            # Drop a filter hint for the Products page to pick up
                            # when/if it starts honouring it.
                            st.session_state["products_filter_status"] = (
                                items[0][0].test_status
                            )
                            st.switch_page(ROUTE_PRODUCTS)
            continue

        # Singleton — keep the detailed per-product card with a direct link
        # so unique items (one paused product, one stuck listing) still get
        # handled individually.
        p, title, hint, _ = items[0]
        with st.container(border=True):
            c1, c2 = st.columns([4, 1], vertical_alignment="center")
            with c1:
                st.markdown(f"**{p.keyword or p.product_id}** — {title}")
                st.caption(
                    f"{hint}  ·  country `{p.country}`  ·  "
                    f"last updated {(p.updated_at or '')[:10] or '—'}"
                )
            with c2:
                if st.button(
                    "Open",
                    key=f"blocker_{p.product_id}",
                    use_container_width=True,
                ):
                    # Manual-review has its own page; everything else is
                    # actionable from the Products drawer.
                    if p.test_status == "pending_manual_review":
                        st.switch_page(ROUTE_MANUAL_REVIEW)
                    else:
                        st.session_state["focus_product_id"] = p.product_id
                        st.switch_page(ROUTE_PRODUCTS)


# ---------------------------------------------------------------------------
# Quick actions — contextual
# ---------------------------------------------------------------------------

def _render_quick_actions(store, site: str) -> None:
    """One-shot buttons for jobs the user runs often. Scoped to the picked site."""
    # Merivalo has nothing to schedule — it's all on-demand clones. Hide the
    # section rather than showing three disabled buttons for no reason.
    if site == "merivalo":
        return

    st.markdown("#### Quick actions")
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("🔬 Discover products", use_container_width=True, key="qa_discover"):
            with st.spinner("Running product discovery…"):
                from src.research.pipeline import ResearchPipeline
                pipeline = ResearchPipeline(store)
                stats = pipeline.run_for_all_countries()
                # Count both lanes: matched Products AND unmatched inbox
                # rows. Matched-only was misleading when every survivor
                # went to the inbox for manual review.
                sourcing = sum(s.get("products_added_to_sourcing", 0) for s in stats)
                inbox = sum(s.get("keywords_written_to_inbox_only", 0) for s in stats)
                if sourcing + inbox == 0:
                    st.success("Discovery complete — no new keywords made it through the filters.")
                else:
                    st.success(
                        f"Discovery complete — {sourcing} to Sourcing, {inbox} to Inbox."
                    )
                st.rerun()

    with col2:
        if st.button("📊 Pull performance", use_container_width=True, key="qa_perf"):
            with st.spinner("Pulling performance data…"):
                from src.scheduler.jobs import JobScheduler
                scheduler = JobScheduler(store)
                scheduler.job_pull_performance()
                st.success("Performance data updated.")
                st.rerun()

    with col3:
        if st.button("🤖 Run decisions", use_container_width=True, key="qa_decisions"):
            with st.spinner("Running decision engine…"):
                from src.decisions.engine import DecisionEngine
                engine = DecisionEngine(store)
                results = engine.evaluate_all_products()
                st.success(f"Decision engine complete — {len(results)} actions taken.")
                st.rerun()


# ---------------------------------------------------------------------------
# Sidebar notifications — unchanged from prior design
# ---------------------------------------------------------------------------

def _render_sidebar_notifications(store) -> None:
    """Surface unread notifications in the sidebar. Silent if none."""
    try:
        notifications = store.get_notifications(unread_only=True, limit=5)
    except Exception:
        # Notifications are optional — never let them break the home page.
        return

    if not notifications:
        return

    with st.sidebar:
        st.markdown(f"### Notifications ({len(notifications)})")
        for n in notifications[:5]:
            icon = {"success": "✅", "warning": "⚠️", "error": "❌", "info": "ℹ️"}.get(
                n.level, "ℹ️"
            )
            with st.expander(f"{icon} {n.title}", expanded=False):
                st.write(n.message)
                st.caption(n.timestamp[:19])
                if st.button("Mark read", key=f"read_{n.notification_id}"):
                    store.mark_notification_read(n.notification_id)
                    st.rerun()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    st.title("Blue Ocean Platform")
    st.caption("Commerce cockpit — pick a site, see what matters for it.")

    # 1. Site picker — always the first thing. Cheap, works offline.
    site = _site_picker()
    st.markdown("")

    # 2. Contextual hero. Different per site, but always a single primary CTA.
    if site == "movanella":
        _movanella_hero()
    elif site == "merivalo":
        _merivalo_hero()
    else:
        _all_sites_hero()

    st.markdown("---")

    # 3. Merivalo doesn't depend on the Sheet — render its pane and return
    #    early. No sense trying to load the Sheet just to ignore it.
    if site == "merivalo":
        _render_clone_pipeline("merivalo")
        return

    # 4. Everything below here needs the Sheet. Degrade gracefully if it's
    #    unreachable (credentials missing, quota exhausted, etc.) rather
    #    than blowing up the landing page.
    try:
        from src.sheets.manager import get_data_store
        store = get_data_store()
    except Exception as e:
        st.info(
            "Research pipeline dashboard is unavailable — "
            "check Google Sheets credentials in `.env`."
        )
        st.caption(f"Reason: {e}")
        return

    _render_sidebar_notifications(store)

    if site == "movanella":
        _render_movanella_pipeline(store)
        st.markdown("---")
        _render_clone_pipeline("movanella")
    else:  # "all"
        # Show the Movanella pipeline (the only one with a Sheet) and then
        # the Merivalo clone jobs inline, so "All sites" is genuinely both.
        _render_movanella_pipeline(store)
        st.markdown("---")
        _render_clone_pipeline("merivalo")

    st.markdown("---")
    _render_blockers(store)
    st.markdown("---")
    _render_quick_actions(store, site)


main()
