"""
Home — unified Movanella platform cockpit.

Two halves:
  1. "[+ New product]" card picker at the top. Two workflow cards that jump
     into the Clone or Research flow via `st.switch_page`. This is the
     primary entry point for new work; the research dashboard below is
     secondary context.
  2. Research pipeline dashboard (preserved from the prior dashboard home).
     Shows product status counts + totals for the Movanella/Google side.
     Only renders if the Sheet is reachable; a stale Sheet shouldn't make
     the landing page error out.

Design intent: you arrive here, you see both workflows at equal weight,
you pick one. The dashboard is there because the research pipeline already
produces real data worth looking at — not because it's the main event.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st


# Routes target files inside dashboard/views/. Keep in sync with the names
# registered in dashboard/app.py — Streamlit validates these at switch time
# and raises a friendly error if they don't match.
ROUTE_CLONE    = "views/_clone.py"
ROUTE_RESEARCH = "views/2_Research.py"


def _render_workflow_picker() -> None:
    """Top-of-page: two cards for the two ways to start a new product."""
    st.markdown("### Add a new product")
    st.caption("Pick the workflow that matches what you have on hand.")

    left, right = st.columns(2, gap="large")

    with left:
        with st.container(border=True):
            st.markdown("#### 🔗 I have a competitor URL")
            st.write(
                "Clone a competitor's product page end-to-end — scrape, "
                "translate, generate a Shopify listing, import reviews."
            )
            st.caption("Used for Meta-style launches (e.g. Merivalo).")
            if st.button(
                "Start clone workflow",
                key="wf_clone",
                type="primary",
                use_container_width=True,
            ):
                st.switch_page(ROUTE_CLONE)

    with right:
        with st.container(border=True):
            st.markdown("#### 🔬 I have a keyword or niche")
            st.write(
                "Run keyword research → AliExpress sourcing → AI content "
                "generation → Shopify listing → Google Ads test campaign."
            )
            st.caption("Used for Google Shopping / PMax launches (e.g. Movanella).")
            if st.button(
                "Start research workflow",
                key="wf_research",
                use_container_width=True,
            ):
                st.switch_page(ROUTE_RESEARCH)


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


def _render_research_dashboard(store) -> None:
    """Research-pipeline metrics (preserved from the old home page)."""
    try:
        products = store.get_products()
    except Exception as e:
        st.warning(f"Couldn't load products from Sheet: {e}")
        return

    # Aggregate once; reuse for both the top-level metrics and the status grid.
    status_counts: dict[str, int] = {}
    total_revenue = 0.0
    total_spend = 0.0
    total_profit = 0.0

    for p in products:
        status_counts[p.test_status] = status_counts.get(p.test_status, 0) + 1
        total_revenue += float(p.revenue or 0)
        total_spend += float(p.spend or 0)
        total_profit += float(p.net_profit or 0)

    st.markdown("### Research pipeline")
    st.caption("Movanella / Google Ads side — keyword → sourcing → testing.")

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total products",  len(products))
    col2.metric("Active testing",  status_counts.get("testing", 0))
    col3.metric("Winners",         status_counts.get("winner", 0))

    overall_roas = round(total_revenue / total_spend, 2) if total_spend > 0 else 0
    col4.metric("Overall ROAS",    f"{overall_roas:.2f}")
    col5.metric("Net profit",      f"€{total_profit:,.2f}")

    st.markdown("#### Products by status")
    status_info = [
        ("discovered",     "Discovered",      "🔍"),
        ("sourcing",       "Awaiting agent",  "📦"),
        ("ready_to_test",  "Ready to test",   "🚀"),
        ("testing",        "Testing",         "🧪"),
        ("winner",         "Winners",         "🏆"),
        ("killed",         "Killed",          "💀"),
        ("paused",         "Paused",          "⏸️"),
        ("rejected",       "Rejected",        "❌"),
    ]

    cols = st.columns(4)
    for i, (status, label, icon) in enumerate(status_info):
        with cols[i % 4]:
            st.metric(f"{icon} {label}", status_counts.get(status, 0))


def _render_quick_actions(store) -> None:
    """Shortcuts for common research/ads jobs. One-shot buttons."""
    st.markdown("#### Quick actions")
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("🔬 Discover products", use_container_width=True):
            with st.spinner("Running product discovery…"):
                from src.research.pipeline import ResearchPipeline
                pipeline = ResearchPipeline(store)
                stats = pipeline.run_for_all_countries()
                added = sum(s.get("products_added_to_sourcing", 0) for s in stats)
                st.success(f"Discovery complete — {added} new products found.")
                st.rerun()

    with col2:
        if st.button("📊 Pull performance", use_container_width=True):
            with st.spinner("Pulling performance data…"):
                from src.scheduler.jobs import JobScheduler
                scheduler = JobScheduler(store)
                scheduler.job_pull_performance()
                st.success("Performance data updated.")
                st.rerun()

    with col3:
        if st.button("🤖 Run decisions", use_container_width=True):
            with st.spinner("Running decision engine…"):
                from src.decisions.engine import DecisionEngine
                engine = DecisionEngine(store)
                results = engine.evaluate_all_products()
                st.success(f"Decision engine complete — {len(results)} actions taken.")
                st.rerun()


def main() -> None:
    st.title("Blue Ocean Platform")
    st.caption("Unified commerce cockpit — Movanella (Google) + Merivalo (Meta).")

    # 1. Workflow picker is the primary CTA and always renders, even if the
    #    Sheet is down. You should always be able to start a clone.
    _render_workflow_picker()
    st.markdown("---")

    # 2. Research dashboard depends on the Sheet. Degrade gracefully if it's
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
    _render_research_dashboard(store)
    st.markdown("---")
    _render_quick_actions(store)


main()
