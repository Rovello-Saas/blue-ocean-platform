"""
Home Page — Dashboard overview with key metrics.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st


def main():
    """Main dashboard page — Overview."""

    st.title("Qoveliqo Ads")
    st.caption("AI-Driven Product Research & Google Ads Automation")

    # Check for notifications
    try:
        from src.sheets.manager import get_data_store
        store = get_data_store()
        notifications = store.get_notifications(unread_only=True, limit=5)

        if notifications:
            with st.sidebar:
                st.markdown(f"### Notifications ({len(notifications)})")
                for n in notifications[:5]:
                    icon = {"success": "✅", "warning": "⚠️", "error": "❌", "info": "ℹ️"}.get(n.level, "ℹ️")
                    with st.expander(f"{icon} {n.title}", expanded=False):
                        st.write(n.message)
                        st.caption(n.timestamp[:19])
                        if st.button("Mark read", key=f"read_{n.notification_id}"):
                            store.mark_notification_read(n.notification_id)
                            st.rerun()

        # Overview metrics
        st.markdown("---")
        st.subheader("Overview")

        products = store.get_products()

        # Count by status
        status_counts = {}
        total_revenue = 0
        total_spend = 0
        total_profit = 0

        for p in products:
            status_counts[p.test_status] = status_counts.get(p.test_status, 0) + 1
            total_revenue += float(p.revenue or 0)
            total_spend += float(p.spend or 0)
            total_profit += float(p.net_profit or 0)

        # Top-level metrics
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric("Total Products", len(products))
        with col2:
            st.metric("Active Testing", status_counts.get("testing", 0))
        with col3:
            st.metric("Winners", status_counts.get("winner", 0))
        with col4:
            overall_roas = round(total_revenue / total_spend, 2) if total_spend > 0 else 0
            st.metric("Overall ROAS", f"{overall_roas:.2f}")
        with col5:
            st.metric("Net Profit", f"€{total_profit:,.2f}")

        st.markdown("---")

        # Status breakdown
        st.subheader("Products by Status")

        status_info = [
            ("discovered", "Discovered", "🔍"),
            ("sourcing", "Awaiting Agent", "📦"),
            ("ready_to_test", "Ready to Test", "🚀"),
            ("testing", "Testing", "🧪"),
            ("winner", "Winners", "🏆"),
            ("killed", "Killed", "💀"),
            ("paused", "Paused", "⏸️"),
            ("rejected", "Rejected", "❌"),
        ]

        cols = st.columns(4)
        for i, (status, label, icon) in enumerate(status_info):
            with cols[i % 4]:
                count = status_counts.get(status, 0)
                st.metric(f"{icon} {label}", count)

        # Recent actions
        st.markdown("---")
        st.subheader("Recent Actions")

        logs = store.get_logs(limit=10)
        if logs:
            for log in logs:
                icon = {
                    "product_killed": "💀",
                    "product_winner": "🏆",
                    "budget_scaled": "📈",
                    "product_paused": "⏸️",
                    "economics_passed": "✅",
                    "economics_failed": "❌",
                    "listing_created": "🛍️",
                    "sourcing_started": "📦",
                    "price_alert": "💰",
                    "stock_alert": "📦",
                }.get(log.action_type, "📋")

                with st.container():
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.markdown(f"{icon} **{log.action_type.replace('_', ' ').title()}** — {log.reason[:100]}")
                    with col2:
                        st.caption(log.timestamp[:19])
        else:
            st.info("No actions recorded yet. Start product discovery to find products.")

        # Quick actions
        st.markdown("---")
        st.subheader("Quick Actions")

        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("🔬 Discover Products", use_container_width=True):
                with st.spinner("Running product discovery..."):
                    from src.research.pipeline import ResearchPipeline
                    pipeline = ResearchPipeline(store)
                    stats = pipeline.run_for_all_countries()
                    st.success(f"Discovery complete! {sum(s.get('products_added_to_sourcing', 0) for s in stats)} new products found.")
                    st.rerun()

        with col2:
            if st.button("📊 Pull Performance", use_container_width=True):
                with st.spinner("Pulling performance data..."):
                    from src.scheduler.jobs import JobScheduler
                    scheduler = JobScheduler(store)
                    scheduler.job_pull_performance()
                    st.success("Performance data updated!")
                    st.rerun()

        with col3:
            if st.button("🤖 Run Decisions", use_container_width=True):
                with st.spinner("Running decision engine..."):
                    from src.decisions.engine import DecisionEngine
                    engine = DecisionEngine(store)
                    results = engine.evaluate_all_products()
                    st.success(f"Decision engine complete! {len(results)} actions taken.")
                    st.rerun()

    except Exception as e:
        st.warning(
            "Dashboard could not connect to Google Sheets. "
            "Please configure your credentials in the .env file."
        )
        st.error(f"Error: {e}")

        st.markdown("---")
        st.markdown("### Getting Started")
        st.markdown("""
        1. Copy `.env.example` to `.env`
        2. Fill in your API credentials
        3. Create a Google Sheet and add its ID to `.env`
        4. Restart the dashboard
        """)


main()
