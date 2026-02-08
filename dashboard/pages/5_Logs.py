"""
Logs & Notifications Page — Audit trail and notification center.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd



def main():
    st.title("📋 Logs & Notifications")

    try:
        from src.sheets.manager import get_data_store
        store = get_data_store()
    except Exception as e:
        st.error(f"Could not connect to data store: {e}")
        return

    tab1, tab2, tab3 = st.tabs([
        "🔔 Notifications", "📋 Action Log", "📊 Scheduler Status"
    ])

    # --- Notifications ---
    with tab1:
        st.subheader("Notifications")

        col1, col2 = st.columns([3, 1])
        with col2:
            show_unread = st.toggle("Unread only", value=True)
            if st.button("Mark all as read"):
                notifications = store.get_notifications(unread_only=True)
                for n in notifications:
                    store.mark_notification_read(n.notification_id)
                st.success("All notifications marked as read!")
                st.rerun()

        notifications = store.get_notifications(unread_only=show_unread, limit=50)

        if not notifications:
            st.info("No notifications" + (" (unread)" if show_unread else "") + ".")
        else:
            for n in notifications:
                icon = {
                    "success": "✅",
                    "warning": "⚠️",
                    "error": "❌",
                    "info": "ℹ️",
                }.get(n.level, "ℹ️")

                border_color = {
                    "success": "green",
                    "warning": "orange",
                    "error": "red",
                    "info": "blue",
                }.get(n.level, "gray")

                with st.container(border=True):
                    col1, col2, col3 = st.columns([1, 6, 1])

                    with col1:
                        st.markdown(f"### {icon}")

                    with col2:
                        unread_marker = " **NEW**" if not n.read else ""
                        st.markdown(f"**{n.title}**{unread_marker}")
                        st.write(n.message)
                        st.caption(f"{n.timestamp[:19]} | Product: {n.product_id if n.product_id else 'N/A'}")

                    with col3:
                        if not n.read:
                            if st.button("✓", key=f"read_{n.notification_id}"):
                                store.mark_notification_read(n.notification_id)
                                st.rerun()

    # --- Action Log ---
    with tab2:
        st.subheader("Action Log")
        st.caption("Complete audit trail of all automated decisions and actions.")

        # Filter
        col1, col2, col3 = st.columns(3)
        with col1:
            action_filter = st.selectbox(
                "Action type",
                options=["All", "product_killed", "product_winner", "budget_scaled",
                         "product_paused", "economics_passed", "economics_failed",
                         "listing_created", "sourcing_started", "price_alert",
                         "stock_alert", "product_retest"],
            )
        with col2:
            limit = st.number_input("Show entries", min_value=10, max_value=500, value=50, step=10)
        with col3:
            product_filter = st.text_input("Filter by Product ID", placeholder="e.g., abc123")

        logs = store.get_logs(
            product_id=product_filter if product_filter else None,
            limit=limit,
        )

        if action_filter != "All":
            logs = [l for l in logs if l.action_type == action_filter]

        if not logs:
            st.info("No actions logged yet.")
        else:
            # Table view
            rows = []
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
                    "product_retest": "🔄",
                }.get(log.action_type, "📋")

                rows.append({
                    "Time": log.timestamp[:19],
                    "Action": f"{icon} {log.action_type.replace('_', ' ').title()}",
                    "Product ID": log.product_id,
                    "Country": log.country,
                    "Status Change": f"{log.old_status} → {log.new_status}" if log.old_status else log.new_status,
                    "Reason": log.reason[:80],
                    "Details": log.details[:80] if log.details else "",
                })

            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, height=500)

            # Summary stats
            st.markdown("---")
            st.subheader("Action Summary")

            action_counts = {}
            for log in logs:
                action_counts[log.action_type] = action_counts.get(log.action_type, 0) + 1

            cols = st.columns(min(len(action_counts), 4))
            for i, (action, count) in enumerate(sorted(action_counts.items(), key=lambda x: x[1], reverse=True)):
                with cols[i % len(cols)]:
                    st.metric(
                        action.replace("_", " ").title(),
                        count,
                    )

    # --- Scheduler Status ---
    with tab3:
        st.subheader("Scheduler Status")
        st.caption("View the status of all automated jobs.")

        try:
            from src.scheduler.jobs import JobScheduler
            # Note: This creates a new scheduler instance just to show status
            # In production, you'd share the instance
            scheduler = JobScheduler(store)

            st.info("The scheduler runs in the background process. This shows the configured jobs and their schedules.")

            jobs = [
                {"Job": "Product Discovery", "Schedule": f"Every {scheduler.config.get('research.research_frequency_hours', 24)} hours", "Description": "Generate keywords, validate, find products"},
                {"Job": "Agent Cost Polling", "Schedule": f"Every {scheduler.config.polling_interval_minutes} minutes", "Description": "Check if agent has filled in landed costs"},
                {"Job": "Performance Data Pull", "Schedule": f"Every {scheduler.config.get('ads.performance_check_interval_hours', 2)} hours", "Description": "Pull performance data from Google Ads"},
                {"Job": "Decision Engine", "Schedule": f"Every {scheduler.config.get('ads.performance_check_interval_hours', 2)} hours", "Description": "Evaluate products: kill/maintain/scale"},
                {"Job": "Label Sync", "Schedule": "Every 2 hours", "Description": "Sync product labels to Merchant Center"},
                {"Job": "Daily Counter Update", "Schedule": "Daily at midnight", "Description": "Update days_testing, days_below_broas, etc."},
                {"Job": "Competitor Price Check", "Schedule": f"Every {scheduler.config.get('monitoring.competitor_price_frequency_days', 7)} days", "Description": "Monitor competitor pricing changes"},
                {"Job": "Stock Check", "Schedule": "Daily at 6 AM (if enabled)", "Description": "Check AliExpress product availability"},
                {"Job": "Research Feedback", "Schedule": "Weekly", "Description": "Analyze winners/losers to improve AI research"},
                {"Job": "Process Ready Products", "Schedule": "Every hour", "Description": "Generate images, content, create Shopify listings"},
            ]

            df = pd.DataFrame(jobs)
            st.dataframe(df, use_container_width=True)

        except Exception as e:
            st.error(f"Could not load scheduler status: {e}")

        # Manual triggers
        st.markdown("---")
        st.subheader("Manual Job Triggers")

        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("🔄 Poll Agent Costs Now", use_container_width=True):
                with st.spinner("Polling..."):
                    scheduler = JobScheduler(store)
                    scheduler.job_poll_agent_costs()
                    st.success("Agent cost polling complete!")

        with col2:
            if st.button("🏷️ Sync Labels Now", use_container_width=True):
                with st.spinner("Syncing labels..."):
                    scheduler = JobScheduler(store)
                    scheduler.job_sync_labels()
                    st.success("Labels synced!")

        with col3:
            if st.button("📊 Update Counters Now", use_container_width=True):
                with st.spinner("Updating..."):
                    scheduler = JobScheduler(store)
                    scheduler.job_update_daily_counters()
                    st.success("Counters updated!")


main()
