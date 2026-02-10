"""
Products Page — Product pipeline view with all statuses.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd
from datetime import datetime


def main():
    st.title("📦 Products")

    try:
        from src.sheets.manager import get_data_store
        from src.core.config import AppConfig
        from src.core.models import ProductStatus
        from dashboard.components.widgets import (
            products_dataframe, status_badge, economics_summary
        )

        store = get_data_store()
        config = AppConfig()

    except Exception as e:
        st.error(f"Could not connect to data store: {e}")
        return

    # Load all products
    all_products = store.get_products()

    if not all_products:
        st.info("No products yet. Start product discovery to find new opportunities.")
        return

    # --- Filters ---
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        status_options = ["All"] + [s.value for s in ProductStatus]
        filter_status = st.selectbox("Status", options=status_options)

    with col2:
        countries = list(set(p.country for p in all_products))
        filter_country = st.selectbox("Country", options=["All"] + sorted(countries))

    with col3:
        sort_options = {
            "Newest First": lambda p: p.created_at,
            "ROAS (High)": lambda p: float(p.roas or 0),
            "Spend (High)": lambda p: float(p.spend or 0),
            "Profit (High)": lambda p: float(p.net_profit or 0),
            "Margin (High)": lambda p: float(p.net_margin_pct or 0),
        }
        sort_by = st.selectbox("Sort by", options=list(sort_options.keys()))

    with col4:
        search = st.text_input("Search keyword", placeholder="Filter by keyword...")

    # Apply filters
    filtered = all_products
    if filter_status != "All":
        filtered = [p for p in filtered if p.test_status == filter_status]
    if filter_country != "All":
        filtered = [p for p in filtered if p.country == filter_country]
    if search:
        filtered = [p for p in filtered if search.lower() in p.keyword.lower()]

    # Sort
    sort_key = sort_options[sort_by]
    reverse = sort_by != "Newest First"
    filtered.sort(key=sort_key, reverse=True)

    # --- Summary Metrics ---
    st.markdown("---")

    status_counts = {}
    for p in filtered:
        status_counts[p.test_status] = status_counts.get(p.test_status, 0) + 1

    cols = st.columns(6)
    status_display = [
        ("sourcing", "📦 Sourcing"),
        ("ready_to_test", "🚀 Ready"),
        ("testing", "🧪 Testing"),
        ("winner", "🏆 Winners"),
        ("killed", "💀 Killed"),
        ("paused", "⏸️ Paused"),
    ]
    for i, (status, label) in enumerate(status_display):
        with cols[i]:
            st.metric(label, status_counts.get(status, 0))

    st.caption(f"Showing {len(filtered)} of {len(all_products)} products")

    # --- Product Table ---
    st.markdown("---")

    df = products_dataframe(filtered)
    if not df.empty:
        st.dataframe(
            df,
            use_container_width=True,
            height=400,
            column_config={
                "Status": st.column_config.TextColumn(
                    "Status", width="small",
                    help="Product lifecycle status: Sourcing → Ready → Testing → Winner/Killed",
                ),
                "Keyword": st.column_config.TextColumn(
                    "Keyword",
                    help="The product search term this listing is based on",
                ),
                "Country": st.column_config.TextColumn(
                    "Country", width="small",
                    help="Target market country",
                ),
                "Selling Price": st.column_config.TextColumn(
                    "Selling Price",
                    help="Your store selling price, based on median competitor price",
                ),
                "Landed Cost": st.column_config.TextColumn(
                    "Landed Cost",
                    help="Total cost to deliver to customer (product + shipping), as reported by your sourcing agent",
                ),
                "Margin %": st.column_config.TextColumn(
                    "Margin %",
                    help="Net margin after product cost, transaction fees, and payment processing fees",
                ),
                "bROAS": st.column_config.TextColumn(
                    "bROAS",
                    help="Break-even Return on Ad Spend. You need at least this ROAS to not lose money on ads.",
                ),
                "Est. CPC": st.column_config.TextColumn(
                    "Est. CPC",
                    help="Estimated Cost Per Click from Google Keyword Planner",
                ),
                "Max CPC": st.column_config.TextColumn(
                    "Max CPC",
                    help="Maximum CPC you can afford based on margins and conversion rate. If Est. CPC > Max CPC, the product may not be profitable.",
                ),
                "Competitors": st.column_config.NumberColumn(
                    "Competitors",
                    help="Number of unique sellers in Google Shopping results",
                ),
                "Diff. Score": st.column_config.TextColumn(
                    "Diff. Score",
                    help="Differentiation Score (0-100). HIGH = competitors sell same product (easy to stand out). LOW = diverse products.",
                ),
                "🔍 Competitors": st.column_config.LinkColumn(
                    "🔍 Competitors",
                    help="Click to view competitor listings on Google Shopping",
                    display_text="View",
                ),
                "🛒 AliExpress": st.column_config.LinkColumn(
                    "🛒 AliExpress",
                    help="Click to view/search this product on AliExpress",
                    display_text="View",
                ),
                "Clicks": st.column_config.NumberColumn(
                    "Clicks",
                    help="Total ad clicks from Google Ads",
                ),
                "Spend": st.column_config.TextColumn(
                    "Spend",
                    help="Total ad spend in Google Ads",
                ),
                "Conv.": st.column_config.NumberColumn(
                    "Conv.",
                    help="Number of purchases (conversions) from Google Ads",
                ),
                "Revenue": st.column_config.TextColumn(
                    "Revenue",
                    help="Total revenue generated from Google Ads conversions",
                ),
                "ROAS": st.column_config.TextColumn(
                    "ROAS", width="small",
                    help="Return on Ad Spend = Revenue / Spend. Higher is better. Must be above bROAS to be profitable.",
                ),
                "Net Profit": st.column_config.TextColumn(
                    "Profit", width="small",
                    help="Net profit after subtracting product costs, fees, and ad spend from revenue",
                ),
                "Days": st.column_config.NumberColumn(
                    "Days",
                    help="Number of days this product has been testing in Google Ads",
                ),
                "Reason": st.column_config.TextColumn(
                    "Reason",
                    help="Explanation for the last automated action taken on this product",
                ),
                "ID": st.column_config.TextColumn(
                    "ID", width="small",
                    help="Unique product identifier",
                ),
            }
        )

    # --- Product Detail View ---
    st.markdown("---")
    st.subheader("Product Details")

    product_options = [f"{p.keyword} ({p.country}) — {status_badge(p.test_status)}" for p in filtered]
    if not product_options:
        st.info("No products match the current filters.")
        return

    selected_idx = st.selectbox(
        "Select a product",
        options=range(len(product_options)),
        format_func=lambda i: product_options[i],
    )

    product = filtered[selected_idx]

    # Detail tabs
    detail_tab1, detail_tab2, detail_tab3, detail_tab4 = st.tabs([
        "📊 Economics", "📈 Performance", "📋 History", "⚡ Actions"
    ])

    with detail_tab1:
        economics_summary(product)

        st.markdown("---")
        st.markdown("**Research Data**")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Search Volume", f"{product.monthly_search_volume:,}")
            st.metric("Estimated CPC", f"€{float(product.estimated_cpc or 0):.2f}")
        with col2:
            st.metric("Competitors", product.competitor_count)
            st.metric("Differentiation", f"{float(product.differentiation_score or 0):.0f}/100")
        with col3:
            st.metric("Competition Type", str(product.competition_type).replace("_", " ").title())
            st.metric("AliExpress Price", f"€{float(product.aliexpress_price or 0):.2f}")

        # Links section
        st.markdown("---")
        st.markdown("**Links**")
        link_cols = st.columns(3)
        with link_cols[0]:
            if product.google_shopping_url:
                st.markdown(f"🔍 [Google Shopping]({product.google_shopping_url})")
            else:
                encoded_kw = product.keyword.replace(" ", "+")
                gs_url = f"https://google.de/search?tbm=shop&q={encoded_kw}"
                st.markdown(f"🔍 [Google Shopping]({gs_url})")
        with link_cols[1]:
            if product.aliexpress_url:
                st.markdown(f"🛒 [AliExpress]({product.aliexpress_url})")
            else:
                encoded_kw = product.keyword.replace(" ", "+")
                ali_url = f"https://www.aliexpress.com/wholesale?SearchText={encoded_kw}"
                st.markdown(f"🛒 [Search AliExpress]({ali_url})")
        with link_cols[2]:
            if product.shopify_product_url:
                st.markdown(f"🛍️ [Shopify]({product.shopify_product_url})")

    with detail_tab2:
        if float(product.spend or 0) > 0:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Clicks", product.clicks)
                st.metric("Impressions", product.impressions)
            with col2:
                st.metric("Spend", f"€{float(product.spend or 0):.2f}")
                st.metric("Revenue", f"€{float(product.revenue or 0):.2f}")
            with col3:
                st.metric("Conversions", product.conversions)
                st.metric("ROAS", f"{float(product.roas or 0):.2f}")
            with col4:
                st.metric("Net Profit", f"€{float(product.net_profit or 0):.2f}")
                st.metric("Days Testing", product.days_testing)

            # Progress bars
            st.markdown("---")
            kill_threshold = float(product.kill_threshold_spend or 0)
            spend = float(product.spend or 0)
            if kill_threshold > 0:
                spend_pct = min(spend / kill_threshold, 1.0)
                st.markdown(f"**Spend vs Kill Threshold:** €{spend:.2f} / €{kill_threshold:.2f}")
                st.progress(spend_pct)

            broas = float(product.break_even_roas or 0)
            roas = float(product.roas or 0)
            if broas > 0:
                roas_pct = min(roas / (broas * 2), 1.0)
                st.markdown(f"**ROAS vs Break-even:** {roas:.2f} / {broas:.2f}")
                st.progress(roas_pct)
        else:
            st.info("No performance data yet. Product needs to be active in Google Ads.")

    with detail_tab3:
        logs = store.get_logs(product_id=product.product_id, limit=20)
        if logs:
            for log in logs:
                with st.container(border=True):
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.markdown(f"**{log.action_type.replace('_', ' ').title()}**")
                        st.write(log.reason)
                        if log.details:
                            st.caption(log.details)
                    with col2:
                        st.caption(f"{log.old_status} → {log.new_status}")
                        st.caption(log.timestamp[:19])
        else:
            st.info("No history for this product yet.")

    with detail_tab4:
        st.markdown("**Manual Actions**")

        col1, col2, col3 = st.columns(3)

        with col1:
            if product.test_status in ("killed", "rejected"):
                if st.button("🔄 Re-test Product", use_container_width=True):
                    from src.core.models import ActionLog, ActionType
                    store.update_product(product.product_id, {
                        "test_status": ProductStatus.DISCOVERED.value,
                        "reason": "Manual re-test triggered",
                        "days_testing": 0,
                        "days_below_broas": 0,
                        "spend": 0,
                        "clicks": 0,
                        "conversions": 0,
                        "revenue": 0,
                        "roas": 0,
                        "net_profit": 0,
                    })
                    store.add_log(ActionLog(
                        product_id=product.product_id,
                        action_type=ActionType.PRODUCT_RETEST.value,
                        old_status=product.test_status,
                        new_status=ProductStatus.DISCOVERED.value,
                        reason="Manual re-test triggered via dashboard",
                        country=product.country,
                    ))
                    st.success("Product reset for re-testing!")
                    st.rerun()

        with col2:
            if product.test_status in ("testing", "winner", "scaling"):
                if st.button("⏸️ Pause Product", use_container_width=True):
                    store.update_product(product.product_id, {
                        "test_status": ProductStatus.PAUSED.value,
                        "reason": "Manually paused via dashboard",
                    })
                    st.success("Product paused!")
                    st.rerun()

            elif product.test_status == "paused":
                if st.button("▶️ Resume Product", use_container_width=True):
                    store.update_product(product.product_id, {
                        "test_status": ProductStatus.TESTING.value,
                        "reason": "Manually resumed via dashboard",
                        "days_below_broas": 0,
                    })
                    st.success("Product resumed!")
                    st.rerun()

        with col3:
            if product.test_status == "winner" and not product.request_real_photos:
                if st.button("📸 Request Real Photos", use_container_width=True):
                    store.update_product(product.product_id, {
                        "request_real_photos": True,
                        "reason": "Real product photos requested for winner",
                    })
                    st.success("Photo request flagged for agent!")

        # Manual price update
        st.markdown("---")
        st.markdown("**Update Economics**")

        col1, col2 = st.columns(2)
        with col1:
            new_selling = st.number_input(
                "New Selling Price (EUR)",
                value=float(product.selling_price or 0),
                step=1.0,
                key="new_selling"
            )
        with col2:
            new_landed = st.number_input(
                "New Landed Cost (EUR)",
                value=float(product.landed_cost or 0),
                step=1.0,
                key="new_landed"
            )

        if st.button("💰 Update Economics", use_container_width=True):
            from src.economics.validator import EconomicValidator
            product.selling_price = new_selling
            product.landed_cost = new_landed
            validator = EconomicValidator(config)
            economics = validator.calculate_economics(product)
            updates = {
                "selling_price": new_selling,
                "landed_cost": new_landed,
                **economics,
            }
            store.update_product(product.product_id, updates)
            st.success("Economics updated!")
            st.rerun()


main()
