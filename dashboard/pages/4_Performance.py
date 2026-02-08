"""
Performance Page — Per-product and portfolio performance monitoring.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd



def main():
    st.title("📈 Performance")

    try:
        from src.sheets.manager import get_data_store
        from src.core.config import AppConfig
        from src.core.models import ProductStatus

        store = get_data_store()
        config = AppConfig()
    except Exception as e:
        st.error(f"Could not connect to data store: {e}")
        return

    # Load products
    all_products = store.get_products()
    active_products = [
        p for p in all_products
        if p.test_status in (
            ProductStatus.TESTING.value,
            ProductStatus.WINNER.value,
            ProductStatus.SCALING.value,
        )
    ]

    tab1, tab2, tab3 = st.tabs([
        "📊 Portfolio Overview", "🏆 Winners", "🧪 Testing"
    ])

    # --- Portfolio Overview ---
    with tab1:
        st.subheader("Portfolio Performance")

        if not active_products:
            st.info("No active products to show performance for.")
        else:
            # Aggregate metrics
            total_spend = sum(float(p.spend or 0) for p in active_products)
            total_revenue = sum(float(p.revenue or 0) for p in active_products)
            total_conversions = sum(int(p.conversions or 0) for p in active_products)
            total_clicks = sum(int(p.clicks or 0) for p in active_products)
            total_profit = sum(float(p.net_profit or 0) for p in active_products)
            overall_roas = total_revenue / total_spend if total_spend > 0 else 0
            overall_cpa = total_spend / total_conversions if total_conversions > 0 else 0
            overall_cpc = total_spend / total_clicks if total_clicks > 0 else 0
            overall_cr = total_conversions / total_clicks * 100 if total_clicks > 0 else 0

            # Top metrics
            col1, col2, col3, col4, col5, col6 = st.columns(6)

            with col1:
                st.metric("Total Spend", f"€{total_spend:,.2f}")
            with col2:
                st.metric("Total Revenue", f"€{total_revenue:,.2f}")
            with col3:
                st.metric("Net Profit", f"€{total_profit:,.2f}")
            with col4:
                st.metric("Overall ROAS", f"{overall_roas:.2f}")
            with col5:
                st.metric("Conversions", f"{total_conversions}")
            with col6:
                st.metric("Avg CPA", f"€{overall_cpa:.2f}")

            # Additional metrics
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Total Clicks", f"{total_clicks:,}")
            with col2:
                st.metric("Avg CPC", f"€{overall_cpc:.2f}")
            with col3:
                st.metric("Conv. Rate", f"{overall_cr:.1f}%")
            with col4:
                st.metric("Active Products", len(active_products))

            # Product performance table
            st.markdown("---")
            st.subheader("Product Performance Breakdown")

            rows = []
            for p in active_products:
                spend = float(p.spend or 0)
                revenue = float(p.revenue or 0)
                roas = float(p.roas or 0)
                profit = float(p.net_profit or 0)
                broas = float(p.break_even_roas or 0)

                # Performance indicator
                if roas >= broas * 1.3:
                    perf = "🟢 Strong"
                elif roas >= broas:
                    perf = "🟡 OK"
                elif spend > 0:
                    perf = "🔴 Below"
                else:
                    perf = "⚪ No data"

                rows.append({
                    "Performance": perf,
                    "Keyword": p.keyword,
                    "Country": p.country,
                    "Status": p.test_status.replace("_", " ").title(),
                    "Spend": f"€{spend:.2f}",
                    "Revenue": f"€{revenue:.2f}",
                    "ROAS": f"{roas:.2f}",
                    "bROAS": f"{broas:.2f}",
                    "Profit": f"€{profit:.2f}",
                    "Conversions": int(p.conversions or 0),
                    "Clicks": int(p.clicks or 0),
                    "Days": int(p.days_testing or 0),
                })

            df = pd.DataFrame(rows)
            df = df.sort_values("Profit", ascending=False, key=lambda x: x.str.replace("€", "").str.replace(",", "").astype(float))
            st.dataframe(df, use_container_width=True, height=400)

    # --- Winners Tab ---
    with tab2:
        st.subheader("Winner Products")

        winners = [p for p in all_products if p.test_status in (ProductStatus.WINNER.value, ProductStatus.SCALING.value)]

        if not winners:
            st.info("No winner products yet. Products become winners after meeting ROAS and conversion thresholds.")
        else:
            winner_profit = sum(float(p.net_profit or 0) for p in winners)
            winner_revenue = sum(float(p.revenue or 0) for p in winners)
            winner_spend = sum(float(p.spend or 0) for p in winners)

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Winner Count", len(winners))
            with col2:
                st.metric("Winner Revenue", f"€{winner_revenue:,.2f}")
            with col3:
                st.metric("Winner Profit", f"€{winner_profit:,.2f}")
            with col4:
                w_roas = winner_revenue / winner_spend if winner_spend > 0 else 0
                st.metric("Winner ROAS", f"{w_roas:.2f}")

            st.markdown("---")

            for p in winners:
                with st.container(border=True):
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.markdown(f"**{p.keyword}** `{p.country}`")
                        st.caption(f"Testing for {p.days_testing} days")
                    with col2:
                        st.metric("ROAS", f"{float(p.roas or 0):.2f}")
                    with col3:
                        st.metric("Revenue", f"€{float(p.revenue or 0):.2f}")
                    with col4:
                        st.metric("Net Profit", f"€{float(p.net_profit or 0):.2f}")
                    with col5:
                        broas = float(p.break_even_roas or 0)
                        roas = float(p.roas or 0)
                        above_pct = ((roas - broas) / broas * 100) if broas > 0 else 0
                        st.metric("Above bROAS", f"+{above_pct:.0f}%")

                        if p.request_real_photos:
                            st.caption("📸 Real photos requested")

    # --- Testing Tab ---
    with tab3:
        st.subheader("Products in Testing")

        testing = [p for p in all_products if p.test_status == ProductStatus.TESTING.value]

        if not testing:
            st.info("No products currently in testing.")
        else:
            for p in testing:
                spend = float(p.spend or 0)
                kill_threshold = float(p.kill_threshold_spend or 0)
                broas = float(p.break_even_roas or 0)
                roas = float(p.roas or 0)

                with st.container(border=True):
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

                    with col1:
                        st.markdown(f"**{p.keyword}** `{p.country}`")
                        st.caption(f"Day {p.days_testing} | {p.clicks} clicks | {p.conversions} conv.")

                    with col2:
                        if kill_threshold > 0:
                            pct = min(spend / kill_threshold, 1.0)
                            st.caption(f"Spend: €{spend:.2f} / €{kill_threshold:.2f}")
                            st.progress(pct)

                    with col3:
                        st.metric("ROAS", f"{roas:.2f}", delta=f"bROAS: {broas:.2f}")

                    with col4:
                        if roas >= broas and int(p.conversions or 0) >= int(config.get("winner_rules.min_conversions", 3)):
                            st.success("Near winner!")
                        elif spend >= kill_threshold * 0.8:
                            st.warning("Near kill")
                        else:
                            st.info("Testing...")

            # Kill zone summary
            st.markdown("---")
            near_kill = [p for p in testing if float(p.spend or 0) >= float(p.kill_threshold_spend or 0) * 0.7]
            if near_kill:
                st.warning(f"⚠️ {len(near_kill)} products are approaching kill threshold")


main()
