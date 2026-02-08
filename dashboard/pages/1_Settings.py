"""
Settings Page — All configurable parameters for the system.
Saved to Google Sheet Config tab and synced bidirectionally.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
from src.core.config import AppConfig


def main():
    st.title("⚙️ Settings")
    st.caption("Configure all system parameters. Changes are saved to Google Sheet automatically.")

    config = AppConfig()

    # Try to load sheet config
    try:
        from src.sheets.manager import get_data_store
        store = get_data_store()
        sheet_config = store.get_config()
        if sheet_config:
            config.merge_sheet_config(sheet_config)
    except Exception:
        st.warning("Could not connect to Google Sheets. Showing defaults.")
        store = None

    # Use tabs for organized settings
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🌍 Global", "🔬 Research", "💰 Economics",
        "📊 Kill/Scale Rules", "🚚 Shipping", "📡 Monitoring"
    ])

    settings_changed = False

    # --- Global Settings ---
    with tab1:
        st.subheader("Global Settings")

        col1, col2 = st.columns(2)

        with col1:
            # Country selection
            all_countries = [
                {"code": "DE", "name": "Germany", "language": "de"},
                {"code": "NL", "name": "Netherlands", "language": "nl"},
                {"code": "AT", "name": "Austria", "language": "de"},
                {"code": "FR", "name": "France", "language": "fr"},
                {"code": "BE", "name": "Belgium", "language": "nl"},
                {"code": "CH", "name": "Switzerland", "language": "de"},
                {"code": "ES", "name": "Spain", "language": "es"},
                {"code": "IT", "name": "Italy", "language": "it"},
                {"code": "PL", "name": "Poland", "language": "pl"},
                {"code": "GB", "name": "United Kingdom", "language": "en"},
            ]

            # Extract country codes from config (now always returns list of dicts)
            valid_options = [c["code"] for c in all_countries]
            current_codes = []
            for c in config.countries:
                code = c.get("code", "") if isinstance(c, dict) else str(c)
                if code in valid_options:
                    current_codes.append(code)
            # Ensure at least one default
            if not current_codes:
                current_codes = ["DE"]
            selected_countries = st.multiselect(
                "Target Countries",
                options=valid_options,
                default=current_codes,
                format_func=lambda x: next(
                    (f"{c['name']} ({c['code']})" for c in all_countries if c["code"] == x), x
                ),
                help="Select countries to run research for. Each country runs independently."
            )

        with col2:
            timezone = st.selectbox(
                "Timezone",
                options=["Europe/Berlin", "Europe/Amsterdam", "Europe/Paris",
                         "Europe/London", "Europe/Madrid", "Europe/Rome"],
                index=0,
                help="Timezone for scheduling and reporting"
            )

    # --- Research Settings ---
    with tab2:
        st.subheader("Research Settings")

        # Auto-discovery toggle
        st.markdown("**Automatic Product Discovery**")
        auto_col1, auto_col2, auto_col3 = st.columns([1, 1, 2])

        with auto_col1:
            auto_discovery = st.toggle(
                "Enable automatic discovery",
                value=config.get("research.auto_discovery_enabled", False),
                help="When enabled, the system automatically discovers new products on a schedule. When disabled, you run discovery manually via the Research page."
            )

        with auto_col2:
            # Parse the stored time string into hours and minutes
            time_str = config.get("research.auto_discovery_time", "03:00")
            try:
                from datetime import time as dt_time
                parts = str(time_str).split(":")
                default_time = dt_time(int(parts[0]), int(parts[1]))
            except (ValueError, IndexError):
                from datetime import time as dt_time
                default_time = dt_time(3, 0)

            auto_time = st.time_input(
                "Run daily at",
                value=default_time,
                help="What time to run automatic discovery each day (in your configured timezone)",
                disabled=not auto_discovery,
            )

        with auto_col3:
            if auto_discovery:
                st.info(f"Discovery will run automatically every day at {auto_time.strftime('%H:%M')} ({config.get('global.timezone', 'Europe/Berlin')})")
            else:
                st.caption("Automatic discovery is off. Use the 'Start Discovery' button on the Research page to run manually.")

        st.markdown("---")
        st.markdown("**Research Filters**")

        col1, col2 = st.columns(2)

        with col1:
            min_volume = st.number_input(
                "Minimum Monthly Search Volume",
                min_value=0, max_value=100000,
                value=config.min_search_volume,
                step=100,
                help="Keywords below this volume are filtered out"
            )

            max_competitors = st.number_input(
                "Maximum Competitors in Google Shopping",
                min_value=1, max_value=50,
                value=config.max_competitors,
                step=1,
                help="Keywords with more competitors are filtered out"
            )

            min_differentiation = st.slider(
                "Minimum Differentiation Score",
                min_value=0, max_value=100,
                value=int(config.min_differentiation_score),
                step=5,
                help="Higher = more competitors sell the same product (easier to differentiate). 0 = all different products."
            )

        with col2:
            research_frequency = st.number_input(
                "Research Frequency (hours)",
                min_value=1, max_value=168,
                value=int(config.get("research.research_frequency_hours", 24)),
                step=1,
                help="How often to run AI product discovery (used when auto-discovery is enabled)",
                disabled=not auto_discovery,
            )

            keywords_per_run = st.number_input(
                "Keywords per Research Run",
                min_value=10, max_value=500,
                value=int(config.get("research.keywords_per_run", 150)),
                step=10,
                help="Number of keywords the AI generates per run"
            )

            min_ali_rating = st.slider(
                "Minimum AliExpress Product Rating",
                min_value=3.0, max_value=5.0,
                value=config.min_aliexpress_rating,
                step=0.1,
                help="Filter out products below this rating"
            )

            min_ali_orders = st.number_input(
                "Minimum AliExpress Orders",
                min_value=0, max_value=50000,
                value=config.min_aliexpress_orders,
                step=100,
                help="Filter out products with fewer orders (quality signal)"
            )

        category_focus = st.text_area(
            "Category Focus (optional, one per line)",
            value="\n".join(config.get("research.category_focus", [])),
            help="Leave empty for all categories. Add categories to focus on, one per line."
        )

    # --- Economics Settings ---
    with tab3:
        st.subheader("Economics Settings")
        st.info("These settings determine how product profitability is calculated. All downstream metrics (max CPC, test budget, etc.) are auto-calculated from these values.")

        col1, col2 = st.columns(2)

        with col1:
            min_sell_price = st.number_input(
                "Minimum Selling Price (EUR)",
                min_value=0.0, max_value=1000.0,
                value=config.min_selling_price,
                step=5.0,
                help="Skip products if the median competitor price is below this amount. Low-priced products have thin margins."
            )

            max_sell_price = st.number_input(
                "Maximum Selling Price (EUR)",
                min_value=0.0, max_value=5000.0,
                value=config.max_selling_price,
                step=10.0,
                help="Skip products if the median competitor price is above this amount. Expensive products are harder to sell from unknown stores."
            )

            conversion_rate = st.slider(
                "Assumed Conversion Rate (%)",
                min_value=0.1, max_value=10.0,
                value=config.assumed_conversion_rate * 100,
                step=0.1,
                help="Expected conversion rate for basic test pages. 1% is conservative."
            )

            safety_factor = st.slider(
                "Safety Factor (above break-even)",
                min_value=1.0, max_value=3.0,
                value=config.safety_factor,
                step=0.1,
                help="1.5 = target ROAS 50% above break-even. Higher = more conservative."
            )

            min_margin = st.slider(
                "Minimum Net Margin (%)",
                min_value=10, max_value=70,
                value=int(config.min_gross_margin_pct * 100),
                step=5,
                help="Products below this margin are rejected"
            )

        with col2:
            test_multiplier = st.slider(
                "Test Budget Multiplier (x selling price)",
                min_value=1.0, max_value=10.0,
                value=config.test_budget_multiplier,
                step=0.5,
                help="3x = spend up to 3 times the selling price testing a product before killing it"
            )

            transaction_fee = st.number_input(
                "Shopify Transaction Fee (%)",
                min_value=0.0, max_value=10.0,
                value=config.transaction_fee_pct * 100,
                step=0.1,
                help="Shopify's cut per transaction"
            )

            payment_fee = st.number_input(
                "Payment Processing Fee (%)",
                min_value=0.0, max_value=10.0,
                value=config.payment_fee_pct * 100,
                step=0.1,
                help="Payment processor fee (e.g., Stripe/Shopify Payments)"
            )

            payment_fixed = st.number_input(
                "Payment Fixed Fee (EUR)",
                min_value=0.0, max_value=2.0,
                value=config.payment_fixed_fee,
                step=0.05,
                help="Fixed fee per transaction"
            )

        # Live economics preview
        st.markdown("---")
        st.subheader("Economics Preview")
        st.caption("See how your settings affect a sample product")

        pcol1, pcol2 = st.columns(2)
        with pcol1:
            preview_selling = st.number_input("Sample Selling Price (EUR)", value=49.90, step=1.0)
            preview_landed = st.number_input("Sample Landed Cost (EUR)", value=15.00, step=1.0)
            preview_cpc = st.number_input("Sample Est. CPC (EUR)", value=0.50, step=0.05)

        with pcol2:
            gross_margin = preview_selling - preview_landed
            gross_margin_pct = gross_margin / preview_selling if preview_selling > 0 else 0
            txn_fees = preview_selling * (transaction_fee / 100 + payment_fee / 100) + payment_fixed
            net_margin = gross_margin - txn_fees
            net_margin_pct = net_margin / preview_selling if preview_selling > 0 else 0
            broas = (1 / net_margin_pct) if net_margin_pct > 0 else 999
            target_roas = broas * safety_factor
            max_cpc = net_margin * (conversion_rate / 100)
            test_budget = preview_selling * test_multiplier

            st.metric("Gross Margin", f"€{gross_margin:.2f} ({gross_margin_pct:.0%})")
            st.metric("Net Margin (after fees)", f"€{net_margin:.2f} ({net_margin_pct:.0%})")
            st.metric("Break-even ROAS", f"{broas:.2f}")
            st.metric("Target ROAS", f"{target_roas:.2f}")
            st.metric("Max Allowed CPC", f"€{max_cpc:.2f}")
            st.metric("Test Budget", f"€{test_budget:.2f}")

            if preview_cpc > max_cpc:
                st.error(f"Estimated CPC (€{preview_cpc:.2f}) exceeds max allowed CPC (€{max_cpc:.2f}) — product would be REJECTED")
            else:
                st.success(f"CPC check passed: €{preview_cpc:.2f} ≤ €{max_cpc:.2f}")

    # --- Kill/Scale Rules ---
    with tab4:
        st.subheader("Kill Rules")

        col1, col2 = st.columns(2)

        with col1:
            kill_multiplier = st.slider(
                "Kill after spending X * selling price with 0 conversions",
                min_value=1.0, max_value=10.0,
                value=config.kill_spend_multiplier,
                step=0.5,
                help="If a product has no sales after spending this much, it gets killed"
            )

            max_days_below = st.number_input(
                "Kill after X consecutive days below break-even ROAS",
                min_value=1, max_value=30,
                value=config.max_days_below_broas,
                step=1,
                help="Product gets killed if below break-even for this many days straight"
            )

        with col2:
            min_test_days = st.number_input(
                "Minimum test duration (days) before killing",
                min_value=1, max_value=30,
                value=config.min_test_duration_days,
                step=1,
                help="Give products at least this many days before making kill decisions"
            )

            min_conversions = st.number_input(
                "Minimum conversions for Winner promotion",
                min_value=1, max_value=50,
                value=int(config.get("winner_rules.min_conversions", 3)),
                step=1,
                help="Product needs at least this many conversions to become a winner"
            )

        st.markdown("---")
        st.subheader("Scale Rules")

        col1, col2 = st.columns(2)

        with col1:
            scale_threshold = st.slider(
                "Scale when ROAS is X% above break-even",
                min_value=10, max_value=100,
                value=int(config.scale_threshold_pct * 100),
                step=5,
                help="Product must exceed break-even ROAS by this percentage to trigger scaling"
            )

            min_days_scale = st.number_input(
                "Consecutive days above threshold before scaling",
                min_value=1, max_value=14,
                value=config.min_days_before_scale,
                step=1,
                help="Product must be above scale threshold for this many days"
            )

        with col2:
            scale_increment = st.slider(
                "Scale budget increment (%)",
                min_value=5, max_value=100,
                value=int(config.scale_increment_pct * 100),
                step=5,
                help="Increase budget by this percentage when scaling"
            )

            scale_frequency = st.number_input(
                "Scale at most every X days",
                min_value=1, max_value=14,
                value=config.scale_frequency_days,
                step=1,
                help="Minimum days between scaling actions"
            )

            max_budget = st.number_input(
                "Maximum daily budget cap (EUR)",
                min_value=10.0, max_value=10000.0,
                value=config.max_daily_budget,
                step=10.0,
                help="Never scale budget beyond this amount"
            )

    # --- Shipping Settings ---
    with tab5:
        st.subheader("Shipping Settings")
        st.info("The landed cost from your agent already includes shipping to the customer. These settings affect how shipping is displayed and factored into revenue calculations.")

        shipping_model = st.selectbox(
            "Shipping Model",
            options=["free", "paid", "threshold"],
            index=["free", "paid", "threshold"].index(config.shipping_model),
            format_func=lambda x: {
                "free": "Free Shipping (included in price)",
                "paid": "Paid Shipping (charged to customer)",
                "threshold": "Free above threshold, paid below"
            }.get(x, x),
        )

        if shipping_model == "paid":
            shipping_charge = st.number_input(
                "Shipping Charge (EUR)", min_value=0.0, value=config.shipping_charge, step=0.50
            )
        elif shipping_model == "threshold":
            shipping_charge = st.number_input(
                "Shipping Charge below threshold (EUR)", min_value=0.0, value=config.shipping_charge, step=0.50
            )
            free_threshold = st.number_input(
                "Free shipping threshold (EUR)", min_value=0.0, value=config.free_shipping_threshold, step=5.0
            )
        else:
            shipping_charge = 0.0

    # --- Monitoring Settings ---
    with tab6:
        st.subheader("Monitoring Settings")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Competitor Price Monitoring**")
            comp_enabled = st.toggle(
                "Enable competitor price monitoring",
                value=config.get("monitoring.competitor_price_enabled", True),
                help="Periodically check if competitor prices have changed"
            )

            if comp_enabled:
                comp_freq = st.number_input(
                    "Check frequency (days)",
                    min_value=1, max_value=30,
                    value=int(config.get("monitoring.competitor_price_frequency_days", 7)),
                    step=1,
                )

                price_alert_threshold = st.slider(
                    "Price change alert threshold (%)",
                    min_value=5, max_value=50,
                    value=int(config.get("monitoring.price_change_alert_threshold_pct", 0.10) * 100),
                    step=5,
                    help="Alert when competitor prices change by more than this percentage"
                )

        with col2:
            st.markdown("**Stock Availability Monitoring**")
            stock_enabled = st.toggle(
                "Enable stock monitoring",
                value=config.get("monitoring.stock_monitoring_enabled", False),
                help="Check if products are still available on AliExpress. Useful if not working with an agent."
            )
            if stock_enabled:
                st.caption("Products will be auto-paused if they become unavailable on AliExpress.")

        st.markdown("---")

        st.markdown("**Ads Settings**")
        col1, col2 = st.columns(2)

        with col1:
            testing_budget = st.number_input(
                "Testing Campaign Daily Budget (EUR)",
                min_value=10.0, max_value=1000.0,
                value=config.testing_campaign_budget,
                step=10.0,
            )

        with col2:
            winners_budget = st.number_input(
                "Winners Campaign Initial Daily Budget (EUR)",
                min_value=10.0, max_value=5000.0,
                value=config.winners_campaign_budget,
                step=10.0,
            )

        poll_interval = st.number_input(
            "Agent Cost Polling Interval (minutes)",
            min_value=5, max_value=120,
            value=config.polling_interval_minutes,
            step=5,
            help="How often to check the Sheet for new agent cost entries"
        )

    # --- Save Button ---
    st.markdown("---")

    if st.button("💾 Save Settings", type="primary", use_container_width=True):
        # Build config dict
        countries_selected = [c for c in all_countries if c["code"] in selected_countries]

        new_config = {
            "global": {
                "countries": countries_selected,
                "timezone": timezone,
            },
            "research": {
                "auto_discovery_enabled": auto_discovery,
                "auto_discovery_time": auto_time.strftime("%H:%M"),
                "min_monthly_search_volume": min_volume,
                "max_competitors": max_competitors,
                "min_differentiation_score": min_differentiation,
                "research_frequency_hours": research_frequency,
                "keywords_per_run": keywords_per_run,
                "min_aliexpress_rating": min_ali_rating,
                "min_aliexpress_orders": min_ali_orders,
                "category_focus": [c.strip() for c in category_focus.split("\n") if c.strip()],
            },
            "economics": {
                "min_selling_price": min_sell_price,
                "max_selling_price": max_sell_price,
                "assumed_conversion_rate": conversion_rate / 100,
                "safety_factor": safety_factor,
                "min_gross_margin_pct": min_margin / 100,
                "test_budget_multiplier": test_multiplier,
                "transaction_fee_pct": transaction_fee / 100,
                "payment_fee_pct": payment_fee / 100,
                "payment_fixed_fee": payment_fixed,
            },
            "kill_rules": {
                "kill_spend_multiplier": kill_multiplier,
                "max_days_below_broas": max_days_below,
                "min_test_duration_days": min_test_days,
            },
            "winner_rules": {
                "min_conversions": min_conversions,
                "min_test_duration_days": min_test_days,
            },
            "scale_rules": {
                "scale_threshold_pct": scale_threshold / 100,
                "min_days_before_scale": min_days_scale,
                "scale_increment_pct": scale_increment / 100,
                "scale_frequency_days": scale_frequency,
                "max_daily_budget": max_budget,
            },
            "shipping": {
                "model": shipping_model,
                "charge_amount": shipping_charge,
                "free_threshold": free_threshold if shipping_model == "threshold" else 0,
            },
            "monitoring": {
                "competitor_price_enabled": comp_enabled,
                "competitor_price_frequency_days": comp_freq if comp_enabled else 7,
                "price_change_alert_threshold_pct": price_alert_threshold / 100 if comp_enabled else 0.10,
                "stock_monitoring_enabled": stock_enabled,
            },
            "ads": {
                "testing_campaign_daily_budget": testing_budget,
                "winners_campaign_daily_budget": winners_budget,
            },
            "polling": {
                "agent_cost_check_interval_minutes": poll_interval,
            },
        }

        # Save to Sheet
        if store:
            try:
                store.save_config(new_config)
                config.merge_sheet_config(new_config)
                st.success("Settings saved successfully!")
            except Exception as e:
                st.error(f"Failed to save settings: {e}")
        else:
            st.warning("Cannot save — Google Sheets not connected")


main()
