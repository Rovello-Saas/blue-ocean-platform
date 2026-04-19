"""
Research Page — AI research results and manual keyword input.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd


def _get_country_codes(countries_list) -> list[str]:
    """Safely extract country codes from config.countries (handles both dict and str)."""
    codes = []
    for c in countries_list:
        if isinstance(c, dict):
            codes.append(c.get("code", "DE"))
        elif isinstance(c, str) and len(c) == 2:
            codes.append(c)
    return codes if codes else ["DE"]


def _get_country_language(countries_list, code: str) -> str:
    """Get the language for a country code from the config."""
    for c in countries_list:
        if isinstance(c, dict) and c.get("code") == code:
            return c.get("language", "de")
    return "de"


def main():
    st.title("Research")

    # Align columns vertically so checkbox + button sit at same height as heading
    st.markdown(
        """<style>
        div[data-testid="stHorizontalBlock"] {
            align-items: center;
        }
        </style>""",
        unsafe_allow_html=True,
    )

    tab1, tab2, tab3 = st.tabs([
        "Research Results", "Manual Input", "Start Discovery"
    ])

    try:
        from src.sheets.manager import get_data_store
        import src.sheets.manager as _sheets_mgr
        from src.core.config import AppConfig

        # Ensure new columns are recognised even if module was loaded before update
        if "aliexpress_top3_json" not in _sheets_mgr.KEYWORD_HEADERS:
            idx = _sheets_mgr.KEYWORD_HEADERS.index("aliexpress_image_urls") + 1
            _sheets_mgr.KEYWORD_HEADERS.insert(idx, "aliexpress_top3_json")
        if "aliexpress_top3_json" not in _sheets_mgr.PRODUCT_HEADERS:
            idx = _sheets_mgr.PRODUCT_HEADERS.index("aliexpress_image_urls") + 1
            _sheets_mgr.PRODUCT_HEADERS.insert(idx, "aliexpress_top3_json")

        store = get_data_store()
        config = AppConfig()

    except Exception as e:
        st.error(f"Could not connect to data store: {e}")
        return

    # --- Research Results Tab ---
    with tab1:
        # Single row: title + checkbox + button — all same height
        hdr1, hdr2, hdr3 = st.columns([3, 1.2, 1.3])
        with hdr1:
            st.subheader("Latest Research Results")
        with hdr2:
            run_aliexpress = st.checkbox("Also search AliExpress", value=True, key="enrich_aliexpress")
        with hdr3:
            run_btn = st.button("Run competition research", type="primary", key="enrich_btn", use_container_width=True)

        # Filters row
        col1, col2, col3 = st.columns(3)
        with col1:
            countries = _get_country_codes(config.countries)
            filter_country = st.selectbox("Country", options=["All"] + countries)
        with col2:
            filter_source = st.selectbox("Source", options=["All", "ai", "manual"])
        with col3:
            sort_by = st.selectbox("Sort by", options=["Created (newest)", "Search Volume", "CPC", "Differentiation"])

        # Load keywords
        keywords = store.get_keywords(
            country=filter_country if filter_country != "All" else None
        )

        if filter_source != "All":
            keywords = [k for k in keywords if k.research_source == filter_source]

        if not keywords:
            st.info("No research results yet. Start product discovery or add keywords manually.")
        else:
            # Sort
            if sort_by == "Search Volume":
                keywords.sort(key=lambda k: k.monthly_search_volume, reverse=True)
            elif sort_by == "CPC":
                keywords.sort(key=lambda k: k.estimated_cpc, reverse=False)
            elif sort_by == "Differentiation":
                keywords.sort(key=lambda k: k.differentiation_score, reverse=True)
            else:
                keywords.sort(key=lambda k: k.created_at, reverse=True)

            # Build table rows with Select checkbox column
            rows = []
            for k in keywords:
                gs_url = k.google_shopping_url
                if not gs_url:
                    encoded_kw = k.keyword.replace(" ", "+")
                    gs_url = f"https://google.de/search?tbm=shop&q={encoded_kw}"
                ali_url = k.aliexpress_url
                if not ali_url:
                    encoded_kw = k.keyword.replace(" ", "+")
                    ali_url = f"https://www.aliexpress.com/wholesale?SearchText={encoded_kw}"
                rows.append({
                    "Select": False,
                    "Keyword": k.keyword,
                    "Country": k.country,
                    "Volume": k.monthly_search_volume,
                    "CPC": f"€{k.estimated_cpc:.2f}",
                    "Competition": k.competition_level,
                    "Competitors": k.competitor_count,
                    "Diff. Score": f"{k.differentiation_score:.0f}",
                    "Type": k.competition_type,
                    "Competitor Price": f"€{k.median_competitor_price:.2f}" if k.median_competitor_price else "—",
                    "AliExpress Price": f"€{k.aliexpress_price:.2f}" if k.aliexpress_price else "—",
                    "Rating": f"{k.aliexpress_rating:.1f}" if k.aliexpress_rating else "—",
                    "Orders": k.aliexpress_orders if k.aliexpress_orders else "—",
                    "Google Shopping": gs_url,
                    "AliExpress": ali_url,
                    "Source": k.research_source,
                    "Date": k.created_at[:10],
                })

            df = pd.DataFrame(rows)
            # Table full width
            edited_df = st.data_editor(
                df,
                use_container_width=True,
                height=500,
                column_config={
                    "Select": st.column_config.CheckboxColumn(
                        "Select",
                        help="Tick to run competition research on this keyword",
                        width="small",
                    ),
                    "Keyword": st.column_config.TextColumn(
                        "Keyword",
                        help="The product-intent search term discovered by AI or added manually",
                    ),
                    "Country": st.column_config.TextColumn(
                        "Country",
                        help="Target country for this keyword",
                        width="small",
                    ),
                    "Volume": st.column_config.NumberColumn(
                        "Volume",
                        help="Estimated monthly search volume from Google Keyword Planner. 0 means Keyword Planner data is not yet available.",
                    ),
                    "CPC": st.column_config.TextColumn(
                        "CPC",
                        help="Estimated Cost Per Click from Google Keyword Planner. This is the average price advertisers pay per click.",
                    ),
                    "Competition": st.column_config.TextColumn(
                        "Competition",
                        help="Google Ads competition level: low / medium / high. Indicates how many advertisers bid on this keyword.",
                    ),
                    "Competitors": st.column_config.NumberColumn(
                        "Competitors",
                        help="Number of unique sellers found in Google Shopping results. More sellers = more competition.",
                    ),
                    "Diff. Score": st.column_config.TextColumn(
                        "Diff. Score",
                        help="Differentiation Score (0-100). HIGH = many sellers sell the SAME product, so it's easier for you to stand out. LOW = sellers offer diverse products, harder to differentiate.",
                    ),
                    "Type": st.column_config.TextColumn(
                        "Type",
                        help="same_product = most competitors sell the same item (good for differentiation). diverse_products = competitors sell many different items.",
                    ),
                    "Competitor Price": st.column_config.TextColumn(
                        "Competitor Price",
                        help="Median selling price of competitors in Google Shopping. This becomes the estimated selling price for the product.",
                    ),
                    "AliExpress Price": st.column_config.TextColumn(
                        "AliExpress Price",
                        help="Best matching product price found on AliExpress. This is a preliminary cost estimate — the agent's landed cost will be more accurate.",
                    ),
                    "Rating": st.column_config.TextColumn(
                        "Rating",
                        help="AliExpress product rating (out of 5.0). Higher ratings indicate better product quality.",
                    ),
                    "Orders": st.column_config.TextColumn(
                        "Orders",
                        help="Number of orders on AliExpress. More orders = proven demand and reliable supplier.",
                    ),
                    "Google Shopping": st.column_config.LinkColumn(
                        "🔍 Competitors",
                        help="Click to view competitor listings on Google Shopping for this keyword",
                        display_text="View",
                    ),
                    "AliExpress": st.column_config.LinkColumn(
                        "🛒 AliExpress",
                        help="Click to view/search this product on AliExpress",
                        display_text="View",
                    ),
                    "Source": st.column_config.TextColumn(
                        "Source",
                        help="How this keyword was found: 'ai' = discovered by AI, 'manual' = added by you.",
                        width="small",
                    ),
                    "Date": st.column_config.TextColumn(
                        "Date",
                        help="When this keyword was researched.",
                        width="small",
                    ),
                },
                key="research_table",
            )

            selected_ids = [
                keywords[i].keyword_id
                for i in range(min(len(keywords), len(edited_df)))
                if edited_df.iloc[i].get("Select")
            ]
            if run_btn:
                if not selected_ids:
                    st.warning("Tick at least one keyword in the table.")
                else:
                    import importlib
                    import src.research.aliexpress as _ali_mod
                    import src.research.pipeline as _pipeline_mod
                    import src.sheets.manager as _sheets_mgr
                    from src.core.config import ALIEXPRESS_APP_KEY
                    importlib.reload(_ali_mod)
                    importlib.reload(_pipeline_mod)
                    # Patch sheet headers in running module so new columns are recognised
                    if "aliexpress_top3_json" not in _sheets_mgr.KEYWORD_HEADERS:
                        idx = _sheets_mgr.KEYWORD_HEADERS.index("aliexpress_image_urls") + 1
                        _sheets_mgr.KEYWORD_HEADERS.insert(idx, "aliexpress_top3_json")
                    if "aliexpress_top3_json" not in _sheets_mgr.PRODUCT_HEADERS:
                        idx = _sheets_mgr.PRODUCT_HEADERS.index("aliexpress_image_urls") + 1
                        _sheets_mgr.PRODUCT_HEADERS.insert(idx, "aliexpress_top3_json")

                    # Warn about missing AliExpress credentials
                    if run_aliexpress and (not ALIEXPRESS_APP_KEY or ALIEXPRESS_APP_KEY.startswith("your_")):
                        st.warning(
                            "**AliExpress API credentials not configured.** "
                            "Competition analysis will run, but AliExpress product data will be skipped. "
                            "Add your AliExpress API key in the `.env` file "
                            "(`ALIEXPRESS_APP_KEY`, `ALIEXPRESS_APP_SECRET`, `ALIEXPRESS_TRACKING_ID`)."
                        )

                    from src.research.pipeline import ResearchPipeline
                    pipeline = ResearchPipeline(store, config)
                    with st.spinner("Running competition analysis..."):
                        stats = pipeline.enrich_keywords(selected_ids, run_aliexpress=run_aliexpress)
                    st.success(
                        f"Enriched {stats['enriched_count']} keyword(s). "
                        f"AliExpress matched: {stats['aliexpress_matched_count']}."
                    )
                    if stats.get("errors"):
                        for err in stats["errors"]:
                            st.error(err)
                    st.rerun()

            st.caption(f"Showing {len(keywords)} keywords")

            # Keyword detail expander
            st.markdown("---")
            st.subheader("Keyword Details")

            selected_keyword = st.selectbox(
                "Select a keyword for details",
                options=[k.keyword for k in keywords],
            )

            if selected_keyword:
                kw = next(k for k in keywords if k.keyword == selected_keyword)

                def _fmt_num(n):
                    if n is None:
                        return "—"
                    if isinstance(n, (int, float)):
                        return f"{int(n):,}" if n == int(n) else f"{n:,.2f}"
                    return str(n)
                def _fmt_eur(n):
                    if n is None or (isinstance(n, (int, float)) and n == 0):
                        return "—"
                    return f"€{float(n):.2f}"
                def _fmt_str(s):
                    return (s or "").replace("_", " ").title() or "—"

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Monthly Search Volume", _fmt_num(kw.monthly_search_volume))
                    st.metric("Estimated CPC", _fmt_eur(kw.estimated_cpc))
                    st.metric("Competition", _fmt_str(kw.competition_level))

                with col2:
                    st.metric("Competitors", _fmt_num(kw.competitor_count))
                    st.metric("Differentiation Score", f"{kw.differentiation_score:.0f}/100" if kw.differentiation_score is not None else "—/100")
                    st.metric("Competition Type", _fmt_str(kw.competition_type))

                with col3:
                    st.metric("Median Competitor Price", _fmt_eur(kw.median_competitor_price))
                    st.metric("Avg Competitor Price", _fmt_eur(kw.avg_competitor_price))

                # --- Top 3 AliExpress Listings ---
                st.markdown("---")
                st.markdown("**Top 3 AliExpress Listings**")

                import json as _json
                top3_items = []
                try:
                    raw = getattr(kw, "aliexpress_top3_json", "") or ""
                    if raw:
                        top3_items = _json.loads(raw)
                except Exception:
                    top3_items = []

                if top3_items:
                    ali_cols = st.columns(len(top3_items))
                    for idx, item in enumerate(top3_items):
                        with ali_cols[idx]:
                            tag = item.get("tag", "")
                            title = item.get("title", "Unknown product")
                            price = item.get("price", 0)
                            rating = item.get("rating", 0)
                            orders = item.get("orders", 0)
                            url = item.get("url", "")
                            margin_pct = item.get("margin_pct", 0)
                            img = item.get("image_url", "")

                            # Tag badge
                            if tag == "Best Seller":
                                st.markdown(f"**:orange[{tag}]**")
                            elif tag == "Best Price":
                                st.markdown(f"**:green[{tag}]**")
                            elif tag == "Best Rated":
                                st.markdown(f"**:blue[{tag}]**")
                            else:
                                st.markdown(f"**{tag}**")

                            # Product image
                            if img:
                                st.image(img, width=120)

                            # Title (truncated)
                            st.caption(title[:80] + ("..." if len(title) > 80 else ""))

                            # Metrics
                            st.metric("Price", f"€{price:.2f}" if price else "—")
                            st.metric("Rating", f"{rating:.1f}/5" if rating else "—")
                            st.metric("Orders", f"{orders:,}" if orders else "—")

                            # Margin indicator
                            if margin_pct and margin_pct != 0:
                                pct_display = f"{margin_pct * 100:.0f}%"
                                if margin_pct >= 0.3:
                                    st.success(f"Est. margin: {pct_display}")
                                elif margin_pct >= 0.15:
                                    st.warning(f"Est. margin: {pct_display}")
                                else:
                                    st.error(f"Est. margin: {pct_display}")

                            # Link
                            if url:
                                st.markdown(f"[View on AliExpress]({url})")
                else:
                    from src.core.config import ALIEXPRESS_APP_KEY as _ali_key
                    if not _ali_key or _ali_key.startswith("your_"):
                        st.warning(
                            "**AliExpress API not configured.** "
                            "Add your credentials to `.env` (`ALIEXPRESS_APP_KEY`, "
                            "`ALIEXPRESS_APP_SECRET`, `ALIEXPRESS_TRACKING_ID`) to see product data here."
                        )
                    else:
                        st.info("No AliExpress data yet. Run competition research with 'Also search AliExpress' enabled.")

                # Links section
                st.markdown("---")
                st.markdown("**Links**")
                link_cols = st.columns(2)
                with link_cols[0]:
                    if kw.google_shopping_url:
                        st.markdown(f"[View on Google Shopping]({kw.google_shopping_url})")
                    else:
                        encoded_kw = kw.keyword.replace(" ", "+")
                        gs_url = f"https://google.de/search?tbm=shop&q={encoded_kw}"
                        st.markdown(f"[Search Google Shopping]({gs_url})")
                with link_cols[1]:
                    encoded_kw = kw.keyword.replace(" ", "+")
                    ali_url = f"https://www.aliexpress.com/wholesale?SearchText={encoded_kw}"
                    st.markdown(f"[Search AliExpress]({ali_url})")

                if kw.notes:
                    st.info(f"Notes: {kw.notes}")

    # --- Manual Input Tab ---
    with tab2:
        st.subheader("Add Manual Keyword")
        st.caption("Add your own keyword research. It follows the same validation process as AI-generated keywords.")

        with st.form("manual_keyword_form"):
            col1, col2 = st.columns(2)

            with col1:
                mk_keyword = st.text_input("Keyword *", placeholder="e.g., kabellose kopfhörer bluetooth")
                mk_country = st.selectbox(
                    "Country *",
                    options=_get_country_codes(config.countries),
                )
                mk_language = st.text_input("Language", value="de")
                mk_volume = st.number_input("Monthly Search Volume", min_value=0, value=0, step=100)

            with col2:
                mk_cpc = st.number_input("Estimated CPC (EUR)", min_value=0.0, value=0.0, step=0.05)
                mk_notes = st.text_area("Notes (optional)", placeholder="Source, reasoning, etc.")

            submitted = st.form_submit_button("Add Keyword", type="primary")

            if submitted:
                if not mk_keyword:
                    st.error("Keyword is required")
                else:
                    from src.research.pipeline import ResearchPipeline
                    pipeline = ResearchPipeline(store, config)
                    kw = pipeline.add_manual_keyword(
                        keyword=mk_keyword,
                        country=mk_country,
                        language=mk_language,
                        monthly_search_volume=mk_volume,
                        estimated_cpc=mk_cpc,
                        notes=mk_notes,
                    )
                    st.success(f"Added keyword: '{mk_keyword}' (ID: {kw.keyword_id})")

    # --- Start Discovery Tab ---
    with tab3:
        st.subheader("Start Product Discovery")
        st.caption("Run the AI-powered product discovery for specific countries.")

        countries_config = config.countries
        country_codes = _get_country_codes(countries_config)
        selected_run_countries = st.multiselect(
            "Countries to research",
            options=country_codes,
            default=country_codes,
        )

        col1, col2 = st.columns(2)
        with col1:
            st.info(f"""
            **How it works:**
            1. AI generates ~{config.get('research.keywords_per_run', 150)} product ideas per country
            2. Search volume & CPC are validated
            3. Competition & pricing are analyzed
            4. Matching products are found on AliExpress
            5. Results are added to the Sheet for sourcing
            """)

        with col2:
            if st.button("🚀 Start Discovery", type="primary", use_container_width=True):
                from src.research.pipeline import ResearchPipeline
                pipeline = ResearchPipeline(store, config)

                progress = st.progress(0)
                status = st.empty()

                all_stats = []
                for i, code in enumerate(selected_run_countries):
                    lang = _get_country_language(countries_config, code)
                    status.text(f"Discovering products for {code}...")
                    stats = pipeline.run_full_pipeline(country=code, language=lang)
                    all_stats.append(stats)
                    progress.progress((i + 1) / len(selected_run_countries))

                progress.progress(1.0)
                status.text("Discovery complete!")

                # Show results
                for stats in all_stats:
                    st.markdown(f"""
                    **{stats.get('country', '?')}:**
                    - Keywords generated: {stats.get('keywords_generated', 0)}
                    - Passed volume filter: {stats.get('keywords_passed_volume', 0)}
                    - Passed competition filter: {stats.get('keywords_passed_competition', 0)}
                    - Passed price filter: {stats.get('keywords_passed_price_filter', 0)}
                    - Products matched on AliExpress: {stats.get('products_matched', 0)}
                    - **Added to sourcing: {stats.get('products_added_to_sourcing', 0)}**
                    """)

        # --- Discovery History ---
        st.markdown("---")
        st.subheader("Discovery History")
        st.caption("Results from previous discovery runs.")

        try:
            logs = store.get_logs(limit=50)
            # Find discovery-related notifications for run summaries
            notifications = store.get_notifications(unread_only=False, limit=20)
            discovery_runs = [n for n in notifications if n.title.startswith("Research complete")]

            if discovery_runs:
                for run in discovery_runs:
                    with st.container(border=True):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.markdown(f"**{run.title}**")
                            st.write(run.message)
                        with col2:
                            st.caption(run.timestamp[:19])
            else:
                # Fall back to showing sourcing_started logs as evidence of runs
                sourcing_logs = [l for l in logs if l.action_type == "sourcing_started"]
                if sourcing_logs:
                    # Group by timestamp (within same minute = same run)
                    runs = {}
                    for log in sourcing_logs:
                        run_key = log.timestamp[:16]  # Group by minute
                        if run_key not in runs:
                            runs[run_key] = {"count": 0, "country": log.country, "time": log.timestamp[:19]}
                        runs[run_key]["count"] += 1

                    for run_key, run_data in sorted(runs.items(), reverse=True):
                        with st.container(border=True):
                            col1, col2 = st.columns([3, 1])
                            with col1:
                                st.markdown(f"**Discovery run — {run_data['country']}**")
                                st.write(f"{run_data['count']} products added to sourcing")
                            with col2:
                                st.caption(run_data["time"])
                else:
                    st.info("No discovery runs yet. Click 'Start Discovery' above to find new products.")

        except Exception as e:
            st.info("No discovery history available yet.")


main()
