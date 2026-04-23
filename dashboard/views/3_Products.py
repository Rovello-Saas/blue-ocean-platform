"""
Products page — kanban-by-stage.

The old Products page was a filterable table plus a four-tab detail panel.
That works fine for a data engineer; it's bad for the actual user, who
walks over to this page to answer one of: "what needs cost filled in?",
"what's ready to list?", "what's winning?". A table doesn't help with
that — every question becomes "filter by status, then scroll".

New layout:

  Sourcing   →   Ready   →   Live   →   Winners
  (waiting      (cost in,    (testing /  (winner /
   for agent    not listed   paused)     scaling)
   cost)        yet)

Each lifecycle status collapses into one of four buckets so the user can
see all four at a glance without scrolling. Cards inside each column show
only the numbers that matter *for that stage* — in Sourcing that's "how
many days has the agent had this?"; in Live that's ROAS and spend; in
Winners it's net profit.

Clicking a card opens a single full-width drawer underneath the kanban
(replacing the old 4-tab layout). The drawer's sections are just headings
inside the drawer — no more clicking between tabs to see economics AND
performance AND history for the same product.

Killed / rejected / long-paused products live in a collapsed **Archive**
expander at the bottom — out of the way until the user explicitly wants
to look at them.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import logging

import streamlit as st

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stage buckets
#   Four columns, mapping N lifecycle statuses each. Keep this in one place
#   so the KPI strip, kanban, and archive all stay in sync.
# ---------------------------------------------------------------------------

STAGES = [
    {
        "key":       "manual_review",
        "title":     "Manual Review",
        "subtitle":  "No AliExpress auto-match — needs manual price lookup",
        "statuses":  ["pending_manual_review"],
        "colour":    "#b45309",
    },
    {
        "key":       "sourcing",
        "title":     "Sourcing",
        "subtitle":  "Agent needs to fill in the landed cost",
        "statuses":  ["discovered", "sourcing"],
        "colour":    "#fd7e14",
    },
    {
        "key":       "ready",
        "title":     "Ready",
        "subtitle":  "Cost in, listing and creative next",
        "statuses":  ["ready_to_test", "listing_created"],
        "colour":    "#0d6efd",
    },
    {
        "key":       "live",
        "title":     "Live",
        "subtitle":  "In ads — watching ROAS",
        "statuses":  ["testing", "paused"],
        "colour":    "#6610f2",
    },
    {
        "key":       "winners",
        "title":     "Winners",
        "subtitle":  "Proven ROAS — ready to scale",
        "statuses":  ["winner", "scaling"],
        "colour":    "#198754",
    },
]

ARCHIVE_STATUSES = ["killed", "rejected"]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    st.title("Products")
    st.caption(
        "Every product, grouped by where it is in the pipeline. "
        "Click a card to open its details below."
    )

    try:
        from dashboard.components.widgets import STATUS_CONFIG
        from src.core.config import AppConfig
        from src.core.models import ProductStatus
        from src.sheets.manager import get_data_store

        store = get_data_store()
        config = AppConfig()
    except Exception as e:
        st.error(f"Could not connect to data store: {e}")
        return

    # ----- Top filters ----------------------------------------------------
    all_products = store.get_products()
    if not all_products:
        st.info(
            "No products yet. Go to **Research** → pick keywords → "
            "**Send to sourcing** to create the first one."
        )
        return

    countries = sorted({p.country for p in all_products})
    f1, f2, f3 = st.columns([1, 1, 2])
    with f1:
        filter_country = st.selectbox(
            "Country",
            options=["All"] + countries,
            key="prod_filter_country",
        )
    with f2:
        search = st.text_input("Search keyword", key="prod_search")
    with f3:
        st.caption(
            f"{len(all_products)} total products · "
            f"{sum(1 for p in all_products if p.test_status in ARCHIVE_STATUSES)} archived"
        )

    # Apply filters once, then partition into stage buckets
    filtered = all_products
    if filter_country != "All":
        filtered = [p for p in filtered if p.country == filter_country]
    if search:
        q = search.lower()
        filtered = [p for p in filtered if q in p.keyword.lower()]

    in_archive = [p for p in filtered if p.test_status in ARCHIVE_STATUSES]
    in_stages = [p for p in filtered if p.test_status not in ARCHIVE_STATUSES]

    # ----- Detail drawer (rendered FIRST when a card is selected) --------
    # Before: drawer rendered below the kanban + archive, so clicking
    # "Details →" on a sourcing card produced no visible change — the user
    # was scrolled at the top of a page with 40 cards between them and the
    # newly-opened drawer. Rendering it above the kanban guarantees the
    # click produces an on-screen reaction every time.
    sel_id = st.session_state.get("prod_selected_id")
    if sel_id:
        selected = next((p for p in all_products if p.product_id == sel_id), None)
        if selected:
            with st.container(border=True):
                _render_drawer(store, config, selected)
            st.divider()

    # ----- KPI strip (one per stage) --------------------------------------
    kpi_cols = st.columns(len(STAGES))
    for col, stage in zip(kpi_cols, STAGES):
        count = sum(1 for p in in_stages if p.test_status in stage["statuses"])
        with col:
            st.metric(stage["title"], count, help=stage["subtitle"])

    st.divider()

    # ----- Kanban columns -------------------------------------------------
    kanban_cols = st.columns(len(STAGES))
    for col, stage in zip(kanban_cols, STAGES):
        items = [p for p in in_stages if p.test_status in stage["statuses"]]
        # Sort per-stage: oldest first for stages where stale items matter
        # (Manual Review and Sourcing — nudge stuck ones); by ROAS for
        # Live/Winners (best on top); newest first for everything else.
        if stage["key"] in ("manual_review", "sourcing"):
            items.sort(key=lambda p: p.created_at or "")
        elif stage["key"] in ("live", "winners"):
            items.sort(key=lambda p: float(p.roas or 0), reverse=True)
        else:
            items.sort(key=lambda p: p.created_at or "", reverse=True)

        with col:
            st.markdown(
                f"<div style='border-top:3px solid {stage['colour']};"
                f"padding-top:.5rem;margin-bottom:.25rem;'>"
                f"<span style='font-weight:700;font-size:1.05rem;'>{stage['title']}</span> "
                f"<span style='color:#888;'>({len(items)})</span></div>",
                unsafe_allow_html=True,
            )
            if not items:
                st.caption(f"_{stage['subtitle']}_")
            for p in items:
                _render_card(p, stage["key"])

    # (Detail drawer renders above the kanban — see top of main().)

    # ----- Archive (collapsed) --------------------------------------------
    st.divider()
    with st.expander(f"📦 Archive — killed / rejected ({len(in_archive)})"):
        if not in_archive:
            st.caption("Nothing archived yet.")
        else:
            _render_archive_table(in_archive)


# ---------------------------------------------------------------------------
# Cards
# ---------------------------------------------------------------------------

def _render_card(product, stage_key):
    """A compact product card — header + one or two numbers + pick button."""
    from dashboard.components.widgets import STATUS_CONFIG

    cfg = STATUS_CONFIG.get(product.test_status, STATUS_CONFIG["discovered"])

    with st.container(border=True):
        # Header: keyword (truncated) + country + current detail-status icon
        title = product.keyword if len(product.keyword) <= 36 else product.keyword[:34] + "…"
        st.markdown(
            f"**{title}**  \n"
            f"<span style='color:#666;font-size:.8rem;'>"
            f"{cfg['icon']} {cfg['label']} · {product.country}"
            f"</span>",
            unsafe_allow_html=True,
        )

        # Stage-specific body: only the number that matters right now.
        body = _card_body(product, stage_key)
        if body:
            st.markdown(
                f"<div style='font-size:.85rem;margin-top:.35rem;'>{body}</div>",
                unsafe_allow_html=True,
            )

        # Open-detail button. Keep it text-only so it blends into the card.
        if st.button(
            "Details →",
            key=f"card_open_{product.product_id}",
            use_container_width=True,
        ):
            st.session_state["prod_selected_id"] = product.product_id
            st.rerun()


def _card_body(product, stage_key) -> str:
    """Return an HTML snippet for the card body — stage-appropriate.

    For Sourcing cards we also surface the research metrics the user is
    actually using to decide "is this still worth chasing?" — search volume,
    CPC, estimated margin, AliExpress match price. Without these the card
    was just "waiting on cost" with no hook to care about the product.
    """
    if stage_key in ("sourcing", "manual_review"):
        days = _days_since(product.created_at)
        cost = float(product.landed_cost or 0)
        vol = int(product.monthly_search_volume or 0)
        cpc = float(product.estimated_cpc or 0)
        ali = float(product.aliexpress_price or 0)
        selling = float(product.selling_price or 0)
        # Rough margin % using the cost we actually have in hand: real
        # landed cost if it's back, else ali price × 1.2 estimated-landed.
        basis_cost = cost if cost > 0 else (ali * 1.2 if ali > 0 else 0)
        margin_pct = (
            ((selling - basis_cost) / selling * 100)
            if selling > 0 and basis_cost > 0 else None
        )
        # Header line — the waiting/received banner. Manual-review cards
        # read differently: there's no agent on the hook, the user is.
        if stage_key == "manual_review":
            head = (
                f"<span style='color:#b45309;'>Awaiting manual AliExpress lookup · "
                f"{days}d</span>"
            )
        elif cost > 0:
            head = f"Cost received: <b>€{cost:.2f}</b>"
        else:
            head = (
                f"<span style='color:#b45309;'>Waiting on cost · "
                f"{days}d in sourcing</span>"
            )
        # Secondary line — the research metrics. Format with en-space
        # separators so it reads as a tight one-liner, wrapping only when
        # the column is narrow.
        # Only show a metric if we actually have it — a bare "CPC €0.00" on
        # the card reads like "CPC is zero" when it really means "Keyword
        # Planner didn't return data". Flag the gap instead.
        bits = []
        if vol:
            bits.append(f"Vol <b>{vol:,}</b>/mo")
        if cpc:
            bits.append(f"CPC <b>€{cpc:.2f}</b>")
        if not vol and not cpc:
            bits.append("<span style='color:#888;'>No Planner data</span>")
        if selling:
            bits.append(f"Sell <b>€{selling:.0f}</b>")
        if ali:
            bits.append(f"Ali <b>€{ali:.2f}</b>")
        if margin_pct is not None:
            colour = "#198754" if margin_pct >= 30 else "#b45309" if margin_pct >= 15 else "#b00020"
            suffix = "" if cost > 0 else " <span style='color:#888;'>(est)</span>"
            bits.append(
                f"<span style='color:{colour};'>Margin <b>{margin_pct:.0f}%</b></span>{suffix}"
            )
        metrics = (
            "<div style='color:#555;font-size:.8rem;margin-top:.25rem;'>"
            + " · ".join(bits)
            + "</div>"
        ) if bits else ""
        return head + metrics
    if stage_key == "ready":
        margin = float(product.net_margin_pct or 0)
        selling = float(product.selling_price or 0)
        return (
            f"Selling: <b>€{selling:.2f}</b> · "
            f"Margin: <b>{margin:.0%}</b>"
        )
    if stage_key == "live":
        spend = float(product.spend or 0)
        roas = float(product.roas or 0)
        broas = float(product.break_even_roas or 0)
        if spend == 0:
            return "<span style='color:#888;'>No spend yet</span>"
        signal = "🟢" if roas >= broas and broas > 0 else "🟡" if roas >= broas * 0.8 else "🔴"
        return (
            f"ROAS: <b>{roas:.2f}</b> {signal} (break-even {broas:.2f})  \n"
            f"Spend: <b>€{spend:.0f}</b> · "
            f"Days: {int(product.days_testing or 0)}"
        )
    if stage_key == "winners":
        profit = float(product.net_profit or 0)
        roas = float(product.roas or 0)
        return (
            f"ROAS: <b>{roas:.2f}</b> · "
            f"Profit: <b>€{profit:.0f}</b>"
        )
    return ""


def _days_since(iso_ts: str) -> int:
    from datetime import datetime
    if not iso_ts:
        return 0
    try:
        then = datetime.fromisoformat(iso_ts.replace("Z", ""))
    except ValueError:
        return 0
    return max(0, (datetime.utcnow() - then).days)


# ---------------------------------------------------------------------------
# Detail drawer
# ---------------------------------------------------------------------------

def _render_drawer(store, config, product):
    """Full-width drawer combining the four old tabs into one scroll."""
    from dashboard.components.widgets import STATUS_CONFIG, economics_summary

    cfg = STATUS_CONFIG.get(product.test_status, STATUS_CONFIG["discovered"])
    h1, h2 = st.columns([5, 1])
    with h1:
        st.subheader(f"{cfg['icon']} {product.keyword}")
        st.caption(
            f"**{cfg['label']}** · {product.country} · ID {product.product_id}"
        )
    with h2:
        if st.button("✕ Close", use_container_width=True, key="drawer_close"):
            st.session_state["prod_selected_id"] = None
            st.rerun()

    # --- Economics --------------------------------------------------------
    st.markdown("#### Economics")
    economics_summary(product)

    # --- Research --------------------------------------------------------
    # A zero from Keyword Planner means "no data came back", not "real zero".
    # Render those as "—" so the user doesn't read a missing value as fact.
    # This happens a lot when the Google Ads dev token is at Explorer-tier
    # (generateKeywordIdeas is gated behind Basic tier) — the pipeline
    # silently falls back to zero and lets downstream steps run.
    st.markdown("#### Research data")
    vol_val = int(product.monthly_search_volume or 0)
    cpc_val = float(product.estimated_cpc or 0)
    comps_val = int(product.competitor_count or 0)
    diff_val = float(product.differentiation_score or 0)
    ali_val = float(product.aliexpress_price or 0)

    r1, r2, r3 = st.columns(3)
    with r1:
        st.metric("Search volume", f"{vol_val:,}" if vol_val else "—")
        st.metric("Estimated CPC", f"€{cpc_val:.2f}" if cpc_val else "—")
    with r2:
        st.metric("Competitors", comps_val if comps_val else "—")
        st.metric("Differentiation", f"{diff_val:.0f}/100" if diff_val else "—")
    with r3:
        st.metric(
            "Competition type",
            (product.competition_type or "").replace("_", " ").title() or "—",
        )
        st.metric("AliExpress price", f"€{ali_val:.2f}" if ali_val else "—")

    # If either of the Planner-sourced numbers is missing, explain why and
    # offer a manual override form so the user isn't stuck.
    if not vol_val or not cpc_val:
        st.caption(
            "⚠️ Search volume / CPC missing — Google Keyword Planner didn't "
            "return data for this keyword (usually Ads API Explorer-tier gating "
            "or a validation failure). You can fill them in below."
        )
        with st.expander("Fill in research data manually", expanded=False):
            m1, m2 = st.columns(2)
            with m1:
                new_vol = st.number_input(
                    "Search volume (monthly)",
                    min_value=0,
                    value=vol_val,
                    step=100,
                    key=f"manual_vol_{product.product_id}",
                )
            with m2:
                new_cpc = st.number_input(
                    "Estimated CPC (EUR)",
                    min_value=0.0,
                    value=cpc_val,
                    step=0.05,
                    format="%.2f",
                    key=f"manual_cpc_{product.product_id}",
                )
            if st.button(
                "Save research data",
                key=f"save_research_{product.product_id}",
                use_container_width=True,
            ):
                store.update_product(product.product_id, {
                    "monthly_search_volume": int(new_vol),
                    "estimated_cpc": float(new_cpc),
                })
                # Keep the Keywords tab in sync so the inbox shows the same
                # numbers if the keyword is still there.
                if getattr(product, "keyword_id", None):
                    try:
                        store.update_keyword(product.keyword_id, {
                            "monthly_search_volume": int(new_vol),
                            "estimated_cpc": float(new_cpc),
                        })
                    except Exception:
                        pass
                st.success("Saved.")
                st.rerun()

    # Links
    encoded = product.keyword.replace(" ", "+")
    gs = product.google_shopping_url or f"https://google.de/search?tbm=shop&q={encoded}"
    ali = product.aliexpress_url or f"https://www.aliexpress.com/wholesale?SearchText={encoded}"
    l1, l2, l3 = st.columns(3)
    with l1:
        st.markdown(f"🔍 [Google Shopping]({gs})")
    with l2:
        st.markdown(f"🛒 [AliExpress]({ali})")
    with l3:
        if product.shopify_product_url:
            st.markdown(f"🛍️ [Shopify]({product.shopify_product_url})")

    # --- Performance ------------------------------------------------------
    if float(product.spend or 0) > 0:
        st.markdown("#### Performance")
        p1, p2, p3, p4 = st.columns(4)
        with p1:
            st.metric("Clicks", product.clicks)
            st.metric("Impressions", product.impressions)
        with p2:
            st.metric("Spend", f"€{float(product.spend or 0):.2f}")
            st.metric("Revenue", f"€{float(product.revenue or 0):.2f}")
        with p3:
            st.metric("Conversions", product.conversions)
            st.metric("ROAS", f"{float(product.roas or 0):.2f}")
        with p4:
            st.metric("Net profit", f"€{float(product.net_profit or 0):.2f}")
            st.metric("Days testing", product.days_testing)

        kill = float(product.kill_threshold_spend or 0)
        spend = float(product.spend or 0)
        if kill > 0:
            st.progress(
                min(spend / kill, 1.0),
                text=f"Spend vs. kill threshold: €{spend:.0f} / €{kill:.0f}",
            )

    # --- History ----------------------------------------------------------
    st.markdown("#### Recent history")
    logs = store.get_logs(product_id=product.product_id, limit=10)
    if not logs:
        st.caption("No actions recorded yet.")
    else:
        for log in logs:
            with st.container(border=True):
                a, b = st.columns([4, 1])
                with a:
                    st.markdown(f"**{log.action_type.replace('_', ' ').title()}**")
                    if log.reason:
                        st.write(log.reason)
                    if log.details:
                        st.caption(log.details)
                with b:
                    if log.old_status or log.new_status:
                        st.caption(f"{log.old_status or '—'} → {log.new_status or '—'}")
                    st.caption(log.timestamp[:19])

    # --- Actions ----------------------------------------------------------
    st.markdown("#### Actions")
    _render_actions(store, config, product)


def _render_actions(store, config, product):
    """Stage-aware action buttons + manual economics override."""
    from src.core.models import ActionLog, ActionType, ProductStatus

    status = product.test_status
    c1, c2, c3 = st.columns(3)

    with c1:
        if status in ("killed", "rejected"):
            if st.button("🔄 Re-test", use_container_width=True, key="act_retest"):
                store.update_product(product.product_id, {
                    "test_status": ProductStatus.DISCOVERED.value,
                    "reason": "Manual re-test",
                    "days_testing": 0, "days_below_broas": 0,
                    "spend": 0, "clicks": 0, "conversions": 0, "revenue": 0,
                    "roas": 0, "net_profit": 0,
                })
                store.add_log(ActionLog(
                    product_id=product.product_id,
                    action_type=ActionType.PRODUCT_RETEST.value,
                    old_status=status,
                    new_status=ProductStatus.DISCOVERED.value,
                    reason="Manual re-test from dashboard",
                    country=product.country,
                ))
                st.success("Reset for re-testing.")
                st.rerun()

    with c2:
        if status in ("testing", "winner", "scaling"):
            if st.button("⏸️ Pause", use_container_width=True, key="act_pause"):
                store.update_product(product.product_id, {
                    "test_status": ProductStatus.PAUSED.value,
                    "reason": "Manually paused",
                })
                st.success("Paused.")
                st.rerun()
        elif status == "paused":
            if st.button("▶️ Resume", use_container_width=True, key="act_resume"):
                store.update_product(product.product_id, {
                    "test_status": ProductStatus.TESTING.value,
                    "reason": "Manually resumed",
                    "days_below_broas": 0,
                })
                st.success("Resumed.")
                st.rerun()

    with c3:
        if status == "winner" and not product.request_real_photos:
            if st.button("📸 Request real photos",
                         use_container_width=True, key="act_photos"):
                store.update_product(product.product_id, {
                    "request_real_photos": True,
                    "reason": "Real photos requested for winner",
                })
                st.success("Flagged for agent.")
                st.rerun()

    # Manual economics override
    with st.expander("Update economics manually"):
        e1, e2 = st.columns(2)
        with e1:
            new_selling = st.number_input(
                "Selling price (EUR)",
                value=float(product.selling_price or 0),
                step=1.0, key="act_new_selling",
            )
        with e2:
            new_landed = st.number_input(
                "Landed cost (EUR)",
                value=float(product.landed_cost or 0),
                step=1.0, key="act_new_landed",
            )
        if st.button("💰 Save & recalc", use_container_width=True, key="act_save_economics"):
            from src.economics.validator import EconomicValidator
            product.selling_price = new_selling
            product.landed_cost = new_landed
            validator = EconomicValidator(config)
            economics = validator.calculate_economics(product)
            store.update_product(product.product_id, {
                "selling_price": new_selling,
                "landed_cost": new_landed,
                **economics,
            })
            st.success("Economics recalculated.")
            st.rerun()


# ---------------------------------------------------------------------------
# Archive (simple read-only table)
# ---------------------------------------------------------------------------

def _render_archive_table(products):
    import pandas as pd

    rows = []
    for p in products:
        rows.append({
            "Keyword": p.keyword,
            "Country": p.country,
            "Status": p.test_status,
            "Last reason": (p.reason or "")[:80],
            "Days tested": int(p.days_testing or 0),
            "Spend": f"€{float(p.spend or 0):.2f}" if p.spend else "—",
            "ID": p.product_id,
        })
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


main()
