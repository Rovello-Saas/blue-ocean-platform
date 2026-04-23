"""
Reusable Streamlit UI components.
"""

import streamlit as st
import pandas as pd
from typing import Optional

from src.core.models import Product, ProductStatus


# ---------------------------------------------------------------------------
# Pipeline funnel hero
# ---------------------------------------------------------------------------

# Statuses that count as "past" each stage. Order matters — stages are
# strictly cumulative (each later stage is a subset of the earlier one)
# so the funnel is guaranteed monotone non-increasing left-to-right.
_STATUS_AFTER_ECONOMICS = {
    ProductStatus.READY_TO_TEST.value,
    ProductStatus.LISTING_CREATED.value,
    ProductStatus.TESTING.value,
    ProductStatus.WINNER.value,
    ProductStatus.SCALING.value,
    ProductStatus.PAUSED.value,
    ProductStatus.KILLED.value,
}
_STATUS_WITH_LISTING = {
    ProductStatus.LISTING_CREATED.value,
    ProductStatus.TESTING.value,
    ProductStatus.WINNER.value,
    ProductStatus.SCALING.value,
    ProductStatus.PAUSED.value,
    ProductStatus.KILLED.value,
}
_STATUS_IN_ADS = {
    ProductStatus.TESTING.value,
    ProductStatus.WINNER.value,
    ProductStatus.SCALING.value,
    ProductStatus.PAUSED.value,
    ProductStatus.KILLED.value,
}


def compute_funnel_counts(keywords: list, products: list[Product]) -> list[dict]:
    """
    Walk keywords + products and return the six-stage funnel.

    Each dict is ``{"label": ..., "count": ..., "hint": ...}`` in order:
      1. Keywords researched
      2. Products sourced      (made it past volume/CPC/competitor gates)
      3. Agent cost received   (landed_cost > 0)
      4. Profit-validated      (passed economics → READY_TO_TEST or later)
      5. Pages created         (LISTING_CREATED or later)
      6. In ads                (TESTING / WINNER / SCALING / PAUSED / KILLED)

    Monotone by construction: every item in stage N+1 is also in stage N.
    """
    total_keywords = len(keywords)
    total_products = len(products)

    with_cost = sum(1 for p in products if float(p.landed_cost or 0) > 0)
    profit_ok = sum(1 for p in products if p.test_status in _STATUS_AFTER_ECONOMICS)
    with_listing = sum(1 for p in products if p.test_status in _STATUS_WITH_LISTING)
    in_ads = sum(1 for p in products if p.test_status in _STATUS_IN_ADS)

    # Break "Products sourced" into auto vs manual so the user can see the
    # manual-review backlog as a distinct number rather than blending it
    # into the "sourced" count. A manual-review product HAS been sourced
    # (it passed every research filter); it's just waiting on a hand price.
    pending_manual = sum(
        1 for p in products
        if p.test_status == ProductStatus.PENDING_MANUAL_REVIEW.value
    )

    return [
        {"label": "Keywords researched", "count": total_keywords,
         "hint": "Kept by the research pipeline after volume/CPC filters."},
        {"label": "Products sourced",    "count": total_products,
         "hint": (
             "Candidates from the research pipeline. "
             f"Includes {pending_manual} waiting on manual AliExpress lookup."
             if pending_manual else
             "Candidates from the research pipeline (auto-matched + manual review)."
         )},
        {"label": "Agent cost received", "count": with_cost,
         "hint": "Landed cost filled in by the sourcing agent or via the Research Inbox."},
        {"label": "Profit-validated",    "count": profit_ok,
         "hint": "Passed the economics engine (margin, max-CPC, bROAS)."},
        {"label": "Pages created",       "count": with_listing,
         "hint": "Shopify listing created with AI content + images."},
        {"label": "In ads",              "count": in_ads,
         "hint": "Currently in a Google Ads PMax campaign."},
    ]


def pipeline_funnel(stages: list[dict]) -> None:
    """
    Render a horizontal funnel hero as pure HTML/CSS inside Streamlit.

    Stages is the list returned by :func:`compute_funnel_counts`. Each
    stage becomes a trapezoid whose height scales with its share of the
    top-of-funnel count. A small inter-stage conversion percentage is
    shown on hover via the ``title`` attribute.

    Design notes:
    - Pure HTML so there's no extra dependency (plotly / altair).
    - Flex row so the funnel wraps cleanly on narrow displays; individual
      trapezoids scale their height, not their width, so the row stays
      readable even at dashboard width.
    - Colour ramps from cool blue at the wide end to green at the pointy
      end — visually reinforces "more valuable as you move right."
    """
    if not stages:
        return

    top_count = max(s["count"] for s in stages) or 1

    # Cool-to-warm ramp. 6 stops for the 6 stages.
    ramp = ["#3a86ff", "#4361ee", "#4895ef", "#4cc9f0", "#52b788", "#2d6a4f"]

    panels_html = []
    for i, stage in enumerate(stages):
        count = stage["count"]
        # Minimum 18% so a 0-count stage is still visible as a thin slice.
        share = max(0.18, count / top_count)
        height_pct = int(share * 100)
        colour = ramp[i % len(ramp)]

        # Conversion pct vs. previous stage (skip on first).
        conv_line = ""
        if i > 0:
            prev = stages[i - 1]["count"]
            if prev > 0:
                conv = count / prev
                conv_line = (
                    f'<div class="bop-funnel__conv" title="from {stages[i-1]["label"]}">'
                    f'{conv:.0%} →</div>'
                )

        panels_html.append(f"""
<div class="bop-funnel__stage" title="{stage['hint']}">
  {conv_line}
  <div class="bop-funnel__bar" style="
    height:{height_pct}%;
    background:linear-gradient(135deg, {colour}, {colour}cc);
  "></div>
  <div class="bop-funnel__count">{count:,}</div>
  <div class="bop-funnel__label">{stage['label']}</div>
</div>
""")

    # Single st.markdown dump — all the CSS lives in the same string so the
    # component is self-contained and safe to drop into any page.
    st.markdown(
        """
<style>
  .bop-funnel {
    display: flex; gap: 6px; align-items: flex-end;
    width: 100%; height: 180px; padding: 10px 0;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  }
  .bop-funnel__stage {
    flex: 1 1 0; min-width: 80px;
    display: flex; flex-direction: column; align-items: center;
    justify-content: flex-end; position: relative;
  }
  .bop-funnel__bar {
    width: 92%; border-radius: 8px 8px 4px 4px;
    transition: transform .15s ease;
    box-shadow: 0 2px 6px rgba(0,0,0,.08);
  }
  .bop-funnel__stage:hover .bop-funnel__bar { transform: translateY(-3px); }
  .bop-funnel__count {
    font-size: 1.35rem; font-weight: 700; color: #111;
    margin-top: 8px;
  }
  .bop-funnel__label {
    font-size: .78rem; color: #555; text-align: center;
    line-height: 1.1; margin-top: 2px;
  }
  .bop-funnel__conv {
    position: absolute; top: -4px; right: -14px;
    font-size: .72rem; font-weight: 600; color: #4361ee;
    background: #eef2ff; padding: 1px 6px; border-radius: 4px;
    z-index: 2;
  }
</style>
<div class="bop-funnel">
"""
        + "".join(panels_html)
        + "</div>",
        unsafe_allow_html=True,
    )


# Status colors and labels
STATUS_CONFIG = {
    "discovered": {"color": "#6c757d", "icon": "🔍", "label": "Discovered"},
    "pending_manual_review": {"color": "#b45309", "icon": "🔎", "label": "Manual Review"},
    "sourcing": {"color": "#fd7e14", "icon": "📦", "label": "Sourcing"},
    "ready_to_test": {"color": "#0d6efd", "icon": "🚀", "label": "Ready to Test"},
    "listing_created": {"color": "#6f42c1", "icon": "🛍️", "label": "Listing Created"},
    "testing": {"color": "#6610f2", "icon": "🧪", "label": "Testing"},
    "winner": {"color": "#198754", "icon": "🏆", "label": "Winner"},
    "scaling": {"color": "#20c997", "icon": "📈", "label": "Scaling"},
    "paused": {"color": "#ffc107", "icon": "⏸️", "label": "Paused"},
    "killed": {"color": "#dc3545", "icon": "💀", "label": "Killed"},
    "rejected": {"color": "#6c757d", "icon": "❌", "label": "Rejected"},
}


def status_badge(status: str) -> str:
    """Return a colored status badge as HTML."""
    config = STATUS_CONFIG.get(status, {"color": "#6c757d", "icon": "❓", "label": status})
    return f'{config["icon"]} {config["label"]}'


def product_card(product: Product, show_actions: bool = True):
    """Render a product card in the dashboard."""
    config = STATUS_CONFIG.get(product.test_status, STATUS_CONFIG["discovered"])

    with st.container(border=True):
        col1, col2, col3 = st.columns([3, 2, 1])

        with col1:
            st.markdown(f"**{product.keyword}** `{product.country}`")
            st.caption(f"ID: {product.product_id} | {config['icon']} {config['label']}")

        with col2:
            if float(product.spend or 0) > 0:
                cols = st.columns(3)
                with cols[0]:
                    st.metric("ROAS", f"{float(product.roas or 0):.2f}", label_visibility="collapsed")
                with cols[1]:
                    st.metric("Spend", f"€{float(product.spend or 0):.0f}", label_visibility="collapsed")
                with cols[2]:
                    st.metric("Profit", f"€{float(product.net_profit or 0):.0f}", label_visibility="collapsed")
            else:
                st.caption(f"Margin: {float(product.net_margin_pct or 0):.0%} | CPC: €{float(product.estimated_cpc or 0):.2f}")

        with col3:
            if product.reason:
                st.caption(product.reason[:80])


def products_dataframe(products: list[Product]) -> pd.DataFrame:
    """Convert products to a display-ready DataFrame."""
    if not products:
        return pd.DataFrame()

    rows = []
    for p in products:
        # Build Google Shopping URL (from data or generate fallback)
        gs_url = p.google_shopping_url if hasattr(p, 'google_shopping_url') and p.google_shopping_url else ""
        if not gs_url:
            encoded_kw = p.keyword.replace(" ", "+")
            gs_url = f"https://google.de/search?tbm=shop&q={encoded_kw}"

        # Build AliExpress URL (from data or generate fallback)
        ali_url = p.aliexpress_url or ""
        if not ali_url:
            encoded_kw = p.keyword.replace(" ", "+")
            ali_url = f"https://www.aliexpress.com/wholesale?SearchText={encoded_kw}"

        rows.append({
            "Status": status_badge(p.test_status),
            "Keyword": p.keyword,
            "Country": p.country,
            "Selling Price": f"€{float(p.selling_price or 0):.2f}",
            "Landed Cost": f"€{float(p.landed_cost or 0):.2f}" if float(p.landed_cost or 0) > 0 else "—",
            "Margin %": f"{float(p.net_margin_pct or 0):.0%}" if float(p.net_margin_pct or 0) > 0 else "—",
            "bROAS": f"{float(p.break_even_roas or 0):.2f}" if float(p.break_even_roas or 0) > 0 else "—",
            "Est. CPC": f"€{float(p.estimated_cpc or 0):.2f}",
            "Max CPC": f"€{float(p.max_allowed_cpc or 0):.2f}" if float(p.max_allowed_cpc or 0) > 0 else "—",
            "Competitors": int(p.competitor_count or 0),
            "Diff. Score": f"{float(p.differentiation_score or 0):.0f}",
            "🔍 Competitors": gs_url,
            "🛒 AliExpress": ali_url,
            "Clicks": int(p.clicks or 0),
            "Spend": f"€{float(p.spend or 0):.2f}",
            "Conv.": int(p.conversions or 0),
            "Revenue": f"€{float(p.revenue or 0):.2f}",
            "ROAS": f"{float(p.roas or 0):.2f}",
            "Net Profit": f"€{float(p.net_profit or 0):.2f}",
            "Days": int(p.days_testing or 0),
            "Reason": (p.reason or "")[:60],
            "ID": p.product_id,
        })

    return pd.DataFrame(rows)


def country_flag(country_code: str) -> str:
    """Return flag emoji for a country code."""
    flags = {
        "DE": "🇩🇪", "NL": "🇳🇱", "AT": "🇦🇹", "FR": "🇫🇷",
        "BE": "🇧🇪", "CH": "🇨🇭", "ES": "🇪🇸", "IT": "🇮🇹",
        "PL": "🇵🇱", "GB": "🇬🇧", "US": "🇺🇸",
    }
    return flags.get(country_code, "🏳️")


def economics_summary(product: Product):
    """Display a compact economics summary for a product."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Selling Price", f"€{float(product.selling_price or 0):.2f}")
        st.metric("Landed Cost", f"€{float(product.landed_cost or 0):.2f}")

    with col2:
        st.metric("Gross Margin", f"€{float(product.gross_margin or 0):.2f}")
        st.metric("Net Margin", f"{float(product.net_margin_pct or 0):.1%}")

    with col3:
        st.metric("Break-even ROAS", f"{float(product.break_even_roas or 0):.2f}")
        st.metric("Target ROAS", f"{float(product.target_roas or 0):.2f}")

    with col4:
        st.metric("Max CPC", f"€{float(product.max_allowed_cpc or 0):.2f}")
        st.metric("Test Budget", f"€{float(product.test_budget or 0):.2f}")
