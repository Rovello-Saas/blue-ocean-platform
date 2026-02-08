"""
Reusable Streamlit UI components.
"""

import streamlit as st
import pandas as pd
from typing import Optional

from src.core.models import Product, ProductStatus


# Status colors and labels
STATUS_CONFIG = {
    "discovered": {"color": "#6c757d", "icon": "🔍", "label": "Discovered"},
    "sourcing": {"color": "#fd7e14", "icon": "📦", "label": "Sourcing"},
    "cost_received": {"color": "#17a2b8", "icon": "💰", "label": "Cost Received"},
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
