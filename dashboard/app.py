"""
Blue Ocean Platform — Streamlit Dashboard

Main entry point. Registers every page with `st.navigation` so Streamlit
renders the sidebar in a fixed, grouped order (not filesystem-dependent).

Run with: python3 -m streamlit run dashboard/app.py

Sidebar grouping — four sections that mirror the user's workflow, so the
sidebar tells a story instead of being a flat list of CRUD pages:

  • Start here  — Home (the cockpit; you should almost always start here).
  • Workflows   — the two ways to add a new product:
                    Clone page  (competitor URL → Merivalo / Meta)
                    Research    (keyword → Movanella / Google Shopping;
                                 also the manual-review inbox)
  • Manage      — what the pipeline produced:
                    Products, Performance, Logs
  • Tools       — hands-on editors + config you reach for occasionally:
                    Image Studio, Content Studio, Settings

If you add a page, put it in the section whose job it most obviously supports.
Resist creating new sections — four is about the visual limit before the
sidebar turns into an overwhelm again.
"""

import os
import sys
from pathlib import Path
from urllib.parse import urlparse

# Add project root to path before any src.* imports happen downstream.
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import logging

import streamlit as st
from src.core.config import PAGE_CLONER_URL, get_env

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

# Page config must be the first Streamlit call in the script.
st.set_page_config(
    page_title="Blue Ocean Platform",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Global CSS — applies to every page. Wider content area + hides Streamlit
# chrome we don't want (deploy button, hamburger, footer).
st.markdown(
    """
<style>
    .main .block-container {
        max-width: 95% !important;
        padding-left: 2rem;
        padding-right: 2rem;
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    [data-testid="stAppDeployButton"] {display: none;}
</style>
""",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Password gate
# ---------------------------------------------------------------------------
# A single shared password — Marc + Dennis both use the same Google account
# (info@rovelloshop.com) and the same credentials for every downstream API, so
# per-user OAuth would be overkill. Password is pulled from st.secrets on the
# deployed instance (Streamlit Community Cloud → Settings → Secrets) and left
# unset on local dev, where the gate becomes a no-op so `streamlit run
# dashboard/app.py` "just works" without a login round-trip.
#
# `st.secrets` reads `.streamlit/secrets.toml` locally if present, else the
# platform-injected secrets on Cloud. Missing key → empty string → skip gate.

def _require_password() -> None:
    """Block rendering until the visitor enters the shared password.

    No password configured (local dev, or partner forgot to set it on Cloud)
    → skip the gate entirely. The gate lives in session state so a successful
    login persists across reruns within the tab.
    """
    try:
        expected = st.secrets.get("dashboard_password", "")
    except Exception:
        # No secrets.toml at all (common on first-run local) → skip gate.
        expected = ""
    if not expected:
        return
    if st.session_state.get("_dashboard_authed"):
        return

    # Centered login card. Hide the sidebar so the nav doesn't peek through
    # with clickable-but-auth-gated pages; `st.stop()` below prevents the
    # nav code from running anyway, but hiding the sidebar removes the
    # empty gutter too.
    st.markdown(
        "<style>[data-testid='stSidebar']{display:none;}</style>",
        unsafe_allow_html=True,
    )
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.markdown("## 🎯 Blue Ocean Platform")
        st.caption("Sign in to continue.")
        with st.form("login", clear_on_submit=False):
            pw = st.text_input("Password", type="password", label_visibility="collapsed",
                               placeholder="Password")
            ok = st.form_submit_button("Sign in", type="primary")
            if ok:
                if pw == expected:
                    st.session_state["_dashboard_authed"] = True
                    st.rerun()
                else:
                    st.error("Wrong password.")
    st.stop()


_require_password()


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------
# Declared one-per-line so the nav order is obvious at a glance.
# File paths are relative to the dashboard/ directory (Streamlit convention).
home_page        = st.Page("views/_home.py",           title="Home",           icon="🏠", default=True)
clone_page       = st.Page("views/_clone.py",          title="Clone page",     icon="🔗")
research_page    = st.Page("views/2_Research.py",      title="Research",       icon="🔬")
products_page    = st.Page("views/3_Products.py",      title="Products",       icon="📦")
performance_page = st.Page("views/4_Performance.py",   title="Performance",    icon="📈")
logs_page        = st.Page("views/5_Logs.py",          title="Logs",           icon="📋")
costs_page       = st.Page("views/8_Costs.py",         title="Costs",          icon="💸")
studio_page      = st.Page("views/6_Image_Studio.py",  title="Image Studio",   icon="🎨")
content_page     = st.Page("views/7_Content_Studio.py", title="Content Studio", icon="📝")
settings_page    = st.Page("views/1_Settings.py",      title="Settings",       icon="⚙️")

# Cloud vs. local: the Clone page requires the Node page-cloner service.
# Localhost only works on the laptop running Streamlit. On Streamlit Cloud,
# show the Clone page only when PAGE_CLONER_URL points at a public service.
_cloud_mode = get_env("BLUE_OCEAN_CLOUD_MODE", "").strip().lower() in {"1", "true", "yes"}
_page_cloner_url = (PAGE_CLONER_URL or "").strip()
_page_cloner_host = urlparse(_page_cloner_url).hostname if _page_cloner_url else ""
_page_cloner_is_placeholder = "your-page-cloner-service" in _page_cloner_url
_page_cloner_is_local = _page_cloner_host in {"localhost", "127.0.0.1", "::1"}
_clone_page_enabled = (not _cloud_mode) or bool(
    _page_cloner_url and not _page_cloner_is_local and not _page_cloner_is_placeholder
)
workflows_pages = [clone_page, research_page] if _clone_page_enabled else [research_page]

# Grouped navigation. Streamlit renders each key as a section heading in the
# sidebar, so the user sees stages of a workflow instead of a flat wall of
# icons. See module docstring for the reasoning on each group.
#
# The Research page IS the manual-review queue: every keyword gets a human
# decision there (fill Ali €, then "Send to Agent" or "Kill"). The old
# separate Manual Review page was retired 2026-04-23 — a PENDING_MANUAL_REVIEW
# Product row just duplicated the same decision the Research Inbox already
# captures.
pg = st.navigation({
    "Start here": [home_page],
    "Workflows":  workflows_pages,
    "Manage":     [products_page, performance_page, costs_page, logs_page],
    "Tools":      [studio_page, content_page, settings_page],
})
pg.run()
