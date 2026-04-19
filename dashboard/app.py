"""
Blue Ocean Platform — Streamlit Dashboard

Main entry point. Registers every page with `st.navigation` so Streamlit
renders the sidebar in a fixed order (not filesystem-dependent).

Run with: python3 -m streamlit run dashboard/app.py

Nav order reasoning:
  1. Home        — unified cockpit with workflow cards + research dashboard.
  2. Clone       — "I have a competitor URL" workflow. Front of the list so
                   users don't have to hunt for it.
  3. Research    — "I have a keyword" workflow. Keyword → sourcing pipeline.
  Then the supporting pages: Products, Performance, Logs, Studios, Settings.
"""

import sys
from pathlib import Path

# Add project root to path before any src.* imports happen downstream.
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import logging

import streamlit as st

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

# Pages. Declared one-per-line so the nav order is obvious at a glance.
# File paths are relative to the dashboard/ directory (Streamlit convention).
home_page       = st.Page("views/_home.py",          title="Home",          icon="🏠", default=True)
clone_page      = st.Page("views/_clone.py",         title="Clone page",    icon="🔗")
research_page   = st.Page("views/2_Research.py",     title="Research",      icon="🔬")
products_page   = st.Page("views/3_Products.py",     title="Products",      icon="📦")
performance_page = st.Page("views/4_Performance.py", title="Performance",   icon="📈")
logs_page       = st.Page("views/5_Logs.py",         title="Logs",          icon="📋")
studio_page     = st.Page("views/6_Image_Studio.py", title="Image Studio",  icon="🎨")
content_page    = st.Page("views/7_Content_Studio.py", title="Content Studio", icon="📝")
settings_page   = st.Page("views/1_Settings.py",     title="Settings",      icon="⚙️")

pg = st.navigation([
    home_page,
    clone_page,
    research_page,
    products_page,
    performance_page,
    logs_page,
    studio_page,
    content_page,
    settings_page,
])
pg.run()
