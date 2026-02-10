"""
Qoveliqo Ads - Streamlit Dashboard
Main entry point for the dashboard application.

Run with: python3 -m streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

# Page config must come first
st.set_page_config(
    page_title="Qoveliqo Ads",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Global CSS — applies to ALL pages
st.markdown("""
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
""", unsafe_allow_html=True)

# Use st.navigation for proper page naming (views/ instead of pages/ to avoid auto-discovery)
home_page = st.Page("views/_home.py", title="Home", icon="🏠", default=True)
settings_page = st.Page("views/1_Settings.py", title="Settings", icon="⚙️")
research_page = st.Page("views/2_Research.py", title="Research", icon="🔬")
products_page = st.Page("views/3_Products.py", title="Products", icon="📦")
performance_page = st.Page("views/4_Performance.py", title="Performance", icon="📈")
logs_page = st.Page("views/5_Logs.py", title="Logs", icon="📋")
studio_page = st.Page("views/6_Image_Studio.py", title="Image Studio", icon="🎨")
content_page = st.Page("views/7_Content_Studio.py", title="Content Studio", icon="📝")

pg = st.navigation([home_page, settings_page, research_page, products_page, performance_page, logs_page, studio_page, content_page])
pg.run()
