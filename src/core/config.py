"""
Configuration management.
Loads defaults from YAML, merges with Sheet-stored settings,
and provides a clean interface for all modules to access settings.
"""

from __future__ import annotations

import ast
import os
import logging
from pathlib import Path
from typing import Any, Optional

import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load .env file. `override=True` so values in .env always win over anything
# already present in the parent shell — we were getting silent failures when
# the launching shell had a credential name (e.g. OPENAI_API_KEY) set to an
# empty string, which dotenv's default behaviour would refuse to overwrite.
load_dotenv(override=True)

# Project root
PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"


def _deep_merge(base: dict, override: dict) -> dict:
    """Deep merge override into base dict."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


class AppConfig:
    """
    Central configuration manager.
    Priority: Sheet config > environment variables > defaults.yaml
    """

    _instance: Optional["AppConfig"] = None
    _config: dict = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_defaults()
        return cls._instance

    def _load_defaults(self):
        """Load defaults from YAML."""
        defaults_path = CONFIG_DIR / "defaults.yaml"
        if defaults_path.exists():
            with open(defaults_path, "r") as f:
                self._config = yaml.safe_load(f) or {}
            logger.info("Loaded default config from %s", defaults_path)
        else:
            logger.warning("No defaults.yaml found at %s", defaults_path)
            self._config = {}

    def merge_sheet_config(self, sheet_config: dict):
        """Merge configuration from Google Sheet (takes priority over defaults)."""
        self._config = _deep_merge(self._config, sheet_config)
        logger.info("Merged sheet config into app config")

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get a config value using dot notation.
        Example: config.get("economics.assumed_conversion_rate")
        """
        keys = key_path.split(".")
        value = self._config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    def set(self, key_path: str, value: Any):
        """Set a config value using dot notation."""
        keys = key_path.split(".")
        config = self._config
        for key in keys[:-1]:
            if key not in config or not isinstance(config[key], dict):
                config[key] = {}
            config = config[key]
        config[keys[-1]] = value

    def to_dict(self) -> dict:
        """Return full config as dict."""
        return self._config.copy()

    def reload(self):
        """Reload from defaults."""
        self._load_defaults()

    # --- Convenience accessors for commonly used values ---

    @property
    def countries(self) -> list[dict]:
        default = [{"code": "DE", "name": "Germany", "language": "de", "currency": "EUR"}]
        raw = self.get("global.countries", default)

        # If it's already a proper list of dicts, return as-is
        if isinstance(raw, list) and raw and isinstance(raw[0], dict):
            return raw

        # If it's a string (e.g. from Google Sheet storage), try to parse it
        if isinstance(raw, str):
            raw = raw.strip()
            # Try ast.literal_eval to parse string repr of list
            try:
                parsed = ast.literal_eval(raw)
                if isinstance(parsed, list):
                    raw = parsed
                else:
                    raw = [parsed]
            except (ValueError, SyntaxError):
                # Might be a single country code like "DE"
                raw = [raw] if raw else []

        # If it's a list, normalise each element to a dict
        if isinstance(raw, list):
            result = []
            for item in raw:
                if isinstance(item, dict):
                    result.append(item)
                elif isinstance(item, str) and len(item) == 2:
                    result.append({"code": item})
                # skip anything else (single chars from broken iteration)
            return result if result else default

        return default

    @property
    def min_search_volume(self) -> int:
        return int(self.get("research.min_monthly_search_volume", 500))

    @property
    def max_cpc(self) -> float:
        """Upper bound on DataForSEO estimated CPC. 0 = disabled."""
        return float(self.get("research.max_cpc", 0.0) or 0.0)

    @property
    def max_competitors(self) -> int:
        return int(self.get("research.max_competitors", 10))

    @property
    def min_differentiation_score(self) -> float:
        return float(self.get("research.min_differentiation_score", 30))

    @property
    def min_aliexpress_rating(self) -> float:
        return float(self.get("research.min_aliexpress_rating", 4.5))

    @property
    def min_aliexpress_orders(self) -> int:
        return int(self.get("research.min_aliexpress_orders", 500))

    @property
    def min_selling_price(self) -> float:
        return float(self.get("economics.min_selling_price", 25.0))

    @property
    def max_selling_price(self) -> float:
        return float(self.get("economics.max_selling_price", 200.0))

    @property
    def assumed_conversion_rate(self) -> float:
        return float(self.get("economics.assumed_conversion_rate", 0.01))

    @property
    def safety_factor(self) -> float:
        return float(self.get("economics.safety_factor", 1.5))

    @property
    def min_gross_margin_pct(self) -> float:
        return float(self.get("economics.min_gross_margin_pct", 0.30))

    @property
    def test_budget_multiplier(self) -> float:
        return float(self.get("economics.test_budget_multiplier", 3.0))

    @property
    def transaction_fee_pct(self) -> float:
        return float(self.get("economics.transaction_fee_pct", 0.02))

    @property
    def payment_fee_pct(self) -> float:
        return float(self.get("economics.payment_fee_pct", 0.029))

    @property
    def payment_fixed_fee(self) -> float:
        return float(self.get("economics.payment_fixed_fee", 0.30))

    @property
    def kill_spend_multiplier(self) -> float:
        return float(self.get("kill_rules.kill_spend_multiplier", 3.0))

    @property
    def max_days_below_broas(self) -> int:
        return int(self.get("kill_rules.max_days_below_broas", 3))

    @property
    def min_test_duration_days(self) -> int:
        return int(self.get("kill_rules.min_test_duration_days", 3))

    @property
    def scale_threshold_pct(self) -> float:
        return float(self.get("scale_rules.scale_threshold_pct", 0.30))

    @property
    def min_days_before_scale(self) -> int:
        return int(self.get("scale_rules.min_days_before_scale", 2))

    @property
    def scale_increment_pct(self) -> float:
        return float(self.get("scale_rules.scale_increment_pct", 0.20))

    @property
    def scale_frequency_days(self) -> int:
        return int(self.get("scale_rules.scale_frequency_days", 3))

    @property
    def max_daily_budget(self) -> float:
        return float(self.get("scale_rules.max_daily_budget", 100.0))

    @property
    def shipping_model(self) -> str:
        return self.get("shipping.model", "free")

    @property
    def shipping_charge(self) -> float:
        return float(self.get("shipping.charge_amount", 0.0))

    @property
    def free_shipping_threshold(self) -> float:
        return float(self.get("shipping.free_threshold", 0.0))

    @property
    def testing_campaign_budget(self) -> float:
        return float(self.get("ads.testing_campaign_daily_budget", 75.0))

    @property
    def winners_campaign_budget(self) -> float:
        return float(self.get("ads.winners_campaign_daily_budget", 150.0))

    @property
    def polling_interval_minutes(self) -> int:
        return int(self.get("polling.agent_cost_check_interval_minutes", 30))


# --- Environment variable accessors ---

def get_env(key: str, default: str = "") -> str:
    """
    Get a runtime setting.

    Priority:
      1. Environment variable / local .env
      2. Streamlit secrets (Cloud Secrets UI or local secrets.toml)
      3. Provided default

    Streamlit Cloud exposes secrets through `st.secrets`; depending on the
    deployment/runtime, relying only on os.getenv can miss values entered in
    the Secrets UI. Keep this helper central so dashboard code and backend
    clients resolve config the same way.
    """
    value = os.getenv(key)
    if value:
        return value

    try:
        import streamlit as st

        secret_value = st.secrets.get(key)
        if secret_value:
            return str(secret_value)
    except Exception:
        pass

    return default


def get_service_account_credentials(scopes: list[str] = None):
    """
    Load Google service account credentials.
    Supports two modes:
      1. Local: reads from GOOGLE_SHEETS_CREDENTIALS_PATH file
      2. Cloud: reads from GOOGLE_SERVICE_ACCOUNT_JSON env var (base64-encoded)
    """
    import json
    import base64
    from google.oauth2.service_account import Credentials

    # Try base64 env var first (cloud deployment)
    b64_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if b64_json:
        try:
            decoded = base64.b64decode(b64_json)
            info = json.loads(decoded)
            return Credentials.from_service_account_info(info, scopes=scopes)
        except Exception as e:
            logging.getLogger(__name__).error("Failed to decode GOOGLE_SERVICE_ACCOUNT_JSON: %s", e)

    # Fall back to file (local development)
    creds_path = get_env("GOOGLE_SHEETS_CREDENTIALS_PATH", "credentials/google_service_account.json")
    return Credentials.from_service_account_file(creds_path, scopes=scopes)


# API credentials
OPENAI_API_KEY = get_env("OPENAI_API_KEY")
GOOGLE_ADS_DEVELOPER_TOKEN = get_env("GOOGLE_ADS_DEVELOPER_TOKEN")
GOOGLE_ADS_CLIENT_ID = get_env("GOOGLE_ADS_CLIENT_ID")
GOOGLE_ADS_CLIENT_SECRET = get_env("GOOGLE_ADS_CLIENT_SECRET")
GOOGLE_ADS_REFRESH_TOKEN = get_env("GOOGLE_ADS_REFRESH_TOKEN")
GOOGLE_ADS_CUSTOMER_ID = get_env("GOOGLE_ADS_CUSTOMER_ID")
GOOGLE_ADS_LOGIN_CUSTOMER_ID = get_env("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
GOOGLE_SHEETS_CREDENTIALS_PATH = get_env("GOOGLE_SHEETS_CREDENTIALS_PATH", "credentials/google_service_account.json")
GOOGLE_SHEETS_SPREADSHEET_ID = get_env("GOOGLE_SHEETS_SPREADSHEET_ID")
# Separate spreadsheet for the sourcing agent's Agent Tasks tab.
# Sheets permissions are per-*spreadsheet*, not per-tab — giving the agent's
# service account access to the main sheet means it can read every tab
# (Keywords, Drops, Config, ActionLog, ...). We keep the agent confined to
# its own spreadsheet so a prompt-injected agent can at worst leak the tasks
# we already handed it. Falls back to the main sheet ID if unset, preserving
# the pre-split behavior for environments that haven't migrated yet.
GOOGLE_SHEETS_AGENT_SPREADSHEET_ID = get_env(
    "GOOGLE_SHEETS_AGENT_SPREADSHEET_ID"
) or GOOGLE_SHEETS_SPREADSHEET_ID
GOOGLE_MERCHANT_CENTER_ID = get_env("GOOGLE_MERCHANT_CENTER_ID")
SHOPIFY_SHOP_URL = get_env("SHOPIFY_SHOP_URL")
SHOPIFY_STOREFRONT_DOMAIN = get_env("SHOPIFY_STOREFRONT_DOMAIN")
SHOPIFY_ACCESS_TOKEN = get_env("SHOPIFY_ACCESS_TOKEN")
ALIEXPRESS_APP_KEY = get_env("ALIEXPRESS_APP_KEY")
ALIEXPRESS_APP_SECRET = get_env("ALIEXPRESS_APP_SECRET")
ALIEXPRESS_TRACKING_ID = get_env("ALIEXPRESS_TRACKING_ID")
SERPAPI_KEY = get_env("SERPAPI_KEY")
ANTHROPIC_API_KEY = get_env("ANTHROPIC_API_KEY")

# DataForSEO — Google Keyword Planner replacement.
# Keywords Data API → Google Ads → Search Volume (Live) returns real
# monthly search volume + CPC without requiring a Google Ads account on
# Basic/Standard access tier. Pricing: $0.05 per task of up to 1000 kws.
DATAFORSEO_LOGIN = get_env("DATAFORSEO_LOGIN")
DATAFORSEO_PASSWORD = get_env("DATAFORSEO_PASSWORD")

# Page cloner engine. Streamlit owns the UI; the Node service only does the
# scrape/generate/upload work behind the Clone page. Local `.env` can still
# point this at http://127.0.0.1:3000, while Cloud falls back to the Render
# service so clone buttons work without a separate dashboard setting.
PAGE_CLONER_URL = get_env("PAGE_CLONER_URL", "https://blue-ocean-page-cloner.onrender.com")
