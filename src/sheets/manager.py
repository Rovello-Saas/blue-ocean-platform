"""
Google Sheets integration layer.
Implements the DataStore interface using Google Sheets as the backend.

Sheet structure:
- Keywords tab: keyword research results
- Products tab: product pipeline with all statuses
- Config tab: system configuration (synced with dashboard)
- Action Log tab: audit trail of all automated actions
- Notifications tab: dashboard notifications
- Feedback tab: research feedback for LLM improvement
"""

from __future__ import annotations

import logging
from typing import Optional

import gspread

from src.core.config import GOOGLE_SHEETS_CREDENTIALS_PATH, GOOGLE_SHEETS_SPREADSHEET_ID, get_service_account_credentials
from src.core.interfaces import DataStore
from src.core.models import (
    Product, KeywordResearch, ActionLog, Notification, CountryConfig,
    ProductStatus
)

logger = logging.getLogger(__name__)

# Sheets API scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Tab names
TAB_KEYWORDS = "Keywords"
TAB_PRODUCTS = "Products"
TAB_AGENT_TASKS = "Agent Tasks"
TAB_CONFIG = "Config"
TAB_ACTION_LOG = "Action Log"
TAB_NOTIFICATIONS = "Notifications"
TAB_FEEDBACK = "Feedback"

# Column headers for each tab
KEYWORD_HEADERS = [
    "keyword_id", "keyword", "country", "language",
    "monthly_search_volume", "estimated_cpc", "competition_level",
    "intent_score", "research_source", "competitor_count",
    "unique_product_count", "competition_type", "differentiation_score",
    "avg_competitor_price", "median_competitor_price", "estimated_selling_price",
    "google_shopping_url", "competitor_pdp_url",
    "aliexpress_url", "aliexpress_price", "aliexpress_rating",
    "aliexpress_orders", "aliexpress_image_urls", "created_at", "notes"
]

PRODUCT_HEADERS = [
    "product_id", "keyword_id", "country", "language", "keyword",
    "monthly_search_volume", "estimated_cpc", "competition_level",
    "competitor_count", "differentiation_score", "competition_type",
    "google_shopping_url", "competitor_pdp_url",
    "aliexpress_url", "aliexpress_price", "aliexpress_rating",
    "aliexpress_orders", "aliexpress_image_urls",
    "selling_price", "landed_cost",
    "gross_margin", "gross_margin_pct", "transaction_fees",
    "net_margin", "net_margin_pct", "break_even_roas", "target_roas",
    "break_even_cpa", "max_allowed_cpc", "test_budget", "kill_threshold_spend",
    "clicks", "impressions", "spend", "conversions", "revenue", "roas", "net_profit",
    "test_status", "ads_action", "listing_group_status",
    "shopify_product_id", "shopify_product_url",
    "days_testing", "days_below_broas",
    "consecutive_days_above_scale_threshold", "days_since_last_scale",
    "testing_started_at", "last_scale_at",
    "reason", "last_action_at", "created_at", "updated_at",
    "request_real_photos"
]

LOG_HEADERS = [
    "log_id", "product_id", "action_type", "old_status", "new_status",
    "reason", "details", "timestamp", "country"
]

NOTIFICATION_HEADERS = [
    "notification_id", "title", "message", "level", "read",
    "product_id", "timestamp"
]

# Agent Tasks tab — simplified view for the sourcing agent
AGENT_TASK_HEADERS = [
    "product_id", "keyword", "country",
    "google_shopping_url", "aliexpress_url", "aliexpress_price",
    "landed_cost", "agent_notes", "status",
]


class GoogleSheetsStore(DataStore):
    """
    Google Sheets implementation of the DataStore interface.
    """

    def __init__(self):
        self._client: Optional[gspread.Client] = None
        self._spreadsheet: Optional[gspread.Spreadsheet] = None

    def _connect(self):
        """Establish connection to Google Sheets."""
        if self._client is not None:
            return

        try:
            logger.info("Connecting to Google Sheets...")
            logger.info("Spreadsheet ID: '%s'", GOOGLE_SHEETS_SPREADSHEET_ID)

            creds = get_service_account_credentials(scopes=SCOPES)
            self._client = gspread.authorize(creds)
            logger.info("Authorized successfully, opening spreadsheet...")
            self._spreadsheet = self._client.open_by_key(GOOGLE_SHEETS_SPREADSHEET_ID.strip())
            logger.info("Connected to Google Sheets: %s", self._spreadsheet.title)
        except FileNotFoundError:
            logger.error("Service account JSON not found at: %s", GOOGLE_SHEETS_CREDENTIALS_PATH)
            raise
        except Exception as e:
            logger.error("Failed to connect to Google Sheets: %s (type: %s)", e, type(e).__name__)
            raise

    def _get_or_create_worksheet(self, tab_name: str, headers: list[str]) -> gspread.Worksheet:
        """Get an existing worksheet or create it with headers."""
        self._connect()
        try:
            ws = self._spreadsheet.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            ws = self._spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=len(headers))
            ws.update("A1", [headers])
            ws.format("1", {"textFormat": {"bold": True}})
            logger.info("Created new worksheet: %s", tab_name)
        return ws

    def _get_all_records(self, tab_name: str, headers: list[str]) -> list[dict]:
        """Get all records from a tab as list of dicts."""
        ws = self._get_or_create_worksheet(tab_name, headers)
        try:
            records = ws.get_all_records()
            return records
        except Exception as e:
            logger.error("Error reading %s: %s", tab_name, e)
            return []

    def _find_row_index(self, ws: gspread.Worksheet, id_column: str, id_value: str) -> Optional[int]:
        """Find the row index (1-based) for a record by its ID."""
        try:
            cell = ws.find(id_value, in_column=1)  # Assumes ID is in column 1
            return cell.row if cell else None
        except gspread.CellNotFound:
            return None

    def _append_row(self, tab_name: str, headers: list[str], data: dict):
        """Append a row to a tab."""
        ws = self._get_or_create_worksheet(tab_name, headers)
        row = [str(data.get(h, "")) for h in headers]
        ws.append_row(row, value_input_option="USER_ENTERED")

    def _update_row(self, tab_name: str, headers: list[str], id_field: str, id_value: str, updates: dict):
        """Update specific fields in a row."""
        ws = self._get_or_create_worksheet(tab_name, headers)
        row_idx = self._find_row_index(ws, id_field, id_value)
        if row_idx is None:
            logger.warning("Row not found for %s=%s in %s", id_field, id_value, tab_name)
            return

        for field_name, value in updates.items():
            if field_name in headers:
                col_idx = headers.index(field_name) + 1
                ws.update_cell(row_idx, col_idx, str(value))

    # --- Keywords ---

    def get_keywords(self, country: str = None, status: str = None) -> list[KeywordResearch]:
        records = self._get_all_records(TAB_KEYWORDS, KEYWORD_HEADERS)
        keywords = [KeywordResearch.from_dict(r) for r in records]
        if country:
            keywords = [k for k in keywords if k.country == country]
        return keywords

    def add_keyword(self, keyword: KeywordResearch) -> None:
        self._append_row(TAB_KEYWORDS, KEYWORD_HEADERS, keyword.to_dict())
        logger.info("Added keyword: %s (%s)", keyword.keyword, keyword.country)

    def update_keyword(self, keyword_id: str, updates: dict) -> None:
        self._update_row(TAB_KEYWORDS, KEYWORD_HEADERS, "keyword_id", keyword_id, updates)

    def keyword_exists(self, keyword: str, country: str) -> bool:
        keywords = self.get_keywords(country=country)
        return any(k.keyword.lower() == keyword.lower() for k in keywords)

    # --- Products ---

    def get_products(self, country: str = None, status: str = None) -> list[Product]:
        records = self._get_all_records(TAB_PRODUCTS, PRODUCT_HEADERS)
        products = [Product.from_dict(r) for r in records]
        if country:
            products = [p for p in products if p.country == country]
        if status:
            products = [p for p in products if p.test_status == status]
        return products

    def get_product(self, product_id: str) -> Optional[Product]:
        products = self.get_products()
        for p in products:
            if p.product_id == product_id:
                return p
        return None

    def add_product(self, product: Product) -> None:
        self._append_row(TAB_PRODUCTS, PRODUCT_HEADERS, product.to_dict())
        logger.info("Added product: %s (keyword: %s)", product.product_id, product.keyword)

    def update_product(self, product_id: str, updates: dict) -> None:
        from datetime import datetime
        updates["updated_at"] = datetime.utcnow().isoformat()
        self._update_row(TAB_PRODUCTS, PRODUCT_HEADERS, "product_id", product_id, updates)

    def get_products_awaiting_cost(self) -> list[Product]:
        """
        Get products where the agent has filled in the landed_cost
        on the Agent Tasks tab. Checks the Agent Tasks tab for completed rows,
        then returns matching products from the Products tab.
        """
        completed = self._get_agent_tasks_with_cost()
        if not completed:
            return []

        # Get matching products from the Products tab
        products = self.get_products(status=ProductStatus.SOURCING.value)
        product_map = {p.product_id: p for p in products}

        result = []
        for task in completed:
            pid = task.get("product_id", "")
            landed_cost = task.get("landed_cost", "")
            agent_notes = task.get("agent_notes", "")

            if pid in product_map and landed_cost:
                product = product_map[pid]
                product.landed_cost = float(landed_cost)
                if agent_notes:
                    product.notes = agent_notes
                result.append(product)

        return result

    # --- Agent Tasks ---

    def sync_product_to_agent_tasks(self, product: Product) -> None:
        """
        Add a product to the Agent Tasks tab when it enters 'sourcing' status.
        Only adds if the product isn't already on the Agent Tasks tab.
        """
        ws = self._get_or_create_worksheet(TAB_AGENT_TASKS, AGENT_TASK_HEADERS)

        # Check if product already exists in Agent Tasks
        existing = self._get_all_records(TAB_AGENT_TASKS, AGENT_TASK_HEADERS)
        existing_ids = {r.get("product_id", "") for r in existing}

        if product.product_id in existing_ids:
            logger.debug("Product %s already in Agent Tasks", product.product_id)
            return

        row = {
            "product_id": product.product_id,
            "keyword": product.keyword,
            "country": product.country,
            "google_shopping_url": product.google_shopping_url or "",
            "aliexpress_url": product.aliexpress_url or "",
            "aliexpress_price": product.aliexpress_price or "",
            "landed_cost": "",  # Agent fills this
            "agent_notes": "",  # Agent fills this
            "status": "pending",  # pending -> cost_filled | rejected
        }

        self._append_row(TAB_AGENT_TASKS, AGENT_TASK_HEADERS, row)
        logger.info("Added product to Agent Tasks: %s (%s)", product.keyword, product.product_id)

    def sync_all_sourcing_to_agent_tasks(self) -> int:
        """
        Sync all products with test_status='sourcing' to the Agent Tasks tab.
        Returns the number of new tasks added.
        """
        sourcing_products = self.get_products(status=ProductStatus.SOURCING.value)
        added = 0
        for product in sourcing_products:
            existing = self._get_all_records(TAB_AGENT_TASKS, AGENT_TASK_HEADERS)
            existing_ids = {r.get("product_id", "") for r in existing}
            if product.product_id not in existing_ids:
                self.sync_product_to_agent_tasks(product)
                added += 1
        if added:
            logger.info("Synced %d new products to Agent Tasks", added)
        return added

    def _get_agent_tasks_with_cost(self) -> list[dict]:
        """Get agent tasks where landed_cost has been filled in."""
        records = self._get_all_records(TAB_AGENT_TASKS, AGENT_TASK_HEADERS)
        completed = []
        for r in records:
            landed_cost = str(r.get("landed_cost", "")).strip()
            status = str(r.get("status", "")).strip().lower()
            if landed_cost and status != "processed":
                try:
                    float(landed_cost)
                    completed.append(r)
                except ValueError:
                    pass
        return completed

    def mark_agent_task_processed(self, product_id: str) -> None:
        """Mark an agent task as processed after the system picks up the cost."""
        self._update_row(
            TAB_AGENT_TASKS, AGENT_TASK_HEADERS,
            "product_id", product_id,
            {"status": "processed"}
        )

    # --- Action Log ---

    def add_log(self, log: ActionLog) -> None:
        self._append_row(TAB_ACTION_LOG, LOG_HEADERS, log.to_dict())

    def get_logs(self, product_id: str = None, limit: int = 100) -> list[ActionLog]:
        records = self._get_all_records(TAB_ACTION_LOG, LOG_HEADERS)
        logs = [ActionLog.from_dict(r) for r in records]
        if product_id:
            logs = [l for l in logs if l.product_id == product_id]
        # Sort by timestamp descending
        logs.sort(key=lambda x: x.timestamp, reverse=True)
        return logs[:limit]

    # --- Notifications ---

    def add_notification(self, notification: Notification) -> None:
        self._append_row(TAB_NOTIFICATIONS, NOTIFICATION_HEADERS, notification.to_dict())

    def get_notifications(self, unread_only: bool = False, limit: int = 50) -> list[Notification]:
        records = self._get_all_records(TAB_NOTIFICATIONS, NOTIFICATION_HEADERS)
        notifications = []
        for r in records:
            n = Notification(
                notification_id=str(r.get("notification_id", "")),
                title=str(r.get("title", "")),
                message=str(r.get("message", "")),
                level=str(r.get("level", "info")),
                read=str(r.get("read", "")).lower() in ("true", "1"),
                product_id=str(r.get("product_id", "")),
                timestamp=str(r.get("timestamp", ""))
            )
            notifications.append(n)
        if unread_only:
            notifications = [n for n in notifications if not n.read]
        notifications.sort(key=lambda x: x.timestamp, reverse=True)
        return notifications[:limit]

    def mark_notification_read(self, notification_id: str) -> None:
        self._update_row(
            TAB_NOTIFICATIONS, NOTIFICATION_HEADERS,
            "notification_id", notification_id,
            {"read": "True"}
        )

    # --- Config ---

    def get_config(self) -> dict:
        """Read config from the Config tab as key-value pairs."""
        ws = self._get_or_create_worksheet(TAB_CONFIG, ["key", "value"])
        try:
            records = ws.get_all_records()
            config = {}
            for r in records:
                key = str(r.get("key", ""))
                value = r.get("value", "")
                if key:
                    # Parse dot-notation keys into nested dict
                    keys = key.split(".")
                    d = config
                    for k in keys[:-1]:
                        if k not in d:
                            d[k] = {}
                        d = d[k]
                    # Try to parse numeric/boolean values
                    d[keys[-1]] = self._parse_value(value)
            return config
        except Exception as e:
            logger.error("Error reading config: %s", e)
            return {}

    def save_config(self, config: dict) -> None:
        """Save config to the Config tab as flattened key-value pairs."""
        ws = self._get_or_create_worksheet(TAB_CONFIG, ["key", "value"])
        # Flatten the nested dict
        flat = self._flatten_dict(config)
        # Clear existing content (except header)
        ws.clear()
        ws.update("A1", [["key", "value"]])
        ws.format("1", {"textFormat": {"bold": True}})
        # Write rows
        rows = [[k, str(v)] for k, v in flat.items()]
        if rows:
            ws.update(f"A2:B{len(rows) + 1}", rows, value_input_option="USER_ENTERED")
        logger.info("Saved %d config entries", len(rows))

    def get_countries(self) -> list[CountryConfig]:
        config = self.get_config()
        countries_raw = config.get("global", {}).get("countries", [])
        if isinstance(countries_raw, list):
            return [
                CountryConfig(
                    code=c.get("code", "DE"),
                    name=c.get("name", "Germany"),
                    language=c.get("language", "de"),
                    currency=c.get("currency", "EUR"),
                )
                for c in countries_raw
            ]
        return [CountryConfig()]

    # --- Research Feedback ---

    def get_research_feedback(self) -> dict:
        ws = self._get_or_create_worksheet(TAB_FEEDBACK, ["key", "value"])
        try:
            records = ws.get_all_records()
            feedback = {}
            for r in records:
                key = str(r.get("key", ""))
                value = r.get("value", "")
                if key:
                    feedback[key] = self._parse_value(value)
            return feedback
        except Exception:
            return {}

    def save_research_feedback(self, feedback: dict) -> None:
        ws = self._get_or_create_worksheet(TAB_FEEDBACK, ["key", "value"])
        ws.clear()
        ws.update("A1", [["key", "value"]])
        rows = [[k, str(v)] for k, v in feedback.items()]
        if rows:
            ws.update(f"A2:B{len(rows) + 1}", rows, value_input_option="USER_ENTERED")

    # --- Helpers ---

    @staticmethod
    def _flatten_dict(d: dict, parent_key: str = "", sep: str = ".") -> dict:
        """Flatten a nested dict to dot-notation keys."""
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(GoogleSheetsStore._flatten_dict(v, new_key, sep))
            elif isinstance(v, list):
                items[new_key] = str(v)
            else:
                items[new_key] = v
        return items

    @staticmethod
    def _parse_value(value):
        """Try to parse a string value into its appropriate type."""
        import ast

        if isinstance(value, (int, float, bool)):
            return value
        s = str(value).strip()
        if s.lower() in ("true", "false"):
            return s.lower() == "true"

        # Try to parse list/dict strings (e.g. stored countries config)
        if s.startswith(("[", "{")):
            try:
                return ast.literal_eval(s)
            except (ValueError, SyntaxError):
                pass

        try:
            if "." in s:
                return float(s)
            return int(s)
        except ValueError:
            return s


def get_data_store() -> DataStore:
    """Factory function to get the data store instance."""
    return GoogleSheetsStore()
