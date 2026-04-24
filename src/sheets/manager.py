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
import random
import time
from typing import Callable, Optional, TypeVar

import gspread
from gspread.exceptions import APIError as GspreadAPIError

T = TypeVar("T")

# In-memory read cache to avoid 429 (quota exceeded) from repeated Sheet reads.
# TTL in seconds; invalidated on any write to that tab.
_SHEETS_READ_CACHE: dict[str, tuple[float, list[dict]]] = {}
# Stale cache: keep last successful read so we can return it on 429 (quota exceeded).
_SHEETS_STALE_CACHE: dict[str, list[dict]] = {}
SHEETS_CACHE_TTL_SECONDS = 55  # Just under 1 minute; stay under read/minute quota

from src.core.config import (
    GOOGLE_SHEETS_AGENT_SPREADSHEET_ID,
    GOOGLE_SHEETS_CREDENTIALS_PATH,
    GOOGLE_SHEETS_SPREADSHEET_ID,
    get_service_account_credentials,
)
from src.core.interfaces import DataStore
from src.core.models import (
    Product, KeywordResearch, ActionLog, Notification, CountryConfig,
    ProductStatus
)

logger = logging.getLogger(__name__)


def _col_num_to_letter(col_num: int) -> str:
    """Convert 1-based column index to A1 letter (1→A, 27→AA, etc)."""
    result = ""
    n = col_num
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def _retry_on_429(fn: Callable[[], T], *, max_attempts: int = 5, op: str = "sheets-op") -> T:
    """Run `fn` and retry on HTTP 429 with exponential backoff + jitter.

    Google Sheets API v4 quotas per project per minute:
      - 60 writes/user/minute
      - 300 reads/user/minute
    Hitting either returns 429. We back off 2s, 4s, 8s, 16s, 32s (+ up to 50%
    jitter) to let the bucket refill, then give up. 5 attempts total.

    Other 4xx/5xx errors raise immediately — this wrapper is narrowly for 429.
    """
    backoffs = [2, 4, 8, 16, 32]
    for attempt in range(max_attempts):
        try:
            return fn()
        except GspreadAPIError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status != 429 or attempt == max_attempts - 1:
                raise
            delay = backoffs[min(attempt, len(backoffs) - 1)]
            delay += random.uniform(0, delay * 0.5)  # jitter up to +50%
            logger.warning(
                "Sheets 429 on %s (attempt %d/%d); sleeping %.1fs before retry",
                op, attempt + 1, max_attempts, delay,
            )
            time.sleep(delay)
    # Unreachable — the loop either returns or raises.
    raise RuntimeError(f"_retry_on_429 exhausted for {op}")

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
TAB_API_COSTS = "API Costs"
TAB_RESEARCH_DROPS = "Research Drops"

# Column headers for each tab
KEYWORD_HEADERS = [
    "keyword_id", "keyword", "country", "language",
    "monthly_search_volume", "estimated_cpc", "competition_level",
    "intent_score", "research_source", "competitor_count",
    "unique_product_count", "competition_type", "differentiation_score",
    "avg_competitor_price", "median_competitor_price", "estimated_selling_price",
    "google_shopping_url", "competitor_pdp_url", "competitor_thumbnail_url",
    "aliexpress_url", "aliexpress_price", "aliexpress_rating",
    "aliexpress_orders", "aliexpress_image_urls", "aliexpress_top3_json",
    "created_at", "notes", "status",
]

PRODUCT_HEADERS = [
    "product_id", "keyword_id", "country", "language", "keyword",
    "monthly_search_volume", "estimated_cpc", "competition_level",
    "competitor_count", "differentiation_score", "competition_type",
    "google_shopping_url", "competitor_pdp_url",
    "aliexpress_url", "aliexpress_price", "aliexpress_rating",
    "aliexpress_orders", "aliexpress_image_urls", "aliexpress_top3_json",
    "aliexpress_match_meta_json",
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
    "request_real_photos", "notes"
]

LOG_HEADERS = [
    "log_id", "product_id", "action_type", "old_status", "new_status",
    "reason", "details", "timestamp", "country"
]

API_COST_HEADERS = [
    "timestamp", "run_id", "run_type", "provider", "endpoint",
    "units", "cost_usd", "context", "estimated",
]

# Research Drops tab — one row per keyword rejected by the discover pipeline.
# Written at end-of-run by ResearchPipeline._finalize_drops so the funnel is
# queryable forever (in-memory `stats["dropped_keywords"]` is otherwise lost
# the moment the run finishes). Stages used today:
#   dedup | length | llm_price | llm_quality | llm_qa
#   volume | volume_no_data | cpc | competition | price | aliexpress | cap
# `aliexpress_soft` was retired 2026-04-21 — placeholder rows with
# manual-only URLs proved more noise than signal. Old drop history still
# carrying that stage is kept for audit but no new runs emit it.
RESEARCH_DROP_HEADERS = [
    "timestamp", "run_id", "country", "keyword", "stage", "reason",
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
        # Separate spreadsheet for the sourcing agent. When
        # GOOGLE_SHEETS_AGENT_SPREADSHEET_ID is unset or equal to the main
        # ID, this resolves to the same handle — preserving the legacy
        # single-sheet layout. When set to a different ID, agent-tasks
        # reads/writes go to that spreadsheet instead.
        self._agent_spreadsheet: Optional[gspread.Spreadsheet] = None

    def _connect(self):
        """Establish connection to Google Sheets. Retries once on 429 (quota exceeded)."""
        if self._client is not None:
            return

        last_error = None
        for attempt in range(2):
            try:
                if attempt > 0:
                    wait_sec = 30
                    logger.warning("Sheets 429 (quota); waiting %ds before retry...", wait_sec)
                    time.sleep(wait_sec)
                logger.info("Connecting to Google Sheets...")
                logger.info("Spreadsheet ID: '%s'", GOOGLE_SHEETS_SPREADSHEET_ID)

                creds = get_service_account_credentials(scopes=SCOPES)
                self._client = gspread.authorize(creds)
                logger.info("Authorized successfully, opening spreadsheet...")
                self._spreadsheet = self._client.open_by_key(GOOGLE_SHEETS_SPREADSHEET_ID.strip())
                logger.info("Connected to Google Sheets: %s", self._spreadsheet.title)
                # Open the agent spreadsheet separately if it's configured
                # to a different ID. If it's the same ID (or unset,
                # defaulting to main), reuse the same handle — no second
                # network call, no second quota charge. Fail-open: if the
                # agent sheet open fails, we fall back to the main sheet
                # so sourcing-sync keeps working rather than breaking the
                # entire pipeline over a sandboxing setting.
                agent_id = (GOOGLE_SHEETS_AGENT_SPREADSHEET_ID or "").strip()
                main_id = GOOGLE_SHEETS_SPREADSHEET_ID.strip()
                if agent_id and agent_id != main_id:
                    try:
                        self._agent_spreadsheet = self._client.open_by_key(agent_id)
                        logger.info(
                            "Connected to Agent Sheet: %s (separate from main)",
                            self._agent_spreadsheet.title,
                        )
                    except Exception as agent_err:
                        logger.warning(
                            "Could not open agent spreadsheet %s: %s — falling back to main sheet",
                            agent_id, agent_err,
                        )
                        self._agent_spreadsheet = self._spreadsheet
                else:
                    self._agent_spreadsheet = self._spreadsheet
                return
            except FileNotFoundError:
                logger.error("Service account JSON not found at: %s", GOOGLE_SHEETS_CREDENTIALS_PATH)
                raise
            except GspreadAPIError as e:
                last_error = e
                if getattr(e, "response", None) and getattr(e.response, "status_code", None) == 429:
                    if attempt == 0:
                        continue
                raise
            except Exception as e:
                logger.error("Failed to connect to Google Sheets: %s (type: %s)", e, type(e).__name__)
                raise
        if last_error is not None:
            raise last_error

    def _spreadsheet_for(self, tab_name: str) -> gspread.Spreadsheet:
        """Return the right gspread Spreadsheet handle for this tab.

        Agent Tasks lives on a separate spreadsheet (when configured) so
        the sourcing agent's service account can't read the rest of the
        pipeline. Everything else uses the main spreadsheet. If agent
        split isn't configured, both handles point at the same object.
        """
        if tab_name == TAB_AGENT_TASKS and self._agent_spreadsheet is not None:
            return self._agent_spreadsheet
        return self._spreadsheet

    def _get_or_create_worksheet(self, tab_name: str, headers: list[str]) -> gspread.Worksheet:
        """Get an existing worksheet or create it with headers.
        Also adds any missing columns to existing sheets (schema migration)."""
        self._connect()
        spreadsheet = self._spreadsheet_for(tab_name)
        try:
            ws = spreadsheet.worksheet(tab_name)
            # Check for missing columns and add them. `update_cell` can only
            # *fill* an existing grid cell — it can't widen the sheet — so
            # when our header list outgrows the sheet's current column count
            # (e.g. we added `status` to KEYWORD_HEADERS) we have to call
            # `add_cols` first, otherwise every write past the current last
            # column throws "exceeds grid limits" and the migration is a no-op.
            try:
                existing_headers = ws.row_values(1)
                # Callers that don't care about migrations pass `headers=[]`
                # (e.g. delete scripts that only want to find/delete rows).
                # Short-circuit here so we don't touch row 1 at all for those.
                if not headers:
                    return ws
                # Special case: sheet exists but row 1 is completely empty —
                # typically a tab created at setup time and never written to.
                # Dropping N `update_cell` calls in this state used to burn
                # quota and silently no-op on 429, leaving the sheet
                # permanently header-less and making every subsequent
                # `append_rows` effectively a no-op (the reason Research
                # Drops wasn't recording any drops). Do a single bulk write
                # in the empty case — atomic, quota-light, and far less
                # likely to fail on a warm sheet.
                if not existing_headers:
                    if ws.col_count < len(headers):
                        ws.add_cols(len(headers) - ws.col_count)
                    ws.update("A1", [headers])
                    ws.format("1", {"textFormat": {"bold": True}})
                    logger.info("Initialized empty headers on %s: %d cols", tab_name, len(headers))
                    return ws
                missing = [h for h in headers if h not in existing_headers]
                if missing:
                    start_col = len(existing_headers) + 1
                    needed_cols = start_col + len(missing) - 1
                    if ws.col_count < needed_cols:
                        ws.add_cols(needed_cols - ws.col_count)
                    for i, col_name in enumerate(missing):
                        ws.update_cell(1, start_col + i, col_name)
                    logger.info("Added %d missing columns to %s: %s", len(missing), tab_name, missing)
            except Exception as e:
                logger.warning("Could not check/add missing columns for %s: %s", tab_name, e)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=len(headers))
            ws.update("A1", [headers])
            ws.format("1", {"textFormat": {"bold": True}})
            logger.info(
                "Created new worksheet: %s on '%s'", tab_name, spreadsheet.title,
            )
        return ws

    def _invalidate_cache(self, tab_name: str) -> None:
        """Clear cached reads for a tab so the next read is fresh (used after writes)."""
        global _SHEETS_READ_CACHE
        _SHEETS_READ_CACHE.pop(tab_name, None)

    def _get_all_records(self, tab_name: str, headers: list[str]) -> list[dict]:
        """Get all records from a tab as list of dicts. Uses a short-lived cache to reduce API reads."""
        global _SHEETS_READ_CACHE, _SHEETS_STALE_CACHE
        now = time.time()
        entry = _SHEETS_READ_CACHE.get(tab_name)
        if entry is not None:
            expiry, records = entry
            if now < expiry:
                return records
            _SHEETS_READ_CACHE.pop(tab_name, None)
        try:
            ws = self._get_or_create_worksheet(tab_name, headers)
            records = ws.get_all_records()
            _SHEETS_READ_CACHE[tab_name] = (now + SHEETS_CACHE_TTL_SECONDS, records)
            _SHEETS_STALE_CACHE[tab_name] = records
            return records
        except GspreadAPIError as e:
            if e.response is not None and e.response.status_code == 429:
                stale = _SHEETS_STALE_CACHE.get(tab_name)
                if stale is not None:
                    logger.warning("Sheets API 429 (quota exceeded); returning cached data for %s", tab_name)
                    return stale
            logger.error("Error reading %s: %s", tab_name, e)
            raise
        except Exception as e:
            # Before: any non-gspread exception (auth failure, bad sheet ID,
            # service account revoked, TOML-mangled credential, network glitch)
            # was caught here, logged, and swallowed by returning []. The UI
            # then cheerfully showed "No products yet" because get_products()
            # saw zero rows — making a broken Cloud deploy look identical to
            # a fresh install with no data. Bit us in prod when secrets got
            # edited: 29 products on the sheet, "No products yet" on screen.
            #
            # Now: log and re-raise so callers + Streamlit surface the real
            # error. The UI is designed to catch exceptions around
            # get_data_store() / get_products() and render a red banner
            # with the exception message, which is infinitely better than
            # the misleading empty state.
            logger.error("Error reading %s: %s", tab_name, e)
            raise

    def _find_row_index(self, ws: gspread.Worksheet, id_column: str, id_value: str) -> Optional[int]:
        """Find the row index (1-based) for a record by its ID.

        gspread 6.x moved `CellNotFound` from the top-level module to
        `gspread.exceptions`. Old code referenced `gspread.CellNotFound`
        directly and crashed with AttributeError on upgrade. We try the
        new location first and fall back to the old name for older installs
        — and we catch a broad `Exception` as a final safety net because
        different gspread versions raise different types for "no match".
        """
        try:
            # gspread 6.x strictly requires `str` or `re.Pattern` as the
            # query type; passing an int (e.g. a keyword_id that happens
            # to be numeric, like `11480593` from a legacy manual-add)
            # trips "query must be of type: 'str' or 're.Pattern'" and
            # kills the update. Coerce defensively — sheet values are
            # always rendered as strings on the wire anyway.
            query = str(id_value) if id_value is not None else ""
            cell = ws.find(query, in_column=1)  # Assumes ID is in column 1
            return cell.row if cell else None
        except Exception as e:
            # Only swallow "cell not found"; re-raise anything else.
            name = type(e).__name__
            if name == "CellNotFound" or "not found" in str(e).lower():
                return None
            raise

    def _live_headers(self, ws: "gspread.Worksheet", expected: list[str]) -> list[str]:
        """Return the headers *as they exist in the sheet right now*.

        This is what writes must align to — our in-memory `headers` constant
        is the *intended* schema, but when the sheet is protected and
        `add_cols` has been blocked (a supported state: sheet owner
        explicitly wants protections on), the live sheet may be narrower
        than what the code expects. Writing past the last column raises
        "exceeds grid limits" and the whole write fails, so we intersect.

        Falls back to `expected` on any read error so we don't regress.
        """
        try:
            live = ws.row_values(1)
            return live if live else expected
        except Exception as e:
            logger.warning("Could not read live headers for %s: %s", ws.title, e)
            return expected

    def _append_row(self, tab_name: str, headers: list[str], data: dict):
        """Append a row to a tab. Wrapped in 429 backoff."""
        ws = self._get_or_create_worksheet(tab_name, headers)
        # Align the outgoing row to whatever columns the sheet *actually*
        # has — not our in-memory headers — so a schema drift (new field
        # added in code but column not yet added to the sheet) silently
        # drops the extra fields instead of breaking every write.
        live = self._live_headers(ws, headers)
        row = [str(data.get(h, "")) for h in live]
        _retry_on_429(
            lambda: ws.append_row(row, value_input_option="USER_ENTERED"),
            op=f"append_row({tab_name})",
        )
        self._invalidate_cache(tab_name)

    def _append_rows(self, tab_name: str, headers: list[str], data_rows: list[dict]):
        """Bulk-append N rows in a single API call. Far more quota-efficient
        than calling `_append_row` in a loop — 1 call vs N calls.
        """
        if not data_rows:
            return
        ws = self._get_or_create_worksheet(tab_name, headers)
        live = self._live_headers(ws, headers)
        rows = [[str(d.get(h, "")) for h in live] for d in data_rows]
        _retry_on_429(
            lambda: ws.append_rows(rows, value_input_option="USER_ENTERED"),
            op=f"append_rows({tab_name}, {len(rows)})",
        )
        self._invalidate_cache(tab_name)

    def _update_row(self, tab_name: str, headers: list[str], id_field: str, id_value: str, updates: dict) -> bool:
        """Update specific fields in a row using a single `batch_update` call
        instead of one `update_cell` per field (which used to spend 10+ API
        calls per product update and was the main 429 trigger).

        Returns True when a write landed (or would have landed — batch was
        empty because every updated field was filtered by skip-rules).
        Returns False when the row couldn't be located by id. Callers that
        need strict guarantees (e.g. the dashboard Save handler writing to
        Agent Tasks) should check the return value and raise — the silent
        no-op used to surface as a false "saved" banner.
        """
        ws = self._get_or_create_worksheet(tab_name, headers)
        row_idx = _retry_on_429(
            lambda: self._find_row_index(ws, id_field, id_value),
            op=f"find({tab_name}, {id_field}={id_value})",
        )
        if row_idx is None:
            logger.warning("Row not found for %s=%s in %s", id_field, id_value, tab_name)
            return False

        # Build a single batch_update payload covering all changed cells.
        # Column indices are keyed off the *live* sheet so we never try to
        # write past the last physical column (see `_live_headers`).
        live = self._live_headers(ws, headers)
        batch = []
        skipped: list[str] = []
        for field_name, value in updates.items():
            if field_name not in live:
                if field_name in headers:
                    skipped.append(field_name)
                continue
            col_idx = live.index(field_name) + 1
            col_letter = _col_num_to_letter(col_idx)
            batch.append({
                "range": f"{col_letter}{row_idx}",
                "values": [[str(value) if value is not None else ""]],
            })
        if skipped:
            logger.warning(
                "Skipped %d field(s) in %s — column(s) missing from sheet: %s. "
                "Add them to the sheet (or unprotect → let code auto-migrate → re-protect).",
                len(skipped), tab_name, skipped,
            )
        if not batch:
            return True
        _retry_on_429(
            lambda: ws.batch_update(batch, value_input_option="USER_ENTERED"),
            op=f"batch_update({tab_name}, {len(batch)} cells)",
        )
        self._invalidate_cache(tab_name)
        return True

    # --- Keywords ---

    def get_keywords(self, country: str = None, status: str = None) -> list[KeywordResearch]:
        """Return keywords, optionally filtered by country and inbox status.

        status == "active"    → treat empty/blank status as active (default view)
        status == "archived"  → only keywords the human explicitly archived
        status == "sent_to_sourcing" → already promoted to Products
        status is None        → no filter (old callers that want everything)
        """
        records = self._get_all_records(TAB_KEYWORDS, KEYWORD_HEADERS)
        keywords = [KeywordResearch.from_dict(r) for r in records]
        if country:
            keywords = [k for k in keywords if k.country == country]
        if status is not None:
            if status == "active":
                keywords = [k for k in keywords if (k.status or "") in ("", "active")]
            else:
                keywords = [k for k in keywords if k.status == status]
        return keywords

    def add_keyword(self, keyword: KeywordResearch) -> None:
        self._append_row(TAB_KEYWORDS, KEYWORD_HEADERS, keyword.to_dict())
        logger.info("Added keyword: %s (%s)", keyword.keyword, keyword.country)

    def add_keywords_bulk(self, keywords: list[KeywordResearch]) -> None:
        """Single-call bulk append — one API call instead of N."""
        if not keywords:
            return
        self._append_rows(TAB_KEYWORDS, KEYWORD_HEADERS, [k.to_dict() for k in keywords])
        logger.info("Added %d keywords in 1 bulk append", len(keywords))

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

    def add_products_bulk(self, products: list[Product]) -> None:
        """Single-call bulk append — one API call instead of N."""
        if not products:
            return
        self._append_rows(TAB_PRODUCTS, PRODUCT_HEADERS, [p.to_dict() for p in products])
        logger.info("Added %d products in 1 bulk append", len(products))

    def update_product(self, product_id: str, updates: dict) -> None:
        from datetime import datetime
        updates["updated_at"] = datetime.utcnow().isoformat()
        self._update_row(TAB_PRODUCTS, PRODUCT_HEADERS, "product_id", product_id, updates)

    def delete_product(self, product_id: str) -> bool:
        """Hard-delete a product row. Also scrubs the matching Agent Tasks row
        if one exists so the agent's queue doesn't keep showing it.

        Returns True if the row was found and removed.
        """
        ws = self._get_or_create_worksheet(TAB_PRODUCTS, PRODUCT_HEADERS)
        row_idx = self._find_row_index(ws, "product_id", product_id)
        if not row_idx:
            return False
        ws.delete_rows(row_idx)

        # Scrub Agent Tasks for the same product so the agent stops seeing it.
        # Best-effort — if the tab doesn't exist yet or the row is absent, no-op.
        try:
            # Note: Agent Tasks's ID column is `product_id` at column 1, same
            # layout as Products — `_find_row_index` assumes column 1.
            agent_ws = self._get_or_create_worksheet(TAB_AGENT_TASKS, AGENT_TASK_HEADERS)
            agent_row = self._find_row_index(agent_ws, "product_id", product_id)
            if agent_row:
                agent_ws.delete_rows(agent_row)
        except Exception as e:
            logger.debug("Agent Tasks cleanup skipped for %s: %s", product_id, e)
        logger.info("Deleted product %s", product_id)
        return True

    def delete_keyword(self, keyword_id: str) -> bool:
        """Hard-delete a keyword row. Returns True if the row was found."""
        ws = self._get_or_create_worksheet(TAB_KEYWORDS, KEYWORD_HEADERS)
        row_idx = self._find_row_index(ws, "keyword_id", keyword_id)
        if not row_idx:
            return False
        ws.delete_rows(row_idx)
        logger.info("Deleted keyword %s", keyword_id)
        return True

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

        Performance: reads the Agent Tasks tab ONCE up-front (was previously
        re-read per product inside the loop → N+1 reads → 429 on large
        pipelines) and writes all new rows in a single `_append_rows` call
        (was N append_row calls = N API hits). For a 71-product pipeline run
        this drops from ~142 API calls to 2.
        """
        sourcing_products = self.get_products(status=ProductStatus.SOURCING.value)
        if not sourcing_products:
            return 0

        # One read of the current Agent Tasks tab up-front.
        existing = self._get_all_records(TAB_AGENT_TASKS, AGENT_TASK_HEADERS)
        existing_ids = {r.get("product_id", "") for r in existing}

        # Build the new rows in memory (no API calls).
        new_rows: list[dict] = []
        for product in sourcing_products:
            if product.product_id in existing_ids:
                continue
            new_rows.append({
                "product_id": product.product_id,
                "keyword": product.keyword,
                "country": product.country,
                "google_shopping_url": product.google_shopping_url or "",
                "aliexpress_url": product.aliexpress_url or "",
                "aliexpress_price": product.aliexpress_price or "",
                "landed_cost": "",
                "agent_notes": "",
                "status": "pending",
            })

        if not new_rows:
            return 0

        # Single bulk-append.
        self._append_rows(TAB_AGENT_TASKS, AGENT_TASK_HEADERS, new_rows)
        logger.info("Synced %d new products to Agent Tasks (1 bulk append)", len(new_rows))
        return len(new_rows)

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

    def update_agent_task(self, product_id: str, updates: dict) -> None:
        """Update arbitrary fields on an existing Agent Tasks row.

        Used when the dashboard-side user edits fields (AliExpress URL /
        price) on a product that's already queued for the agent — we want
        the agent's sheet to reflect the latest values so they don't waste
        time sourcing from a stale link.

        Raises `LookupError` if the product_id isn't on the Agent Tasks
        tab. The previous silent-no-op behaviour let a "row not found"
        condition masquerade as a successful save — the dashboard's
        "Saved — all updated" banner lied and the agent's sheet stayed
        stale. Callers that genuinely want upsert semantics should catch
        LookupError + call `sync_product_to_agent_tasks` to append.
        """
        wrote = self._update_row(
            TAB_AGENT_TASKS, AGENT_TASK_HEADERS,
            "product_id", product_id,
            updates,
        )
        if not wrote:
            raise LookupError(
                f"Agent Tasks row not found for product_id={product_id!r}"
            )

    # --- Action Log ---

    def add_log(self, log: ActionLog) -> None:
        self._append_row(TAB_ACTION_LOG, LOG_HEADERS, log.to_dict())

    def add_logs_bulk(self, logs: list[ActionLog]) -> None:
        """Single-call bulk append — one API call instead of N."""
        if not logs:
            return
        self._append_rows(TAB_ACTION_LOG, LOG_HEADERS, [l.to_dict() for l in logs])

    def get_logs(self, product_id: str = None, limit: int = 100) -> list[ActionLog]:
        records = self._get_all_records(TAB_ACTION_LOG, LOG_HEADERS)
        logs = [ActionLog.from_dict(r) for r in records]
        if product_id:
            logs = [l for l in logs if l.product_id == product_id]
        # Sort by timestamp descending
        logs.sort(key=lambda x: x.timestamp, reverse=True)
        return logs[:limit]

    # --- API Costs ---

    def append_cost_records(self, records: list[dict]) -> None:
        """Append a batch of CostTracker rows to the API Costs tab in a
        single API call. Called once at the end of each Discover / Page
        Clone run by `CostTracker.persist()`.
        """
        if not records:
            return
        self._append_rows(TAB_API_COSTS, API_COST_HEADERS, records)

    def get_cost_records(self, since: Optional[str] = None, run_type: Optional[str] = None, limit: int = 1000) -> list[dict]:
        """Fetch cost records for reporting. `since` is an ISO timestamp
        filter. Returns newest-first up to `limit`.
        """
        records = self._get_all_records(TAB_API_COSTS, API_COST_HEADERS)
        if since:
            records = [r for r in records if (r.get("timestamp") or "") >= since]
        if run_type:
            records = [r for r in records if r.get("run_type") == run_type]
        records.sort(key=lambda r: r.get("timestamp") or "", reverse=True)
        return records[:limit]

    # --- Research drops ---

    def append_drop_records(self, records: list[dict]) -> None:
        """Bulk-append dropped-keyword rows to the Research Drops tab. Called
        once per country run by `ResearchPipeline._finalize_drops`. Silent
        no-op on empty input so callers can pass through without guarding.
        """
        if not records:
            return
        self._append_rows(TAB_RESEARCH_DROPS, RESEARCH_DROP_HEADERS, records)

    def get_drop_records(
        self,
        run_id: Optional[str] = None,
        country: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 5000,
    ) -> list[dict]:
        """Fetch drop rows for reporting. All filters are optional. Newest-
        first ordering so the dashboard's "most recent run" view is a simple
        prefix slice."""
        records = self._get_all_records(TAB_RESEARCH_DROPS, RESEARCH_DROP_HEADERS)
        if run_id:
            records = [r for r in records if r.get("run_id") == run_id]
        if country:
            records = [r for r in records if r.get("country") == country]
        if since:
            records = [r for r in records if (r.get("timestamp") or "") >= since]
        records.sort(key=lambda r: r.get("timestamp") or "", reverse=True)
        return records[:limit]

    # Stages the pipeline treats as permanent "don't re-research" signals
    # when re-checking LLM output against drop history. Every stage here
    # represents a judgment we don't want to spend budget re-litigating.
    # Note: `aliexpress_soft` was retired 2026-04-21 — placeholder rows
    # were replaced with hard drops under stage="aliexpress", which is
    # already in this set. `economics` was added when per-keyword
    # max_allowed_cpc / min_margin gating moved into Step 4 — if a
    # keyword's selling-price vs landed-cost economics don't work today
    # they won't work on a re-run either.
    PERMANENT_DROP_STAGES = frozenset({
        "dedup", "length",
        "llm_price", "llm_quality", "llm_qa",
        "volume", "volume_no_data", "cpc",
        "competition", "price", "aliexpress",
        "economics",
    })

    def get_dropped_keyword_set(self, country: Optional[str] = None) -> set[str]:
        """Return the lowercase set of keywords that have been permanently
        dropped in past runs. Used by the discover pipeline's dedup step so
        the LLM never re-pays for ideation candidates we already decided
        not to pursue.

        Scopes by country when provided — DE-dropped keywords may still be
        worth researching in US, but not vice versa within the same market.

        Reads once per Discover run; the per-run cost is one Sheets read
        against a tab that grows ~50 rows per run, capped at 10k rows by
        the data layer.
        """
        records = self._get_all_records(TAB_RESEARCH_DROPS, RESEARCH_DROP_HEADERS)
        out: set[str] = set()
        for r in records:
            if r.get("stage") not in self.PERMANENT_DROP_STAGES:
                continue
            if country and r.get("country") != country:
                continue
            kw = (r.get("keyword") or "").strip().lower()
            if kw:
                out.add(kw)
        return out

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


_singleton_store: Optional[GoogleSheetsStore] = None


def get_data_store() -> DataStore:
    """Return the singleton data store so we reuse one connection and avoid 429 quota errors."""
    global _singleton_store
    if _singleton_store is None:
        _singleton_store = GoogleSheetsStore()
    return _singleton_store
