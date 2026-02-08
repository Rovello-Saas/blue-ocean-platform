"""
Setup script to create all required tabs and columns in the Google Sheet.
Run once to initialize the sheet structure.

Usage:
    python3 setup_sheet.py
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

creds_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH", "credentials/google_service_account.json")
sheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "")

print(f"Credentials path: {creds_path}")
print(f"Sheet ID: {sheet_id}")
print()

if not sheet_id:
    print("ERROR: GOOGLE_SHEETS_SPREADSHEET_ID not set in .env")
    sys.exit(1)

try:
    from google.oauth2.service_account import Credentials
    import gspread

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    print("Connecting to Google Sheets...")
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id.strip())
    print(f"Connected to: {spreadsheet.title}")
    print()

    # -------------------------------------------------------------------------
    # Tab definitions: (tab_name, headers, column_widths, notes)
    # -------------------------------------------------------------------------

    TABS = {
        "Products": {
            "headers": [
                "product_id", "keyword_id", "country", "language", "keyword",
                "monthly_search_volume", "estimated_cpc", "competition_level",
                "competitor_count", "differentiation_score", "competition_type",
                "google_shopping_url",
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
                "request_real_photos",
            ],
            # Columns the agent needs to fill (1-indexed for Sheets)
            "agent_columns": {
                "R": "landed_cost",  # Column R = landed_cost
            },
        },
        "Keywords": {
            "headers": [
                "keyword_id", "keyword", "country", "language",
                "monthly_search_volume", "estimated_cpc", "competition_level",
                "intent_score", "research_source", "competitor_count",
                "unique_product_count", "competition_type", "differentiation_score",
                "avg_competitor_price", "median_competitor_price", "estimated_selling_price",
                "google_shopping_url",
                "aliexpress_url", "aliexpress_price", "aliexpress_rating",
                "aliexpress_orders", "aliexpress_image_urls", "created_at", "notes",
            ],
        },
        "Config": {
            "headers": ["key", "value"],
        },
        "Action Log": {
            "headers": [
                "log_id", "product_id", "action_type", "old_status", "new_status",
                "reason", "details", "timestamp", "country",
            ],
        },
        "Notifications": {
            "headers": [
                "notification_id", "title", "message", "level", "read",
                "product_id", "timestamp",
            ],
        },
        "Agent Tasks": {
            "headers": [
                "product_id", "keyword", "country",
                "google_shopping_url", "aliexpress_url", "aliexpress_price",
                "landed_cost", "agent_notes", "status",
            ],
        },
        "Feedback": {
            "headers": ["key", "value"],
        },
    }

    # -------------------------------------------------------------------------
    # Create tabs
    # -------------------------------------------------------------------------

    existing_tabs = {ws.title for ws in spreadsheet.worksheets()}
    print(f"Existing tabs: {existing_tabs}")
    print()

    for tab_name, tab_config in TABS.items():
        headers = tab_config["headers"]

        if tab_name in existing_tabs:
            print(f"  Tab '{tab_name}' already exists — updating headers...")
            ws = spreadsheet.worksheet(tab_name)
            # Update header row
            ws.update("A1", [headers], value_input_option="RAW")
        else:
            print(f"  Creating tab '{tab_name}'...")
            ws = spreadsheet.add_worksheet(
                title=tab_name, rows=1000, cols=len(headers)
            )
            ws.update("A1", [headers], value_input_option="RAW")

        # Bold header row
        ws.format("1", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.9, "green": 0.93, "blue": 0.98},
        })

        # Freeze header row
        ws.freeze(rows=1)

        print(f"    ✓ {tab_name}: {len(headers)} columns")

    # -------------------------------------------------------------------------
    # Format the Agent Tasks tab (agent-friendly)
    # -------------------------------------------------------------------------

    print()
    print("Formatting Agent Tasks tab...")
    agent_ws = spreadsheet.worksheet("Agent Tasks")

    # Highlight the landed_cost column header (column F) in yellow
    agent_ws.format("F1", {
        "textFormat": {"bold": True, "fontSize": 11,
                       "foregroundColor": {"red": 0.6, "green": 0.0, "blue": 0.0}},
        "backgroundColor": {"red": 1.0, "green": 0.95, "blue": 0.6},
    })
    print("    ✓ Highlighted 'landed_cost' column (F) — agent fills this")

    # Highlight the agent_notes column header (column G) in light yellow
    agent_ws.format("G1", {
        "textFormat": {"bold": True, "fontSize": 11},
        "backgroundColor": {"red": 1.0, "green": 0.98, "blue": 0.8},
    })
    print("    ✓ Highlighted 'agent_notes' column (G)")

    # Add note with instructions to landed_cost header
    agent_ws.update_note("F1",
        "Fill this column with the TOTAL LANDED COST in EUR.\n\n"
        "This is the total cost to deliver the product\n"
        "to the customer, including:\n"
        "• Product cost\n"
        "• Shipping cost to customer\n\n"
        "The system will automatically process the product\n"
        "once you enter a number here."
    )
    print("    ✓ Added instructions note to landed_cost header")

    # Add note to status column
    agent_ws.update_note("H1",
        "Status values:\n"
        "• pending = waiting for your input\n"
        "• processed = system has picked up the cost\n\n"
        "Do not edit this column."
    )

    # Set column widths for readability
    try:
        requests_body = {"requests": [
            {"updateDimensionProperties": {
                "range": {"sheetId": agent_ws.id, "dimension": "COLUMNS",
                          "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 120}, "fields": "pixelSize"}},
            {"updateDimensionProperties": {
                "range": {"sheetId": agent_ws.id, "dimension": "COLUMNS",
                          "startIndex": 1, "endIndex": 2},
                "properties": {"pixelSize": 250}, "fields": "pixelSize"}},
            {"updateDimensionProperties": {
                "range": {"sheetId": agent_ws.id, "dimension": "COLUMNS",
                          "startIndex": 2, "endIndex": 3},
                "properties": {"pixelSize": 80}, "fields": "pixelSize"}},
            {"updateDimensionProperties": {
                "range": {"sheetId": agent_ws.id, "dimension": "COLUMNS",
                          "startIndex": 3, "endIndex": 4},
                "properties": {"pixelSize": 350}, "fields": "pixelSize"}},
            {"updateDimensionProperties": {
                "range": {"sheetId": agent_ws.id, "dimension": "COLUMNS",
                          "startIndex": 4, "endIndex": 5},
                "properties": {"pixelSize": 120}, "fields": "pixelSize"}},
            {"updateDimensionProperties": {
                "range": {"sheetId": agent_ws.id, "dimension": "COLUMNS",
                          "startIndex": 5, "endIndex": 6},
                "properties": {"pixelSize": 130}, "fields": "pixelSize"}},
            {"updateDimensionProperties": {
                "range": {"sheetId": agent_ws.id, "dimension": "COLUMNS",
                          "startIndex": 6, "endIndex": 7},
                "properties": {"pixelSize": 200}, "fields": "pixelSize"}},
            {"updateDimensionProperties": {
                "range": {"sheetId": agent_ws.id, "dimension": "COLUMNS",
                          "startIndex": 7, "endIndex": 8},
                "properties": {"pixelSize": 100}, "fields": "pixelSize"}},
        ]}
        spreadsheet.batch_update(requests_body)
        print("    ✓ Set column widths for readability")
    except Exception as e:
        print(f"    ⚠ Could not set column widths: {e}")

    # -------------------------------------------------------------------------
    # Remove the default 'Sheet1' tab if it exists and is empty
    # -------------------------------------------------------------------------

    try:
        sheet1 = spreadsheet.worksheet("Sheet1")
        if not sheet1.get_all_values() or sheet1.get_all_values() == [[]]:
            spreadsheet.del_worksheet(sheet1)
            print("\n  Removed empty 'Sheet1' tab")
    except gspread.WorksheetNotFound:
        pass

    # -------------------------------------------------------------------------
    # Done
    # -------------------------------------------------------------------------

    print()
    print("=" * 60)
    print("SETUP COMPLETE!")
    print("=" * 60)
    print()
    print("Sheet structure created:")
    print("  • Products    — Full product data (system-managed)")
    print("  • Agent Tasks — Simple view for agent (fills landed_cost)")
    print("  • Keywords    — Research results from AI discovery")
    print("  • Config      — Dashboard settings (auto-managed)")
    print("  • Action Log  — Audit trail of all automated decisions")
    print("  • Notifications — Dashboard alerts")
    print("  • Feedback    — AI learning data from winners/losers")
    print()
    print("AGENT WORKFLOW:")
    print("  1. System adds rows to 'Agent Tasks' tab automatically")
    print("  2. Agent fills in 'landed_cost' column (F)")
    print("  3. System picks up the cost every 30 minutes")
    print("  4. Product is automatically validated and processed")
    print()

except Exception as e:
    print(f"FAILED: {e}")
    import traceback
    traceback.print_exc()
