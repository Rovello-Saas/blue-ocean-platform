"""Quick test to debug Google Sheets connection."""
import os
from dotenv import load_dotenv
load_dotenv()

creds_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH", "credentials/google_service_account.json")
sheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "")

print(f"Credentials path: {creds_path}")
print(f"Credentials file exists: {os.path.exists(creds_path)}")
print(f"Sheet ID: '{sheet_id}'")
print(f"Sheet ID length: {len(sheet_id)}")
print()

try:
    from google.oauth2.service_account import Credentials
    import gspread

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    print("Loading credentials...")
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    print(f"Service account email: {creds.service_account_email}")

    print("Authorizing with Google...")
    client = gspread.authorize(creds)
    print("Authorized!")

    print(f"Opening spreadsheet '{sheet_id}'...")
    spreadsheet = client.open_by_key(sheet_id)
    print(f"SUCCESS! Connected to: {spreadsheet.title}")
    print(f"Tabs: {[ws.title for ws in spreadsheet.worksheets()]}")

except Exception as e:
    print(f"FAILED: {e}")
    print(f"Error type: {type(e).__name__}")
    import traceback
    traceback.print_exc()
