"""
Google Ads OAuth flow — mint a refresh token for the google-ads-python client.

Uses the Desktop OAuth client configured under blue-ocean-platform-493819.
The browser flow must be completed as the Google account that has access to
the target Ads entity:

    - For the MCC login customer (8939830325 "Blue Ocean Commerce"):
      sign in as info@rovelloshop.com (that's where the MCC was created)
    - For the client account (1884652074 "Movanella"):
      info@rovelloshop.com also has access since the MCC was linked to it,
      so the same account covers both calls.

Requested scope is "https://www.googleapis.com/auth/adwords" only — that's
all google-ads-python needs. The refresh token printed at the end goes into
.env as GOOGLE_ADS_REFRESH_TOKEN.

Run:  python3 scripts/google_ads_oauth.py
"""
import os
import sys
from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow

load_dotenv()

CLIENT_ID = os.getenv("GOOGLE_ADS_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("GOOGLE_ADS_CLIENT_SECRET", "")
SCOPES = ["https://www.googleapis.com/auth/adwords"]


def main() -> None:
    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERROR: GOOGLE_ADS_CLIENT_ID / GOOGLE_ADS_CLIENT_SECRET missing in .env")
        sys.exit(1)

    # Build an InstalledAppFlow from a client_config dict so we don't need a
    # client_secret.json on disk — we already keep the values in .env.
    client_config = {
        "installed": {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }

    flow = InstalledAppFlow.from_client_config(client_config, scopes=SCOPES)

    print("\n=== Google Ads OAuth — Desktop flow ===")
    print("A browser window will open. Sign in as info@rovelloshop.com")
    print("(the account that owns MCC 893-983-0325 and has access to client")
    print("account 188-465-2074 Movanella). Grant the 'Manage your AdWords")
    print("campaigns' scope.\n")

    # run_local_server spins up an ephemeral loopback listener, opens the
    # browser, and handles the redirect automatically. port=0 lets the OS
    # pick a free port; prompt="consent" forces Google to re-issue a refresh
    # token even if this account has previously consented.
    creds = flow.run_local_server(
        port=0,
        prompt="consent",
        access_type="offline",
        open_browser=True,
        authorization_prompt_message="If the browser didn't open, visit:\n\n{url}\n",
        success_message="OAuth complete. You can close this window.",
    )

    if not creds.refresh_token:
        print("\nERROR: Google did not return a refresh_token.")
        print("This usually means the account has already granted consent and")
        print("Google is reusing the cached token. Revoke at")
        print("https://myaccount.google.com/permissions and re-run.")
        sys.exit(2)

    print("\nRefresh token (paste into .env as GOOGLE_ADS_REFRESH_TOKEN):\n")
    print(f"    {creds.refresh_token}\n")


if __name__ == "__main__":
    main()
