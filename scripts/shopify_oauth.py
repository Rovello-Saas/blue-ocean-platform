"""
Shopify OAuth flow to obtain Admin API access token.
"""
import os
import sys
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("SHOPIFY_API_KEY", "")
CLIENT_SECRET = os.getenv("SHOPIFY_API_SECRET", "")
SHOP = os.getenv("SHOPIFY_SHOP_URL", "").replace("https://", "").replace("http://", "").strip("/")
SCOPES = "write_products,read_products,write_inventory,read_inventory"
REDIRECT_URI = "http://localhost:9876/callback"

auth_code_received = None


class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code_received
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        
        if "code" in params:
            auth_code_received = params["code"][0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h1>Success! You can close this window.</h1>")
        else:
            self.send_response(400)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h1>Error: no code received</h1>")
    
    def log_message(self, format, *args):
        pass  # Suppress logs


def main():
    if not CLIENT_ID or not CLIENT_SECRET or not SHOP:
        print("ERROR: Missing SHOPIFY_API_KEY, SHOPIFY_API_SECRET, or SHOPIFY_SHOP_URL in .env")
        sys.exit(1)
    
    # Build authorization URL
    auth_url = (
        f"https://{SHOP}/admin/oauth/authorize"
        f"?client_id={CLIENT_ID}"
        f"&scope={SCOPES}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
    )
    
    print("\n=== Shopify OAuth Flow ===\n")
    print("Open this URL in your browser:\n")
    print(auth_url)
    print("\n(If the redirect doesn't work automatically, paste the full redirect URL here)\n")
    
    # Try local server first
    try:
        server = HTTPServer(("localhost", 9876), CallbackHandler)
        server.timeout = 120
        print("Waiting for callback on http://localhost:9876 ...")
        
        while auth_code_received is None:
            server.handle_request()
        
        server.server_close()
        code = auth_code_received
    except Exception:
        # Manual fallback
        print("\nLocal server failed. Paste the full redirect URL here:")
        redirect_url = input("> ").strip()
        parsed = urllib.parse.urlparse(redirect_url)
        params = urllib.parse.parse_qs(parsed.query)
        code = params.get("code", [None])[0]
        if not code:
            print("ERROR: No authorization code found in URL")
            sys.exit(1)
    
    print(f"\nGot authorization code: {code[:10]}...")
    
    # Exchange code for access token
    token_url = f"https://{SHOP}/admin/oauth/access_token"
    resp = requests.post(token_url, json={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
    })
    
    if resp.status_code == 200:
        data = resp.json()
        token = data.get("access_token", "")
        print(f"\n✅ SUCCESS! Your Admin API access token:\n")
        print(f"   {token}")
        print(f"\nAdd this to your .env file as SHOPIFY_ACCESS_TOKEN={token}")
    else:
        print(f"\n❌ Error {resp.status_code}: {resp.text}")


if __name__ == "__main__":
    main()
