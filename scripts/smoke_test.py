"""
Unified API smoke test for Blue Ocean Platform.

Verifies every external credential in .env is still live, bound to the
correct entity, and able to perform a minimal real call. Use this after
any credential rotation, billing change, or migration.

    python3 scripts/smoke_test.py
    python3 scripts/smoke_test.py --only google_ads aliexpress

Exits 0 only if every check passes. Each check is isolated — one failure
doesn't abort the suite, so you can see the full state of the stack.

Minimal side effects / costs:
  - Google Ads: ~2 free GAQL reads
  - Google Sheets: one worksheet read
  - AliExpress: 3 read-only calls (category/feed/feed.get)
  - OpenAI: one completion of ~10 tokens (~$0.00003)
  - Gemini: one generation of ~10 tokens (free tier)
  - SerpAPI: one search (~$0.01 or free-tier credit)
  - Shopify: one shop.json read (free)
  - Fal.ai: NO network call — only key-format sanity check
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from dotenv import load_dotenv

# Make the project root importable for src.research.aliexpress etc.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

load_dotenv(REPO_ROOT / ".env")


# -----------------------------------------------------------------------------
# Output helpers
# -----------------------------------------------------------------------------

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
GREY = "\033[90m"
BOLD = "\033[1m"
RESET = "\033[0m"


@dataclass
class Result:
    name: str
    status: str  # "pass" | "fail" | "skip"
    message: str = ""
    duration_s: float = 0.0
    details: list[str] = field(default_factory=list)

    def emoji(self) -> str:
        return {"pass": "✓", "fail": "✗", "skip": "~"}[self.status]

    def color(self) -> str:
        return {"pass": GREEN, "fail": RED, "skip": YELLOW}[self.status]


def _run_check(name: str, fn: Callable[[], tuple[str, list[str]]]) -> Result:
    start = time.time()
    try:
        msg, details = fn()
        return Result(name=name, status="pass", message=msg, duration_s=time.time() - start, details=details)
    except SkipCheck as e:
        return Result(name=name, status="skip", message=str(e), duration_s=time.time() - start)
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        return Result(
            name=name, status="fail", message=f"{type(e).__name__}: {e}",
            duration_s=time.time() - start, details=[tb.strip()],
        )


class SkipCheck(Exception):
    """Raise to mark a check as skipped (credentials missing, etc)."""


def _require_env(*keys: str) -> None:
    missing = [k for k in keys if not os.getenv(k) or os.getenv(k, "").startswith("your_")]
    if missing:
        raise SkipCheck(f"missing env vars: {', '.join(missing)}")


# -----------------------------------------------------------------------------
# Individual API checks
# -----------------------------------------------------------------------------

def check_openai() -> tuple[str, list[str]]:
    _require_env("OPENAI_API_KEY")
    from openai import OpenAI  # type: ignore
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": "Reply with the single word: ok"}],
        max_tokens=5,
        temperature=0,
    )
    text = (resp.choices[0].message.content or "").strip().lower()
    return f"completion returned '{text}'", []


def check_google_ads() -> tuple[str, list[str]]:
    _require_env(
        "GOOGLE_ADS_DEVELOPER_TOKEN",
        "GOOGLE_ADS_CLIENT_ID",
        "GOOGLE_ADS_CLIENT_SECRET",
        "GOOGLE_ADS_REFRESH_TOKEN",
        "GOOGLE_ADS_CUSTOMER_ID",
        "GOOGLE_ADS_LOGIN_CUSTOMER_ID",
    )
    from google.ads.googleads.client import GoogleAdsClient  # type: ignore

    cfg = {
        "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
        "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
        "login_customer_id": os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"],
        "use_proto_plus": True,
    }
    client = GoogleAdsClient.load_from_dict(cfg)

    details = []

    # 1. Access check
    cust_svc = client.get_service("CustomerService")
    resources = cust_svc.list_accessible_customers().resource_names
    details.append(f"accessible customers: {len(resources)}")

    expected_mcc = f"customers/{os.environ['GOOGLE_ADS_LOGIN_CUSTOMER_ID']}"
    if expected_mcc not in resources:
        raise RuntimeError(f"LOGIN_CUSTOMER_ID ({expected_mcc}) not in accessible list")

    # 2. GAQL against the client account (cascaded via MCC login)
    ga_svc = client.get_service("GoogleAdsService")
    query = (
        "SELECT customer.id, customer.descriptive_name, customer.currency_code, "
        "customer.time_zone FROM customer LIMIT 1"
    )
    response = ga_svc.search(customer_id=os.environ["GOOGLE_ADS_CUSTOMER_ID"], query=query)
    row = next(iter(response), None)
    if not row:
        raise RuntimeError("GAQL customer query returned no rows")

    cust = row.customer
    details.append(f"customer: id={cust.id} name='{cust.descriptive_name}'")
    details.append(f"currency={cust.currency_code} tz={cust.time_zone}")

    return f"MCC → client cascade OK ({cust.currency_code}/{cust.time_zone})", details


def check_google_sheets() -> tuple[str, list[str]]:
    _require_env("GOOGLE_SHEETS_CREDENTIALS_PATH", "GOOGLE_SHEETS_SPREADSHEET_ID")
    from google.oauth2.service_account import Credentials  # type: ignore
    import gspread  # type: ignore

    creds_path = os.environ["GOOGLE_SHEETS_CREDENTIALS_PATH"]
    if not Path(creds_path).is_absolute():
        creds_path = str(REPO_ROOT / creds_path)
    if not Path(creds_path).exists():
        raise RuntimeError(f"service account JSON not found at {creds_path}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(os.environ["GOOGLE_SHEETS_SPREADSHEET_ID"])

    title = sh.title
    ws_names = [ws.title for ws in sh.worksheets()]
    details = [
        f"spreadsheet: '{title}'",
        f"worksheets: {len(ws_names)} ({', '.join(ws_names[:5])}{'...' if len(ws_names) > 5 else ''})",
        f"service_account: {creds.service_account_email}",
    ]
    return f"'{title}' with {len(ws_names)} tabs", details


def check_shopify() -> tuple[str, list[str]]:
    _require_env("SHOPIFY_SHOP_URL", "SHOPIFY_ACCESS_TOKEN")
    import requests  # type: ignore

    shop_url = os.environ["SHOPIFY_SHOP_URL"].replace("https://", "").strip("/")
    token = os.environ["SHOPIFY_ACCESS_TOKEN"]

    resp = requests.get(
        f"https://{shop_url}/admin/api/2024-10/shop.json",
        headers={"X-Shopify-Access-Token": token},
        timeout=10,
    )
    resp.raise_for_status()
    shop = resp.json().get("shop", {})

    details = [
        f"shop: '{shop.get('name')}' ({shop.get('myshopify_domain')})",
        f"primary_domain: {shop.get('domain')}",
        f"currency: {shop.get('currency')}",
        f"plan: {shop.get('plan_name')}",
    ]
    return f"'{shop.get('name')}' live", details


def check_aliexpress() -> tuple[str, list[str]]:
    _require_env("ALIEXPRESS_APP_KEY", "ALIEXPRESS_APP_SECRET", "ALIEXPRESS_TRACKING_ID")
    from src.research import aliexpress  # type: ignore

    cats = aliexpress.list_categories()
    if not cats:
        raise RuntimeError("list_categories returned empty — check credentials / Online status")

    feeds = aliexpress.list_feeds()
    if not feeds:
        raise RuntimeError("list_feeds returned empty")

    products = aliexpress.browse_feed(page_size=3)
    if not products:
        raise RuntimeError("browse_feed returned no products — feed may have been rotated; update DEFAULT_SOURCING_FEED")

    sample = products[0]
    details = [
        f"categories: {len(cats)}",
        f"feeds: {len(feeds)}",
        f"feed sample: {sample['title'][:60]} (${sample['price']}, {sample['orders']} orders)",
    ]
    return f"{len(feeds)} feeds · {len(cats)} cats · feed browse OK", details


def check_serpapi() -> tuple[str, list[str]]:
    _require_env("SERPAPI_KEY")
    import requests  # type: ignore

    resp = requests.get(
        "https://serpapi.com/search.json",
        params={
            "engine": "google",
            "q": "blue ocean commerce",
            "num": 1,
            "api_key": os.environ["SERPAPI_KEY"],
        },
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        raise RuntimeError(f"SerpAPI returned error: {data['error']}")

    organic = data.get("organic_results", [])
    remaining = data.get("search_metadata", {}).get("status", "?")
    details = [
        f"organic results: {len(organic)}",
        f"search_id: {data.get('search_metadata', {}).get('id', '?')}",
        f"status: {remaining}",
    ]
    return f"{len(organic)} results, status={remaining}", details


def check_gemini() -> tuple[str, list[str]]:
    _require_env("GEMINI_API_KEY")
    import requests  # type: ignore

    api_key = os.environ["GEMINI_API_KEY"]
    # Use REST to avoid adding google-generativeai as a hard dep if it isn't installed
    model = "gemini-2.5-flash"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    resp = requests.post(
        url,
        params={"key": api_key},
        json={
            "contents": [{"parts": [{"text": "Reply with exactly: ok"}]}],
            # gemini-2.5-flash burns ~30 "thinking tokens" before output; a
            # tiny budget produces an empty string. 64 leaves headroom for
            # reasoning + the one-token answer.
            "generationConfig": {"maxOutputTokens": 64, "temperature": 0},
        },
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")

    data = resp.json()
    text = (
        data.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
            .strip()
            .lower()
    )
    return f"{model} returned '{text}'", []


def check_fal_ai() -> tuple[str, list[str]]:
    """Key-format sanity check only — real image gen costs money."""
    _require_env("FAL_KEY")
    key = os.environ["FAL_KEY"]
    if ":" not in key or len(key) < 30:
        raise RuntimeError(f"FAL_KEY doesn't look right (got {len(key)} chars, expected 'id:secret')")
    return f"key format ok ({len(key)} chars)", [
        f"prefix: {key.split(':')[0][:8]}...",
        "note: no network call (skipping to avoid image-gen cost)",
    ]


def check_google_merchant_center() -> tuple[str, list[str]]:
    _require_env("GOOGLE_MERCHANT_CENTER_ID")
    # MC API is gated behind a separate OAuth flow we haven't wired up yet;
    # just verify the ID is configured and numeric.
    mc_id = os.environ["GOOGLE_MERCHANT_CENTER_ID"]
    if not mc_id.isdigit() or len(mc_id) < 8:
        raise RuntimeError(f"GOOGLE_MERCHANT_CENTER_ID doesn't look right: '{mc_id}'")
    return f"ID {mc_id} configured (no live check)", [
        "note: MC API OAuth not wired; format check only",
    ]


# -----------------------------------------------------------------------------
# Registry + runner
# -----------------------------------------------------------------------------

CHECKS: dict[str, Callable[[], tuple[str, list[str]]]] = {
    "openai":        check_openai,
    "google_ads":    check_google_ads,
    "google_sheets": check_google_sheets,
    "shopify":       check_shopify,
    "aliexpress":    check_aliexpress,
    "serpapi":       check_serpapi,
    "gemini":        check_gemini,
    "fal_ai":        check_fal_ai,
    "merchant_center": check_google_merchant_center,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Blue Ocean Platform API smoke test")
    parser.add_argument(
        "--only", nargs="+", choices=list(CHECKS.keys()),
        help="Run only the named checks",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Print per-check details (env vars, IDs, etc)",
    )
    args = parser.parse_args()

    selected = {k: v for k, v in CHECKS.items() if not args.only or k in args.only}

    print(f"{BOLD}Blue Ocean Platform — API smoke test{RESET}")
    print(f"{GREY}repo: {REPO_ROOT}{RESET}")
    print(f"{GREY}checks: {', '.join(selected.keys())}{RESET}")
    print()

    results: list[Result] = []
    for name, fn in selected.items():
        print(f"  … {name}", end="", flush=True)
        r = _run_check(name, fn)
        results.append(r)
        # Overwrite the "… name" line with the result
        sys.stdout.write("\r\033[K")  # carriage return + clear line
        print(f"  {r.color()}{r.emoji()}{RESET} {BOLD}{name:<17}{RESET} "
              f"{GREY}({r.duration_s:.2f}s){RESET}  {r.message}")
        if args.verbose and r.details:
            for d in r.details:
                for line in d.splitlines():
                    print(f"      {GREY}{line}{RESET}")
        elif r.status == "fail" and r.details:
            for d in r.details:
                for line in d.splitlines():
                    print(f"      {RED}{line}{RESET}")

    # Summary
    print()
    passed = sum(1 for r in results if r.status == "pass")
    failed = sum(1 for r in results if r.status == "fail")
    skipped = sum(1 for r in results if r.status == "skip")
    total = len(results)
    status_line = f"{passed}/{total} passed"
    if skipped:
        status_line += f", {skipped} skipped"
    if failed:
        status_line += f", {RED}{failed} failed{RESET}"
        print(f"{BOLD}{status_line}{RESET}")
        return 1
    print(f"{GREEN}{BOLD}{status_line}{RESET}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
