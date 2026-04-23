"""
AliExpress product research integration.

This module targets the **Drop Shipping** (`aliexpress.ds.*`) API surface, which
is what the BOC Open Platform app (AppKey 532468) is registered for. See
the long header in .env for why we're on DS and not Affiliate.

What works with signed-only access (no OAuth, verified 2026-04-19):
  - aliexpress.ds.category.get       → full category tree (558 nodes)
  - aliexpress.ds.feedname.get       → promo feed catalog (~131 feeds)
  - aliexpress.ds.recommend.feed.get → products in a feed (title/img/price/
                                       rating/orders/detail URL)

What's BLOCKED and why (do not re-debug, confirmed scope boundary):
  - aliexpress.ds.text.search        → EXCEPTION_TEXT_SEARCH_FOR_DS (free-form
                                       keyword search is Affiliate-only)
  - aliexpress.ds.product.get        → MissingParameter access_token (user
                                       OAuth required)
  - aliexpress.affiliate.*           → InsufficientPermission (wrong profile)

As a result, `search_products` / `find_top3_matches` no longer do true keyword
search against the full AliExpress catalog. They:
  1. Fetch a page from one of the curated bestseller feeds (200k+ products),
  2. Filter client-side by title substring match on the keyword,
  3. Return the top-N after applying rating/orders thresholds.

This gives us a decent proxy for "what bestsellers on AliExpress roughly match
this keyword", which is exactly what the research pipeline needs for margin
estimation. It is NOT exhaustive keyword search — niche keywords may return
nothing. The pipeline already handles empty results by falling back to
manual-review URLs via `build_search_url`.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from typing import Optional

import requests

from src.core.config import (
    AppConfig,
    ALIEXPRESS_APP_KEY,
    ALIEXPRESS_APP_SECRET,
    ALIEXPRESS_TRACKING_ID,
)

logger = logging.getLogger(__name__)

API_BASE_URL = "https://api-sg.aliexpress.com/sync"

# Feeds scanned for keyword matching. The Affiliate keyword-search API is
# blocked for DS-scope apps, so we approximate text search by scanning a
# curated ensemble of **consumer-category** bestseller feeds and filtering
# titles locally.
#
# Feed selection is deliberate: each feed corresponds to a category of
# product-intent keywords we actually ideate over (kitchen, sports, pets,
# furniture, etc). Industrial-component feeds (e.g. `DS_ElectronicComponents_
# bestsellers`) and holiday-themed feeds are excluded — their contents don't
# match consumer dropshipping searches.
#
# Ordering: `DS_*` (clean dropshipper lists) first, then the broader regional
# `AEB_*` and `*_ZA topsellers` feeds which are noisier but cover niches the
# DS feeds miss (office furniture, pet supplies, kitchen appliances). First-
# match-wins per keyword so the clean lists are hit first.
#
# Product counts below are as returned by `aliexpress.ds.feedname.get`
# and double as a rough "how much coverage does this feed add" hint.
SOURCING_FEEDS = [
    # --- Core DS dropshipper feeds (highest signal, English titles) ---
    "DS_ConsumerElectronics_bestsellers",   # 20,280 — LEDs, cables, audio, smart home
    "DS_Home&Kitchen_bestsellers",          # 13,080 — kitchen, bed, bath, storage
    "DS_Sports&Outdoors_bestsellers",       # 27,652 — yoga, cycling, camping, fitness
    "DS_Beauty_bestsellers",                #  2,696 — skincare, makeup, hair
    "DS_Global_topsellers",                 # 11,670 — cross-category
    "DS_Jewelry&Watch 10$+",                #  1,200 — watches (fills the watch gap)
    "DS_Automobile&Accessories_bestsellers",# 20,760 — car accessories
    # --- Regional AEB / ZA topsellers (noisier but fill consumer niches) ---
    "AEB_US_Furniture_TopSellers",          # 13,984 — chairs, desks, shelves
    "AEB_US_Home&Garden_TopSellers",        # 73,722 — bedding, decor, garden (huge)
    "home appliances_ZA topsellers_ 20240423",  # 16,981 — milk frothers, blenders, vacuums
    "pets&supplies_ZA topsellers_ 20240423",    # 19,712 — cat trees, pet beds, toys
    "computer&office_ZA topsellers_ 20240423",  # 14,753 — office chairs, desk accessories
]
# Kept for callers that still import the single-name constant (older pages).
DEFAULT_SOURCING_FEED = SOURCING_FEEDS[0]

# LLM `category` (free-form English noun, e.g. "kitchen", "home appliances")
# → best-matching feed name. When we know the category, we hoist its feed to
# the front of the scan order so category-relevant pages get the deeper
# (max_pages) budget first. Unmatched categories fall through to the default
# feed order — no harm, just no boost.
#
# Keys are lowercased substrings; we check each substring against the
# lowercased category string. First substring hit wins. Ordering within a
# bucket doesn't matter for correctness, only for tie-breaking readability.
_CATEGORY_FEED_HINTS: list[tuple[str, str]] = [
    # Consumer electronics / tech
    ("electronic",       "DS_ConsumerElectronics_bestsellers"),
    ("tech",             "DS_ConsumerElectronics_bestsellers"),
    ("gadget",           "DS_ConsumerElectronics_bestsellers"),
    ("audio",            "DS_ConsumerElectronics_bestsellers"),
    ("led",              "DS_ConsumerElectronics_bestsellers"),
    ("smart home",       "DS_ConsumerElectronics_bestsellers"),
    ("phone",            "DS_ConsumerElectronics_bestsellers"),
    # Home appliances (separate feed — blenders/vacuums/frothers)
    ("appliance",        "home appliances_ZA topsellers_ 20240423"),
    ("kitchen appliance","home appliances_ZA topsellers_ 20240423"),
    ("vacuum",           "home appliances_ZA topsellers_ 20240423"),
    ("blender",          "home appliances_ZA topsellers_ 20240423"),
    # Kitchen / home (non-appliance)
    ("kitchen",          "DS_Home&Kitchen_bestsellers"),
    ("cookware",         "DS_Home&Kitchen_bestsellers"),
    ("bath",             "DS_Home&Kitchen_bestsellers"),
    ("bedroom",          "DS_Home&Kitchen_bestsellers"),
    ("storage",          "DS_Home&Kitchen_bestsellers"),
    ("home",             "DS_Home&Kitchen_bestsellers"),  # generic fallback — keep AFTER "home appliance" entry
    # Sports / outdoors / fitness
    ("sport",            "DS_Sports&Outdoors_bestsellers"),
    ("fitness",          "DS_Sports&Outdoors_bestsellers"),
    ("outdoor",          "DS_Sports&Outdoors_bestsellers"),
    ("yoga",             "DS_Sports&Outdoors_bestsellers"),
    ("camping",          "DS_Sports&Outdoors_bestsellers"),
    ("cycling",          "DS_Sports&Outdoors_bestsellers"),
    ("bike",             "DS_Sports&Outdoors_bestsellers"),
    # Beauty / personal care
    ("beauty",           "DS_Beauty_bestsellers"),
    ("skincare",         "DS_Beauty_bestsellers"),
    ("makeup",           "DS_Beauty_bestsellers"),
    ("cosmetic",         "DS_Beauty_bestsellers"),
    ("hair",             "DS_Beauty_bestsellers"),
    ("personal care",    "DS_Beauty_bestsellers"),
    # Jewelry / watches
    ("jewelry",          "DS_Jewelry&Watch 10$+"),
    ("jewellery",        "DS_Jewelry&Watch 10$+"),
    ("watch",            "DS_Jewelry&Watch 10$+"),
    ("accessory",        "DS_Jewelry&Watch 10$+"),
    # Automotive
    ("auto",             "DS_Automobile&Accessories_bestsellers"),
    ("car",              "DS_Automobile&Accessories_bestsellers"),
    ("vehicle",          "DS_Automobile&Accessories_bestsellers"),
    # Furniture
    ("furniture",        "AEB_US_Furniture_TopSellers"),
    ("chair",            "AEB_US_Furniture_TopSellers"),
    ("desk",             "AEB_US_Furniture_TopSellers"),
    ("shelf",            "AEB_US_Furniture_TopSellers"),
    # Garden / decor (AEB_US_Home&Garden is huge, use for garden/decor)
    ("garden",           "AEB_US_Home&Garden_TopSellers"),
    ("decor",            "AEB_US_Home&Garden_TopSellers"),
    ("plant",            "AEB_US_Home&Garden_TopSellers"),
    ("outdoor furniture","AEB_US_Home&Garden_TopSellers"),
    # Pets
    ("pet",              "pets&supplies_ZA topsellers_ 20240423"),
    ("dog",              "pets&supplies_ZA topsellers_ 20240423"),
    ("cat",              "pets&supplies_ZA topsellers_ 20240423"),
    ("aquarium",         "pets&supplies_ZA topsellers_ 20240423"),
    # Office / computer
    ("office",           "computer&office_ZA topsellers_ 20240423"),
    ("computer",         "computer&office_ZA topsellers_ 20240423"),
    ("stationery",       "computer&office_ZA topsellers_ 20240423"),
]


def _category_to_feed(category: Optional[str]) -> Optional[str]:
    """Map an LLM-supplied category string to the best-matching feed name.

    Returns None if no feed matches — the caller should fall back to the
    default feed scan order. First substring hit wins, so entries in
    `_CATEGORY_FEED_HINTS` are ordered most-specific-first (e.g. "kitchen
    appliance" → appliances feed comes BEFORE generic "kitchen" → kitchen
    feed; "home appliance" → appliances feed comes BEFORE generic "home").
    """
    if not category:
        return None
    c = str(category).strip().lower()
    if not c:
        return None
    for needle, feed in _CATEGORY_FEED_HINTS:
        if needle in c:
            return feed
    return None


def _prioritised_feed_order(category: Optional[str]) -> list[str]:
    """Return SOURCING_FEEDS with the category-matched feed (if any) moved
    to the front. Identity otherwise — same default order callers have
    always seen."""
    matched = _category_to_feed(category)
    if not matched or matched not in SOURCING_FEEDS:
        return list(SOURCING_FEEDS)
    reordered = [matched] + [f for f in SOURCING_FEEDS if f != matched]
    return reordered

# Cache feed pages for a short time to avoid N+1 fetches when pipeline
# iterates over many keywords back-to-back. Keyed by (feed, page_no, page_size).
_FEED_CACHE: dict[tuple, tuple[float, list[dict]]] = {}
_FEED_CACHE_TTL_SECONDS = 300  # 5 min


# -----------------------------------------------------------------------------
# Low-level signed request
# -----------------------------------------------------------------------------

def _sign_request(params: dict, secret: str) -> str:
    """Generate HMAC-SHA256 signature for AliExpress TOP API request.

    Algorithm: sort params by key, concatenate as "k1v1k2v2...", HMAC-SHA256
    with the app secret as key, hex digest uppercased. NO secret-wrapper
    (`secret + str + secret`) — that's the deprecated MD5 sign_method, not SHA256.
    Verified working 2026-04-19 against category.get / feedname.get / feed.get.
    """
    sorted_kv = "".join(f"{k}{v}" for k, v in sorted(params.items()))
    return hmac.new(
        secret.encode("utf-8"),
        sorted_kv.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest().upper()


def _call_ds_api(method: str, extra_params: dict, timeout: int = 20) -> dict:
    """
    POST a signed request to the DS API. Returns the parsed JSON response,
    or an empty dict on transport/signing failure (logged).

    Note: POST with form-encoded body is what works — a number of AliExpress
    docs show GET with query-string, but we verified the live endpoint rejects
    GET silently for some methods.
    """
    if not ALIEXPRESS_APP_KEY or ALIEXPRESS_APP_KEY.startswith("your_"):
        logger.warning("AliExpress credentials not configured — skipping %s", method)
        return {}

    params = {
        "app_key": ALIEXPRESS_APP_KEY,
        "method": method,
        "sign_method": "sha256",
        "timestamp": str(int(time.time() * 1000)),
        "format": "json",
        "v": "2.0",
        **{k: str(v) for k, v in extra_params.items() if v is not None and v != ""},
    }
    params["sign"] = _sign_request(params, ALIEXPRESS_APP_SECRET)

    try:
        resp = requests.post(API_BASE_URL, data=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error("AliExpress API %s transport failure: %s", method, e)
        return {}


# -----------------------------------------------------------------------------
# Feed-based primitives (verified working in scope)
# -----------------------------------------------------------------------------

def list_feeds() -> list[dict]:
    """Return the full list of promo feeds with product counts.

    Each entry: {"promo_name": str, "product_num": int, "promo_desc": str}.
    Useful for picking a feed to source products from.
    """
    data = _call_ds_api("aliexpress.ds.feedname.get", {})
    resp = data.get("aliexpress_ds_feedname_get_response", {})
    feeds_raw = (
        resp.get("resp_result", {})
            .get("result", {})
            .get("promos", {})
            .get("promo", [])
    )
    out = []
    for f in feeds_raw:
        try:
            product_num = int(f.get("product_num", 0))
        except (TypeError, ValueError):
            product_num = 0
        out.append({
            "promo_name": f.get("promo_name", ""),
            "product_num": product_num,
            "promo_desc": f.get("promo_desc", ""),
        })
    return out


def list_categories() -> list[dict]:
    """Return the DS category tree."""
    data = _call_ds_api("aliexpress.ds.category.get", {})
    resp = data.get("aliexpress_ds_category_get_response", {})
    cats = (
        resp.get("resp_result", {})
            .get("result", {})
            .get("categories", {})
            .get("category", [])
    )
    return cats if isinstance(cats, list) else []


def browse_feed(
    feed_name: str = DEFAULT_SOURCING_FEED,
    page_no: int = 1,
    page_size: int = 50,
    country: Optional[str] = None,
    currency: str = "USD",
    language: str = "EN",
    tracking_id: Optional[str] = None,
) -> list[dict]:
    """
    Fetch a page of products from a feed. Cached for 5 min by (feed, page,
    size, country, language).

    The global bestseller feeds (DEFAULT_SOURCING_FEED being the biggest) work
    best with no country filter. Country-locked feeds (AEB_UK_*, AEB_US_*)
    need a matching country or return zero.

    `language` is forwarded as `target_language` to the DS feed API —
    AliExpress returns LOCALISED product titles when a real ISO code is
    passed (DE/FR/NL/ES/IT/PL…), which is how the pipeline avoids the old
    English-only substring match that was driving wrong-category hits.
    Verified 2026-04-21: "Kitchen Knives Set" → "Küchenmesser-Set", etc.
    Translation quality is good, not machine-garbage.
    """
    # Cache key MUST include language — same page with different
    # target_language gives different titles, and a stale EN cache entry would
    # defeat the whole point of the DE-direct matcher.
    lang_key = (language or "EN").upper()
    cache_key = (feed_name, page_no, page_size, country or "GLOBAL", lang_key)
    now = time.time()
    cached = _FEED_CACHE.get(cache_key)
    if cached and (now - cached[0]) < _FEED_CACHE_TTL_SECONDS:
        return cached[1]

    extra = {
        "feed_name": feed_name,
        "page_no": page_no,
        "page_size": page_size,
        "target_currency": currency,
        "target_language": lang_key,
        "tracking_id": tracking_id or ALIEXPRESS_TRACKING_ID or "",
    }
    if country:
        extra["country"] = country

    data = _call_ds_api("aliexpress.ds.recommend.feed.get", extra)
    resp = data.get("aliexpress_ds_recommend_feed_get_response", {})
    products_raw = (
        resp.get("result", {})
            .get("products", {})
            .get("traffic_product_d_t_o", [])
    )
    if not isinstance(products_raw, list):
        products_raw = []

    parsed = [p for p in (_parse_product(item) for item in products_raw) if p]
    _FEED_CACHE[cache_key] = (now, parsed)
    return parsed


# -----------------------------------------------------------------------------
# Matching helpers
# -----------------------------------------------------------------------------
# Short particles per language — we strip these from the keyword before
# tokenising so multi-word keywords like "Holzuhren für Herren" end up as
# meaningful noun tokens ("holzuhren", "herren") rather than being diluted
# by grammatical glue. If a language isn't listed, we fall back to "use
# every token ≥4 chars long" which is a reasonable stopword-free heuristic.
_STOPWORDS: dict[str, set[str]] = {
    "de": {"für", "und", "mit", "von", "der", "die", "das", "den", "dem",
           "zum", "zur", "im", "am", "ein", "eine", "einen", "einem"},
    "en": {"for", "and", "with", "the", "a", "an", "of", "to", "in"},
    "nl": {"voor", "en", "met", "de", "het", "een", "van"},
    "fr": {"pour", "et", "avec", "le", "la", "les", "un", "une", "de"},
    "es": {"para", "y", "con", "el", "la", "los", "las", "un", "una", "de"},
    "it": {"per", "e", "con", "il", "la", "i", "le", "un", "una", "di"},
    "pl": {"dla", "i", "z", "w", "na", "do"},
}

# Minimum token length to participate in the word-match. Shorter tokens
# trigger too many false positives (German "tür" appearing inside "türkisch",
# English "men" inside "women", etc.) and the noise isn't worth the recall
# win from catching tiny words.
_MIN_TOKEN_LEN = 4


def _title_word_tokens(title: str) -> set[str]:
    """Split a product title into its lowercase word tokens. Unicode-aware
    so German umlauts (`ä`, `ö`, `ü`, `ß`) stay intact as part of a token —
    otherwise "Küchenmesser-Set" would become {"k","chenmesser","set"}
    instead of {"küchenmesser","set"}."""
    import re as _re
    return set(_re.findall(r"[\w]+", (title or "").lower(), flags=_re.UNICODE))


def _match_keyword_in_title(
    keyword: str,
    title: str,
    language: str = "en",
) -> Optional[str]:
    """Return a match-strategy label if `keyword` plausibly describes the
    product titled `title` (both expected to be in the same language), or
    None if there's no principled match.

    The matcher has two legs:

    1. **Whole-phrase substring** — strongest signal. If the exact keyword
       phrase appears anywhere in the title, we take it.
    2. **All significant tokens present as whole words** — for multi-word
       keywords whose tokens don't land in the title contiguously. Each
       keyword token (after stopword + short-word filtering) must match a
       title word either exactly or as a prefix (`"holzuhr"` matches
       `"holzuhren"` for German plurals; `"watch"` matches `"watches"` for
       English). We do NOT fall back to bare substring containment here —
       that's what caused the infamous "Schlüsselbrett" → *Pet Tracker*
       match (needle "rack" substring-hit inside "tracker").

    Single-token keywords need a whole-phrase substring hit — we don't try
    to "fuzzy match" a bare one-word needle because that's exactly where
    word-boundary bugs live.
    """
    if not keyword or not title:
        return None
    kw = keyword.strip().lower()
    t = title.lower()

    # Leg 1: whole-phrase substring
    if kw and kw in t:
        return "substring"

    # Leg 2: all significant tokens present as whole words in the title
    stopwords = _STOPWORDS.get((language or "").lower(), set())
    raw_tokens = kw.split()
    tokens = [
        tok for tok in raw_tokens
        if tok not in stopwords and len(tok) >= _MIN_TOKEN_LEN
    ]
    # Need at least two meaningful tokens to trust a token-only match; with
    # one token we already tried substring above and nothing else is safe.
    if len(tokens) < 2:
        return None

    title_tokens = _title_word_tokens(title)
    # Each keyword token must be in the title as a whole word OR as a prefix
    # of a title word (plural/declined-form tolerance).
    def _tok_in(title_set: set[str], tok: str) -> bool:
        if tok in title_set:
            return True
        return any(w.startswith(tok) for w in title_set)

    if all(_tok_in(title_tokens, tok) for tok in tokens):
        return "tokens"
    return None


# -----------------------------------------------------------------------------
# Public API (signatures preserved from pre-migration version)
# -----------------------------------------------------------------------------

def search_products(
    keyword: str,
    country: str = "DE",
    language: str = "de",
    min_rating: float = 4.5,
    min_orders: int = 500,
    max_results: int = 10,
    config: AppConfig = None,
    english_search_terms: Optional[list[str]] = None,
    category: Optional[str] = None,
) -> list[dict]:
    """
    Find products matching a keyword using the feed-filter fallback.

    Since `aliexpress.ds.text.search` is blocked for DS-scope apps, this pulls
    pages from a curated ensemble of DS bestseller feeds and filters
    client-side by matching the keyword against localised product titles.

    Match strategy (2026-04-21 rewrite):
    - The DS feed is requested with `target_language=<market>` so titles come
      back localised (German for DE, French for FR, etc.). Verified
      experimentally: "Kitchen Knives Set" → "Küchenmesser-Set" in the DE
      feed, not machine-garbage.
    - We match the raw target-language `keyword` directly against those
      localised titles (via `_match_keyword_in_title`). This replaces the
      old English-substring approach that was generating wrong-category
      matches (e.g. "Schlüsselbrett" → *Wireless Key Finder Pet Tracker*
      because needle "key rack" substring-hit inside "tracker").
    - `english_search_terms` is kept as a **last-ditch rescue** only when
      the target-language match returns zero AND we're not already in an
      English market. If you're in doubt, just don't pass it — the direct
      match is stricter and produces cleaner results.
    - `category` (optional, free-form English like "kitchen", "pets",
      "electronics") moves the best-matching feed to the FRONT of the
      scan order via `_prioritised_feed_order`. With the deeper per-feed
      page budget (8 pages as of 2026-04-21), this means category-relevant
      pages get scanned first-and-deepest before moving on — better match
      rate on the first hit, and the first-match-wins early-exit usually
      saves the other feeds entirely.

    Each returned product carries `match_via` / `match_feed` fields the
    caller can persist for diagnostics.
    """
    config = config or AppConfig()

    if not ALIEXPRESS_APP_KEY or ALIEXPRESS_APP_KEY.startswith("your_"):
        logger.warning("AliExpress API credentials not configured — skipping product search")
        return []

    kw_primary = (keyword or "").strip()
    if not kw_primary:
        return []

    lang_code = (language or "").lower() or "en"

    # Optional rescue needles: only used if the primary direct-language match
    # turns up nothing. Kept conservative — same strict word-boundary rules as
    # the primary keyword.
    rescue_needles: list[str] = []
    if english_search_terms and lang_code != "en":
        rescue_needles = [
            t.strip().lower() for t in english_search_terms
            if t and isinstance(t, str) and t.strip()
        ]

    # Scan a sequence of category-focused feeds, each paginated in chunks of
    # 50 (safe upper bound for feed.get). First-match-wins per feed — once
    # we've got enough matches to return, stop. Two passes:
    #   Pass A: strict target-language match against localised titles.
    #   Pass B: only if A is empty → rescue with english_search_terms.
    #
    # Page budget (2026-04-21): bumped from 3 → 8 per feed. The 5-min feed
    # cache absorbs most of the cost (second keyword in a run pays nothing
    # for pages already fetched), and the early-exit on max_results means
    # the extra pages only get pulled for keywords that don't match early.
    # Net effect: ~2.5x coverage (8×50=400 products/feed × 12 feeds ≈ 4.8k
    # candidates per pass) for roughly the same per-run API spend on repeat
    # keywords.
    matches: list[dict] = []
    max_pages_per_feed = 8
    seen_ids: set[str] = set()

    # Category-aware feed ordering: hoist the category-relevant feed to the
    # front of the queue. Unmatched categories get the default order.
    feed_order = _prioritised_feed_order(category)

    def _collect(pass_label: str, strategy_fn, feed_language: str) -> None:
        """Scan every (feed, page) in order, applying `strategy_fn(title)`
        → Optional[match_via_string]. Appends to `matches` / `seen_ids` in
        closure scope; returns when `max_results` is hit."""
        for feed_name in feed_order:
            if len(matches) >= max_results:
                return
            for page in range(1, max_pages_per_feed + 1):
                page_products = browse_feed(
                    feed_name=feed_name,
                    page_no=page,
                    page_size=50,
                    language=feed_language,
                    country=country if country else None,
                )
                if not page_products:
                    break
                for p in page_products:
                    pid = str(p.get("aliexpress_product_id") or p.get("product_id") or "")
                    if pid and pid in seen_ids:
                        continue
                    title = p.get("title") or ""
                    via = strategy_fn(title)
                    if not via:
                        continue
                    if p.get("rating", 0) < min_rating:
                        continue
                    if p.get("orders", 0) < min_orders:
                        continue
                    if pid:
                        seen_ids.add(pid)
                    # Diagnostics — consumed by the pipeline + dashboard to
                    # show why a product was picked (and let us audit weak
                    # matches later without re-running).
                    p["match_via"] = via
                    p["match_feed"] = feed_name
                    p["match_pass"] = pass_label
                    p["match_needle"] = title[:200]  # the title itself is the needle's context
                    matches.append(p)
                    if len(matches) >= max_results:
                        return
                if len(matches) >= max_results:
                    return

    # Pass A: direct target-language match
    def _direct_match(title: str) -> Optional[str]:
        return _match_keyword_in_title(kw_primary, title, language=lang_code)
    _collect("direct", _direct_match, feed_language=lang_code.upper() or "EN")

    # Pass B: rescue via english_search_terms, only if Pass A yielded nothing.
    # We fetch the ENGLISH feed this time because the rescue needles are in
    # English — matching English needles against German titles defeats the
    # point. This costs one extra round of feed fetches, but only in the
    # niche-coverage-hole case (which is less common now that Pass A does
    # proper localisation).
    if not matches and rescue_needles:
        def _rescue_match(title: str) -> Optional[str]:
            for n in rescue_needles:
                via = _match_keyword_in_title(n, title, language="en")
                if via:
                    return f"rescue_{via}"
            return None
        _collect("rescue_en", _rescue_match, feed_language="EN")

    logger.info(
        "AliExpress feed search '%s' (country=%s, lang=%s): %d matches "
        "[primary=%d, rescue=%d]",
        keyword, country, lang_code, len(matches),
        sum(1 for m in matches if m.get("match_pass") == "direct"),
        sum(1 for m in matches if m.get("match_pass") == "rescue_en"),
    )
    return matches


def get_product_details(product_id: str) -> Optional[dict]:
    """
    Return details for a specific product ID by scanning the sourcing feed
    for a match. This is a best-effort lookup — `aliexpress.ds.product.get`
    itself requires OAuth which we don't have.

    If the product isn't in the first few pages of the sourcing feed, we
    return None. For a fuller lookup in the future, wire up DS OAuth.
    """
    if not product_id:
        return None

    target = str(product_id)
    for page in range(1, 5):
        page_products = browse_feed(page_no=page, page_size=50)
        if not page_products:
            break
        for p in page_products:
            if str(p.get("aliexpress_product_id")) == target:
                return p
    return None


def check_product_availability(product_id: str) -> bool:
    """
    Check if a product is still available on AliExpress.

    Without `product.get` (OAuth required) we can't do a real availability
    check. Conservative policy: return True (assume available) unless we have
    positive evidence of removal. The stock checker's purpose is to flag
    *regressions* in what's working, so silent "assume OK" is safer than
    false-positive kill signals on every call.
    """
    if not product_id:
        return True
    # If we happen to find the product in our cached feed pages, we know it's live.
    # If not, we don't know either way, so we defer to "assume live".
    if get_product_details(product_id):
        return True
    return True


def find_best_match(
    keyword: str,
    estimated_selling_price: float,
    country: str = "DE",
    language: str = "de",
    config: AppConfig = None,
) -> Optional[dict]:
    """Find the best-scored product for a keyword given an expected sell price."""
    config = config or AppConfig()

    products = search_products(
        keyword=keyword,
        country=country,
        language=language,
        min_rating=config.min_aliexpress_rating,
        min_orders=config.min_aliexpress_orders,
        max_results=10,
        config=config,
    )
    if not products:
        return None

    best_product = None
    best_score = -1.0

    for product in products:
        ali_price = product.get("price", 0)
        if ali_price <= 0:
            continue

        estimated_landed_cost = ali_price * 1.2
        estimated_margin = estimated_selling_price - estimated_landed_cost
        margin_pct = (
            estimated_margin / estimated_selling_price
            if estimated_selling_price > 0 else 0
        )
        if margin_pct < 0.2:
            continue

        rating_score = (product.get("rating", 0) / 5.0) * 30
        order_score = min(product.get("orders", 0) / 10000, 1.0) * 30
        margin_score = min(margin_pct, 0.6) / 0.6 * 40
        score = rating_score + order_score + margin_score

        if score > best_score:
            best_score = score
            best_product = dict(product)
            best_product["estimated_margin_pct"] = round(margin_pct, 4)
            best_product["match_score"] = round(score, 1)

    if best_product:
        logger.info(
            "Best match for '%s': %s (score: %.1f, margin: %.1f%%)",
            keyword,
            (best_product.get("title") or "?")[:50],
            best_product.get("match_score", 0),
            best_product.get("estimated_margin_pct", 0) * 100,
        )
    return best_product


def find_top3_matches(
    keyword: str,
    estimated_selling_price: float = 0,
    country: str = "DE",
    language: str = "de",
    config: AppConfig = None,
    english_search_terms: Optional[list[str]] = None,
    category: Optional[str] = None,
) -> dict:
    """
    Return the **Top 3** products for a keyword:
      - best_seller  (most orders)
      - best_price   (cheapest)
      - best_rated   (highest rating)

    Each entry is a product dict plus a 'tag' field, or None if nothing was
    found at all. The returned dict is the source of truth for the pipeline's
    AliExpress-match step: if `best_seller` is None, the pipeline routes
    the keyword into the manual-review queue (status=pending_manual_review)
    rather than hard-dropping — the user does the AliExpress search and
    fills in the real landed cost, then economics fires on real data.

    Quality floor (configurable, 2026-04-21 change): we now apply a relaxed
    but non-zero minimum rating / orders floor so the feed's long tail of
    low-evidence SKUs (20 orders, 3.8 stars, new seller) can't win by being
    the only substring hit. Keys:
      - `research.min_match_rating`  (default 4.3)
      - `research.min_match_orders`  (default 100)
    Set to 0 to disable.

    `english_search_terms` is only used as a rescue pass when the
    direct-language match returns zero (see `search_products`). Direct-
    language matching against localised titles is the primary strategy.

    `category` is the LLM-supplied product category (free-form English,
    e.g. "kitchen" / "pets" / "electronics"). When provided, the matcher
    hoists the best-fit feed to the front of its scan order — raising the
    match rate with no extra API cost.
    """
    config = config or AppConfig()

    # Relaxed-but-non-zero floor — the old code passed rating=0/orders=0
    # which meant any substring hit won, including 20-order junk listings.
    min_rating = float(config.get("research.min_match_rating", 4.3) or 0)
    min_orders = int(config.get("research.min_match_orders", 100) or 0)

    products = search_products(
        keyword=keyword,
        country=country,
        language=language,
        min_rating=min_rating,
        min_orders=min_orders,
        max_results=30,
        config=config,
        english_search_terms=english_search_terms,
        category=category,
    )

    result: dict = {
        "best_seller": None,
        "best_price": None,
        "best_rated": None,
        "all_products": products,
    }
    if not products:
        return result

    priced = [p for p in products if p.get("price", 0) > 0]
    if not priced:
        return result

    best_seller = dict(sorted(priced, key=lambda p: p.get("orders", 0), reverse=True)[0])
    best_seller["tag"] = "Best Seller"
    result["best_seller"] = best_seller

    best_price = dict(sorted(priced, key=lambda p: p.get("price", 9999))[0])
    best_price["tag"] = "Best Price"
    result["best_price"] = best_price

    best_rated = dict(sorted(
        priced,
        key=lambda p: (p.get("rating", 0), p.get("orders", 0)),
        reverse=True,
    )[0])
    best_rated["tag"] = "Best Rated"
    result["best_rated"] = best_rated

    if estimated_selling_price > 0:
        for key in ("best_seller", "best_price", "best_rated"):
            p = result[key]
            if p:
                ali_price = p.get("price", 0)
                landed = ali_price * 1.2
                margin = estimated_selling_price - landed
                p["estimated_margin_pct"] = round(
                    margin / estimated_selling_price if estimated_selling_price else 0, 4
                )

    logger.info(
        "Top-3 AliExpress for '%s': seller=%s orders, price=$%.2f, rated=%.1f/5",
        keyword,
        best_seller.get("orders", 0),
        best_price.get("price", 0),
        best_rated.get("rating", 0),
    )
    return result


def build_search_url(keyword: str) -> dict:
    """Build AliExpress and Alibaba search URLs for manual review."""
    encoded_keyword = (keyword or "").replace(" ", "+")
    return {
        "aliexpress_search_url": f"https://www.aliexpress.com/wholesale?SearchText={encoded_keyword}",
        "alibaba_search_url": f"https://www.alibaba.com/trade/search?SearchText={encoded_keyword}",
    }


# -----------------------------------------------------------------------------
# Parsing
# -----------------------------------------------------------------------------

def _parse_product(item: dict) -> Optional[dict]:
    """Parse a raw feed-API product into the canonical shape used downstream.

    DS feed-API response uses `productSmallImageUrl` (camelCase) inside
    `product_small_image_urls` — NOT `string` like the old affiliate API.
    """
    try:
        price_str = item.get("target_sale_price", item.get("target_original_price", "0"))
        try:
            price = float(str(price_str).replace(",", "."))
        except (ValueError, TypeError):
            price = 0.0

        image_url = item.get("product_main_image_url", "")
        small = item.get("product_small_image_urls", {})
        small_list = (
            small.get("productSmallImageUrl", [])
            if isinstance(small, dict) else []
        )
        # De-dup while preserving order
        seen = set()
        all_images = []
        for url in ([image_url] if image_url else []) + small_list:
            if url and url not in seen:
                seen.add(url)
                all_images.append(url)

        # DS API's `evaluate_rate` is a positive-feedback percentage like
        # "97.6%". We map to a nominal 5-star scale (% / 20) for backward
        # compatibility with downstream code that expects 0-5. We also keep
        # the raw percentage in `feedback_rate` for anything more honest.
        rating_str = item.get("evaluate_rate", "0")
        try:
            pct = float(str(rating_str).replace("%", ""))
        except (ValueError, TypeError):
            pct = 0.0
        rating_5 = round(pct / 20, 1) if pct else 0.0

        orders_str = item.get("lastest_volume", item.get("latest_volume", "0"))
        try:
            orders = int(orders_str)
        except (ValueError, TypeError):
            orders = 0

        return {
            "aliexpress_product_id": str(item.get("product_id", "")),
            "title": item.get("product_title", ""),
            "url": item.get("promotion_link", item.get("product_detail_url", "")),
            "price": price,
            "original_price": float(item.get("target_original_price", price) or price),
            "rating": rating_5,
            "feedback_rate": pct,  # raw % (new field, more honest)
            "orders": orders,
            "image_url": image_url,
            "image_urls": all_images,
            "shipping_cost": 0.0,  # not provided by feed API
            "currency": item.get("target_sale_price_currency", "USD"),
            "category_id": str(item.get("first_level_category_id", "")),
            "category_name": item.get("first_level_category_name", ""),
            "shop_id": str(item.get("shop_id", "")),
            "shop_url": item.get("shop_url", ""),
            "discount_pct_raw": item.get("discount", ""),
        }

    except Exception as e:
        logger.warning("Failed to parse AliExpress product: %s", e)
        return None
