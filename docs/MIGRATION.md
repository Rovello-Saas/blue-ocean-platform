# BOC Migration — April 2026

Reference record of the XCVLS/Goveliqo → Blue Ocean Commerce B.V. API
migration. Keep this file up to date whenever credentials move entities.

## Final state (2026-04-20, migration complete)

| API | Entity | Status |
|---|---|---|
| Google Ads | BOC MCC `893-983-0325` → client `188-465-2074` (Movanella) | ✅ live, Explorer tier |
| Google Sheets | `blue-ocean-sheets@blue-ocean-platform-493819.iam.gserviceaccount.com` | ✅ live |
| Google Merchant Center | `5765886623` (BOC) | ✅ ID configured |
| Google Cloud / Vertex AI | GCP project `blue-ocean-platform-493819` | ✅ live |
| Gemini | Regenerated inside BOC project | ✅ live |
| Shopify Admin | Movanella (`qnkd5e-3r.myshopify.com`) | ✅ live |
| AliExpress DS | Open Platform app `532468`, Portals `blueocean` tracking ID | ✅ Online |
| OpenAI | BOC org (`info@rovelloshop.com`), service-account key | ✅ live |
| Anthropic | BOC org (`info@rovelloshop.com`) | ✅ live |
| SerpAPI | BOC account (`info@rovelloshop.com`), 250 free searches/mo | ✅ live |
| Fal.ai | Billing address BOC; account display name still "Rovello ." | ⚠️ rename requested via support |

Run `python scripts/smoke_test.py` to verify end-to-end — all 10 checks
must pass. The Anthropic key lives in both the platform `.env` (for the
smoke test) and `page-cloner/.env` (the Node service that actually calls
the API); keep the two in sync on rotation.

## Lessons learned — do not re-debug these

### Google Ads: MCC cascade for standalone client

The Movanella account (`188-465-2074`) is a **standalone** Ads account (not
sub-account of the MCC). The dev token was approved under BOC MCC
`893-983-0325`. For the dev token to be accepted on calls to Movanella, the
MCC must be passed as `login_customer_id` so Google recognizes the permission
chain:

    login_customer_id = 8939830325   # MCC that owns the dev token
    customer_id       = 1884652074   # target account

The MCC admin (`info@rovelloshop.com`) was separately granted access to the
Movanella account (`info@movanella.com` owner), so the cascade works.
`list_accessible_customers()` returns 5 customers including both the MCC and
Movanella.

### AliExpress: scope boundary between DS and Affiliate

The BOC Open Platform app's developer profile is registered as **"Drop
Shipping"** type. This gates what API methods we can call:

- ✅ `aliexpress.ds.category.get`       — 558 categories
- ✅ `aliexpress.ds.feedname.get`       — 131 promo feeds with product counts
- ✅ `aliexpress.ds.recommend.feed.get` — product listings (title/img/price/rating/orders)
- ❌ `aliexpress.ds.text.search`        — `EXCEPTION_TEXT_SEARCH_FOR_DS` (Affiliate-only)
- ❌ `aliexpress.ds.product.get`        — needs OAuth `access_token` (user auth required)
- ❌ `aliexpress.affiliate.*`           — `InsufficientPermission` (wrong profile)

This means **free-form keyword search is not available to us**. The research
pipeline now uses feed-based browsing: pull from a bestseller feed, filter
title-contains-keyword locally. See `src/research/aliexpress.py` docstring.

To get `aliexpress.affiliate.*` methods: switch the Portals account's
developer profile from "Drop Shipping" to "Affiliate" and re-submit for
approval. Not worth doing unless we actually need tracking-link generation
or commission data.

### AliExpress: signed-request protocol

The working signer is **clean HMAC-SHA256 over sorted `k1v1k2v2...`, hex
uppercased** — no `secret + str + secret` wrapper (that's the deprecated MD5
style). POST with form-encoded body, not GET with query-string. See
`_sign_request` / `_call_ds_api` in `src/research/aliexpress.py`.

### Gemini 2.5: thinking tokens

`gemini-2.5-flash` burns ~30 "thinking tokens" before producing visible
output. `maxOutputTokens: 10` produces an empty string because the entire
budget is spent on thinking. Minimum practical budget is ~50 for short
responses. The response object includes `usageMetadata.thoughtsTokenCount`
which is useful for debugging.

### GCP: service account JSON key creation is org-blocked by default

The `rovelloshop.com` org has a policy that blocks SA key creation. Override
at the BOC GCP project level before creating keys (Organization Policies →
`iam.disableServiceAccountKeyCreation` → enforcement: Off at project level).

### Google Sheets: cross-workspace ownership transfer is blocked

Google blocks transferring Drive ownership across Workspaces. The only path
from Goveliqo-owned sheets to BOC-owned sheets was **File → Make a Copy while
signed in as the target Workspace account** (incognito to avoid multi-login
confusion). Two orphaned intermediate copies remain in the Goveliqo Drive
queued for trash.

### AI Studio: binding Gemini keys to GCP projects

When creating a new Gemini API key via AI Studio, BOC project didn't appear in
the "GCP project" dropdown. Fix: click **"Import project"** first to register
the project with AI Studio, then create the key. Without this step the key
ends up in an auto-created `gen-lang-client-*` project with billing in the
wrong place.

### Shopify OAuth: keep Goveliqo app creds

The Shopify `API_KEY` / `API_SECRET` in `.env` are still from the old
Goveliqo app — they're only consumed by `scripts/shopify_oauth.py` during
install, not by runtime API calls. Runtime uses `SHOPIFY_ACCESS_TOKEN`. Leave
in place unless we need to re-install the app against the Movanella store.

## Outstanding

All migration-blocking work is done. Remaining items are passive (waiting
on a third party) or low-priority hygiene:

1. **fal.ai account rename** — support email sent 2026-04-20 asking to
   rename "Rovello ." → "Blue Ocean Commerce B.V." on the Personal
   account. Billing address and payment entity are already BOC; only the
   display name on invoices differs. Awaiting reply from `support@fal.ai`.
2. **Add Dutch VAT ID to billing pages** — OpenAI / Anthropic / SerpAPI /
   fal.ai all have empty Tax ID fields. Drop in the BOC NL VAT number on
   each provider's billing page whenever convenient; nothing currently
   breaks without it.
3. **Revoke superseded keys** — after a week of stable operation on the
   new keys, revoke these in each provider's dashboard:
   - OpenAI: `sk-proj-0IU3...r4A` (originally "Qoveliqo Cursor", now
     renamed to "Blue Ocean Platform") and `sk-...ebUA` ("T-Lab") if
     unused.
   - Anthropic: `sk-ant-api03-6K4qiIXq...QAA` (Qoveliqo-era personal key).
   - SerpAPI: `3d389bbc...aaaa5d` (XCVLS-era key).

### Done during this migration (historical)

- ✅ Orphan GCP project `gen-lang-client-0161867213` — shut down
  2026-04-20, scheduled to be fully deleted 2026-05-20.
- ✅ Old Goveliqo-owned sheets (`1pCut1UA5FXYEYz68Xx0VL0z49XgUhFvmDgINv2KrXpw`,
  `1GyeKaZLVQJtPSQfzt4TQpxZYmwP30Fp-eMfj29_I0Y0`) — trashed via the
  Goveliqo Workspace admin.
- ✅ Fresh BOC signups: OpenAI (service-account key), Anthropic, SerpAPI
  (250/mo free tier). All smoke-tested live on 2026-04-20.

## Re-running the migration verification

```bash
python scripts/smoke_test.py -v
```

Expected output: `9/9 passed` with EUR/Europe/Amsterdam for Google Ads,
`blue-ocean-sheets@...` as the Sheets service account, Movanella as the
Shopify shop, 131 AliExpress feeds.
