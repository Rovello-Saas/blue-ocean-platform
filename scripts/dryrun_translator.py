"""
Dry-run the Best Sellers → Claude translator on DE.

Why this exists: the translator skips products for reasons (branded, regulated,
perishable, counterfeit_risk, unsuitable_dropship) that the normal pipeline
only *counts* — before we spend SerpAPI money downstream we want to see the
actual skip list and keeper list to confirm the filter is doing what we think.

Usage:
    python3 scripts/dryrun_translator.py            # DE, top 20 per cat
    python3 scripts/dryrun_translator.py DE 20
"""

from __future__ import annotations

import json
import logging
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import anthropic

from src.core.config import ANTHROPIC_API_KEY
from src.research.google_best_sellers import fetch_best_sellers
from src.research.llm_ideation import CLAUDE_MODEL, TRANSLATE_SYSTEM_PROMPT


def run(country: str = "DE", per_cat: int = 20) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    print(f"Fetching Best Sellers for {country} (limit {per_cat}/cat)...")
    products = fetch_best_sellers(country=country, per_category_limit=per_cat)
    print(f"  → {len(products)} unique products\n")
    if not products:
        print("Nothing to translate. Aborting.")
        return

    # Compact input — same shape as translate_products_to_keywords, with
    # inline double-quotes stripped from free-text (see that function's
    # docstring for the rationale — Claude mis-escapes them on echo).
    def _clean(s: str) -> str:
        return (s or "").replace('"', "").replace("“", "").replace("”", "").replace("„", "").strip()

    compact = []
    for p in products:
        row = {"title": _clean(p.title)}
        if p.brand:
            row["brand"] = _clean(p.brand)
        cat = " > ".join(x for x in [p.category_l1, p.category_l2, p.category_l3] if x)
        if cat:
            row["category"] = cat
        if p.relative_demand_change:
            row["demand_change"] = p.relative_demand_change
        compact.append(row)

    country_name = {"DE": "Germany", "US": "United States"}.get(country, country)
    lang_name = {"DE": "German", "US": "English"}.get(country, country)

    user_prompt = (
        f"Target market: {country_name} ({country}) / {lang_name}.\n\n"
        f"Convert each of these {len(compact)} top-selling products on Google "
        f"Shopping into up to 3 head-term search keywords a buyer would type. "
        f"Skip products that are branded-only, regulated, perishable, "
        f"counterfeit-risk, or unsuitable for AliExpress dropshipping.\n\n"
        f"Products (JSON):\n{json.dumps(compact, ensure_ascii=False)}\n\n"
        f"Return ONLY the JSON object described in the system prompt."
    )

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    print("Calling Claude translator...")
    msg = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=20000,
        temperature=0.4,
        system=TRANSLATE_SYSTEM_PROMPT,
        messages=[
            {"role": "user", "content": user_prompt},
            {"role": "assistant", "content": "{"},
        ],
    )
    raw_text = "".join(b.text for b in msg.content if hasattr(b, "text")).strip()
    full = "{" + raw_text

    # Stash the raw response for offline inspection if the parse fails.
    raw_path = Path("/tmp/dryrun_translator_raw.json")
    raw_path.write_text(full, encoding="utf-8")
    print(f"  → raw response written to {raw_path} ({len(full):,} chars)")

    # Strip code fences if Claude used them
    cleaned = full
    if cleaned.startswith("{```") or "```json" in cleaned[:20]:
        cleaned = cleaned.replace("```json", "").replace("```", "").strip()

    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError as je:
        print(f"\n✗ JSON parse FAILED: {je}")
        # Dump the offending window
        near = max(je.pos - 120, 0)
        print(f"  …near char {je.pos}:")
        print("  " + cleaned[near:je.pos + 120].replace("\n", " "))
        print(f"\n  Stop reason: {msg.stop_reason}")
        print(f"  Tokens: in={msg.usage.input_tokens} out={msg.usage.output_tokens}")
        print(f"\nRaw response is at {raw_path} for inspection.")
        sys.exit(1)

    translations = data.get("translations", [])
    kept, skipped, empty = [], [], []
    for t in translations:
        if not isinstance(t, dict):
            continue
        if t.get("skip_reason"):
            skipped.append(t)
        elif not t.get("keywords"):
            empty.append(t)
        else:
            kept.append(t)

    cost_in = msg.usage.input_tokens / 1_000_000 * 3
    cost_out = msg.usage.output_tokens / 1_000_000 * 15
    print()
    print("=" * 80)
    print(f"RESULT: {len(translations)} products → "
          f"{len(kept)} kept  |  {len(skipped)} skipped  |  {len(empty)} empty")
    print(f"Tokens: in={msg.usage.input_tokens:,}  out={msg.usage.output_tokens:,}  "
          f"cost≈${cost_in + cost_out:.4f}")
    print("=" * 80)

    print("\nSKIPS by reason:")
    for reason, n in Counter(s.get("skip_reason") for s in skipped).most_common():
        print(f"  {n:>3}  {reason}")

    print("\nSkipped products (all):")
    for s in sorted(skipped, key=lambda x: x.get("skip_reason") or ""):
        print(f"  [{(s.get('skip_reason') or '?'):<20}] {(s.get('source_title') or '')[:80]}")

    total_kw = sum(len(k.get("keywords") or []) for k in kept)
    print(f"\nKEPT: {len(kept)} products → {total_kw} raw keywords\n")
    print("Kept products and their keywords:")
    for k in kept:
        title = (k.get("source_title") or "")[:70]
        kws = [(kw.get("keyword") or "").strip() for kw in (k.get("keywords") or [])]
        kws = [x for x in kws if x]
        print(f"  {title}")
        for kw in kws:
            print(f"      → {kw}")


if __name__ == "__main__":
    country = sys.argv[1] if len(sys.argv) > 1 else "DE"
    per_cat = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    run(country=country, per_cat=per_cat)
