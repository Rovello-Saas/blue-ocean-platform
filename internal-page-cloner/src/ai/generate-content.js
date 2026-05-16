const { callClaudeWithImage } = require('./client');
const { extractPalette } = require('../scraper/palette-extractor');
const fs = require('fs');
const path = require('path');
const sharp = require('sharp');

// Reference liquid files live at the project root, one per store.
// We pick the one that matches the destination brand — e.g. a Merivalo clone
// uses the live Merivalo `cloud-alignment-pillow` content as its structural
// blueprint (purple + coral accents, du-form copy), and a Movanella clone uses
// the Movanella version (blue + green). Falling back to cloud-alignment-pillow-content.liquid
// keeps older setups working.
const REFERENCE_FILES = {
  merivalo: path.join(__dirname, '../../../merivalo-reference.liquid'),
  movanella: path.join(__dirname, '../../../movanella-reference.liquid')
};
const LEGACY_REFERENCE_FILE = path.join(__dirname, '../../../cloud-alignment-pillow-content.liquid');

function getReferencePath(storeId) {
  const candidate = REFERENCE_FILES[storeId];
  if (candidate && fs.existsSync(candidate)) return candidate;
  return LEGACY_REFERENCE_FILE;
}

const LANGUAGE_LABELS = {
  en: 'English',
  de: 'German (du-form, NOT Sie-form)',
  fr: 'French',
  es: 'Spanish',
  it: 'Italian',
  nl: 'Dutch'
};

// Currency mapping per target language. Source pages are typically USD; we
// convert to the shopper's market currency before showing the AI. Keep this
// list narrow — add more only when a market is wired up end-to-end.
const LANGUAGE_CURRENCY = {
  de: { code: 'EUR', symbol: '€', rateKey: 'USD_EUR' },
  fr: { code: 'EUR', symbol: '€', rateKey: 'USD_EUR' },
  es: { code: 'EUR', symbol: '€', rateKey: 'USD_EUR' },
  it: { code: 'EUR', symbol: '€', rateKey: 'USD_EUR' },
  nl: { code: 'EUR', symbol: '€', rateKey: 'USD_EUR' }
};

let _fxRates = null;
function loadFxRates() {
  if (_fxRates) return _fxRates;
  try {
    _fxRates = JSON.parse(fs.readFileSync(path.join(__dirname, '../../data/fx-rates.json'), 'utf-8'));
  } catch (e) {
    _fxRates = { USD_EUR: 0.92 };
  }
  return _fxRates;
}

function currencyForLanguage(lang) {
  return LANGUAGE_CURRENCY[lang] || null;
}

// Convert a price string like "$169" / "169.00" / "USD 169" to the target
// currency. Returns the converted display string with the new symbol, e.g.
// "€156". Falls back to the input unchanged if no number can be parsed.
function convertPriceString(input, currency) {
  if (input == null || input === '' || input === 'N/A') return input;
  const text = String(input);
  const match = text.match(/(\d+(?:[.,]\d{1,2})?)/);
  if (!match) return input;
  const usd = parseFloat(match[1].replace(',', '.'));
  if (!isFinite(usd)) return input;
  const rates = loadFxRates();
  const rate = rates[currency.rateKey];
  if (!rate) return input;
  const converted = Math.round(usd * rate);
  return `${currency.symbol}${converted}`;
}

function getSystemPrompt(storeId, targetLanguage, layoutMode = 'source_clone') {
  const storeConfig = {
    movanella: {
      name: 'Movanella', domain: 'movanella.com',
      lang: 'English'
    },
    merivalo: {
      name: 'Merivalo', domain: 'merivalo.com',
      lang: 'German (du-form, NOT Sie-form)'
    }
  };
  const store = storeConfig[storeId] || storeConfig.movanella;
  // Override language if explicitly requested
  if (targetLanguage && LANGUAGE_LABELS[targetLanguage]) {
    store.lang = LANGUAGE_LABELS[targetLanguage];
  }

  if (layoutMode === 'brand_pdp') {
    return `You are a senior Shopify PDP strategist and designer for the ${store.name} store. You create complete, production-ready custom Liquid content sections for product pages.

Brand: ${store.name} (${store.domain})
Language: ${store.lang}

This job is NOT a 1-to-1 page clone. It is a researched ${store.name} product page using our fixed high-converting PDP structure.

## CORE OBJECTIVE

Use the reference competitor page, supplier/product page, screenshot, scraped section blueprint, product metadata, and image list as research inputs. Build an original ${store.name} PDP body that:
- Uses ${store.name}'s own page structure and wording.
- Uses the supplier/product images when available for the product gallery and content.
- Learns facts, benefits, use cases, features, specs, and objections from competitor pages.
- Rewrites all copy from scratch so it is copyright-safe and brand-safe.
- Replaces every source brand mention with "${store.name}" or generic product wording.

## FIXED BRAND PDP STRUCTURE

Your output starts BELOW the Shopify hero/buy-box. Do not recreate the product hero, price, bundle picker, variant picker, or add-to-cart form.

Emit these body sections in this order when the data supports them:
1. Outcome-led intro strip: 3-5 compact benefit tiles.
2. Main benefits section: a clean grid of customer outcomes.
3. Product / technology section: what the product does and why the mechanism matters.
4. How to use: 3-4 steps, practical and specific.
5. Feature/spec section: materials, settings, controls, size, safety, maintenance, included items.
6. Comparison section: ${store.name} versus ordinary alternatives or expensive sessions, without naming source competitors.
7. Results / expectation section: realistic routine-based outcomes, no fake clinical numbers.
8. Safety and trust section: practical cautions, comfort, warranty/returns/shipping if supported by source facts.
9. FAQ accordion: 6-9 questions based on buyer objections and product use.

For an infrared sauna blanket, prioritize these research topics when present:
- Far-infrared warmth, sweat support, relaxation, muscle comfort, post-workout recovery, circulation/comfort language.
- Temperature range, session length, timers, layers/materials, waterproof/cleanable inner layer, controller, portability/storage.
- How to use safely: hydrate, start low, wear clothing or use towel barrier, wipe clean, do not use when contraindicated.
- Avoid medical-cure claims. Use "supports", "helps you feel", "designed for", "can help with" rather than disease claims.

## RESEARCH AND COPYRIGHT RULES

- Do not copy competitor sentences. Rewrite and reframe all text.
- Do not preserve the competitor's section names unless they are generic.
- Do not use source brand names in visible copy.
- Do not invent certifications, FDA/medical claims, clinical percentages, review counts, awards, or guaranteed results.
- If exact technical specs are present, keep them as specs. If specs are uncertain, phrase them generically.
- If supplier data conflicts with competitor data, prefer conservative language or omit the claim.
- Keep claims ecommerce-safe and Google Merchant Center-safe.

## DESIGN RULES

- Use a polished wellness / home-spa PDP style: premium, calm, high contrast, spacious but not oversized.
- Do not mimic the competitor's layout exactly. The page must look like ${store.name}'s own template.
- Use one coherent accent color from the source/product palette. For sauna/wellness products, warm burgundy, clay, rose, charcoal, cream, or soft taupe usually fit. Avoid bright green unless the product/source truly uses it.
- Images must never sit in giant empty boxes. Image wrappers hug the image and use natural proportions.
- Content-card images: width 100%, height auto, object-fit contain, white/cream background.
- Do not use 100vh poster sections, massive hero-like blocks, or sections with large empty vertical whitespace.
- Mobile breakpoint: @media (max-width: 749px).
- Output a complete file with one <style> block, one wrapper div using a unique 2-4 character CSS prefix, and one <script> block for FAQ accordion/interactions.

## REQUIRED OUTPUT FORMAT

Return ONLY raw HTML/CSS/JS. No markdown. No explanation.`;
  }

  return `You are a Shopify page-cloning designer for the ${store.name} store. You create complete, production-ready custom Liquid content sections for product pages.

Brand: ${store.name} (${store.domain})
Language: ${store.lang}
Brand voice: Translate the source page's voice faithfully. Do NOT impose ${store.name}'s house tone over the source's tone. If the source is playful, the target language version is playful; if clinical, clinical; if punchy, punchy. Your job is to localize copy, not rebrand it.

You will receive a screenshot and scraped data from a source product page. Your job is to create a COMPLETE Liquid file (HTML + CSS + JavaScript) that faithfully recreates the source product page's visual direction and section flow inside Shopify. All text must be in ${store.lang}. Never use the source brand name in written copy — use generic product language or "${store.name}" where a brand is required.

## FIDELITY PRIORITY

This is a page clone, not a generic ${store.name} template.
- Preserve the source page's color palette from the screenshot and image assets. If the source page is blush/pink/rose/cream, use blush/pink/rose/cream. If it is blue, use blue. Do NOT force ${store.name}'s default colors.
- Preserve the source page's BODY section sequence and visual concepts (the hero/gallery/buy-box is rendered separately by Movanella above your output and is OUT of scope): benefits grid, science/technology explanation, how-to-use, results/statistics, comparison chart, before/after proof, expert/social proof, guarantee, FAQ.
- Preserve source image compositions. If an image is already a before/after composite, comparison chart, infographic, dermatologist card, quote card, or guarantee graphic, use it as ONE whole image. Do NOT split it into separate "before" and "after" images. Do NOT recreate it as unrelated cards.
- Keep the look AND copy close to the source. Translate text faithfully into ${store.lang}. Do NOT paraphrase, condense, or "improve" the source's wording. Strip any reference to the source brand by name.

## NUMBERS POLICY (HARD RULES)

For every numeric claim in the source, follow this table — never invent, never inflate:

- Clinical-study percentages (e.g. "33% / 31% / 30% after 8 weeks") → KEEP THE EXACT NUMBERS. Never replace 33 with 95, never round up, never invent new percentages.
- Customer counts ≥ 250,000 → soften to a believable number ≤ 50,000, or use phrasing like "tausende zufriedene Kund:innen" (German) / "thousands of happy customers" (other languages). Do NOT preserve "726,000+ customers" or similar inflated counts.
- Award counts ≥ 20 → replace the number with "ausgezeichnet" / "preisgekrönt" (German) / "award-winning". Do NOT print "60+ awards".
- Star rating across reviews: keep the average stars (round to 1 decimal, max 4.9). Drop the review count entirely if ≥ 50,000 — never claim "726,000 reviews".
- Retail-store presence claims ("in 2,100+ U.S. retailers", "available at Ulta / Nordstrom", "found in stores nationwide") → DROP THE ENTIRE SECTION. Do not localize. Do not emit a "we're in many European stores" replacement. The retail-presence concept is removed from the page.
- Years, dates, study durations (8 weeks, 12 weeks, 2024, etc.) → keep verbatim.
- Prices ($169, $250) → see CURRENCY POLICY (use the converted price from productMeta).

## SECTION FIDELITY (HARD REQUIREMENT)

The user message includes a SOURCE SECTION BLUEPRINT — a JSON array of every distinct content section detected on the source page, in vertical order, with headings, paragraphs, and images for each.

Your output MUST contain ONE rendered section for EACH meaningful blueprint entry, in the same order. Do not collapse multiple distinct source sections into one. Do not stop at "the recommended ten sections." If the blueprint has 14 entries, your output should have ~14 corresponding sections.

You MAY merge two near-duplicate adjacent blueprint entries (e.g. the same hero shown twice) into one. You MAY skip a blueprint entry only when it is clearly site chrome that escaped the scraper — navigation residue, cookie banner, related-products carousel, footer fragment. Otherwise emit it.

When you encounter these recurring source-page patterns, emit a section that mirrors the structure (do not flatten them into a generic content-row):

- **"Why us vs others" / competitive comparison** — usually a 2-column or 4-row layout with our-claim vs their-claim. Render as a 2-column comparison grid with checkmark-vs-X icons, NOT as plain bullets. **Localization rule: if the source brags about US-specific retailers ("2,100+ U.S. retailers", "Ulta / Nordstrom / Neiman Marcus") or shows a U.S. map, REPLACE the geographic claim with a localized equivalent for the target language market — e.g. for German, "über 2.100 Fachgeschäfte in Europa" with a Europe map or generic retail-rosette graphic. Never ship a U.S. map on a non-English page.**
- **"Good to know" educational bullets** — 3-5 short fact bullets with small icons. Emit as a compact icon-list strip, not as full content-rows.
- **"4 technologies / one device" tech breakdown** — 4 cards in a grid, each with an icon/diagram, a technology name, and a 1-2-sentence description. Emit as a 4-card grid, NOT as 4 stacked content-rows.
- **3-step "how to use"** — a numbered 1/2/3 step layout with a small image or icon per step. Emit as a 3-column step grid.
- **Shipping / returns / warranty 3-4 box strip** — small icon + heading + one-line copy in 3 or 4 columns. Emit as an icon-strip, not as content-rows.
- **"Targeted ritual" / 4 benefits with icons** — 4 column grid with icons and short labels. Emit as a 4-column icon grid.
- **"Inside the tech" / labeled diagram** — a central product photo with 4 callouts pointing to features. Emit as a centered figure with surrounding labeled blocks (or as the source image used whole if it already has the labels baked in).
- **Expert / doctor endorsement cards** — 2-3 expert headshots with name, credentials, affiliation, and an optional quote or video thumbnail only when the source provides distinct expert cards. If the source provides one complete dermatologist/expert graphic, show it as one complete image; do not invent extra experts, names, credentials, or affiliations.
- **Real results / before-after testimonials carousel** — a horizontal scroller of 4+ before/after pairs, each with a customer name, age, result label, and quote. Use a slider/carousel, not a stack of 3 separate sections.

## REQUIRED OUTPUT STRUCTURE

Your output must be a complete file with these parts:

1. \`<style>\` block with ALL CSS (unique prefix per product, e.g. \`xyz-\` for "XYZ Product")
2. \`<div class="PREFIX-wrap">\` containing all sections in source order
3. \`<script>\` block for any interactivity (FAQ accordion, before/after slider, carousels)

## YOU BUILD THE BODY SECTIONS — NOT THE HERO

The Movanella product hero (gallery + product title + price + bundle picker + add-to-cart) renders ABOVE your output, from the store's existing Horizon theme + bundle app. Do NOT recreate it. Your output starts at the FIRST BODY SECTION (eyebrow, benefits grid, science explanation, comparison, before/after, expert proof, FAQ, etc).

DO NOT emit ANY of the following — they already exist in the Movanella hero above:
- Product title (\`<h1>\`) or \`{{ product.title }}\`
- Price block, compare-at strikethrough, \`{{ product.price | money }}\`
- Star rating with review count
- Product image gallery (main shot + thumbnails)
- Variant / color / size picker
- Add-to-cart form, \`<form action="/cart/add">\`, ATC button
- "Choose your offer" / Single / Duo / Trio bundle UI
- Trust-bar with shipping/guarantee icons (the Movanella hero already has this)

Your first emitted section should be the FIRST BODY SECTION the source page shows BELOW its own hero — usually a "Why us vs others" comparison, a benefits / "ritual" grid, a science explanation, or a 3-step "how to use". Skip blueprint entries flagged \`consumedByHero: true\` — those are source-hero elements that Movanella's hero already covers.

In-body price CTAs (e.g. comparison cards saying "Movanella €169 vs dermatologist €250–800") are FINE to include — but they should link to \`#shopify-section-template--main\` or use \`<a href="#add-to-cart">\` to scroll to the existing hero, NOT emit a duplicate ATC form.

## RECOLOR THE MOVANELLA HERO TO THE SOURCE PALETTE

Your output ALSO injects CSS that overrides the Movanella hero's theme colors — the green ATC button and the green checkmark ticks — to use the SOURCE PAGE'S accent color (from the palette extracted from the screenshot). This is how a Solawave clone shows pink/blush controls in Movanella's hero, not Movanella green. Include this CSS block at the top of your \`<style>\` (replace \`{{ACCENT}}\` and \`{{ACCENT_DARK}}\` with the actual hex values from the SOURCE DESIGN PROFILE):

\`\`\`css
/* Recolor Movanella's stock product-info to match the source palette */
.add-to-cart-button { background: {{ACCENT}} !important; border-color: {{ACCENT}} !important; }
.add-to-cart-button:hover, .add-to-cart-button:focus { background: {{ACCENT_DARK}} !important; }
.pd-rating { color: {{ACCENT}} !important; }
.pd-rating svg, .pd-rating path { fill: {{ACCENT}} !important; }
[id^="shopify-section-template"][id$="__main"] span[style*="rgb(7, 148, 26)"],
[id^="shopify-section-template"][id$="__main"] span[style*="#07941a"] { color: {{ACCENT}} !important; }
[id^="shopify-section-template"][id$="__main"] [class*="check"] svg path,
[id^="shopify-section-template"][id$="__main"] [class*="tick"] svg path { fill: {{ACCENT}} !important; }
\`\`\`

This CSS is ONLY loaded on cloned product templates (custom_liquid_cloned), so it cannot affect non-cloned Movanella products.

## CRITICAL RULES

- Generate a UNIQUE 2-4 character CSS class prefix from the product name (e.g., \`cap-\` for Cloud Alignment Pillow, \`mls-\` for Motion LED Strip)
- Use source product images by URL directly (they will be hosted on Shopify CDN after upload)
- Color scheme:
    • Derive colors from the source screenshot and source image assets.
    • For Solawave-like beauty pages, prefer soft blush/pink/rose, warm beige/cream, muted coral, deep burgundy/plum text, and white cards. Avoid Movanella blue/green.
    • Use one coherent accent color for buttons, stats, stars, checkmarks, and links. It should match the source page's pop color.
    • Do NOT use green (#07941a, #16a34a, #22c55e, etc.) unless the source page itself is green.
    • Do NOT use navy/blue dark sections unless the source page uses blue. If the source uses pink/cream, use pink/cream.
- Font: \`-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif\`
- Mobile breakpoint: \`@media (max-width: 749px)\` — Horizon theme uses 749px
- SVG circle stats: use \`stroke-dasharray\` to show percentage (e.g., 207 = full circle with r=33, so 97% = \`stroke-dasharray="201 20"\`). Ring stroke = source accent color.
- FAQ accordion JavaScript: toggle \`.open\` class on \`.PREFIX-faq-item\`
- TRANSLATION, NOT REWRITING. The SOURCE SECTION BLUEPRINT below contains \`headingsText\`, \`paragraphsText\`, and \`bullets\` arrays for every detected source section. Translate these faithfully into ${store.lang}. Do NOT paraphrase, embellish, condense, or "improve". A short German clause that mirrors the source is correct; an expanded marketing sentence is wrong. Strip the source brand name where it appears in copy.
- NEVER FABRICATE NUMBERS. If the source shows "33% reduction in fine lines after 8 weeks", you emit the German for "33% reduction in fine lines after 8 weeks". Never invent percentages, customer counts, award counts, or retailer counts. Apply the NUMBERS POLICY above to soften specific claims that look implausibly large for ${store.name}.
- REVIEWS — generate 4–6 short, plausible ${store.lang} reviews tied to product benefits. Each review: a first name, a star rating between 4 and 5, 1–3 short sentences, no implausible claims. Average rating around 4.7–4.8 (one decimal, max 4.9). Do NOT include a review count from the source (no "726.000 Kund:innen", no "12,400+ Bewertungen"). If you must show a count, keep it under 5,000.
- The only place where you may compose new copy from scratch is when localizing locale-specific claims that must be dropped (US retail-presence) or making minor grammatical adjustments for the target language.
- CURRENCY: when you reference a price in body copy (e.g. comparison cards saying "€169 vs dermatologist treatment €800"), use the productMeta.price value exactly as provided — it has already been converted to the target market's currency. Never reference USD or "$" anywhere in the visible copy of a non-English clone. Movanella's hero above your output renders \`{{ product.price | money }}\` for the buy-box; that's not your concern.
- FINANCING: DROP any "X interest-free payments of $Y", "as low as $Z/mo with Affirm", "Buy now pay later", or similar instalment-financing line. Do NOT localize to Klarna, Afterpay, or Sezzle unless productMeta.financing is explicitly set (it is not, today). On a non-English clone, simply omit any financing copy that appears in the source.
- SVG icons: use Feather-style stroke icons (stroke=source heading color, stroke-width="1.5", fill="none", viewBox="0 0 24 24"). For small decorative accent icons (checkmarks in the trust bar, stars), use source accent color.
- IMAGE CROPPING — content-card images (use-case cards, feature cards, diagrams, sleep-position illustrations, comparison images, review photos) MUST use \`aspect-ratio: 4/3; object-fit: contain; background: #fff; padding: 8px;\` — NEVER \`height: Npx; object-fit: cover\`. Source images are frequently infographics with labels, icons, arrows, or text baked in; \`cover\` crops that content off. Only the main hero or dark-hero banner — an edge-to-edge lifestyle photo — may use \`object-fit: cover\` (and even then, prefer a large min-height over a fixed pixel height).
- BODY LAYOUT QUALITY — never create oversized empty frames around images. Do NOT use \`min-height: 100vh\`, \`height: 100vh\`, \`height: 80vh\`, \`padding-top\`/ \`padding-bottom\` above 96px, or full-screen poster sections in the cloned body. Every section after the hero should feel like a PDP content block, not a landing-page hero.
- IMAGE FRAME QUALITY — image wrappers must hug the image. If an image is a complete card/infographic/composite, render it at natural proportions with \`width: 100%; height: auto; object-fit: contain; padding: 0\`. Do not put a 600px image in a 1200px tall white box. Do not add large blank white space above or below images.
- BACKGROUND COLOR QUALITY — never use solid green, chroma-key green, or Movanella green as an image background unless the source image itself had that exact green background. For Solawave-like skincare pages, image frames should be white, cream, blush, or transparent, and dark sections should use a readable deep berry tone, not near-black.
- TEXT-IN-IMAGE QUALITY — do not invent new text inside images. Do not output typo-prone generated text such as misspelled instructions. If a source image already has baked-in text, the image-editing pipeline will reframe/translate it; in Liquid, use the completed image whole instead of recreating its text.
- BEFORE/AFTER RULE — DO NOT build the before/after / "Real Results" / "Real Skin. Real Change." section yourself. The post-processor builds it deterministically from the labeled image pairs the scraper detected, because you don't reliably know which "before" matches which "after" image. Instead, emit the placeholder marker:
    \`<!-- BEFORE_AFTER_SLIDER_PLACEHOLDER -->\`
  on its own line, in the spot where this section should sit (typically between the technology / clinical-results sections and the expert-endorsement section). The post-processor will replace the placeholder with a working drag-to-reveal carousel using the correct pairs and localized labels. Never reference before-after composite images, "DAY 0/DAY 30" photos, or testimonial faces in any other section — they belong only in this slot.
- DROP-LIST — These source images must NOT appear ANYWHERE in your output. Do not reference them, do not embed them, do not even mention them:
  • Any U.S. map / retail-locations graphic (filenames like \`map-locations\`, \`us-map\`, \`retailer-map\`). Skip the entire "we're in 2,100+ retailers" claim — do not include a retail-presence section, do not write a localized text-only equivalent, just drop this concept from the page. The country outline cannot be reshaped by image translation and a German page does not need a U.S.-centric retail brag.
  • Cross-promotional images of OTHER products (Solawave LED face mask, neck/décolleté mask, any device that is not the wand on this page). Filenames containing \`mask\`, \`led-mask\`, \`face-mask\`, \`pro-mask\` are a giveaway.
  • Award badge collages, magazine logo strips, and press-mention covers. Reference brands by name in copy instead.

## OUTPUT FORMAT

Return ONLY the raw HTML/CSS/JS code. No markdown code fences. No explanation text. Just the complete \`<style>...<div>...<script>\` file.`;
}

/**
 * Generate a complete custom liquid content file for a product.
 * Single AI call that produces the entire file.
 */
async function generateFullLiquid(productMeta, sections, screenshotPath, storeId = 'movanella', targetLanguage = null, options = {}) {
  const storeNames = { movanella: 'Movanella', merivalo: 'Merivalo' };
  const storeName = storeNames[storeId] || 'Movanella';
  const layoutMode = options.layoutMode === 'brand_pdp' ? 'brand_pdp' : 'source_clone';
  const supplierMeta = options.supplierMeta || null;
  const supplierSections = options.supplierSections || [];
  const extraResearch = Array.isArray(options.extraResearch) ? options.extraResearch : [];
  const productUrl = options.productUrl || productMeta.productResearch?.productUrl || null;

  // Convert source USD prices to the target market's currency before feeding
  // them to the AI. The source pages we clone (e.g. Solawave) are USD; the
  // target shopper (e.g. Movanella DE) sees euros. We mutate productMeta in
  // place so every downstream consumer (system prompt, user message, fallback
  // copy) reads the localized number.
  const currency = currencyForLanguage(targetLanguage);
  if (currency) {
    productMeta.price = convertPriceString(productMeta.price, currency);
    productMeta.compareAtPrice = convertPriceString(productMeta.compareAtPrice, currency);
    productMeta.currency = currency.code;
  }
  // Load reference file for the user message context.
  // Per-store — each store has its own design system (Merivalo purple+coral vs
  // Movanella blue+green), and the AI copies patterns from whatever reference
  // we feed it. Feeding a Movanella ref to a Merivalo clone = wrong colors.
  let referenceSnippet = '';
  const refPath = getReferencePath(storeId);
  try {
    const ref = fs.readFileSync(refPath, 'utf-8');
    // Include just the HTML structure (skip the CSS) as a structural reference
    const htmlStart = ref.indexOf('<div class="cap-wrap">');
    const htmlEnd = ref.indexOf('</script>');
    if (htmlStart > 0 && htmlEnd > 0) {
      referenceSnippet = ref.substring(htmlStart, htmlEnd + '</script>'.length);
    }
    console.log(`  [AI] Using reference: ${path.basename(refPath)}`);
  } catch (e) {
    console.log(`  [AI] Reference file not found at ${refPath}, continuing without it`);
  }

  // Resize screenshot
  const resizedBuffer = await sharp(screenshotPath)
    .resize({ width: 800, height: 6000, fit: 'inside', withoutEnlargement: true })
    .jpeg({ quality: 50 })
    .toBuffer();

  console.log(`  [AI] Screenshot: ${(resizedBuffer.length / 1024 / 1024).toFixed(1)}MB`);
  const screenshotBase64 = resizedBuffer.toString('base64');

  // Build available images list with semantic labels so Claude can pick the
  // right image for each section (e.g. "side sleeper" card, "size guide" card).
  // Label = alt text if non-empty, else the filename stem (lowercased, dashes/underscores → spaces).
  const seenSrcs = new Set();
  const availableImagesWithLabels = [];

  function labelFor(img) {
    const alt = (img.alt || '').trim();
    if (alt && alt.length > 1) return alt;
    try {
      const u = new URL(img.src);
      const base = u.pathname.split('/').pop() || '';
      const stem = base.replace(/\.[a-z0-9]+$/i, '');
      return stem.replace(/[-_]+/g, ' ').replace(/\s+/g, ' ').trim();
    } catch (e) {
      return '';
    }
  }

  // Build a section-by-section blueprint. This is more important than a generic
  // "recommended flow": it tells the generator which source sections appeared,
  // in which order, and which image belonged to each section.
  //
  // Shape per entry: headingsText / paragraphsText / bullets are full source
  // strings (paragraphs capped at ~2500 chars per section, sentence-aware) so
  // the AI translates them faithfully instead of paraphrasing from a truncated
  // hint. numbersInSection lists every percentage/integer detected in the
  // source so the AI can apply NUMBERS POLICY (drop/soften/keep) explicitly.
  const sectionSummary = sections.slice(0, 18).map(s => ({
    sourceIndex: s.index,
    className: s.className,
    layout: s.layout,
    boundingRect: s.boundingRect,
    headingsText: (s.headings || [])
      .map(h => (typeof h === 'string' ? h : h.text || ''))
      .filter(Boolean)
      .slice(0, 8),
    paragraphsText: capParagraphsToTotal((s.paragraphs || []).slice(0, 8), 2500),
    bullets: (s.bullets || []).slice(0, 12),
    numbersInSection: extractNumbersFromSection(s),
    images: (s.images || []).slice(0, 4).map(img => ({
      label: labelFor(img),
      src: img.src,
      sourceRole: img.sourceRole || null,
      displaySize: `${img.displayWidth || 0}x${img.displayHeight || 0}`,
      ratio: img.ratio || null,
      ratioClass: img.ratioClass || null,
      isBackground: !!img.isBackground
    }))
  }));

  // Mark the first entry as already rendered by the hero/buy-box if it sits at
  // the top of the page AND its headings include the product title. The hero
  // spec in the system prompt already covers title + price + bullets + ATC, so
  // emitting a body section for the same content produces the duplicate-hero
  // bug we saw on movanella.com.
  if (sectionSummary.length > 0) {
    const first = sectionSummary[0];
    const isTopOfPage = (first.boundingRect?.top ?? 9999) < 800;
    const titleTokens = (productMeta.title || '')
      .toLowerCase()
      .split(/\s+/)
      .filter(t => t.length > 3);
    const headingsLower = first.headingsText.join(' ').toLowerCase();
    const matchesTitle = titleTokens.length > 0 &&
      titleTokens.some(tok => headingsLower.includes(tok));
    if (isTopOfPage && (matchesTitle || first.headingsText.length === 0)) {
      first.consumedByHero = true;
    }
    // Forward sticky-hero hint from the scraper if present
    if (sections[0]?.stickyHero) {
      first.stickyHero = true;
    }
  }

  function pushImage(img, sourceType = 'source image') {
    if (!img?.src || seenSrcs.has(img.src)) return;
    seenSrcs.add(img.src);
    availableImagesWithLabels.push({ src: img.src, label: labelFor(img), sourceType });
  }

  // 1. Product gallery images first — these are the canonical product photos.
  //    Raise cap from 8 → 25 so sleep-position photos / size guides / callouts
  //    that sit later in the gallery actually reach the liquid generator.
  (productMeta.images || []).slice(0, 25).forEach(img => {
    const pageType = img.sourcePageType === 'supplier'
      ? 'SUPPLIER / PRODUCT SOURCE '
      : img.sourcePageType === 'reference'
        ? 'REFERENCE PAGE '
        : '';
    const role = img.sourceRole === 'product-media-gallery'
      ? `${pageType}PRODUCT MEDIA CAROUSEL / product-card thumbnail`
      : img.sourceRole === 'product-structured-data'
        ? `${pageType}PRODUCT STRUCTURED DATA / product-card image`
        : `${pageType}PRODUCT GALLERY / product-card image`;
    pushImage(img, role);
  });

  // 2. Important images from each scraped section for section-specific diagrams,
  // before/after proof, comparison charts, usage callouts, guarantees, etc.
  // We keep the cap conservative for prompt size, but high enough for pages
  // like Solawave where the product gallery doubles as a long visual PDP.
  sections.slice(0, 18).forEach(s => {
    if (availableImagesWithLabels.length >= 42) return;
    (s.images || []).slice(0, 2).forEach(img => {
      pushImage(img, `SOURCE SECTION ${s.index}`);
    });
  });

  supplierSections.slice(0, 10).forEach(s => {
    if (availableImagesWithLabels.length >= 52) return;
    (s.images || []).slice(0, 2).forEach(img => {
      pushImage(img, `PRODUCT / SUPPLIER SECTION ${s.index}`);
    });
  });

  extraResearch.slice(0, 4).forEach((research, researchIndex) => {
    (research.sections || []).slice(0, 6).forEach(s => {
      if (availableImagesWithLabels.length >= 60) return;
      (s.images || []).slice(0, 1).forEach(img => {
        pushImage(img, `EXTRA RESEARCH ${researchIndex + 1} SECTION ${s.index}`);
      });
    });
  });

  // Flat list of URLs (kept for backwards-compatible consumers below)
  const availableImages = availableImagesWithLabels.map(x => x.src);
  // Extract a real palette from the source screenshot so the AI uses the
  // actual source colors instead of inferring them visually (which kept
  // falling back to Movanella defaults for source pages with non-default
  // palettes — e.g. Solawave's pink/blush rendering as Movanella green).
  let palette = null;
  try {
    palette = await extractPalette(screenshotPath);
    console.log(`  [AI] Extracted palette: accent=${palette.accent} dark=${palette.accentDark} surface=${palette.surface}`);
  } catch (e) {
    console.log(`  [AI] Palette extraction failed (${e.message}); falling back to inferred design only`);
  }
  const sourceDesign = inferSourceDesign(productMeta, sections, availableImagesWithLabels, palette);
  const criticalImages = availableImagesWithLabels.filter(x =>
    /(before[-\s]?after|before and after|day\s*0|day\s*30|real results|comparison chart|dermatologist|guarantee|how to use|easy to use|3-5x|visible results)/i.test(x.label || '')
  );
  const beforeAfterAssets = findBeforeAfterImages(availableImagesWithLabels);
  const productCardAssets = findProductCardVisualAssets(availableImagesWithLabels);

  const supplierSectionSummary = supplierSections.slice(0, 10).map(s => ({
    sourceIndex: s.index,
    className: s.className,
    headingsText: (s.headings || [])
      .map(h => (typeof h === 'string' ? h : h.text || ''))
      .filter(Boolean)
      .slice(0, 6),
    paragraphsText: capParagraphsToTotal((s.paragraphs || []).slice(0, 6), 1600),
    bullets: (s.bullets || []).slice(0, 12),
    numbersInSection: extractNumbersFromSection(s),
    images: (s.images || []).slice(0, 4).map(img => ({
      label: labelFor(img),
      src: img.src,
      sourceRole: img.sourceRole || null,
      displaySize: `${img.displayWidth || 0}x${img.displayHeight || 0}`,
      ratio: img.ratio || null,
      ratioClass: img.ratioClass || null,
      isBackground: !!img.isBackground
    }))
  }));

  const extraResearchSummary = extraResearch.slice(0, 4).map((research, researchIndex) => ({
    sourceIndex: researchIndex,
    url: research.url,
    meta: research.meta ? {
      title: research.meta.title,
      price: research.meta.price,
      description: research.meta.description?.substring(0, 800),
      imageCount: research.meta.images?.length || 0
    } : null,
    sections: (research.sections || []).slice(0, 8).map(s => ({
      sourceIndex: s.index,
      headingsText: (s.headings || [])
        .map(h => (typeof h === 'string' ? h : h.text || ''))
        .filter(Boolean)
        .slice(0, 5),
      paragraphsText: capParagraphsToTotal((s.paragraphs || []).slice(0, 5), 1200),
      bullets: (s.bullets || []).slice(0, 10),
      numbersInSection: extractNumbersFromSection(s)
    }))
  }));

  const modeInstructions = layoutMode === 'brand_pdp'
    ? `## PDP APPROACH
Brand PDP template mode.
- Use the reference competitor page and supplier/product page as research sources.
- Do NOT mirror the competitor's section order or visual composition 1-to-1.
- Build the fixed ${storeName} PDP structure from the system prompt.
- Rewrite all copy from scratch, copyright-safe, with ${storeName} branding.
- Prefer supplier/product-link images for product visuals when available; use competitor/reference images only when they are generic product/lifestyle visuals and safe after image editing.
- Focus on useful buyer information: benefits, features, specs, how to use, safety, comparisons, and FAQs.`
    : `## PDP APPROACH
Source-style clone mode.
- Closely recreate the source page's body section flow and visual direction.
- Translate/localize source copy faithfully while removing source-brand names.`;

  const sourceBlueprintGuidance = layoutMode === 'brand_pdp'
    ? `This blueprint is research input, not a layout contract. Use it to understand benefits, features, objections, specs, and useful image context, then rebuild the page in the fixed Brand PDP template structure from the system prompt.`
    : `This blueprint is a HARD REQUIREMENT, not a recommendation. Every entry below should become a corresponding section in your output unless it is obviously site chrome (nav, footer, cookie banner, related-products). Do not collapse 4-card grids, 3-step how-tos, or expert-card rows into single content-rows. Do not stop at 10 sections — match the blueprint count.`;

  const finalInstruction = layoutMode === 'brand_pdp'
    ? `Now generate the complete Brand PDP template body for "${productMeta.title}". Use the fixed structure, researched benefits/specs, copyright-safe rewritten copy, unique CSS prefix, mobile responsive CSS, and FAQ JavaScript.`
    : `Now generate the complete file for "${productMeta.title}". Remember: unique CSS prefix, all sections, mobile responsive, FAQ JavaScript.`;

  const userMessage = `Create a complete ${storeName} product page liquid file for this product.

${modeInstructions}

## PRODUCT METADATA
- Title: ${productMeta.title}
- Price: ${productMeta.price}
- Compare-at Price: ${productMeta.compareAtPrice || 'N/A'}
- Description: ${productMeta.description?.substring(0, 500) || 'N/A'}
- Variants: ${JSON.stringify(productMeta.variants?.slice(0, 5) || [])}
- Reference URL: ${productMeta.productResearch?.referenceUrl || 'source URL provided in job'}
- Product / supplier URL: ${productUrl || 'not provided'}

## SUPPLIER / PRODUCT PAGE METADATA
${supplierMeta ? JSON.stringify({
  title: supplierMeta.title,
  price: supplierMeta.price,
  compareAtPrice: supplierMeta.compareAtPrice,
  currency: supplierMeta.currency,
  description: supplierMeta.description?.substring(0, 1000),
  variants: supplierMeta.variants?.slice(0, 10),
  imageCount: supplierMeta.images?.length || 0
}, null, 2) : 'No separate supplier/product page was scraped.'}

## SOURCE SECTION BLUEPRINT (${sectionSummary.length} entries)
${sourceBlueprintGuidance}

${JSON.stringify(sectionSummary, null, 2)}

## SUPPLIER / PRODUCT PAGE RESEARCH BLUEPRINT (${supplierSectionSummary.length} entries)
Use this as additional product research, specs, image context, and variant/detail evidence. In Brand PDP template mode, this is especially important. Do not copy supplier wording directly.

${supplierSectionSummary.length ? JSON.stringify(supplierSectionSummary, null, 2) : 'No separate supplier/product page sections available.'}

## EXTRA COMPETITOR / RESEARCH PAGES (${extraResearchSummary.length})
Use these pages as additional research for benefits, features, specs, objections, comparison angles, and FAQs. Rewrite everything; do not copy competitor wording.

${extraResearchSummary.length ? JSON.stringify(extraResearchSummary, null, 2) : 'No extra research pages provided.'}

## SOURCE DESIGN PROFILE
${sourceDesign.instructions}

## AVAILABLE IMAGES (use these URLs in your HTML — each has a semantic label)
${availableImagesWithLabels.map((x, i) => `${i + 1}. [${x.sourceType || 'source image'} | ${x.label || 'unlabeled'}]  ${x.src}`).join('\n')}

## CRITICAL SOURCE IMAGES TO PRESERVE AS WHOLE IMAGES
${criticalImages.length ? criticalImages.map((x, i) => `${i + 1}. [${x.sourceType || 'source image'} | ${x.label || 'unlabeled'}]  ${x.src}`).join('\n') : 'No critical composite images detected.'}

## BEFORE/AFTER SLIDER ASSETS
${formatBeforeAfterAssets(beforeAfterAssets)}

## PRODUCT CARD / MEDIA CAROUSEL VISUAL ASSETS
${formatProductCardAssets(productCardAssets)}

IMAGE USAGE RULES:
- The PRODUCT CARD / MEDIA CAROUSEL VISUAL ASSETS are the same card-like images the shopper sees in the source product media carousel. Use them only when they semantically match an actual body section. Do NOT create a standalone "product cards", "important product cards", "product carousel story", or grid-of-all-gallery-images section. That repeats the hero gallery and makes the PDP look machine-built.
- If a product-card image is an infographic, comparison card, stats card, usage card, or feature card, show it as a complete image in the one matching section. Keep the surrounding section compact and purposeful.
- Pick the SEMANTICALLY most appropriate URL for each slot. The label in square brackets tells you what each image shows (e.g. "hotel pillow meagan side sleeping" is a side-sleeper photo; "size guide" is a sizing diagram; "hotel pillow callouts" is a features-callout diagram).
- Prefer the PRODUCT GALLERY / product-card images for every image slot when they match the section. The Shopify product gallery and the generated content sections should reuse the same source assets, so the collection/product card and page body feel connected.
- For each SOURCE SECTION BLUEPRINT item that has an image, use that exact image URL in the matching cloned section where possible. Do not substitute a random lifestyle shot for a comparison chart, before/after image, usage infographic, stats graphic, or guarantee card.
- DO NOT reuse the same image URL across multiple different sections. If you have a "Side Sleeper / Back Sleeper / Stomach Sleeper" grid and three distinct sleeper photos are available, use three different URLs — one for each card. Only reuse an image if the layout intentionally shows the same product angle twice (e.g. hero + dark-hero split) AND no alternate angle is available.
- If you run out of distinct semantically-matching images for a section, pick the closest-fitting unused image rather than repeating one you already used.
- Prefer images with descriptive labels (diagrams, callouts, benefits, lifestyle shots) for content sections. Reserve the clean product-only shots for the gallery/hero.
- If a critical source image is listed above, preserve it as a complete visual asset in the matching section, except before/after proof images.
- Never build the before/after section yourself. Do not reference before/after composite images directly. The deterministic post-processor will size the proof carousel tightly; only emit \`<!-- BEFORE_AFTER_SLIDER_PLACEHOLDER -->\` where the proof section belongs.
${targetLanguage ? `- Target language is ${LANGUAGE_LABELS[targetLanguage] || targetLanguage}. Still reference the exact source image URLs in the HTML. The pipeline will edit those images with Nano Banana Pro, translate visible image text to ${LANGUAGE_LABELS[targetLanguage] || targetLanguage}, upload them to Shopify, and rewrite these URLs to the translated Shopify CDN versions.` : ''}

## REFERENCE STRUCTURE
Here is the HTML structure of a previous product page we built. Use it only for Liquid mechanics, class-prefix conventions, responsive CSS, and FAQ JavaScript. Do NOT copy its colors or force its section order when the source screenshot/images show a different visual style:

${referenceSnippet.substring(0, 4000)}

${finalInstruction}`;

  console.log(`  [AI] Generating full liquid content...`);
  const response = await callClaudeWithImage(
    getSystemPrompt(storeId, targetLanguage, layoutMode),
    screenshotBase64,
    userMessage,
    { maxTokens: 32000 }
  );

  // Clean up response — remove markdown fences if present
  let liquid = response;
  if (liquid.startsWith('```')) {
    liquid = liquid.replace(/^```(?:html|liquid)?\n?/, '').replace(/\n?```$/, '');
  }

  // Safety net — fix image-cropping CSS that would chop infographics/diagrams.
  // Even with an explicit prompt rule, the model sometimes emits
  // `height: 200px; object-fit: cover` on `.xxx-card img` selectors. Swap
  // those to `aspect-ratio: 4/3; object-fit: contain; background: #fff` so
  // the full source image stays visible. We only touch rules whose selector
  // ends with `img` AND that use a small fixed height (<= 400px), so hero
  // sections keep their intentional crop.
  liquid = sanitizeCardImageCropping(liquid);
  liquid = applySourcePaletteGuard(liquid, sourceDesign);
  liquid = injectBeforeAfterSliderFallback(liquid, beforeAfterAssets, sourceDesign, targetLanguage);
  liquid = ensureProductCardVisualsCovered(liquid, productCardAssets);
  liquid = injectHorizonAtcOverride(liquid, sourceDesign);
  liquid = injectCloneLayoutGuard(liquid, sourceDesign);

  // Validate it has the required parts
  if (!liquid.includes('<style>') || !liquid.includes('<div')) {
    throw new Error('AI output missing required <style> or <div> elements');
  }

  return liquid;
}

function findBeforeAfterImages(labeledImages) {
  const before = [];
  const after = [];
  const composite = [];

  for (const image of labeledImages) {
    const label = (image.label || '').toLowerCase();
    if (!label) continue;

    const isComposite = /before[-\s]?after|before and after|before\s*&\s*after|real results|day\s*0\s*(?:vs\.?|versus|\/|-|to)\s*day\s*(?:28|30|60|90)/i.test(label);
    const isBefore = /\bbefore\b|day\s*0\b|baseline|starting point|week\s*0\b/i.test(label);
    const isAfter = /\bafter\b|day\s*(?:28|30|60|90)\b|week\s*(?:4|6|8|12)\b|result|results/i.test(label);

    if (isComposite) {
      composite.push(image);
      continue;
    }
    if (isBefore) before.push(image);
    if (isAfter) after.push(image);
  }

  return {
    before: uniqueImages(before),
    after: uniqueImages(after),
    composite: uniqueImages(composite)
  };
}

function uniqueImages(images) {
  const seen = new Set();
  return images.filter(image => {
    if (!image?.src || seen.has(image.src)) return false;
    seen.add(image.src);
    return true;
  });
}

function formatBeforeAfterAssets(assets) {
  const lines = [];
  const pairCount = Math.min(assets.before.length, assets.after.length);
  for (let i = 0; i < pairCount; i++) {
    lines.push(`Pair ${i + 1}:`);
    lines.push(`- Before: [${assets.before[i].label || 'before'}] ${assets.before[i].src}`);
    lines.push(`- After: [${assets.after[i].label || 'after'}] ${assets.after[i].src}`);
  }
  if (assets.composite.length) {
    lines.push('Composite before/after images (show whole if no separate pair is available):');
    assets.composite.slice(0, 4).forEach((image, i) => {
      lines.push(`${i + 1}. [${image.label || 'before-after composite'}] ${image.src}`);
    });
  }
  return lines.length ? lines.join('\n') : 'No before/after assets detected.';
}

function findProductCardVisualAssets(labeledImages) {
  const cardLabelRe = /4[-\s]?in[-\s]?1|treatment|science|radiant|easy to use|how to use|3[-\s]?5x|visible results|comparison|vs\.?|others|benefit|feature|guarantee|chart|routine|step|week|stat|result|thermal|galvanic|red light|massage/i;
  const productMedia = labeledImages.filter(image =>
    /product (?:media carousel|gallery|structured data)|product-card/i.test(image.sourceType || '')
  );
  const semanticallyCardLike = productMedia.filter(image => cardLabelRe.test(image.label || ''));
  return uniqueImages(semanticallyCardLike.length >= 2 ? semanticallyCardLike : productMedia)
    .slice(0, 8);
}

function formatProductCardAssets(images) {
  if (!images.length) return 'No product-card/media-carousel assets detected.';
  return images.map((image, i) =>
    `${i + 1}. [${image.sourceType || 'product media'} | ${image.label || 'unlabeled'}] ${image.src}`
  ).join('\n');
}

function injectProductCardVisualsFallback(liquid, productCardAssets, sourceDesign, targetLanguage) {
  if (!productCardAssets.length) return liquid;
  const required = Math.min(3, productCardAssets.length);
  const used = productCardAssets.filter(image => liquid.includes(image.src)).length;
  if (used >= required) return liquid;

  const prefix = inferCssPrefix(liquid);
  const colors = {
    accent: sourceDesign.accent || '#e66f8f',
    dark: sourceDesign.dark || '#52263a',
    soft: sourceDesign.soft || '#fde8ee',
    cream: sourceDesign.cream || '#fff7f1'
  };
  const assetsToShow = productCardAssets.slice(0, Math.min(6, productCardAssets.length));
  return injectSectionAndScript(
    liquid,
    buildProductCardVisualsSection(prefix, colors, assetsToShow, targetLanguage),
    `  [AI] Injected product-card visual fallback (${used}/${required} primary asset(s) used by model)`
  );
}

function ensureProductCardVisualsCovered(liquid, productCardAssets) {
  if (!productCardAssets.length) return liquid;
  const required = Math.min(3, productCardAssets.length);
  const used = productCardAssets.filter(image => liquid.includes(image.src)).length;
  if (used < required) {
    console.log(`  [AI] Product-card visuals underused by model (${used}/${required}); leaving body unchanged to avoid duplicating the product gallery`);
  }
  return liquid;
}

function productCardFallbackCopy(targetLanguage) {
  const copy = {
    de: {
      title: 'Alles, was dieses Gerät kann',
      subtitle: 'Die wichtigsten Produktvisuals aus dem ursprünglichen Karussell, vollständig erhalten.'
    },
    nl: {
      title: 'Alles wat dit apparaat doet',
      subtitle: 'De belangrijkste visuals uit de oorspronkelijke productcarrousel, volledig behouden.'
    },
    fr: {
      title: 'Tout ce que cet appareil peut faire',
      subtitle: 'Les visuels clés du carrousel produit d’origine, conservés dans leur intégralité.'
    },
    es: {
      title: 'Todo lo que este dispositivo puede hacer',
      subtitle: 'Las imágenes clave del carrusel original del producto, conservadas completas.'
    },
    it: {
      title: 'Tutto quello che questo dispositivo può fare',
      subtitle: 'Le immagini chiave del carosello prodotto originale, mantenute complete.'
    }
  };
  return copy[targetLanguage] || {
    title: 'Everything this device is built to do',
    subtitle: 'The key visuals from the original product carousel, preserved as complete reference cards.'
  };
}

function buildProductCardVisualsSection(prefix, colors, images, targetLanguage) {
  const copy = productCardFallbackCopy(targetLanguage);
  const cards = images.map((image, i) => `
      <figure class="${prefix}-pcv-card">
        <img src="${escapeHtml(image.src)}" alt="${escapeHtml(image.label || `Product visual ${i + 1}`)}" loading="lazy">
      </figure>`).join('');

  const css = `

  .${prefix}-pcv-section {
    margin: clamp(36px, 7vw, 76px) auto;
    padding: clamp(24px, 5vw, 54px);
    border-radius: 32px;
    background: ${colors.cream};
  }
  .${prefix}-pcv-heading {
    max-width: 780px;
    margin: 0 auto 24px;
    text-align: center;
  }
  .${prefix}-pcv-heading h2 {
    margin: 0 0 10px;
    color: ${colors.dark};
    font-size: clamp(30px, 4.5vw, 52px);
    line-height: 1;
  }
  .${prefix}-pcv-heading p {
    margin: 0;
    color: ${colors.dark};
    opacity: .74;
    font-size: clamp(15px, 1.8vw, 19px);
  }
  .${prefix}-pcv-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(min(260px, 100%), 1fr));
    gap: clamp(14px, 2.2vw, 24px);
    max-width: 1120px;
    margin: 0 auto;
  }
  .${prefix}-pcv-card {
    margin: 0;
    border-radius: 24px;
    overflow: hidden;
    background: #fff;
    box-shadow: 0 16px 45px rgba(82, 38, 58, .12);
  }
  .${prefix}-pcv-card img {
    display: block;
    width: 100%;
    aspect-ratio: 1 / 1;
    object-fit: contain;
    background: #fff;
  }
  @media (max-width: 749px) {
    .${prefix}-pcv-section {
      padding: 22px 14px;
      border-radius: 24px;
    }
    .${prefix}-pcv-grid {
      grid-template-columns: 1fr;
    }
  }`;

  const section = `
  <section class="${prefix}-pcv-section" data-product-card-visuals>
    <div class="${prefix}-pcv-heading">
      <h2>${escapeHtml(copy.title)}</h2>
      <p>${escapeHtml(copy.subtitle)}</p>
    </div>
    <div class="${prefix}-pcv-grid">
${cards}
    </div>
  </section>`;

  return { css, section, script: '' };
}

function injectBeforeAfterSliderFallback(liquid, assets, sourceDesign, targetLanguage) {
  // Always strip any AI-built before/after section first — the model frequently
  // pairs unrelated photos as "before" and "after" because it doesn't know
  // which images correspond to the same person. We rebuild the section from
  // the labeled pairs the scraper found.
  let workingLiquid = stripAIBuiltBeforeAfter(liquid);

  const prefix = inferCssPrefix(workingLiquid);
  const colors = {
    accent: sourceDesign.accent || '#e66f8f',
    dark: sourceDesign.dark || '#52263a',
    soft: sourceDesign.soft || '#fde8ee',
    cream: sourceDesign.cream || '#fff7f1'
  };

  // Pair before[i] with after[i] when we have explicit pairs, plus add any
  // composite images at the end. Each pair / composite becomes one slide in a
  // single carousel of compare-sliders.
  const pairCount = Math.min(assets.before.length, assets.after.length);
  const pairSlides = [];
  for (let i = 0; i < pairCount && pairSlides.length < 8; i++) {
    pairSlides.push({
      kind: 'pair',
      before: assets.before[i],
      after: assets.after[i],
    });
  }
  for (const composite of assets.composite.slice(0, Math.max(0, 8 - pairSlides.length))) {
    pairSlides.push({ kind: 'composite', composite });
  }

  // Even with no detected pairs we should still drop the placeholder if the AI
  // emitted one — otherwise it ships as a visible HTML comment.
  const placeholderRe = /<!--\s*BEFORE_AFTER_SLIDER_PLACEHOLDER\s*-->/gi;

  if (pairSlides.length === 0) {
    return workingLiquid.replace(placeholderRe, '');
  }

  const parts = buildBeforeAfterPairCarousel(prefix, colors, pairSlides, targetLanguage);

  // If the AI honored the placeholder, swap it in place. Otherwise inject the
  // section before the closing wrapper / first script (existing helper logic).
  if (placeholderRe.test(workingLiquid)) {
    placeholderRe.lastIndex = 0;
    let out = workingLiquid.replace(placeholderRe, parts.section);
    if (out.includes('</style>')) {
      out = out.replace('</style>', `${parts.css}\n</style>`);
    } else {
      out = `${parts.css}\n${out}`;
    }
    if (out.includes('</script>')) {
      out = out.replace('</script>', `${parts.script}\n</script>`);
    } else {
      out += `\n<script>${parts.script}\n</script>`;
    }
    console.log(`  [AI] Replaced before/after placeholder with carousel (${pairSlides.length} slide(s)${pairCount ? `, ${pairCount} pair(s)` : ''})`);
    return out;
  }

  return injectSectionAndScript(
    workingLiquid,
    parts,
    `  [AI] Injected before/after carousel (${pairSlides.length} slide(s)${pairCount ? `, ${pairCount} pair(s)` : ''})`
  );
}

// Remove anything that looks like an AI-built before/after / real-results
// section. The AI can't reliably figure out which "before" matches which
// "after" image, so its output is often unrelated photos paired together
// (or a static side-by-side card layout with no actual interactivity, which
// is what we keep seeing on Solawave clones — three composite cards in a
// row with non-functional "<>" arrows). Either way, we rebuild from the
// labeled pairs the scraper detected.
function stripAIBuiltBeforeAfter(liquid) {
  // Match a <section ...>...</section> whose contents look like a before/after
  // proof section. We're conservative on vocabulary but generous on layout
  // markers — the AI keeps inventing new ways to render a static carousel,
  // so we flag anything that has BOTH before/after wording AND multiple
  // images OR carousel/grid markup.
  const sectionRe = /<section\b[^>]*>[\s\S]*?<\/section>/gi;
  let totalStripped = 0;
  const out = liquid.replace(sectionRe, (block) => {
    const lower = block.toLowerCase();
    const hasBeforeAfterVocab =
      /before\s*[\/&-]\s*after|before-after|before and after|real\s*results|real\s*skin|real\s*change|day\s*0|day\s*30|vorher|nachher|voor\s*[\/&-]\s*na|avant\s*[\/&-]\s*apr/i.test(lower);
    if (!hasBeforeAfterVocab) return block;

    const imgMatches = block.match(/<img\b/gi) || [];
    const hasMultipleImages = imgMatches.length >= 2;
    const looksLikeCarousel =
      /slider|carousel|range|compare|track|slide|results|ba-|grid|swiper|splide|results?-(?:slider|carousel)|testimonial/i.test(lower);
    // Also catch static side-by-side card layouts that don't use carousel
    // class names — typically a flex/grid with 3 figure / div children that
    // each contain an <img> + a "vorher"/"nachher" or "before"/"after" tag.
    const looksLikeStaticPairCards =
      hasMultipleImages &&
      (lower.match(/vorher/g) || []).length + (lower.match(/before/g) || []).length >= 2;

    if (!looksLikeCarousel && !looksLikeStaticPairCards && !hasMultipleImages) return block;

    totalStripped++;
    return ''; // drop it
  });
  if (totalStripped > 0) {
    console.log(`  [AI] Stripped ${totalStripped} AI-built before/after section(s) so we can rebuild from labeled pairs`);
  }
  return out;
}

function beforeAfterCopy(targetLanguage) {
  const copy = {
    de: { heading: 'Echte Ergebnisse', subhead: 'Schiebe den Regler, um Vorher und Nachher zu vergleichen.', carouselSubhead: 'Sieh dir die Ergebnisse in einer kompakten Galerie an.', before: 'Vorher', after: 'Nachher', prev: 'Vorheriges Ergebnis', next: 'Nächstes Ergebnis' },
    nl: { heading: 'Echte resultaten', subhead: 'Schuif de regelaar om voor en na te vergelijken.', carouselSubhead: 'Bekijk de resultaten in een compacte galerij.', before: 'Voor', after: 'Na', prev: 'Vorig resultaat', next: 'Volgend resultaat' },
    fr: { heading: 'De vrais résultats', subhead: 'Faites glisser le curseur pour comparer avant et après.', carouselSubhead: 'Parcourez les résultats dans une galerie compacte.', before: 'Avant', after: 'Après', prev: 'Résultat précédent', next: 'Résultat suivant' },
    es: { heading: 'Resultados reales', subhead: 'Desliza el control para comparar antes y después.', carouselSubhead: 'Explora los resultados en una galería compacta.', before: 'Antes', after: 'Después', prev: 'Resultado anterior', next: 'Siguiente resultado' },
    it: { heading: 'Risultati reali', subhead: 'Trascina il cursore per confrontare prima e dopo.', carouselSubhead: 'Guarda i risultati in una galleria compatta.', before: 'Prima', after: 'Dopo', prev: 'Risultato precedente', next: 'Risultato successivo' }
  };
  return copy[targetLanguage] || { heading: 'Real results you can see', subhead: 'Drag the handle to compare before and after.', carouselSubhead: 'Browse the results in a compact gallery.', before: 'Before', after: 'After', prev: 'Previous result', next: 'Next result' };
}

// Build a carousel of compare-sliders. Each slide is either a true pair
// (separate before + after photos stacked, with clip-path reveal) or a
// composite (one [BEFORE | AFTER] image split via background-position).
// The carousel itself uses prev/next + dots, exactly like buildBeforeAfterCarousel.
function buildBeforeAfterPairCarousel(prefix, colors, slides, targetLanguage) {
  const copy = beforeAfterCopy(targetLanguage);
  const hasTruePair = slides.some(slot => slot.kind === 'pair');
  const subhead = hasTruePair ? copy.subhead : copy.carouselSubhead;

  const slideHtml = slides.map((slot, i) => {
    if (slot.kind === 'pair') {
      const beforeSrc = escapeHtml(slot.before.src);
      const afterSrc = escapeHtml(slot.after.src);
      const beforeAlt = escapeHtml(slot.before.label || copy.before);
      const afterAlt = escapeHtml(slot.after.label || copy.after);
      return `
        <div class="${prefix}-ba-slide" role="group" aria-label="Result ${i + 1} of ${slides.length}">
          <div class="${prefix}-ba-pair" data-before-after-pair style="--position: 50%;">
            <img class="${prefix}-ba-after-img" src="${afterSrc}" alt="${afterAlt}" loading="lazy">
            <img class="${prefix}-ba-before-img" src="${beforeSrc}" alt="${beforeAlt}" loading="lazy">
            <span class="${prefix}-ba-tag ${prefix}-ba-tag-before">${escapeHtml(copy.before)}</span>
            <span class="${prefix}-ba-tag ${prefix}-ba-tag-after">${escapeHtml(copy.after)}</span>
            <span class="${prefix}-ba-divider" aria-hidden="true"></span>
            <span class="${prefix}-ba-handle" aria-hidden="true">↔</span>
            <input class="${prefix}-ba-range" type="range" min="0" max="100" value="50" aria-label="${escapeHtml(copy.subhead)}">
          </div>
        </div>`;
    }
    // Composite before/after cards already contain the visual comparison.
    // Keep them whole; forcing a fake split-slider stretches/crops the card.
    const src = escapeHtml(slot.composite.src);
    const alt = escapeHtml(slot.composite.label || `Before and after result ${i + 1}`);
    return `
        <div class="${prefix}-ba-slide" role="group" aria-label="Result ${i + 1} of ${slides.length}">
          <figure class="${prefix}-ba-composite-card">
            <img src="${src}" alt="${alt}" loading="lazy">
          </figure>
        </div>`;
  }).join('');

  const dots = slides.map((_, i) => `
        <button class="${prefix}-ba-dot" type="button" aria-label="Show result ${i + 1}" aria-current="${i === 0 ? 'true' : 'false'}"></button>`).join('');
  const controls = slides.length > 1 ? `
      <div class="${prefix}-ba-controls" aria-label="${escapeHtml(copy.heading)}">
        <button class="${prefix}-ba-button ${prefix}-ba-prev" type="button" aria-label="${escapeHtml(copy.prev)}">‹</button>
        <div class="${prefix}-ba-dots">
${dots}
        </div>
        <button class="${prefix}-ba-button ${prefix}-ba-next" type="button" aria-label="${escapeHtml(copy.next)}">›</button>
      </div>` : '';

  const css = `

  .${prefix}-ba-section {
    margin: clamp(36px, 7vw, 76px) auto;
    padding: clamp(24px, 5vw, 56px);
    border-radius: 32px;
    background: linear-gradient(135deg, ${colors.cream}, ${colors.soft});
  }
  .${prefix}-ba-heading {
    max-width: 780px;
    margin: 0 auto 24px;
    text-align: center;
  }
  .${prefix}-ba-heading h2 {
    margin: 0 0 10px;
    color: ${colors.dark};
    font-size: clamp(32px, 5vw, 58px);
    line-height: 0.98;
  }
  .${prefix}-ba-heading p {
    margin: 0;
    color: ${colors.dark};
    opacity: .76;
    font-size: clamp(16px, 2vw, 20px);
  }
  .${prefix}-ba-carousel {
    max-width: 980px;
    margin: 0 auto;
  }
  .${prefix}-ba-viewport {
    overflow: hidden;
    border-radius: 28px;
    background: #fff;
    box-shadow: 0 24px 70px rgba(82, 38, 58, .16);
  }
  .${prefix}-ba-track {
    display: flex;
    transition: transform .36s ease;
    will-change: transform;
  }
  .${prefix}-ba-slide {
    flex: 0 0 100%;
    padding: clamp(10px, 2vw, 18px);
    background: #fff;
  }
  /* True before/after pair: two stacked images, "before" clipped by --position. */
  .${prefix}-ba-pair {
    position: relative;
    width: 100%;
    aspect-ratio: 4 / 3;
    overflow: hidden;
    border-radius: 22px;
    background: #f6eee9;
    user-select: none;
    touch-action: none;
  }
  .${prefix}-ba-pair img {
    display: block;
    width: 100%;
    height: 100%;
    object-fit: cover;
    background: #fff;
  }
  .${prefix}-ba-pair .${prefix}-ba-after-img {
    position: absolute;
    inset: 0;
  }
  .${prefix}-ba-pair .${prefix}-ba-before-img {
    position: absolute;
    inset: 0;
    clip-path: inset(0 calc(100% - var(--position, 50%)) 0 0);
  }
  /* Composite: same image, two halves shown via background-position. */
  .${prefix}-ba-compare {
    position: relative;
    width: 100%;
    aspect-ratio: 4 / 3;
    overflow: hidden;
    border-radius: 22px;
    background: #f6eee9;
    user-select: none;
    touch-action: none;
  }
  .${prefix}-ba-after-layer,
  .${prefix}-ba-before-layer {
    position: absolute;
    top: 0;
    bottom: 0;
    background-size: 200% 100%;
    background-repeat: no-repeat;
    background-color: #fff;
  }
  .${prefix}-ba-after-layer {
    left: 0;
    right: 0;
    background-position: 100% 50%;
  }
  .${prefix}-ba-before-layer {
    left: 0;
    width: var(--position, 50%);
    background-position: 0% 50%;
  }
  .${prefix}-ba-composite-card {
    margin: 0 auto;
    max-width: 760px;
    border-radius: 22px;
    overflow: hidden;
    background: #fff;
  }
  .${prefix}-ba-composite-card img {
    display: block;
    width: 100%;
    height: auto;
    max-height: min(70vh, 640px);
    object-fit: contain;
    background: #fff;
  }
  /* Shared overlays: tags, divider, handle, slider input. */
  .${prefix}-ba-tag {
    position: absolute;
    top: 14px;
    z-index: 3;
    padding: 6px 12px;
    border-radius: 999px;
    background: rgba(255, 255, 255, .92);
    color: ${colors.dark};
    font-weight: 800;
    letter-spacing: .04em;
    text-transform: uppercase;
    font-size: 11px;
    pointer-events: none;
  }
  .${prefix}-ba-tag-before { left: 14px; }
  .${prefix}-ba-tag-after  { right: 14px; }
  .${prefix}-ba-divider {
    position: absolute;
    top: 0;
    bottom: 0;
    left: var(--position, 50%);
    z-index: 4;
    width: 3px;
    transform: translateX(-50%);
    background: #fff;
    box-shadow: 0 0 0 1px rgba(82, 38, 58, .14);
    pointer-events: none;
  }
  .${prefix}-ba-handle {
    position: absolute;
    top: 50%;
    left: var(--position, 50%);
    z-index: 5;
    display: grid;
    place-items: center;
    width: 52px;
    height: 52px;
    border-radius: 999px;
    transform: translate(-50%, -50%);
    background: #fff;
    color: ${colors.accent};
    box-shadow: 0 12px 32px rgba(82, 38, 58, .25);
    font-weight: 800;
    pointer-events: none;
    font-size: 18px;
  }
  .${prefix}-ba-range {
    position: absolute;
    inset: 0;
    z-index: 6;
    width: 100%;
    height: 100%;
    margin: 0;
    appearance: none;
    background: transparent;
    opacity: 0;
    cursor: ew-resize;
  }
  .${prefix}-ba-range::-webkit-slider-thumb {
    appearance: none;
    width: 60px;
    height: 100%;
  }
  .${prefix}-ba-controls {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 16px;
    margin-top: 18px;
  }
  .${prefix}-ba-button {
    display: grid;
    place-items: center;
    width: 46px;
    height: 46px;
    border: 0;
    border-radius: 999px;
    background: #fff;
    color: ${colors.dark};
    box-shadow: 0 10px 28px rgba(82, 38, 58, .16);
    cursor: pointer;
    font-size: 22px;
    line-height: 1;
  }
  .${prefix}-ba-dots {
    display: flex;
    gap: 8px;
  }
  .${prefix}-ba-dot {
    width: 9px;
    height: 9px;
    padding: 0;
    border: 0;
    border-radius: 999px;
    background: rgba(82, 38, 58, .25);
    cursor: pointer;
  }
  .${prefix}-ba-dot[aria-current="true"] {
    width: 26px;
    background: ${colors.accent};
  }
  @media (max-width: 749px) {
    .${prefix}-ba-section {
      padding: 22px 14px;
      border-radius: 24px;
    }
    .${prefix}-ba-slide {
      padding: 8px;
    }
    .${prefix}-ba-pair,
    .${prefix}-ba-compare {
      aspect-ratio: 4 / 3;
      border-radius: 18px;
    }
    .${prefix}-ba-composite-card {
      border-radius: 18px;
    }
    .${prefix}-ba-handle {
      width: 44px;
      height: 44px;
    }
  }`;

  const section = `
  <section class="${prefix}-ba-section">
    <div class="${prefix}-ba-heading">
      <h2>${escapeHtml(copy.heading)}</h2>
      <p>${escapeHtml(subhead)}</p>
    </div>
    <div class="${prefix}-ba-carousel" data-before-after-carousel>
      <div class="${prefix}-ba-viewport">
        <div class="${prefix}-ba-track">
${slideHtml}
        </div>
      </div>
${controls}
    </div>
  </section>`;

  const script = `
  document.querySelectorAll('.${prefix}-ba-pair, .${prefix}-ba-compare').forEach(function(el) {
    var range = el.querySelector('.${prefix}-ba-range');
    if (!range) return;
    var update = function() {
      el.style.setProperty('--position', range.value + '%');
    };
    range.addEventListener('input', update);
    range.addEventListener('change', update);
    update();
  });
  document.querySelectorAll('.${prefix}-ba-carousel').forEach(function(carousel) {
    var track = carousel.querySelector('.${prefix}-ba-track');
    var slides = Array.prototype.slice.call(carousel.querySelectorAll('.${prefix}-ba-slide'));
    var dots = Array.prototype.slice.call(carousel.querySelectorAll('.${prefix}-ba-dot'));
    var prev = carousel.querySelector('.${prefix}-ba-prev');
    var next = carousel.querySelector('.${prefix}-ba-next');
    var index = 0;
    if (!track || slides.length < 2) return;
    var show = function(nextIndex) {
      index = (nextIndex + slides.length) % slides.length;
      track.style.transform = 'translateX(' + (-index * 100) + '%)';
      dots.forEach(function(dot, dotIndex) {
        dot.setAttribute('aria-current', dotIndex === index ? 'true' : 'false');
      });
    };
    if (prev) prev.addEventListener('click', function() { show(index - 1); });
    if (next) next.addEventListener('click', function() { show(index + 1); });
    dots.forEach(function(dot, dotIndex) {
      dot.addEventListener('click', function() { show(dotIndex); });
    });
    show(0);
  });`;

  return { css, section, script };
}

// Legacy helper kept for back-compat. Delegates to the unified pair carousel
// so all callers produce the same interactive widget.
function buildBeforeAfterCarousel(prefix, colors, images, targetLanguage) {
  return buildBeforeAfterPairCarousel(
    prefix,
    colors,
    images.map(image => ({ kind: 'composite', composite: image })),
    targetLanguage
  );
}

function buildBeforeAfterCompareSlider(prefix, colors, before, after, targetLanguage) {
  const copy = beforeAfterCopy(targetLanguage);
  const css = `

  .${prefix}-ba-section {
    margin: clamp(36px, 7vw, 76px) auto;
    padding: clamp(24px, 5vw, 56px);
    border-radius: 32px;
    background: linear-gradient(135deg, ${colors.cream}, ${colors.soft});
  }
  .${prefix}-ba-heading {
    max-width: 780px;
    margin: 0 auto 24px;
    text-align: center;
  }
  .${prefix}-ba-heading h2 {
    margin: 0 0 10px;
    color: ${colors.dark};
    font-size: clamp(32px, 5vw, 58px);
    line-height: 0.98;
  }
  .${prefix}-ba-heading p {
    margin: 0;
    color: ${colors.dark};
    opacity: .76;
    font-size: clamp(16px, 2vw, 20px);
  }
  .${prefix}-ba-slider {
    --position: 50%;
    position: relative;
    max-width: 920px;
    margin: 0 auto;
    overflow: hidden;
    border-radius: 28px;
    background: #fff;
    box-shadow: 0 24px 70px rgba(82, 38, 58, .16);
  }
  .${prefix}-ba-slider img {
    display: block;
    width: 100%;
    aspect-ratio: 16 / 10;
    object-fit: cover;
    background: #fff;
  }
  .${prefix}-ba-before-img {
    position: absolute;
    inset: 0;
    clip-path: inset(0 calc(100% - var(--position)) 0 0);
  }
  .${prefix}-ba-range {
    position: absolute;
    inset: 0;
    z-index: 5;
    width: 100%;
    height: 100%;
    opacity: 0;
    cursor: ew-resize;
  }
  .${prefix}-ba-divider {
    position: absolute;
    top: 0;
    bottom: 0;
    left: var(--position);
    z-index: 4;
    width: 3px;
    transform: translateX(-50%);
    background: #fff;
    box-shadow: 0 0 0 1px rgba(82, 38, 58, .12);
    pointer-events: none;
  }
  .${prefix}-ba-handle {
    position: absolute;
    top: 50%;
    left: var(--position);
    z-index: 4;
    display: grid;
    place-items: center;
    width: 58px;
    height: 58px;
    border-radius: 999px;
    transform: translate(-50%, -50%);
    background: #fff;
    color: ${colors.accent};
    box-shadow: 0 12px 32px rgba(82, 38, 58, .25);
    font-weight: 800;
    pointer-events: none;
  }
  .${prefix}-ba-label {
    position: absolute;
    top: 18px;
    z-index: 3;
    padding: 8px 14px;
    border-radius: 999px;
    background: rgba(255, 255, 255, .9);
    color: ${colors.dark};
    font-weight: 800;
    letter-spacing: .02em;
    text-transform: uppercase;
    font-size: 12px;
  }
  .${prefix}-ba-label-before { left: 18px; }
  .${prefix}-ba-label-after { right: 18px; }
  @media (max-width: 749px) {
    .${prefix}-ba-section {
      padding: 22px 14px;
      border-radius: 24px;
    }
    .${prefix}-ba-slider img {
      aspect-ratio: 4 / 5;
    }
    .${prefix}-ba-handle {
      width: 48px;
      height: 48px;
    }
  }`;

  const section = `
  <section class="${prefix}-ba-section">
    <div class="${prefix}-ba-heading">
      <h2>${escapeHtml(copy.heading)}</h2>
      <p>${escapeHtml(copy.subhead)}</p>
    </div>
    <div class="${prefix}-ba-slider" data-before-after-slider>
      <img src="${escapeHtml(after.src)}" alt="${escapeHtml(after.label || copy.after)}" loading="lazy">
      <img class="${prefix}-ba-before-img" src="${escapeHtml(before.src)}" alt="${escapeHtml(before.label || copy.before)}" loading="lazy">
      <span class="${prefix}-ba-label ${prefix}-ba-label-before">${escapeHtml(copy.before)}</span>
      <span class="${prefix}-ba-label ${prefix}-ba-label-after">${escapeHtml(copy.after)}</span>
      <span class="${prefix}-ba-divider"></span>
      <span class="${prefix}-ba-handle" aria-hidden="true">↔</span>
      <input class="${prefix}-ba-range" type="range" min="0" max="100" value="50" aria-label="${escapeHtml(copy.subhead)}">
    </div>
  </section>`;

  const script = `
  document.querySelectorAll('.${prefix}-ba-slider').forEach(function(slider) {
    var input = slider.querySelector('.${prefix}-ba-range');
    if (!input) return;
    var update = function() {
      slider.style.setProperty('--position', input.value + '%');
    };
    input.addEventListener('input', update);
    update();
  });`;

  return { css, section, script };
}

function injectSectionAndScript(liquid, parts, logMessage) {
  let out = liquid.includes('</style>')
    ? liquid.replace('</style>', `${parts.css}\n</style>`)
    : `${parts.css}\n${liquid}`;

  const scriptIndex = out.search(/<script\b/i);
  if (scriptIndex >= 0) {
    const beforeScript = out.slice(0, scriptIndex);
    const afterScript = out.slice(scriptIndex);
    const lastDiv = beforeScript.lastIndexOf('</div>');
    if (lastDiv >= 0) {
      out = `${beforeScript.slice(0, lastDiv)}${parts.section}\n${beforeScript.slice(lastDiv)}${afterScript}`;
    } else {
      out = `${beforeScript}${parts.section}\n${afterScript}`;
    }
  } else {
    out += parts.section;
  }

  if (out.includes('</script>')) {
    out = out.replace('</script>', `${parts.script}\n</script>`);
  } else {
    out += `\n<script>${parts.script}\n</script>`;
  }

  console.log(logMessage);
  return out;
}

function inferCssPrefix(liquid) {
  const wrapMatch = liquid.match(/class=["']([a-z0-9]{2,5})-wrap["']/i);
  if (wrapMatch) return wrapMatch[1];
  const classMatch = liquid.match(/class=["']([a-z0-9]{2,5})-[a-z0-9-]+["']/i);
  return classMatch ? classMatch[1] : 'pdp';
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// Cap a list of paragraphs at a total character budget, sentence-aware. Keeps
// whole paragraphs whenever they fit; for the paragraph that overflows the
// budget, slices at the last sentence boundary in the prefix that still fits.
// We do this so the AI sees full source text (better translations) without
// blowing up the prompt size on long PDPs.
function capParagraphsToTotal(paragraphs, totalLimit) {
  const out = [];
  let used = 0;
  for (const p of paragraphs) {
    if (used >= totalLimit) break;
    const remaining = totalLimit - used;
    if (p.length <= remaining) {
      out.push(p);
      used += p.length;
      continue;
    }
    const slice = p.substring(0, remaining);
    const lastSentence = Math.max(slice.lastIndexOf('. '), slice.lastIndexOf('! '), slice.lastIndexOf('? '));
    if (lastSentence > remaining * 0.5) {
      out.push(slice.substring(0, lastSentence + 1));
    } else {
      // No good sentence break — just take what fits.
      out.push(slice);
    }
    break;
  }
  return out;
}

// Pull integer / decimal / percentage tokens from a section's text so the AI
// can see exactly which numeric claims are present. NUMBERS POLICY in the
// system prompt then tells the model which to keep, drop, or soften.
function extractNumbersFromSection(s) {
  const headingsText = (s.headings || [])
    .map(h => (typeof h === 'string' ? h : h.text || ''))
    .filter(Boolean);
  const text = [...headingsText, ...(s.paragraphs || []), ...(s.bullets || [])].join(' ');
  const matches = text.match(
    /\b\d{1,3}(?:[.,]\d{1,2})?\s*%|\b\d{1,3}(?:[,.]\d{3})+(?:[.,]\d{1,2})?\b|\b\d{1,4}(?:[.,]\d{1,2})?\b/g
  ) || [];
  return [...new Set(matches)].slice(0, 30);
}

function inferSourceDesign(productMeta, sections, labeledImages, palette = null) {
  const haystack = [
    productMeta.title,
    productMeta.description,
    ...(productMeta.images || []).map(img => img.alt || ''),
    ...labeledImages.map(x => x.label || ''),
    ...sections.flatMap(s => [
      ...(s.headings || []).map(h => h.text || ''),
      ...(s.paragraphs || [])
    ])
  ].join(' ').toLowerCase();

  const isSolawaveLike = /solawave|red light|skincare wand|rose gold|radiant renewal|light therapy|galvanic|before[-\s]?after|day\s*30|real results/.test(haystack);

  // If a palette was extracted from the screenshot, prefer those exact hex
  // values. We keep the genre-specific defaults (e66f8f etc.) only as a
  // fallback when extraction failed or the extractor returned something
  // obviously wrong (e.g. all greys, signaling a bad screenshot).
  const extracted = palette && isUsablePalette(palette) ? palette : null;

  if (extracted) {
    const accent = extracted.accent;
    const accentDark = extracted.accentDark;
    const surface = extracted.surface;
    const background = extracted.background;
    const textPrimary = extracted.textPrimary;
    return {
      kind: isSolawaveLike ? 'beauty-red-light' : 'source-extracted',
      accent,
      dark: accentDark,
      soft: background,
      cream: surface,
      textPrimary,
      palette: extracted,
      instructions: [
        'Source page palette extracted from screenshot — use these exact hex values:',
        `- accent (buttons, stat rings, CTA, badges, links): ${accent}`,
        `- accent dark (hover/active, dark text-on-light): ${accentDark}`,
        `- background (page sections, soft fill behind cards): ${background}`,
        `- surface (cards, hero panel): ${surface}`,
        `- text primary (headings, body): ${textPrimary}`,
        'Do NOT substitute Movanella defaults. Do NOT use green (#07941a/#16a34a/#22c55e) or Movanella navy unless the extracted palette literally contains those values.',
        'For shadows and dividers, use a low-opacity tone of textPrimary.',
        'Composite source images (before/after, comparison charts, dermatologist cards, guarantee graphics) must be used as complete images, not split into generated cards.'
      ].join('\n')
    };
  }

  // No palette extracted — use the existing genre heuristic.
  if (isSolawaveLike) {
    return {
      kind: 'beauty-red-light',
      accent: '#e66f8f',
      dark: '#52263a',
      soft: '#fde8ee',
      cream: '#fff7f1',
      instructions: [
        'Detected a beauty/red-light skincare source page.',
        'Use a Solawave-like visual direction: blush pink, rose, soft cream, warm white cards, muted coral accents, and deep berry/plum text.',
        'Do not use Movanella green or navy/blue as the primary page palette.',
        'Use rounded cream/pink cards and full-width proof sections similar to a skincare PDP.',
        'The before/after, comparison chart, dermatologist/expert quote, usage infographic, and guarantee images should be shown as complete images, not broken into separate generated cards.'
      ].join('\n')
    };
  }

  return {
    kind: 'source-generic',
    instructions: [
      'Derive the palette, spacing, card shapes, and section order from the screenshot.',
      'Do not force the destination store palette if it conflicts with the source page.'
    ].join('\n')
  };
}

// Reject palettes where every channel is essentially equal across all 5
// slots (i.e. all greys) — that means the screenshot didn't decode or the
// page rendered on a blank canvas. Fall back to the heuristic in that case.
function isUsablePalette(p) {
  if (!p?.accent) return false;
  const hexes = [p.accent, p.accentDark, p.background, p.surface, p.textPrimary];
  const allGrey = hexes.every(h => {
    const m = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(h);
    if (!m) return false;
    const r = parseInt(m[1], 16), g = parseInt(m[2], 16), b = parseInt(m[3], 16);
    const span = Math.max(r, g, b) - Math.min(r, g, b);
    return span < 12;
  });
  return !allGrey;
}

// Recolor Movanella's existing Horizon product-information section so its
// green ATC button and green ✓ ticks pick up the source page's accent color.
//
// Strategy reversal vs. the prior "hide-the-hero" approach: keeping
// Movanella's hero (gallery + bundle picker + ATC + bullets) avoids the
// duplicate-product-card problem we kept hitting. The cloned section is
// pure body content — benefits, science, comparisons, before/after,
// testimonials, FAQ — so the page reads as ONE Movanella PDP whose
// content body has been swapped for the source's content.
//
// This function injects a programmatic safety-net CSS block targeting
// Movanella's specific selectors (.add-to-cart-button, .pd-rating, the
// inline-styled green checkmarks the bullet renderer emits). The AI's
// generated CSS may already include similar rules; this one guarantees
// the recolor even if the AI's pass missed it. The CSS is scoped to the
// Horizon stock product section via [id^="shopify-section-template"][id$="__main"]
// so it cannot leak to non-cloned Movanella products (which do not load
// this template's custom_liquid section anyway).
function injectHorizonAtcOverride(liquid, sourceDesign) {
  const accent = (sourceDesign && sourceDesign.accent) || '#e66f8f';
  const accentDark = (sourceDesign && (sourceDesign.dark || sourceDesign.accentDark)) || '#52263a';

  const overrideCss = `
/* Recolor Movanella's stock product-info section to the source palette
   so the visible hero (Movanella's gallery + ATC + bullets) doesn't
   render in Movanella green on a pink/blush source clone. This rule is
   scoped to the cloned product's main section ID, plus the global
   .add-to-cart-button class that Movanella's bundle picker also reuses. */
.add-to-cart-button { background: ${accent} !important; border-color: ${accent} !important; }
.add-to-cart-button:hover, .add-to-cart-button:focus { background: ${accentDark} !important; }
[id^="shopify-section-template"][id$="__main"] .pd-rating { color: ${accent} !important; }
[id^="shopify-section-template"][id$="__main"] .pd-rating svg,
[id^="shopify-section-template"][id$="__main"] .pd-rating path { fill: ${accent} !important; }
[id^="shopify-section-template"][id$="__main"] span[style*="rgb(7, 148, 26)"],
[id^="shopify-section-template"][id$="__main"] span[style*="#07941a"],
[id^="shopify-section-template"][id$="__main"] [style*="color: rgb(7"] { color: ${accent} !important; }
[id^="shopify-section-template"][id$="__main"] [class*="check"] svg path,
[id^="shopify-section-template"][id$="__main"] [class*="tick"] svg path { fill: ${accent} !important; }
[id^="shopify-section-template"][id$="__main"] [class*="bundle"],
[id^="shopify-section-template"][id$="__main"] [class*="Bundle"],
[id^="shopify-section-template"][id$="__main"] [class*="offer"],
[id^="shopify-section-template"][id$="__main"] [class*="Offer"],
[id^="shopify-section-template"][id$="__main"] [class*="rapi"],
[id^="shopify-section-template"][id$="__main"] [class*="Rapi"] {
  --accent-color: ${accent} !important;
  --primary-color: ${accent} !important;
  --selected-color: ${accent} !important;
  --selected-border-color: ${accent} !important;
  --bundle-primary-color: ${accent} !important;
}
[id^="shopify-section-template"][id$="__main"] [style*="#7042c9"],
[id^="shopify-section-template"][id$="__main"] [style*="#7c3aed"],
[id^="shopify-section-template"][id$="__main"] [style*="#6d28d9"],
[id^="shopify-section-template"][id$="__main"] [style*="#5433ff"],
[id^="shopify-section-template"][id$="__main"] [style*="rgb(112, 66, 201)"],
[id^="shopify-section-template"][id$="__main"] [style*="rgb(124, 58, 237)"],
[id^="shopify-section-template"][id$="__main"] [style*="rgb(84, 51, 255)"] {
  border-color: ${accent} !important;
  color: ${accent} !important;
}
[id^="shopify-section-template"][id$="__main"] [style*="background: #7042c9"],
[id^="shopify-section-template"][id$="__main"] [style*="background:#7042c9"],
[id^="shopify-section-template"][id$="__main"] [style*="background-color: #7042c9"],
[id^="shopify-section-template"][id$="__main"] [style*="background-color:#7042c9"],
[id^="shopify-section-template"][id$="__main"] [style*="background: rgb(112, 66, 201)"],
[id^="shopify-section-template"][id$="__main"] [style*="background-color: rgb(112, 66, 201)"] {
  background: ${accent} !important;
  border-color: ${accent} !important;
  color: #fff !important;
}
`;

  const runtimeJs = `
(function() {
  var accent = ${JSON.stringify(accent)};
  var dark = ${JSON.stringify(accentDark)};
  var purpleNeedles = [
    '111, 66, 193', '112, 66, 201', '124, 58, 237',
    '#6f42c1', '#7042c9', '#7c3aed', '#6d28d9', '#5433ff'
  ];
  function isPurple(value) {
    if (!value) return false;
    var lower = String(value).toLowerCase();
    return purpleNeedles.some(function(needle) { return lower.indexOf(needle) !== -1; });
  }
  function recolorNode(el) {
    if (!el || !el.style) return;
    var cs = window.getComputedStyle(el);
    if (isPurple(cs.borderTopColor) || isPurple(cs.borderRightColor) || isPurple(cs.borderBottomColor) || isPurple(cs.borderLeftColor)) {
      el.style.setProperty('border-color', accent, 'important');
    }
    if (isPurple(cs.color)) {
      el.style.setProperty('color', accent, 'important');
    }
    if (isPurple(cs.backgroundColor)) {
      el.style.setProperty('background-color', accent, 'important');
      el.style.setProperty('border-color', accent, 'important');
      el.style.setProperty('color', '#fff', 'important');
    }
    ['--accent-color', '--primary-color', '--selected-color', '--selected-border-color', '--bundle-primary-color', '--rapi-primary-color'].forEach(function(prop) {
      el.style.setProperty(prop, accent, 'important');
    });
  }
  function recolorBundle() {
    var root = document.querySelector('[id^="shopify-section-template"][id$="__main"]');
    if (!root) return;
    var candidates = root.querySelectorAll('[class*="bundle"], [class*="Bundle"], [class*="offer"], [class*="Offer"], [class*="rapi"], [class*="Rapi"], [style*="#6F42C1"], [style*="#7042c9"], [style*="rgb(112, 66, 201)"]');
    candidates.forEach(function(el) {
      recolorNode(el);
      el.querySelectorAll('*').forEach(recolorNode);
    });
    root.querySelectorAll('input[type="radio"]:checked').forEach(function(input) {
      var host = input.closest('label, div, li');
      if (host) host.style.setProperty('border-color', accent, 'important');
    });
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', recolorBundle);
  } else {
    recolorBundle();
  }
  [250, 750, 1500, 3000].forEach(function(ms) { window.setTimeout(recolorBundle, ms); });
  var main = document.querySelector('[id^="shopify-section-template"][id$="__main"]');
  if (main && window.MutationObserver) {
    new MutationObserver(recolorBundle).observe(main, { childList: true, subtree: true, attributes: true, attributeFilter: ['style', 'class'] });
  }
})();`;

  let out = liquid.includes('<style>')
    ? liquid.replace('<style>', `<style>${overrideCss}`)
    : `<style>${overrideCss}</style>\n${liquid}`;

  if (out.includes('</script>')) {
    return out.replace('</script>', `${runtimeJs}\n</script>`);
  }
  return `${out}\n<script>${runtimeJs}\n</script>`;
}

function applySourcePaletteGuard(liquid, sourceDesign) {
  // Apply to every clone where we have a non-default accent — not just the
  // beauty-red-light heuristic. The AI keeps falling back to the Movanella
  // defaults (green / navy) even when an extracted palette is in the prompt,
  // so this is the last-mile safety net.
  if (!sourceDesign?.accent) return liquid;
  if (sourceDesign.kind === 'source-generic') return liquid; // no extracted palette to enforce
  const replacements = [
    [/#07941a/gi, sourceDesign.accent],
    [/#16a34a/gi, sourceDesign.accent],
    [/#22c55e/gi, sourceDesign.accent],
    [/#15803d/gi, sourceDesign.accent],
    [/#7042c9/gi, sourceDesign.accent],
    [/#7c3aed/gi, sourceDesign.accent],
    [/#6d28d9/gi, sourceDesign.accent],
    [/#5433ff/gi, sourceDesign.accent],
    [/#5b35f5/gi, sourceDesign.accent],
    [/#1b2d5b/gi, sourceDesign.dark],
    [/#0f172a/gi, sourceDesign.dark],
    [/#1e293b/gi, sourceDesign.dark],
    [/#f8f9ff/gi, sourceDesign.soft],
    [/#e8f5e9/gi, sourceDesign.soft]
  ];
  let out = liquid;
  let changed = 0;
  for (const [from, to] of replacements) {
    out = out.replace(from, () => {
      changed++;
      return to;
    });
  }
  if (changed > 0) {
    console.log(`  [AI] Applied source palette guard (${changed} color replacement(s) → ${sourceDesign.accent}/${sourceDesign.dark})`);
  }
  return out;
}

function injectCloneLayoutGuard(liquid, sourceDesign) {
  const prefix = inferCssPrefix(liquid);
  const accent = (sourceDesign && sourceDesign.accent) || '#e66f8f';
  const dark = (sourceDesign && (sourceDesign.dark || sourceDesign.accentDark)) || '#52263a';
  const soft = (sourceDesign && sourceDesign.soft) || '#fde8ee';
  const cream = (sourceDesign && sourceDesign.cream) || '#fff7f1';
  const imageBg = '#fff';
  const darkSection = readableDarkSectionColor(dark);

  const guardCss = `
/* Cloner layout guard.
   Keeps AI-generated body sections from becoming oversized poster blocks,
   oversized image frames, or off-palette green/purple leftovers. Scoped to
   this clone wrapper only, so theme chrome and global footer stay untouched. */
.${prefix}-wrap {
  --clone-accent: ${accent};
  --clone-dark: ${dark};
  --clone-soft: ${soft};
  --clone-cream: ${cream};
  --clone-dark-section: ${darkSection};
}
.${prefix}-wrap section {
  min-height: auto !important;
  padding-top: clamp(28px, 5vw, 76px) !important;
  padding-bottom: clamp(28px, 5vw, 76px) !important;
}
.${prefix}-wrap img {
  max-width: 100%;
  height: auto;
}
.${prefix}-wrap [class*="image-card"],
.${prefix}-wrap [class*="img-card"],
.${prefix}-wrap [class*="media-card"],
.${prefix}-wrap [class*="visual-card"],
.${prefix}-wrap [class*="photo-card"],
.${prefix}-wrap [class*="figure-card"] {
  min-height: 0 !important;
  padding: clamp(8px, 1.5vw, 22px) !important;
  background: ${imageBg} !important;
  display: flex;
  align-items: center;
  justify-content: center;
}
.${prefix}-wrap [class*="image-card"] > img,
.${prefix}-wrap [class*="img-card"] > img,
.${prefix}-wrap [class*="media-card"] > img,
.${prefix}-wrap [class*="visual-card"] > img,
.${prefix}-wrap [class*="photo-card"] > img,
.${prefix}-wrap [class*="figure-card"] > img,
.${prefix}-wrap figure > img {
  width: 100% !important;
  height: auto !important;
  max-height: min(760px, 72vh);
  aspect-ratio: auto !important;
  object-fit: contain !important;
  object-position: center !important;
  padding: 0 !important;
  background: transparent !important;
}
.${prefix}-wrap [class*="before"] img,
.${prefix}-wrap [class*="after"] img,
.${prefix}-wrap [class*="result"] img,
.${prefix}-wrap [class*="ba-"] img {
  object-fit: contain !important;
}
.${prefix}-wrap [class*="-ba-section"] {
  margin: clamp(28px, 5vw, 64px) auto !important;
  padding: clamp(20px, 4vw, 48px) !important;
}
.${prefix}-wrap [class*="-ba-carousel"],
.${prefix}-wrap [class*="-ba-slider"],
.${prefix}-wrap [class*="-ba-viewport"] {
  max-width: min(1040px, 92vw) !important;
  margin-left: auto !important;
  margin-right: auto !important;
}
.${prefix}-wrap [class*="-ba-composite-card"] {
  max-width: min(900px, 92vw) !important;
  background: #fff !important;
}
.${prefix}-wrap [class*="-ba-composite-card"] img {
  width: 100% !important;
  height: auto !important;
  max-height: min(760px, 72vh);
  aspect-ratio: auto !important;
  object-fit: contain !important;
}
.${prefix}-wrap [style*="#07941a"],
.${prefix}-wrap [style*="#16a34a"],
.${prefix}-wrap [style*="#22c55e"],
.${prefix}-wrap [style*="rgb(7, 148, 26)"],
.${prefix}-wrap [style*="rgb(22, 163, 74)"],
.${prefix}-wrap [style*="rgb(34, 197, 94)"] {
  background-color: ${soft} !important;
  border-color: ${accent} !important;
  color: ${dark} !important;
}
.${prefix}-wrap [style*="#7042c9"],
.${prefix}-wrap [style*="#7c3aed"],
.${prefix}-wrap [style*="#6d28d9"],
.${prefix}-wrap [style*="#5433ff"],
.${prefix}-wrap [style*="rgb(112, 66, 201)"],
.${prefix}-wrap [style*="rgb(124, 58, 237)"],
.${prefix}-wrap [style*="rgb(84, 51, 255)"] {
  border-color: ${accent} !important;
  color: ${accent} !important;
}
.${prefix}-wrap section[class*="dark"],
.${prefix}-wrap section[class*="night"],
.${prefix}-wrap section[class*="price"],
.${prefix}-wrap section[class*="cost"],
.${prefix}-wrap [class*="dark-section"],
.${prefix}-wrap [class*="price-section"],
.${prefix}-wrap [class*="cost-section"] {
  background: ${darkSection} !important;
}
@media (max-width: 749px) {
  .${prefix}-wrap section {
    padding-top: 28px !important;
    padding-bottom: 28px !important;
  }
  .${prefix}-wrap [class*="image-card"],
  .${prefix}-wrap [class*="img-card"],
  .${prefix}-wrap [class*="media-card"],
  .${prefix}-wrap [class*="visual-card"],
  .${prefix}-wrap [class*="photo-card"],
  .${prefix}-wrap [class*="figure-card"] {
    padding: 8px !important;
  }
  .${prefix}-wrap figure > img {
    max-height: none;
  }
}
`;

  if (liquid.includes('<style>')) {
    return liquid.replace('</style>', `${guardCss}\n</style>`);
  }
  return `<style>${guardCss}</style>\n${liquid}`;
}

function readableDarkSectionColor(hex) {
  const m = /^#([0-9a-f]{6})$/i.exec(hex || '');
  if (!m) return '#52263a';
  const r = parseInt(m[1].slice(0, 2), 16);
  const g = parseInt(m[1].slice(2, 4), 16);
  const b = parseInt(m[1].slice(4, 6), 16);
  const luminance = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
  if (luminance >= 0.08) return hex;
  const mix = (channel) => Math.round(channel * 0.82 + 255 * 0.18);
  return `#${[mix(r), mix(g), mix(b)].map(v => v.toString(16).padStart(2, '0')).join('')}`;
}

/**
 * Rewrite dangerous CSS patterns that crop content images.
 *
 * Matches any CSS rule whose selector ends with `img { ... }` and whose body
 * contains BOTH `object-fit: cover` AND a fixed `height: Npx` ≤ 400px. Swaps
 * the crop for a contain-fit on a 4:3 canvas with white background padding,
 * which preserves labels, icons, and arrows that source infographics bake
 * directly into the photo.
 *
 * Heroes (banner-style, typically >= 500px tall) are intentionally left
 * alone — those really do want the edge-to-edge cover crop.
 */
function sanitizeCardImageCropping(css) {
  const ruleRe = /([^{}]*?\bimg\b[^{}]*?)\{([^{}]+)\}/g;
  let patched = 0;
  const out = css.replace(ruleRe, (match, selector, body) => {
    if (!/object-fit\s*:\s*cover/i.test(body)) return match;
    const heightMatch = body.match(/height\s*:\s*(\d+)\s*px/i);
    if (!heightMatch) return match;
    const h = parseInt(heightMatch[1], 10);
    if (h > 400) return match; // leave hero/banner rules alone

    let newBody = body
      .replace(/object-fit\s*:\s*cover\s*;?/gi, '')
      .replace(/height\s*:\s*\d+\s*px\s*;?/gi, '')
      .replace(/\n\s*\n/g, '\n')
      .trimEnd();
    if (newBody && !newBody.trim().endsWith(';')) newBody += ';';
    newBody += `\n  aspect-ratio: 4 / 3;\n  object-fit: contain;\n  background: #fff;\n  padding: 8px;\n`;
    patched++;
    return `${selector}{${newBody}}`;
  });
  if (patched > 0) {
    console.log(`  [AI] Sanitized ${patched} card-image CSS rule(s) — swapped cover→contain`);
  }
  return out;
}

// Keep old exports for backward compatibility
async function generateContent(blockId, variant, scrapedSection, allScrapedSections) {
  throw new Error('generateContent is deprecated. Use generateFullLiquid instead.');
}

module.exports = { generateContent, generateFullLiquid };
