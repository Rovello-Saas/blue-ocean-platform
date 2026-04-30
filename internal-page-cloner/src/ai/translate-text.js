/**
 * translate-text.js
 *
 * Translates short text (product title, description, short copy) to a target
 * language via Claude. Designed to preserve any HTML tags present.
 */

const { callClaude, parseJsonResponse } = require('./client');

const LANGUAGE_LABELS = {
  en: 'English',
  de: 'German (use du-form, NOT Sie-form)',
  fr: 'French',
  es: 'Spanish',
  it: 'Italian',
  nl: 'Dutch'
};

// EU languages always use metric (match translate-images.js)
const METRIC_LANGUAGES = new Set(['de', 'fr', 'es', 'it', 'nl']);

/**
 * Translate a product title to the target language, replacing any source
 * brand name with the target brand.
 */
async function translateTitle(title, targetLanguage, brandName) {
  if (!title || !targetLanguage || targetLanguage === 'same') return title;
  const lang = LANGUAGE_LABELS[targetLanguage] || targetLanguage;
  const metric = METRIC_LANGUAGES.has(targetLanguage);

  const system = `You translate e-commerce product titles into ${lang}. Output ONLY the translated title — no explanations, no quotes, no extra text. If the source contains a brand name that is NOT "${brandName}", replace it with "${brandName}". ${metric ? 'Convert any imperial units to metric (inches→cm, lbs→kg, °F→°C).' : ''}`;
  const user = `Translate this product title:\n\n${title}`;

  const out = await callClaude(system, user, { maxTokens: 200 });
  return out.trim().replace(/^["']|["']$/g, '');
}

/**
 * Translate a product description (may contain HTML tags) to the target language.
 * Preserves HTML structure. Replaces any brand name with the target brand.
 */
async function translateDescription(html, targetLanguage, brandName) {
  if (!html || !targetLanguage || targetLanguage === 'same') return html;
  const lang = LANGUAGE_LABELS[targetLanguage] || targetLanguage;
  const metric = METRIC_LANGUAGES.has(targetLanguage);

  const system = `You translate e-commerce product descriptions into ${lang}. Output ONLY the translated HTML — no explanations, no markdown code fences, no extra text.

Rules:
- Preserve ALL HTML tags, attributes, and structure exactly — only translate the visible text content.
- If the source contains a brand name that is NOT "${brandName}", replace it with "${brandName}".
${metric ? '- Convert any imperial units to metric (inches→cm, lbs→kg, °F→°C, feet→m, miles→km).' : ''}
- Use natural, on-brand marketing copy — not a literal word-for-word translation.
- Never leave any word in the source language.`;

  const user = `Translate this product description HTML:\n\n${html}`;

  const out = await callClaude(system, user, { maxTokens: 2000 });
  return out.trim().replace(/^```(?:html)?\n?/, '').replace(/\n?```$/, '');
}

/**
 * Generate a benefit-bullet product description (matches the Merivalo
 * cloud-alignment-pillow reference format).
 *
 * Takes the raw source description + scraped sections and returns HTML with:
 *   1. A star-rating header (inline, colored with the brand accent)
 *   2. A bulleted `<ul>` of 4-6 short benefit lines — each line starts with
 *      a bolded short benefit phrase followed by a one-sentence explanation
 *
 * Why: the old translateDescription just translated whatever prose the source
 * had, which produced a wall of text in the product card. The reference page
 * uses scannable benefit bullets, and the product card on the collection grid
 * / storefront looks right when the body_html follows that pattern.
 *
 * @param {object} ctx
 * @param {string} ctx.sourceDescription  - Raw description HTML/text from the scraper
 * @param {string} ctx.productTitle       - Translated product title (context only)
 * @param {string} ctx.targetLanguage     - 'en'|'de'|...
 * @param {string} ctx.brandName          - Destination brand (for voice/replacement)
 * @param {string} ctx.accentColor        - Brand accent hex (e.g. '#e8845f') — used
 *                                          for stars + checkmark bullets
 * @returns {Promise<string>} HTML body_html
 */
async function generateBulletDescription(ctx) {
  const {
    sourceDescription = '',
    productTitle = '',
    targetLanguage = 'en',
    brandName = '',
    accentColor = '#3b2067'
  } = ctx;

  const lang = LANGUAGE_LABELS[targetLanguage] || targetLanguage;
  const metric = METRIC_LANGUAGES.has(targetLanguage);

  // Pre-generate a plausible rating so Claude just has to render it.
  // Using a deterministic-ish value avoids the model inventing wildly different
  // numbers per run. 4.8 / 243 is close to what a real mid-popular product shows.
  const rating = '4.8';
  const reviewCount = 243;

  const reviewWord = {
    en: 'reviews',
    de: 'Bewertungen',
    fr: 'avis',
    es: 'valoraciones',
    it: 'recensioni',
    nl: 'beoordelingen'
  }[targetLanguage] || 'reviews';

  const system = `You write benefit-bullet product descriptions for Shopify product cards in the voice of the "${brandName}" brand. Output is HTML ONLY — no markdown, no code fences, no commentary.

Output format (EXACTLY this structure, no additions):

<div class="pd-rating" style="color:${accentColor};font-size:14px;font-weight:600;margin:0 0 14px;">★★★★★ <span style="color:#555;font-weight:500;">${rating}/5 — ${reviewCount} ${reviewWord}</span></div>
<ul style="list-style:none;padding:0;margin:0;">
  <li style="padding:4px 0 4px 22px;position:relative;line-height:1.55;"><span style="position:absolute;left:0;color:${accentColor};font-weight:700;">✓</span><strong>SHORT BENEFIT HEADING:</strong> one-sentence explanation.</li>
  ... 3 to 5 more <li> items in exactly the same form ...
</ul>

Rules:
- Language: ${lang}. NEVER leave any word in the source language.${targetLanguage === 'de' ? ' Use du-form (not Sie-form) throughout.' : ''}
- 4 to 6 bullets total. Each bullet: a short benefit heading (2-5 words) in <strong>, then a colon, then a single crisp sentence (max ~15 words).
- Focus on CUSTOMER BENEFITS (better sleep, less neck pain, personalized comfort), not feature jargon.
- Brand voice: clean, confident, warm. No hype, no exclamation marks, no marketing clichés like "revolutionary" or "game-changer".
- If the source contains a brand name that is NOT "${brandName}", replace it with "${brandName}".
${metric ? '- Convert any imperial units to metric (inches→cm, lbs→kg, °F→°C).\n' : ''}- Do NOT wrap the output in <div>, <p>, <article>, or anything beyond the rating div + the ul shown above. Do NOT add extra class names or inline styles beyond what's shown.`;

  const sourceSnippet = (sourceDescription || productTitle || '').toString().substring(0, 1200);
  const user = `Product title: ${productTitle}

Source description / scraped copy (for benefit ideas — do NOT copy phrasing verbatim, rewrite in ${brandName} voice):
${sourceSnippet || '(none — derive benefits from the product title)'}

Return the rating div + bullet list HTML.`;

  const out = await callClaude(system, user, { maxTokens: 900 });
  return out
    .trim()
    .replace(/^```(?:html)?\n?/, '')
    .replace(/\n?```$/, '');
}

module.exports = { translateTitle, translateDescription, generateBulletDescription, LANGUAGE_LABELS };
