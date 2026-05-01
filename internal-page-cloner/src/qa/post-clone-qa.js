/**
 * post-clone-qa.js
 *
 * Automated quality checks that run after the clone pipeline completes.
 *
 * The goal: catch obvious failure modes BEFORE the user has to manually review
 * the published page. Each check reports one of three severities:
 *   - error   → something is almost certainly broken (e.g. only 1 image for 8 sections,
 *               source brand name still in HTML, source domain URLs still in liquid)
 *   - warning → suspicious but could be legitimate (e.g. low image diversity,
 *               description looks short)
 *   - info    → status notes (e.g. "14 images uploaded, 0 source URLs remain")
 *
 * The report is attached to the job object and surfaced in the UI so you don't
 * have to click through the storefront for every clone.
 *
 * Checks deliberately DON'T block publishing — the pipeline still completes and
 * the product goes live. This is a trust-and-verify signal, not a gate. If you
 * want to make errors fatal later, that's a one-line change in api.js.
 */

const LANGUAGE_NAMES = {
  en: 'English',
  de: 'German',
  fr: 'French',
  es: 'Spanish',
  it: 'Italian',
  nl: 'Dutch'
};

/**
 * Count unique image URLs referenced in a blob of HTML/liquid.
 * Works for both <img src="..."> and background-image: url(...)
 */
function extractImageUrls(html) {
  if (!html) return [];
  const urls = new Set();
  // <img src="...">
  const imgRe = /<img[^>]+src\s*=\s*["']([^"']+)["']/gi;
  let m;
  while ((m = imgRe.exec(html)) !== null) {
    if (!m[1].startsWith('data:')) urls.add(m[1].split('?')[0]);
  }
  // background-image: url(...)
  const bgRe = /url\(\s*["']?([^"')]+)["']?\s*\)/gi;
  while ((m = bgRe.exec(html)) !== null) {
    if (!m[1].startsWith('data:')) urls.add(m[1].split('?')[0]);
  }
  return [...urls];
}

function normalizeImageUrl(url) {
  if (!url || typeof url !== 'string') return '';
  let out = url.trim().replace(/&amp;/g, '&');
  if (out.startsWith('//')) out = 'https:' + out;
  return out;
}

function imageBaseKey(url) {
  const normalized = normalizeImageUrl(url);
  if (!normalized) return '';
  try {
    const u = new URL(normalized);
    return `${u.hostname}${u.pathname}`.toLowerCase();
  } catch (e) {
    return normalized.split('?')[0].toLowerCase();
  }
}

/**
 * Strip HTML tags and return visible text content.
 * Not a full parser — good enough for word-level scans.
 */
function htmlToText(html) {
  if (!html) return '';
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Run all QA checks. Returns a structured report.
 *
 * @param {object} ctx
 * @param {string} ctx.sourceUrl        - Source product URL we scraped
 * @param {string} ctx.targetLanguage   - 'en'|'de'|... or null for brand-only
 * @param {string} ctx.brandName        - Destination brand name (e.g. 'Merivalo')
 * @param {string} ctx.productTitle     - Final (possibly translated) title
 * @param {object} ctx.productMeta      - { title, description, ... } as sent to Shopify
 * @param {string[]} ctx.scrapedImageUrls - All images the scraper found
 * @param {string[]} ctx.uploadedImageUrls - Shopify CDN URLs after upload
 * @param {string} ctx.liquidContent    - The custom-liquid HTML pushed to the template
 * @param {number} ctx.urlRewriteCount  - How many source URLs we rewrote in the liquid
 * @returns {{ pass: boolean, errors: string[], warnings: string[], info: string[] }}
 */
function runPostCloneQa(ctx) {
  const errors = [];
  const warnings = [];
  const info = [];

  const {
    sourceUrl,
    targetLanguage,
    brandName,
    productTitle,
    productMeta,
    scrapedImageUrls = [],
    uploadedImageUrls = [],
    liquidContent = '',
    urlRewriteCount = 0,
  } = ctx;

  // ── Check 1: Image count sanity ────────────────────────────────────────
  // If the scraper only found 1-2 images for a product page, it usually means
  // lazy-loaded images weren't hydrated. This was the alcedohealth bug.
  if (scrapedImageUrls.length === 0) {
    errors.push('No product images were scraped from the source page. The product will have no photos.');
  } else if (scrapedImageUrls.length === 1) {
    errors.push('Only 1 product image was scraped from the source page. This almost always means lazy-loaded images weren\'t hydrated — the clone will look repetitive with the same image in every section.');
  } else if (scrapedImageUrls.length === 2) {
    warnings.push(`Only 2 product images were scraped. A typical product page has 5+ distinct photos; the clone may look repetitive.`);
  } else {
    info.push(`${scrapedImageUrls.length} product images scraped from source.`);
  }

  // ── Check 2: Scraped vs uploaded parity ────────────────────────────────
  if (scrapedImageUrls.length > 0 && uploadedImageUrls.length < scrapedImageUrls.length) {
    const dropped = scrapedImageUrls.length - uploadedImageUrls.length;
    warnings.push(`${dropped} of ${scrapedImageUrls.length} scraped images failed to upload to Shopify.`);
  }

  // ── Check 3: Image diversity in generated liquid ───────────────────────
  // If the AI-generated liquid references very few distinct URLs across many
  // sections, we're showing the same photo repeatedly (the problem that led to
  // this QA system in the first place).
  const liquidImageUrls = extractImageUrls(liquidContent);
  const distinctLiquidImages = liquidImageUrls.length;
  // Rough heuristic: a "rich" liquid is > 5000 chars and would normally span
  // 6-10 sections, so we expect at least 4 distinct images. A short liquid can
  // legitimately reuse one image.
  const liquidSize = liquidContent.length;
  if (liquidSize > 5000) {
    if (distinctLiquidImages <= 1) {
      errors.push(`The generated page content references only ${distinctLiquidImages} unique image${distinctLiquidImages === 1 ? '' : 's'} across ${Math.round(liquidSize / 1000)}KB of content. Sections will show the same photo everywhere.`);
    } else if (distinctLiquidImages === 2) {
      warnings.push(`The generated page content references only 2 unique images across multiple sections — some repetition is likely.`);
    } else if (distinctLiquidImages === 3) {
      warnings.push(`The generated page content references only 3 unique images; ideal is 4+ for a rich product page.`);
    } else {
      info.push(`${distinctLiquidImages} distinct images used across generated sections.`);
    }
  }

  // ── Check 4: Source URLs remaining in liquid ───────────────────────────
  // After the URL-rewrite pass we should have no source-domain URLs in the
  // liquid. If any remain, the page will load the UNTRANSLATED source image.
  let sourceHost = '';
  try {
    sourceHost = new URL(sourceUrl).hostname.replace(/^www\./, '');
  } catch (e) {}
  if (sourceHost && liquidContent.includes(sourceHost)) {
    const matches = liquidContent.match(new RegExp(sourceHost.replace(/\./g, '\\.'), 'g')) || [];
    errors.push(`${matches.length} reference(s) to the source domain "${sourceHost}" remain in the generated HTML. These will load untranslated images from the original store.`);
  }

  // Source product images are often hosted on Shopify's shared CDN instead of
  // the source brand domain. Catch those too by comparing base image paths.
  const liquidImageKeys = new Set(liquidImageUrls.map(imageBaseKey).filter(Boolean));
  const leakedSourceImages = scrapedImageUrls
    .filter(Boolean)
    .filter(src => liquidImageKeys.has(imageBaseKey(src)));
  if (leakedSourceImages.length > 0 && uploadedImageUrls.length > 0) {
    const sample = leakedSourceImages[0].split('?')[0].split('/').pop();
    errors.push(`${leakedSourceImages.length} original scraped image URL(s) still appear in the generated HTML after Shopify upload/rewrite${sample ? ` (example: ${sample})` : ''}. These may show untranslated source images.`);
  }

  if (urlRewriteCount > 0) {
    info.push(`Rewrote ${urlRewriteCount} source-image URL(s) → Shopify CDN.`);
  }

  // ── Check 5: Source brand contamination ────────────────────────────────
  // If a recognizable source brand name leaks into the final HTML, the copy
  // wasn't fully rebranded. We check a few common source brands plus the
  // source domain's brand stem.
  if (brandName) {
    const text = htmlToText(liquidContent).toLowerCase();
    const stemsToCheck = new Set();

    // Derive a brand stem from the source hostname (e.g. "alcedohealth.com" → "alcedo", "alcedohealth")
    if (sourceHost) {
      const hostStem = sourceHost.split('.')[0];
      if (hostStem && hostStem.length >= 3) stemsToCheck.add(hostStem);
      // Also add a "shorter" variant for compound brand names (e.g. "alcedohealth" → "alcedo")
      const match = hostStem.match(/^([a-z]{3,})(health|sleep|shop|store|brand|life|home)$/i);
      if (match) stemsToCheck.add(match[1]);
    }
    // Known source brands we've cloned from before
    ['mellow', 'mellowsleep'].forEach(b => stemsToCheck.add(b));

    // Don't flag the destination brand itself
    stemsToCheck.delete(brandName.toLowerCase());

    for (const stem of stemsToCheck) {
      if (!stem || stem.length < 3) continue;
      // Word boundary match to avoid e.g. "alcedo" hitting "alcedomania"
      const re = new RegExp(`\\b${stem}\\b`, 'i');
      if (re.test(text)) {
        errors.push(`Source brand name "${stem}" still appears in the generated page copy. It should read "${brandName}".`);
        break; // one per product is enough — don't flood the report
      }
    }
  }

  // ── Check 6: Title translated (for non-English targets) ────────────────
  if (targetLanguage && targetLanguage !== 'en' && productTitle) {
    // Very rough signal: if title has common English words and target is non-English, flag.
    const englishMarkers = /\b(for|the|with|and|your|cooling|support|pillow|premium|ultra|cloud)\b/i;
    const germanMarkers = /\b(für|der|die|das|mit|und|dein|deine|kühlend|stütz|kissen)\b/i;
    const frenchMarkers = /\b(pour|le|la|les|avec|et|votre|ton|ta|oreiller)\b/i;
    const spanishMarkers = /\b(para|el|la|los|con|y|tu|tus|almohada)\b/i;
    const italianMarkers = /\b(per|il|la|i|le|con|e|tuo|tua|cuscino)\b/i;
    const dutchMarkers = /\b(voor|de|het|met|en|jouw|je|kussen)\b/i;

    const markers = { de: germanMarkers, fr: frenchMarkers, es: spanishMarkers, it: italianMarkers, nl: dutchMarkers };
    const expectedMarker = markers[targetLanguage];

    if (englishMarkers.test(productTitle) && expectedMarker && !expectedMarker.test(productTitle)) {
      warnings.push(`Product title appears to still be in English: "${productTitle}". Expected ${LANGUAGE_NAMES[targetLanguage] || targetLanguage}.`);
    }
  }

  // ── Check 7: Description sanity ────────────────────────────────────────
  const desc = productMeta?.description || '';
  if (!desc || desc.length < 30) {
    warnings.push(`Product description is very short (${desc.length} chars). This shows in the product card on collection pages.`);
  }

  // ── Check 8: Liquid structural sanity ──────────────────────────────────
  if (liquidContent && liquidSize < 2000) {
    warnings.push(`Generated page content is very short (${liquidSize} chars). Normally 10KB+ — the AI may have failed mid-output.`);
  }
  if (liquidContent && !liquidContent.includes('<style>')) {
    warnings.push('Generated page content is missing a <style> block; layout may look broken.');
  }

  const pass = errors.length === 0;

  return { pass, errors, warnings, info };
}

/**
 * Format the report as a plain-text block suitable for log output.
 */
function formatQaReport(report) {
  const lines = [];
  lines.push(`QA: ${report.pass ? '✓ PASS' : '✗ FAIL'} (${report.errors.length} error(s), ${report.warnings.length} warning(s))`);
  report.errors.forEach(e => lines.push(`  ✗ ERROR:   ${e}`));
  report.warnings.forEach(w => lines.push(`  ⚠ WARNING: ${w}`));
  report.info.forEach(i => lines.push(`  ℹ ${i}`));
  return lines.join('\n');
}

module.exports = { runPostCloneQa, formatQaReport, extractImageUrls };
