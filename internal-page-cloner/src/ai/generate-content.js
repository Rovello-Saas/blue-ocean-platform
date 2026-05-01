const { callClaudeWithImage } = require('./client');
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

function getSystemPrompt(storeId, targetLanguage) {
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

  return `You are a Shopify page-cloning designer for the ${store.name} store. You create complete, production-ready custom Liquid content sections for product pages.

Brand: ${store.name} (${store.domain})
Language: ${store.lang}
Brand voice: Clean, confident, benefit-focused. Short sentences. No hype or excessive exclamation marks. Professional but warm.

You will receive a screenshot and scraped data from a source product page. Your job is to create a COMPLETE Liquid file (HTML + CSS + JavaScript) that faithfully recreates the source product page's visual direction and section flow inside Shopify. All text must be in ${store.lang}. Never use the source brand name in written copy — use generic product language or "${store.name}" where a brand is required.

## FIDELITY PRIORITY

This is a page clone, not a generic ${store.name} template.
- Preserve the source page's color palette from the screenshot and image assets. If the source page is blush/pink/rose/cream, use blush/pink/rose/cream. If it is blue, use blue. Do NOT force ${store.name}'s default colors.
- Preserve the source page's section sequence and visual concepts as much as possible: product hero, benefits, science/technology explanation, how-to-use, results/statistics, comparison chart, before/after proof, expert/social proof, guarantee, FAQ.
- Preserve source image compositions. If an image is already a before/after composite, comparison chart, infographic, dermatologist card, quote card, or guarantee graphic, use it as ONE whole image. Do NOT split it into separate "before" and "after" images. Do NOT recreate it as unrelated cards.
- Keep the look close to the source, but rewrite copy so it is original and does not mention the source brand.

## REQUIRED OUTPUT STRUCTURE

Your output must be a complete file with these parts:

1. \`<style>\` block with ALL CSS (unique prefix per product, e.g. \`xyz-\` for "XYZ Product")
2. \`<div class="PREFIX-wrap">\` containing all sections
3. \`<script>\` block for FAQ accordion interactivity

## RECOMMENDED SECTION FLOW

Follow the source page's order when the screenshot/images show it. A skincare device page usually needs:
1. **Trust / proof strip** — small credibility badges and guarantee points
2. **Benefit cards or technology grid** — red light, warmth, massage, absorption/current, etc.
3. **Science / how it works section** — use the source diagram or technology infographic if available
4. **How to use section** — use the source usage image/steps if available
5. **Results/statistics section** — preserve the source stats style and palette
6. **Comparison chart** — if a chart image is available, use the chart image whole; otherwise create a close table
7. **Before & after / real results** — use the before-after composite image(s) whole and prominent
8. **Expert/social proof / customer quotes**
9. **Guarantee / risk-free section**
10. **FAQ Accordion** — 5-7 collapsible Q&A items with JavaScript toggle

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
- REWRITE all content — never copy source text verbatim. Never mention the source brand name.
- All review content must be original (create realistic-sounding reviews based on product benefits)
- Stats percentages should be realistic and may mirror the source page's visible style/range.
- SVG icons: use Feather-style stroke icons (stroke=source heading color, stroke-width="1.5", fill="none", viewBox="0 0 24 24"). For small decorative accent icons (checkmarks in the trust bar, stars), use source accent color.
- IMAGE CROPPING — content-card images (use-case cards, feature cards, diagrams, sleep-position illustrations, comparison images, review photos) MUST use \`aspect-ratio: 4/3; object-fit: contain; background: #fff; padding: 8px;\` — NEVER \`height: Npx; object-fit: cover\`. Source images are frequently infographics with labels, icons, arrows, or text baked in; \`cover\` crops that content off. Only the main hero or dark-hero banner — an edge-to-edge lifestyle photo — may use \`object-fit: cover\` (and even then, prefer a large min-height over a fixed pixel height).
- BEFORE/AFTER RULE — Any image whose label or alt text includes "before-after", "before and after", "Day 0", "Day 30", or "Real Results" must be shown as a single complete image in a before/after proof section. Never crop it, split it, or rebuild it as two separate unrelated images.
- BEFORE/AFTER SLIDER RULE — For Solawave-like pages, the before/after proof should feel like the source site: a horizontal results slider/carousel of whole before/after composite images with arrows/dots. If truly separate before and after photos are available instead, build a draggable compare slider with stacked images, a range input, a vertical divider, and "Before" / "After" labels. Never turn composite before/after images into unrelated separate cards.

## OUTPUT FORMAT

Return ONLY the raw HTML/CSS/JS code. No markdown code fences. No explanation text. Just the complete \`<style>...<div>...<script>\` file.`;
}

/**
 * Generate a complete custom liquid content file for a product.
 * Single AI call that produces the entire file.
 */
async function generateFullLiquid(productMeta, sections, screenshotPath, storeId = 'movanella', targetLanguage = null) {
  const storeNames = { movanella: 'Movanella', merivalo: 'Merivalo' };
  const storeName = storeNames[storeId] || 'Movanella';
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

  // Build section summaries (limit to avoid token overflow)
  const sectionSummary = sections.slice(0, 10).map(s => ({
    headings: s.headings?.slice(0, 3),
    paragraphs: s.paragraphs?.slice(0, 2).map(p => p.substring(0, 200)),
    imageCount: s.images?.length || 0,
    firstImage: s.images?.[0]?.src?.substring(0, 150) || null
  }));

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

  function pushImage(img) {
    if (!img?.src || seenSrcs.has(img.src)) return;
    seenSrcs.add(img.src);
    availableImagesWithLabels.push({ src: img.src, label: labelFor(img) });
  }

  // 1. Product gallery images first — these are the canonical product photos.
  //    Raise cap from 8 → 25 so sleep-position photos / size guides / callouts
  //    that sit later in the gallery actually reach the liquid generator.
  (productMeta.images || []).slice(0, 25).forEach(pushImage);

  // 2. First image from each scraped section (up to ~10 extra) for section-specific diagrams etc.
  sections.slice(0, 10).forEach(s => {
    if (availableImagesWithLabels.length >= 30) return;
    if (s.images?.[0]) pushImage(s.images[0]);
  });

  // Flat list of URLs (kept for backwards-compatible consumers below)
  const availableImages = availableImagesWithLabels.map(x => x.src);
  const sourceDesign = inferSourceDesign(productMeta, sections, availableImagesWithLabels);
  const criticalImages = availableImagesWithLabels.filter(x =>
    /(before[-\s]?after|before and after|day\s*0|day\s*30|real results|comparison chart|dermatologist|guarantee|how to use|easy to use|3-5x|visible results)/i.test(x.label || '')
  );
  const beforeAfterAssets = findBeforeAfterImages(availableImagesWithLabels);

  const userMessage = `Create a complete ${storeName} product page liquid file for this product.

## PRODUCT METADATA
- Title: ${productMeta.title}
- Price: ${productMeta.price}
- Compare-at Price: ${productMeta.compareAtPrice || 'N/A'}
- Description: ${productMeta.description?.substring(0, 500) || 'N/A'}
- Variants: ${JSON.stringify(productMeta.variants?.slice(0, 5) || [])}

## SCRAPED PAGE SECTIONS
${JSON.stringify(sectionSummary, null, 2)}

## SOURCE DESIGN PROFILE
${sourceDesign.instructions}

## AVAILABLE IMAGES (use these URLs in your HTML — each has a semantic label)
${availableImagesWithLabels.map((x, i) => `${i + 1}. [${x.label || 'unlabeled'}]  ${x.src}`).join('\n')}

## CRITICAL SOURCE IMAGES TO PRESERVE AS WHOLE IMAGES
${criticalImages.length ? criticalImages.map((x, i) => `${i + 1}. [${x.label || 'unlabeled'}]  ${x.src}`).join('\n') : 'No critical composite images detected.'}

## BEFORE/AFTER SLIDER ASSETS
${formatBeforeAfterAssets(beforeAfterAssets)}

IMAGE USAGE RULES:
- Pick the SEMANTICALLY most appropriate URL for each slot. The label in square brackets tells you what each image shows (e.g. "hotel pillow meagan side sleeping" is a side-sleeper photo; "size guide" is a sizing diagram; "hotel pillow callouts" is a features-callout diagram).
- DO NOT reuse the same image URL across multiple different sections. If you have a "Side Sleeper / Back Sleeper / Stomach Sleeper" grid and three distinct sleeper photos are available, use three different URLs — one for each card. Only reuse an image if the layout intentionally shows the same product angle twice (e.g. hero + dark-hero split) AND no alternate angle is available.
- If you run out of distinct semantically-matching images for a section, pick the closest-fitting unused image rather than repeating one you already used.
- Prefer images with descriptive labels (diagrams, callouts, benefits, lifestyle shots) for content sections. Reserve the clean product-only shots for the gallery/hero.
- If a critical source image is listed above, preserve it as a complete visual asset in the matching section. Before/after composite images must remain composite images.
- If BEFORE/AFTER SLIDER ASSETS lists composite images, you MUST build a horizontal results slider/carousel using those complete images. If it lists a before + after pair instead, build a draggable compare slider with those exact two image URLs.

## REFERENCE STRUCTURE
Here is the HTML structure of a previous product page we built. Use it only for Liquid mechanics, class-prefix conventions, responsive CSS, and FAQ JavaScript. Do NOT copy its colors or force its section order when the source screenshot/images show a different visual style:

${referenceSnippet.substring(0, 4000)}

Now generate the complete file for "${productMeta.title}". Remember: unique CSS prefix, all sections, mobile responsive, FAQ JavaScript.`;

  console.log(`  [AI] Generating full liquid content...`);
  const response = await callClaudeWithImage(
    getSystemPrompt(storeId, targetLanguage),
    screenshotBase64,
    userMessage,
    { maxTokens: 16384 }
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
  liquid = injectBeforeAfterSliderFallback(liquid, beforeAfterAssets, sourceDesign);

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

function injectBeforeAfterSliderFallback(liquid, assets, sourceDesign) {
  if (hasBeforeAfterInteractiveSection(liquid)) return liquid;

  const prefix = inferCssPrefix(liquid);
  const colors = {
    accent: sourceDesign.accent || '#e66f8f',
    dark: sourceDesign.dark || '#52263a',
    soft: sourceDesign.soft || '#fde8ee',
    cream: sourceDesign.cream || '#fff7f1'
  };

  if (assets.composite.length) {
    return injectSectionAndScript(
      liquid,
      buildBeforeAfterCarousel(prefix, colors, assets.composite.slice(0, 6)),
      '  [AI] Injected before/after carousel fallback'
    );
  }

  const before = assets.before[0];
  const after = assets.after[0];
  if (!before?.src || !after?.src) return liquid;

  return injectSectionAndScript(
    liquid,
    buildBeforeAfterCompareSlider(prefix, colors, before, after),
    '  [AI] Injected before/after comparison slider fallback'
  );
}

function hasBeforeAfterInteractiveSection(liquid) {
  return /type=["']range["']|before-after-slider|ba-slider|ba-carousel|data-before-after-(?:slider|carousel)|results?-(?:slider|carousel)/i.test(liquid);
}

function buildBeforeAfterCarousel(prefix, colors, images) {
  const slides = images.map((image, i) => `
        <div class="${prefix}-ba-slide" role="group" aria-label="Result ${i + 1} of ${images.length}">
          <img src="${escapeHtml(image.src)}" alt="${escapeHtml(image.label || `Before and after result ${i + 1}`)}" loading="lazy">
        </div>`).join('');
  const dots = images.map((_, i) => `
        <button class="${prefix}-ba-dot" type="button" aria-label="Show result ${i + 1}" aria-current="${i === 0 ? 'true' : 'false'}"></button>`).join('');

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
  .${prefix}-ba-slide img {
    display: block;
    width: 100%;
    aspect-ratio: 4 / 3;
    object-fit: contain;
    background: #fff;
    border-radius: 22px;
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
    .${prefix}-ba-slide img {
      aspect-ratio: 3 / 4;
      border-radius: 18px;
    }
  }`;

  const section = `
  <section class="${prefix}-ba-section">
    <div class="${prefix}-ba-heading">
      <h2>Real results you can see</h2>
      <p>Swipe through the before and after proof, shown as complete source images.</p>
    </div>
    <div class="${prefix}-ba-carousel" data-before-after-carousel>
      <div class="${prefix}-ba-viewport">
        <div class="${prefix}-ba-track">
${slides}
        </div>
      </div>
      <div class="${prefix}-ba-controls" aria-label="Before and after results">
        <button class="${prefix}-ba-button ${prefix}-ba-prev" type="button" aria-label="Previous result">‹</button>
        <div class="${prefix}-ba-dots">
${dots}
        </div>
        <button class="${prefix}-ba-button ${prefix}-ba-next" type="button" aria-label="Next result">›</button>
      </div>
    </div>
  </section>`;

  const script = `
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

function buildBeforeAfterCompareSlider(prefix, colors, before, after) {
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
      <h2>Real results you can compare</h2>
      <p>Drag the slider to see how the before and after photos line up.</p>
    </div>
    <div class="${prefix}-ba-slider" data-before-after-slider>
      <img src="${escapeHtml(after.src)}" alt="${escapeHtml(after.label || 'After result')}" loading="lazy">
      <img class="${prefix}-ba-before-img" src="${escapeHtml(before.src)}" alt="${escapeHtml(before.label || 'Before result')}" loading="lazy">
      <span class="${prefix}-ba-label ${prefix}-ba-label-before">Before</span>
      <span class="${prefix}-ba-label ${prefix}-ba-label-after">After</span>
      <span class="${prefix}-ba-divider"></span>
      <span class="${prefix}-ba-handle" aria-hidden="true">↔</span>
      <input class="${prefix}-ba-range" type="range" min="0" max="100" value="50" aria-label="Compare before and after photos">
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

function inferSourceDesign(productMeta, sections, labeledImages) {
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

function applySourcePaletteGuard(liquid, sourceDesign) {
  if (sourceDesign.kind !== 'beauty-red-light') return liquid;
  const replacements = [
    [/#07941a/gi, sourceDesign.accent],
    [/#16a34a/gi, sourceDesign.accent],
    [/#22c55e/gi, sourceDesign.accent],
    [/#1b2d5b/gi, sourceDesign.dark],
    [/#0f172a/gi, sourceDesign.dark],
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
    console.log(`  [AI] Applied source palette guard (${changed} color replacement(s))`);
  }
  return out;
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
