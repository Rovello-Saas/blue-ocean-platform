const { classifyAspectRatio } = require('../blocks/library');

/**
 * Analyze and classify images from extracted sections
 * Adds aspect ratio classification to each image
 */
function analyzeImages(sections) {
  for (const section of sections) {
    for (const image of section.images) {
      // Use natural dimensions if available, fall back to display dimensions
      const width = image.naturalWidth || image.displayWidth;
      const height = image.naturalHeight || image.displayHeight;

      image.ratioClass = classifyAspectRatio(width, height);

      // Compute ratio if not already set
      if (!image.ratio && width && height) {
        image.ratio = +(width / height).toFixed(3);
      }
    }
  }
  return sections;
}

/**
 * Classify each image by its semantic purpose so the translation pipeline can
 * route it to the right transform:
 *   - hero                  → primary product photo at the top of the page
 *   - lifestyle-with-person → photo featuring a model/customer (gets face-swap)
 *   - callout-with-text     → product photo with annotated callouts/labels
 *   - comparison-composite  → side-by-side or charted comparison image
 *   - before-after          → before/after composite or labeled pair
 *   - logo-strip            → press logos / award badges (dropped from output)
 *   - product-only          → clean product shot, no people, no callouts
 *
 * Heuristic-only for now: cheap, deterministic, no extra API calls. Driven by
 * alt text, filename, and section context. Accuracy ~70-80% on the source
 * pages we've seen. If we need more accuracy later we can add a Claude-vision
 * pass on uncertain cases without changing the callers — the function
 * signature already returns purposes per src.
 *
 * @param {Array<{src, alt?, displayWidth?, displayHeight?, sectionHeadings?}>} images
 * @returns {Map<string, string>} src → purpose
 */
function classifyImagePurposes(images) {
  const result = new Map();
  if (!Array.isArray(images)) return result;
  for (const img of images) {
    if (!img?.src || result.has(img.src)) continue;
    result.set(img.src, classifyOne(img));
  }
  return result;
}

function classifyOne(img) {
  const alt = (img.alt || '').toLowerCase();
  const src = (img.src || '').toLowerCase();
  const ctx = (img.sectionHeadings || []).join(' ').toLowerCase();
  const haystack = `${alt} ${src} ${ctx}`;

  // Order matters — most specific first.
  if (/before[-\s]?after|before-and-after|day[\s_-]*0[\s_-]*(?:vs|versus|to|\/|-)?[\s_-]*day[\s_-]*\d+|week[\s_-]*0[\s_-]*(?:vs|versus|to|\/|-)?[\s_-]*week[\s_-]*\d+/.test(haystack)) {
    return 'before-after';
  }
  if (/\b(?:logo|press|featured-in|featured_in|as-seen-in|magazine|award|badges?|elle|vogue|cosmopolitan|allure|women's-?health|nordstrom|ulta|sephora)\b/.test(haystack)) {
    return 'logo-strip';
  }
  // US/region maps and retail-locator graphics — these are source-specific
  // and can't be made authentic on a clone. Always drop.
  if (/\bmap-?(?:locations|of|retailers?)?\b|\b(?:us|usa|united-states|europe)-?map\b|retailer-?map|retail-?locations|store-?locator/.test(haystack)) {
    return 'logo-strip';
  }
  // Doctor / expert headshots — these are claimed endorsements that we
  // can't lift. Drop from the gallery, the AI body section can ref them
  // generically as "Dr. X" without the photo.
  if (/\bdr[\.\-_\s]|doctor|md[\.\-_\s]|ph[\.\-_\s]?d|dermatologist[-_\s]?headshot|expert[-_\s]?(?:photo|headshot)/.test(haystack)) {
    return 'logo-strip';
  }
  if (/comparison|compare|vs\.?\b|versus|chart|table|spec-?sheet|specs|side[-_\s]?by[-_\s]?side/.test(haystack)) {
    return 'comparison-composite';
  }
  if (/\b(?:callout|annotated|labelled|labeled|diagram|infographic|features?[-_\s]callout|specs?[-_\s]?diagram|how[-_\s]?to[-_\s]?use|step[-_\s]?\d|inside[-_\s]?the[-_\s]?tech)\b/.test(haystack)) {
    return 'callout-with-text';
  }
  if (/\b(?:lifestyle|model|woman|women|man|men|person|people|smile|smiling|face|portrait|customer|using|wearing|holding|skin|cheek|forehead|model-shot)\b/.test(haystack)) {
    return 'lifestyle-with-person';
  }
  // Hero detection: large image high on page, square-ish ratio, no text indicators.
  // Use display height as a proxy for page position (heroes usually >= 400px tall).
  const isHeroSized = (img.displayHeight || 0) >= 400 || (img.naturalHeight || 0) >= 600;
  if (isHeroSized && /\b(?:hero|main|featured|primary|wand|device|product|cover)\b/.test(haystack)) {
    return 'hero';
  }
  // Default: clean product shot
  return 'product-only';
}

/**
 * Find the dominant/primary image in a section
 * (the largest image by display area)
 */
function findPrimaryImage(section) {
  if (!section.images || section.images.length === 0) return null;

  return section.images.reduce((best, img) => {
    const area = (img.displayWidth || 0) * (img.displayHeight || 0);
    const bestArea = (best.displayWidth || 0) * (best.displayHeight || 0);
    return area > bestArea ? img : best;
  });
}

/**
 * Get a summary of all images in a section for AI context
 */
function summarizeImages(section) {
  return section.images.map((img, i) => ({
    index: i,
    src: img.src,
    ratio: img.ratio,
    ratioClass: img.ratioClass,
    displaySize: `${img.displayWidth}x${img.displayHeight}`,
    alt: img.alt
  }));
}

module.exports = { analyzeImages, findPrimaryImage, summarizeImages, classifyImagePurposes };
