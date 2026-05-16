const express = require('express');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');

// Download an arbitrary image URL into a Node Buffer. Used only for assets that
// are explicitly safe to pass through. Risky source images are no longer
// uploaded as originals when AI editing fails.
function fetchImageBuffer(url, maxBytes = 8 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let target;
    try {
      target = new URL(url);
    } catch (e) {
      return reject(new Error(`Invalid image URL: ${url}`));
    }
    const client = target.protocol === 'http:' ? http : https;
    const req = client.get(target, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        return fetchImageBuffer(res.headers.location, maxBytes).then(resolve, reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} fetching ${url}`));
      }
      const chunks = [];
      let total = 0;
      res.on('data', (chunk) => {
        total += chunk.length;
        if (total > maxBytes) {
          req.destroy();
          return reject(new Error(`Image exceeds ${maxBytes} bytes: ${url}`));
        }
        chunks.push(chunk);
      });
      res.on('end', () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.setTimeout(15000, () => {
      req.destroy();
      reject(new Error(`Timeout fetching ${url}`));
    });
  });
}

const { scrapePage } = require('../scraper/browser');
const { extractSections, extractProductMeta } = require('../scraper/dom-extractor');
const { analyzeImages, classifyImagePurposes } = require('../scraper/image-analyzer');
const { policyFor } = require('../ai/image-policy');
const { dedupeByPerceptualHash } = require('../scraper/dedupe-images');
const { generateFullLiquid } = require('../ai/generate-content');
const { translateProductImages, generateJobFace, LANGUAGE_NAMES } = require('../ai/translate-images');
const { translateTitle, translateDescription, generateBulletDescription } = require('../ai/translate-text');
const { getAllBlocks } = require('../blocks/library');
const {
  createProduct,
  setVariantsAndPricing,
  uploadImages,
  buildHorizonTemplate,
  buildClonedSectionAsset,
  pushSectionAsset,
  clonedSectionType,
  pushTemplate,
  publishProduct,
  getStoreConfig
} = require('../shopify/automation');
const { importReviews } = require('../reviews/loox-importer');
const { runPostCloneQa, formatQaReport } = require('../qa/post-clone-qa');
const { CostTracker } = require('../core/cost-tracker');

const router = express.Router();

// Test endpoint to verify API key works
router.get('/test-api', async (req, res) => {
  try {
    const { callClaude } = require('../ai/client');
    const result = await callClaude('You are helpful.', 'Say "API works" and nothing else.', { maxTokens: 20 });
    res.json({ success: true, response: result });
  } catch (e) {
    res.json({ success: false, error: e.message, stack: e.stack?.substring(0, 500) });
  }
});

const JOBS_DIR = path.join(__dirname, '../../data/jobs');

// In-memory job store
const jobs = {};

router.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    service: 'page-cloner',
    stores: ['movanella', 'merivalo']
  });
});

router.get('/status', (req, res) => {
  res.json({
    status: 'ok',
    service: 'page-cloner',
    activeJobs: Object.values(jobs).filter(j => !['done', 'failed'].includes(j.status)).length,
    totalJobs: Object.keys(jobs).length
  });
});

function updateJob(jobId, updates) {
  Object.assign(jobs[jobId], updates);
  const jobDir = jobs[jobId]._jobDir;
  if (!fs.existsSync(jobDir)) fs.mkdirSync(jobDir, { recursive: true });
  fs.writeFileSync(path.join(jobDir, 'job.json'), JSON.stringify(jobs[jobId], null, 2));
}

function normalizeLayoutMode(layoutMode) {
  return layoutMode === 'brand_pdp' ? 'brand_pdp' : 'source_clone';
}

function normalizeUrlList(urls, cap = 4) {
  if (!Array.isArray(urls)) return [];
  const out = [];
  const seen = new Set();
  for (const raw of urls) {
    const value = String(raw || '').trim();
    if (!/^https?:\/\//i.test(value)) continue;
    const key = value.replace(/[?#].*$/, '').toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(value);
    if (out.length >= cap) break;
  }
  return out;
}

function createCloneJob({ url, storeId = 'movanella', targetLanguage = null, layoutMode = 'source_clone', productUrl = null, researchUrls = [] }) {
  const store = ['movanella', 'merivalo'].includes(storeId) ? storeId : 'movanella';
  // Validate targetLanguage — must be a known language code or empty
  const lang = (targetLanguage && LANGUAGE_NAMES[targetLanguage]) ? targetLanguage : null;
  const mode = normalizeLayoutMode(layoutMode);
  const normalizedProductUrl = productUrl && /^https?:\/\//i.test(productUrl) ? productUrl : null;
  const normalizedResearchUrls = normalizeUrlList(researchUrls, 4);
  const jobId = uuidv4().substring(0, 8);
  const jobDir = path.join(JOBS_DIR, jobId);
  fs.mkdirSync(jobDir, { recursive: true });

  const steps = {
    scraping: { status: 'running' },
    generating: { status: 'pending' },
    creating: { status: 'pending' },
    ...(lang ? { translating: { status: 'pending' } } : {}),
    pushing: { status: 'pending' },
    publishing: { status: 'pending' },
    reviews: { status: 'pending' }
  };

  jobs[jobId] = {
    id: jobId,
    url,
    productUrl: normalizedProductUrl,
    researchUrls: normalizedResearchUrls,
    storeId: store,
    targetLanguage: lang,
    layoutMode: mode,
    status: 'scraping',
    progress: 0,
    steps,
    result: null,
    error: null,
    createdAt: new Date().toISOString(),
    _jobDir: jobDir
  };

  fs.writeFileSync(path.join(jobDir, 'job.json'), JSON.stringify(jobs[jobId], null, 2));

  runPipeline(jobId, url, jobDir, store, lang, {
    layoutMode: mode,
    productUrl: normalizedProductUrl,
    researchUrls: normalizedResearchUrls
  }).catch(err => {
    console.error(`Job ${jobId} failed:`, err);
    updateJob(jobId, { status: 'failed', error: err.message });
  });

  return { jobId, status: 'scraping', storeId: store, targetLanguage: lang, layoutMode: mode, productUrl: normalizedProductUrl, researchUrls: normalizedResearchUrls };
}

function normalizeImageUrl(src) {
  if (!src || typeof src !== 'string') return '';
  let out = src.trim().replace(/&amp;/g, '&');
  if (out.startsWith('//')) out = 'https:' + out;
  return out;
}

function imageDedupeKey(src) {
  const normalized = normalizeImageUrl(src);
  try {
    const u = new URL(normalized);
    return `${u.hostname}${u.pathname}`.toLowerCase();
  } catch (e) {
    return normalized.split('?')[0].toLowerCase();
  }
}

function extractImageUrlsFromLiquid(liquidContent) {
  if (!liquidContent) return [];
  const urls = [];
  const add = (value) => {
    const normalized = normalizeImageUrl(value);
    if (normalized && /^https?:\/\//i.test(normalized)) urls.push(normalized);
  };

  let match;
  const imgRe = /<img[^>]+src\s*=\s*["']([^"']+)["']/gi;
  while ((match = imgRe.exec(liquidContent)) !== null) add(match[1]);

  const bgRe = /url\(\s*["']?([^"')]+)["']?\s*\)/gi;
  while ((match = bgRe.exec(liquidContent)) !== null) add(match[1]);

  return urls;
}

// Split scraped images into two destinations:
//   * gallery: shows up as Shopify product images (visible in the product
//     card thumbnails). Only canonical product photos belong here — JSON-LD
//     `Product.image` entries plus the BUY-BOX product-media carousel.
//   * content: shows up only inline in body sections via the AI-generated
//     liquid. Uploaded as theme assets (CDN-hosted, no card pollution).
//     Includes everything else: comparison charts, expert headshots,
//     before/after photos, lifestyle shots, press logos, cross-promo for
//     other products.
//
// The split fixes the gallery-pollution we kept seeing on Solawave clones,
// where the LED face mask, doctor headshots, and before/after photos were
// landing in the product card.
// GALLERY-ELIGIBLE purposes: things a customer expects to see in a
// product-card carousel (multiple thumbnails of the wand, lifestyle
// shots, hero shots). The cloner pre-classifies every image into a
// purpose; here we use that to decide gallery vs content placement
// instead of relying on the source theme's CSS class names (which is
// what the old `sourceRole` check did, and which falsely tagged ALL
// of Solawave's gallery images as `page-image` because Solawave's
// theme uses non-standard class names).
const GALLERY_PURPOSES = new Set(['hero', 'product-only', 'lifestyle-with-person']);

function categorizeProductImages(productImages) {
  const all = (productImages || []).filter(Boolean);

  const gallery = [];
  const content = [];
  const seen = new Set();
  const GALLERY_CAP = 15;

  // First pass: JSON-LD product images are always canonical (the source
  // page itself declared these as the product's photos).
  for (const img of all) {
    if (typeof img === 'string') continue;
    if (img?.sourceRole !== 'product-structured-data') continue;
    const k = imageDedupeKey(img.src);
    if (!k || seen.has(k)) continue;
    seen.add(k);
    gallery.push(img);
  }

  // Second pass: source's own product-media-gallery container, if we
  // detected one (this is the OLD selector-based path — works on Shopify
  // themes that use product__media-style class names).
  for (const img of all) {
    if (typeof img === 'string') continue;
    if (img?.sourceRole !== 'product-media-gallery') continue;
    const k = imageDedupeKey(img.src);
    if (!k || seen.has(k)) continue;
    if (gallery.length < GALLERY_CAP) {
      seen.add(k);
      gallery.push(img);
    } else {
      seen.add(k);
      content.push(img);
    }
  }

  // Third pass — purpose-based fallback. When the source theme doesn't
  // expose a product-media container in a recognizable way (Solawave's
  // case), fall through to image-purpose classification: any image
  // tagged hero/product-only/lifestyle-with-person is gallery-eligible.
  // This is what makes a Solawave clone show 5+ thumbnails instead of 1.
  for (const img of all) {
    if (typeof img !== 'object' || !img) continue;
    if (!GALLERY_PURPOSES.has(img.purpose)) continue;
    const k = imageDedupeKey(img.src);
    if (!k || seen.has(k)) continue;
    if (gallery.length < GALLERY_CAP) {
      seen.add(k);
      gallery.push(img);
    } else {
      seen.add(k);
      content.push(img);
    }
  }

  // Final pass: everything else (unclassified, callout-with-text,
  // comparison-composite, etc.) is content — the AI body sections
  // reference these as theme assets, never as Shopify gallery images.
  for (const img of all) {
    if (typeof img === 'string') {
      const k = imageDedupeKey(img);
      if (!k || seen.has(k)) continue;
      seen.add(k);
      content.push({ src: img });
      continue;
    }
    const k = imageDedupeKey(img.src);
    if (!k || seen.has(k)) continue;
    seen.add(k);
    content.push(img);
  }

  return { gallery, content };
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function replaceImageUrlReferences(content, oldUrl, newUrl) {
  let out = content;
  let changed = 0;
  const normalizedOld = normalizeImageUrl(oldUrl);
  if (!normalizedOld || !newUrl) return { content: out, changed };

  const literalVariants = new Set([
    oldUrl,
    normalizedOld,
    normalizedOld.replace(/&/g, '&amp;')
  ]);

  if (normalizedOld.startsWith('https://')) {
    literalVariants.add(normalizedOld.replace(/^https:/, ''));
  }

  for (const variant of literalVariants) {
    if (!variant) continue;
    const before = out;
    out = out.split(variant).join(newUrl);
    if (out !== before) changed++;
  }

  // Replace any version of the same image URL that kept the same base path but
  // changed/encoded/dropped query params. This avoids leaving untranslated
  // cdn.shopify.com URLs behind, and avoids appending old source query strings
  // to the new Shopify CDN URL.
  try {
    const u = new URL(normalizedOld);
    const baseUrl = `${u.protocol}//${u.host}${u.pathname}`;
    const protoRelativeBase = baseUrl.replace(/^https:/, '');
    const patterns = [baseUrl, protoRelativeBase];
    for (const base of patterns) {
      const re = new RegExp(`${escapeRegExp(base)}(?:\\?[^"'<>\\s)]*)?`, 'g');
      const before = out;
      out = out.replace(re, newUrl);
      if (out !== before) changed++;
    }
  } catch (e) {}

  return { content: out, changed };
}

function deriveSourceBrandTerms(sourceUrl, explicit = []) {
  const terms = new Set((explicit || []).filter(Boolean).map(s => String(s).toLowerCase()));
  try {
    const host = new URL(sourceUrl).hostname.replace(/^www\./, '');
    const stem = host.split('.')[0].toLowerCase();
    if (stem.length >= 3) terms.add(stem);
    const compound = stem.match(/^([a-z]{3,})(health|sleep|shop|store|brand|life|home|skin|skincare|beauty)$/i);
    if (compound) terms.add(compound[1].toLowerCase());
  } catch (e) {}
  ['solawave', 'mellow', 'mellowsleep'].forEach(t => terms.add(t));
  return [...terms].filter(t => t.length >= 3);
}

function sanitizeLiquidBeforePublish(liquidContent, ctx = {}) {
  let out = liquidContent || '';
  const brandName = ctx.brandName || 'Movanella';
  const targetLanguage = ctx.targetLanguage || null;
  const sourceBrandTerms = ctx.sourceBrandTerms || [];

  // Remove/rewrite source-brand leftovers anywhere in the Liquid, including
  // image alt attributes created from source labels. This is deliberately
  // broad: visible copy, comments, and attributes should all be clean.
  for (const term of sourceBrandTerms) {
    if (!term || term.toLowerCase() === brandName.toLowerCase()) continue;
    const re = new RegExp(`\\b${escapeRegExp(term)}\\b`, 'gi');
    out = out.replace(re, brandName);
  }

  // Clean specific awkward mixed-language leftovers we keep seeing on German
  // skincare clones. Keep this tiny and deterministic; broader translation
  // remains the AI/copy pass' job.
  if (targetLanguage === 'de') {
    const replacements = [
      [/\bandere Wands\b/g, 'andere Geräte'],
      [/\bAndere Wands\b/g, 'Andere Geräte'],
      [/\bSkincare Wand\b/g, 'Hautpflege-Stab'],
      [/\bskincare wand\b/g, 'Hautpflege-Stab'],
      [/\bLight Therapy\b/g, 'Lichttherapie'],
      [/\bReal Results\b/g, 'Echte Ergebnisse'],
      [/\bDay 0\b/g, 'Tag 0'],
      [/\bDay 30\b/g, 'Tag 30']
    ];
    for (const [from, to] of replacements) {
      out = out.replace(from, to);
    }
  }

  return out;
}

function isOriginalFallbackAllowed(item, policy) {
  // Only permit originals for images explicitly marked safe by policy. Today
  // every regular source image goes through IP-safe rewrite, so default=false.
  if (item?.skipped) return false;
  return !!policy?.allowOriginalFallback;
}

function stripRejectedImageReferences(liquidContent, rejectedSourceUrls) {
  let out = liquidContent;
  let stripped = 0;
  for (const rejUrl of rejectedSourceUrls) {
    const escaped = rejUrl.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/&/g, '(?:&|&amp;)');
    const figureRe = new RegExp(`<figure\\b[^>]*>(?:(?!<\\/figure>)[\\s\\S])*?${escaped}(?:(?!<\\/figure>)[\\s\\S])*?<\\/figure>`, 'gi');
    out = out.replace(figureRe, () => { stripped++; return '<!-- image rejected: source brand or untranslatable -->'; });
    const pictureRe = new RegExp(`<picture\\b[^>]*>(?:(?!<\\/picture>)[\\s\\S])*?${escaped}(?:(?!<\\/picture>)[\\s\\S])*?<\\/picture>`, 'gi');
    out = out.replace(pictureRe, () => { stripped++; return '<!-- image rejected: source brand or untranslatable -->'; });
    const imgRe = new RegExp(`<img\\b[^>]*${escaped}[^>]*>`, 'gi');
    out = out.replace(imgRe, () => { stripped++; return '<!-- image rejected -->'; });
  }
  return { liquidContent: out, stripped };
}

function sameUrl(a, b) {
  try {
    const ua = new URL(a);
    const ub = new URL(b);
    return `${ua.hostname}${ua.pathname}`.toLowerCase() === `${ub.hostname}${ub.pathname}`.toLowerCase();
  } catch (e) {
    return false;
  }
}

function mergeImageLists(primaryImages = [], supplierImages = []) {
  const merged = [];
  const seen = new Set();
  const add = (img, sourcePageType = '') => {
    if (!img?.src) return;
    const key = imageDedupeKey(img.src);
    if (!key || seen.has(key)) return;
    seen.add(key);
    merged.push({
      ...img,
      sourceRole: img.sourceRole || 'image',
      sourcePageType
    });
  };
  // In Brand PDP mode the supplier/product link is the product truth, so its
  // images should lead the Shopify gallery. The competitor/reference images
  // still feed the content/research path.
  supplierImages.forEach(img => add(img, 'supplier'));
  primaryImages.forEach(img => add(img, 'reference'));
  return merged;
}

function mergeProductMetaForBrandPdp(referenceMeta, supplierMeta, productUrl, referenceUrl = null) {
  if (!supplierMeta) return referenceMeta;
  const merged = { ...referenceMeta };
  merged.title = referenceMeta.title || supplierMeta.title || 'Product';
  merged.price = referenceMeta.price || supplierMeta.price || '';
  merged.compareAtPrice = referenceMeta.compareAtPrice || supplierMeta.compareAtPrice || null;
  merged.currency = referenceMeta.currency || supplierMeta.currency || 'USD';
  merged.description = [
    referenceMeta.description ? `Reference page: ${referenceMeta.description}` : '',
    supplierMeta.description ? `Supplier/product page: ${supplierMeta.description}` : ''
  ].filter(Boolean).join('\n\n') || referenceMeta.description || supplierMeta.description || '';
  merged.variants = (supplierMeta.variants && supplierMeta.variants.length > 1)
    ? supplierMeta.variants
    : (referenceMeta.variants || supplierMeta.variants || []);
  merged.images = mergeImageLists(referenceMeta.images || [], supplierMeta.images || []);
  merged.productResearch = {
    mode: 'brand_pdp',
    productUrl,
    referenceUrl,
    referenceMeta,
    supplierMeta
  };
  return merged;
}

// POST /api/jobs - Start a clone job
router.post('/jobs', (req, res) => {
  const { url, storeId, targetLanguage, layoutMode, productUrl, researchUrls } = req.body;
  if (!url) return res.status(400).json({ error: 'URL is required' });

  res.json(createCloneJob({ url, storeId, targetLanguage, layoutMode, productUrl, researchUrls }));
});

// Clone-only endpoints for dashboards that should bypass product research.
router.post('/clone/movanella', (req, res) => {
  const { url } = req.body;
  if (!url) return res.status(400).json({ error: 'URL is required' });

  res.json(createCloneJob({ url, storeId: 'movanella', targetLanguage: null }));
});

router.post('/clone/merivalo', (req, res) => {
  const { url, targetLanguage } = req.body;
  if (!url) return res.status(400).json({ error: 'URL is required' });

  res.json(createCloneJob({ url, storeId: 'merivalo', targetLanguage: targetLanguage || 'de' }));
});

// GET /api/jobs - List all jobs
router.get('/jobs', (req, res) => {
  const jobList = Object.values(jobs).map(j => ({
    id: j.id,
    url: j.url,
    productUrl: j.productUrl,
    researchUrls: j.researchUrls,
    storeId: j.storeId,
    targetLanguage: j.targetLanguage,
    layoutMode: j.layoutMode,
    status: j.status,
    progress: j.progress,
    createdAt: j.createdAt,
    error: j.error
  }));
  jobList.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  res.json(jobList);
});

router.get('/jobs/:id', (req, res) => {
  const job = jobs[req.params.id];
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json(job);
});

router.get('/jobs/:id/screenshot', (req, res) => {
  const job = jobs[req.params.id];
  if (!job) return res.status(404).json({ error: 'Job not found' });
  const screenshotPath = path.join(job._jobDir, 'screenshot.png');
  if (!fs.existsSync(screenshotPath)) return res.status(404).json({ error: 'No screenshot' });
  res.sendFile(screenshotPath);
});

router.get('/jobs/:id/output', (req, res) => {
  const job = jobs[req.params.id];
  if (!job || !job.result) return res.status(404).json({ error: 'No output yet' });
  const outputPath = job.result.outputPath;
  if (!fs.existsSync(outputPath)) return res.status(404).json({ error: 'Output file missing' });
  res.setHeader('Content-Type', 'text/plain');
  res.sendFile(outputPath);
});

router.get('/jobs/:id/liquid', (req, res) => {
  const job = jobs[req.params.id];
  if (!job || !job.result) return res.status(404).json({ error: 'No output yet' });
  res.json({ content: job.result.content, filename: job.result.filename });
});

router.get('/blocks', (req, res) => {
  res.json(getAllBlocks());
});

// GET /api/jobs/:id/cost — cost summary for a single clone job.
// Returns { runId, totalUsd, breakdown, summary } once the pipeline has
// finished its Step 3b (image translation). Before that, returns 202 so
// the UI can poll.
router.get('/jobs/:id/cost', (req, res) => {
  const job = jobs[req.params.id];
  if (!job) return res.status(404).json({ error: 'Job not found' });
  const cost = job.result && job.result.cost;
  if (!cost) return res.status(202).json({ pending: true });
  res.json(cost);
});

// GET /api/costs/recent — slurp the tail of cost-log.jsonl so an admin
// dashboard can show "last N clone runs and what they cost" without
// walking the Sheet. Limit defaults to 200.
router.get('/costs/recent', (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 200, 2000);
  const logPath = path.join(__dirname, '../../data/cost-log.jsonl');
  if (!fs.existsSync(logPath)) return res.json({ records: [] });
  try {
    const lines = fs.readFileSync(logPath, 'utf-8').trim().split('\n').filter(Boolean);
    const tail = lines.slice(-limit).map(l => {
      try { return JSON.parse(l); } catch (e) { return null; }
    }).filter(Boolean);
    res.json({ records: tail });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/**
 * Main pipeline: scrape → generate → create → push → publish
 * Fully automated — no manual steps needed.
 */
async function runPipeline(jobId, url, jobDir, storeId = 'movanella', targetLanguage = null, options = {}) {
  let browser;
  const layoutMode = normalizeLayoutMode(options.layoutMode);
  const productUrl = options.productUrl || null;
  const researchUrls = normalizeUrlList(options.researchUrls || [], 4);

  // Cost tracker — one per clone job. Records fal.ai image translations
  // (the biggest cost driver) today; Claude text calls will be wired in
  // follow-up work. Summary is attached to the job object so the UI can
  // display it; full record list is appended to data/cost-log.jsonl for
  // platform-side sync.
  const costTracker = new CostTracker({
    runId: `clone_${jobId}`,
    runType: 'page_clone',
  });

  try {
    // ── Step 1: Scrape ──
    console.log(`[${jobId}] Step 1: Scraping ${url} (store: ${storeId}, layout: ${layoutMode})...`);
    updateJob(jobId, { status: 'scraping', progress: 5 });

    const { page, browser: br, screenshotPath } = await scrapePage(url, jobDir);
    browser = br;

    updateJob(jobId, { progress: 10 });

    // Extract DOM sections
    console.log(`[${jobId}] Extracting DOM sections...`);
    let sections = await extractSections(page);
    sections = analyzeImages(sections);
    console.log(`[${jobId}]   Found ${sections.length} sections`);

    // Extract product metadata
    console.log(`[${jobId}] Extracting product metadata...`);
    let productMeta = await extractProductMeta(page);
    console.log(`[${jobId}]   Title: ${productMeta.title}`);
    console.log(`[${jobId}]   Price: ${productMeta.price}`);
    console.log(`[${jobId}]   Images: ${productMeta.images.length}`);
    console.log(`[${jobId}]   Variants: ${productMeta.variants.length}`);

    await browser.close().catch(() => {});
    browser = null;

    let supplierMeta = null;
    let supplierSections = [];
    let supplierScreenshotPath = null;
    const extraResearch = [];
    if (layoutMode === 'brand_pdp' && productUrl && !sameUrl(productUrl, url)) {
      let supplierBrowser;
      const supplierDir = path.join(jobDir, 'product-source');
      fs.mkdirSync(supplierDir, { recursive: true });
      try {
        console.log(`[${jobId}] Brand PDP mode: scraping product/supplier URL ${productUrl}...`);
        updateJob(jobId, { progress: 14 });
        const supplierScrape = await scrapePage(productUrl, supplierDir);
        supplierBrowser = supplierScrape.browser;
        supplierScreenshotPath = supplierScrape.screenshotPath;
        supplierSections = analyzeImages(await extractSections(supplierScrape.page));
        supplierMeta = await extractProductMeta(supplierScrape.page);
        await supplierBrowser.close().catch(() => {});
        supplierBrowser = null;
        console.log(`[${jobId}]   Supplier title: ${supplierMeta.title}`);
        console.log(`[${jobId}]   Supplier images: ${supplierMeta.images.length}`);
        productMeta = mergeProductMetaForBrandPdp(productMeta, supplierMeta, productUrl, url);
      } catch (supplierErr) {
        if (supplierBrowser) await supplierBrowser.close().catch(() => {});
        console.warn(`[${jobId}]   Product/supplier scrape failed (continuing with reference page only): ${supplierErr.message}`);
      }
    } else if (layoutMode === 'brand_pdp') {
      productMeta.productResearch = {
        mode: 'brand_pdp',
        productUrl: productUrl || url,
        referenceUrl: url,
        referenceMeta: productMeta,
        supplierMeta: null
      };
    }

    if (layoutMode === 'brand_pdp' && researchUrls.length) {
      for (let i = 0; i < researchUrls.length; i++) {
        const researchUrl = researchUrls[i];
        if (sameUrl(researchUrl, url) || (productUrl && sameUrl(researchUrl, productUrl))) continue;
        let researchBrowser;
        const researchDir = path.join(jobDir, `research-${i + 1}`);
        fs.mkdirSync(researchDir, { recursive: true });
        try {
          console.log(`[${jobId}] Brand PDP mode: scraping extra research URL ${researchUrl}...`);
          updateJob(jobId, { progress: Math.min(18, 15 + i) });
          const researchScrape = await scrapePage(researchUrl, researchDir);
          researchBrowser = researchScrape.browser;
          const researchSections = analyzeImages(await extractSections(researchScrape.page));
          const researchMeta = await extractProductMeta(researchScrape.page);
          await researchBrowser.close().catch(() => {});
          researchBrowser = null;
          extraResearch.push({
            url: researchUrl,
            meta: researchMeta,
            sections: researchSections
          });
          console.log(`[${jobId}]   Research page: ${researchMeta.title || '(untitled)'} (${researchSections.length} section(s))`);
        } catch (researchErr) {
          if (researchBrowser) await researchBrowser.close().catch(() => {});
          console.warn(`[${jobId}]   Extra research scrape failed (${researchUrl}): ${researchErr.message}`);
        }
      }
    }

    // Perceptual-hash dedup — drops near-duplicate gallery images that
    // slipped past the URL-based dedup in the scraper (e.g. the same
    // photo at two different CDN paths, or two nearly-identical product
    // shots). Runs after browser close so we're not holding open a page.
    try {
      const before = productMeta.images.length;
      productMeta.images = await dedupeByPerceptualHash(productMeta.images, {
        log: (msg) => console.log(`[${jobId}]${msg}`)
      });
      if (productMeta.images.length !== before) {
        console.log(`[${jobId}]   Images after perceptual dedup: ${productMeta.images.length}`);
      }
    } catch (e) {
      console.warn(`[${jobId}]   Perceptual dedup failed (keeping all ${productMeta.images.length} images): ${e.message}`);
    }

    // ── Image classification (purpose) ──
    // Classify every image so the translate step can apply the right transform
    // (face-swap on lifestyle photos, freeze-layout on comparison composites,
    // skip-and-drop on press-logo strips). Also drops logo-strip images from
    // productMeta.images BEFORE liquid generation so the AI never references
    // them in the rendered page.
    const classificationInput = [];
    for (const section of sections) {
      const sectionHeadings = (section.headings || []).map(h => (typeof h === 'string' ? h : h.text || ''));
      for (const img of (section.images || [])) {
        classificationInput.push({
          src: img.src,
          alt: img.alt,
          displayWidth: img.displayWidth,
          displayHeight: img.displayHeight,
          naturalWidth: img.naturalWidth,
          naturalHeight: img.naturalHeight,
          sectionHeadings
        });
      }
    }
    for (const img of productMeta.images) {
      classificationInput.push({
        src: img.src,
        alt: img.alt,
        displayWidth: img.displayWidth,
        displayHeight: img.displayHeight,
        naturalWidth: img.naturalWidth,
        naturalHeight: img.naturalHeight,
        sectionHeadings: []
      });
    }
    const classifications = classifyImagePurposes(classificationInput);
    const policiesByUrl = new Map();
    for (const [src, purpose] of classifications) {
      policiesByUrl.set(src, { ...policyFor(purpose), purpose });
    }
    const sourceBrandTerms = deriveSourceBrandTerms(url, [
      productUrl,
      ...researchUrls
    ].filter(Boolean).flatMap(u => {
      try {
        const host = new URL(u).hostname.replace(/^www\./, '');
        return [host.split('.')[0]];
      } catch (e) {
        return [];
      }
    }));
    console.log(`[${jobId}]   Source-brand terms blocked in copy/images: ${sourceBrandTerms.join(', ') || 'none'}`);

    const droppedFromOutput = [];
    productMeta.images = productMeta.images.filter(img => {
      const p = policiesByUrl.get(img.src);
      if (p?.dropFromOutput) {
        droppedFromOutput.push({ src: img.src, purpose: p.purpose });
        return false;
      }
      return true;
    });
    // Also strip logo-strip images from each section's image list so the AI's
    // SOURCE SECTION BLUEPRINT doesn't surface them either. Tag survivors
    // with purpose so downstream consumers can read it without re-classifying.
    for (const section of sections) {
      const filtered = [];
      for (const img of (section.images || [])) {
        const p = policiesByUrl.get(img.src);
        if (p?.dropFromOutput) continue;
        if (p) img.purpose = p.purpose;
        filtered.push(img);
      }
      section.images = filtered;
    }

    const purposeCounts = {};
    for (const purpose of classifications.values()) {
      purposeCounts[purpose] = (purposeCounts[purpose] || 0) + 1;
    }
    console.log(`[${jobId}]   Image purpose breakdown: ${JSON.stringify(purposeCounts)} (${droppedFromOutput.length} dropped as logo-strip)`);

    fs.writeFileSync(path.join(jobDir, 'image-classification.json'), JSON.stringify({
      purposeCounts,
      droppedFromOutput,
      perImage: Array.from(classifications.entries()).map(([src, purpose]) => ({ src, purpose }))
    }, null, 2));

    // Derive handle from URL, make unique if already exists
    let handle = new URL(url).pathname.split('/').filter(Boolean).pop() || 'product';
    try {
      const { restApi } = require('../shopify/automation');
      const existing = await restApi('GET', `/products.json?handle=${handle}`, null, storeId);
      if (existing.products && existing.products.length > 0) {
        // Handle exists, append suffix to make unique
        let suffix = 2;
        while (true) {
          const check = await restApi('GET', `/products.json?handle=${handle}-${suffix}`, null, storeId);
          if (!check.products || check.products.length === 0) break;
          suffix++;
        }
        console.log(`[${jobId}]   Handle "${handle}" exists, using "${handle}-${suffix}"`);
        handle = `${handle}-${suffix}`;
      }
    } catch (e) {
      // If check fails, just proceed with original handle
    }
    productMeta.handle = handle;

    // Save extracted data
    fs.writeFileSync(path.join(jobDir, 'sections.json'), JSON.stringify(sections, null, 2));
    if (supplierSections.length || supplierMeta) {
      fs.writeFileSync(path.join(jobDir, 'product-source-sections.json'), JSON.stringify(supplierSections, null, 2));
      fs.writeFileSync(path.join(jobDir, 'product-source-meta.json'), JSON.stringify(supplierMeta, null, 2));
    }
    if (extraResearch.length) {
      fs.writeFileSync(path.join(jobDir, 'extra-research.json'), JSON.stringify(extraResearch.map(item => ({
        url: item.url,
        meta: item.meta,
        sections: item.sections
      })), null, 2));
    }
    fs.writeFileSync(path.join(jobDir, 'product-meta.json'), JSON.stringify(productMeta, null, 2));

    updateJob(jobId, {
      progress: 20,
      steps: { ...jobs[jobId].steps, scraping: { status: 'done' }, generating: { status: 'running' } }
    });

    // ── Step 2: Generate liquid content ──
    console.log(`[${jobId}] Step 2: AI generating full liquid content (lang: ${targetLanguage || 'store default'})...`);
    let liquidContent = await generateFullLiquid(productMeta, sections, screenshotPath, storeId, targetLanguage, {
      layoutMode,
      productUrl,
      supplierMeta,
      supplierSections,
      extraResearch,
      supplierScreenshotPath
    });
    console.log(`[${jobId}]   Generated ${liquidContent.length} chars of liquid`);

    // Save liquid file
    const liquidFilename = `${handle}-content.liquid`;
    const liquidPath = path.join(jobDir, liquidFilename);
    fs.writeFileSync(liquidPath, liquidContent);

    updateJob(jobId, {
      progress: 50,
      steps: { ...jobs[jobId].steps, generating: { status: 'done' }, creating: { status: 'running' } }
    });

    // ── Step 3: Create product in Shopify ──
    console.log(`[${jobId}] Step 3: Creating product in Shopify...`);

    // Translate title if targetLanguage is set, then generate the product-card
    // description as a stars-header + bullet-benefit list (matches the Merivalo
    // cloud-alignment-pillow reference). The old flow just translated source
    // prose into prose, which looked like a wall of text on the collection card.
    //
    // Accent color used for the stars + checkmark bullets is taken from the
    // store config so Merivalo renders coral and Movanella renders green.
    const storeConfigForText = getStoreConfig(storeId);
    const brandForText = storeConfigForText.storeName || storeId;
    const accentForText = storeConfigForText.accentColor
      || (storeId === 'merivalo' ? '#e8845f' : '#07941a');
    const descLang = targetLanguage || (storeConfigForText.language || 'en');

    let translatedTitle = productMeta.title;
    const sourceDesc = productMeta.description || '';

    if (targetLanguage) {
      try {
        console.log(`[${jobId}]   Translating title → ${LANGUAGE_NAMES[targetLanguage]} (brand: ${brandForText})...`);
        translatedTitle = await translateTitle(productMeta.title, targetLanguage, brandForText);
        console.log(`[${jobId}]     Title: "${productMeta.title}" → "${translatedTitle}"`);
      } catch (e) {
        console.warn(`[${jobId}]   Title translation failed (using source): ${e.message}`);
      }
    }
    productMeta.title = translatedTitle;

    // Build bullet-format description (stars header + 4-6 benefit bullets).
    // Falls back to the legacy prose translator only if the bullet generator
    // throws — so we never lose the description entirely.
    let description;
    try {
      console.log(`[${jobId}]   Generating bullet description (${descLang}, accent ${accentForText})...`);
      description = await generateBulletDescription({
        sourceDescription: sourceDesc,
        productTitle: translatedTitle,
        targetLanguage: descLang,
        brandName: brandForText,
        accentColor: accentForText
      });
    } catch (e) {
      console.warn(`[${jobId}]   Bullet description failed, falling back to prose: ${e.message}`);
      let fallback = sourceDesc ? sourceDesc.substring(0, 500) : translatedTitle;
      if (targetLanguage && fallback) {
        try {
          fallback = await translateDescription(`<p>${fallback}</p>`, targetLanguage, brandForText);
        } catch (err) { /* keep source */ }
      }
      description = fallback.startsWith('<') ? fallback : `<p>${fallback}</p>`;
    }

    // Legacy `rawDescription` is still consumed below by the post-clone QA
    // (which checks description length). Feed it the rendered HTML so the
    // length check reflects what actually ships.
    const rawDescription = description;

    // Prepare variants.
    //
    // compare_at_price fallback: if the source page has no sale price, we
    // synthesize one at roughly 2× the selling price. This is what makes the
    // Rapi Bundle "Einzeln" (single) row render the crossed-out regular price
    // next to the selling price — matching the Merivalo cloud-alignment-pillow
    // reference (€49 / €99). Without a compare_at_price, Rapi only shows the
    // current price, which looks empty next to the Duo/Trio "Du sparst 10%"
    // strikethroughs.
    const cleanPrice = (p) => (p || '').toString().replace(/[^0-9.]/g, '');
    const synthesizeCompareAt = (p) => {
      const n = parseFloat(cleanPrice(p));
      if (!Number.isFinite(n) || n <= 0) return null;
      // 2× price, rounded up to the next whole unit, then -1 cent so the
      // strikethrough lands on a .99 boundary (the convention across both
      // Merivalo and Movanella bundles — e.g. €49.99 → €99.99, not €100.00).
      // Rapi will multiply this by qty for Duo/Trio tiers, so you get
      // ~€99.99 / ~€199.98 / ~€299.97 on the bundle strikethroughs.
      return (Math.ceil(n * 2) - 0.01).toFixed(2);
    };
    const sourceCompareAt = cleanPrice(productMeta.compareAtPrice);
    let variants;
    if (productMeta.variants && productMeta.variants.length > 1) {
      variants = productMeta.variants.map(v => {
        const price = cleanPrice(v.price || productMeta.price || '29.99');
        const compareAt = sourceCompareAt || synthesizeCompareAt(price);
        return {
          option1: v.name || v.value || 'Default',
          optionName: v.optionName || productMeta.optionName || 'Size',
          price: price || '29.99',
          compareAtPrice: compareAt
        };
      });
    } else {
      const price = cleanPrice(productMeta.price || '29.99') || '29.99';
      const compareAt = sourceCompareAt || synthesizeCompareAt(price);
      variants = [{
        option1: 'Default',
        price,
        compareAtPrice: compareAt
      }];
    }
    if (!sourceCompareAt && variants[0]?.compareAtPrice) {
      console.log(`[${jobId}]   Source has no sale price — synthesized compare_at_price ${variants[0].compareAtPrice} (2× ${variants[0].price})`);
    }

    // Check if variants need to be split into multiple options (e.g. "Blue / S" → Color: Blue, Size: S)
    const hasSlash = variants.length > 1 && variants.some(v => (v.option1 || '').includes(' / '));

    if (hasSlash) {
      // Create product directly via REST with multi-option support
      console.log(`[${jobId}]   Detected multi-option variants, creating with Color + Size...`);
      const { restApi } = require('../shopify/automation');

      const parts = variants.map(v => {
        const split = (v.option1 || '').split(' / ').map(s => s.trim());
        return { color: split[0] || 'Default', size: split[1] || 'Default' };
      });

      const sizePattern = /^(XS|S|M|L|XL|XXL|2XL|3XL|\d+)$/i;
      const sizes = [...new Set(parts.map(p => p.size))];
      const opt2IsSize = sizes.some(v => sizePattern.test(v));
      const option1Name = opt2IsSize ? 'Color' : 'Style';
      const option2Name = opt2IsSize ? 'Size' : 'Option';

      const result = await restApi('POST', '/products.json', {
        product: {
          title: productMeta.title || handle,
          handle,
          template_suffix: handle,
          body_html: description,
          status: 'draft',
          options: [{ name: option1Name }, { name: option2Name }],
          variants: variants.map((v, i) => ({
            option1: parts[i].color,
            option2: parts[i].size,
            price: v.price || '29.99',
            compare_at_price: v.compareAtPrice || null,
            inventory_management: null,
            inventory_policy: 'continue'
          }))
        }
      }, storeId);

      if (result.errors) {
        console.error(`[${jobId}]   Multi-option creation failed:`, JSON.stringify(result.errors).substring(0, 200));
        // Fallback: create without options, then set single-option variants
        await createProduct(handle, productMeta.title || handle, description, handle, storeId);
        await setVariantsAndPricing(handle, variants, storeId);
      } else {
        const colors = [...new Set(parts.map(p => p.color))];
        console.log(`[${jobId}]   Created with ${option1Name}(${colors.length}) × ${option2Name}(${sizes.length}) = ${result.product.variants.length} variants`);
      }
    } else {
      // Standard single-option creation
      await createProduct(handle, productMeta.title || handle, description, handle, storeId);
      await setVariantsAndPricing(handle, variants, storeId);
    }

    // ── Step 3b: Translate + upload images on TWO paths ──
    //
    // Gallery path  → POST /products/{id}/images.json (visible in product card)
    // Content path  → PUT  /themes/{id}/assets.json   (CDN-hosted, NOT in card)
    //
    // Why split: dumping every scraped image into the product gallery polluted
    // the Shopify card with cross-promo (LED face mask), expert headshots,
    // before/after photos, and lifestyle shots. Those belong inline in body
    // sections, not in the carousel thumbnails. categorizeProductImages keeps
    // only canonical product photos in the gallery — JSON-LD product images
    // plus a capped fallback of buy-box product-media. Everything else gets
    // CDN-hosted as a theme asset, the AI references it inline, and the
    // residue sweep rewrites Solawave URLs to Shopify CDN URLs.
    const { gallery: galleryImagesObjs, content: contentImagesObjs } =
      categorizeProductImages(productMeta.images);

    // Pull URLs the AI actually referenced in the generated liquid into the
    // content set as well (some are page-images already in contentImagesObjs,
    // others may be liquid-only refs the scraper didn't pick up). These get
    // FIRST priority because any one of them can leak the source domain/brand
    // directly into the published page.
    const liquidUrls = extractImageUrlsFromLiquid(liquidContent);
    const gallerySeen = new Set(galleryImagesObjs.map(i => imageDedupeKey(i.src)).filter(Boolean));
    const contentSeen = new Set(gallerySeen);
    const liquidContentImagesObjs = [];
    for (const url2 of liquidUrls) {
      const norm = normalizeImageUrl(url2);
      if (!norm || !/^https?:\/\//i.test(norm)) continue;
      const k = imageDedupeKey(norm);
      if (!k || contentSeen.has(k)) continue;
      contentSeen.add(k);
      liquidContentImagesObjs.push({ src: norm, sourceRole: 'liquid-referenced-image' });
    }

    const remainingContentImagesObjs = [];
    for (const img of contentImagesObjs) {
      const k = imageDedupeKey(img?.src);
      if (!k || contentSeen.has(k)) continue;
      contentSeen.add(k);
      remainingContentImagesObjs.push(img);
    }

    const galleryUrls = galleryImagesObjs.map(i => i.src).filter(Boolean).slice(0, 15);
    const CONTENT_IMAGE_CAP = 60;
    const prioritizedContentUrls = [
      ...liquidContentImagesObjs.map(i => i.src).filter(Boolean),
      ...remainingContentImagesObjs.map(i => i.src).filter(Boolean)
    ];
    // Never cap out URLs that are already present in the Liquid. The cap only
    // trims opportunistic extra content images that are not referenced.
    const contentUrls = prioritizedContentUrls.slice(0, Math.max(CONTENT_IMAGE_CAP, liquidContentImagesObjs.length));
    const galleryCount = galleryUrls.length;
    const allImageUrls = [...galleryUrls, ...contentUrls];
    console.log(`[${jobId}]   Image plan: ${galleryCount} gallery (product card) + ${contentUrls.length} content (theme assets) = ${allImageUrls.length} total`);

    // Build `urlMap` (sourceUrl → Shopify CDN URL) so we can rewrite the
    // AI-generated liquid below, pointing it at the translated images we just
    // uploaded rather than the untranslated source URLs.
    const urlMap = {}; // sourceUrl → newShopifyCdnUrl

    const storeConfigForImgs = getStoreConfig(storeId);
    const brandName = storeConfigForImgs.storeName || storeId;
    let falApiKey = process.env.FAL_API_KEY || process.env.FAL_KEY || '';
    if (!falApiKey) {
      try {
        const envContent = require('fs').readFileSync(require('path').join(__dirname, '../../.env'), 'utf-8');
        const falKeyMatch = envContent.match(/^FAL_API_KEY=(.+)$/m) || envContent.match(/^FAL_KEY=(.+)$/m);
        falApiKey = falKeyMatch ? falKeyMatch[1].trim() : '';
      } catch (e) {
        falApiKey = '';
      }
    }

    // Track which source URLs the translate step rejected (failed both
    // nano-banana and Imagen QA) so we can strip the corresponding <img>
    // tags from the AI's liquid below — preventing untranslated source
    // assets from ending up on the live page. Must be declared OUTSIDE
    // the `if (allImageUrls.length > 0)` block since the post-process
    // that consumes it runs unconditionally.
    const rejectedSourceUrls = new Set();

    if (allImageUrls.length > 0) {
      if (targetLanguage) {
        updateJob(jobId, {
          progress: 55,
          steps: { ...jobs[jobId].steps, creating: { status: 'done' }, translating: { status: 'running' } }
        });
      }

      const mode = targetLanguage ? targetLanguage : 'same';
      const modeLabel = targetLanguage
        ? `translate → ${LANGUAGE_NAMES[targetLanguage]} + brand → ${brandName}`
        : `brand-only → ${brandName}`;

      // Resolve Google Nano Banana / Imagen API key. The page cloner used to
      // silently upload originals when FAL_API_KEY was absent; the broader
      // platform also supports Nano Banana Pro through GEMINI_API_KEY, so use
      // that path as a fallback instead of skipping image safety edits. When
      // FAL_API_KEY is present, the primary editor is GPT Image 2 on fal.ai.
      let googleApiKey = process.env.GOOGLE_IMAGEN_API_KEY || process.env.GEMINI_API_KEY || '';
      if (!googleApiKey) {
        try {
          const envContent = require('fs').readFileSync(require('path').join(__dirname, '../../.env'), 'utf-8');
          const m = envContent.match(/^GOOGLE_IMAGEN_API_KEY=(.+)$/m) || envContent.match(/^GEMINI_API_KEY=(.+)$/m);
          googleApiKey = m ? m[1].trim() : '';
        } catch (e) {
          googleApiKey = '';
        }
      }

      // Translate everything in one pass so we batch through Nano Banana once.
      // Items below `galleryCount` go to product gallery, the rest to theme
      // assets. translateProductImages returns { originalUrl, buffer? } per
      // input — when there's no buffer it means translation was skipped/failed
      // and we fall back to the original URL.
      let translated;
      if (!falApiKey && !googleApiKey) {
        console.warn(`[${jobId}] No FAL_API_KEY or GEMINI_API_KEY found — rejecting source images instead of uploading originals`);
        translated = allImageUrls.map(u => ({ originalUrl: u, buffer: null, rejected: true, reason: 'no-image-editor-key' }));
      } else {
        // Generate one reference face per job IF any image needs face-swap.
        // Same face is reused across every face-swap call, so the cloned
        // page shows one consistent model instead of random AI faces.
        let jobFaceUrl = null;
        const faceSwapCount = allImageUrls.filter(u => policiesByUrl.get(u)?.faceSwap).length;
        if (faceSwapCount > 0 && falApiKey) {
          try {
            jobFaceUrl = await generateJobFace(falApiKey, { costTracker });
            console.log(`[${jobId}]   Job face generated for ${faceSwapCount} face-swap image(s)`);
          } catch (e) {
            console.warn(`[${jobId}]   Job face generation failed (${e.message?.substring(0, 100)}) — face-swap will use prompt-only fallback`);
          }
        } else if (faceSwapCount > 0) {
          console.log(`[${jobId}]   ${faceSwapCount} image(s) need person replacement; using Gemini Nano Banana Pro prompt-only identity rewrite`);
        }
        if (!googleApiKey) {
          console.log(`[${jobId}]   No GEMINI_API_KEY / GOOGLE_IMAGEN_API_KEY set — Imagen fallback disabled`);
        }

        console.log(`[${jobId}] Step 3b: Processing ${allImageUrls.length} images (${modeLabel})...`);
        translated = await translateProductImages(
          allImageUrls,
          mode,
          brandName,
          falApiKey,
          (i, total) => {
            if (targetLanguage) {
              const pct = 55 + Math.round((i / total) * 12);
              updateJob(jobId, { progress: pct });
            }
          },
          {
            costTracker,
            policies: policiesByUrl,
            faceRefUrl: jobFaceUrl,
            googleApiKey,
            sourceBrandNames: sourceBrandTerms
          }
        );
      }

      const { restApi } = require('../shopify/automation');
      const products = await restApi('GET', `/products.json?handle=${handle}`, null, storeId);
      const productId = products.products?.[0]?.id;
      const themeId = storeConfigForImgs.themeId;

      const safeHandle = handle.replace(/[^a-z0-9-]+/g, '-').replace(/-+/g, '-').slice(0, 40);
      const guessExt = (src) => {
        try {
          const u = new URL(src);
          const m = u.pathname.match(/\.([a-z0-9]{2,5})$/i);
          if (m) return m[1].toLowerCase().replace('jpeg', 'jpg');
        } catch (e) {}
        return 'jpg';
      };

      let safeGalleryUploads = 0;
      for (let i = 0; i < translated.length; i++) {
        const item = translated[i];
        const isGallery = i < galleryCount;

        // Strict-drop on rejected: do not upload the source asset (which still
        // shows English text + source-brand watermark). Track for HTML cleanup
        // below so the source URL doesn't leak into the live page either.
        if (item.rejected) {
          console.log(`  [Shopify] Rejected image ${i + 1}/${translated.length}: ${item.reason || 'qa-fail'} — skipping upload`);
          rejectedSourceUrls.add(item.originalUrl);
          continue;
        }
        // Skipped (e.g. before-after composite handled by post-processor):
        // leave the source URL in place — the post-processor will rewrite it.
        if (item.skipped) {
          continue;
        }

        try {
          if (isGallery && productId) {
            // Gallery: visible in product card
            let res;
            if (item.buffer) {
              res = await restApi('POST', `/products/${productId}/images.json`, {
                image: {
                  attachment: item.buffer.toString('base64'),
                  filename: `gallery-${i + 1}.jpg`,
                  position: i + 1
                }
              }, storeId);
            } else if (isOriginalFallbackAllowed(item, policiesByUrl.get(item.originalUrl))) {
              res = await restApi('POST', `/products/${productId}/images.json`, {
                image: { src: item.originalUrl, position: i + 1 }
              }, storeId);
            } else {
              console.warn(`  [Shopify] Gallery image ${i + 1} has no safe edited buffer — rejecting original fallback`);
              rejectedSourceUrls.add(item.originalUrl);
              continue;
            }
            if (res?.image?.src) urlMap[item.originalUrl] = res.image.src;
            safeGalleryUploads++;
            console.log(`  [Shopify] Gallery ${i + 1}/${galleryCount}`);
          } else if (themeId) {
            // Content: CDN-hosted via theme asset, NOT visible in product card.
            // Prefer the edited buffer. Do not silently upload source originals
            // for risky assets; that is how source-brand/product images leaked
            // into live cloned PDPs.
            let buffer = item.buffer;
            if (!buffer && isOriginalFallbackAllowed(item, policiesByUrl.get(item.originalUrl))) {
              try {
                buffer = await fetchImageBuffer(item.originalUrl);
                console.log(`  [Shopify] Translation skipped safe-pass image ${i + 1}, uploading original buffer`);
              } catch (fetchErr) {
                console.warn(`  [Shopify] Could not fetch original for ${item.originalUrl}: ${fetchErr.message?.substring(0, 100)}`);
              }
            }
            if (!buffer) {
              console.warn(`  [Shopify] Content image ${i + 1} has no safe edited buffer — rejecting original fallback`);
              rejectedSourceUrls.add(item.originalUrl);
              continue;
            }
            const ext = guessExt(item.originalUrl);
            const seq = i - galleryCount + 1;
            const assetKey = `assets/cloned-${safeHandle}-${seq}.${ext}`;
            const res = await restApi('PUT', `/themes/${themeId}/assets.json`, {
              asset: { key: assetKey, attachment: buffer.toString('base64') }
            }, storeId);
            const newUrl = res?.asset?.public_url || res?.asset?.src || null;
            if (newUrl) {
              urlMap[item.originalUrl] = newUrl;
              console.log(`  [Shopify] Content ${seq}/${contentUrls.length} → ${assetKey}${item.buffer ? '' : ' (untranslated fallback)'}`);
            } else {
              console.warn(`  [Shopify] Content ${seq} returned no public_url`);
            }
          }
        } catch (imgErr) {
          const where = isGallery ? 'gallery' : 'content';
          console.warn(`  [Shopify] ${where} image ${i + 1} upload failed: ${imgErr.message?.substring(0, 120)}`);
        }
      }
      if (galleryCount > 0 && safeGalleryUploads === 0) {
        throw new Error('All gallery images failed IP-safe editing. Refusing to publish a product with original/source-branded gallery images.');
      }
    }

    // Strip rejected images from the liquid before any URL rewriting so we
    // don't leak source-branded photos to the live page. Rejected = both
    // nano-banana and Imagen failed to produce an acceptable translation;
    // shipping the source asset would put English text and the source brand
    // wordmark on a Movanella page, which is what we're explicitly avoiding.
    if (rejectedSourceUrls && rejectedSourceUrls.size > 0) {
      const strippedResult = stripRejectedImageReferences(liquidContent, rejectedSourceUrls);
      liquidContent = strippedResult.liquidContent;
      const stripped = strippedResult.stripped;
      if (stripped > 0) {
        console.log(`[${jobId}]   Stripped ${stripped} rejected image reference(s) from liquid`);
      }
    }

    // Rewrite liquid content: replace source image URLs with the new Shopify
    // CDN URLs we uploaded above. This prevents untranslated source images
    // from showing up in the AI-generated custom sections.
    //
    // Two-pass strategy:
    //  1. Exact string replace on each urlMap key (handles the happy path).
    //  2. Residue sweep: any URL still pointing at the source host gets matched
    //     by path basename to an uploaded CDN URL. Catches variants the AI
    //     introduces — different query strings, `&amp;` encoding, protocol-
    //     relative URLs, dropped query, etc. — that the exact match misses.
    const urlMapSize = Object.keys(urlMap).length;
    let urlRewriteCount = 0;
    let sourceHostForRewrite = '';
    try {
      sourceHostForRewrite = new URL(url).hostname.replace(/^www\./, '');
    } catch (e) {}

    if (urlMapSize > 0) {
      let replaced = 0;

      // ── Pass 1: exact match ──
      for (const [oldUrl, newUrl] of Object.entries(urlMap)) {
        const result = replaceImageUrlReferences(liquidContent, oldUrl, newUrl);
        liquidContent = result.content;
        replaced += result.changed;
      }

      // ── Pass 2: residue sweep by basename ──
      // Index urlMap by filename stem so we can match "foo.jpg" references
      // even if the query string, scheme, or host prefix differs.
      if (sourceHostForRewrite) {
        const basenameToNew = {};
        for (const [oldUrl, newUrl] of Object.entries(urlMap)) {
          try {
            const basename = new URL(oldUrl).pathname.split('/').pop();
            if (basename && !basenameToNew[basename]) basenameToNew[basename] = newUrl;
          } catch (e) {}
        }

        // Find every URL in the liquid that still points at the source host.
        // Regex captures the full URL up to a whitespace/quote/paren boundary.
        const hostEscaped = sourceHostForRewrite.replace(/\./g, '\\.');
        const residueRe = new RegExp(
          `(?:https?:)?\\/\\/(?:www\\.)?${hostEscaped}[^\\s"'<>)]*`,
          'gi'
        );
        const residual = liquidContent.match(residueRe) || [];
        const uniqueResidual = [...new Set(residual)];

        for (const leaked of uniqueResidual) {
          // Get basename from the leaked URL (strip query and `&amp;` noise)
          let basename = '';
          try {
            const cleaned = leaked.replace(/&amp;/g, '&');
            const withScheme = cleaned.startsWith('//') ? 'https:' + cleaned : cleaned;
            basename = new URL(withScheme).pathname.split('/').pop() || '';
          } catch (e) {
            // Fallback: parse basename by hand
            const bareNoQuery = leaked.split('?')[0];
            basename = bareNoQuery.split('/').pop() || '';
          }

          const replacement = basenameToNew[basename];
          if (replacement) {
            const before = liquidContent;
            liquidContent = liquidContent.split(leaked).join(replacement);
            if (liquidContent !== before) replaced++;
          }
        }
      }

      console.log(`[${jobId}]   Rewrote ${replaced} source-image URL(s) in liquid → Shopify CDN`);
      urlRewriteCount = replaced;
    }

    liquidContent = sanitizeLiquidBeforePublish(liquidContent, {
      brandName,
      targetLanguage,
      sourceBrandTerms
    });
    fs.writeFileSync(liquidPath, liquidContent);

    // Pre-publish gate: do not push live pages that still contain source-brand
    // names or original source images. The later QA report is still kept for
    // the UI, but these critical failures now stop the run before publish.
    const prePublishQa = runPostCloneQa({
      sourceUrl: url,
      targetLanguage,
      brandName,
      productTitle: productMeta.title,
      productMeta: {
        title: productMeta.title,
        description: rawDescription
      },
      scrapedImageUrls: allImageUrls,
      uploadedImageUrls: Object.values(urlMap),
      liquidContent,
      urlRewriteCount
    });
    if (!prePublishQa.pass) {
      console.error(`[${jobId}] Pre-publish QA failed:\n${formatQaReport(prePublishQa)}`);
      throw new Error(`Pre-publish QA failed: ${prePublishQa.errors.slice(0, 2).join(' | ')}`);
    }

    if (targetLanguage) {
      updateJob(jobId, {
        progress: 68,
        steps: { ...jobs[jobId].steps, translating: { status: 'done' }, pushing: { status: 'running' } }
      });
    } else {
      updateJob(jobId, {
        progress: 70,
        steps: { ...jobs[jobId].steps, creating: { status: 'done' }, pushing: { status: 'running' } }
      });
    }

    // ── Step 4: Build and push template ──
    // Push the cloned content as a stand-alone section asset
    // (sections/cloned-<handle>.liquid, ~256KB cap) instead of inlining it as
    // a custom_liquid setting (50KB cap). The template just references the
    // section by type. This is what unblocks Solawave-density clones, which
    // routinely produce 60-90KB of liquid after the maxTokens bump.
    console.log(`[${jobId}] Step 4: Building and pushing template...`);
    const sectionType = clonedSectionType(handle);
    const sectionAsset = buildClonedSectionAsset(liquidContent);
    await pushSectionAsset(sectionType, sectionAsset, storeId);
    const templateJson = buildHorizonTemplate(liquidContent, { sectionType });
    await pushTemplate(handle, templateJson, storeId);

    updateJob(jobId, {
      progress: 90,
      steps: { ...jobs[jobId].steps, pushing: { status: 'done' }, publishing: { status: 'running' } }
    });

    // ── Step 5: Publish ──
    console.log(`[${jobId}] Step 5: Publishing product...`);
    const { productUrl: publishedProductUrl, adminUrl } = await publishProduct(handle, storeId);

    console.log(`[${jobId}] Product published: ${publishedProductUrl}`);

    updateJob(jobId, {
      progress: 92,
      steps: { ...jobs[jobId].steps, publishing: { status: 'done' }, reviews: { status: 'running' } }
    });

    // ── Step 6: Scrape, translate, and prepare reviews for Loox ──
    let reviewResult = null;
    try {
      console.log(`[${jobId}] Step 6: Importing reviews...`);
      const storeConfig = getStoreConfig(storeId);
      // Use targetLanguage if specified, else fall back to store's configured language
      const language = targetLanguage
        ? (LANGUAGE_NAMES[targetLanguage] || storeConfig.language || 'en')
        : (storeConfig.language || 'en');

      reviewResult = await importReviews(
        url,                          // source URL to scrape reviews from
        handle,                       // target product handle
        language,                     // target language
        productMeta.title || handle,  // product name for translation context
        jobDir                        // output directory for CSV/JSON files
      );

      console.log(`[${jobId}]   Reviews: ${reviewResult.reviewCount} scraped and translated`);
      if (reviewResult.csvPath) {
        console.log(`[${jobId}]   Loox CSV: ${reviewResult.csvPath}`);
      }
    } catch (reviewErr) {
      console.warn(`[${jobId}] Review import failed (non-fatal): ${reviewErr.message}`);
      reviewResult = { reviewCount: 0, csvPath: null };
    }

    // ── Step 7: Automated post-clone QA ──
    // Quick, non-blocking sanity checks so the user doesn't have to click through
    // every cloned page manually. Surfaces image-repetition, brand leakage, and
    // source-URL residue as errors/warnings in the UI.
    let qaReport = null;
    try {
      const storeConfigForQa = getStoreConfig(storeId);
      const brandForQa = storeConfigForQa.storeName || storeId;
      qaReport = runPostCloneQa({
        sourceUrl: url,
        targetLanguage,
        brandName: brandForQa,
        productTitle: productMeta.title,
        productMeta: {
          title: productMeta.title,
          description: rawDescription
        },
        scrapedImageUrls: allImageUrls,
        uploadedImageUrls: Object.values(urlMap),
        liquidContent,
        urlRewriteCount
      });
      console.log(`[${jobId}] ${formatQaReport(qaReport)}`);
    } catch (qaErr) {
      console.warn(`[${jobId}] QA check threw (non-fatal): ${qaErr.message}`);
      qaReport = { pass: true, errors: [], warnings: [`QA check failed to run: ${qaErr.message}`], info: [] };
    }

    console.log(`[${jobId}] Done! ${publishedProductUrl}`);

    // --- Cost summary: print + persist + attach to job ------------------
    costTracker.printSummary();
    await costTracker.persistToFile();

    updateJob(jobId, {
      status: 'done',
      progress: 100,
      result: {
        content: liquidContent,
        filename: liquidFilename,
        outputPath: liquidPath,
        handle,
        productUrl: publishedProductUrl,
        adminUrl,
        productMeta: {
          title: productMeta.title,
          price: productMeta.price,
          compareAtPrice: productMeta.compareAtPrice,
          variantCount: variants.length,
          imageCount: allImageUrls.length
        },
        reviews: reviewResult ? {
          count: reviewResult.reviewCount,
          csvPath: reviewResult.csvPath,
          totalSource: reviewResult.totalSourceReviews
        } : null,
        qa: qaReport,
        cost: {
          runId: costTracker.runId,
          totalUsd: costTracker.totalUsd(),
          breakdown: costTracker.breakdown(),
          summary: costTracker.summary(),
        }
      },
      steps: { ...jobs[jobId].steps, reviews: { status: 'done' } }
    });

  } catch (err) {
    if (browser) await browser.close().catch(() => {});
    // Even on failure, persist whatever costs we incurred (fal.ai charges
    // us whether the downstream pipeline succeeds or not).
    try {
      costTracker.printSummary();
      await costTracker.persistToFile();
    } catch (costErr) {
      console.error('[cost-tracker] Persist on failure also failed:', costErr.message);
    }
    throw err;
  }
}

module.exports = router;
// Also export internals so dry-clone.js (and tests) can simulate the
// gallery-vs-content split without spinning up the full HTTP server.
module.exports.categorizeProductImages = categorizeProductImages;
module.exports.imageDedupeKey = imageDedupeKey;
module.exports.normalizeImageUrl = normalizeImageUrl;
