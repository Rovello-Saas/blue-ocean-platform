const express = require('express');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

const { scrapePage } = require('../scraper/browser');
const { extractSections, extractProductMeta } = require('../scraper/dom-extractor');
const { analyzeImages } = require('../scraper/image-analyzer');
const { dedupeByPerceptualHash } = require('../scraper/dedupe-images');
const { generateFullLiquid } = require('../ai/generate-content');
const { translateProductImages, LANGUAGE_NAMES } = require('../ai/translate-images');
const { translateTitle, translateDescription, generateBulletDescription } = require('../ai/translate-text');
const { getAllBlocks } = require('../blocks/library');
const {
  createProduct,
  setVariantsAndPricing,
  uploadImages,
  buildHorizonTemplate,
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

function createCloneJob({ url, storeId = 'movanella', targetLanguage = null }) {
  const store = ['movanella', 'merivalo'].includes(storeId) ? storeId : 'movanella';
  // Validate targetLanguage — must be a known language code or empty
  const lang = (targetLanguage && LANGUAGE_NAMES[targetLanguage]) ? targetLanguage : null;
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
    storeId: store,
    targetLanguage: lang,
    status: 'scraping',
    progress: 0,
    steps,
    result: null,
    error: null,
    createdAt: new Date().toISOString(),
    _jobDir: jobDir
  };

  fs.writeFileSync(path.join(jobDir, 'job.json'), JSON.stringify(jobs[jobId], null, 2));

  runPipeline(jobId, url, jobDir, store, lang).catch(err => {
    console.error(`Job ${jobId} failed:`, err);
    updateJob(jobId, { status: 'failed', error: err.message });
  });

  return { jobId, status: 'scraping', storeId: store, targetLanguage: lang };
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

function mergeImageUrlsForProcessing(productImages, liquidContent, maxImages = 28) {
  const urls = [];
  const seen = new Set();
  const productUrls = (productImages || [])
    .map(img => typeof img === 'string' ? img : img.src)
    .filter(Boolean);
  const liquidUrls = extractImageUrlsFromLiquid(liquidContent);
  const primaryGalleryCount = Math.min(18, maxImages);

  const add = (src) => {
    const normalized = normalizeImageUrl(src);
    if (!normalized || !/^https?:\/\//i.test(normalized)) return;
    const key = imageDedupeKey(normalized);
    if (seen.has(key)) return;
    seen.add(key);
    urls.push(normalized);
  };

  // Keep the gallery/card images first, but do not let a very long gallery
  // crowd out section-specific images the generated Liquid actually uses.
  productUrls.slice(0, primaryGalleryCount).forEach(add);
  liquidUrls.forEach(add);
  productUrls.slice(primaryGalleryCount).forEach(add);

  return urls.slice(0, maxImages);
}

// POST /api/jobs - Start a clone job
router.post('/jobs', (req, res) => {
  const { url, storeId, targetLanguage } = req.body;
  if (!url) return res.status(400).json({ error: 'URL is required' });

  res.json(createCloneJob({ url, storeId, targetLanguage }));
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
    storeId: j.storeId,
    targetLanguage: j.targetLanguage,
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
async function runPipeline(jobId, url, jobDir, storeId = 'movanella', targetLanguage = null) {
  let browser;

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
    console.log(`[${jobId}] Step 1: Scraping ${url} (store: ${storeId})...`);
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
    const productMeta = await extractProductMeta(page);
    console.log(`[${jobId}]   Title: ${productMeta.title}`);
    console.log(`[${jobId}]   Price: ${productMeta.price}`);
    console.log(`[${jobId}]   Images: ${productMeta.images.length}`);
    console.log(`[${jobId}]   Variants: ${productMeta.variants.length}`);

    await browser.close().catch(() => {});
    browser = null;

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
    fs.writeFileSync(path.join(jobDir, 'product-meta.json'), JSON.stringify(productMeta, null, 2));

    updateJob(jobId, {
      progress: 20,
      steps: { ...jobs[jobId].steps, scraping: { status: 'done' }, generating: { status: 'running' } }
    });

    // ── Step 2: Generate liquid content ──
    console.log(`[${jobId}] Step 2: AI generating full liquid content (lang: ${targetLanguage || 'store default'})...`);
    let liquidContent = await generateFullLiquid(productMeta, sections, screenshotPath, storeId, targetLanguage);
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

    // Upload images. Product-gallery images go first so the Shopify product
    // card/gallery keeps the same primary visuals as the source. Then include
    // any extra image URLs the generated custom sections actually referenced
    // (comparison charts, before/after graphics, usage infographics, etc.).
    // This prevents generated sections from leaking untranslated source URLs.
    const productGalleryImageCount = productMeta.images.length;
    const liquidImageCount = extractImageUrlsFromLiquid(liquidContent).length;
    const imageUrls = mergeImageUrlsForProcessing(productMeta.images, liquidContent, 28);
    console.log(`[${jobId}]   Image processing set: ${imageUrls.length} unique URL(s) (${productGalleryImageCount} gallery, ${liquidImageCount} section reference(s))`);

    // ── Step 3b: Translate + upload product images ──
    // Also builds `urlMap` (sourceUrl → Shopify CDN URL) so we can rewrite the
    // AI-generated liquid below, pointing it at the translated images we just
    // uploaded rather than the untranslated source URLs.
    const urlMap = {}; // sourceUrl → newShopifyCdnUrl

    const storeConfigForImgs = getStoreConfig(storeId);
    const brandName = storeConfigForImgs.storeName || storeId;
    let falApiKey = process.env.FAL_API_KEY || '';
    if (!falApiKey) {
      try {
        const envContent = require('fs').readFileSync(require('path').join(__dirname, '../../.env'), 'utf-8');
        const falKeyMatch = envContent.match(/^FAL_API_KEY=(.+)$/m);
        falApiKey = falKeyMatch ? falKeyMatch[1].trim() : '';
      } catch (e) {
        falApiKey = '';
      }
    }

    if (imageUrls.length > 0) {
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

      if (!falApiKey) {
        console.warn(`[${jobId}] FAL_API_KEY not found — uploading original images with no edits`);
        const { restApi } = require('../shopify/automation');
        const products = await restApi('GET', `/products.json?handle=${handle}`, null, storeId);
        const productId = products.products?.[0]?.id;
        if (productId) {
          for (let i = 0; i < imageUrls.length; i++) {
            try {
              const res = await restApi('POST', `/products/${productId}/images.json`, {
                image: { src: imageUrls[i], position: i + 1 }
              }, storeId);
              if (res?.image?.src) urlMap[imageUrls[i]] = res.image.src;
            } catch (e) {
              console.warn(`  [Shopify] Image ${i + 1} upload failed: ${e.message?.substring(0, 100)}`);
            }
          }
        }
      } else {
        console.log(`[${jobId}] Step 3b: Processing ${imageUrls.length} product images (${modeLabel})...`);
        const translated = await translateProductImages(
          imageUrls,
          mode,
          brandName,
          falApiKey,
          (i, total) => {
            if (targetLanguage) {
              const pct = 55 + Math.round((i / total) * 12);
              updateJob(jobId, { progress: pct });
            }
          },
          { costTracker }
        );

        const { restApi } = require('../shopify/automation');
        const products = await restApi('GET', `/products.json?handle=${handle}`, null, storeId);
        const productId = products.products?.[0]?.id;
        if (productId) {
          for (let i = 0; i < translated.length; i++) {
            const item = translated[i];
            try {
              let res;
              if (item.buffer) {
                res = await restApi('POST', `/products/${productId}/images.json`, {
                  image: {
                    attachment: item.buffer.toString('base64'),
                    filename: `image-${i + 1}.jpg`,
                    position: i + 1
                  }
                }, storeId);
              } else {
                res = await restApi('POST', `/products/${productId}/images.json`, {
                  image: { src: item.originalUrl, position: i + 1 }
                }, storeId);
              }
              if (res?.image?.src) urlMap[item.originalUrl] = res.image.src;
              console.log(`  [Shopify] Uploaded image ${i + 1}/${translated.length}`);
            } catch (imgErr) {
              console.warn(`  [Shopify] Image ${i + 1} upload failed: ${imgErr.message?.substring(0, 100)}`);
            }
          }
        }
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
        const before = liquidContent;
        liquidContent = liquidContent.split(oldUrl).join(newUrl);
        if (liquidContent !== before) replaced++;

        // URL without query string — AI sometimes strips the `?v=...`
        const oldNoQuery = oldUrl.split('?')[0];
        if (oldNoQuery !== oldUrl) {
          const before2 = liquidContent;
          liquidContent = liquidContent.split(oldNoQuery).join(newUrl);
          if (liquidContent !== before2) replaced++;
        }

        // Protocol-relative form: `//host/path` — happens when the scraper kept
        // the protocol-relative URL and the AI quoted that variant.
        if (oldUrl.startsWith('https://')) {
          const protoRel = oldUrl.replace(/^https:/, '');
          const before3 = liquidContent;
          liquidContent = liquidContent.split(protoRel).join(newUrl);
          if (liquidContent !== before3) replaced++;
        }
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
    console.log(`[${jobId}] Step 4: Building and pushing template...`);
    const templateJson = buildHorizonTemplate(liquidContent);
    await pushTemplate(handle, templateJson, storeId);

    updateJob(jobId, {
      progress: 90,
      steps: { ...jobs[jobId].steps, pushing: { status: 'done' }, publishing: { status: 'running' } }
    });

    // ── Step 5: Publish ──
    console.log(`[${jobId}] Step 5: Publishing product...`);
    const { productUrl, adminUrl } = await publishProduct(handle, storeId);

    console.log(`[${jobId}] Product published: ${productUrl}`);

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
        scrapedImageUrls: imageUrls,
        uploadedImageUrls: Object.values(urlMap),
        liquidContent,
        urlRewriteCount
      });
      console.log(`[${jobId}] ${formatQaReport(qaReport)}`);
    } catch (qaErr) {
      console.warn(`[${jobId}] QA check threw (non-fatal): ${qaErr.message}`);
      qaReport = { pass: true, errors: [], warnings: [`QA check failed to run: ${qaErr.message}`], info: [] };
    }

    console.log(`[${jobId}] Done! ${productUrl}`);

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
        productUrl,
        adminUrl,
        productMeta: {
          title: productMeta.title,
          price: productMeta.price,
          compareAtPrice: productMeta.compareAtPrice,
          variantCount: variants.length,
          imageCount: imageUrls.length
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
