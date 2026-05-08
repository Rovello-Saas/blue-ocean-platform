#!/usr/bin/env node
/**
 * dry-clone.js — local end-to-end test for the page cloner without
 * pushing to Shopify. Runs scrape → AI generation → writes output.liquid
 * to disk. Caches the puppeteer scrape per-URL so iterations on the AI
 * prompt re-use the same scraped sections without launching Chrome again.
 *
 * Usage:
 *   node scripts/dry-clone.js <url>
 *   node scripts/dry-clone.js <url> --no-cache    # force fresh scrape
 *   node scripts/dry-clone.js <url> --lang=de --store=movanella
 *   node scripts/dry-clone.js <url> --translate   # also call fal.ai (slow + costs $)
 *
 * Output: ./data/dry-runs/{urlHash}/
 *   - scrape.json        (cached sections + productMeta)
 *   - screenshot.png     (full page screenshot)
 *   - output.liquid      (the generated liquid file)
 *   - palette.json       (extracted color palette)
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// Load .env from cloner dir. Use the same regex-based fallback that
// src/ai/client.js uses, because dotenv silently drops some valid lines
// when values contain unusual characters or wrap across lines.
(function loadEnv() {
  const envPath = path.join(__dirname, '..', '.env');
  try {
    require('dotenv').config({ path: envPath });
  } catch (e) {}
  if (!fs.existsSync(envPath)) return;
  const content = fs.readFileSync(envPath, 'utf-8');
  for (const line of content.split('\n')) {
    const m = line.match(/^([A-Z][A-Z0-9_]+)=(.*)$/);
    if (!m) continue;
    const [, key, rawValue] = m;
    if (process.env[key]) continue; // already set by dotenv or shell
    let value = rawValue.trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    process.env[key] = value;
  }
})();

const { scrapePage } = require('../src/scraper/browser');
const { extractSections, extractProductMeta } = require('../src/scraper/dom-extractor');
const { analyzeImages, classifyImagePurposes } = require('../src/scraper/image-analyzer');
const { policyFor } = require('../src/ai/image-policy');
const { extractPalette } = require('../src/scraper/palette-extractor');
const { generateFullLiquid } = require('../src/ai/generate-content');

function parseArgs(argv) {
  const out = { url: null, noCache: false, translate: false, lang: 'de', store: 'movanella' };
  for (const a of argv) {
    if (a.startsWith('http')) out.url = a;
    else if (a === '--no-cache') out.noCache = true;
    else if (a === '--translate') out.translate = true;
    else if (a.startsWith('--lang=')) out.lang = a.slice(7);
    else if (a.startsWith('--store=')) out.store = a.slice(8);
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  if (!args.url) {
    console.error('Usage: node scripts/dry-clone.js <url> [--no-cache] [--lang=de] [--store=movanella] [--translate]');
    process.exit(1);
  }

  if (!process.env.ANTHROPIC_API_KEY) {
    console.error('❌ ANTHROPIC_API_KEY missing from .env — generation will fail.');
    process.exit(1);
  }

  const urlHash = crypto.createHash('md5').update(args.url).digest('hex').slice(0, 8);
  const jobDir = path.join(__dirname, '..', 'data', 'dry-runs', urlHash);
  fs.mkdirSync(jobDir, { recursive: true });

  const cachePath = path.join(jobDir, 'scrape.json');
  const screenshotPath = path.join(jobDir, 'screenshot.png');

  console.log(`📂 Job dir: ${jobDir}`);
  console.log(`🎯 Source:  ${args.url}`);
  console.log(`🏪 Store:   ${args.store}`);
  console.log(`🌐 Lang:    ${args.lang}`);
  console.log('');

  let sections, productMeta;
  const cacheValid =
    !args.noCache &&
    fs.existsSync(cachePath) &&
    fs.existsSync(screenshotPath);

  if (cacheValid) {
    console.log(`📦 Reusing cached scrape (--no-cache to force fresh)`);
    const cached = JSON.parse(fs.readFileSync(cachePath, 'utf-8'));
    sections = cached.sections;
    productMeta = cached.productMeta;
  } else {
    console.log('🌐 Scraping page (puppeteer)...');
    const t0 = Date.now();
    const { page, browser } = await scrapePage(args.url, jobDir);
    try {
      sections = await extractSections(page);
      sections = analyzeImages(sections);
      productMeta = await extractProductMeta(page);
    } finally {
      await browser.close().catch(() => {});
    }
    fs.writeFileSync(cachePath, JSON.stringify({ sections, productMeta }, null, 2));
    console.log(`📦 Scrape cached (${((Date.now() - t0) / 1000).toFixed(1)}s) → ${cachePath}`);
  }

  console.log(`📊 Sections: ${sections.length}, Images: ${productMeta.images?.length || 0}`);

  // Run image classification (heuristic, no API calls — fast)
  const classifyInput = [];
  for (const s of sections) {
    const sectionHeadings = (s.headings || []).map(h => (typeof h === 'string' ? h : h.text || ''));
    for (const img of (s.images || [])) {
      classifyInput.push({ ...img, sectionHeadings });
    }
  }
  for (const img of (productMeta.images || [])) {
    classifyInput.push({ ...img, sectionHeadings: [] });
  }
  const classifications = classifyImagePurposes(classifyInput);
  const purposeCounts = {};
  for (const purpose of classifications.values()) purposeCounts[purpose] = (purposeCounts[purpose] || 0) + 1;
  console.log(`🏷️  Image purposes: ${JSON.stringify(purposeCounts)}`);

  // Drop logo-strip images BEFORE liquid gen (matches the API server behavior)
  const droppedFromOutput = [];
  productMeta.images = (productMeta.images || []).filter(img => {
    const p = classifications.get(img.src);
    const policy = p ? policyFor(p) : null;
    if (policy?.dropFromOutput) {
      droppedFromOutput.push(img.src);
      return false;
    }
    return true;
  });
  for (const s of sections) {
    s.images = (s.images || []).filter(img => {
      const p = classifications.get(img.src);
      const policy = p ? policyFor(p) : null;
      return !policy?.dropFromOutput;
    });
  }
  if (droppedFromOutput.length) console.log(`🗑️  Dropped ${droppedFromOutput.length} logo-strip image(s) before liquid gen`);

  // Extract palette so we can show it before generation
  try {
    const palette = await extractPalette(screenshotPath);
    fs.writeFileSync(path.join(jobDir, 'palette.json'), JSON.stringify(palette, null, 2));
    console.log(`🎨 Palette: accent=${palette.accent} dark=${palette.accentDark} surface=${palette.surface} (dominant hue: ${palette.dominantHue?.toFixed(1) || 'n/a'}°)`);
  } catch (e) {
    console.warn(`⚠️  Palette extraction failed: ${e.message}`);
  }

  console.log('🤖 Calling Claude to generate liquid...');
  const t1 = Date.now();
  const liquid = await generateFullLiquid(productMeta, sections, screenshotPath, args.store, args.lang);
  console.log(`✅ Generated ${liquid.length} chars in ${((Date.now() - t1) / 1000).toFixed(1)}s`);

  const outPath = path.join(jobDir, 'output.liquid');
  fs.writeFileSync(outPath, liquid);
  console.log('');
  console.log(`📄 Output written: ${outPath}`);
  console.log('');
  console.log('Inspect:');
  console.log(`  open ${jobDir}                              # all artifacts`);
  console.log(`  open ${screenshotPath}                       # source screenshot`);
  console.log(`  head -100 ${outPath}                         # first 100 lines of generated liquid`);
  console.log(`  grep -A 25 "Recolor Movanella" ${outPath}    # the injected recolor CSS`);
  console.log(`  grep -E "rgb\\(|#[0-9a-f]{3,6}" ${outPath} | head -20  # all colors used`);
}

main().catch(e => {
  console.error('❌ Dry-clone failed:', e.message);
  if (e.stack) console.error(e.stack);
  process.exit(1);
});
