/**
 * dedupe-images.js
 *
 * Removes near-duplicate product images AFTER the DOM scraper has collected
 * them but BEFORE they get uploaded / translated. The scraper's URL-based
 * dedup catches identical URLs (`?v=123` variants), but misses cases where
 * the source page shows the same photo under two unrelated CDN paths, or
 * two extremely similar product shots that look redundant in a gallery.
 *
 * Strategy: perceptual hash (dHash).
 *   1. Fetch a small thumbnail of each image (`?width=200` when we can).
 *   2. Resize to 9×8 grayscale via Sharp.
 *   3. Compare each pixel to its right neighbor → 64-bit fingerprint.
 *   4. Hamming distance ≤ threshold → considered a near-duplicate → drop.
 *
 * Notes:
 * - Order is preserved. The first occurrence of a hash is kept; later
 *   near-duplicates are dropped.
 * - If hashing fails for any reason (404, non-image, timeout), we keep the
 *   image. Silent "correct" is better than silently losing real gallery
 *   images to a transient network hiccup.
 */

const sharp = require('sharp');
const https = require('https');
const http = require('http');

// Hamming distance threshold. 0–5 ≈ identical. 6–10 ≈ very similar (same scene,
// minor crop/angle changes). 11–15 ≈ related. We pick 10 so that two product
// shots of the same pillow from nearly the same angle — which look redundant
// in a carousel — get collapsed, while genuinely different images (pillow-only
// vs. lifestyle shot) stay separate.
const DEFAULT_THRESHOLD = 10;
const FETCH_TIMEOUT_MS = 12000;

/**
 * Normalize a raw image URL into something `new URL()` won't reject.
 * The scraper often returns protocol-relative URLs (`//cdn.shopify.com/...`)
 * — those have no scheme so the URL constructor throws, which previously
 * caused dedup to silently skip them (net effect: duplicates survived).
 */
function normalizeUrl(url) {
  if (!url) return url;
  if (url.startsWith('//')) return 'https:' + url;
  if (url.startsWith('/')) return null; // relative to origin — can't resolve blindly
  return url;
}

function fetchBuffer(url, { timeoutMs = FETCH_TIMEOUT_MS, maxRedirects = 3 } = {}) {
  return new Promise((resolve, reject) => {
    let done = false;
    const finish = (fn, arg) => { if (!done) { done = true; fn(arg); } };

    const normalized = normalizeUrl(url);
    if (!normalized) return finish(reject, new Error(`unusable url: ${url}`));

    let target;
    try {
      target = new URL(normalized);
    } catch (e) {
      return finish(reject, new Error(`bad url: ${url}`));
    }
    const client = target.protocol === 'https:' ? https : http;

    const req = client.get(target, { timeout: timeoutMs }, (res) => {
      const status = res.statusCode || 0;
      if (status >= 300 && status < 400 && res.headers.location && maxRedirects > 0) {
        res.resume();
        const next = new URL(res.headers.location, target).toString();
        fetchBuffer(next, { timeoutMs, maxRedirects: maxRedirects - 1 })
          .then((buf) => finish(resolve, buf), (err) => finish(reject, err));
        return;
      }
      if (status >= 400) {
        res.resume();
        return finish(reject, new Error(`HTTP ${status} for ${url}`));
      }
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => finish(resolve, Buffer.concat(chunks)));
      res.on('error', (err) => finish(reject, err));
    });

    req.on('timeout', () => {
      req.destroy(new Error('timeout'));
    });
    req.on('error', (err) => finish(reject, err));
  });
}

/**
 * Rewrite a Shopify CDN URL to fetch a 200px-wide thumbnail. Other hosts
 * are left untouched — we'll still pull the full image, it just costs more
 * bandwidth.
 */
function smallerVariant(url) {
  const normalized = normalizeUrl(url);
  if (!normalized) return normalized;
  if (normalized.includes('cdn.shopify.com') || normalized.includes('/cdn/shop/')) {
    const bare = normalized.split('?')[0];
    return `${bare}?width=200`;
  }
  return normalized;
}

/**
 * Compute a 64-bit dHash (difference hash) for the given image buffer.
 * Returns null if the buffer isn't a decodable image.
 */
async function computeDHash(buffer) {
  try {
    const { data } = await sharp(buffer)
      .resize(9, 8, { fit: 'fill' })
      .grayscale()
      .raw()
      .toBuffer({ resolveWithObject: true });

    let hash = 0n;
    let bit = 0n;
    for (let y = 0; y < 8; y++) {
      for (let x = 0; x < 8; x++) {
        const left = data[y * 9 + x];
        const right = data[y * 9 + x + 1];
        if (left < right) hash |= (1n << bit);
        bit++;
      }
    }
    return hash;
  } catch (e) {
    return null;
  }
}

function hamming(a, b) {
  let x = a ^ b;
  let dist = 0;
  while (x) {
    dist += Number(x & 1n);
    x >>= 1n;
  }
  return dist;
}

async function hashUrl(url) {
  // Try the small variant first; fall back to the full URL if that 404s.
  const tries = [smallerVariant(url), url].filter((v, i, arr) => v && arr.indexOf(v) === i);
  for (const candidate of tries) {
    try {
      const buf = await fetchBuffer(candidate);
      const h = await computeDHash(buf);
      if (h !== null) return h;
    } catch (_) { /* try next */ }
  }
  return null;
}

/**
 * Drop near-duplicate images by perceptual hash. Preserves order.
 *
 * @param {Array<{src: string}>} images
 * @param {object} [opts]
 * @param {number} [opts.threshold=6]  Hamming distance ≤ this → duplicate
 * @param {(msg: string) => void} [opts.log]
 * @returns {Promise<Array>}
 */
async function dedupeByPerceptualHash(images, opts = {}) {
  const threshold = opts.threshold ?? DEFAULT_THRESHOLD;
  const log = opts.log || (() => {});

  if (!Array.isArray(images) || images.length < 2) return images || [];

  // Hash all images in parallel. Each slot is either a bigint or null (skip).
  const hashes = await Promise.all(images.map((img) => hashUrl(img.src)));

  const kept = [];
  const keptHashes = [];
  let droppedCount = 0;
  const droppedExamples = [];

  for (let i = 0; i < images.length; i++) {
    const h = hashes[i];
    if (h === null) {
      // Couldn't hash — keep it. Safer than silently losing a real image.
      kept.push(images[i]);
      keptHashes.push(null);
      continue;
    }
    const dupOf = keptHashes.findIndex((k) => k !== null && hamming(h, k) <= threshold);
    if (dupOf >= 0) {
      droppedCount++;
      if (droppedExamples.length < 4) {
        const name = (images[i].src.split('?')[0].split('/').pop() || '').slice(-40);
        droppedExamples.push(name);
      }
    } else {
      kept.push(images[i]);
      keptHashes.push(h);
    }
  }

  if (droppedCount > 0) {
    log(`  [dedup] dropped ${droppedCount} near-duplicate image(s) (${images.length} → ${kept.length})`);
    if (droppedExamples.length) log(`  [dedup] examples: ${droppedExamples.join(', ')}`);
  }
  return kept;
}

module.exports = { dedupeByPerceptualHash, computeDHash, hamming };
