/**
 * palette-extractor.js
 *
 * Extracts a small color palette from the source page screenshot using sharp
 * (already a dependency). The palette is fed into the AI prompt as concrete
 * hex values so the model uses the source's actual colors instead of guessing
 * from the screenshot — which today often falls back to Movanella's blue/green
 * defaults even on visibly pink/blush source pages.
 *
 * Algorithm: bucket pixels into a 16×16×16 cube (top 4 bits per channel),
 * drop near-white and near-black, then pick the dominant bucket as `surface`,
 * the second as `background`, the most-saturated mid-bright bucket as `accent`,
 * and the darkest saturated bucket as `accentDark`. Cheap, deterministic, no
 * extra deps.
 */

const sharp = require('sharp');

const NEAR_BLACK_SUM = 60;    // R+G+B below this → treat as text shadow
const SAMPLE_WIDTH = 200;     // resize before sampling — trades precision for speed

// A pixel is "page whitespace" when it's pure-white-ish AND essentially
// hueless. We can't just threshold on R+G+B because cream/blush backgrounds
// (e.g. Solawave's #fff7f1 with sum=743) are actual page colors, not gaps,
// and dropping them would force the bucketing to pick a low-density accent
// as the surface. Treat as whitespace only when the channel max is high
// AND saturation is essentially zero (max-min span < 8/255).
function isWhitespace(r, g, b) {
  const max = Math.max(r, g, b);
  const min = Math.min(r, g, b);
  return max >= 248 && (max - min) < 8;
}

function rgbToHex(r, g, b) {
  const h = (n) => n.toString(16).padStart(2, '0');
  return `#${h(r)}${h(g)}${h(b)}`;
}

function saturationScore(r, g, b) {
  const max = Math.max(r, g, b);
  const min = Math.min(r, g, b);
  if (max + min === 0) return 0;
  return (max - min) / (max + min);
}

// Hue in degrees, 0–360. Red = 0, green = 120, blue = 240.
// We use this to filter out JPEG-compression noise: e.g. on a pink-dominated
// page, every legitimate accent has hue ≈ 350° (pink/red), and a teal noise
// cluster at hue ≈ 175° gets rejected even if its saturation looks high.
function hueDegrees(r, g, b) {
  const max = Math.max(r, g, b);
  const min = Math.min(r, g, b);
  const delta = max - min;
  if (delta === 0) return 0;
  let h;
  if (max === r) h = ((g - b) / delta) % 6;
  else if (max === g) h = (b - r) / delta + 2;
  else h = (r - g) / delta + 4;
  h *= 60;
  if (h < 0) h += 360;
  return h;
}

// Smallest signed angle between two hues, in degrees (0–180).
function hueDistance(a, b) {
  const d = Math.abs(a - b) % 360;
  return d > 180 ? 360 - d : d;
}

// Median value of an array (numeric).
function medianOf(arr) {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const m = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[m] : (sorted[m - 1] + sorted[m]) / 2;
}

async function extractPalette(screenshotPath) {
  let raw;
  try {
    const { data, info } = await sharp(screenshotPath)
      .resize({ width: SAMPLE_WIDTH, withoutEnlargement: true })
      .raw()
      .ensureAlpha(0)
      .toBuffer({ resolveWithObject: true });
    raw = { data, info };
  } catch (e) {
    return defaultPalette();
  }

  const { data, info } = raw;
  const channels = info.channels || 3;
  const buckets = new Map(); // key = rrggbb (4-bit each), value = { count, r, g, b }

  for (let i = 0; i < data.length; i += channels) {
    const r = data[i];
    const g = data[i + 1];
    const b = data[i + 2];
    const sum = r + g + b;
    if (isWhitespace(r, g, b)) continue;
    if (sum < NEAR_BLACK_SUM) continue;
    // Quantize to 4 bits per channel (16 levels)
    const qr = r >> 4;
    const qg = g >> 4;
    const qb = b >> 4;
    const key = (qr << 8) | (qg << 4) | qb;
    const existing = buckets.get(key);
    if (existing) {
      existing.count += 1;
      existing.r += r;
      existing.g += g;
      existing.b += b;
    } else {
      buckets.set(key, { count: 1, r, g, b });
    }
  }

  if (buckets.size === 0) return defaultPalette();

  // Materialize buckets with average RGB and saturation score
  const items = Array.from(buckets.values()).map(b => {
    const r = Math.round(b.r / b.count);
    const g = Math.round(b.g / b.count);
    const blue = Math.round(b.b / b.count);
    return {
      r,
      g,
      b: blue,
      count: b.count,
      sum: r + g + blue,
      saturation: saturationScore(r, g, blue),
      hue: hueDegrees(r, g, blue)
    };
  });
  const totalCount = items.reduce((acc, it) => acc + it.count, 0);

  // Surface = the dominant LOW-saturation bucket (cream, beige, soft grey,
  // tinted-white). Exclude high-saturation buckets so a small bright accent
  // doesn't get labeled as the page surface.
  const lowSatItems = items.filter(it => it.saturation < 0.20);
  const byCountLowSat = [...lowSatItems].sort((a, b) => b.count - a.count);
  const surface = byCountLowSat[0] || items.sort((a, b) => b.count - a.count)[0];
  const background = byCountLowSat[1] || surface;

  // Compute the dominant hue family from chromatic pixels. Count-weighted
  // circular mean on the unit circle — small JPEG-noise clusters can't drag
  // the average away from the page's real palette because real palette
  // hues appear in many places (eyebrow + ATC + badges + body accents) and
  // their counts add up. This is the key fix for the "Solawave clone
  // shipped teal" bug.
  const chromaticItems = items.filter(it => it.saturation >= 0.18);
  let cx = 0, cy = 0;
  for (const it of chromaticItems) {
    const rad = (it.hue * Math.PI) / 180;
    cx += Math.cos(rad) * it.count;
    cy += Math.sin(rad) * it.count;
  }
  const dominantHue = chromaticItems.length
    ? ((Math.atan2(cy, cx) * 180) / Math.PI + 360) % 360
    : null;

  // Accent = saturated mid-bright bucket whose hue is in the dominant
  // family. Tiered to prefer high-saturation UI colors over large areas of
  // mid-saturation content (skin tones in lifestyle photos kept beating
  // small ATC buttons in v1). Within each tier, score = saturation × log(1+count)
  // — count weight is gentle so a 50,000-pixel skin-tone area can't outrank
  // a 1,000-pixel firmly-saturated brand color.
  function scoreCandidate(it) {
    return it.saturation * Math.log1p(it.count);
  }
  function pickBest(filterFn) {
    const candidates = items.filter(filterFn);
    if (!candidates.length) return null;
    return candidates.sort((a, b) => scoreCandidate(b) - scoreCandidate(a))[0];
  }
  const hueOk = (it, maxDist = 30) =>
    dominantHue === null || hueDistance(it.hue, dominantHue) <= maxDist;
  const brightnessOk = (it) => it.sum >= 200 && it.sum <= 560;
  const accent =
    // Tier 1: firmly saturated UI color (button/badge) in dominant hue
    pickBest(it => brightnessOk(it) && it.saturation >= 0.45 && hueOk(it, 30)) ||
    // Tier 2: medium saturation in dominant hue (mid-tone brand color)
    pickBest(it => brightnessOk(it) && it.saturation >= 0.30 && hueOk(it, 30)) ||
    // Tier 3: pastel — relax saturation AND widen hue gate
    pickBest(it => it.sum >= 200 && it.sum <= 600 && it.saturation >= 0.18 && hueOk(it, 45)) ||
    byCountLowSat[2] ||
    surface;

  // Accent dark = darker shade in the same hue family (for hover/buttons-dark)
  const darkCandidates = items.filter(it =>
    it.sum < 380 &&
    it.saturation >= 0.20 &&
    hueDistance(it.hue, accent.hue) <= 45
  );
  const accentDark = darkCandidates.length
    ? darkCandidates.sort((a, b) => a.sum - b.sum)[0]
    : darken(accent);

  // Text primary = darkest substantial bucket (page body text color).
  // Require some count so we don't pick a single shadow noise pixel.
  const textMinCount = Math.max(20, Math.round(totalCount * 0.001));
  const textPrimary = [...items]
    .filter(it => it.count >= textMinCount)
    .sort((a, b) => a.sum - b.sum)[0] || items.sort((a, b) => a.sum - b.sum)[0];

  return {
    accent: rgbToHex(accent.r, accent.g, accent.b),
    accentDark: rgbToHex(accentDark.r, accentDark.g, accentDark.b),
    background: rgbToHex(background.r, background.g, background.b),
    surface: rgbToHex(surface.r, surface.g, surface.b),
    textPrimary: rgbToHex(textPrimary.r, textPrimary.g, textPrimary.b),
    dominantHue
  };
}

function darken(color) {
  return {
    r: Math.max(0, Math.round(color.r * 0.55)),
    g: Math.max(0, Math.round(color.g * 0.55)),
    b: Math.max(0, Math.round(color.b * 0.55))
  };
}

function defaultPalette() {
  // Neutral fallback so the rest of the pipeline doesn't crash on a
  // missing/corrupt screenshot. Picks values that are obviously placeholder.
  return {
    accent: '#888888',
    accentDark: '#444444',
    background: '#f5f5f5',
    surface: '#ffffff',
    textPrimary: '#1a1a1a'
  };
}

module.exports = { extractPalette };
