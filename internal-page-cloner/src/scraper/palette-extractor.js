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
      saturation: saturationScore(r, g, blue)
    };
  });

  // Surface = the dominant LOW-saturation bucket (cream, beige, soft grey,
  // tinted-white). We exclude high-saturation buckets here so a small but
  // bright accent button doesn't get labeled as the page surface.
  const lowSatItems = items.filter(it => it.saturation < 0.20);
  const byCountLowSat = [...lowSatItems].sort((a, b) => b.count - a.count);
  const surface = byCountLowSat[0] || items.sort((a, b) => b.count - a.count)[0];
  // Background = second most-dominant low-saturation bucket (the "soft"
  // panel zone behind cards). Falls back to surface if none.
  const background = byCountLowSat[1] || surface;

  // Accent = most-saturated mid-bright bucket. Brightness window 220–600 keeps
  // it visible (not muddy, not washed out). Saturation > 0.15 to filter grays.
  const accentCandidates = items.filter(it =>
    it.sum >= 220 && it.sum <= 600 && it.saturation >= 0.15
  );
  const accent = accentCandidates.length
    ? accentCandidates.sort((a, b) => b.saturation * Math.log10(b.count + 1) - a.saturation * Math.log10(a.count + 1))[0]
    : byCount[2] || surface;

  // Accent dark = most-saturated darker bucket (for hover/text/buttons-darker)
  const darkCandidates = items.filter(it =>
    it.sum < 380 && it.saturation >= 0.12
  );
  const accentDark = darkCandidates.length
    ? darkCandidates.sort((a, b) => b.saturation - a.saturation)[0]
    : darken(accent);

  // Text primary = darkest bucket overall (page body text color)
  const textPrimary = [...items].sort((a, b) => a.sum - b.sum)[0];

  return {
    accent: rgbToHex(accent.r, accent.g, accent.b),
    accentDark: rgbToHex(accentDark.r, accentDark.g, accentDark.b),
    background: rgbToHex(background.r, background.g, background.b),
    surface: rgbToHex(surface.r, surface.g, surface.b),
    textPrimary: rgbToHex(textPrimary.r, textPrimary.g, textPrimary.b)
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
