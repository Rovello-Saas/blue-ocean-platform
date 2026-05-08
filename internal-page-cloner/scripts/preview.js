#!/usr/bin/env node
/**
 * preview.js — local browser preview for dry-clone output.
 *
 * Serves the most recent (or specified) dry-run's output.liquid at
 * http://127.0.0.1:3030 so you can see the cloned section in a browser
 * without needing a Shopify store. Mocks the {{ product.* }} Liquid
 * fields with sample values so the layout renders correctly.
 *
 * IMPORTANT — what you can and cannot see in the preview:
 *   ✅ The AI-built body sections (benefits, comparison, before/after,
 *      stats, FAQ) WITH the source palette applied
 *   ✅ The injected recolor CSS (you can confirm `.add-to-cart-button`
 *      etc. have the correct hex by viewing source)
 *   ❌ Movanella's actual product hero, gallery, bundle picker, and
 *      ATC button — those are rendered by Movanella's Shopify theme,
 *      not by the cloner. To see those recolored you have to push to
 *      Shopify (which is what the live cloner does).
 *
 * Usage:
 *   node scripts/preview.js                          # serves the most recent dry-run
 *   node scripts/preview.js c22da020                 # serves a specific job (urlHash)
 *   node scripts/preview.js --port=3030              # custom port
 */

const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');

const ROOT = path.join(__dirname, '..');
const DRY_RUNS_DIR = path.join(ROOT, 'data', 'dry-runs');

// Default Movanella product URL used by --full mode. Any existing cloned
// product works — we splice our new output into its custom_liquid_cloned
// section. Override with --movanella=URL.
const DEFAULT_MOVANELLA_URL =
  'https://movanella.com/products/radiant-renewal-skincare-wand-with-red-light-therapy';

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        return resolve(httpsGet(new URL(res.headers.location, url).toString()));
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
    }).on('error', reject);
  });
}

function findLatestJob() {
  if (!fs.existsSync(DRY_RUNS_DIR)) return null;
  const dirs = fs.readdirSync(DRY_RUNS_DIR)
    .map(name => ({ name, fullPath: path.join(DRY_RUNS_DIR, name) }))
    .filter(d => {
      try { return fs.statSync(d.fullPath).isDirectory(); } catch { return false; }
    })
    .map(d => ({ ...d, mtime: fs.statSync(path.join(d.fullPath, 'output.liquid')).mtime }))
    .sort((a, b) => b.mtime - a.mtime);
  return dirs[0]?.name || null;
}

function parseArgs(argv) {
  const out = {
    jobId: null,
    port: 3030,
    full: false,
    movanellaUrl: DEFAULT_MOVANELLA_URL,
  };
  for (const a of argv) {
    if (a.startsWith('--port=')) out.port = parseInt(a.slice(7), 10) || 3030;
    else if (a === '--full') out.full = true;
    else if (a.startsWith('--movanella=')) {
      out.full = true;
      out.movanellaUrl = a.slice(12);
    }
    else if (!a.startsWith('--')) out.jobId = a;
  }
  return out;
}

// Replace common Liquid expressions with sample values so the preview
// renders. We only stub what shows up visibly — variables inside CSS
// values are left alone (rare).
function stubLiquid(liquid) {
  return liquid
    .replace(/{{\s*product\.title\s*}}/g, 'Sample Product Title')
    .replace(/{{\s*product\.handle\s*}}/g, 'sample-handle')
    .replace(/{{\s*product\.price\s*\|\s*money\s*}}/g, '€155')
    .replace(/{{\s*product\.compare_at_price\s*\|\s*money\s*}}/g, '€310')
    .replace(/{{\s*product\.featured_image\s*\|[^}]*}}/g, 'https://placehold.co/600x600/eee/aaa?text=Featured')
    .replace(/{{\s*image\s*\|[^}]*}}/g, 'https://placehold.co/600x600/eee/aaa?text=Img')
    .replace(/{{\s*product\.selected_or_first_available_variant\.id\s*}}/g, '1')
    // {% for image in product.images %} blocks: replace with 5 placeholder imgs
    .replace(
      /{%\s*for\s+image\s+in\s+product\.images\s*%}([\s\S]*?){%\s*endfor\s*%}/g,
      (_, body) => Array.from({ length: 5 }, (_, i) =>
        body.replace(/{{\s*image[^}]*}}/g, `https://placehold.co/600x600/eee/aaa?text=Img+${i + 1}`)
      ).join('')
    )
    // Other {% for %} blocks: keep one iteration, drop the loop
    .replace(/{%\s*for\s+\w+\s+in\s+[^%]+%}([\s\S]*?){%\s*endfor\s*%}/g, '$1')
    // Strip remaining {% ... %} tags
    .replace(/{%[^%]*%}/g, '')
    // Strip any remaining {{ ... }} expressions
    .replace(/{{[^}]*}}/g, '');
}

// Splice our new output.liquid into a fetched Movanella product page so
// the preview shows the FULL Shopify-rendered card (gallery, ATC button,
// bundle picker) WITH our cloned body section AND our recolor CSS.
//
// What we do:
//   1. Add <base href="https://movanella.com/"> so all the page's
//      relative asset URLs (CSS, fonts, images) resolve to Movanella's
//      CDN. The user gets the actual Shopify-rendered look without a
//      local Shopify environment.
//   2. Find the section with id ending in __custom_liquid_cloned and
//      replace its inner content with the stubbed-Liquid output of the
//      most recent dry-clone.
//   3. Inject the recolor CSS at the end of <head> so the .add-to-cart-button
//      etc. on Movanella's existing hero get repainted to the source palette.
function spliceIntoMovanellaHtml(movanellaHtml, ourLiquid, palette) {
  const stubbed = stubLiquid(ourLiquid);

  // 1. Base href so assets resolve. Insert right after <head>.
  let out = movanellaHtml.replace(
    /<head([^>]*)>/i,
    `<head$1>\n  <base href="https://movanella.com/">`
  );

  // 2. Replace the custom_liquid_cloned section's inner content. The
  //    section is a <div id="shopify-section-template--XXX__custom_liquid_cloned"
  //    class="shopify-section">...</div>. We find it and swap inner content.
  const sectionRe = /(<div id="shopify-section-template--[^"]*?__custom_liquid_cloned"[^>]*>)([\s\S]*?)(<\/div>(?=\s*(?:<div id="shopify-section|<\/main|<\/body)))/i;
  if (sectionRe.test(out)) {
    out = out.replace(sectionRe, `$1\n${stubbed}\n$3`);
  } else {
    // Fallback: append before </main> if the section wasn't found
    out = out.replace(/<\/main>/i, `\n${stubbed}\n</main>`);
  }

  // 3. Inject recolor CSS at end of <head>. The cloner already puts this
  //    inside the cloned section, but in case the splice missed or the
  //    extracted palette differs from what the deployed clone uses, this
  //    guarantees the visible recolor matches the dry-run's palette.
  if (palette) {
    const recolorCss = `
<style id="cloner-preview-recolor">
.add-to-cart-button { background: ${palette.accent} !important; border-color: ${palette.accent} !important; }
.add-to-cart-button:hover, .add-to-cart-button:focus { background: ${palette.accentDark} !important; }
.pd-rating { color: ${palette.accent} !important; }
.pd-rating svg, .pd-rating path { fill: ${palette.accent} !important; }
[id^="shopify-section-template"][id$="__main"] span[style*="rgb(7, 148, 26)"],
[id^="shopify-section-template"][id$="__main"] span[style*="#07941a"],
[id^="shopify-section-template"][id$="__main"] [style*="color: rgb(7"] { color: ${palette.accent} !important; }
</style>
`;
    out = out.replace(/<\/head>/i, `${recolorCss}\n</head>`);
  }

  // 4. Add a top banner so the user knows this is a local mock
  const banner = `
<div style="position:fixed;top:0;left:0;right:0;background:#fef3c7;border-bottom:1px solid #f59e0b;padding:8px 16px;font:13px/1.4 -apple-system,sans-serif;color:#78350f;z-index:99999;">
  <strong>Local FULL preview</strong> — Movanella product page fetched from movanella.com, with your dry-run output spliced into the cloned section. Some interactive features (cart, search) may not work due to cross-origin restrictions, but the visual rendering is faithful.
</div>
<div style="height:38px"></div>
`;
  out = out.replace(/<body([^>]*)>/i, `<body$1>${banner}`);

  return out;
}

function buildHtml(liquid, jobId, palette) {
  const stubbed = stubLiquid(liquid);
  const paletteHex = palette
    ? `<div style="position:fixed;top:8px;right:8px;background:#111;color:#fff;padding:10px 14px;font:12px/1.4 -apple-system,sans-serif;border-radius:6px;z-index:9999;">
         <strong>Palette (${jobId})</strong><br/>
         <span style="display:inline-block;width:14px;height:14px;background:${palette.accent};border-radius:2px;vertical-align:middle;"></span> accent ${palette.accent}<br/>
         <span style="display:inline-block;width:14px;height:14px;background:${palette.accentDark};border-radius:2px;vertical-align:middle;"></span> dark ${palette.accentDark}<br/>
         <span style="display:inline-block;width:14px;height:14px;background:${palette.surface};border:1px solid #ccc;border-radius:2px;vertical-align:middle;"></span> surface ${palette.surface}
       </div>`
    : '';

  return `<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <title>Cloner preview — ${jobId}</title>
  <style>
    body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
    .preview-banner { background: #fef3c7; border-bottom: 1px solid #f59e0b; padding: 12px 20px; font-size: 13px; color: #78350f; }
    .preview-banner code { background: #fde68a; padding: 1px 6px; border-radius: 3px; }
  </style>
</head>
<body>
  <div class="preview-banner">
    <strong>Local preview</strong> — this is the cloner's body section only.
    Movanella's hero (gallery, ATC, bundle picker) is rendered by Shopify and is NOT shown here.
    To see those recolored, push to Shopify via the cockpit.
  </div>
  ${paletteHex}
  ${stubbed}
</body>
</html>`;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const jobId = args.jobId || findLatestJob();

  if (!jobId) {
    console.error('❌ No dry-runs found. Run `node scripts/dry-clone.js <url>` first.');
    process.exit(1);
  }

  const jobDir = path.join(DRY_RUNS_DIR, jobId);
  const liquidPath = path.join(jobDir, 'output.liquid');
  const palettePath = path.join(jobDir, 'palette.json');

  if (!fs.existsSync(liquidPath)) {
    console.error(`❌ ${liquidPath} not found. Did the dry-clone finish?`);
    process.exit(1);
  }

  const palette = fs.existsSync(palettePath)
    ? JSON.parse(fs.readFileSync(palettePath, 'utf-8'))
    : null;

  // For --full mode, fetch the Movanella shell HTML once at startup and
  // cache it. Re-fetching on every request would be slow and would hammer
  // movanella.com. The page itself rarely changes; what changes is OUR
  // generated section, which we re-read per-request below.
  let movanellaShell = null;
  if (args.full) {
    process.stdout.write(`🌐 Fetching Movanella shell from ${args.movanellaUrl}... `);
    try {
      movanellaShell = await httpsGet(args.movanellaUrl);
      console.log(`OK (${(movanellaShell.length / 1024).toFixed(0)} KB)`);
    } catch (e) {
      console.log(`FAILED: ${e.message}`);
      console.log('   Falling back to body-only preview. Use --full --movanella=URL to retry.');
      args.full = false;
    }
  }

  const server = http.createServer((req, res) => {
    // Re-read on every request so editing the output (or re-running
    // dry-clone) is instantly visible — just refresh the browser tab.
    const liquid = fs.readFileSync(liquidPath, 'utf-8');
    const currentPalette = fs.existsSync(palettePath)
      ? JSON.parse(fs.readFileSync(palettePath, 'utf-8'))
      : palette;

    let html;
    if (args.full && movanellaShell) {
      html = spliceIntoMovanellaHtml(movanellaShell, liquid, currentPalette);
    } else {
      html = buildHtml(liquid, jobId, currentPalette);
    }

    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
  });

  server.listen(args.port, '127.0.0.1', () => {
    console.log('');
    console.log(`✅ Preview server running (${args.full ? 'FULL — Movanella shell' : 'body-only'})`);
    console.log(`   Job:  ${jobId}`);
    console.log(`   File: ${liquidPath}`);
    console.log('');
    console.log(`   Open: http://127.0.0.1:${args.port}`);
    console.log('');
    console.log('   Refresh the browser after re-running dry-clone to see new output.');
    console.log('   Ctrl+C to stop.');
  });
}

main().catch(err => {
  console.error('❌ Preview failed:', err.message);
  process.exit(1);
});
