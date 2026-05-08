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

const ROOT = path.join(__dirname, '..');
const DRY_RUNS_DIR = path.join(ROOT, 'data', 'dry-runs');

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
  const out = { jobId: null, port: 3030 };
  for (const a of argv) {
    if (a.startsWith('--port=')) out.port = parseInt(a.slice(7), 10) || 3030;
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

function main() {
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

  const server = http.createServer((req, res) => {
    // Re-read on every request so editing the output (or re-running
    // dry-clone) is instantly visible — just refresh the browser tab.
    const liquid = fs.readFileSync(liquidPath, 'utf-8');
    const currentPalette = fs.existsSync(palettePath)
      ? JSON.parse(fs.readFileSync(palettePath, 'utf-8'))
      : palette;
    const html = buildHtml(liquid, jobId, currentPalette);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
  });

  server.listen(args.port, '127.0.0.1', () => {
    console.log('');
    console.log(`✅ Preview server running`);
    console.log(`   Job:  ${jobId}`);
    console.log(`   File: ${liquidPath}`);
    console.log('');
    console.log(`   Open: http://127.0.0.1:${args.port}`);
    console.log('');
    console.log('   Refresh the browser after re-running dry-clone to see new output.');
    console.log('   Ctrl+C to stop.');
  });
}

main();
