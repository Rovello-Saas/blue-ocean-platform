/**
 * translate-images.js
 *
 * Reusable module for translating text in product images via fal.ai Nano Banana 2.
 * Used by the page cloner pipeline when a target language is specified.
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');

const LANGUAGE_NAMES = {
  en: 'English',
  de: 'German',
  fr: 'French',
  es: 'Spanish',
  it: 'Italian',
  nl: 'Dutch'
};

// Target-language → uses metric units for dimensions/weight/temperature.
// English is kept unit-neutral (source is preserved) because en-US uses imperial
// and en-UK is mixed; the EU languages below are always metric.
const METRIC_LANGUAGES = new Set(['de', 'fr', 'es', 'it', 'nl']);

function buildTranslatePrompt(targetLanguage, brandName) {
  const isBrandOnly = targetLanguage === 'same';
  const langName = isBrandOnly ? null : (LANGUAGE_NAMES[targetLanguage] || targetLanguage);
  const needsMetric = !isBrandOnly && METRIC_LANGUAGES.has(targetLanguage);

  // --- Translation block ---
  const translateSection = isBrandOnly ? '' : `TRANSLATE every piece of visible text to ${langName}. This includes headlines, taglines, feature callouts (e.g. "Machine Washable", "Cloud-Soft Feel"), badges, buttons, overlays, size/dimension labels, legal text, and any other readable words. Every word must be in ${langName} — no exceptions, no mixing languages.

`;

  // --- Unit conversion block (only for metric languages) ---
  const unitSection = needsMetric ? `CONVERT IMPERIAL UNITS TO METRIC. This is mandatory for ${langName} — never keep imperial units, and never translate "inch/inches" to "Zoll/pouce/pulgadas" etc.:
   - inches / " / inch → cm  (multiply by 2.54, round sensibly)
   - feet / ft → m  (multiply by 0.305)
   - lbs / lb / pounds → kg  (multiply by 0.454)
   - oz / ounces → g  (multiply by 28.35)
   - °F → °C  ((F−32) × 5/9)
   - miles / mi → km  (multiply by 1.61)
   Worked examples: "20 inches" → "51 cm", "26 inches" → "66 cm", "4 lbs" → "1.8 kg", "70°F" → "21°C", "Queen (20\\"×30\\")" → "Queen (51 × 76 cm)".

` : '';

  // --- Brand replacement block ---
  // Spell the brand letter-by-letter to force precise rendering even on small tags.
  let brandSection = '';
  if (brandName) {
    const letters = brandName.split('');
    const spelledDash = letters.join('-');
    const spelledComma = letters.join(', ');
    const upper = brandName.toUpperCase();
    const firstLetterUpper = brandName.charAt(0).toUpperCase();

    // Build a short "do not produce" list of the most common near-miss garbles
    // (one-letter substitutions + O↔0 + I↔l).
    // This biases the model away from the specific failure modes we've seen.
    const COMMON_LOOKALIKES = { O: '0', I: '1', l: '1', S: '5', B: '8', G: '6', Z: '2' };
    const misspellings = [];
    for (let i = 0; i < letters.length && misspellings.length < 4; i++) {
      const ch = letters[i];
      const sub = COMMON_LOOKALIKES[ch.toUpperCase()];
      if (sub) {
        const garbled = upper.substring(0, i) + sub + upper.substring(i + 1);
        misspellings.push(garbled);
      }
    }
    // Always include a couple of adjacent-letter-drop garbles
    if (letters.length >= 4) {
      misspellings.push(upper.substring(0, 2) + upper.substring(3));       // drop 3rd letter
      misspellings.push(upper.substring(0, letters.length - 1));           // drop last letter
    }
    const misspellingList = misspellings.length
      ? ` Do NOT produce near-miss garbles like: ${misspellings.map(m => `"${m}"`).join(', ')}, or any other variant where letters are dropped, duplicated, swapped, or replaced.`
      : '';

    brandSection = `REPLACE BRAND NAME. The correct brand for this image is "${brandName}".
   - Find every brand name, logo, wordmark, label, pillow tag, or packaging text in the image that is NOT already "${brandName}" and replace it with "${brandName}". This applies to any source brand that appears (common examples include "mellow", "mellowsleep", or any other brand name) — detect it automatically.
   - EXACT SPELLING: The brand is spelled ${spelledDash} — that is ${letters.length} letters in this exact order: ${spelledComma} (uppercase form: ${upper}). Every occurrence must spell "${brandName}" correctly with every letter crisp and correctly formed. Do not invent approximate letter shapes, do not scramble letters, do not produce text that looks like "${brandName}" from a distance but is garbled up close.${misspellingList}
   - PREFER UPPERCASE. Render the brand as "${upper}" (all capital letters) wherever you can — uppercase letterforms are more legible at small sizes and harder to garble. Only keep mixed case if the original wordmark is clearly stylized mixed case and there is plenty of space.
   - MATCH SOURCE SCALE EXACTLY. The brand text in the output must occupy the SAME physical size, position, angle, perspective, and visual weight as the source's brand text. If the source shows a small 20×60px fabric care-tag, the output shows a small 20×60px fabric care-tag — NOT a hero banner, NOT an inflated label, NOT a promoted callout. Do not enlarge a tag to "make room" for letters. Do not relocate a tag from a corner to the center. Do not change an angled tag to face the camera. A small label that is hard to read in the source must stay a small label that is hard to read in the output.
   - SIZE-AWARE FALLBACK (only when the source's own text scale is too small for "${brandName}" to fit legibly — you still MUST NOT enlarge the tag):
       • Default → render the full "${upper}" at the source's scale in a clean geometric sans-serif (Inter, Helvetica, DIN, or similar). Every letter must be correctly shaped and spelled.
       • If the source-scale area genuinely cannot fit all ${letters.length} letters → render ONLY the monogram letter "${firstLetterUpper}" (single uppercase initial) at the same position and size as the source text. A clean single-letter monogram at source scale is better than a full word with wrong/scrambled letters.
       • If even a clean monogram will not fit legibly at source scale → leave the tag blank (solid color) with no text. A clean blank tag at source scale is better than unreadable scribbles OR an enlarged tag.
   - Preserve tag/background color, positioning, and shape (e.g. a purple cloth tag with white text stays purple with white text, just with the new content).
   - SELF-CHECK BEFORE FINALIZING: for every place you rendered brand text, mentally read it letter by letter. If the letters you see do not spell exactly ${spelledComma} (or ${firstLetterUpper} for a monogram), it is WRONG — re-render using the smaller/monogram fallback from the rule above. ALSO check: did I enlarge any tag, add a new label, or promote a small care-tag into a hero brand banner? If yes, that is WRONG — re-render at source scale.

`;
  }

  // --- Final assembly ---
  const header = `You are editing a product marketing image. Make EXACTLY the changes listed below — nothing else.

FIDELITY FIRST — read this before anything else:
You are NOT designing a new image. You are performing a MINIMAL text-only edit on an existing marketing image. The output must be indistinguishable from the source at first glance, differing only in the specific text swaps listed below.

DO NOT under any circumstances:
- ADD any text, label, badge, callout, size tag, dimension marker, or inset circle that is not in the source.
- ENLARGE any existing label, tag, or text element. A small care-tag in the source remains a small care-tag in the output.
- REPOSITION text from an edge/corner to a more prominent location.
- PROMOTE a subtle fabric tag into a hero product banner.
- INVENT specifications like "60 × 40 cm", "Maße:", "Größe:", model numbers, or care instructions that are not clearly present in the source.
- ADD the brand name to surfaces where no brand text was visible in the source (e.g. across the front of a pillow, onto a bedsheet, or onto a person's clothing).
- CHANGE the number of visible text elements. If the source has one small tag, the output has one small tag — not three, not a big banner plus a tag.
- RE-COMPOSE, RE-STYLE, or CLEAN UP the image. Keep backgrounds, lighting, perspective, and cropping identical.

If you are tempted to add a label, enlarge a tag, or improve legibility by making text bigger, STOP — that is out of scope. Fidelity to the source wins over "making the brand stand out."

`;

  const rules = `STRICT RULES — do not break these:
- Keep the EXACT same layout, composition, and image structure.
- Keep product photos, backgrounds, lighting, perspective, cropping, and decorative elements 100% unchanged.
- Keep all non-text visual elements (icons, arrows, diagrams, people, products) untouched.
- Only modify text content as described above — do not move, resize, restyle, add, or remove any other element.
- Preserve the visual scale, position, angle, and prominence of every text element. A 20×60px care-tag stays a 20×60px care-tag in the output.
- If a badge or label has an icon, keep the icon and only change the text next to it.
- Do NOT fabricate product specifications, size labels, dimensions, or care instructions. If a piece of text is not clearly present in the source, it must not appear in the output.
- READABILITY IS NON-NEGOTIABLE: never output garbled, misspelled, half-formed, or scrambled letters in any language, at any size. Every rendered character — even on the smallest pillow tag or tiny thumbnail — must be a correctly shaped, correctly spelled letter. If a tag is so small that "${brandName || 'the brand'}" cannot fit legibly at source scale, use the monogram or blank fallback from the brand section above. NEVER render a full word with wrong letters. NEVER enlarge the tag to compensate. Garbled brand text is the single worst possible output — except for an enlarged or fabricated label, which is the second-worst. Avoid both at all costs.

Output the edited image as JPEG with maximum quality preservation.`;

  return header + translateSection + unitSection + brandSection + rules;
}

function download(url, destPath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(destPath);
    const client = url.startsWith('https') ? https : http;
    client.get(url, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        file.close();
        return download(res.headers.location, destPath).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        file.close();
        reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        return;
      }
      res.pipe(file);
      file.on('finish', () => { file.close(); resolve(); });
    }).on('error', (e) => { file.close(); reject(e); });
  });
}

function httpsJson(options, body = null) {
  return new Promise((resolve, reject) => {
    const postData = body ? JSON.stringify(body) : '';
    if (body) {
      options.headers = {
        ...options.headers,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(postData),
      };
    }
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, data: JSON.parse(data) }); }
        catch (e) { resolve({ status: res.statusCode, data }); }
      });
    });
    req.on('error', reject);
    if (body) req.write(postData);
    req.end();
  });
}

/**
 * Translate a single image URL using fal.ai Nano Banana Pro.
 * Returns a Buffer with the translated image, or null on failure.
 *
 * @param {string} promptAddition - Optional extra sentences appended to the base
 *   prompt. The self-review loop uses this to feed in corrections discovered
 *   from the canary image.
 */
async function translateImageWithFal(imageUrl, targetLanguage, brandName, falApiKey, retries = 2, promptAddition = '', costTracker = null) {
  const basePrompt = buildTranslatePrompt(targetLanguage, brandName);
  const prompt = promptAddition
    ? `${basePrompt}\n\nADDITIONAL GUIDANCE (from QA review of a previous render — obey strictly):\n${promptAddition}`
    : basePrompt;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await httpsJson({
        hostname: 'fal.run',
        path: '/fal-ai/nano-banana-pro/edit',
        method: 'POST',
        headers: { 'Authorization': `Key ${falApiKey}` },
      }, {
        prompt,
        image_urls: [imageUrl],
        aspect_ratio: 'auto',
        num_images: 1,
        output_format: 'jpeg',
      });

      // Synchronous response with result
      if (res.status === 200 && res.data?.images?.[0]?.url) {
        const tmpPath = path.join(require('os').tmpdir(), `fal-translate-${Date.now()}.jpg`);
        await download(res.data.images[0].url, tmpPath);
        const buf = fs.readFileSync(tmpPath);
        fs.unlinkSync(tmpPath);
        if (costTracker) {
          costTracker.recordFal({
            model: 'nano-banana-pro/edit',
            numImages: 1,
            context: `translate → ${targetLanguage}`,
          });
        }
        return buf;
      }

      // Queue response — poll for completion
      if (res.status === 200 && res.data?.request_id) {
        const requestId = res.data.request_id;
        for (let poll = 0; poll < 90; poll++) {
          await new Promise(r => setTimeout(r, 2000));
          const statusRes = await httpsJson({
            hostname: 'queue.fal.run',
            path: `/fal-ai/nano-banana-pro/edit/requests/${requestId}/status`,
            method: 'GET',
            headers: { 'Authorization': `Key ${falApiKey}` },
          });
          if (statusRes.data?.status === 'COMPLETED') {
            const resultRes = await httpsJson({
              hostname: 'queue.fal.run',
              path: `/fal-ai/nano-banana-pro/edit/requests/${requestId}`,
              method: 'GET',
              headers: { 'Authorization': `Key ${falApiKey}` },
            });
            if (resultRes.data?.images?.[0]?.url) {
              const tmpPath = path.join(require('os').tmpdir(), `fal-translate-${Date.now()}.jpg`);
              await download(resultRes.data.images[0].url, tmpPath);
              const buf = fs.readFileSync(tmpPath);
              fs.unlinkSync(tmpPath);
              if (costTracker) {
                costTracker.recordFal({
                  model: 'nano-banana-pro/edit',
                  numImages: 1,
                  context: `translate → ${targetLanguage} (queued)`,
                });
              }
              return buf;
            }
          } else if (statusRes.data?.status === 'FAILED') {
            throw new Error('fal.ai job failed: ' + JSON.stringify(statusRes.data).substring(0, 200));
          }
        }
        throw new Error('fal.ai: timeout after polling');
      }

      throw new Error(`fal.ai error ${res.status}: ${JSON.stringify(res.data).substring(0, 300)}`);

    } catch (err) {
      if (attempt < retries) {
        console.log(`    [translate] Retry ${attempt + 1}/${retries}: ${err.message?.substring(0, 100)}`);
        await new Promise(r => setTimeout(r, 3000));
      } else {
        throw err;
      }
    }
  }
}

/**
 * Translate an array of image URLs to the target language.
 * Returns array of { buffer, originalUrl } — buffer is null if translation failed.
 *
 * The first image acts as a "canary": we translate it, ask Claude to review the
 * output against a QA checklist, and if problems are found (garbled brand text,
 * leftover English words, unit-conversion misses, etc.), we append Claude's
 * suggested prompt-tightening and retry the canary — up to 2 retries — BEFORE
 * batching the remaining images. The refined prompt is then used for 2..N.
 *
 * This calibrates the prompt once per job so we don't waste 15× renders on a
 * prompt that was going to fail the same way every time.
 *
 * @param {string[]} imageUrls - Source image URLs
 * @param {string} targetLanguage - Language code: 'en', 'de', 'fr', 'es', 'it', 'nl', or 'same' (brand-only)
 * @param {string} brandName - Target brand name to replace any source brand with (e.g. 'Merivalo')
 * @param {string} falApiKey - fal.ai API key
 * @param {function} onProgress - Optional callback(index, total)
 * @param {object} opts - { selfReview: boolean, maxReviewRetries: number }
 */
async function translateProductImages(imageUrls, targetLanguage, brandName, falApiKey, onProgress, opts = {}) {
  const selfReview = opts.selfReview !== false; // default ON
  const maxReviewRetries = opts.maxReviewRetries ?? 2;
  const costTracker = opts.costTracker || null;

  // Lazy-require to avoid circular-import surprises
  const { reviewTranslatedImage } = require('./review-translated-image');

  const results = [];
  let calibratedPromptAddition = '';

  for (let i = 0; i < imageUrls.length; i++) {
    const url = imageUrls[i];
    if (onProgress) onProgress(i, imageUrls.length);
    try {
      console.log(`  [translate] Image ${i + 1}/${imageUrls.length}: ${url.substring(0, 80)}...`);
      let buffer = await translateImageWithFal(url, targetLanguage, brandName, falApiKey, 2, calibratedPromptAddition, costTracker);

      // Self-review only the canary (first image). Later images use the
      // calibrated prompt from this step.
      if (selfReview && i === 0) {
        let attempt = 0;
        while (attempt < maxReviewRetries) {
          console.log(`  [translate] Reviewing canary (image 1) for quality issues...`);
          const verdict = await reviewTranslatedImage(buffer, targetLanguage, brandName);

          if (verdict.acceptable) {
            console.log(`  [translate] Canary review: PASS`);
            break;
          }

          console.log(`  [translate] Canary review: FAIL — ${verdict.issues.length} issue(s):`);
          verdict.issues.slice(0, 4).forEach(iss => console.log(`    • ${iss}`));

          if (!verdict.promptAddition) {
            console.log(`  [translate] Reviewer returned no actionable fix — keeping current render`);
            break;
          }

          attempt++;
          // Accumulate so each retry builds on prior guidance
          calibratedPromptAddition = calibratedPromptAddition
            ? `${calibratedPromptAddition}\n${verdict.promptAddition}`
            : verdict.promptAddition;

          console.log(`  [translate] Retry ${attempt}/${maxReviewRetries} with tightened prompt...`);
          console.log(`    Added guidance: ${verdict.promptAddition.substring(0, 180)}${verdict.promptAddition.length > 180 ? '…' : ''}`);

          buffer = await translateImageWithFal(url, targetLanguage, brandName, falApiKey, 2, calibratedPromptAddition, costTracker);
        }

        if (calibratedPromptAddition) {
          console.log(`  [translate] Using calibrated prompt for remaining ${imageUrls.length - 1} image(s)`);
        }
      }

      results.push({ buffer, originalUrl: url });
      console.log(`  [translate] Image ${i + 1} done (${(buffer.length / 1024).toFixed(0)}KB)`);
    } catch (e) {
      console.warn(`  [translate] Image ${i + 1} failed (using original): ${e.message?.substring(0, 100)}`);
      results.push({ buffer: null, originalUrl: url });
    }
  }
  return results;
}

module.exports = { translateProductImages, LANGUAGE_NAMES };
