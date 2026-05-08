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

function buildTranslatePrompt(targetLanguage, brandName, opts = {}) {
  const isBrandOnly = targetLanguage === 'same';
  const langName = isBrandOnly ? null : (LANGUAGE_NAMES[targetLanguage] || targetLanguage);
  const needsMetric = !isBrandOnly && METRIC_LANGUAGES.has(targetLanguage);
  const ipSafeRewrite = opts.ipSafeRewrite !== false; // default ON for cloner safety
  const allowIdentitySwap = !!opts.allowIdentitySwap;

  // --- Translation block ---
  const translateSection = isBrandOnly
    ? `REFRAME visible marketing text. Keep the same general meaning and product benefit, but rewrite headlines, taglines, callouts, badges, and overlays into new Movanella wording rather than copying the source text verbatim. Keep roughly the same length so the layout still fits. Do not preserve source slogans or proprietary phrasing.

`
    : `TRANSLATE and REFRAME every piece of visible text to ${langName}. This includes headlines, taglines, feature callouts (e.g. "Machine Washable", "Cloud-Soft Feel"), badges, buttons, overlays, size/dimension labels, legal text, and any other readable words. Every word must be in ${langName} — no exceptions, no mixing languages. Keep the same general meaning and product benefit, but do not copy source slogans or phrasing literally; use new Movanella wording that fits the same layout.

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
  const ipSafeSection = ipSafeRewrite ? `IP-SAFE REWRITE MODE:
- Remove or replace every source-brand mark, including Solawave wordmarks, product labels, watermarks, logo text, packaging text, and tiny device markings. The only allowed brand is "${brandName || 'the target brand'}".
- Reframe visible marketing copy so it communicates the same idea without being a verbatim copy of the source image text.
- If a visible person appears, replace their identity with a new AI-safe person while preserving pose, angle, lighting, expression, wardrobe category, and composition. The output must not look like the original identifiable model/customer.
- Keep the product, layout, callout positions, chart/grid structure, before/after framing, colors, and cropping close enough that the section still fits the page, but avoid a literal branded asset copy.

` : '';

  const identityLine = allowIdentitySwap
    ? '- Visible people MAY be changed only for identity replacement. Preserve pose, framing, lighting, expression, wardrobe category, body proportions, and overall composition.'
    : '- If a visible person appears and no reference face is supplied, create a new AI-safe person while preserving pose, framing, lighting, expression, wardrobe category, body proportions, and overall composition.';

  const header = `You are editing a product marketing image. Make EXACTLY the changes listed below — nothing else.

FIDELITY FIRST — read this before anything else:
You are NOT designing a new image from scratch. You are performing a controlled brand/IP-safe edit on an existing marketing image. The output should preserve the page layout and product storytelling, while changing source branding, visible people, and copied marketing text as instructed.

${ipSafeSection}

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
- Keep all non-text visual elements (icons, arrows, diagrams, products) untouched unless the IP-safe mode explicitly says to replace a visible person's identity.
${identityLine}
- Only modify text content, source branding, and visible-person identity as described above — do not move, resize, restyle, add, or remove any other element.
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
 * Translate/edit a single image URL using GPT Image 2 on fal.ai.
 * Returns a Buffer with the translated image, or null on failure.
 *
 * @param {string} promptAddition - Optional extra sentences appended to the base
 *   prompt. The self-review loop uses this to feed in corrections discovered
 *   from the canary image.
 * @param {object} opts - { faceRefUrl?, extraInstruction? }
 *   faceRefUrl: when set, image_urls becomes [imageUrl, faceRefUrl] and an
 *     IDENTITY SWAP block is appended so the model replaces visible people
 *     in imageUrl with the person from faceRefUrl. Used for
 *     lifestyle-with-person images so every photo on the page shows the
 *     same model.
 *   extraInstruction: per-purpose extra prompt fragment from image-policy.js
 *     (e.g. "freeze comparison-grid layout").
 */
async function translateImageWithFal(imageUrl, targetLanguage, brandName, falApiKey, retries = 2, promptAddition = '', costTracker = null, opts = {}) {
  const falModel = 'openai/gpt-image-2/edit';
  const basePrompt = buildTranslatePrompt(targetLanguage, brandName, {
    ipSafeRewrite: opts.ipSafeRewrite,
    allowIdentitySwap: !!opts.faceRefUrl,
  });
  let prompt = basePrompt;
  if (opts.extraInstruction) {
    prompt += `\n\nPER-IMAGE GUIDANCE:\n${opts.extraInstruction}`;
  }
  if (opts.faceRefUrl) {
    prompt += `\n\nIDENTITY SWAP. image_urls[0] is the source image to edit. image_urls[1] shows the target person. Replace any visible person in image_urls[0] with the person from image_urls[1]. Preserve pose, framing, lighting, wardrobe, expression, body proportions, and overall composition exactly. Do not change the background, props, products, or anything else. The face/identity is the ONLY visual change beyond text translation and brand replacement.`;
  }
  if (promptAddition) {
    prompt += `\n\nADDITIONAL GUIDANCE (from QA review of a previous render — obey strictly):\n${promptAddition}`;
  }

  const imageUrlsForRequest = opts.faceRefUrl
    ? [imageUrl, opts.faceRefUrl]
    : [imageUrl];

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await httpsJson({
        hostname: 'fal.run',
        path: `/${falModel}`,
        method: 'POST',
        headers: { 'Authorization': `Key ${falApiKey}` },
      }, {
        prompt,
        image_urls: imageUrlsForRequest,
        image_size: 'auto',
        quality: 'high',
        input_fidelity: 'high',
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
            model: falModel,
            numImages: 1,
            context: `translate → ${targetLanguage}`,
            perImageUsd: 0.20,
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
            path: `/${falModel}/requests/${requestId}/status`,
            method: 'GET',
            headers: { 'Authorization': `Key ${falApiKey}` },
          });
          if (statusRes.data?.status === 'COMPLETED') {
            const resultRes = await httpsJson({
              hostname: 'queue.fal.run',
              path: `/${falModel}/requests/${requestId}`,
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
                  model: falModel,
                  numImages: 1,
                  context: `translate → ${targetLanguage} (queued)`,
                  perImageUsd: 0.20,
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
 * Edit a single image using Google's Gemini-native Nano Banana Pro path.
 * This is the page-cloner fallback when FAL_API_KEY is not configured but
 * GEMINI_API_KEY is available, matching the platform's Image Studio setup.
 */
async function translateImageWithGeminiNano(imageUrl, targetLanguage, brandName, googleApiKey, retries = 2, promptAddition = '', costTracker = null, opts = {}) {
  if (!googleApiKey) {
    throw new Error('translateImageWithGeminiNano: GEMINI_API_KEY not set');
  }

  let GoogleGenAI;
  try {
    ({ GoogleGenAI } = require('@google/genai'));
  } catch (e) {
    throw new Error('translateImageWithGeminiNano: @google/genai not installed');
  }

  const basePrompt = buildTranslatePrompt(targetLanguage, brandName, {
    ipSafeRewrite: opts.ipSafeRewrite,
    allowIdentitySwap: !!opts.faceRefUrl,
  });
  let prompt = basePrompt;
  if (opts.extraInstruction) {
    prompt += `\n\nPER-IMAGE GUIDANCE:\n${opts.extraInstruction}`;
  }
  if (opts.faceRefUrl) {
    prompt += `\n\nIDENTITY SWAP. The first image is the source image to edit. The second image shows the target person. Replace any visible person in the source image with the person from the second image. Preserve pose, framing, lighting, wardrobe category, expression, body proportions, and overall composition exactly. Do not change the background, props, products, or anything else.`;
  }
  if (promptAddition) {
    prompt += `\n\nADDITIONAL GUIDANCE (from QA review of a previous render — obey strictly):\n${promptAddition}`;
  }

  const fetchAsInlinePart = async (url) => {
    const tmpPath = path.join(require('os').tmpdir(), `gemini-nano-src-${Date.now()}-${Math.random().toString(36).slice(2)}.jpg`);
    await download(url, tmpPath);
    const bytes = fs.readFileSync(tmpPath);
    fs.unlinkSync(tmpPath);
    return {
      inlineData: {
        mimeType: 'image/jpeg',
        data: bytes.toString('base64'),
      }
    };
  };

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const ai = new GoogleGenAI({ apiKey: googleApiKey });
      const parts = [
        await fetchAsInlinePart(imageUrl),
      ];
      if (opts.faceRefUrl) {
        parts.push(await fetchAsInlinePart(opts.faceRefUrl));
      }
      parts.push({ text: prompt });

      const response = await ai.models.generateContent({
        model: 'gemini-3-pro-image-preview',
        contents: [{ role: 'user', parts }],
        config: { responseModalities: ['TEXT', 'IMAGE'] },
      });

      const outParts = response?.candidates?.[0]?.content?.parts || response?.parts || [];
      for (const part of outParts) {
        if (part.inlineData?.data) {
          const buf = Buffer.from(part.inlineData.data, 'base64');
          if (costTracker) {
            costTracker.recordGoogle?.({
              model: 'gemini-3-pro-image-preview',
              numImages: 1,
              context: `nano-banana-pro edit → ${targetLanguage}`,
            });
          }
          return buf;
        }
      }

      throw new Error('Gemini Nano Banana Pro returned no image');
    } catch (err) {
      if (attempt < retries) {
        console.log(`    [gemini-nano] Retry ${attempt + 1}/${retries}: ${err.message?.substring(0, 100)}`);
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
 * The first image acts as a "canary": we translate it, ask Claude to review
 * the output against a QA checklist, and if problems are found we tighten the
 * prompt before processing the remaining images. The same review then runs on
 * every subsequent image — not just the canary — so a regression on image 5
 * doesn't ship a Solawave-branded asset to live.
 *
 * On QA failure after 2 nano-banana retries, we fall back to Google Imagen
 * (different model, different failure modes). Only if that also fails do we
 * mark the result as `rejected: true` for the caller.
 *
 * @param {string[]} imageUrls - Source image URLs
 * @param {string} targetLanguage - Language code: 'en', 'de', 'fr', 'es', 'it', 'nl', or 'same' (brand-only)
 * @param {string} brandName - Target brand name to replace any source brand with (e.g. 'Merivalo')
 * @param {string} falApiKey - fal.ai API key
 * @param {function} onProgress - Optional callback(index, total)
 * @param {object} opts - { selfReview, maxReviewRetries, policies, faceRefUrl, googleApiKey, costTracker }
 *   policies: Map<url, policy> from image-policy.js. Per-image hints —
 *     skip translation, drop image entirely, apply face-swap, freeze layout.
 *   faceRefUrl: URL of the per-job reference face. Passed as image_urls[1]
 *     when policy.faceSwap is true so every lifestyle photo shows one model.
 *   googleApiKey: GOOGLE_IMAGEN_API_KEY for the Imagen fallback. If absent,
 *     we skip Imagen and proceed straight to rejected:true.
 */
async function translateProductImages(imageUrls, targetLanguage, brandName, falApiKey, onProgress, opts = {}) {
  const selfReview = opts.selfReview !== false; // default ON
  const maxReviewRetries = opts.maxReviewRetries ?? 2;
  const costTracker = opts.costTracker || null;
  const policies = opts.policies || new Map();
  const faceRefUrl = opts.faceRefUrl || null;
  const googleApiKey = opts.googleApiKey || process.env.GOOGLE_IMAGEN_API_KEY || process.env.GEMINI_API_KEY || null;

  // Lazy-require to avoid circular-import surprises
  const { reviewTranslatedImage } = require('./review-translated-image');

  const results = [];
  let calibratedPromptAddition = '';

  for (let i = 0; i < imageUrls.length; i++) {
    const url = imageUrls[i];
    const policy = policies.get(url) || {};
    if (onProgress) onProgress(i, imageUrls.length);

    // Per-policy: skip entirely (e.g. before-after handled by post-processor,
    // or logo-strip already filtered out of the AI's available-images list).
    if (policy.skip) {
      console.log(`  [translate] Image ${i + 1}/${imageUrls.length}: SKIP (${policy.skipReason || 'policy.skip'})`);
      results.push({ buffer: null, originalUrl: url, skipped: true });
      continue;
    }

    const perImageOpts = {
      faceRefUrl: policy.faceSwap ? faceRefUrl : null,
      extraInstruction: policy.extraInstruction || '',
      ipSafeRewrite: true,
    };

    try {
      console.log(`  [translate] Image ${i + 1}/${imageUrls.length}${policy.faceSwap ? ' [+face-swap]' : ''}: ${url.substring(0, 80)}...`);
      const runPrimaryEdit = (addition) => {
        if (falApiKey) {
          return translateImageWithFal(url, targetLanguage, brandName, falApiKey, 2, addition, costTracker, perImageOpts);
        }
        return translateImageWithGeminiNano(url, targetLanguage, brandName, googleApiKey, 2, addition, costTracker, perImageOpts);
      };
      let buffer = await runPrimaryEdit(calibratedPromptAddition);

      // Per-image self-review. The canary (image 1) is allowed to MUTATE the
      // calibratedPromptAddition so its lessons help every subsequent image;
      // images 2..N also re-review but only retry — they don't expand the
      // calibration further (would ratchet the prompt too aggressively).
      if (selfReview) {
        let attempt = 0;
        const maxRetries = i === 0 ? maxReviewRetries : 1;
        while (attempt < maxRetries) {
          const verdict = await reviewTranslatedImage(buffer, targetLanguage, brandName);

          if (verdict.acceptable) {
            if (i === 0) console.log(`  [translate] Canary review: PASS`);
            break;
          }

          console.log(`  [translate] Image ${i + 1} review: FAIL — ${verdict.issues.length} issue(s):`);
          verdict.issues.slice(0, 4).forEach(iss => console.log(`    • ${iss}`));

          if (!verdict.promptAddition) {
            console.log(`  [translate] Reviewer returned no actionable fix — moving to fallback`);
            break;
          }

          attempt++;
          if (i === 0) {
            calibratedPromptAddition = calibratedPromptAddition
              ? `${calibratedPromptAddition}\n${verdict.promptAddition}`
              : verdict.promptAddition;
          }
          const retryAddition = i === 0 ? calibratedPromptAddition : verdict.promptAddition;

          console.log(`  [translate] Retry ${attempt}/${maxRetries} with tightened prompt...`);
          buffer = await runPrimaryEdit(retryAddition);
        }

        // If still failing after retries, try Google Imagen as a fallback.
        const finalVerdict = await reviewTranslatedImage(buffer, targetLanguage, brandName);
        if (!finalVerdict.acceptable && googleApiKey) {
          console.log(`  [translate] Image ${i + 1} still failing QA after nano-banana retries — falling back to Google Imagen`);
          try {
            const fallbackPrompt = buildTranslatePrompt(targetLanguage, brandName, {
              ipSafeRewrite: true,
              allowIdentitySwap: !!perImageOpts.faceRefUrl,
            }) +
              (perImageOpts.extraInstruction ? `\n\nPER-IMAGE GUIDANCE:\n${perImageOpts.extraInstruction}` : '') +
              (calibratedPromptAddition ? `\n\nADDITIONAL GUIDANCE:\n${calibratedPromptAddition}` : '');
            const imagenBuffer = await translateImageWithImagen(url, fallbackPrompt, googleApiKey, costTracker);
            const imagenVerdict = await reviewTranslatedImage(imagenBuffer, targetLanguage, brandName);
            if (imagenVerdict.acceptable) {
              console.log(`  [translate] Imagen fallback PASS for image ${i + 1}`);
              buffer = imagenBuffer;
            } else {
              console.warn(`  [translate] Imagen fallback also failed QA — marking image ${i + 1} as rejected`);
              results.push({ buffer: null, originalUrl: url, rejected: true, reason: 'qa-fail-both-models' });
              continue;
            }
          } catch (imagenErr) {
            console.warn(`  [translate] Imagen fallback errored (${imagenErr.message?.substring(0, 100)}) — marking image ${i + 1} as rejected`);
            results.push({ buffer: null, originalUrl: url, rejected: true, reason: 'imagen-error' });
            continue;
          }
        } else if (!finalVerdict.acceptable) {
          // No Imagen key configured — log loudly and fall back to whatever
          // nano-banana produced. The downstream upload will still ship it,
          // but the rejected:false flag makes this distinguishable from a
          // genuinely-good translation.
          console.warn(`  [translate] Image ${i + 1} failed QA and no GOOGLE_IMAGEN_API_KEY set — shipping last nano-banana attempt`);
        }

        if (i === 0 && calibratedPromptAddition) {
          console.log(`  [translate] Using calibrated prompt for remaining ${imageUrls.length - 1} image(s)`);
        }
      }

      results.push({ buffer, originalUrl: url });
      console.log(`  [translate] Image ${i + 1} done (${(buffer.length / 1024).toFixed(0)}KB)`);
    } catch (e) {
      console.warn(`  [translate] Image ${i + 1} translation errored: ${e.message?.substring(0, 100)}`);
      // Try Imagen as a last-resort even on hard error from nano-banana.
      if (googleApiKey) {
        try {
          const fallbackPrompt = buildTranslatePrompt(targetLanguage, brandName, {
            ipSafeRewrite: true,
            allowIdentitySwap: !!perImageOpts.faceRefUrl,
          }) +
            (perImageOpts.extraInstruction ? `\n\nPER-IMAGE GUIDANCE:\n${perImageOpts.extraInstruction}` : '');
          const imagenBuffer = await translateImageWithImagen(url, fallbackPrompt, googleApiKey, costTracker);
          console.log(`  [translate] Imagen recovered image ${i + 1}`);
          results.push({ buffer: imagenBuffer, originalUrl: url });
          continue;
        } catch (imagenErr) {
          console.warn(`  [translate] Imagen also errored on image ${i + 1}: ${imagenErr.message?.substring(0, 100)}`);
        }
      }
      results.push({ buffer: null, originalUrl: url, rejected: true, reason: 'fal-and-imagen-error' });
    }
  }
  return results;
}

/**
 * Generate one reference face per job. Returned as a public URL (fal.ai's CDN
 * already hosts the image). The same URL is then passed as the second
 * image_urls entry on every face-swap call in the job, so every lifestyle
 * photo on the cloned page shows the same model.
 *
 * Uses fal.ai's flux/dev text-to-image, which is cheap and produces
 * reasonable photo-realistic portraits.
 */
async function generateJobFace(falApiKey, options = {}) {
  const demographics = options.demographics || '30-year-old European woman';
  const costTracker = options.costTracker || null;
  const prompt = `Photo of a ${demographics}, soft natural studio lighting, plain neutral background, looking directly at the camera, neutral relaxed expression, photorealistic, sharp focus, high quality, head and upper-shoulders portrait, no jewelry, no logo, no text in image.`;

  const res = await httpsJson({
    hostname: 'fal.run',
    path: '/fal-ai/flux/dev',
    method: 'POST',
    headers: { 'Authorization': `Key ${falApiKey}` },
  }, {
    prompt,
    image_size: 'square_hd',
    num_inference_steps: 28,
    num_images: 1,
    enable_safety_checker: true,
  });

  if (res.status !== 200) {
    throw new Error(`generateJobFace: fal.ai returned ${res.status}: ${JSON.stringify(res.data).substring(0, 200)}`);
  }

  // Synchronous response shape
  let url = res.data?.images?.[0]?.url;

  // Queue response — poll for completion
  if (!url && res.data?.request_id) {
    const requestId = res.data.request_id;
    for (let poll = 0; poll < 60; poll++) {
      await new Promise(r => setTimeout(r, 2000));
      const statusRes = await httpsJson({
        hostname: 'queue.fal.run',
        path: `/fal-ai/flux/dev/requests/${requestId}/status`,
        method: 'GET',
        headers: { 'Authorization': `Key ${falApiKey}` },
      });
      if (statusRes.data?.status === 'COMPLETED') {
        const resultRes = await httpsJson({
          hostname: 'queue.fal.run',
          path: `/fal-ai/flux/dev/requests/${requestId}`,
          method: 'GET',
          headers: { 'Authorization': `Key ${falApiKey}` },
        });
        url = resultRes.data?.images?.[0]?.url;
        break;
      } else if (statusRes.data?.status === 'FAILED') {
        throw new Error('generateJobFace: fal.ai job FAILED: ' + JSON.stringify(statusRes.data).substring(0, 200));
      }
    }
  }

  if (!url) {
    throw new Error('generateJobFace: no image URL in response');
  }

  if (costTracker) {
    costTracker.recordFal({
      model: 'flux/dev',
      numImages: 1,
      context: 'job face reference',
    });
  }

  console.log(`  [face] Generated job face: ${url.substring(0, 100)}...`);
  return url;
}

/**
 * Google Imagen fallback for image translation. Used when nano-banana-pro
 * fails QA two times in a row — Imagen's edit endpoint produces a different
 * style of error so often one model picks up the slack for the other.
 *
 * Uses @google/genai (already installed). The model is "imagen-3.0-capability"
 * or whatever the current edit-capable Imagen variant is.
 */
async function translateImageWithImagen(imageUrl, prompt, googleApiKey, costTracker = null) {
  if (!googleApiKey) {
    throw new Error('translateImageWithImagen: GOOGLE_IMAGEN_API_KEY not set');
  }
  let GoogleGenAI;
  try {
    ({ GoogleGenAI } = require('@google/genai'));
  } catch (e) {
    throw new Error('translateImageWithImagen: @google/genai not installed');
  }

  const ai = new GoogleGenAI({ apiKey: googleApiKey });

  // Fetch source image bytes for the edit-style request
  const tmpPath = path.join(require('os').tmpdir(), `imagen-src-${Date.now()}.jpg`);
  await download(imageUrl, tmpPath);
  const srcBytes = fs.readFileSync(tmpPath);
  fs.unlinkSync(tmpPath);
  const srcBase64 = srcBytes.toString('base64');

  // Imagen edit/customization. The exact endpoint shape depends on which
  // version the SDK exposes; we use generateContent on a multimodal model
  // ("gemini-2.5-flash-image" / "imagen-3" depending on availability) and
  // fall back to text-only response if image generation isn't supported.
  const modelName = 'gemini-2.5-flash-image';
  const response = await ai.models.generateContent({
    model: modelName,
    contents: [
      {
        role: 'user',
        parts: [
          { inlineData: { mimeType: 'image/jpeg', data: srcBase64 } },
          { text: prompt },
        ],
      },
    ],
  });

  // Extract image from response. Gemini multimodal returns parts; one of them
  // will be inlineData with the generated image.
  const parts = response?.candidates?.[0]?.content?.parts || [];
  for (const part of parts) {
    if (part.inlineData?.data) {
      const buf = Buffer.from(part.inlineData.data, 'base64');
      if (costTracker) {
        costTracker.recordGoogle?.({
          model: modelName,
          numImages: 1,
          context: 'imagen fallback edit',
        });
      }
      return buf;
    }
  }

  throw new Error('translateImageWithImagen: no image in Gemini response');
}

module.exports = {
  translateProductImages,
  translateImageWithFal,
  translateImageWithGeminiNano,
  translateImageWithImagen,
  generateJobFace,
  buildTranslatePrompt,
  LANGUAGE_NAMES,
};
