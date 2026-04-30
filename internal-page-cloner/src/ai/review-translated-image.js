/**
 * review-translated-image.js
 *
 * Self-review step for translated product images.
 *
 * After nano-banana-pro edits an image (translating text + replacing the brand),
 * we send the result to Claude with a QA checklist. Claude returns a verdict:
 *   - acceptable: bool
 *   - issues: list of concrete problems spotted (garbled letters, wrong language, etc.)
 *   - promptAddition: 1–3 extra sentences to append to the nano-banana prompt
 *                     that would specifically address those problems.
 *
 * The translate loop uses this to calibrate on the FIRST image before batching
 * the rest — so if the base prompt produces garbage, we tighten it once instead
 * of burning 16× bad renders.
 */

const { callClaudeWithImage, parseJsonResponse } = require('./client');

const LANGUAGE_NAMES = {
  en: 'English',
  de: 'German',
  fr: 'French',
  es: 'Spanish',
  it: 'Italian',
  nl: 'Dutch'
};

const METRIC_LANGUAGES = new Set(['de', 'fr', 'es', 'it', 'nl']);

/**
 * Review a translated image.
 *
 * @param {Buffer} imageBuffer - The JPEG buffer nano-banana-pro returned
 * @param {string} targetLanguage - Language code ('en', 'de', ...) or 'same' for brand-only
 * @param {string} brandName - Target brand (e.g. 'Merivalo')
 * @returns {Promise<{ acceptable: boolean, issues: string[], promptAddition: string }>}
 */
async function reviewTranslatedImage(imageBuffer, targetLanguage, brandName) {
  const isBrandOnly = targetLanguage === 'same';
  const lang = isBrandOnly ? null : (LANGUAGE_NAMES[targetLanguage] || targetLanguage);
  const needsMetric = !isBrandOnly && METRIC_LANGUAGES.has(targetLanguage);
  const upper = brandName ? brandName.toUpperCase() : null;

  const checklist = [];
  if (lang) checklist.push(`Every piece of visible text is in ${lang}. No leftover English, French, or any other non-${lang} words (including tiny badges, footer lines, size labels, and callouts).`);
  if (brandName) checklist.push(`Every brand wordmark / logo / tag text spells exactly "${brandName}" (uppercase form: "${upper}"). Read each one letter by letter. If any instance reads anything else — even a one-letter garble like "MEIVALO", "MERIVAIO", "MER1VALO", "MERIVAL0", or a fully unreadable scribble — it is a failure.`);
  if (brandName) checklist.push(`Small fabric tags / pillow labels / folded-corner labels either show "${upper}" cleanly, OR a clean single-letter "${brandName.charAt(0).toUpperCase()}" monogram, OR are blank. They must NEVER show garbled approximations of the brand.`);
  if (needsMetric) checklist.push(`All measurements are in metric units (cm, m, kg, g, °C, km). There should be no "inches", "inch", "lbs", "°F", or translated-but-still-imperial words like "Zoll".`);
  checklist.push(`No obviously corrupted / half-formed / partially-erased text anywhere.`);
  checklist.push(`Product photography, composition, background, icons, and non-text graphics look unchanged and natural (no visible AI smearing around edited regions).`);
  // Fidelity checks — catches the nano-banana failure mode where a small care-tag
  // gets inflated into a hero banner, or size/spec labels are invented wholesale.
  if (brandName) checklist.push(`FIDELITY — brand-tag scale: every brand label looks like a plausible real-world product marking for this category. A fabric care-tag on a pillow is SMALL (a few cm / <5% of the pillow surface) and sits at an edge or corner. It does NOT span the center of the pillow, does NOT appear as a large hero banner, and is NOT more prominent than in a normal product photo. If any "${upper}" marking looks oversized, overly prominent, or like a cover-art banner rather than a discreet tag, it is a fidelity failure.`);
  checklist.push(`FIDELITY — no fabricated content: the image should not contain size callouts ("60 × 40 cm", "Maße:", "Größe:"), dimension labels, model numbers, or care-instruction text that look invented. If a large inset circle, size stamp, or spec callout appears on the image and you cannot tell whether it belongs to the real product photography vs. an AI hallucination, flag it as a fidelity failure.`);
  checklist.push(`FIDELITY — no new brand placements: the brand name should NOT appear painted across the face of the pillow, on bedsheets, on clothing, or on any surface that would not normally carry a brand marking. Brand appears only where a real product tag / printed label / package text would be.`);

  const systemPrompt = `You are a meticulous QA reviewer for AI-edited product marketing images.

Your job: look at ONE edited image and decide whether it meets the quality bar. Be strict — marketing images with garbled text are commercially unusable.

Checklist:
${checklist.map((c, i) => `${i + 1}. ${c}`).join('\n')}

Respond with ONLY a JSON object (no markdown, no code fences), with this exact shape:
{
  "acceptable": true | false,
  "issues": ["concrete problem 1", "concrete problem 2", ...],
  "promptAddition": "1-3 extra sentences that, when appended to the image-editor's prompt, would fix the issues. Leave empty string if acceptable."
}

Rules:
- "acceptable": true only if EVERY checklist item passes. Any garbled brand text = false. Any non-target-language word = false.
- "issues": list the SPECIFIC things you see that are wrong. Reference what the text actually reads (e.g. "top-left tag reads 'MEIVALO' instead of 'MERIVALO'"). Use empty array if acceptable.
- "promptAddition": actionable, concrete instructions to add to the editor's prompt. Focus on the specific failure modes you saw. Examples of good additions:
    * "Pay extra attention to the small fabric tag in the top-right corner — use the monogram fallback there."
    * "The previous render confused V and I — double-check those letters."
    * "A size label slipped through in English ('Queen 20 inches'). Force German + metric on every size label."
    * "The previous render enlarged the care-tag into a full pillow banner. Keep the tag at its original small size (roughly 5% of the pillow surface, at the corner) — do not make the brand hero-sized."
    * "The previous render invented a 'Maße: 60 × 40 cm' callout that was not in the source. Do not add any size/dimension/spec text that wasn't clearly visible in the original."
    * "The previous render placed the brand name across the front of the pillow. Brand names must only appear on small fabric tags or existing label positions — never painted across the product surface."
  Avoid vague additions like "be more careful" — always name the specific region, letter, or fabrication.`;

  const userMessage = `Review this edited product image against the checklist and respond with the JSON verdict.`;

  try {
    const base64 = imageBuffer.toString('base64');
    const raw = await callClaudeWithImage(systemPrompt, base64, userMessage, {
      mediaType: 'image/jpeg',
      maxTokens: 800
    });
    const verdict = parseJsonResponse(raw);

    return {
      acceptable: !!verdict.acceptable,
      issues: Array.isArray(verdict.issues) ? verdict.issues : [],
      promptAddition: typeof verdict.promptAddition === 'string' ? verdict.promptAddition.trim() : ''
    };
  } catch (err) {
    // If review itself fails, default to "acceptable" so we don't block the pipeline.
    // Log the error for visibility.
    console.warn(`    [review] Self-review failed (defaulting to acceptable): ${err.message?.substring(0, 120)}`);
    return { acceptable: true, issues: [], promptAddition: '' };
  }
}

module.exports = { reviewTranslatedImage };
