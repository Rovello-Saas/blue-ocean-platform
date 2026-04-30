const { callClaude, parseJsonResponse } = require('./client');
const { getAllBlocks } = require('../blocks/library');

const SYSTEM_PROMPT = `You are a Shopify block template matcher for the Movanella e-commerce store.

Given analyzed sections from a scraped product page and a block library, determine the best block template for each section.

BLOCK LIBRARY RULES:
1. PREFER matching to existing blocks over creating new ones
2. Only propose "create" if the section represents a genuinely reusable pattern (useful across multiple products)
3. Use "skip" for site-specific elements that add no value to Movanella's product pages
4. The "trust-badges" block is ALWAYS included as the last section (it's static and identical across all products)

IMAGE RATIO COMPATIBILITY:
- "features-grid-center-image" needs a PORTRAIT image (3:4, 2:3, 9:16). If the source has a landscape image, DO NOT use this block — use "content-row" instead
- "content-row" accepts any image ratio (portrait, square, landscape)
- If a section has a very wide landscape image (16:9 or wider), use "content-row" with natural aspect ratio

LAYOUT MAPPING:
- Source has icon + title + description items around a center image → "features-grid-center-image"
- Source has image beside text (any side) → "content-row"
- Source has trust/guarantee badges → skip (we always add our own "trust-badges")
- Source has a simple image above or below text → "content-row"

For ALTERNATING content rows, set the first one as "normal" variant and second as "reverse" variant.

For each analyzed section, output a JSON array:
{
  "sectionIndex": number,
  "action": "match" | "create" | "skip",
  "matchedBlockId": "features-grid-center-image" | "content-row" | "trust-badges" | null,
  "variant": null | "normal" | "reverse",
  "imageStrategy": {
    "sourceRatioClass": "landscape" | "portrait" | etc,
    "approach": "direct" | "crop-cover" | "natural-aspect"
  },
  "skipReason": null | "reason",
  "newBlockProposal": null | {
    "name": "proposed block name",
    "description": "what it does",
    "reason": "why existing blocks don't work"
  }
}

IMPORTANT: Always add trust-badges as the final block (action: "match", matchedBlockId: "trust-badges").

Return ONLY the JSON array.`;

/**
 * Match analyzed sections to blocks in the library
 */
async function matchBlocks(analyzedSections, scrapedSections) {
  const blocks = getAllBlocks();

  // Build context for the AI
  const blockSummary = blocks.map(b => ({
    id: b.id,
    name: b.name,
    description: b.description,
    category: b.category,
    imageConstraints: b.imageConstraints,
    matchingHints: b.matchingHints,
    variants: b.variants || null,
    static: b.static || false
  }));

  const sectionSummary = analyzedSections.map(section => {
    const scraped = scrapedSections.find(s => s.index === section.sectionIndex);
    return {
      ...section,
      images: scraped?.images?.map(img => ({
        src: img.src?.substring(0, 80),
        ratio: img.ratio,
        ratioClass: img.ratioClass,
        size: `${img.displayWidth}x${img.displayHeight}`
      })),
      paragraphs: scraped?.paragraphs?.slice(0, 3)
    };
  });

  const userMessage = `Match these analyzed sections to the best blocks from the library.

BLOCK LIBRARY:
${JSON.stringify(blockSummary, null, 2)}

ANALYZED SECTIONS:
${JSON.stringify(sectionSummary, null, 2)}

Return the matching JSON array. Remember to add trust-badges as the final block.`;

  const response = await callClaude(SYSTEM_PROMPT, userMessage);
  return parseJsonResponse(response);
}

module.exports = { matchBlocks };
