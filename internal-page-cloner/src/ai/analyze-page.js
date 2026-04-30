const fs = require('fs');
const sharp = require('sharp');
const { callClaudeWithImage, parseJsonResponse } = require('./client');

const SYSTEM_PROMPT = `You are a product page layout analyst for a Shopify e-commerce store called Movanella.

Given a screenshot and DOM data of a product page, identify and classify each content section below the main product gallery and buy-box (add to cart area).

Focus on sections that contain:
- Product features/benefits with icons
- Image + text explanations
- Trust/confidence badges
- Specification tables
- Testimonials/reviews
- Comparison charts
- FAQ sections
- Hero/banner images with text

SKIP these (they are site chrome, not content):
- Navigation bars, headers, footers
- Product image galleries (the main buy-box photos)
- Add to cart / buy buttons / product forms
- Review widgets / star ratings sections
- Breadcrumbs
- Cookie banners / popups
- Related products carousels
- Newsletter signup forms

For each content section you identify, output a JSON array of objects with these fields:
{
  "sectionIndex": number (matching the DOM extraction index),
  "sectionType": one of ["features-grid", "content-row", "hero-banner", "specs-table", "testimonials", "comparison", "trust-badges", "faq", "video", "gallery", "unknown"],
  "confidence": 0.0 to 1.0,
  "description": "Brief description of what this section shows",
  "contentSummary": {
    "mainHeading": "The primary heading text if any",
    "subHeadings": ["list of sub-headings"],
    "imageCount": number,
    "dominantImageRatio": "portrait" | "landscape" | "square" | "mixed",
    "hasIcons": boolean,
    "hasFeatureList": boolean,
    "keyText": ["Important text snippets from this section"]
  },
  "reusable": true/false,
  "skipReason": null or "reason to skip this section"
}

Return ONLY the JSON array. No explanation text outside the JSON.`;

/**
 * Analyze a page's sections using Claude's vision + DOM data
 */
async function analyzePage(screenshotPath, sections) {
  // Resize screenshot to fit under 5MB API limit (base64 adds ~33% overhead)
  // Target: ~3MB raw = ~4MB base64, safely under 5MB
  const metadata = await sharp(screenshotPath).metadata();
  const maxHeight = 6000; // Crop very long pages
  const resizedBuffer = await sharp(screenshotPath)
    .resize({
      width: 800,
      height: maxHeight,
      fit: 'inside',
      withoutEnlargement: true
    })
    .jpeg({ quality: 50 })
    .toBuffer();

  console.log(`  Screenshot resized: ${(resizedBuffer.length / 1024 / 1024).toFixed(1)}MB`);
  const screenshotBase64 = resizedBuffer.toString('base64');

  // Build a compact summary of the DOM extraction
  const domSummary = sections.map(s => ({
    index: s.index,
    className: s.className.substring(0, 100),
    position: `top:${s.boundingRect.top}px, height:${s.boundingRect.height}px`,
    layout: {
      display: s.layout.display,
      columns: s.layout.gridTemplateColumns !== 'none' ? s.layout.gridTemplateColumns : undefined
    },
    images: s.images.map(img => ({
      src: img.src.substring(0, 100),
      ratio: img.ratio,
      ratioClass: img.ratioClass,
      size: `${img.displayWidth}x${img.displayHeight}`
    })),
    headings: s.headings,
    paragraphCount: s.paragraphs.length,
    firstParagraph: s.paragraphs[0]?.substring(0, 150)
  }));

  const userMessage = `Here is a product page screenshot and the extracted DOM section data.

Analyze the page and identify each content section (below the product gallery/buy-box).

DOM Sections Data:
${JSON.stringify(domSummary, null, 2)}

Return a JSON array classifying each section.`;

  const response = await callClaudeWithImage(SYSTEM_PROMPT, screenshotBase64, userMessage);
  return parseJsonResponse(response);
}

module.exports = { analyzePage };
