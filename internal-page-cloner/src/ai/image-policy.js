/**
 * image-policy.js
 *
 * Maps an image purpose (from image-analyzer.classifyImagePurposes) to the
 * transformation policy the translate-images pipeline should apply.
 *
 * The policy fields are consumed by translate-images.js to:
 *   - skip translation entirely (before-after — handled by post-processor),
 *   - drop the image from the rendered page (logo-strip — false advertising),
 *   - apply face-swap to consistent identity (lifestyle-with-person),
 *   - inject extra fidelity guards (comparison-composite — freeze layout).
 */

const POLICIES = {
  'hero': {
    skip: false,
    dropFromOutput: false,
    faceSwap: false,
    extraInstruction: ''
  },
  'lifestyle-with-person': {
    skip: false,
    dropFromOutput: false,
    faceSwap: true,
    extraInstruction: ''
  },
  'callout-with-text': {
    skip: false,
    dropFromOutput: false,
    faceSwap: false,
    extraInstruction: 'This image is an annotated diagram with callouts pointing at parts of the product. Translate every callout label and any heading text. Do NOT move callouts, do NOT redraw arrows, do NOT change which part of the product each label points to.'
  },
  'comparison-composite': {
    skip: false,
    dropFromOutput: false,
    faceSwap: false,
    extraInstruction: 'This image is a comparison chart or side-by-side composite. Freeze the layout — do not re-compose the grid, do not move columns, do not add or remove rows. Translate text only.'
  },
  'product-only': {
    skip: false,
    dropFromOutput: false,
    faceSwap: false,
    extraInstruction: ''
  },
  'before-after': {
    skip: true,
    dropFromOutput: false,
    faceSwap: false,
    extraInstruction: '',
    skipReason: 'handled by deterministic before-after post-processor'
  },
  'logo-strip': {
    skip: true,
    dropFromOutput: true,
    faceSwap: false,
    extraInstruction: '',
    skipReason: 'press/award logos cannot be claimed by Movanella; image is removed from page'
  }
};

const DEFAULT_POLICY = POLICIES['product-only'];

function policyFor(purpose) {
  return POLICIES[purpose] || DEFAULT_POLICY;
}

module.exports = { policyFor, POLICIES };
