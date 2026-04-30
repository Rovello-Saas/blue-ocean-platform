const fs = require('fs');
const path = require('path');
const { renderBlock } = require('../blocks/renderer');

const SHARED_STYLES_PATH = path.join(__dirname, '../blocks/templates/shared-styles.hbs');
const OUTPUT_DIR = path.join(__dirname, '../../data/output');

function loadSharedStyles() {
  return fs.readFileSync(SHARED_STYLES_PATH, 'utf-8');
}

/**
 * Assemble a complete .liquid file from matched blocks and their content.
 *
 * @param {Array} sections - Array of { blockId, content } objects in page order
 * @param {string} slug - Product slug for filename
 * @returns {string} The complete .liquid file content
 */
function assemblePage(sections, slug) {
  const styles = loadSharedStyles();

  const renderedBlocks = sections.map(section => {
    return renderBlock(section.blockId, section.content);
  });

  const page = `${styles}

<div class="pcc-wrapper" style="margin-top: 60px; margin-bottom: 60px;">

${renderedBlocks.join('\n\n')}

</div>
`;

  // Save to output directory
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const filename = `${slug}-content.liquid`;
  const outputPath = path.join(OUTPUT_DIR, filename);
  fs.writeFileSync(outputPath, page);

  return { content: page, filename, outputPath };
}

module.exports = { assemblePage };
