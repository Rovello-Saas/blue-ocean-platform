const fs = require('fs');
const path = require('path');

const LIBRARY_PATH = path.join(__dirname, '../../data/block-library.json');

function loadLibrary() {
  const raw = fs.readFileSync(LIBRARY_PATH, 'utf-8');
  return JSON.parse(raw);
}

function saveLibrary(library) {
  fs.writeFileSync(LIBRARY_PATH, JSON.stringify(library, null, 2));
}

function getBlock(blockId) {
  const library = loadLibrary();
  return library.blocks.find(b => b.id === blockId) || null;
}

function getAllBlocks() {
  const library = loadLibrary();
  return library.blocks;
}

function addBlock(block) {
  const library = loadLibrary();
  library.blocks.push(block);
  saveLibrary(library);
  return block;
}

function getAspectRatioRules() {
  const library = loadLibrary();
  return library.aspectRatioRules;
}

function classifyAspectRatio(width, height) {
  if (!width || !height) return 'unknown';
  const ratio = width / height;
  const rules = getAspectRatioRules();

  for (const [className, range] of Object.entries(rules.classifications)) {
    if (ratio >= range.min && ratio < range.max) {
      return className;
    }
  }
  return 'unknown';
}

module.exports = {
  loadLibrary,
  saveLibrary,
  getBlock,
  getAllBlocks,
  addBlock,
  getAspectRatioRules,
  classifyAspectRatio
};
