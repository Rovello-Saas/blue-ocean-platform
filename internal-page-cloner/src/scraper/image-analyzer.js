const { classifyAspectRatio } = require('../blocks/library');

/**
 * Analyze and classify images from extracted sections
 * Adds aspect ratio classification to each image
 */
function analyzeImages(sections) {
  for (const section of sections) {
    for (const image of section.images) {
      // Use natural dimensions if available, fall back to display dimensions
      const width = image.naturalWidth || image.displayWidth;
      const height = image.naturalHeight || image.displayHeight;

      image.ratioClass = classifyAspectRatio(width, height);

      // Compute ratio if not already set
      if (!image.ratio && width && height) {
        image.ratio = +(width / height).toFixed(3);
      }
    }
  }
  return sections;
}

/**
 * Find the dominant/primary image in a section
 * (the largest image by display area)
 */
function findPrimaryImage(section) {
  if (!section.images || section.images.length === 0) return null;

  return section.images.reduce((best, img) => {
    const area = (img.displayWidth || 0) * (img.displayHeight || 0);
    const bestArea = (best.displayWidth || 0) * (best.displayHeight || 0);
    return area > bestArea ? img : best;
  });
}

/**
 * Get a summary of all images in a section for AI context
 */
function summarizeImages(section) {
  return section.images.map((img, i) => ({
    index: i,
    src: img.src,
    ratio: img.ratio,
    ratioClass: img.ratioClass,
    displaySize: `${img.displayWidth}x${img.displayHeight}`,
    alt: img.alt
  }));
}

module.exports = { analyzeImages, findPrimaryImage, summarizeImages };
