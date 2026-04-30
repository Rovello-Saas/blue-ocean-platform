const fs = require('fs');
const path = require('path');
const Handlebars = require('handlebars');

const TEMPLATES_DIR = path.join(__dirname, 'templates');
const templateCache = {};

function loadTemplate(templateName) {
  if (templateCache[templateName]) return templateCache[templateName];

  const templatePath = path.join(TEMPLATES_DIR, templateName);
  const raw = fs.readFileSync(templatePath, 'utf-8');
  const compiled = Handlebars.compile(raw);
  templateCache[templateName] = compiled;
  return compiled;
}

function renderFeaturesGrid(content) {
  const template = loadTemplate('features-grid.hbs');

  const data = {
    sectionTitle: content.sectionTitle,
    centerImage: content.centerImage,
    centerImageAlt: content.centerImageAlt || 'Product image',
    leftFeatures: content.features.slice(0, 2),
    rightFeatures: content.features.slice(2, 4)
  };

  return template(data);
}

function renderContentRow(content) {
  const template = loadTemplate('content-row.hbs');

  const isReverse = content.variant === 'reverse';
  const data = {
    cssClass: isReverse ? 'pcc-row pcc-row-reverse' : 'pcc-row',
    isReverse,
    image: content.image,
    imageAlt: content.imageAlt || 'Product feature',
    heading: content.heading,
    paragraph: content.paragraph
  };

  return template(data);
}

function renderTrustBadges() {
  const template = loadTemplate('trust-badges.hbs');
  return template({});
}

function renderBlock(blockId, content) {
  switch (blockId) {
    case 'features-grid-center-image':
      return renderFeaturesGrid(content);
    case 'content-row':
      return renderContentRow(content);
    case 'trust-badges':
      return renderTrustBadges();
    default:
      throw new Error(`Unknown block: ${blockId}`);
  }
}

module.exports = {
  renderBlock,
  renderFeaturesGrid,
  renderContentRow,
  renderTrustBadges
};
