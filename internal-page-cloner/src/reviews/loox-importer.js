/**
 * reviews/loox-importer.js
 *
 * Generates a Loox-compatible CSV file for review import.
 * Loox doesn't have a public import API, so we generate the CSV
 * in the exact format Loox requires and provide the import path.
 *
 * CSV Format (from Loox docs):
 *   product_handle, rating, author, body, created_at, email, verified_purchase, photo_url
 */

const fs = require('fs');
const path = require('path');

/**
 * Generate a Loox-compatible CSV file from translated reviews.
 *
 * @param {Array} reviews - Array of {name, body, rating, date, imageUrl?}
 * @param {string} productHandle - Shopify product handle
 * @param {string} outputDir - Directory to save the CSV file
 * @param {object} options - Optional settings
 * @param {string} options.productTitle - Product title for reference
 * @returns {string} - Path to the generated CSV file
 */
function generateLooxCSV(reviews, productHandle, outputDir, options = {}) {
  // CSV header per Loox docs
  let csv = 'product_handle,rating,author,body,created_at,email,verified_purchase,photo_url\n';

  reviews.forEach(review => {
    const escapedBody = review.body.replace(/"/g, '""');
    const email = review.name.toLowerCase().replace(/[^a-z]/g, '') + '@email.com';
    const date = normalizeDate(review.date);
    const photoUrl = review.imageUrl || '';

    csv += `${productHandle},${review.rating || 5},"${review.name}","${escapedBody}",${date},"${email}",TRUE,"${photoUrl}"\n`;
  });

  const csvPath = path.join(outputDir, `loox-reviews-${productHandle}.csv`);
  fs.writeFileSync(csvPath, csv, 'utf-8');

  console.log(`[reviews] Generated Loox CSV with ${reviews.length} reviews: ${csvPath}`);
  return csvPath;
}

/**
 * Normalize date to YYYY-MM-DD format.
 * Handles: DD/MM/YYYY, MM/DD/YYYY, YYYY-MM-DD, etc.
 */
function normalizeDate(dateStr) {
  if (!dateStr) {
    // Default to recent date
    const d = new Date();
    d.setDate(d.getDate() - Math.floor(Math.random() * 90)); // Random date within last 90 days
    return d.toISOString().split('T')[0];
  }

  // Already in YYYY-MM-DD format
  if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) return dateStr;

  // DD/MM/YYYY format (common in Loox widgets)
  const ddmmyyyy = dateStr.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (ddmmyyyy) return `${ddmmyyyy[3]}-${ddmmyyyy[2]}-${ddmmyyyy[1]}`;

  // MM/DD/YYYY format
  const mmddyyyy = dateStr.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (mmddyyyy) return `${mmddyyyy[3]}-${mmddyyyy[1]}-${mmddyyyy[2]}`;

  // Fallback
  try {
    const d = new Date(dateStr);
    if (!isNaN(d.getTime())) return d.toISOString().split('T')[0];
  } catch (e) {}

  return new Date().toISOString().split('T')[0];
}

/**
 * Full review pipeline: scrape → translate → generate CSV
 *
 * @param {string} sourceUrl - URL of the product to scrape reviews from
 * @param {string} productHandle - Handle for the target product
 * @param {string} language - Target language code
 * @param {string} productName - Product name in target language
 * @param {string} outputDir - Directory to save output files
 * @returns {Promise<{csvPath: string, reviewCount: number, reviews: Array}>}
 */
async function importReviews(sourceUrl, productHandle, language, productName, outputDir) {
  const { scrapeLooxReviews } = require('./scraper');
  const { translateReviews } = require('./translator');

  console.log(`[reviews] Starting review import pipeline for ${productHandle}`);

  // Step 1: Scrape reviews from source
  const { reviews: rawReviews, totalCount } = await scrapeLooxReviews(sourceUrl, { maxReviews: 20 });

  if (rawReviews.length === 0) {
    console.log('[reviews] No reviews found on source page');
    return { csvPath: null, reviewCount: 0, reviews: [] };
  }

  // Step 2: Translate if needed
  const translatedReviews = await translateReviews(rawReviews, language, productName);

  // Step 3: Generate Loox CSV
  fs.mkdirSync(outputDir, { recursive: true });
  const csvPath = generateLooxCSV(translatedReviews, productHandle, outputDir);

  // Also save JSON for reference
  const jsonPath = path.join(outputDir, `loox-reviews-${productHandle}.json`);
  fs.writeFileSync(jsonPath, JSON.stringify(translatedReviews, null, 2));

  console.log(`[reviews] ✅ Review import pipeline complete:`);
  console.log(`[reviews]   - Scraped: ${rawReviews.length} reviews (${totalCount} total on source)`);
  console.log(`[reviews]   - Translated: ${language !== 'en' ? 'Yes' : 'No'}`);
  console.log(`[reviews]   - CSV: ${csvPath}`);
  console.log(`[reviews]   - Import in Loox Admin: Reviews → Import → Upload CSV`);

  return {
    csvPath,
    jsonPath,
    reviewCount: translatedReviews.length,
    reviews: translatedReviews,
    totalSourceReviews: totalCount
  };
}

module.exports = { generateLooxCSV, importReviews };
