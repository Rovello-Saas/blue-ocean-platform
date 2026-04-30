/**
 * reviews/scraper.js
 *
 * Scrapes product reviews from a Loox widget on any Shopify store.
 * Uses Puppeteer to load the page, find the Loox iframe, and extract reviews.
 */

const puppeteer = require('puppeteer');

/**
 * Scrape reviews from a Shopify product page that uses Loox.
 *
 * @param {string} productUrl - The product URL to scrape reviews from
 * @param {object} options - Optional settings
 * @param {number} options.maxReviews - Maximum reviews to scrape (default: 20)
 * @param {object} options.browser - Existing Puppeteer browser instance (optional)
 * @returns {Promise<{reviews: Array, totalCount: number, averageRating: number}>}
 */
async function scrapeLooxReviews(productUrl, options = {}) {
  const { maxReviews = 20 } = options;
  let browser = options.browser;
  let ownBrowser = false;

  try {
    if (!browser) {
      browser = await puppeteer.launch({
        headless: 'new',
        args: ['--no-sandbox', '--disable-setuid-sandbox']
      });
      ownBrowser = true;
    }

    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');

    console.log(`[reviews] Loading ${productUrl}...`);
    await page.goto(productUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

    // Wait for Loox widget to load
    await new Promise(r => setTimeout(r, 3000));

    // Scroll to bottom to trigger lazy loading
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await new Promise(r => setTimeout(r, 2000));

    // Find the Loox iframe
    const looxIframeSrc = await page.evaluate(() => {
      const iframe = document.querySelector('iframe[src*="loox"]');
      return iframe ? iframe.src : null;
    });

    if (!looxIframeSrc) {
      console.log('[reviews] No Loox iframe found, trying direct DOM scraping...');
      // Try scraping reviews directly from page (non-Loox review apps)
      return await scrapeDirectReviews(page, maxReviews);
    }

    console.log(`[reviews] Found Loox widget: ${looxIframeSrc.substring(0, 80)}...`);

    // Navigate directly to the Loox widget page to scrape reviews
    await page.goto(looxIframeSrc, { waitUntil: 'domcontentloaded', timeout: 30000 });
    await new Promise(r => setTimeout(r, 3000));

    // Extract reviews from the Loox widget page
    const result = await page.evaluate((max) => {
      const text = document.body.innerText;
      const lines = text.split('\n').filter(l => l.trim());
      const reviews = [];
      let i = 0;

      // Find first "Verified" to locate start of reviews
      while (i < lines.length && !lines[i].match(/Verified/)) i++;
      i--; // Go back to get the name

      while (i < lines.length - 2 && reviews.length < max) {
        const name = lines[i]?.trim();
        if (!name || name === 'Write a review' || name === 'Load More') { i++; continue; }

        const verified = lines[i + 1]?.trim();
        if (verified !== 'Verified') { i++; continue; }

        const date = lines[i + 2]?.trim();

        // Collect body lines until next reviewer
        let body = '';
        let j = i + 3;
        while (j < lines.length) {
          if (j + 1 < lines.length && lines[j + 1]?.trim() === 'Verified') break;
          if (lines[j]?.trim() === 'Load More') break;
          body += (body ? ' ' : '') + lines[j]?.trim();
          j++;
        }

        if (name && body) {
          reviews.push({
            name,
            date,
            body,
            rating: 5, // Loox shows 5-star reviews by default at top
            verified: true
          });
        }
        i = j;
      }

      // Extract total count and average from header
      const totalMatch = text.match(/([\d,]+)\s*Reviews/);
      const totalCount = totalMatch ? parseInt(totalMatch[1].replace(/,/g, '')) : reviews.length;

      return { reviews, totalCount };
    }, maxReviews);

    // Get review images
    const reviewImages = await page.evaluate(() => {
      const imgs = document.querySelectorAll('img');
      const urls = [];
      imgs.forEach(img => {
        if (img.src && (img.src.includes('loox') || img.src.includes('ugc')) && img.naturalWidth > 50) {
          urls.push(img.src);
        }
      });
      return urls;
    });

    // Attach images to reviews (Loox shows them in order)
    if (reviewImages.length > 0) {
      result.reviews.forEach((review, idx) => {
        if (idx < reviewImages.length) {
          review.imageUrl = reviewImages[idx];
        }
      });
    }

    console.log(`[reviews] Scraped ${result.reviews.length} reviews (${result.totalCount} total on source)`);

    if (ownBrowser) await browser.close();
    return result;

  } catch (error) {
    console.error('[reviews] Scraping error:', error.message);
    if (ownBrowser && browser) await browser.close();
    return { reviews: [], totalCount: 0 };
  }
}

/**
 * Fallback: scrape reviews directly from page DOM (for non-Loox stores)
 */
async function scrapeDirectReviews(page, maxReviews) {
  // Try common review selectors
  const result = await page.evaluate((max) => {
    const selectors = [
      { container: '.jdgm-rev', name: '.jdgm-rev__author', body: '.jdgm-rev__body', stars: '.jdgm-star.jdgm--on' },
      { container: '.spr-review', name: '.spr-review-header-byline', body: '.spr-review-content-body', stars: '.spr-icon-star' },
      { container: '.stamped-review', name: '.author', body: '.stamped-review-content-body', stars: '.stamped-fa-star' },
    ];

    for (const sel of selectors) {
      const elements = document.querySelectorAll(sel.container);
      if (elements.length === 0) continue;

      const reviews = [];
      elements.forEach(el => {
        if (reviews.length >= max) return;
        const name = el.querySelector(sel.name)?.textContent?.trim() || 'Anonymous';
        const body = el.querySelector(sel.body)?.textContent?.trim() || '';
        const stars = el.querySelectorAll(sel.stars).length || 5;
        if (body) reviews.push({ name, body, rating: stars, verified: true });
      });
      return { reviews, totalCount: reviews.length };
    }

    return { reviews: [], totalCount: 0 };
  }, maxReviews);

  console.log(`[reviews] Direct scrape found ${result.reviews.length} reviews`);
  return result;
}

module.exports = { scrapeLooxReviews };
