const puppeteer = require('puppeteer');
const path = require('path');
const fs = require('fs');

/**
 * Launch a fresh browser, scrape the page, close the browser.
 * No shared browser instance — avoids stale connection issues.
 */
async function scrapePage(url, jobDir) {
  console.log('  Launching browser...');
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
  });

  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1440, height: 900 });
    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });

    // Load page with JS enabled, but neutralize Google consent redirects
    // Inject script BEFORE page loads that blocks window.location changes to google.com
    await page.evaluateOnNewDocument(() => {
      // Override window.location.replace and assign to block Google redirects
      const _origReplace = window.location.replace.bind(window.location);
      const _origAssign = window.location.assign.bind(window.location);

      window.location.replace = function(url) {
        if (typeof url === 'string' && url.includes('google.com')) return;
        return _origReplace(url);
      };
      window.location.assign = function(url) {
        if (typeof url === 'string' && url.includes('google.com')) return;
        return _origAssign(url);
      };

      // Block window.open to google
      const _origOpen = window.open;
      window.open = function(url) {
        if (typeof url === 'string' && url.includes('google.com')) return null;
        return _origOpen.apply(window, arguments);
      };

      // Intercept setting window.location.href via a proxy on document.location
      // This catches: window.location = "https://google.com"
      // Note: this doesn't always work but covers many cases
      Object.defineProperty(window, '__locationProxy', {
        set: function(val) {
          if (typeof val === 'string' && val.includes('google.com')) return;
          window.location.href = val;
        }
      });
    });

    // Navigate to the URL with JS enabled
    console.log('  Navigating to URL...');
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });

    // Wait for JS to render
    await new Promise(r => setTimeout(r, 3000));

    // If Google hijacked us, go back with JS disabled and use that content
    if (page.url().includes('google.com') || page.url().includes('google.nl')) {
      console.log('  ⚠️ Google redirected us, loading without JS...');
      await page.setJavaScriptEnabled(false);
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
      await page.setJavaScriptEnabled(true);
      await new Promise(r => setTimeout(r, 2000));
    } else {
      // JS loaded fine, wait a bit more for dynamic content
      await new Promise(r => setTimeout(r, 2000));
    }

    // Auto-scroll to trigger lazy loading
    console.log('  Scrolling page...');
    await autoScroll(page);

    // Wait for REAL images to load. Skip placeholder data: URIs — they're `complete`
    // instantly (they're inline base64), which would otherwise cause this check to
    // pass the moment the page loads, before any lazy image actually hydrates.
    // We only care that non-placeholder imgs have a real src AND have finished loading.
    await page.waitForFunction(() => {
      const imgs = Array.from(document.querySelectorAll('img'));
      // An image counts as "real" if its current src is not a data: URI
      // OR if it has a lazy-load attribute with a real URL.
      const realImgs = imgs.filter(img => {
        const src = img.currentSrc || img.src || '';
        const ds = img.dataset || {};
        const hasLazyAttr = ds.src || ds.original || ds.lazySrc || ds.lazyloadSrc;
        const srcIsReal = src && !src.startsWith('data:');
        return srcIsReal || hasLazyAttr;
      });
      if (realImgs.length === 0) return true; // nothing to wait for
      return realImgs.every(img => {
        const src = img.currentSrc || img.src || '';
        if (src.startsWith('data:')) return false; // placeholder still in src — not hydrated
        return img.complete && img.naturalWidth > 0;
      });
    }, { timeout: 15000 }).catch(() => {});

    // One more scroll pass + short wait, for pages that only kick off lazy-load
    // a second time after initial hydration (some page builders do this).
    await autoScroll(page);
    await new Promise(r => setTimeout(r, 1500));

    // Take screenshot (viewport only if full page is too large, with fallback)
    console.log('  Taking screenshot...');
    const screenshotPath = path.join(jobDir, 'screenshot.png');
    try {
      await page.screenshot({
        path: screenshotPath,
        fullPage: true,
        type: 'jpeg',
        quality: 70
      });
    } catch (screenshotErr) {
      console.log('  ⚠️ Full-page screenshot too large, capturing viewport only');
      await page.screenshot({
        path: screenshotPath,
        fullPage: false,
        type: 'jpeg',
        quality: 70
      });
    }

    return { page, browser, screenshotPath };
  } catch (err) {
    await browser.close().catch(() => {});
    throw err;
  }
}

/**
 * Dismiss cookie consent popups, overlays, and modals
 */
async function dismissPopups(page) {
  await page.evaluate(() => {
    // Common cookie/consent button selectors
    const buttonSelectors = [
      '[class*="cookie"] button', '[id*="cookie"] button',
      '[class*="consent"] button', '[id*="consent"] button',
      '[class*="gdpr"] button',
      'button[class*="accept"]', 'button[class*="agree"]', 'button[class*="dismiss"]',
      'button[class*="close"]', 'button[aria-label*="close"]', 'button[aria-label*="Close"]',
      '[class*="popup"] button[class*="close"]',
      '.cc-dismiss', '.cc-allow', '#onetrust-accept-btn-handler',
      '[data-action="accept"]', '[data-testid*="accept"]',
    ];

    for (const sel of buttonSelectors) {
      const buttons = document.querySelectorAll(sel);
      buttons.forEach(btn => {
        const text = btn.textContent.toLowerCase();
        if (text.includes('accept') || text.includes('agree') || text.includes('dismiss') ||
            text.includes('close') || text.includes('got it') || text.includes('ok') ||
            text.includes('akzeptieren') || text.includes('zustimmen') || text.includes('schließen')) {
          btn.click();
        }
      });
    }

    // Remove common overlay elements
    const overlaySelectors = [
      '[class*="cookie-banner"]', '[class*="cookie-notice"]', '[class*="cookie-popup"]',
      '[class*="consent-banner"]', '[class*="consent-overlay"]',
      '[id*="cookie-banner"]', '[id*="consent-banner"]',
      '.fc-consent-root', '#onetrust-banner-sdk',
    ];
    overlaySelectors.forEach(sel => {
      document.querySelectorAll(sel).forEach(el => el.remove());
    });

    // Remove Google consent iframe if present
    document.querySelectorAll('iframe[src*="consent.google"]').forEach(el => el.remove());
  });

  // Small wait for any animations
  await new Promise(r => setTimeout(r, 500));
}

/**
 * Scroll the page gradually to trigger lazy-loading images
 */
async function autoScroll(page) {
  await page.evaluate(async () => {
    await new Promise((resolve) => {
      let totalHeight = 0;
      const distance = 400;
      const delay = 150;
      const maxScroll = 30000; // Safety limit
      const timer = setInterval(() => {
        const scrollHeight = document.body.scrollHeight;
        window.scrollBy(0, distance);
        totalHeight += distance;
        if (totalHeight >= scrollHeight || totalHeight >= maxScroll) {
          clearInterval(timer);
          window.scrollTo(0, 0);
          resolve();
        }
      }, delay);
    });
  });
}

module.exports = { scrapePage };
