/**
 * DOM Extractor - runs inside Puppeteer's page context via page.evaluate()
 * Extracts content sections with images, headings, text, and layout info.
 */

/**
 * Extract structured content sections from a page
 * @param {import('puppeteer').Page} page - Puppeteer page object
 * @returns {Array} Array of section data objects
 */
async function extractSections(page) {
  return await page.evaluate(() => {
    const SKIP_SELECTORS = [
      'nav', 'header', 'footer',
      '[role="navigation"]', '[role="banner"]',
      '.header', '.footer', '.nav', '.navbar',
      '.cart', '.cart-drawer',
      '.shopify-section-header', '.shopify-section-footer',
      '.product-form', '.product__form',
      '.announcement-bar',
      'script', 'style', 'noscript'
    ];

    const SKIP_CLASSES = [
      'header', 'footer', 'nav', 'navbar', 'menu',
      // Keep review/testimonial/proof sections: PDPs like Solawave use those as
      // core content, and the generator needs them to mirror the source flow.
      'cart', 'breadcrumb', 'cookie',
      'popup', 'modal', 'overlay', 'announcement'
    ];

    function shouldSkip(el) {
      // Check tag/selector matches
      for (const sel of SKIP_SELECTORS) {
        try {
          if (el.matches(sel)) return true;
        } catch (e) {}
      }

      // Check class names
      const classList = el.className?.toString().toLowerCase() || '';
      for (const cls of SKIP_CLASSES) {
        if (classList.includes(cls)) return true;
      }

      return false;
    }

    function getLayoutInfo(el) {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
        gridTemplateRows: style.gridTemplateRows,
        flexDirection: style.flexDirection,
        gap: style.gap
      };
    }

    // Resolve the REAL image URL, preferring non-placeholder sources.
    // Many lazy-load libs (alcedohealth, various Shopify themes, PageFly, etc.) put
    // a `data:image/gif;base64,...` placeholder in `src` until the image scrolls into
    // view, with the real URL sitting in `data-src`, `data-srcset`, `data-original`,
    // `data-lazy-src`, `data-lazyload-src`, etc. Ignoring those means we miss ALL
    // lazy-loaded product images — which is exactly what alcedohealth does.
    function resolveRealSrc(img) {
      const candidates = [];
      const src = img.src || '';
      const ds = img.dataset || {};
      // Helper: pick the largest URL out of a srcset string
      const bestFromSrcset = (srcset) => {
        if (!srcset) return '';
        const parts = srcset.split(',').map(s => s.trim());
        const last = parts[parts.length - 1];
        return last ? last.split(/\s+/)[0] : '';
      };
      // Order: direct attributes first, then srcsets
      candidates.push(
        src,
        ds.src, ds.original, ds.lazySrc, ds.lazyloadSrc, ds.lazyload, ds.originalSrc,
        ds.imageSrc, ds.zoomSrc, ds.photoswipeSrc, ds.fullsizeSrc,
        bestFromSrcset(img.srcset),
        bestFromSrcset(ds.srcset),
        bestFromSrcset(ds.lazySrcset)
      );
      for (const c of candidates) {
        if (!c) continue;
        if (c.startsWith('data:')) continue; // placeholder
        return c;
      }
      return '';
    }

    function extractImages(el) {
      const images = [];
      const imgElements = el.querySelectorAll('img');

      imgElements.forEach(img => {
        const src = resolveRealSrc(img);
        if (!src || src.startsWith('data:image/svg') || src.includes('icon') || src.includes('logo')) return;

        const rect = img.getBoundingClientRect();
        // Skip tiny images (likely icons)
        if (rect.width < 80 && rect.height < 80) return;

        images.push({
          src,
          alt: img.alt || '',
          naturalWidth: img.naturalWidth || 0,
          naturalHeight: img.naturalHeight || 0,
          displayWidth: Math.round(rect.width),
          displayHeight: Math.round(rect.height),
          ratio: img.naturalWidth && img.naturalHeight
            ? +(img.naturalWidth / img.naturalHeight).toFixed(3)
            : 0
        });
      });

      // Also check for background images
      const bgElements = el.querySelectorAll('[style*="background-image"]');
      bgElements.forEach(bgEl => {
        const style = bgEl.getAttribute('style') || '';
        const match = style.match(/url\(['"]?(.*?)['"]?\)/);
        if (match && match[1] && !match[1].startsWith('data:')) {
          const rect = bgEl.getBoundingClientRect();
          if (rect.width >= 80 && rect.height >= 80) {
            images.push({
              src: match[1],
              alt: '',
              naturalWidth: 0,
              naturalHeight: 0,
              displayWidth: Math.round(rect.width),
              displayHeight: Math.round(rect.height),
              ratio: rect.width / rect.height,
              isBackground: true
            });
          }
        }
      });

      return images;
    }

    function extractHeadings(el) {
      const headings = [];
      const headingEls = el.querySelectorAll('h1, h2, h3, h4');
      headingEls.forEach(h => {
        const text = h.textContent?.trim();
        if (text && text.length > 0) {
          headings.push({
            level: parseInt(h.tagName.substring(1)),
            text
          });
        }
      });
      return headings;
    }

    function extractParagraphs(el) {
      const paragraphs = [];
      const pElements = el.querySelectorAll('p, .description, .text-content');
      pElements.forEach(p => {
        const text = p.textContent?.trim();
        if (text && text.length > 20) { // Skip very short text
          paragraphs.push(text);
        }
      });
      return paragraphs;
    }

    // Find content sections - try common container patterns
    const candidateSelectors = [
      'section',
      '[class*="section"]',
      'main > div',
      '.page-content > div',
      '.product-description > div',
      '.shopify-section',
      'article > div'
    ];

    let candidates = [];

    // Gather candidate elements
    for (const selector of candidateSelectors) {
      try {
        const els = document.querySelectorAll(selector);
        els.forEach(el => candidates.push(el));
      } catch (e) {}
    }

    // If we didn't find much, try broader selectors
    if (candidates.length < 3) {
      const mainContent = document.querySelector('main') || document.querySelector('.main-content') || document.body;
      const children = mainContent.children;
      for (let i = 0; i < children.length; i++) {
        candidates.push(children[i]);
      }
    }

    // Deduplicate and filter
    const seen = new Set();
    const sections = [];

    for (const el of candidates) {
      if (seen.has(el)) continue;
      seen.add(el);

      if (shouldSkip(el)) continue;

      const rect = el.getBoundingClientRect();
      // Skip elements that are too small
      if (rect.height < 100 || rect.width < 200) continue;

      const images = extractImages(el);
      const headings = extractHeadings(el);
      const paragraphs = extractParagraphs(el);

      // Skip sections with no meaningful content
      if (images.length === 0 && headings.length === 0 && paragraphs.length === 0) continue;

      // Get trimmed HTML (limit to 3000 chars to avoid huge payloads)
      let html = el.outerHTML;
      if (html.length > 3000) {
        html = html.substring(0, 3000) + '... [truncated]';
      }

      sections.push({
        index: sections.length,
        tagName: el.tagName.toLowerCase(),
        className: (el.className?.toString() || '').substring(0, 200),
        layout: getLayoutInfo(el),
        boundingRect: {
          top: Math.round(rect.top + window.scrollY),
          left: Math.round(rect.left),
          width: Math.round(rect.width),
          height: Math.round(rect.height)
        },
        images,
        headings,
        paragraphs,
        html
      });
    }

    // Sort by vertical position
    sections.sort((a, b) => a.boundingRect.top - b.boundingRect.top);

    // Re-index after sorting
    sections.forEach((s, i) => s.index = i);

    return sections;
  });
}

/**
 * Extract product metadata (title, price, variants, images) from a product page
 * Uses JSON-LD structured data as primary source, falls back to DOM
 */
async function extractProductMeta(page) {
  return await page.evaluate(() => {
    const meta = {
      title: '',
      price: '',
      compareAtPrice: null,
      currency: 'USD',
      description: '',
      variants: [],
      images: [],
      handle: ''
    };

    // Try JSON-LD first (most reliable for Shopify/e-commerce)
    const ldScripts = document.querySelectorAll('script[type="application/ld+json"]');
    for (const script of ldScripts) {
      try {
        const data = JSON.parse(script.textContent);
        const product = data['@type'] === 'Product' ? data :
          (Array.isArray(data['@graph']) ? data['@graph'].find(g => g['@type'] === 'Product') : null);

        if (product) {
          meta.title = product.name || meta.title;
          meta.description = product.description || meta.description;

          if (product.offers) {
            const offers = Array.isArray(product.offers) ? product.offers :
              (product.offers['@type'] === 'AggregateOffer' && product.offers.offers) ? product.offers.offers :
              [product.offers];

            if (offers.length > 0) {
              meta.price = offers[0].price || offers[0].lowPrice || '';
              meta.currency = offers[0].priceCurrency || 'USD';
            }

            meta.variants = offers.map((o, i) => ({
              name: o.name || `Variant ${i + 1}`,
              price: o.price || meta.price,
              sku: o.sku || ''
            }));
          }

          // Enrich variant names from Shopify analytics data (JSON-LD often lacks names)
          try {
            const shopifyVariants = window.ShopifyAnalytics?.meta?.product?.variants;
            if (shopifyVariants && shopifyVariants.length && meta.variants.length) {
              // Match by SKU or position
              meta.variants.forEach((v, i) => {
                const match = shopifyVariants.find(sv => sv.sku === v.sku) || shopifyVariants[i];
                if (match) {
                  v.name = match.public_title || match.name?.split(' - ').pop() || v.name;
                  v.price = match.price ? (match.price / 100).toString() : v.price;
                }
              });
            }
          } catch (e) {}

          if (product.image) {
            const imgs = Array.isArray(product.image) ? product.image : [product.image];
            meta.images = imgs.map((img, i) => ({
              src: typeof img === 'string' ? img : img.url || img.contentUrl || '',
              alt: typeof img === 'string' ? '' : img.name || '',
              position: i + 1,
              sourceRole: 'product-structured-data'
            })).filter(img => img.src);
          }
        }
      } catch (e) {}
    }

    // Fallback: DOM extraction
    if (!meta.title) {
      const titleEl = document.querySelector('h1[class*="product"], h1[class*="title"], .product-title h1, h1');
      meta.title = titleEl?.textContent?.trim() || '';
    }

    if (!meta.title) {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      meta.title = ogTitle?.content || document.title || '';
    }

    if (!meta.price) {
      // Try specific sale price element first
      const salePriceEl = document.querySelector('.price-item--sale, .price--sale .price-item, [class*="price"] [class*="sale"] .money, [class*="sale-price"] .money');
      if (salePriceEl) {
        meta.price = salePriceEl.textContent.replace(/[^0-9.]/g, '');
      } else {
        // Fallback: find first element that looks like a price (contains $ or € followed by numbers)
        const priceEls = document.querySelectorAll('.price-item, [class*="price"] .money, [class*="product-price"]');
        for (const el of priceEls) {
          const text = el.textContent.trim();
          const match = text.match(/[\$€£]?\s*(\d+[.,]\d{2})/);
          if (match) {
            meta.price = match[1].replace(',', '.');
            break;
          }
        }
      }
    }

    if (!meta.compareAtPrice) {
      const compareEl = document.querySelector('.price-item--regular, [class*="price"] del, [class*="price"] s, .price--compare, .compare-at-price');
      if (compareEl) {
        const match = compareEl.textContent.match(/[\$€£]?\s*(\d+[.,]\d{2})/);
        meta.compareAtPrice = match ? match[1].replace(',', '.') : null;
      }
    }

    if (!meta.description) {
      const descEl = document.querySelector('.product-description, [class*="product-description"], meta[property="og:description"]');
      meta.description = descEl?.textContent?.trim() || descEl?.content || '';
    }

    // Extract handle from URL EARLY so the image sweep below can use it to
    // filter out images inside <a href="/products/OTHER-HANDLE"> tiles.
    {
      const urlPath = window.location.pathname;
      const pathParts = urlPath.split('/').filter(Boolean);
      const productsIdx = pathParts.indexOf('products');
      if (productsIdx >= 0 && pathParts[productsIdx + 1]) {
        meta.handle = pathParts[productsIdx + 1];
      }
    }

    // Always scrape product images from DOM
    // Strategy: find all sizeable images from the store's CDN (works regardless of page builder)
    // IMPORTANT: must not pick up images from "related products", "you may also like",
    // "recently viewed", cross-sell/upsell tiles, reviews with user photos, etc.
    // Those are images of OTHER products that happen to sit on this product page.
    {
      const existingSrcs = new Set(meta.images.map(img => img.src.split('?')[0]));
      const hostname = window.location.hostname;
      const currentHandle = meta.handle; // set earlier from URL

      // Ancestor-class patterns that indicate "this is NOT the main product's content"
      const EXCLUDE_ANCESTOR_PATTERNS = [
        'related', 'recommend', 'you-may', 'you-might', 'also-like', 'also-bought',
        'recently-viewed', 'recently_viewed',
        'cross-sell', 'crosssell', 'upsell', 'up-sell',
        'complete-the-look', 'complete-your',
        'trending', 'bestsell', 'best-sell',
        'collection-list', 'collection-grid', 'product-grid',
        'featured-products', 'featured-collection',
        'review', 'testimonial',           // user-uploaded review photos
        'footer', 'header', 'nav', 'menu',
        'cart', 'drawer', 'modal', 'popup',
        'announcement'
      ];

      function hasExcludedAncestor(el) {
        let cur = el;
        // Walk up at most 12 levels; that's more than enough for any realistic layout.
        for (let i = 0; i < 12 && cur && cur !== document.body; i++) {
          const cls = (cur.className?.toString() || '').toLowerCase();
          const id = (cur.id || '').toLowerCase();
          const sectionId = (cur.getAttribute && cur.getAttribute('data-section-type')) || '';
          const hay = cls + ' ' + id + ' ' + sectionId.toLowerCase();
          for (const pat of EXCLUDE_ANCESTOR_PATTERNS) {
            if (hay.includes(pat)) return true;
          }
          cur = cur.parentElement;
        }
        return false;
      }

      function linksToOtherProduct(img) {
        // If the img is inside an <a> whose href points to a DIFFERENT /products/<handle>,
        // it's a related-product tile, not this product's own image.
        const a = img.closest && img.closest('a[href*="/products/"]');
        if (!a) return false;
        const href = a.getAttribute('href') || '';
        const m = href.match(/\/products\/([^/?#]+)/);
        if (!m) return false;
        const linkedHandle = m[1];
        if (!currentHandle) return false; // can't tell — be permissive
        return linkedHandle !== currentHandle;
      }

      function hasProductMediaAncestor(img) {
        let cur = img;
        for (let i = 0; i < 14 && cur && cur !== document.body; i++) {
          const cls = (cur.className?.toString() || '').toLowerCase();
          const id = (cur.id || '').toLowerCase();
          const aria = (cur.getAttribute && (cur.getAttribute('aria-label') || '')).toLowerCase();
          const role = (cur.getAttribute && (cur.getAttribute('role') || '')).toLowerCase();
          const hay = `${cls} ${id} ${aria} ${role}`;

          if (
            hay.includes('product') && (
              hay.includes('media') ||
              hay.includes('gallery') ||
              hay.includes('image') ||
              hay.includes('photo') ||
              hay.includes('thumb') ||
              hay.includes('slider') ||
              hay.includes('carousel') ||
              hay.includes('swiper') ||
              hay.includes('splide')
            )
          ) return true;

          if (
            hay.includes('product__media') ||
            hay.includes('product-media') ||
            hay.includes('product-gallery') ||
            hay.includes('product_gallery') ||
            hay.includes('media-gallery') ||
            hay.includes('product-form__media') ||
            hay.includes('thumbnail-list') ||
            hay.includes('product__thumb') ||
            hay.includes('product-thumbnail')
          ) return true;

          cur = cur.parentElement;
        }
        return false;
      }

      // Resolve real image URL — falls through placeholder data:image/gif to
      // data-src / data-srcset / etc. See explanation in extractImages above;
      // this copy is needed because each page.evaluate() has its own scope.
      function resolveRealSrc(img) {
        const candidates = [];
        const src = img.src || '';
        const ds = img.dataset || {};
        const bestFromSrcset = (srcset) => {
          if (!srcset) return '';
          const parts = srcset.split(',').map(s => s.trim());
          const last = parts[parts.length - 1];
          return last ? last.split(/\s+/)[0] : '';
        };
        candidates.push(
          src,
          ds.src, ds.original, ds.lazySrc, ds.lazyloadSrc, ds.lazyload, ds.originalSrc,
          ds.imageSrc, ds.zoomSrc, ds.photoswipeSrc, ds.fullsizeSrc,
          bestFromSrcset(img.srcset),
          bestFromSrcset(ds.srcset),
          bestFromSrcset(ds.lazySrcset)
        );
        for (const c of candidates) {
          if (!c) continue;
          if (c.startsWith('data:')) continue;
          return c;
        }
        return '';
      }

      const allImgs = document.querySelectorAll('img');

      allImgs.forEach((img) => {
        const src = resolveRealSrc(img);
        if (!src) return;

        // Only include images from the store's CDN (Shopify CDN pattern)
        const isStoreCDN = src.includes(hostname) || src.includes('cdn.shopify.com') || src.includes('/cdn/shop/');
        if (!isStoreCDN) return;

        // Skip tiny images (icons, logos, badges)
        const rect = img.getBoundingClientRect();
        const inProductMedia = hasProductMediaAncestor(img);
        const naturalW = img.naturalWidth || 0;
        const naturalH = img.naturalHeight || 0;
        const srcsetText = [
          img.srcset || '',
          img.dataset?.srcset || '',
          img.dataset?.lazySrcset || ''
        ].join(',');
        const hasLargeSource = naturalW >= 300 || naturalH >= 300 ||
          /(?:width|w)=(?:3\d\d|[4-9]\d\d|\d{4,})/i.test(src) ||
          /(?:\s|,)(?:3\d\d|[4-9]\d\d|\d{4,})w\b/i.test(srcsetText) ||
          /(?:width|w)=(?:3\d\d|[4-9]\d\d|\d{4,})/i.test(srcsetText);
        const visibleEnough = rect.width >= 150 && rect.height >= 150;
        const productMediaThumbnail = inProductMedia && rect.width >= 45 && rect.height >= 45 && hasLargeSource;
        if (!visibleEnough && !productMediaThumbnail) return;

        // Skip images with icon/logo in path
        if (src.includes('icon') || src.includes('logo') || src.includes('badge') || src.includes('payment')) return;

        // Skip images inside related/recommended/review/footer containers
        if (hasExcludedAncestor(img)) return;

        // Skip images inside links that point to a different product
        if (linksToOtherProduct(img)) return;

        // Get highest resolution from srcset if available (already checked in resolveRealSrc,
        // but re-check here because we want the LARGEST, not just the first non-placeholder)
        let bestSrc = src;
        if (img.srcset && !img.srcset.startsWith('data:')) {
          const srcsetParts = img.srcset.split(',').map(s => s.trim());
          const last = srcsetParts[srcsetParts.length - 1];
          if (last && !last.startsWith('data:')) bestSrc = last.split(' ')[0];
        }

        // Deduplicate by base URL (ignore query params)
        const baseSrc = bestSrc.split('?')[0];
        if (!existingSrcs.has(baseSrc)) {
          existingSrcs.add(baseSrc);
          meta.images.push({
            src: bestSrc,
            alt: img.alt || '',
            position: meta.images.length + 1,
            sourceRole: inProductMedia ? 'product-media-gallery' : 'page-image'
          });
        }
      });
    }

    // Extract variant option names from DOM (works when JS is disabled)
    if (meta.variants.length <= 1 || meta.variants.every(v => v.name.startsWith('Variant'))) {
      try {
        // Find ALL selects and look for one with product variant options
        const allSelects = document.querySelectorAll('select');
        for (const sel of allSelects) {
          const opts = Array.from(sel.options).filter(o => o.value && o.textContent.trim());

          // Skip selects with fewer than 2 real options or that look like country/currency/language selectors
          if (opts.length < 2) continue;
          const selectName = (sel.name || '').toLowerCase();
          const selectId = (sel.id || '').toLowerCase();
          const firstText = opts[0].textContent.trim().toLowerCase();
          if (firstText.includes('currency') || firstText.includes('country') ||
              selectName.includes('country') || selectName.includes('currency') || selectName.includes('locale') ||
              selectId.includes('country') || selectId.includes('currency') || selectId.includes('locale') ||
              opts.some(o => o.textContent.includes('USD') && o.textContent.includes('$)'))) continue;

          // Check if options contain price info (like "Blue / S - $36.99")
          const hasPriceInOption = opts.some(o => o.textContent.match(/\$|€|£/));

          if (hasPriceInOption || sel.name?.includes('id') || sel.closest('[class*="variant"], [class*="product-form"]')) {
            const variants = [];
            const label = sel.closest('label, fieldset, .product-form__input')?.querySelector('label, legend')?.textContent?.trim();

            opts.forEach(opt => {
              const text = opt.textContent.trim();
              if (!text || text === '' || opt.value === '') return;

              // Parse "Color / Size - $price" or "Color / Size\n- $price" format
              const cleanText = text.replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();
              const priceMatch = cleanText.match(/([\$€£])\s*(\d+[.,]\d{2})/);
              const nameMatch = cleanText.match(/^(.+?)(?:\s*-\s*[\$€£]|\s*[\$€£])/);

              const name = nameMatch ? nameMatch[1].trim() : cleanText.split(' - ')[0].trim();
              const price = priceMatch ? priceMatch[2].replace(',', '.') : meta.price;
              const soldOut = cleanText.toLowerCase().includes('sold out') || opt.disabled;

              if (name && !soldOut) {
                variants.push({ name, price, optionName: label || 'Style' });
              }
            });

            if (variants.length > 0) {
              meta.variants = variants;
              meta.optionName = variants[0].optionName;
              break; // Use first matching select
            }
          }
        }

        // Try radio buttons / swatches as fallback
        if (meta.variants.length <= 1) {
          const fieldsets = document.querySelectorAll('fieldset[class*="variant"], fieldset[class*="option"], .product-form__input');
          fieldsets.forEach(fs => {
            const label = fs.querySelector('legend, label, .form__label')?.textContent?.trim()?.replace(/:$/, '');
            const inputs = fs.querySelectorAll('input[type="radio"]');
            if (inputs.length > 1) {
              const options = [];
              inputs.forEach(inp => {
                const name = inp.value || inp.nextElementSibling?.textContent?.trim() || '';
                if (name) options.push({ name, optionName: label || 'Size', price: meta.price });
              });
              if (options.length > 0) {
                meta.variants = options;
                meta.optionName = label || 'Size';
              }
            }
          });
        }
      } catch (e) {}
    }

    // Clean up prices - remove currency symbols
    if (meta.price) meta.price = meta.price.toString().replace(/[^0-9.]/g, '');
    if (meta.compareAtPrice) meta.compareAtPrice = meta.compareAtPrice.toString().replace(/[^0-9.]/g, '');
    meta.variants.forEach(v => {
      if (v.price) v.price = v.price.toString().replace(/[^0-9.]/g, '');
    });

    // (handle already extracted earlier, before the DOM image sweep)

    return meta;
  });
}

module.exports = { extractSections, extractProductMeta };
