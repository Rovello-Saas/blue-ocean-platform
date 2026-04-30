const https = require('https');

function getConfig() {
  const domain = process.env.SHOPIFY_STORE_DOMAIN;
  const token = process.env.SHOPIFY_ACCESS_TOKEN;

  if (!domain || !token) {
    throw new Error('Missing SHOPIFY_STORE_DOMAIN or SHOPIFY_ACCESS_TOKEN in .env');
  }

  return { domain, token };
}

/**
 * Make a Shopify Admin API request
 */
function shopifyRequest(method, path, body = null) {
  const { domain, token } = getConfig();

  return new Promise((resolve, reject) => {
    const options = {
      hostname: domain,
      path: `/admin/api/2024-01${path}`,
      method,
      headers: {
        'X-Shopify-Access-Token': token,
        'Content-Type': 'application/json'
      }
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (res.statusCode >= 400) {
            reject(new Error(`Shopify API ${res.statusCode}: ${JSON.stringify(parsed.errors || parsed)}`));
          } else {
            resolve(parsed);
          }
        } catch (e) {
          reject(new Error(`Shopify API ${res.statusCode}: ${data.substring(0, 500)}`));
        }
      });
    });

    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

/**
 * Get the active/main theme
 */
async function getActiveTheme() {
  const result = await shopifyRequest('GET', '/themes.json');
  const active = result.themes.find(t => t.role === 'main');
  if (!active) throw new Error('No active theme found');
  return active;
}

/**
 * Create or update a theme asset
 */
async function putThemeAsset(themeId, key, value) {
  return await shopifyRequest('PUT', `/themes/${themeId}/assets.json`, {
    asset: { key, value }
  });
}

/**
 * Get a theme asset
 */
async function getThemeAsset(themeId, key) {
  try {
    return await shopifyRequest('GET', `/themes/${themeId}/assets.json?asset[key]=${encodeURIComponent(key)}`);
  } catch (e) {
    return null; // Asset doesn't exist
  }
}

/**
 * Create a product
 */
async function createProduct(productData) {
  return await shopifyRequest('POST', '/products.json', {
    product: productData
  });
}

/**
 * Update a product
 */
async function updateProduct(productId, productData) {
  return await shopifyRequest('PUT', `/products/${productId}.json`, {
    product: productData
  });
}

/**
 * List products
 */
async function listProducts(limit = 50) {
  return await shopifyRequest('GET', `/products.json?limit=${limit}`);
}

module.exports = {
  shopifyRequest,
  getActiveTheme,
  putThemeAsset,
  getThemeAsset,
  createProduct,
  updateProduct,
  listProducts
};
