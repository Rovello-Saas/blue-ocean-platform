const https = require('https');
const fs = require('fs');
const path = require('path');

const SCOPES = 'write_themes,read_themes,write_products,read_products,write_discounts,read_discounts,write_inventory,read_inventory,read_online_store_navigation,write_online_store_navigation';
const REDIRECT_URI = 'http://localhost:3000/auth/callback';

/**
 * Get the Shopify OAuth authorization URL
 */
function getAuthUrl() {
  const shop = process.env.SHOPIFY_STORE_DOMAIN;
  const clientId = process.env.SHOPIFY_CLIENT_ID;

  return `https://${shop}/admin/oauth/authorize?client_id=${clientId}&scope=${SCOPES}&redirect_uri=${encodeURIComponent(REDIRECT_URI)}`;
}

/**
 * Exchange the authorization code for an access token
 */
function exchangeCodeForToken(code) {
  const shop = process.env.SHOPIFY_STORE_DOMAIN;
  const clientId = process.env.SHOPIFY_CLIENT_ID;
  const clientSecret = process.env.SHOPIFY_CLIENT_SECRET;

  return new Promise((resolve, reject) => {
    const postData = JSON.stringify({
      client_id: clientId,
      client_secret: clientSecret,
      code: code
    });

    const options = {
      hostname: shop,
      path: '/admin/oauth/access_token',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': postData.length
      }
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.access_token) {
            // Save the token to .env
            saveAccessToken(parsed.access_token);
            resolve(parsed.access_token);
          } else {
            reject(new Error('No access_token in response: ' + data));
          }
        } catch (e) {
          reject(new Error('Failed to parse token response: ' + data.substring(0, 500)));
        }
      });
    });

    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

/**
 * Save the access token to .env and process.env
 */
function saveAccessToken(token) {
  process.env.SHOPIFY_ACCESS_TOKEN = token;

  // Also update the .env file
  const envPath = path.join(__dirname, '../../.env');
  let envContent = fs.readFileSync(envPath, 'utf-8');

  if (envContent.includes('SHOPIFY_ACCESS_TOKEN=')) {
    envContent = envContent.replace(/^SHOPIFY_ACCESS_TOKEN=.*$/m, `SHOPIFY_ACCESS_TOKEN=${token}`);
  } else {
    envContent += `\nSHOPIFY_ACCESS_TOKEN=${token}\n`;
  }

  fs.writeFileSync(envPath, envContent);
  console.log('  [Shopify] Access token saved to .env');
}

/**
 * Check if we have a valid access token
 */
function hasAccessToken() {
  const token = process.env.SHOPIFY_ACCESS_TOKEN;
  return token && token.length > 10 && token.startsWith('shpat_');
}

module.exports = { getAuthUrl, exchangeCodeForToken, hasAccessToken };
