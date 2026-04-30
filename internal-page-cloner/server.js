const fs = require('fs');
const path = require('path');
const envPath = path.join(__dirname, '.env');
require('dotenv').config({ path: envPath, override: false });

const express = require('express');
const apiRoutes = require('./src/routes/api');
const { getStoreConfig, getStoreDefaults } = require('./src/shopify/automation');

const app = express();
const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '127.0.0.1';
const STORES_DIR = path.join(__dirname, 'stores');
const PLATFORM_API_URL = (process.env.PLATFORM_API_URL || 'http://127.0.0.1:8000').replace(/\/+$/, '');
const PLATFORM_API_KEY = process.env.PLATFORM_API_KEY || '';

// Middleware
app.use(express.json());

async function proxyPlatformRequest(req, res) {
  const prefix = '/api/platform';
  const original = new URL(req.originalUrl, `http://${req.headers.host || 'localhost'}`);
  const suffix = original.pathname.slice(prefix.length) || '/';
  const targetPath = suffix === '/' || suffix === '/health' ? suffix : `/api${suffix}`;
  const targetUrl = `${PLATFORM_API_URL}${targetPath}${original.search}`;

  const headers = {};
  for (const [key, value] of Object.entries(req.headers)) {
    if (!['host', 'connection', 'content-length', 'accept-encoding'].includes(key.toLowerCase())) {
      headers[key] = value;
    }
  }
  if (PLATFORM_API_KEY && !headers['x-api-key'] && !headers.authorization) {
    headers['x-api-key'] = PLATFORM_API_KEY;
  }

  const options = {
    method: req.method,
    headers
  };

  if (!['GET', 'HEAD'].includes(req.method.toUpperCase())) {
    options.body = JSON.stringify(req.body || {});
    options.headers['content-type'] = 'application/json';
  }

  try {
    const response = await fetch(targetUrl, options);
    const body = await response.text();
    const contentType = response.headers.get('content-type');
    if (contentType) res.setHeader('content-type', contentType);
    res.status(response.status).send(body);
  } catch (err) {
    res.status(503).json({
      error: 'Platform backend is unavailable',
      detail: err.message,
      target: PLATFORM_API_URL
    });
  }
}

// Platform API proxy. The browser keeps using localhost:3000 while the
// migrated FastAPI platform can run beside the cloner on port 8000.
app.use('/api/platform', proxyPlatformRequest);

// API routes FIRST (before static files)
app.use('/api', apiRoutes);

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'page-cloner' });
});

// Static files
app.use(express.static(path.join(__dirname, 'public')));

// OAuth callback for Shopify app install (captures access token)
const https = require('https');
app.get('/auth/callback', async (req, res) => {
  const { code, shop, hmac, state } = req.query;
  if (!code || !shop) return res.status(400).send('Missing code or shop');
  if (!/^[a-z0-9][a-z0-9-]*\.myshopify\.com$/i.test(shop)) {
    return res.status(400).send('Invalid shop domain');
  }

  console.log(`\n  [OAuth] Callback from ${shop}`);

  // Exchange code for access token
  const clientId = process.env.SHOPIFY_CLIENT_ID;
  const clientSecret = process.env.SHOPIFY_CLIENT_SECRET;
  if (!clientId || !clientSecret) {
    return res.status(500).send('Missing SHOPIFY_CLIENT_ID or SHOPIFY_CLIENT_SECRET');
  }

  const postData = JSON.stringify({
    client_id: clientId,
    client_secret: clientSecret,
    code
  });

  try {
    const token = await new Promise((resolve, reject) => {
      const options = {
        hostname: shop,
        path: '/admin/oauth/access_token',
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(postData) }
      };
      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(new Error(data)); } });
      });
      req.on('error', reject);
      req.write(postData);
      req.end();
    });

    console.log('  [OAuth] Got access token');
    console.log(`  [OAuth] Scope: ${token.scope}`);

    // Save to merivalo.json
    const storeFile = path.join(STORES_DIR, 'merivalo.json');
    if (fs.existsSync(storeFile)) {
      const config = JSON.parse(fs.readFileSync(storeFile, 'utf-8'));
      config.shopify_access_token = token.access_token;
      fs.writeFileSync(storeFile, JSON.stringify(config, null, 2));
      console.log(`  [OAuth] Token saved to merivalo.json`);
    }

    res.send('<html><body><h1>✅ Token captured!</h1><p>Access token saved. You can close this tab and go back to the page cloner.</p></body></html>');
  } catch (e) {
    console.error('  [OAuth] Error:', e.message);
    res.status(500).send('OAuth error: ' + e.message);
  }
});

// Shopify app entry point (redirects to OAuth)
app.get('/auth', (req, res) => {
  const { shop } = req.query;
  if (!shop) return res.status(400).send('Missing shop parameter');
  if (!/^[a-z0-9][a-z0-9-]*\.myshopify\.com$/i.test(shop)) {
    return res.status(400).send('Invalid shop domain');
  }
  const clientId = process.env.SHOPIFY_CLIENT_ID;
  if (!clientId) return res.status(500).send('Missing SHOPIFY_CLIENT_ID');
  const scopes = 'read_products,write_products,read_themes,write_themes,read_content,write_content,read_discounts,write_discounts';
  const redirectUri = process.env.SHOPIFY_REDIRECT_URI || `http://${HOST}:${PORT}/auth/callback`;
  const installUrl = `https://${shop}/admin/oauth/authorize?client_id=${clientId}&scope=${scopes}&redirect_uri=${encodeURIComponent(redirectUri)}`;
  res.redirect(installUrl);
});

// List available stores
app.get('/api/stores', (req, res) => {
  try {
    const configIds = new Set(Object.keys(getStoreDefaults()));
    if (fs.existsSync(STORES_DIR)) {
      for (const f of fs.readdirSync(STORES_DIR).filter(f => f.endsWith('.json'))) {
        const config = JSON.parse(fs.readFileSync(path.join(STORES_DIR, f), 'utf-8'));
        if (config.id) configIds.add(config.id);
      }
    }

    const stores = [...configIds].map(id => {
      const resolved = getStoreConfig(id);
      return {
        id: resolved.storeId,
        name: resolved.storeName,
        domain: resolved.publicDomain,
        connected: !!resolved.accessToken
      };
    });
    res.json(stores);
  } catch (e) {
    res.json([]);
  }
});

// Fallback: serve index.html for any non-API, non-static request
app.use((req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const server = app.listen(PORT, HOST, () => {
  console.log(`
  ╔══════════════════════════════════════════╗
  ║     Page Cloner                          ║
  ║     Running at http://${HOST}:${PORT}     ║
  ╚══════════════════════════════════════════╝
  `);

  // Show all configured stores
  try {
    const files = fs.readdirSync(STORES_DIR).filter(f => f.endsWith('.json'));
    files.forEach(f => {
      const config = JSON.parse(fs.readFileSync(path.join(STORES_DIR, f), 'utf-8'));
      const resolved = getStoreConfig(config.id);
      console.log(`  Store: ${config.name} (${resolved.storeDomain}) ${resolved.accessToken ? '✓' : '✗'}`);
    });
  } catch (e) {}
  console.log(`  API Key: ${process.env.ANTHROPIC_API_KEY ? 'configured' : 'missing'}`);
});

// Some hosted Node runtimes aggressively exit when the event loop looks idle
// during startup. The HTTP server should normally keep the process alive, but
// this tiny heartbeat makes the service lifecycle explicit and harmless.
const keepAlive = setInterval(() => {}, 60 * 60 * 1000);

server.on('error', (err) => {
  console.error(`  [Server] Failed to listen on ${HOST}:${PORT}: ${err.message}`);
  clearInterval(keepAlive);
  process.exit(1);
});
