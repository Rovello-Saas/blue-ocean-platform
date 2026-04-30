/**
 * Shopify Automation — creates products, sets variants, uploads images,
 * builds Horizon templates, pushes, and publishes.
 *
 * All API helpers are copied directly from the working push-cloud-alignment-pillow.js script.
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
require('dotenv').config({ path: path.join(__dirname, '../../.env'), override: false });

const THEME_DIR = path.join(__dirname, '../../data/theme-staging');
const STORES_DIR = path.join(__dirname, '../../stores');

const STORE_DEFAULTS = {
  movanella: {
    id: 'movanella',
    name: 'Movanella',
    shopify_domain: 'qnkd5e-3r.myshopify.com',
    shopify_theme_id: '196660887895',
    public_domain: 'goveliqo.com',
    language: 'en',
    primary_color: '#07941a',
    dark_color: '#1b2d5b',
    accent_color: '#07941a'
  },
  merivalo: {
    id: 'merivalo',
    name: 'Merivalo',
    shopify_domain: 'pt2bny-0y.myshopify.com',
    shopify_theme_id: '198390874451',
    public_domain: 'merivalo.com',
    language: 'de',
    primary_color: '#3b2067',
    dark_color: '#3b2067',
    accent_color: '#e8845f'
  }
};

function getStoreDefaults() {
  return STORE_DEFAULTS;
}

function normalizeStoreConfig(config, requestedStoreId = 'movanella') {
  const id = config.id || requestedStoreId;
  const envPrefix = id.toUpperCase().replace(/[^A-Z0-9]/g, '_');
  const fallback = STORE_DEFAULTS[id] || STORE_DEFAULTS.movanella;
  const genericToken = id === 'movanella' ? process.env.SHOPIFY_ACCESS_TOKEN : '';

  return {
    accessToken: process.env[`${envPrefix}_SHOPIFY_ACCESS_TOKEN`] || config.shopify_access_token || genericToken,
    storeDomain: process.env[`${envPrefix}_SHOPIFY_STORE_DOMAIN`] || config.shopify_domain || fallback.shopify_domain,
    themeId: process.env[`${envPrefix}_SHOPIFY_THEME_ID`] || config.shopify_theme_id || fallback.shopify_theme_id,
    publicDomain: config.public_domain || fallback.public_domain,
    storeId: id,
    storeName: config.name || fallback.name,
    language: config.language || fallback.language,
    primaryColor: config.primary_color || fallback.primary_color,
    darkColor: config.dark_color || fallback.dark_color,
    accentColor: config.accent_color || fallback.accent_color
  };
}

// --- Credentials ---

function getStoreConfig(storeId = 'movanella') {
  const storeConfigPath = path.join(STORES_DIR, `${storeId}.json`);
  if (fs.existsSync(storeConfigPath)) {
    const config = JSON.parse(fs.readFileSync(storeConfigPath, 'utf-8'));
    return normalizeStoreConfig(config, storeId);
  }

  // Fallback: read from .env (backwards compatible)
  const envPath = path.join(__dirname, '../../.env');
  let accessToken = '';
  let storeDomain = '';

  if (fs.existsSync(envPath)) {
    const content = fs.readFileSync(envPath, 'utf-8');
    const tokenMatch = content.match(/^SHOPIFY_ACCESS_TOKEN=(.+)$/m);
    const storeMatch = content.match(/^SHOPIFY_STORE_DOMAIN=(.+)$/m);
    if (tokenMatch) accessToken = tokenMatch[1].trim();
    if (storeMatch) storeDomain = storeMatch[1].trim();
  }

  if (!storeDomain) storeDomain = 'qnkd5e-3r.myshopify.com';

  return normalizeStoreConfig({
    ...STORE_DEFAULTS[storeId],
    shopify_access_token: accessToken,
    shopify_domain: storeDomain
  }, storeId);
}

// --- API helpers (exact copy from push-cloud-alignment-pillow.js) ---

function graphql(query, variables = {}, storeId = 'movanella') {
  const { accessToken, storeDomain } = getStoreConfig(storeId);
  return new Promise((resolve, reject) => {
    const postData = JSON.stringify({ query, variables });
    const options = {
      hostname: storeDomain,
      path: '/admin/api/2025-01/graphql.json',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': accessToken,
        'Content-Length': Buffer.byteLength(postData)
      }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.errors) reject(new Error(JSON.stringify(parsed.errors)));
          else resolve(parsed.data);
        } catch (e) {
          reject(new Error('Status ' + res.statusCode + ': ' + data.substring(0, 300)));
        }
      });
    });
    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

function restApi(method, apiPath, body = null, storeId = 'movanella') {
  const { accessToken, storeDomain } = getStoreConfig(storeId);
  return new Promise((resolve, reject) => {
    const options = {
      hostname: storeDomain,
      path: `/admin/api/2025-01${apiPath}`,
      method,
      headers: {
        'X-Shopify-Access-Token': accessToken,
        'Content-Type': 'application/json'
      }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error(data.substring(0, 500))); }
      });
    });
    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

// --- Product operations ---

async function createProduct(handle, title, description, templateSuffix, storeId = 'movanella') {
  console.log(`  [Shopify] Creating product: ${title} (store: ${storeId})`);
  try {
    const result = await graphql(`
      mutation productCreate($input: ProductInput!) {
        productCreate(input: $input) {
          product { id handle title }
          userErrors { field message }
        }
      }
    `, {
      input: {
        title,
        handle,
        templateSuffix,
        descriptionHtml: description,
        status: 'DRAFT'
      }
    }, storeId);

    if (result.productCreate.userErrors.length > 0) {
      console.log('  [Shopify] Product may exist, updating template...');
      const search = await graphql(`{ products(first:1, query:"handle:${handle}") { nodes { id title } } }`, {}, storeId);
      if (search.products.nodes.length > 0) {
        const pid = search.products.nodes[0].id;
        await graphql(`mutation { productUpdate(input: { id: "${pid}", templateSuffix: "${templateSuffix}" }) { product { id } userErrors { message } } }`, {}, storeId);
        console.log('  [Shopify] Updated template suffix on existing product');
        return pid;
      }
    } else {
      console.log(`  [Shopify] Created: ${result.productCreate.product.title}`);
      return result.productCreate.product.id;
    }
  } catch (e) {
    console.error('  [Shopify] Create error:', e.message.substring(0, 300));
    throw e;
  }
}

async function setVariantsAndPricing(handle, variants, storeId = 'movanella') {
  console.log(`  [Shopify] Setting variants for: ${handle}`);
  const products = await restApi('GET', `/products.json?handle=${handle}`, null, storeId);
  if (!products.products || products.products.length === 0) {
    throw new Error(`Product not found: ${handle}`);
  }

  const productId = products.products[0].id;
  const hasMultiple = variants.length > 1;

  // Detect if variants have multi-option names like "Color / Size"
  const hasSlash = hasMultiple && variants.some(v => (v.option1 || v.name || '').includes(' / '));

  let options;
  let mappedVariants;

  if (hasSlash) {
    // Split "Color / Size" into two separate Shopify options
    const parts = variants.map(v => {
      const fullName = v.option1 || v.name || '';
      const split = fullName.split(' / ').map(s => s.trim());
      return { option1: split[0] || 'Default', option2: split[1] || 'Default' };
    });

    const option1Values = [...new Set(parts.map(p => p.option1))];
    const option2Values = [...new Set(parts.map(p => p.option2))];

    const sizePattern = /^(XS|S|M|L|XL|XXL|2XL|3XL|\d+)$/i;
    const opt2IsSize = option2Values.some(v => sizePattern.test(v));

    const option1Name = opt2IsSize ? 'Color' : (variants[0].optionName || 'Style');
    const option2Name = opt2IsSize ? 'Size' : 'Option';

    console.log(`  [Shopify] Split variants into ${option1Name} (${option1Values.length}) × ${option2Name} (${option2Values.length})`);

    // Shopify can't add new options to existing product via REST.
    // Delete and recreate the product with correct options from scratch.
    const existingProduct = products.products[0];
    const templateSuffix = existingProduct.template_suffix || handle;
    const title = existingProduct.title;
    const descHtml = existingProduct.body_html || '';

    console.log(`  [Shopify] Recreating product ${productId} with multi-option variants...`);
    const deleteResult = await restApi('DELETE', `/products/${productId}.json`, null, storeId);
    console.log(`  [Shopify] Delete result:`, JSON.stringify(deleteResult).substring(0, 200));

    // Wait a moment for Shopify to process the deletion
    await new Promise(r => setTimeout(r, 2000));

    const created = await restApi('POST', '/products.json', {
      product: {
        title,
        handle,
        template_suffix: templateSuffix,
        body_html: descHtml,
        status: 'draft',
        options: [{ name: option1Name }, { name: option2Name }],
        variants: variants.map((v, i) => ({
          option1: parts[i].option1,
          option2: parts[i].option2,
          price: v.price || '29.99',
          compare_at_price: v.compareAtPrice || null,
          sku: `${handle.substring(0,10).toUpperCase()}-${parts[i].option1}-${parts[i].option2}`.replace(/\s+/g, '-').toUpperCase().substring(0, 40),
          inventory_management: null,
          inventory_policy: 'continue'
        }))
      }
    }, storeId);

    if (created.errors) {
      console.error(`  [Shopify] Recreate error:`, JSON.stringify(created.errors).substring(0, 300));
      throw new Error('Failed to recreate product with multi-option variants: ' + JSON.stringify(created.errors));
    }
    console.log(`  [Shopify] Recreated product ${created.product.id} with ${created.product.variants.length} variants`);
    console.log(`  [Shopify] Options: ${created.product.options.map(o => o.name + '(' + o.values.length + ')').join(' × ')}`);
    return created.product.id;
  } else {
    // Single option
    const optionName = hasMultiple ? (variants[0].optionName || 'Size') : 'Title';
    options = hasMultiple ? [{ name: optionName }] : undefined;
    mappedVariants = variants.map(v => ({
      option1: v.option1 || v.name || 'Default',
      price: v.price || '29.99',
      compare_at_price: v.compareAtPrice || null,
      sku: v.sku || `${handle.toUpperCase()}-${(v.option1 || v.name || 'DEFAULT').replace(/\s+/g, '-').toUpperCase()}`,
      inventory_management: null,
      inventory_policy: 'continue'
    }));
  }

  await restApi('PUT', `/products/${productId}.json`, {
    product: {
      id: productId,
      options,
      variants: mappedVariants
    }
  }, storeId);

  console.log(`  [Shopify] Set ${mappedVariants.length} variant(s)`);
  return productId;
}

async function uploadImages(handle, imageUrls, storeId = 'movanella') {
  console.log(`  [Shopify] Uploading ${imageUrls.length} images for: ${handle}`);
  const products = await restApi('GET', `/products.json?handle=${handle}`, null, storeId);
  if (!products.products || products.products.length === 0) return;

  const productId = products.products[0].id;

  for (let i = 0; i < imageUrls.length; i++) {
    try {
      await restApi('POST', `/products/${productId}/images.json`, {
        image: {
          src: typeof imageUrls[i] === 'string' ? imageUrls[i] : imageUrls[i].src,
          position: i + 1
        }
      }, storeId);
      console.log(`  [Shopify] Uploaded image ${i + 1}/${imageUrls.length}`);
    } catch (e) {
      console.log(`  [Shopify] Image ${i + 1} error:`, e.message.substring(0, 100));
    }
    // Small delay to avoid rate limiting
    if (i < imageUrls.length - 1) await new Promise(r => setTimeout(r, 200));
  }
}

// --- Template (exact copy from push-cloud-alignment-pillow.js buildTemplate) ---

function buildHorizonTemplate(liquidContent) {
  return {
    sections: {
      main: {
        type: "product-information",
        blocks: {
          "media-gallery": {
            type: "_product-media-gallery",
            static: true,
            settings: {
              media_presentation: "grid",
              media_columns: "two",
              image_gap: 4,
              large_first_image: false,
              icons_style: "none",
              slideshow_controls_style: "counter",
              // Mobile gallery: thumbnail strip instead of dots, matching the
              // Merivalo cloud-alignment-pillow reference. Horizon's default
              // is "dots"; the reference template explicitly sets "thumbnails".
              slideshow_mobile_controls_style: "thumbnails",
              thumbnail_position: "right",
              thumbnail_width: 44,
              thumbnail_radius: 0,
              aspect_ratio: "adapt",
              constrain_to_viewport: true,
              media_fit: "contain",
              media_radius: 12,
              extend_media: true,
              zoom: true,
              video_loop: false,
              hide_variants: true,
              "padding-block-start": 0, "padding-block-end": 0,
              "padding-inline-start": 0, "padding-inline-end": 0
            },
            blocks: {}
          },
          "product-details": {
            type: "_product-details",
            static: true,
            settings: {
              width: "fill", custom_width: 100,
              width_mobile: "fill", custom_width_mobile: 100,
              height: "fit", details_position: "flex-start",
              gap: 28, sticky_details_desktop: true,
              inherit_color_scheme: true, color_scheme: "scheme-1",
              background_media: "none", video_position: "cover",
              background_image_position: "cover",
              border: "none", border_width: 1, border_opacity: 100, border_radius: 0,
              "padding-block-start": 0, "padding-block-end": 24,
              "padding-inline-start": 0, "padding-inline-end": 0
            },
            blocks: {
              group_header: {
                type: "group", name: "Header",
                settings: {
                  content_direction: "column", vertical_on_mobile: true,
                  horizontal_alignment: "flex-start", vertical_alignment: "center",
                  align_baseline: false,
                  horizontal_alignment_flex_direction_column: "flex-start",
                  vertical_alignment_flex_direction_column: "center",
                  gap: 12, width: "fill", custom_width: 100,
                  width_mobile: "fill", custom_width_mobile: 100,
                  height: "fit", inherit_color_scheme: true,
                  background_media: "none", border: "none",
                  "padding-block-start": 0, "padding-block-end": 0,
                  "padding-inline-start": 0, "padding-inline-end": 0
                },
                blocks: {
                  title_block: {
                    type: "text", name: "Product title",
                    settings: {
                      text: "<h5>{{ closest.product.title }}</h5>",
                      width: "100%", max_width: "normal", alignment: "left",
                      type_preset: "custom", font: "var(--font-subheading--family)",
                      font_size: "1.25rem", line_height: "normal",
                      letter_spacing: "normal", case: "none", wrap: "pretty",
                      color: "var(--color-foreground)", background: false,
                      "padding-block-start": 0, "padding-block-end": 0,
                      "padding-inline-start": 0, "padding-inline-end": 0
                    },
                    blocks: {}
                  },
                  price_block: {
                    type: "price",
                    settings: {
                      show_sale_price_first: true, show_installments: false,
                      show_tax_info: false, type_preset: "custom",
                      width: "fit-content", alignment: "left",
                      font: "var(--font-heading--family)", font_size: "1rem",
                      line_height: "normal", letter_spacing: "normal", case: "none",
                      color: "var(--color-foreground)",
                      "padding-block-start": 12, "padding-block-end": 12,
                      "padding-inline-start": 0, "padding-inline-end": 0
                    },
                    blocks: {}
                  },
                  description_block: {
                    type: "product-description", name: "Product description",
                    settings: {
                      text: "{{ closest.product.description }}",
                      width: "fit-content", max_width: "normal", alignment: "left",
                      type_preset: "rte", font: "var(--font-body--family)",
                      font_size: "1rem", line_height: "normal",
                      letter_spacing: "normal", case: "none", wrap: "pretty",
                      color: "var(--color-foreground)", background: false,
                      "padding-block-start": 0, "padding-block-end": 0,
                      "padding-inline-start": 0, "padding-inline-end": 0
                    },
                    blocks: {}
                  }
                },
                block_order: ["title_block", "price_block", "description_block"]
              },
              variant_picker: {
                type: "variant-picker",
                settings: {
                  variant_style: "buttons", show_swatches: true, alignment: "left",
                  "padding-block-start": 0, "padding-block-end": 0,
                  "padding-inline-start": 0, "padding-inline-end": 0
                },
                blocks: {}
              },
              buy_buttons: {
                type: "buy-buttons",
                settings: {
                  stacking: true, show_pickup_availability: false, gift_card_form: true,
                  "padding-block-start": 0, "padding-block-end": 0,
                  "padding-inline-start": 0, "padding-inline-end": 0
                },
                blocks: {
                  quantity: { type: "quantity", disabled: true, static: true, settings: {}, blocks: {} },
                  "add-to-cart": { type: "add-to-cart", static: true, settings: { style_class: "button" }, blocks: {} },
                  "accelerated-checkout": { type: "accelerated-checkout", static: true, settings: {}, blocks: {} }
                },
                block_order: []
              }
            },
            block_order: ["group_header", "variant_picker", "buy_buttons"]
          }
        },
        settings: {}
      },
      custom_liquid_cloned: {
        type: "custom-liquid",
        settings: {
          custom_liquid: liquidContent
        }
      },
      loox_reviews: {
        type: "apps",
        blocks: {
          loox_widget: {
            type: "shopify://apps/loox/blocks/loox-reviews/loox-dynamic-section",
            settings: {}
          }
        },
        block_order: ["loox_widget"],
        settings: {
          "padding-block-start": 0,
          "padding-block-end": 0
        }
      }
    },
    order: ["main", "custom_liquid_cloned", "loox_reviews"]
  };
}

// --- Template push ---

async function pushTemplate(templateSuffix, templateJson, storeId = 'movanella') {
  const { themeId } = getStoreConfig(storeId);
  const assetKey = `templates/product.${templateSuffix}.json`;
  const assetValue = JSON.stringify(templateJson, null, 2);

  console.log(`  [Shopify] Pushing template via API: ${assetKey} (store: ${storeId}, theme: ${themeId})`);

  try {
    const result = await restApi('PUT', `/themes/${themeId}/assets.json`, {
      asset: {
        key: assetKey,
        value: assetValue
      }
    }, storeId);

    if (result.errors) {
      throw new Error(JSON.stringify(result.errors));
    }

    console.log(`  [Shopify] Template pushed successfully via API`);
    return true;
  } catch (e) {
    console.error('  [Shopify] API push failed:', e.message.substring(0, 300));
    throw new Error('Template push failed: ' + e.message.substring(0, 200));
  }
}

// --- Publish ---

async function publishProduct(handle, storeId = 'movanella') {
  console.log(`  [Shopify] Publishing: ${handle} (store: ${storeId})`);
  const products = await restApi('GET', `/products.json?handle=${handle}`, null, storeId);
  if (!products.products || products.products.length === 0) {
    throw new Error(`Product not found for publishing: ${handle}`);
  }

  const productId = products.products[0].id;
  // Setting `status: 'active'` alone no longer publishes to the Online Store
  // in recent API versions — must also set `published: true` to attach the
  // product to the Online Store sales channel so the storefront URL resolves.
  const pubResult = await restApi('PUT', `/products/${productId}.json`, {
    product: {
      id: productId,
      status: 'active',
      published: true,
      published_scope: 'web'
    }
  }, storeId);

  if (pubResult?.product?.published_at) {
    console.log(`  [Shopify] published_at: ${pubResult.product.published_at}`);
  } else {
    console.warn(`  [Shopify] WARNING: published_at is null — product may not be live on storefront`);
  }

  const { storeDomain, publicDomain } = getStoreConfig(storeId);
  const storeSlug = storeDomain.replace('.myshopify.com', '');
  const productUrl = `https://${publicDomain}/products/${handle}`;
  const adminUrl = `https://admin.shopify.com/store/${storeSlug}/products/${productId}`;

  console.log(`  [Shopify] Published! ${productUrl}`);
  return { productUrl, adminUrl, productId };
}

module.exports = {
  createProduct,
  setVariantsAndPricing,
  uploadImages,
  buildHorizonTemplate,
  pushTemplate,
  publishProduct,
  getStoreConfig,
  getStoreDefaults,
  restApi,
  graphql
};
