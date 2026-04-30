const {
  getActiveTheme,
  putThemeAsset,
  getThemeAsset,
  createProduct,
  updateProduct
} = require('./client');

/**
 * Push a cloned page to Shopify — fully automated:
 * 1. Create the product (or use an existing one)
 * 2. Create a product template JSON with the custom liquid section
 * 3. Assign the product to use that template
 *
 * @param {Object} options
 * @param {string} options.title - Product title
 * @param {string} options.liquidContent - The generated .liquid code
 * @param {string} options.slug - URL handle/slug
 * @param {number|null} options.existingProductId - If set, update this product instead of creating
 * @param {string} options.description - Product description (optional)
 * @param {string} options.price - Product price (optional, default "0.00")
 */
async function pushToShopify(options) {
  const {
    title,
    liquidContent,
    slug,
    existingProductId = null,
    description = '',
    price = '0.00'
  } = options;

  const templateSuffix = slug.toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-');

  console.log(`  [Shopify] Getting active theme...`);
  const theme = await getActiveTheme();
  console.log(`  [Shopify] Active theme: ${theme.name} (ID: ${theme.id})`);

  // Step 1: Create product template JSON with custom liquid section
  console.log(`  [Shopify] Creating product template: product.${templateSuffix}.json`);

  // Check if the default product template exists to use as base
  const defaultTemplate = await getThemeAsset(theme.id, 'templates/product.json');
  let templateJson;

  if (defaultTemplate && defaultTemplate.asset && defaultTemplate.asset.value) {
    // Parse the default template and inject our custom liquid section
    try {
      templateJson = JSON.parse(defaultTemplate.asset.value);

      // Add our custom liquid section after the main section
      templateJson.sections = templateJson.sections || {};

      // Add custom liquid section
      templateJson.sections['custom_liquid_cloned'] = {
        type: 'custom-liquid',
        settings: {
          custom_liquid: liquidContent
        }
      };

      // Make sure our section is in the order array
      if (templateJson.order) {
        // Insert after 'main' if it exists, otherwise at the end
        const mainIndex = templateJson.order.indexOf('main');
        if (mainIndex >= 0) {
          templateJson.order.splice(mainIndex + 1, 0, 'custom_liquid_cloned');
        } else {
          templateJson.order.push('custom_liquid_cloned');
        }
      }
    } catch (e) {
      console.log(`  [Shopify] Could not parse default template, creating minimal one`);
      templateJson = createMinimalTemplate(liquidContent);
    }
  } else {
    // No default template found, create a minimal one
    templateJson = createMinimalTemplate(liquidContent);
  }

  // Upload the template
  const templateKey = `templates/product.${templateSuffix}.json`;
  await putThemeAsset(theme.id, templateKey, JSON.stringify(templateJson, null, 2));
  console.log(`  [Shopify] Template created: ${templateKey}`);

  // Step 2: Create or update the product
  let product;

  if (existingProductId) {
    console.log(`  [Shopify] Updating product ${existingProductId}...`);
    const result = await updateProduct(existingProductId, {
      template_suffix: templateSuffix
    });
    product = result.product;
  } else {
    console.log(`  [Shopify] Creating product: ${title}...`);
    const result = await createProduct({
      title,
      body_html: description || `<p>${title}</p>`,
      handle: templateSuffix,
      template_suffix: templateSuffix,
      status: 'draft', // Start as draft so you can review
      variants: [{
        price,
        requires_shipping: true
      }]
    });
    product = result.product;
  }

  console.log(`  [Shopify] Product ${existingProductId ? 'updated' : 'created'}: ${product.title} (ID: ${product.id})`);

  return {
    productId: product.id,
    productTitle: product.title,
    productHandle: product.handle,
    templateSuffix,
    templateKey,
    adminUrl: `https://${process.env.SHOPIFY_STORE_DOMAIN}/admin/products/${product.id}`,
    previewUrl: `https://${process.env.SHOPIFY_STORE_DOMAIN}/products/${product.handle}`,
    status: 'draft'
  };
}

/**
 * Create a minimal product template JSON
 */
function createMinimalTemplate(liquidContent) {
  return {
    sections: {
      main: {
        type: 'main-product',
        blocks: {
          title: { type: 'title', settings: {} },
          price: { type: 'price', settings: {} },
          variant_picker: { type: 'variant_picker', settings: { picker_type: 'button' } },
          buy_buttons: { type: 'buy_buttons', settings: { show_dynamic_checkout: true } },
          description: { type: 'description', settings: {} }
        },
        block_order: ['title', 'price', 'variant_picker', 'buy_buttons', 'description'],
        settings: {}
      },
      custom_liquid_cloned: {
        type: 'custom-liquid',
        settings: {
          custom_liquid: liquidContent
        }
      }
    },
    order: ['main', 'custom_liquid_cloned']
  };
}

module.exports = { pushToShopify };
