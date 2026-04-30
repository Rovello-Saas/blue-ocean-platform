const { execSync, exec } = require('child_process');
const fs = require('fs');
const path = require('path');

const THEME_DIR = path.join(__dirname, '../../data/theme-staging');
const STORE = process.env.SHOPIFY_STORE_DOMAIN;

/**
 * Ensure theme staging directory exists with basic structure
 */
function ensureThemeDir() {
  const dirs = ['templates', 'sections', 'config', 'layout'];
  for (const dir of dirs) {
    fs.mkdirSync(path.join(THEME_DIR, dir), { recursive: true });
  }
}

/**
 * Get the active theme ID via Shopify CLI
 */
function getActiveThemeId() {
  try {
    const output = execSync(`npx shopify theme list --store ${STORE} 2>&1`, {
      encoding: 'utf-8',
      timeout: 30000
    });
    // Parse output to find the [live] theme
    const lines = output.split('\n');
    for (const line of lines) {
      if (line.includes('[live]')) {
        const match = line.match(/#(\d+)/);
        if (match) return match[1];
      }
    }
    throw new Error('Could not find live theme ID from: ' + output);
  } catch (e) {
    throw new Error('Failed to get theme ID: ' + e.message);
  }
}

/**
 * Push a product template to the live theme via Shopify CLI
 */
function pushTemplate(templateSuffix, liquidContent) {
  ensureThemeDir();

  // Create the template JSON that includes our custom liquid section
  const templateJson = {
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

  const templatePath = path.join(THEME_DIR, 'templates', `product.${templateSuffix}.json`);
  fs.writeFileSync(templatePath, JSON.stringify(templateJson, null, 2));

  console.log(`  [Shopify CLI] Pushing template: product.${templateSuffix}.json`);

  // Get theme ID
  const themeId = getActiveThemeId();
  console.log(`  [Shopify CLI] Live theme ID: ${themeId}`);

  // Push only our template file
  try {
    const output = execSync(
      `npx shopify theme push --store ${STORE} --theme ${themeId} --path "${THEME_DIR}" --only "templates/product.${templateSuffix}.json" --allow-live 2>&1`,
      {
        encoding: 'utf-8',
        timeout: 30000
      }
    );
    console.log(`  [Shopify CLI] Push output:`, output.substring(0, 300));
    return true;
  } catch (e) {
    throw new Error('Theme push failed: ' + e.message.substring(0, 500));
  }
}

/**
 * Create a product via Shopify CLI's GraphQL
 */
function createProduct(title, slug, price) {
  // Use the REST Admin API via CLI's authenticated session
  // We'll use a direct GraphQL mutation through the CLI
  const mutation = `
    mutation {
      productCreate(product: {
        title: "${title.replace(/"/g, '\\"')}"
        handle: "${slug}"
        status: DRAFT
      }) {
        product {
          id
          handle
          title
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  try {
    // Write mutation to temp file
    const mutationPath = path.join(THEME_DIR, '_mutation.graphql');
    fs.writeFileSync(mutationPath, mutation);

    // Try using the admin API directly via curl with the CLI's stored auth
    // The CLI stores auth in the system keychain, so we use the CLI as a proxy
    const output = execSync(
      `npx shopify app function run 2>&1 || echo "fallback"`,
      { encoding: 'utf-8', timeout: 10000 }
    ).trim();

    console.log('  [CLI] Product creation via CLI:', output.substring(0, 200));
    return null; // Will use REST API fallback
  } catch (e) {
    return null; // Fallback to REST API
  }
}

/**
 * Full push: create template and push to theme
 * Product creation still needs to be done manually or via REST API
 */
async function pushToShopify(options) {
  const { title, liquidContent, slug, price } = options;
  const templateSuffix = slug.toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-');

  // Push the template to the live theme
  pushTemplate(templateSuffix, liquidContent);

  return {
    templateSuffix,
    templateKey: `templates/product.${templateSuffix}.json`,
    message: `Template pushed! Create a product in Shopify Admin and set its template to "product.${templateSuffix}"`,
    adminUrl: `https://admin.shopify.com/store/${STORE.replace('.myshopify.com', '')}/products/new`,
    status: 'template_pushed'
  };
}

module.exports = { pushToShopify, pushTemplate, getActiveThemeId };
