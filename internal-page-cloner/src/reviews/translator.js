/**
 * reviews/translator.js
 *
 * Translates reviews to the target store's language using Claude API.
 * Uses du-form German for Merivalo, keeps names unchanged.
 */

const { callClaude } = require('../ai/client');

/**
 * Translate an array of reviews to the target language.
 *
 * @param {Array} reviews - Array of {name, body, rating, date, ...}
 * @param {string} language - Target language code ('de', 'en', 'fr', etc.)
 * @param {string} productName - Product name in the target language
 * @returns {Promise<Array>} - Reviews with translated body text
 */
async function translateReviews(reviews, language = 'en', productName = '') {
  if (language === 'en') {
    console.log('[reviews] Target language is English, skipping translation');
    return reviews;
  }

  const langConfig = LANGUAGE_CONFIG[language] || LANGUAGE_CONFIG.de;

  console.log(`[reviews] Translating ${reviews.length} reviews to ${langConfig.name}...`);

  // Batch translate all reviews in one API call for efficiency
  const reviewTexts = reviews.map((r, i) => `${i}. "${r.body}"`).join('\n');

  const prompt = `Translate these product reviews from English to ${langConfig.name} (${langConfig.formality}). The product is "${productName}".

${langConfig.instructions}

Return ONLY a JSON array of strings with the translated reviews in the same order. No explanation, no markdown, just the JSON array.

Reviews:
${reviewTexts}`;

  try {
    const result = await callClaude(
      'You are a professional translator specializing in e-commerce product reviews.',
      prompt,
      { maxTokens: 4000 }
    );

    // Parse JSON array from response
    const cleanResult = result.trim().replace(/^```json\s*/, '').replace(/\s*```$/, '');
    const translated = JSON.parse(cleanResult);

    if (translated.length !== reviews.length) {
      console.warn(`[reviews] Warning: got ${translated.length} translations for ${reviews.length} reviews`);
    }

    // Apply translations to reviews
    const translatedReviews = reviews.map((review, i) => ({
      ...review,
      body: translated[i] || review.body,
      originalBody: review.body
    }));

    console.log(`[reviews] Successfully translated ${translatedReviews.length} reviews`);
    return translatedReviews;

  } catch (error) {
    console.error('[reviews] Translation error:', error.message);
    // Return original reviews if translation fails
    return reviews;
  }
}

const LANGUAGE_CONFIG = {
  de: {
    name: 'German',
    formality: 'du-form, NOT Sie-form',
    instructions: `Keep translations natural and authentic-sounding, like real German customers would write.
- Always use du-form (informal), NEVER Sie-form
- Use cm instead of inches, kg instead of lbs
- Keep reviewer emotions and tone intact
- Keep brand/product names unchanged
- Make it sound like a native German speaker wrote it`
  },
  fr: {
    name: 'French',
    formality: 'tu-form for casual reviews',
    instructions: `Keep translations natural and authentic-sounding, like real French customers.
- Use tu-form for casual reviews
- Keep reviewer emotions and tone intact
- Keep brand/product names unchanged`
  },
  nl: {
    name: 'Dutch',
    formality: 'je-form, informal',
    instructions: `Keep translations natural and authentic-sounding, like real Dutch customers.
- Use je-form (informal)
- Keep reviewer emotions and tone intact
- Keep brand/product names unchanged`
  },
  es: {
    name: 'Spanish',
    formality: 'tú-form, informal',
    instructions: `Keep translations natural and authentic-sounding, like real Spanish customers.
- Use tú-form (informal)
- Keep reviewer emotions and tone intact
- Keep brand/product names unchanged`
  }
};

module.exports = { translateReviews };
