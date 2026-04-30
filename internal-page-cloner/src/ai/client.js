const Anthropic = require('@anthropic-ai/sdk');
const path = require('path');
const fs = require('fs');

let client = null;
let resolvedApiKey = null;

// Read API key directly from .env file (bypasses all env var issues)
function getApiKey() {
  if (resolvedApiKey) return resolvedApiKey;

  if (process.env.ANTHROPIC_API_KEY) {
    resolvedApiKey = process.env.ANTHROPIC_API_KEY.trim();
    return resolvedApiKey;
  }

  try {
    const envPath = path.join(__dirname, '../../.env');
    const envContent = fs.readFileSync(envPath, 'utf-8');
    const match = envContent.match(/^ANTHROPIC_API_KEY=(.+)$/m);
    if (match) {
      resolvedApiKey = match[1].trim();
      // Also force-set process.env so the SDK doesn't try to read an empty one
      process.env.ANTHROPIC_API_KEY = resolvedApiKey;
      return resolvedApiKey;
    }
  } catch (e) {
    console.error('Failed to read .env file:', e.message);
  }

  return null;
}

function getClient() {
  if (!client) {
    const apiKey = getApiKey();
    if (!apiKey) {
      throw new Error('No ANTHROPIC_API_KEY found. Add it to the .env file.');
    }
    console.log('Creating Anthropic client with configured API key');
    client = new Anthropic({ apiKey });
  }
  return client;
}

/**
 * Record the token usage from a Claude API response into an optional cost
 * tracker. Silent no-op if no tracker (or no usage payload) was provided.
 *
 * We always pass the real model string the SDK echoes back, not whatever the
 * caller requested — occasionally the response is served by a newer revision
 * and we want the log to reflect the actual model that ran.
 */
function _recordClaudeUsage(response, options, requestedModel) {
  const tracker = options && options.costTracker;
  if (!tracker) return;
  const usage = response && response.usage;
  if (!usage) return;
  tracker.recordAnthropic({
    model: response.model || requestedModel,
    inputTokens: usage.input_tokens || 0,
    outputTokens: usage.output_tokens || 0,
    context: options.context || '',
  });
}

/**
 * Call Claude API with text prompt.
 *
 * Optional `options.costTracker` — if passed, records token usage via the
 * tracker's recordAnthropic() method. `options.context` is an optional short
 * label shown in the tracker's breakdown (e.g. "hero copy", "FAQ rewrite").
 */
async function callClaude(systemPrompt, userMessage, options = {}) {
  const anthropic = getClient();
  const model = options.model || 'claude-sonnet-4-6';

  const response = await anthropic.messages.create({
    model,
    max_tokens: options.maxTokens || 4096,
    system: systemPrompt,
    messages: [{ role: 'user', content: userMessage }]
  });

  _recordClaudeUsage(response, options, model);
  const text = response.content[0]?.text || '';
  return text;
}

/**
 * Call Claude API with an image (screenshot) + text.
 *
 * Same tracker support as callClaude — pass `options.costTracker` to record
 * token usage (vision calls are typically much heavier in tokens because the
 * image is expanded to several thousand input tokens).
 */
async function callClaudeWithImage(systemPrompt, imageBase64, textMessage, options = {}) {
  const anthropic = getClient();
  const model = options.model || 'claude-sonnet-4-6';

  const response = await anthropic.messages.create({
    model,
    max_tokens: options.maxTokens || 4096,
    system: systemPrompt,
    messages: [{
      role: 'user',
      content: [
        {
          type: 'image',
          source: {
            type: 'base64',
            media_type: options.mediaType || 'image/jpeg',
            data: imageBase64
          }
        },
        {
          type: 'text',
          text: textMessage
        }
      ]
    }]
  });

  _recordClaudeUsage(response, options, model);
  const text = response.content[0]?.text || '';
  return text;
}

/**
 * Parse JSON from Claude's response (handles markdown code blocks)
 */
function parseJsonResponse(text) {
  // Try direct parse first
  try {
    return JSON.parse(text);
  } catch (e) {}

  // Try extracting from markdown code block
  const jsonMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (jsonMatch) {
    try {
      return JSON.parse(jsonMatch[1].trim());
    } catch (e) {}
  }

  // Try finding JSON array or object in the text
  const arrMatch = text.match(/\[[\s\S]*\]/);
  if (arrMatch) {
    try {
      return JSON.parse(arrMatch[0]);
    } catch (e) {}
  }

  const objMatch = text.match(/\{[\s\S]*\}/);
  if (objMatch) {
    try {
      return JSON.parse(objMatch[0]);
    } catch (e) {}
  }

  throw new Error('Could not parse JSON from AI response:\n' + text.substring(0, 500));
}

module.exports = { callClaude, callClaudeWithImage, parseJsonResponse };
