/**
 * Page Cloner cost tracker.
 *
 * Mirrors the Python CostTracker in Blue Ocean Platform's src/core/cost_tracker.py
 * so records flowing out of this process match the schema the platform uses
 * in the "API Costs" Google Sheet.
 *
 * Usage:
 *   const { CostTracker } = require('./core/cost-tracker');
 *   const tracker = new CostTracker({ runId: 'clone_12345', runType: 'page_clone' });
 *
 *   // Pass to modules that hit paid APIs
 *   const text = await callClaude(sys, user, { costTracker: tracker, context: 'content gen' });
 *   const buf  = await translateImageWithFal(url, 'de', 'Merivalo', falKey, 2, '', tracker);
 *
 *   // At end of run — prints summary and appends to data/cost-log.jsonl
 *   tracker.printSummary();
 *   await tracker.persistToFile();
 *
 * Cross-project sync (to the platform's Google Sheet) is a follow-up — for
 * now each run writes one JSONL block to ./data/cost-log.jsonl that the
 * platform worker can slurp on a schedule.
 *
 * Pricing constants kept in sync with the Python tracker:
 *  - Anthropic Claude Sonnet 4.5: $3 / $15 per Mtok
 *  - fal.ai nano-banana-2:         $0.04 / image
 *  - fal.ai nano-banana-pro:       $0.04 / image (same pricing tier)
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// ---------------------------------------------------------------------------
// Pricing constants (USD) — keep in sync with Python tracker.
// ---------------------------------------------------------------------------
const CLAUDE_SONNET_INPUT_PER_MTOK = 3.00;
const CLAUDE_SONNET_OUTPUT_PER_MTOK = 15.00;
const FAL_NANOBANANA_PER_IMAGE = 0.04;

const PROVIDER_ANTHROPIC = 'anthropic';
const PROVIDER_FAL = 'fal';

class CostTracker {
  constructor({ runId, runType = 'page_clone' } = {}) {
    this.runId = runId || `${runType}_${crypto.randomBytes(4).toString('hex')}`;
    this.runType = runType;
    this.records = [];
    this.startedAt = new Date();
  }

  /**
   * Record an Anthropic Claude call. Cost is exact from token counts.
   * `usage` comes from the SDK response's `usage` field.
   */
  recordAnthropic({ model, inputTokens, outputTokens, context = '' }) {
    inputTokens = Number(inputTokens || 0);
    outputTokens = Number(outputTokens || 0);
    // All current Claude Sonnet variants share the same Sonnet pricing tier.
    // Unknown models fall back to Sonnet rates with a warning so cost is never
    // silently zero.
    const lower = (model || '').toLowerCase();
    if (!lower.includes('sonnet') && !lower.includes('claude')) {
      console.warn(`[cost-tracker] Unknown Anthropic model ${model} — using Sonnet pricing`);
    }
    const cost = (
      inputTokens * CLAUDE_SONNET_INPUT_PER_MTOK / 1_000_000 +
      outputTokens * CLAUDE_SONNET_OUTPUT_PER_MTOK / 1_000_000
    );
    this._add({
      provider: PROVIDER_ANTHROPIC,
      endpoint: model,
      units: `${_fmtK(inputTokens)} in + ${_fmtK(outputTokens)} out tokens`,
      costUsd: cost,
      context,
      estimated: false,
    });
  }

  /**
   * Record a fal.ai image call. Always estimated — fal.ai doesn't return
   * per-call cost. Override `perImageUsd` if on a different plan tier.
   */
  recordFal({ model, numImages = 1, context = '', perImageUsd = null }) {
    const unit = perImageUsd != null ? perImageUsd : FAL_NANOBANANA_PER_IMAGE;
    this._add({
      provider: PROVIDER_FAL,
      endpoint: model,
      units: `${numImages} image${numImages !== 1 ? 's' : ''}`,
      costUsd: unit * numImages,
      context,
      estimated: true,
    });
  }

  _add({ provider, endpoint, units, costUsd, context, estimated }) {
    this.records.push({
      timestamp: new Date().toISOString().replace(/\.\d+Z$/, ''),
      run_id: this.runId,
      run_type: this.runType,
      provider,
      endpoint,
      units,
      cost_usd: Number(costUsd.toFixed(6)),
      context,
      estimated,
    });
  }

  // -------------------------------------------------------------------------
  // Reporting
  // -------------------------------------------------------------------------

  totalUsd() {
    return Number(this.records.reduce((s, r) => s + r.cost_usd, 0).toFixed(4));
  }

  /**
   * Group by (provider, endpoint). Returns rows sorted by cost desc.
   */
  breakdown() {
    const agg = new Map();
    for (const r of this.records) {
      const key = `${r.provider}\u0000${r.endpoint}`;
      if (!agg.has(key)) {
        agg.set(key, {
          provider: r.provider,
          endpoint: r.endpoint,
          calls: 0,
          cost_usd: 0,
          any_estimated: false,
        });
      }
      const row = agg.get(key);
      row.calls += 1;
      row.cost_usd += r.cost_usd;
      row.any_estimated = row.any_estimated || r.estimated;
    }
    const rows = Array.from(agg.values()).map(r => ({
      ...r,
      cost_usd: Number(r.cost_usd.toFixed(4)),
    }));
    rows.sort((a, b) => b.cost_usd - a.cost_usd);
    return rows;
  }

  summary() {
    const total = this.totalUsd();
    const lines = [`Run ${this.runId} (${this.runType}) — $${total.toFixed(4)} total`];
    for (const row of this.breakdown()) {
      const flag = row.any_estimated ? ' ~' : '';
      lines.push(
        `  ${row.provider.padEnd(12)} ${row.endpoint.padEnd(40)} ` +
        `${String(row.calls).padStart(4)} calls   $${row.cost_usd.toFixed(4).padStart(8)}${flag}`
      );
    }
    return lines.join('\n');
  }

  printSummary() {
    if (this.records.length === 0) return;
    console.log('\n' + this.summary() + '\n');
  }

  /**
   * Append this run's records to data/cost-log.jsonl. One JSON object per line.
   * The platform's worker can slurp this file periodically to sync into the
   * shared "API Costs" sheet.
   */
  async persistToFile(logPath = null) {
    if (!this.records.length) return;
    const resolved = logPath || path.join(__dirname, '..', '..', 'data', 'cost-log.jsonl');
    try {
      const dir = path.dirname(resolved);
      fs.mkdirSync(dir, { recursive: true });
      const lines = this.records.map(r => JSON.stringify({
        ...r,
        estimated: r.estimated ? 'yes' : 'no',
      }));
      fs.appendFileSync(resolved, lines.join('\n') + '\n', 'utf-8');
      console.log(`[cost-tracker] Appended ${this.records.length} records to ${resolved} (run total $${this.totalUsd().toFixed(4)})`);
    } catch (e) {
      // Never fail a run because cost logging failed.
      console.error('[cost-tracker] Failed to persist cost log:', e.message);
    }
  }
}

function _fmtK(n) {
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`;
  return String(n);
}

module.exports = {
  CostTracker,
  PROVIDER_ANTHROPIC,
  PROVIDER_FAL,
  CLAUDE_SONNET_INPUT_PER_MTOK,
  CLAUDE_SONNET_OUTPUT_PER_MTOK,
  FAL_NANOBANANA_PER_IMAGE,
};
