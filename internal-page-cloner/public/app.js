// State
let currentJobId = null;
let pollInterval = null;
let currentLiquid = null;

// Language selector hint
document.getElementById('language-select').addEventListener('change', function() {
  const hint = document.getElementById('translate-hint');
  if (this.value) {
    hint.textContent = 'All text content + product images will be translated';
  } else {
    hint.textContent = '';
  }
});

// Load stores on page load and restore last selected store
(async function loadStores() {
  const select = document.getElementById('store-select');

  // Save selection to localStorage on every change
  select.addEventListener('change', () => {
    localStorage.setItem('selectedStore', select.value);
  });

  try {
    const res = await fetch('/api/stores');
    const stores = await res.json();
    if (stores.length > 0) {
      select.innerHTML = stores.map(s =>
        `<option value="${s.id}">${s.name} (${s.domain})</option>`
      ).join('');

      // Restore last selected store AFTER rebuilding options
      const savedStore = localStorage.getItem('selectedStore');
      if (savedStore && stores.some(s => s.id === savedStore)) {
        select.value = savedStore;
      }
    }
  } catch (e) {
    console.error('Failed to load stores:', e);
  }
})();

// Navigation
document.querySelectorAll('.nav-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById(`view-${btn.dataset.view}`).classList.add('active');

    if (btn.dataset.view === 'research') loadResearchView();
    if (btn.dataset.view === 'jobs') loadJobs();
    if (btn.dataset.view === 'blocks') loadBlocks();
  });
});

document.getElementById('research-run-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const button = e.target.querySelector('button');
  const keywordCount = Number(document.getElementById('keyword-count').value) || 25;

  button.disabled = true;
  button.textContent = 'Running...';
  showResearchMessage('Research is running. This can take a while when Google Shopping checks are included.', 'info');

  try {
    const res = await fetch('/api/platform/research/run-pipeline', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ keyword_count: keywordCount })
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || data.error || 'Research failed');

    const stats = data.stats || {};
    showResearchMessage(
      `Research complete. Generated ${stats.keywords_generated || 0}, analyzed ${stats.keywords_analyzed || 0}, created ${stats.products_created || 0} products.`,
      'success'
    );
    await loadResearchKeywords();
  } catch (err) {
    showResearchMessage(err.message, 'error');
  } finally {
    button.disabled = false;
    button.textContent = 'Start research';
  }
});

document.getElementById('manual-keyword-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const keyword = document.getElementById('manual-keyword').value.trim();
  const sellingPrice = Number(document.getElementById('manual-price').value) || 0;
  if (!keyword) return;

  const button = e.target.querySelector('button');
  button.disabled = true;
  button.textContent = 'Analyzing...';
  showResearchMessage(`Analyzing "${keyword}"...`, 'info');

  try {
    const res = await fetch('/api/platform/research/add-keyword', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ keyword, selling_price: sellingPrice })
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || data.error || 'Keyword analysis failed');

    showResearchMessage(`Added "${data.keyword || keyword}" as a sourcing product.`, 'success');
    document.getElementById('manual-keyword').value = '';
    document.getElementById('manual-price').value = '';
    await loadResearchKeywords();
  } catch (err) {
    showResearchMessage(err.message, 'error');
  } finally {
    button.disabled = false;
    button.textContent = 'Analyze keyword';
  }
});

// Clone Form
document.getElementById('clone-form').addEventListener('submit', async (e) => {
  e.preventDefault();

  const url = document.getElementById('url-input').value.trim();
  if (!url) return;

  const cloneBtn = document.getElementById('clone-btn');
  cloneBtn.disabled = true;
  cloneBtn.textContent = 'Starting...';

  const targetLanguage = document.getElementById('language-select').value;

  hide('result-card');
  hide('error-card');

  // Show/hide the translating step based on language selection
  const translateStep = document.getElementById('step-translating');
  if (translateStep) {
    if (targetLanguage) {
      translateStep.classList.remove('hidden');
    } else {
      translateStep.classList.add('hidden');
    }
  }

  try {
    const res = await fetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        url,
        storeId: document.getElementById('store-select').value,
        targetLanguage: targetLanguage || null
      })
    });

    const data = await res.json();

    if (data.error) {
      showError(data.error);
      cloneBtn.disabled = false;
      cloneBtn.textContent = 'Clone';
      return;
    }

    currentJobId = data.jobId;
    show('progress-card');
    resetSteps();
    startPolling();

  } catch (err) {
    showError(err.message);
    cloneBtn.disabled = false;
    cloneBtn.textContent = 'Clone';
  }
});

// Polling
function startPolling() {
  if (pollInterval) clearInterval(pollInterval);
  pollInterval = setInterval(pollJob, 2000);
  pollJob();
}

async function pollJob() {
  if (!currentJobId) return;

  try {
    const res = await fetch(`/api/jobs/${currentJobId}`);
    const job = await res.json();

    document.getElementById('progress-fill').style.width = `${job.progress}%`;

    if (job.steps) {
      for (const [stepName, stepData] of Object.entries(job.steps)) {
        const stepEl = document.querySelector(`.step[data-step="${stepName}"]`);
        if (!stepEl) continue;

        stepEl.className = `step ${stepData.status === 'running' ? 'running' : ''} ${stepData.status === 'done' ? 'done' : ''}`.trim();
        const iconEl = stepEl.querySelector('.step-icon');
        if (stepData.status === 'done') iconEl.textContent = '✓';
        else if (stepData.status === 'running') iconEl.textContent = '◉';
        else iconEl.textContent = '○';
      }
    }

    // Show screenshot when available
    if (job.progress >= 15) {
      const screenshotImg = document.getElementById('screenshot-img');
      if (!screenshotImg.src.includes(currentJobId)) {
        screenshotImg.src = `/api/jobs/${currentJobId}/screenshot`;
        show('screenshot-preview');
      }
    }

    if (job.status === 'done') {
      clearInterval(pollInterval);
      pollInterval = null;
      showResult(job);
    }

    if (job.status === 'failed') {
      clearInterval(pollInterval);
      pollInterval = null;
      hide('progress-card');
      showError(job.error || 'Unknown error');
    }

  } catch (err) {
    console.error('Poll error:', err);
  }
}

function showResult(job) {
  hide('progress-card');
  show('result-card');

  currentLiquid = { content: job.result.content, filename: job.result.filename };

  const infoDiv = document.getElementById('result-product-info');
  const meta = job.result.productMeta || {};

  infoDiv.innerHTML = `
    <div style="margin-bottom:16px;">
      <span class="filename">${escapeHtml(job.result.filename)}</span>
    </div>
    <div style="margin-bottom:16px;">
      <strong style="font-size:18px;">${escapeHtml(meta.title || job.result.handle)}</strong>
      ${meta.price ? `<span style="margin-left:12px;color:#166534;font-weight:600;">$${escapeHtml(meta.price)}</span>` : ''}
      ${meta.compareAtPrice ? `<span style="margin-left:6px;color:#999;text-decoration:line-through;">$${escapeHtml(meta.compareAtPrice)}</span>` : ''}
      ${meta.variantCount > 1 ? `<span style="margin-left:12px;font-size:13px;color:#666;">${meta.variantCount} variants</span>` : ''}
      ${meta.imageCount ? `<span style="margin-left:12px;font-size:13px;color:#666;">${meta.imageCount} images</span>` : ''}
    </div>
    ${renderQaReport(job.result.qa)}
    <div style="display:flex;gap:12px;flex-wrap:wrap;">
      ${job.result.productUrl ? `<a href="${escapeHtml(job.result.productUrl)}" target="_blank" class="btn-primary" style="display:inline-block;text-decoration:none;font-size:14px;">View on Store</a>` : ''}
      ${job.result.adminUrl ? `<a href="${escapeHtml(job.result.adminUrl)}" target="_blank" class="btn-secondary" style="display:inline-block;text-decoration:none;font-size:13px;">Edit in Shopify Admin</a>` : ''}
    </div>
  `;

  const cloneBtn = document.getElementById('clone-btn');
  cloneBtn.disabled = false;
  cloneBtn.textContent = 'Clone';
}

function showError(message) {
  show('error-card');
  document.getElementById('error-message').textContent = message;

  const cloneBtn = document.getElementById('clone-btn');
  cloneBtn.disabled = false;
  cloneBtn.textContent = 'Clone';
}

// Actions
async function copyLiquid() {
  if (!currentJobId) return;

  try {
    const res = await fetch(`/api/jobs/${currentJobId}/liquid`);
    const data = await res.json();
    await navigator.clipboard.writeText(data.content);

    const btn = document.getElementById('copy-btn');
    btn.textContent = 'Copied!';
    setTimeout(() => btn.textContent = 'Copy Liquid', 2000);
  } catch (err) {
    console.error('Copy failed:', err);
  }
}

async function downloadLiquid() {
  if (!currentJobId) return;
  window.open(`/api/jobs/${currentJobId}/output`, '_blank');
}

function togglePreview() {
  const previewArea = document.getElementById('preview-area');
  const codeArea = document.getElementById('code-area');

  if (previewArea.classList.contains('hidden')) {
    show('preview-area');
    hide('code-area');

    if (currentLiquid) {
      document.getElementById('preview-iframe').srcdoc = currentLiquid.content;
    }

    document.getElementById('preview-btn').textContent = 'View Code';
  } else {
    hide('preview-area');
    show('code-area');

    if (currentLiquid) {
      document.getElementById('code-content').textContent = currentLiquid.content;
    }

    document.getElementById('preview-btn').textContent = 'Preview';
  }
}

function resetClone() {
  hide('error-card');
  hide('progress-card');
  hide('result-card');
  currentJobId = null;

  const cloneBtn = document.getElementById('clone-btn');
  cloneBtn.disabled = false;
  cloneBtn.textContent = 'Clone';
}

function resetSteps() {
  document.querySelectorAll('.step').forEach(step => {
    const isHidden = step.classList.contains('hidden');
    step.className = isHidden ? 'step hidden' : 'step';
    step.querySelector('.step-icon').textContent = '○';
  });
  document.getElementById('progress-fill').style.width = '0%';
  hide('screenshot-preview');
}

// Jobs View
async function loadJobs() {
  try {
    const res = await fetch('/api/jobs');
    const jobs = await res.json();
    const container = document.getElementById('jobs-list');

    if (jobs.length === 0) {
      container.innerHTML = '<p class="empty-state">No jobs yet. Clone your first page!</p>';
      return;
    }

    container.innerHTML = jobs.map(job => `
      <div class="job-item">
        <div>
          <div class="job-url">${escapeHtml(job.url)}</div>
          <span class="job-status ${job.status === 'done' ? 'done' : job.status === 'failed' ? 'failed' : 'running'}">${job.status}</span>
        </div>
        <div class="job-actions">
          ${job.status === 'done' ? `
            <button onclick="viewJob('${job.id}')">View</button>
            <button onclick="copyJobLiquid('${job.id}')">Copy</button>
          ` : ''}
        </div>
      </div>
    `).join('');
  } catch (err) {
    console.error('Failed to load jobs:', err);
  }
}

async function viewJob(jobId) {
  currentJobId = jobId;
  try {
    const res = await fetch(`/api/jobs/${jobId}`);
    const job = await res.json();
    if (job.status === 'done') {
      document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
      document.querySelector('[data-view="clone"]').classList.add('active');
      document.getElementById('view-clone').classList.add('active');
      showResult(job);
    }
  } catch (err) {
    console.error('Failed to view job:', err);
  }
}

async function copyJobLiquid(jobId) {
  try {
    const res = await fetch(`/api/jobs/${jobId}/liquid`);
    const data = await res.json();
    await navigator.clipboard.writeText(data.content);
    alert('Copied to clipboard!');
  } catch (err) {
    console.error('Copy failed:', err);
  }
}

// Blocks View
async function loadBlocks() {
  try {
    const res = await fetch('/api/blocks');
    const blocks = await res.json();
    const container = document.getElementById('blocks-list');

    container.innerHTML = blocks.map(block => `
      <div class="block-card">
        <h3>${escapeHtml(block.name)}</h3>
        <div class="block-id">${block.id}</div>
        <p>${escapeHtml(block.description)}</p>
        <div class="block-meta">
          Category: ${block.category} |
          ${block.static ? 'Static content' : `${Object.keys(block.slots).length} customizable slots`}
        </div>
      </div>
    `).join('');
  } catch (err) {
    console.error('Failed to load blocks:', err);
  }
}

// Research View
async function loadResearchView() {
  await Promise.all([
    loadPlatformStatus(),
    loadResearchKeywords()
  ]);
}

async function loadPlatformStatus() {
  const statusEl = document.getElementById('platform-status');
  statusEl.className = 'status-pill status-loading';
  statusEl.textContent = 'Checking platform...';

  try {
    const res = await fetch('/api/platform/health');
    const data = await res.json();
    if (!res.ok || data.status !== 'healthy') throw new Error(data.detail || data.error || 'Not healthy');

    statusEl.className = 'status-pill status-ready';
    statusEl.textContent = 'Platform online';
  } catch (err) {
    statusEl.className = 'status-pill status-error';
    statusEl.textContent = 'Platform offline';
    showResearchMessage('The research backend is not running yet. Start the combined platform service, then refresh this tab.', 'error');
  }
}

async function loadResearchKeywords() {
  const container = document.getElementById('keywords-list');
  container.innerHTML = '<p class="empty-state">Loading keywords...</p>';

  try {
    const res = await fetch('/api/platform/research/keywords?limit=25');
    const keywords = await res.json();
    if (!res.ok) throw new Error(keywords.detail || keywords.error || 'Could not load keywords');

    if (!Array.isArray(keywords) || keywords.length === 0) {
      container.innerHTML = '<p class="empty-state">No researched keywords yet.</p>';
      return;
    }

    container.innerHTML = `
      <div class="keywords-table">
        <div class="keywords-row keywords-head">
          <span>Keyword</span>
          <span>Competitors</span>
          <span>Diff. score</span>
          <span>Price</span>
          <span>Source</span>
        </div>
        ${keywords.map(renderKeywordRow).join('')}
      </div>
    `;
  } catch (err) {
    container.innerHTML = `<p class="empty-state">${escapeHtml(err.message)}</p>`;
  }
}

function renderKeywordRow(keyword) {
  return `
    <div class="keywords-row">
      <span class="keyword-name">${escapeHtml(keyword.keyword)}</span>
      <span>${keyword.competitor_count ?? '-'}</span>
      <span>${formatScore(keyword.differentiation_score)}</span>
      <span>${formatMoney(keyword.estimated_selling_price || keyword.median_competitor_price)}</span>
      <span>${escapeHtml(keyword.research_source || '-')}</span>
    </div>
  `;
}

function showResearchMessage(message, type) {
  const el = document.getElementById('research-message');
  el.className = `message message-${type || 'info'}`;
  el.textContent = message;
}

function formatScore(value) {
  if (value === null || value === undefined) return '-';
  return Number(value).toFixed(2);
}

function formatMoney(value) {
  if (!value) return '-';
  return `$${Number(value).toFixed(2)}`;
}

// QA report rendering
// Shown on the completion screen so the user can see at a glance whether the
// clone is suspicious (repeated images, source brand leaked through, etc.)
// without manually clicking through the storefront.
function renderQaReport(qa) {
  if (!qa) return '';
  const errors = qa.errors || [];
  const warnings = qa.warnings || [];
  const info = qa.info || [];
  const pass = qa.pass !== false;

  const badgeClass = pass && warnings.length === 0
    ? 'qa-badge qa-badge-pass'
    : pass
      ? 'qa-badge qa-badge-warn'
      : 'qa-badge qa-badge-fail';

  const badgeLabel = pass && warnings.length === 0
    ? '✓ Clone looks good'
    : pass
      ? `⚠ ${warnings.length} warning${warnings.length === 1 ? '' : 's'} — review recommended`
      : `✗ ${errors.length} issue${errors.length === 1 ? '' : 's'} detected — review before sharing`;

  const items = [];
  errors.forEach(e => items.push(`<li class="qa-item qa-error"><span class="qa-icon">✗</span>${escapeHtml(e)}</li>`));
  warnings.forEach(w => items.push(`<li class="qa-item qa-warn"><span class="qa-icon">⚠</span>${escapeHtml(w)}</li>`));
  info.forEach(i => items.push(`<li class="qa-item qa-info"><span class="qa-icon">ℹ</span>${escapeHtml(i)}</li>`));

  return `
    <div class="qa-report">
      <div class="${badgeClass}">${badgeLabel}</div>
      ${items.length > 0 ? `<ul class="qa-list">${items.join('')}</ul>` : ''}
    </div>
  `;
}

// Utility
function show(id) { document.getElementById(id).classList.remove('hidden'); }
function hide(id) { document.getElementById(id).classList.add('hidden'); }

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text || '';
  return div.innerHTML;
}
