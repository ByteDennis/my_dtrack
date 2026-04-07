/* col_compare.js - Column Compare page logic */

// ---------------------------------------------------------------------------
// Navigation
// ---------------------------------------------------------------------------
function navigateToStep(step) {
    window.location.href = '/' + step;
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let pairsData = [];
let compareCache = {};   // pair_name -> comparison result

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', async () => {
    await loadPairs();
    await runAll();
});

async function loadPairs() {
    try {
        const res = await fetch('/api/status');
        const data = await res.json();
        pairsData = data.pairs || [];
        renderAccordion();
    } catch (e) {
        document.getElementById('pairs-accordion').innerHTML =
            '<div class="empty-message">Failed to load pairs.</div>';
    }
}

// ---------------------------------------------------------------------------
// Render accordion
// ---------------------------------------------------------------------------
function renderAccordion() {
    const container = document.getElementById('pairs-accordion');
    if (!pairsData.length) {
        container.innerHTML = '<div class="empty-message">No pairs configured.</div>';
        return;
    }
    container.innerHTML = pairsData.map(p => pairAccordionHTML(p)).join('');
}

function pairAccordionHTML(p) {
    const name = p.pair_name;
    const hasData = p.left.col_count > 0 && p.right.col_count > 0;
    return `
    <div class="rc-pair" id="pair-${name}">
        <div class="rc-pair-header" onclick="togglePair('${name}')">
            <span class="pair-expand"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"/></svg></span>
            <span class="pair-name">${name}</span>
            <span class="rc-pair-status" id="status-${name}">
                ${hasData ? '<span class="status-badge ready">ready</span>' : '<span class="status-badge warning">no col data</span>'}
            </span>
        </div>
        <div class="rc-pair-body" id="body-${name}">
            <div class="empty-message">Click Run All to compare.</div>
        </div>
    </div>`;
}

// ---------------------------------------------------------------------------
// Accordion toggle
// ---------------------------------------------------------------------------
function togglePair(name) {
    const el = document.getElementById(`pair-${name}`);
    el.classList.toggle('expanded');
}

// ---------------------------------------------------------------------------
// Compare one pair
// ---------------------------------------------------------------------------
async function comparePair(name) {
    const body = document.getElementById(`body-${name}`);
    body.innerHTML = '<div class="empty-message">Running comparison...</div>';

    document.getElementById(`pair-${name}`).classList.add('expanded');

    const from = document.getElementById('global-from').value;
    const to = document.getElementById('global-to').value;
    const params = new URLSearchParams();
    if (from) params.set('from_date', from);
    if (to) params.set('to_date', to);

    try {
        const res = await fetch(`/api/compare/col/${name}?${params}`);
        if (!res.ok) throw new Error(await res.text());
        const data = await res.json();
        compareCache[name] = data;
        renderPairBody(name, data);

        // Update status badge
        const statusEl = document.getElementById(`status-${name}`);
        const s = data.summary;
        if (s.n_diff === 0 && s.n_left_only === 0 && s.n_right_only === 0) {
            statusEl.innerHTML = '<span class="status-badge ready">all match</span>';
        } else {
            const parts = [];
            if (s.n_diff > 0) parts.push(`${s.n_diff} diff`);
            if (s.n_left_only > 0) parts.push(`${s.n_left_only} L-only`);
            if (s.n_right_only > 0) parts.push(`${s.n_right_only} R-only`);
            statusEl.innerHTML = `<span class="status-badge warning">${parts.join(', ')}</span>`;
        }
    } catch (e) {
        body.innerHTML = `<div class="empty-message" style="color:var(--jp-error-color0);">Error: ${e.message}</div>`;
    }
}

// ---------------------------------------------------------------------------
// Run all pairs
// ---------------------------------------------------------------------------
async function runAll() {
    for (const p of pairsData) {
        if (p.skip) continue;
        await comparePair(p.pair_name);
    }
}

// ---------------------------------------------------------------------------
// Render pair body
// ---------------------------------------------------------------------------
function renderPairBody(name, data) {
    const body = document.getElementById(`body-${name}`);
    const s = data.summary;

    body.innerHTML = `
        <!-- Summary chips -->
        <div class="rc-summary-chips">
            <span class="rc-chip match">${s.n_matched} matched</span>
            <span class="rc-chip mismatch">${s.n_diff} diff</span>
            <span class="rc-chip left-only">${s.n_left_only} L-only</span>
            <span class="rc-chip right-only">${s.n_right_only} R-only</span>
        </div>

        <!-- Matched columns table -->
        ${data.matched.length > 0 ? `
        <details class="rc-details" open>
            <summary class="rc-details-summary">
                <span>Matched Columns (${data.matched.length})</span>
            </summary>
            <div class="rc-details-body">
                <div class="rc-date-table-wrap">
                    <table class="data-table compact">
                        <thead>
                            <tr>
                                <th>Left Column</th>
                                <th>Right Column</th>
                                <th>Type L</th>
                                <th>Type R</th>
                                <th style="text-align:right;">N Total L</th>
                                <th style="text-align:right;">N Total R</th>
                                <th style="text-align:right;">N Missing L</th>
                                <th style="text-align:right;">N Missing R</th>
                                <th style="text-align:right;">N Unique L</th>
                                <th style="text-align:right;">N Unique R</th>
                                <th>Status</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${data.matched.map(col => {
                                const hasDiff = col.has_diff;
                                const rowClass = hasDiff ? 'mismatch' : 'match';
                                const statusIcon = hasDiff
                                    ? '<span style="color:var(--jp-warn-color0);">&#9888;</span>'
                                    : '<span style="color:var(--jp-success-color0);">&#10003;</span>';
                                return `<tr class="rc-date-row ${rowClass}">
                                    <td>${esc(col.left_col)}</td>
                                    <td>${esc(col.right_col)}</td>
                                    <td>${esc(col.left_type || '')}</td>
                                    <td>${esc(col.right_type || '')}</td>
                                    <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.left_n_total)}</td>
                                    <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.right_n_total)}</td>
                                    <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.left_n_missing)}</td>
                                    <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.right_n_missing)}</td>
                                    <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.left_n_unique)}</td>
                                    <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.right_n_unique)}</td>
                                    <td>${statusIcon}</td>
                                </tr>`;
                            }).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        </details>` : ''}

        <!-- Left-only columns -->
        ${data.left_only.length > 0 ? `
        <details class="rc-details">
            <summary class="rc-details-summary">
                <span>Left-Only Columns (${data.left_only.length})</span>
            </summary>
            <div class="rc-details-body">
                <div class="rc-date-table-wrap">
                    <table class="data-table compact">
                        <thead>
                            <tr>
                                <th>Column Name</th>
                                <th>Type</th>
                                <th style="text-align:right;">N Total</th>
                                <th style="text-align:right;">N Missing</th>
                                <th style="text-align:right;">N Unique</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${data.left_only.map(col => `<tr>
                                <td>${esc(col.column_name)}</td>
                                <td>${esc(col.col_type || '')}</td>
                                <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.n_total)}</td>
                                <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.n_missing)}</td>
                                <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.n_unique)}</td>
                            </tr>`).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        </details>` : ''}

        <!-- Right-only columns -->
        ${data.right_only.length > 0 ? `
        <details class="rc-details">
            <summary class="rc-details-summary">
                <span>Right-Only Columns (${data.right_only.length})</span>
            </summary>
            <div class="rc-details-body">
                <div class="rc-date-table-wrap">
                    <table class="data-table compact">
                        <thead>
                            <tr>
                                <th>Column Name</th>
                                <th>Type</th>
                                <th style="text-align:right;">N Total</th>
                                <th style="text-align:right;">N Missing</th>
                                <th style="text-align:right;">N Unique</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${data.right_only.map(col => `<tr>
                                <td>${esc(col.column_name)}</td>
                                <td>${esc(col.col_type || '')}</td>
                                <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.n_total)}</td>
                                <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.n_missing)}</td>
                                <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.n_unique)}</td>
                            </tr>`).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        </details>` : ''}
    `;
}

// ---------------------------------------------------------------------------
// Export
// ---------------------------------------------------------------------------
async function downloadHTML() {
    try {
        const res = await fetch('/api/report/col');
        if (!res.ok) {
            notify('No col report found. Run compare-col from CLI first.', 'error');
            return;
        }
        const html = await res.text();
        const blob = new Blob([html], { type: 'text/html' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'col_compare.html';
        a.click();
        URL.revokeObjectURL(url);
    } catch (e) {
        notify(`HTML export failed: ${e.message}`, 'error');
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function fmtVal(v) {
    if (v == null || v === '') return '&mdash;';
    const n = Number(v);
    if (isNaN(n)) return esc(String(v));
    return n.toLocaleString();
}

function esc(s) {
    if (!s) return '';
    return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function notify(msg, type = 'success') {
    console.log(`[${type}] ${msg}`);
    const el = document.getElementById('db-status');
    if (el) {
        el.style.background = type === 'error' ? 'var(--jp-error-color0)' : 'var(--jp-success-color0)';
        el.title = msg;
        setTimeout(() => { el.style.background = 'var(--jp-success-color0)'; }, 2000);
    }
}
