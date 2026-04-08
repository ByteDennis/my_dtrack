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
        const parts = [];
        if (s.n_diff > 0) parts.push(`${s.n_diff} diff`);
        if (s.n_type_mismatch > 0) parts.push(`${s.n_type_mismatch} type`);
        if (s.n_left_only > 0) parts.push(`${s.n_left_only} L-only`);
        if (s.n_right_only > 0) parts.push(`${s.n_right_only} R-only`);

        if (parts.length === 0) {
            statusEl.innerHTML = '<span class="status-badge ready">all match</span>';
        } else {
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

    const typeMismatchChip = s.n_type_mismatch > 0
        ? `<span class="rc-chip type-mismatch">${s.n_type_mismatch} type mismatch</span>`
        : '';

    body.innerHTML = `
        <!-- Summary chips -->
        <div class="rc-summary-chips">
            <span class="rc-chip match">${s.n_matched} matched</span>
            <span class="rc-chip mismatch">${s.n_diff} diff</span>
            ${typeMismatchChip}
            <span class="rc-chip left-only">${s.n_left_only} L-only</span>
            <span class="rc-chip right-only">${s.n_right_only} R-only</span>
        </div>

        <!-- Matched columns table -->
        ${data.matched.length > 0 ? renderMatchedTable(name, data) : ''}

        <!-- Left-only columns -->
        ${data.left_only.length > 0 ? renderOnlyTable('Left', data.left_only) : ''}

        <!-- Right-only columns -->
        ${data.right_only.length > 0 ? renderOnlyTable('Right', data.right_only) : ''}
    `;
}

function renderMatchedTable(pairName, data) {
    const rows = data.matched.map(col => {
        const hasDiff = col.has_diff;
        const typeMismatch = col.type_mismatch;
        const rowClass = hasDiff ? 'mismatch' : 'match';
        const statusIcon = hasDiff
            ? '<span style="color:var(--jp-warn-color0);">&#9888;</span>'
            : '<span style="color:var(--jp-success-color0);">&#10003;</span>';

        // Type cell: show mismatch warning with override dropdown
        let typeCell;
        if (typeMismatch) {
            const overrideLabel = col.has_override ? ' (override)' : '';
            typeCell = `
                <td class="type-cell type-mismatch-cell" title="Type mismatch: L=${col.left_type}, R=${col.right_type}">
                    <div style="display:flex; align-items:center; gap:4px;">
                        <span style="color:var(--jp-warn-color0);">&#9888;</span>
                        <select class="type-override-select"
                                data-pair="${esc(pairName)}"
                                data-col="${esc(col.left_col)}"
                                onchange="onTypeOverride(this)">
                            <option value="categorical" ${col.resolved_type === 'categorical' ? 'selected' : ''}>categorical</option>
                            <option value="numeric" ${col.resolved_type === 'numeric' ? 'selected' : ''}>numeric</option>
                        </select>
                        ${col.has_override ? '<span class="override-badge">override</span>' : ''}
                    </div>
                    <div class="type-detail">L: ${esc(col.left_type)} / R: ${esc(col.right_type)}</div>
                </td>`;
        } else {
            typeCell = `<td>${esc(col.resolved_type || col.left_type || '')}</td>`;
        }

        // Show mean/std for numeric columns
        const isNumeric = col.resolved_type === 'numeric';
        const meanCell = isNumeric
            ? `<td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtNum(col.left_mean)}</td>
               <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtNum(col.right_mean)}</td>`
            : `<td style="text-align:right; color:var(--jp-ui-font-color3);">&mdash;</td>
               <td style="text-align:right; color:var(--jp-ui-font-color3);">&mdash;</td>`;
        const stdCell = isNumeric
            ? `<td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtNum(col.left_std)}</td>
               <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtNum(col.right_std)}</td>`
            : `<td style="text-align:right; color:var(--jp-ui-font-color3);">&mdash;</td>
               <td style="text-align:right; color:var(--jp-ui-font-color3);">&mdash;</td>`;

        return `<tr class="rc-date-row ${rowClass}">
            <td>${esc(col.left_col)}</td>
            <td>${esc(col.right_col)}</td>
            ${typeCell}
            <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.left_n_total)}</td>
            <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.right_n_total)}</td>
            <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.left_n_missing)}</td>
            <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.right_n_missing)}</td>
            <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.left_n_unique)}</td>
            <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.right_n_unique)}</td>
            ${meanCell}
            ${stdCell}
            <td>${statusIcon}</td>
        </tr>`;
    }).join('');

    return `
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
                            <th>Type</th>
                            <th style="text-align:right;">N Total L</th>
                            <th style="text-align:right;">N Total R</th>
                            <th style="text-align:right;">N Missing L</th>
                            <th style="text-align:right;">N Missing R</th>
                            <th style="text-align:right;">N Unique L</th>
                            <th style="text-align:right;">N Unique R</th>
                            <th style="text-align:right;">Mean L</th>
                            <th style="text-align:right;">Mean R</th>
                            <th style="text-align:right;">Std L</th>
                            <th style="text-align:right;">Std R</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        </div>
    </details>`;
}

function renderOnlyTable(side, cols) {
    const rows = cols.map(col => `<tr>
        <td>${esc(col.column_name)}</td>
        <td>${esc(col.col_type || '')}</td>
        <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.n_total)}</td>
        <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.n_missing)}</td>
        <td style="text-align:right; font-family:var(--jp-code-font-family);">${fmtVal(col.n_unique)}</td>
    </tr>`).join('');

    return `
    <details class="rc-details">
        <summary class="rc-details-summary">
            <span>${side}-Only Columns (${cols.length})</span>
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
                    <tbody>${rows}</tbody>
                </table>
            </div>
        </div>
    </details>`;
}

// ---------------------------------------------------------------------------
// Type override handler
// ---------------------------------------------------------------------------
let _typeOverrideTimer = null;

async function onTypeOverride(selectEl) {
    const pairName = selectEl.dataset.pair;
    const colName = selectEl.dataset.col;
    const newType = selectEl.value;

    // Collect all current overrides for this pair from the cache
    const data = compareCache[pairName];
    if (!data) return;

    const overrides = { ...(data.col_type_overrides || {}) };
    overrides[colName] = newType;

    // Update local cache
    data.col_type_overrides = overrides;

    // Debounce save — batch rapid changes
    clearTimeout(_typeOverrideTimer);
    _typeOverrideTimer = setTimeout(() => saveTypeOverrides(pairName, overrides), 400);
}

async function saveTypeOverrides(pairName, overrides) {
    try {
        const res = await fetch(`/api/pairs/${pairName}/col-type-overrides`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ overrides }),
        });
        const result = await res.json();
        if (result.ok) {
            notify(`Type overrides saved for ${pairName}`);
            // Re-run comparison to reflect changes in diff detection
            await comparePair(pairName);
        } else {
            notify(`Failed to save: ${result.error}`, 'error');
        }
    } catch (e) {
        notify(`Save failed: ${e.message}`, 'error');
    }
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

function fmtNum(v) {
    if (v == null || v === '') return '&mdash;';
    const n = Number(v);
    if (isNaN(n)) return esc(String(v));
    // Show up to 4 decimal places for mean/std
    if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, {maximumFractionDigits: 2});
    return n.toLocaleString(undefined, {maximumFractionDigits: 4});
}

function esc(s) {
    if (!s) return '';
    return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function notify(msg, type = 'success') {
    const el = document.createElement('div');
    const isError = type === 'error';
    el.style.cssText = `
        position:fixed; top:20px; right:20px; z-index:2000;
        background:${isError ? '#fbe9e7' : '#e8f5e9'};
        color:${isError ? '#c62828' : '#2e7d32'};
        border:1px solid ${isError ? '#ef9a9a' : '#a5d6a7'};
        padding:12px 20px; border-radius:6px;
        box-shadow:0 4px 12px rgba(0,0,0,0.3);
        font-size:13px;
    `;
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 3000);
}
