/* col_compare.js — Col Compare page logic (vintage-aware, row_compare-style) */

const ICON = {
    chevron: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"/></svg>`,
};

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
let compareCache = {};

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
// Accordion
// ---------------------------------------------------------------------------
function renderAccordion() {
    const container = document.getElementById('pairs-accordion');
    if (!pairsData.length) {
        container.innerHTML = '<div class="empty-message">No pairs configured.</div>';
        return;
    }
    container.innerHTML = pairsData.map(p => {
        const hasData = p.left.col_count > 0 && p.right.col_count > 0;
        return `
        <div class="rc-pair" id="pair-${p.pair_name}">
            <div class="rc-pair-header" onclick="togglePair('${p.pair_name}')">
                <span class="pair-expand">${ICON.chevron}</span>
                <span class="pair-name">${p.pair_name}</span>
                <span class="rc-pair-status" id="status-${p.pair_name}">
                    ${hasData ? '<span class="status-badge ready">ready</span>' : '<span class="status-badge warning">no col data</span>'}
                </span>
            </div>
            <div class="rc-pair-body" id="body-${p.pair_name}">
                <div class="empty-message">Running comparison...</div>
            </div>
        </div>`;
    }).join('');
}

function togglePair(name) {
    document.getElementById(`pair-${name}`).classList.toggle('expanded');
}

// ---------------------------------------------------------------------------
// Compare
// ---------------------------------------------------------------------------
async function comparePair(name) {
    const body = document.getElementById(`body-${name}`);
    body.innerHTML = '<div class="empty-message">Running comparison...</div>';
    document.getElementById(`pair-${name}`).classList.add('expanded');

    const from = document.getElementById('global-from')?.value || '';
    const to = document.getElementById('global-to')?.value || '';
    const params = new URLSearchParams();
    if (from) params.set('from_date', from);
    if (to) params.set('to_date', to);

    try {
        const res = await fetch(`/api/compare/col/${name}?${params}`);
        if (!res.ok) throw new Error(await res.text());
        const data = await res.json();
        compareCache[name] = data;
        renderPairBody(name, data);

        const s = data.summary;
        const statusEl = document.getElementById(`status-${name}`);
        if (s.n_diff === 0 && s.n_left_only === 0 && s.n_right_only === 0) {
            statusEl.innerHTML = '<span class="status-badge ready">all match</span>';
        } else {
            statusEl.innerHTML = `<span class="status-badge warning">${s.n_diff} diff</span>`;
        }
    } catch (e) {
        body.innerHTML = `<div class="empty-message" style="color:var(--jp-error-color0);">Error: ${e.message}</div>`;
    }
}

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
    const metaLeft = data.meta_left || {};
    const metaRight = data.meta_right || {};

    body.innerHTML = `
        <div class="rc-stats-grid">
            <div class="rc-stat-card">
                <div class="rc-stat-label">LEFT: ${data.source_left.toUpperCase()} — ${data.table_left}</div>
                <div class="rc-stat-row">
                    <span>vintage: ${metaLeft.vintage || '—'}</span>
                    <span>date_var: ${metaLeft.date_var || '—'}</span>
                </div>
            </div>
            <div class="rc-stat-card">
                <div class="rc-stat-label">RIGHT: ${data.source_right.toUpperCase()} — ${data.table_right}</div>
                <div class="rc-stat-row">
                    <span>vintage: ${metaRight.vintage || '—'}</span>
                    <span>date_var: ${metaRight.date_var || '—'}</span>
                </div>
            </div>
        </div>

        <div class="rc-summary-chips">
            <span class="rc-chip match">${s.n_matched} matched</span>
            <span class="rc-chip mismatch">${s.n_diff} diff</span>
            <span class="rc-chip left-only">${s.n_left_only} L-only</span>
            <span class="rc-chip right-only">${s.n_right_only} R-only</span>
            ${s.n_type_mismatch ? `<span class="rc-chip mismatch">${s.n_type_mismatch} type mismatch</span>` : ''}
            <span class="rc-chip" style="background:var(--jp-layout-color2);">${s.n_vintages} vintage${s.n_vintages !== 1 ? 's' : ''}</span>
        </div>

        ${renderVintages(name, data)}
        ${renderOnlyCols(data.left_only, 'Left-Only Columns')}
        ${renderOnlyCols(data.right_only, 'Right-Only Columns')}
    `;
}

// ---------------------------------------------------------------------------
// Per-vintage comparison
// ---------------------------------------------------------------------------
function renderVintages(name, data) {
    const vintages = data.vintages || [];
    if (!vintages.length) return '<div class="empty-message">No vintage data</div>';

    return vintages.map((v, vi) => {
        const cols = v.columns || [];
        const nDiff = cols.filter(c => c.has_diff).length;
        const nMatch = cols.length - nDiff;
        const statusText = nDiff > 0
            ? `<span style="color:var(--jp-warn-color0);">${nDiff} diff</span>, ${nMatch} match`
            : `<span style="color:var(--jp-success-color0);">${nMatch} match</span>`;

        return `
        <details class="rc-details" ${vi === 0 ? 'open' : ''}>
            <summary class="rc-details-summary">
                <span style="font-weight:600;">${esc(v.label || v.dt || 'unknown')}</span>
                <span class="rc-details-counts">${cols.length} cols — ${statusText}</span>
            </summary>
            <div class="rc-details-body">
                <div class="rc-date-table-wrap">
                    ${renderColTable(name, cols)}
                </div>
            </div>
        </details>`;
    }).join('');
}

function renderColTable(name, cols) {
    if (!cols.length) return '<div class="empty-message">No matched columns</div>';

    const hasNumeric = cols.some(c => c.col_type === 'numeric');

    const rows = cols.map(c => {
        const statusIcon = c.has_diff
            ? '<span style="color:var(--jp-warn-color0);">&#9888;</span>'
            : '<span style="color:var(--jp-success-color0);">&#10003;</span>';
        const typeBadge = c.type_mismatch
            ? `<span style="color:var(--jp-warn-color0);">${esc(c.col_type_left)}/${esc(c.col_type_right)}</span>`
            : esc(c.col_type);

        let numCells = '';
        if (hasNumeric) {
            if (c.col_type === 'numeric') {
                numCells = `
                    <td style="text-align:right;">${fmtNum(c.mean_left)}</td>
                    <td style="text-align:right;">${fmtNum(c.mean_right)}</td>
                    <td style="text-align:right;">${fmtNum(c.std_left)}</td>
                    <td style="text-align:right;">${fmtNum(c.std_right)}</td>`;
            } else {
                numCells = `<td colspan="4" style="text-align:center; color:var(--jp-ui-font-color2);">—</td>`;
            }
        }

        const diffClass = c.has_diff ? ' rc-date-row mismatch' : '';

        return `<tr class="${diffClass}">
            <td>${statusIcon}</td>
            <td>${esc(c.left_col)}</td>
            <td>${esc(c.right_col !== c.left_col ? c.right_col : '')}</td>
            <td>${typeBadge}</td>
            <td style="text-align:right;">${fmtVal(c.n_total_left)}</td>
            <td style="text-align:right;">${fmtVal(c.n_total_right)}</td>
            <td style="text-align:right;">${fmtDiff(c.n_total_diff)}</td>
            <td style="text-align:right;">${fmtVal(c.n_missing_left)}</td>
            <td style="text-align:right;">${fmtVal(c.n_missing_right)}</td>
            <td style="text-align:right;">${fmtVal(c.n_unique_left)}</td>
            <td style="text-align:right;">${fmtVal(c.n_unique_right)}</td>
            ${numCells}
        </tr>`;
    }).join('');

    const numHeaders = hasNumeric ? `
        <th style="text-align:right;">Mean L</th>
        <th style="text-align:right;">Mean R</th>
        <th style="text-align:right;">Std L</th>
        <th style="text-align:right;">Std R</th>` : '';

    return `
    <table class="data-table compact">
        <thead><tr>
            <th></th>
            <th>Left Col</th>
            <th>Right Col</th>
            <th>Type</th>
            <th style="text-align:right;">Total L</th>
            <th style="text-align:right;">Total R</th>
            <th style="text-align:right;">Diff</th>
            <th style="text-align:right;">Miss L</th>
            <th style="text-align:right;">Miss R</th>
            <th style="text-align:right;">Uniq L</th>
            <th style="text-align:right;">Uniq R</th>
            ${numHeaders}
        </tr></thead>
        <tbody>${rows}</tbody>
    </table>`;
}

// ---------------------------------------------------------------------------
// Left-only / Right-only
// ---------------------------------------------------------------------------
function renderOnlyCols(cols, title) {
    if (!cols || !cols.length) return '';
    const rows = cols.map(c => `<tr>
        <td>${esc(c.column_name)}</td>
        <td>${esc(c.col_type)}</td>
        <td style="text-align:right;">${fmtVal(c.n_total)}</td>
        <td style="text-align:right;">${fmtVal(c.n_missing)}</td>
        <td style="text-align:right;">${fmtVal(c.n_unique)}</td>
    </tr>`).join('');

    return `
    <details class="rc-details">
        <summary class="rc-details-summary">
            <span style="font-weight:600;">${title}</span>
            <span class="rc-details-counts">${cols.length} columns</span>
        </summary>
        <div class="rc-details-body">
            <table class="data-table compact">
                <thead><tr>
                    <th>Column</th><th>Type</th>
                    <th style="text-align:right;">Total</th>
                    <th style="text-align:right;">Missing</th>
                    <th style="text-align:right;">Unique</th>
                </tr></thead>
                <tbody>${rows}</tbody>
            </table>
        </div>
    </details>`;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function fmtVal(n) {
    if (n == null || n === '') return '—';
    const num = Number(n);
    return isNaN(num) ? String(n) : num.toLocaleString();
}

function fmtNum(n) {
    if (n == null || n === '') return '—';
    const num = Number(n);
    return isNaN(num) ? String(n) : num.toLocaleString(undefined, {maximumFractionDigits: 4});
}

function fmtDiff(n) {
    if (n == null || n === '' || n === 0) return '0';
    const num = Number(n);
    if (isNaN(num)) return String(n);
    const sign = num > 0 ? '+' : '';
    return sign + num.toLocaleString();
}

function esc(s) {
    if (!s) return '';
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function notify(msg, type = 'success') {
    const el = document.getElementById('db-status');
    if (el) {
        el.style.background = type === 'error' ? 'var(--jp-error-color0)' : 'var(--jp-success-color0)';
        el.title = msg;
        setTimeout(() => { el.style.background = 'var(--jp-success-color0)'; }, 2000);
    }
}
