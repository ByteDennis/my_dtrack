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

        // Auto-save to _col_comparison
        await syncColPair(name, data);
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

async function syncColPair(name, data) {
    const vintages = data.vintages || [];
    const allCols = new Set();
    const matchedCols = new Set();
    const diffCols = new Set();

    for (const v of vintages) {
        for (const c of (v.columns || [])) {
            allCols.add(c.left_col);
            if (c.has_diff) diffCols.add(c.left_col);
            else matchedCols.add(c.left_col);
        }
    }
    // Remove from matched if it differed in any vintage
    for (const d of diffCols) matchedCols.delete(d);

    try {
        const resp = await fetch(`/api/compare/col/${name}`, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                columns_compared: Array.from(allCols),
                matched_columns: Array.from(matchedCols),
                diff_columns: Array.from(diffCols),
            }),
        });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            console.error(`syncColPair(${name}) failed:`, err.error || resp.status);
        }
    } catch (e) {
        console.error(`syncColPair(${name}) error:`, e);
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

    // Sort: diff columns first
    const sorted = [...cols].sort((a, b) => {
        if (a.has_diff !== b.has_diff) return a.has_diff ? -1 : 1;
        return (a.left_col || '').localeCompare(b.left_col || '');
    });

    // Stat rows to display
    const statDefs = [
        {label: 'Type',      keyL: 'col_type',       keyR: 'col_type',       isDiff: () => false, fmt: 'text'},
        {label: 'N_Total',   keyL: 'n_total_left',   keyR: 'n_total_right',  isDiff: c => c.n_total_diff !== 0, fmt: 'int'},
        {label: 'N_Missing', keyL: 'n_missing_left',  keyR: 'n_missing_right', isDiff: c => c.n_missing_diff !== 0, fmt: 'int'},
        {label: 'N_Unique',  keyL: 'n_unique_left',   keyR: 'n_unique_right',  isDiff: c => c.n_unique_diff !== 0, fmt: 'int'},
        {label: 'Mean',      keyL: 'mean_left',       keyR: 'mean_right',      isDiff: c => c.mean_match === false, fmt: 'num'},
        {label: 'Std',       keyL: 'std_left',        keyR: 'std_right',       isDiff: c => c.std_match === false, fmt: 'num'},
        {label: 'Min',       keyL: 'min_left',        keyR: 'min_right',       isDiff: () => false, fmt: 'text'},
        {label: 'Max',       keyL: 'max_left',        keyR: 'max_right',       isDiff: () => false, fmt: 'text'},
    ];

    function fmtCell(val, fmt) {
        if (val == null || val === '') return '&mdash;';
        if (fmt === 'int') return fmtVal(val);
        if (fmt === 'num') return fmtNum(val);
        return esc(String(val));
    }

    // Find which (col_idx, stat_idx) are diffs for coloring
    const diffCells = new Set();
    const problemCols = new Set();
    sorted.forEach((c, ci) => {
        statDefs.forEach((sd, si) => {
            if (si > 0 && sd.isDiff(c)) {
                diffCells.add(`${ci},${si}`);
                problemCols.add(ci);
            }
        });
    });

    // Build header row (variable names) — per side
    function colHeadersForSide(side) {
        return sorted.map((c, ci) => {
            const name = side === 'left' ? c.left_col : (c.right_col || c.left_col);
            const icon = c.has_diff ? '<span style="color:var(--jp-warn-color0);">&#9888;</span> ' : '';
            return `<th style="text-align:right; font-size:11px;">${icon}${esc(name)}</th>`;
        }).join('');
    }

    // Build stat rows for one side
    function buildSideRows(side, sideLabel, sourceLabel) {
        const key = side === 'left' ? 'keyL' : 'keyR';
        let html = `<tr><td colspan="${sorted.length + 1}" style="font-weight:600; padding-top:8px;">${esc(sourceLabel)}</td></tr>`;
        html += `<tr><td style="font-weight:600;"></td>${colHeadersForSide(side)}</tr>`;
        for (let si = 0; si < statDefs.length; si++) {
            const sd = statDefs[si];
            html += '<tr>';
            html += `<td style="font-weight:600; font-size:11px; white-space:nowrap;">${sd.label}</td>`;
            for (let ci = 0; ci < sorted.length; ci++) {
                const c = sorted[ci];
                const val = c[sd[key]];
                let style = 'text-align:right; font-size:11px;';
                if (diffCells.has(`${ci},${si}`)) {
                    style += ' background:var(--jp-error-color2, #ffc7ce); color:var(--jp-error-color0, #c00);';
                } else if (problemCols.has(ci) && si > 0) {
                    style += ' background:var(--jp-success-color2, #c6efce);';
                }
                html += `<td style="${style}">${fmtCell(val, sd.fmt)}</td>`;
            }
            html += '</tr>';
        }
        return html;
    }

    // Get source labels from parent data
    const data = compareCache[name] || {};
    const srcLeft = (data.source_left || 'LEFT').toUpperCase();
    const srcRight = (data.source_right || 'RIGHT').toUpperCase();
    const tblLeft = data.table_left || '';
    const tblRight = data.table_right || '';

    const leftRows = buildSideRows('left', 'L', `${srcLeft}: ${tblLeft}`);
    const rightRows = buildSideRows('right', 'R', `${srcRight}: ${tblRight}`);


    return `
    <div class="rc-date-table-wrap" style="overflow-x:auto;">
        <table class="data-table compact">
            <tbody>
                ${leftRows}
                ${rightRows}
            </tbody>
        </table>
    </div>`;
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
// Export: Excel download
// ---------------------------------------------------------------------------
function downloadExcel() {
    const from = document.getElementById('global-from')?.value || '';
    const to = document.getElementById('global-to')?.value || '';
    const params = new URLSearchParams();
    if (from) params.set('from_date', from);
    if (to) params.set('to_date', to);
    window.open(`/api/compare/col/export/excel?${params}`, '_blank');
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

// ---------------------------------------------------------------------------
// Export: HTML download
// ---------------------------------------------------------------------------
async function downloadHTML() {
    try {
        const res = await fetch('/api/compare/col/export/html', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                from_date: document.getElementById('global-from')?.value || '',
                to_date: document.getElementById('global-to')?.value || '',
                title: document.getElementById('global-title')?.value || '',
                subtitle: document.getElementById('global-subtitle')?.value || '',
            }),
        });
        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            notify(`HTML export failed: ${err.error || res.status}`, 'error');
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
// Export: Save comparison logs
// ---------------------------------------------------------------------------
async function saveLogs() {
    try {
        const res = await fetch('/api/compare/col/export/log', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                from_date: document.getElementById('global-from')?.value || '',
                to_date: document.getElementById('global-to')?.value || '',
            }),
        });
        const data = await res.json();
        if (data.ok) {
            notify(`Logs saved to ${data.outdir}`);
        } else {
            notify(`Log export failed: ${data.error}`, 'error');
        }
    } catch (e) {
        notify(`Log export failed: ${e.message}`, 'error');
    }
}
