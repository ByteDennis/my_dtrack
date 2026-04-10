/* row_compare.js — Row Compare page logic */

// ---------------------------------------------------------------------------
// SVG icons
// ---------------------------------------------------------------------------
const ICON = {
    // Chevron-right for expand/collapse
    chevron: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"/></svg>`,
    // Details/list icon for expandable section
    details: `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg>`,
    // Paperclip/attachment icon for future CSV attachment
    attachment: `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.44 11.05l-9.19 9.19a6 6 0 01-8.49-8.49l9.19-9.19a4 4 0 015.66 5.66l-9.2 9.19a2 2 0 01-2.83-2.83l8.49-8.48"/></svg>`,
};

// ---------------------------------------------------------------------------
// Navigation (shared pattern)
// ---------------------------------------------------------------------------
function navigateToStep(step) {
    window.location.href = '/' + step;
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let pairsData = [];      // from /api/status
let compareCache = {};   // pair_name -> comparison result from API

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
    const hasData = p.left.row_count && p.right.row_count;
    return `
    <div class="rc-pair" id="pair-${name}">
        <div class="rc-pair-header" onclick="togglePair('${name}')">
            <span class="pair-expand">${ICON.chevron}</span>
            <span class="pair-name">${name}</span>
            <span class="rc-pair-status" id="status-${name}">
                ${hasData ? '<span class="status-badge ready">ready</span>' : '<span class="status-badge warning">no data</span>'}
            </span>
        </div>
        <div class="rc-pair-body" id="body-${name}">
            <div class="empty-message">Click Compare to run.</div>
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

    // Expand it
    document.getElementById(`pair-${name}`).classList.add('expanded');

    const from = document.getElementById('global-from').value;
    const to = document.getElementById('global-to').value;
    const params = new URLSearchParams();
    if (from) params.set('from_date', from);
    if (to) params.set('to_date', to);

    try {
        const res = await fetch(`/api/compare/row/${name}?${params}`);
        if (!res.ok) throw new Error(await res.text());
        const data = await res.json();
        compareCache[name] = data;
        renderPairBody(name, data);

        // Update status badge
        const statusEl = document.getElementById(`status-${name}`);
        const s = data.summary;
        if (s.n_mismatch === 0 && s.n_left_only === 0 && s.n_right_only === 0) {
            statusEl.innerHTML = '<span class="status-badge ready">all match</span>';
        } else {
            statusEl.innerHTML = `<span class="status-badge warning">${s.n_mismatch} mismatch</span>`;
        }

        // Auto-save matching dates to _row_comparison so col_gen can use them
        await syncPair(name);

        // Auto-refresh HTML preview
        refreshPreview(name);
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
    const ann = data.annotations;
    const saved = data.saved;

    // Build excluded set from saved state
    const excludedSet = new Set(saved.excluded_dates || []);
    data._excludedSet = excludedSet;

    body.innerHTML = `
        <!-- Side-by-side stats -->
        <div class="rc-stats-grid">
            <div class="rc-stat-card">
                <div class="rc-stat-label">LEFT: ${data.source_left.toUpperCase()} — ${data.table_left}</div>
                <div class="rc-stat-row"><span>${s.count_left} dates</span><span>${fmtRange(s.date_range_left)}</span><span>${fmtCount(s.total_left)} rows</span></div>
            </div>
            <div class="rc-stat-card">
                <div class="rc-stat-label">RIGHT: ${data.source_right.toUpperCase()} — ${data.table_right}</div>
                <div class="rc-stat-row"><span>${s.count_right} dates</span><span>${fmtRange(s.date_range_right)}</span><span>${fmtCount(s.total_right)} rows</span></div>
            </div>
        </div>

        ${s.overlap_start ? `<div class="rc-overlap">Overlap: ${s.overlap_start} → ${s.overlap_end} (${s.n_match + s.n_mismatch} dates)</div>` : ''}

        <div class="rc-summary-chips">
            <span class="rc-chip match">${s.n_match} match</span>
            <span class="rc-chip mismatch">${s.n_mismatch} mismatch</span>
            <span class="rc-chip left-only">${s.n_left_only} L-only</span>
            <span class="rc-chip right-only">${s.n_right_only} R-only</span>
        </div>

        <!-- Editable annotations -->
        <div class="rc-annotations">
            <div class="rc-ann-row">
                <label>Left comment:</label>
                <input type="text" id="comment-left-${name}" value="${esc(ann.comment_left)}" placeholder="optional note" oninput="debouncedSync('${name}')">
                <label>Time:</label>
                <input type="text" id="time-left-${name}" value="${esc(ann.time_left)}" placeholder="e.g. 42s" style="width:80px;" oninput="debouncedSync('${name}')">
            </div>
            <div class="rc-ann-row">
                <label>Right comment:</label>
                <input type="text" id="comment-right-${name}" value="${esc(ann.comment_right)}" placeholder="optional note" oninput="debouncedSync('${name}')">
                <label>Time:</label>
                <input type="text" id="time-right-${name}" value="${esc(ann.time_right)}" placeholder="e.g. 112s" style="width:80px;" oninput="debouncedSync('${name}')">
            </div>
        </div>

        <!-- Date details (collapsible) -->
        <details class="rc-details" id="details-${name}">
            <summary class="rc-details-summary">
                ${ICON.details}
                <span>Date Details</span>
                <span class="rc-details-counts">${s.n_mismatch} mismatch, ${s.n_left_only} L-only, ${s.n_right_only} R-only</span>
            </summary>
            <div class="rc-details-body">
                <div class="rc-date-table-wrap">
                    <table class="data-table compact" id="date-table-${name}">
                        <thead>
                            <tr>
                                <th>Date</th>
                                <th style="text-align:right;">Left</th>
                                <th style="text-align:right;">Right</th>
                                <th style="text-align:right;">Diff</th>
                                <th>Status</th>
                                <th style="text-align:center;">Excl</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${data.dates.map(d => dateRowHTML(name, d, excludedSet)).join('')}
                        </tbody>
                    </table>
                </div>
                <div class="rc-matching-summary" id="match-summary-${name}">
                    ${matchingSummaryText(name, data)}
                </div>
            </div>
        </details>

        <!-- HTML Preview -->
        <div class="rc-preview-section">
            <div class="rc-preview-header">
                <span style="font-weight:600; font-size:12px; text-transform:uppercase; letter-spacing:0.5px;">HTML Preview</span>
                <div style="display:flex; gap:8px;">
                    <button class="btn-text" onclick="downloadCSV('${name}')">CSV</button>
                    <button class="btn-text" onclick="refreshPreview('${name}')">Refresh</button>
                </div>
            </div>
            <div class="rc-preview-frame" id="preview-${name}">
                <div class="empty-message">Click Refresh to generate preview.</div>
            </div>
        </div>
    `;
}

// ---------------------------------------------------------------------------
// Date row HTML
// ---------------------------------------------------------------------------
function dateRowHTML(name, d, excludedSet) {
    const isExcluded = excludedSet.has(d.dt);
    const statusIcon = {
        'match': '<span style="color:var(--jp-success-color0);">&#10003;</span>',
        'mismatch': '<span style="color:var(--jp-warn-color0);">&#9888;</span>',
        'left_only': '<span style="color:var(--jp-brand-color0);">L</span>',
        'right_only': '<span style="color:var(--jp-brand-color0);">R</span>',
    }[d.status] || '';

    const overlapClass = d.in_overlap ? '' : ' rc-outside-overlap';
    const excludedClass = isExcluded ? ' rc-excluded' : '';

    return `<tr class="rc-date-row ${d.status}${overlapClass}${excludedClass}" data-status="${d.status}">
        <td class="date-cell">${d.dt}</td>
        <td style="text-align:right; font-family:var(--jp-code-font-family);">${d.left != null ? fmtCount(d.left) : '—'}</td>
        <td style="text-align:right; font-family:var(--jp-code-font-family);">${d.right != null ? fmtCount(d.right) : '—'}</td>
        <td style="text-align:right; font-family:var(--jp-code-font-family);">${d.diff != null ? fmtDiff(d.diff) : '—'}</td>
        <td>${statusIcon}</td>
        <td style="text-align:center;">
            ${d.status !== 'match' ? `<input type="checkbox" ${isExcluded ? 'checked' : ''} onchange="toggleExclude('${name}', '${d.dt}', this.checked)">` : ''}
        </td>
    </tr>`;
}

// ---------------------------------------------------------------------------
// Toggle exclude
// ---------------------------------------------------------------------------
function toggleExclude(name, dt, excluded) {
    const data = compareCache[name];
    if (!data) return;
    if (excluded) {
        data._excludedSet.add(dt);
    } else {
        data._excludedSet.delete(dt);
    }
    // Update the row styling
    const table = document.getElementById(`date-table-${name}`);
    const rows = table.querySelectorAll('tbody tr');
    rows.forEach(row => {
        const dateCell = row.querySelector('.date-cell');
        if (dateCell && dateCell.textContent === dt) {
            if (excluded) {
                row.classList.add('rc-excluded');
            } else {
                row.classList.remove('rc-excluded');
            }
        }
    });
    document.getElementById(`match-summary-${name}`).innerHTML = matchingSummaryText(name, data);
    debouncedSync(name);
}

function matchingSummaryText(name, data) {
    const manualExcl = data._excludedSet ? data._excludedSet.size : 0;
    const s = data.summary;
    const totalDates = s.n_match + s.n_mismatch + s.n_left_only + s.n_right_only;
    const colGenDates = s.n_match - manualExcl;
    const nonMatch = s.n_mismatch + s.n_left_only + s.n_right_only + manualExcl;

    return `Matching: <strong>${s.n_match}</strong> &nbsp; `
        + `Manual excl: <strong>${manualExcl}</strong> &nbsp; `
        + `<span style="color:var(--jp-ui-font-color2);">Col Gen will use <strong>${colGenDates}</strong>/${totalDates} dates `
        + `(${nonMatch} excluded: ${s.n_mismatch} mismatch, ${s.n_left_only} L-only, ${s.n_right_only} R-only`
        + (manualExcl ? `, ${manualExcl} manual` : '') + `)</span>`;
}

// ---------------------------------------------------------------------------
// Preview
// ---------------------------------------------------------------------------
async function refreshPreview(name) {
    const frame = document.getElementById(`preview-${name}`);
    frame.innerHTML = '<div class="empty-message">Generating...</div>';

    const data = compareCache[name];
    const excluded = data ? Array.from(data._excludedSet) : [];

    try {
        const res = await fetch(`/api/compare/row/${name}/preview`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                excluded_dates: excluded,
                comment_left: document.getElementById(`comment-left-${name}`)?.value || '',
                comment_right: document.getElementById(`comment-right-${name}`)?.value || '',
                time_left: document.getElementById(`time-left-${name}`)?.value || '',
                time_right: document.getElementById(`time-right-${name}`)?.value || '',
            }),
        });
        const result = await res.json();
        frame.innerHTML = result.html || '<div class="empty-message">No preview.</div>';
    } catch (e) {
        frame.innerHTML = `<div class="empty-message" style="color:var(--jp-error-color0);">Preview error: ${e.message}</div>`;
    }
}

// ---------------------------------------------------------------------------
// Auto-sync (debounced save to DB on every change)
// ---------------------------------------------------------------------------
const _syncTimers = {};

function debouncedSync(name) {
    clearTimeout(_syncTimers[name]);
    _syncTimers[name] = setTimeout(() => syncPair(name), 600);
}

async function syncPair(name) {
    const data = compareCache[name];
    if (!data) {
        apiLog(`syncPair(${name}): no data in cache — skipped`, 'error');
        return;
    }
    if (!data._excludedSet) {
        apiLog(`syncPair(${name}): _excludedSet not set — skipped`, 'error');
        return;
    }
    if (!data.dates) {
        apiLog(`syncPair(${name}): no dates array — skipped`, 'error');
        return;
    }

    const excluded = Array.from(data._excludedSet);
    const matching = data.dates
        .filter(d => d.status === 'match' && !data._excludedSet.has(d.dt))
        .map(d => d.dt);

    // Non-matching dates within overlap (mismatch + left_only + right_only + manual excluded)
    const ov_start = data.summary.overlap_start;
    const ov_end = data.summary.overlap_end;
    const nonMatching = data.dates
        .filter(d => {
            if (!ov_start || !ov_end) return false;
            if (d.dt < ov_start || d.dt > ov_end) return false;
            return d.status !== 'match' || data._excludedSet.has(d.dt);
        })
        .map(d => d.dt);

    apiLog(`syncPair(${name}): saving ${matching.length} matching, ${nonMatching.length} non-matching in overlap, ${excluded.length} manual excluded`, 'info');

    try {
        const resp = await fetch(`/api/compare/row/${name}`, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                excluded_dates: excluded,
                matching_dates: matching,
                non_matching_dates: nonMatching,
                comment_left: document.getElementById(`comment-left-${name}`)?.value || '',
                comment_right: document.getElementById(`comment-right-${name}`)?.value || '',
                time_left: document.getElementById(`time-left-${name}`)?.value || '',
                time_right: document.getElementById(`time-right-${name}`)?.value || '',
            }),
        });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            apiLog(`syncPair(${name}) PUT ${resp.status}: ${err.error || 'failed'}`, 'error');
        }
    } catch (e) {
        apiLog(`syncPair(${name}) error: ${e.message}`, 'error');
    }
}

// ---------------------------------------------------------------------------
// Export
// ---------------------------------------------------------------------------
async function downloadHTML() {
    // Collect live UI state for all compared pairs
    const pairsState = {};
    for (const [name, data] of Object.entries(compareCache)) {
        pairsState[name] = {
            excluded_dates: Array.from(data._excludedSet || []),
            comment_left: document.getElementById(`comment-left-${name}`)?.value || '',
            comment_right: document.getElementById(`comment-right-${name}`)?.value || '',
            time_left: document.getElementById(`time-left-${name}`)?.value || '',
            time_right: document.getElementById(`time-right-${name}`)?.value || '',
        };
    }

    try {
        const res = await fetch('/api/compare/row/export/html', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                from_date: document.getElementById('global-from').value,
                to_date: document.getElementById('global-to').value,
                title: document.getElementById('global-title').value,
                subtitle: document.getElementById('global-subtitle').value,
                pairs: pairsState,
            }),
        });
        const html = await res.text();
        const blob = new Blob([html], { type: 'text/html' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'row_compare.html';
        a.click();
        URL.revokeObjectURL(url);
    } catch (e) {
        notify(`HTML export failed: ${e.message}`, 'error');
    }
}

function downloadCSV(name) {
    const from = document.getElementById('global-from').value;
    const to = document.getElementById('global-to').value;
    const params = new URLSearchParams();
    if (from) params.set('from_date', from);
    if (to) params.set('to_date', to);
    window.open(`/api/compare/row/export/excel/${name}?${params}`, '_blank');
}

async function downloadAllCSV() {
    for (const p of pairsData) {
        if (p.skip) continue;
        downloadCSV(p.pair_name);
        await new Promise(r => setTimeout(r, 500));
    }
}

function downloadExcel() {
    const from = document.getElementById('global-from').value;
    const to = document.getElementById('global-to').value;
    const params = new URLSearchParams();
    if (from) params.set('from_date', from);
    if (to) params.set('to_date', to);
    window.open(`/api/compare/row/export/excel-all?${params}`, '_blank');
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function fmtCount(n) {
    if (n == null) return '—';
    return Number(n).toLocaleString();
}

function fmtDiff(n) {
    if (n === 0) return '0';
    const sign = n > 0 ? '+' : '';
    return sign + Number(n).toLocaleString();
}

function fmtRange(range) {
    if (!range || !range[0]) return '—';
    return `${range[0]} → ${range[1]}`;
}

function esc(s) {
    if (!s) return '';
    return s.replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
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
