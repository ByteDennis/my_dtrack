/* col_compare.js — Col Compare page logic (vintage-aware, row_compare-style) */

const ICON = {
    chevron: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"/></svg>`,
};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let pairsData = [];
let compareCache = {};

function _currentPreset() {
    // Empty string means "no filter" -- includes every row regardless of
    // preset (the historical behavior before persisted preset existed).
    // Users explicitly pick Quick / Standard / Detailed to narrow the view.
    return document.getElementById('preset-select')?.value || '';
}

// >>> field-level Compare filter: drives table render + every export <<< //
const COMPARE_FIELDS = [
    {key: 'n_total',    label: 'N_Total'},
    {key: 'n_missing',  label: 'N_Missing'},
    {key: 'n_unique',   label: 'N_Unique'},
    {key: 'min',        label: 'Min'},
    {key: 'max',        label: 'Max'},
    {key: 'length_avg', label: 'Length avg'},
    {key: 'length_max', label: 'Length max'},
    {key: 'hash_sum',   label: 'Hash sum'},
];
const _CF_KEY = 'dtrack-col-compare-fields';

// >>> read selected field-keys from localStorage; default = all <<< //
function _getCompareFields() {
    try {
        const raw = localStorage.getItem(_CF_KEY);
        if (raw == null) return COMPARE_FIELDS.map(f => f.key);
        const arr = JSON.parse(raw);
        if (!Array.isArray(arr)) return COMPARE_FIELDS.map(f => f.key);
        const valid = new Set(COMPARE_FIELDS.map(f => f.key));
        return arr.filter(k => valid.has(k));
    } catch (e) { return COMPARE_FIELDS.map(f => f.key); }
}

function _saveCompareFields(arr) {
    try { localStorage.setItem(_CF_KEY, JSON.stringify(arr)); } catch (e) {}
}

// >>> per-field diff predicate used by both filter + non-filter paths <<< //
const _DIFF_FNS = {
    n_total:    c => (c.n_total_diff || 0) !== 0,
    n_missing:  c => (c.n_missing_diff || 0) !== 0,
    n_unique:   c => (c.n_unique_diff || 0) !== 0,
    min:        c => c.min_match === false,
    max:        c => c.max_match === false,
    length_avg: c => c.length_avg_match === false,
    length_max: c => c.length_max_match === false,
    hash_sum:   c => c.hash_sum_match === false,
};

// >>> recompute has_diff against the current Compare-field selection <<< //
function _compHasDiff(comp) {
    const active = new Set(_getCompareFields());
    for (const [k, fn] of Object.entries(_DIFF_FNS)) {
        if (active.has(k) && fn(comp)) return true;
    }
    return false;
}

function _refreshCompareFieldsSummary() {
    const arr = _getCompareFields();
    const el = document.getElementById('compare-fields-summary');
    if (!el) return;
    if (arr.length === COMPARE_FIELDS.length) el.textContent = 'all';
    else if (arr.length === 0) el.textContent = 'none';
    else el.textContent = `${arr.length}/${COMPARE_FIELDS.length}`;
}

function _renderCompareFieldsPanel() {
    const list = document.getElementById('compare-fields-list');
    if (!list) return;
    const active = new Set(_getCompareFields());
    list.innerHTML = COMPARE_FIELDS.map(f => `
        <label style="display:flex; align-items:center; gap:6px; font-size:12px; cursor:pointer;">
            <input type="checkbox" data-field="${f.key}" ${active.has(f.key) ? 'checked' : ''}>
            ${f.label}
        </label>`).join('');
    list.querySelectorAll('input[type=checkbox]').forEach(cb => {
        cb.addEventListener('change', () => {
            const sel = [...list.querySelectorAll('input:checked')].map(x => x.dataset.field);
            _saveCompareFields(sel);
            _refreshCompareFieldsSummary();
            onCompareFieldsChange();
        });
    });
}

function setCompareFields(mode) {
    const all = COMPARE_FIELDS.map(f => f.key);
    _saveCompareFields(mode === 'all' ? all : []);
    _renderCompareFieldsPanel();
    _refreshCompareFieldsSummary();
    onCompareFieldsChange();
}

// >>> re-render every expanded pair so the field filter takes effect <<< //
function onCompareFieldsChange() {
    for (const p of pairsData) {
        const body = document.getElementById(`body-${p.pair_name}`);
        if (body && body.style.display !== 'none' && compareCache[p.pair_name]) {
            renderPairBody(p.pair_name, compareCache[p.pair_name]);
        }
    }
}

function onPresetChange() {
    // Preset filter changes the rows pulled from _col_stats — drop the
    // cache and re-fetch every pair so the displayed comparison reflects
    // only the preset's data.
    compareCache = {};
    runAll();
}

// ---------------------------------------------------------------------------
// Cell selection — for "drill into these (col, vintage) cells at fine level"
// ---------------------------------------------------------------------------
// Stored as Set of "<pair>|<col>|<vintage_label>" so cross-pair selections
// survive table re-renders. Persisted to sessionStorage so an accidental
// reload doesn't wipe the user's clicks.
const _SEL_KEY = 'dtrack-col-fine-cells';
let selectedCells = new Set();
try {
    const saved = sessionStorage.getItem(_SEL_KEY);
    if (saved) selectedCells = new Set(JSON.parse(saved));
} catch (e) { /* ignore */ }

function _saveSelection() {
    try { sessionStorage.setItem(_SEL_KEY, JSON.stringify([...selectedCells])); }
    catch (e) {}
}

function toggleCellSelection(pair, col, vintage) {
    const key = `${pair}|${col}|${vintage}`;
    if (selectedCells.has(key)) selectedCells.delete(key);
    else selectedCells.add(key);
    _saveSelection();
    _refreshSelectionBar();
    _refreshCellMarkers();
}

function clearSelection() {
    selectedCells.clear();
    _saveSelection();
    _refreshSelectionBar();
    _refreshCellMarkers();
}

function _refreshCellMarkers() {
    // Update visual state on existing rendered cells without a full re-render.
    document.querySelectorAll('[data-cell-key]').forEach(el => {
        const key = el.dataset.cellKey;
        if (selectedCells.has(key)) el.classList.add('cell-selected');
        else el.classList.remove('cell-selected');
    });
}

function _refreshSelectionBar() {
    const bar = document.getElementById('selection-bar');
    if (!bar) return;
    const n = selectedCells.size;
    bar.style.display = n > 0 ? '' : 'none';
    const countEl = document.getElementById('sel-count');
    if (countEl) countEl.textContent = n;
}

async function generateFineForSelected() {
    if (!selectedCells.size) return;
    const cells = [...selectedCells].map(k => {
        const [pair, col, vintage] = k.split('|');
        return {pair, col, vintage};
    });
    const sas_outdir = prompt('Left outdir (SAS / Hadoop / Oracle; extract_col.{sas,sql}):', './sas/');
    if (sas_outdir == null) return;
    const aws_outdir = prompt('Right outdir (AWS / Athena; extract_col.sql):', './csv/');
    if (aws_outdir == null) return;

    const btn = document.getElementById('gen-fine-btn');
    if (btn) { btn.disabled = true; btn.textContent = 'Generating…'; }
    try {
        const resp = await fetch('/api/generate', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                type: 'col',
                level: 'fine',
                cells: cells,
                sas_outdir, aws_outdir,
            }),
        });
        const data = await resp.json();
        if (resp.ok && data.ok) {
            alert(`Fine SQL/SAS generated for ${cells.length} cells.\n` +
                  (data.sas_file ? `  SAS: ${data.sas_file}\n` : '') +
                  (data.sql_file ? `  SQL: ${data.sql_file}\n` : '') +
                  `\nRun the generated extract, then load-col with level=fine, ` +
                  `then switch the page Level dropdown to Fine to see the drill-down.`);
        } else {
            alert(`Gen-fine failed: ${data.error || resp.status}\n\n${data.output || ''}`);
        }
    } catch (e) {
        alert(`Gen-fine error: ${e.message}`);
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = `Generate Fine for selected`; }
    }
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', async () => {
    _renderCompareFieldsPanel();
    _refreshCompareFieldsSummary();
    await loadPairs();
    await runAll();
    _refreshSelectionBar();
});

async function loadPairs() {
    try {
        const res = await fetch('/api/status');
        const data = await res.json();
        // Hide pairs the user toggled off on /pairs (skip=true).
        pairsData = (data.pairs || []).filter(p => !p.skip);
        // Honor the global pair selector: when narrowed, only show those
        // pairs (so the user doesn't have to scroll past pairs they're
        // not currently working with). Empty global = show all (today's
        // behavior).
        if (typeof getGlobalSelectedPairs === 'function') {
            const sel = new Set(getGlobalSelectedPairs());
            if (sel.size > 0) {
                pairsData = pairsData.filter(p => sel.has(p.pair_name));
            }
            // Re-render when the global set changes elsewhere (e.g. user
            // toggled on the row-gen page or via the header picker).
            if (typeof onGlobalPairSelectionChange === 'function') {
                onGlobalPairSelectionChange(() => loadPairs());
            }
        }
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
    const preset = _currentPreset();
    const params = new URLSearchParams();
    if (from) params.set('from_date', from);
    if (to) params.set('to_date', to);
    if (preset) params.set('preset', preset);

    try {
        const res = await fetch(`/api/compare/col/${name}?${params}`);
        if (!res.ok) {
            // Try to parse the structured error -- a preset filter with
            // no rows on one side returns 400 with {preset_missing: 'left'|'right'}.
            // Render a precise banner so the user knows which side to
            // re-extract under the requested preset (loading the same
            // (col, dt) under a new preset OVERWRITES the prior preset's
            // data because the PK is (source_table, column_name, dt) --
            // not (source_table, column_name, dt, preset)).
            let payload = null;
            try { payload = await res.json(); } catch (e) {}
            if (payload && payload.preset_missing) {
                const side = payload.preset_missing === 'left' ? 'LEFT' : 'RIGHT';
                body.innerHTML = `<div class="empty-message" style="color:var(--jp-error-color0);">
                    No <b>${preset}</b>-preset data loaded for <b>${side}</b> side of <b>${name}</b>.<br><br>
                    To fix: <b>generate + extract + load</b> col stats under preset=<b>${preset}</b>
                    for the ${side.toLowerCase()} side (loading a different preset for the same
                    (column, date) overwrites the previously loaded preset's row).
                </div>`;
            } else {
                throw new Error(payload?.error || await res.text());
            }
            return;
        }
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
            if (_compHasDiff(c)) diffCols.add(c.left_col);
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
    const ann = data.annotations || {comment_left: '', comment_right: '', time_left: '', time_right: ''};

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

        <!-- Editable annotations: render in HTML/Excel exports as the
             ⓘ tooltip on each side and the time-elapsed cell. Persisted
             via PUT /api/pairs/${name}/annotations on edit (debounced). -->
        <div class="rc-annotations">
            <div class="rc-ann-row">
                <label>Left comment:</label>
                <input type="text" id="cc-comment-left-${name}" value="${esc(ann.comment_left || '')}" placeholder="optional note" oninput="ccDebouncedSync('${name}')">
                <label>Time:</label>
                <input type="text" id="cc-time-left-${name}" value="${esc(ann.time_left || '')}" placeholder="e.g. 42s" style="width:80px;" oninput="ccDebouncedSync('${name}')">
            </div>
            <div class="rc-ann-row">
                <label>Right comment:</label>
                <input type="text" id="cc-comment-right-${name}" value="${esc(ann.comment_right || '')}" placeholder="optional note" oninput="ccDebouncedSync('${name}')">
                <label>Time:</label>
                <input type="text" id="cc-time-right-${name}" value="${esc(ann.time_right || '')}" placeholder="e.g. 112s" style="width:80px;" oninput="ccDebouncedSync('${name}')">
            </div>
        </div>

        ${renderVintages(name, data)}
        ${renderOnlyCols(data.left_only, 'Left-Only Columns')}
        ${renderOnlyCols(data.right_only, 'Right-Only Columns')}
    `;
}

// Persist col-scope annotations into pair_cfg.time_map.col / comment_map.col
// (debounced). Frontend IDs are prefixed cc-* to avoid collision with the
// row_compare annotation inputs that share this page's URL space.
const _ccSyncTimers = {};
function ccDebouncedSync(name) {
    clearTimeout(_ccSyncTimers[name]);
    _ccSyncTimers[name] = setTimeout(() => ccSyncPair(name), 600);
}
async function ccSyncPair(name) {
    const cl = document.getElementById(`cc-comment-left-${name}`)?.value || '';
    const cr = document.getElementById(`cc-comment-right-${name}`)?.value || '';
    const tl = document.getElementById(`cc-time-left-${name}`)?.value || '';
    const tr = document.getElementById(`cc-time-right-${name}`)?.value || '';
    try {
        await fetch(`/api/pairs/${encodeURIComponent(name)}/annotations`, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                time_map: {col: {left: tl, right: tr}},
                comment_map: {col: {left: cl, right: cr}},
            }),
        });
    } catch (e) {
        console.error('annotation sync failed:', e);
    }
}

// ---------------------------------------------------------------------------
// Per-vintage comparison
// ---------------------------------------------------------------------------
function renderVintages(name, data) {
    const vintages = data.vintages || [];
    if (!vintages.length) return '<div class="empty-message">No vintage data</div>';

    return vintages.map((v, vi) => {
        const cols = v.columns || [];
        const nDiff = cols.filter(c => _compHasDiff(c)).length;
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
                    ${renderColTable(name, cols, v.label || v.dt || '')}
                </div>
            </div>
        </details>`;
    }).join('');
}

function renderColTable(name, cols, vintage) {
    if (!cols.length) return '<div class="empty-message">No matched columns</div>';
    const vlabel = vintage || '';

    const sorted = [...cols].sort((a, b) => {
        const ad = _compHasDiff(a), bd = _compHasDiff(b);
        if (ad !== bd) return ad ? -1 : 1;
        return (a.left_col || '').localeCompare(b.left_col || '');
    });

    // Stat rows. The preset-driven model: every row is rendered, but
    // stats not produced by the active preset (e.g. mean/std are
    // intentionally dropped from all presets now; n_unique is only in
    // Standard/Detailed; top_10 only in Detailed) come back as '' from
    // the loaded _col_stats and render as "—". This keeps the table
    // shape stable across preset switches and tells the user at a
    // glance which stats their preset chose to skip.
    const _statDefsAll = [
        {key: 'type',       label: 'Type',      keyL: 'col_type',        keyR: 'col_type',         isDiff: () => false, fmt: 'text'},
        {key: 'n_total',    label: 'N_Total',   keyL: 'n_total_left',    keyR: 'n_total_right',    isDiff: c => c.n_total_diff !== 0, fmt: 'int'},
        {key: 'n_missing',  label: 'N_Missing', keyL: 'n_missing_left',  keyR: 'n_missing_right',  isDiff: c => c.n_missing_diff !== 0, fmt: 'int'},
        {key: 'n_unique',   label: 'N_Unique',  keyL: 'n_unique_left',   keyR: 'n_unique_right',   isDiff: c => c.n_unique_diff !== 0, fmt: 'int'},
        {key: 'min',        label: 'Min',       keyL: 'min_left',        keyR: 'min_right',        isDiff: c => c.min_match === false, fmt: 'text'},
        {key: 'max',        label: 'Max',       keyL: 'max_left',        keyR: 'max_right',        isDiff: c => c.max_match === false, fmt: 'text'},
        {key: 'length_avg', label: 'Length avg',keyL: 'length_avg_left', keyR: 'length_avg_right', isDiff: c => c.length_avg_match === false, fmt: 'num'},
        {key: 'length_max', label: 'Length max',keyL: 'length_max_left', keyR: 'length_max_right', isDiff: c => c.length_max_match === false, fmt: 'int'},
        {key: 'hash_sum',   label: 'Hash sum',  keyL: 'hash_sum_left',   keyR: 'hash_sum_right',   isDiff: c => c.hash_sum_match === false, fmt: 'sci'},
    ];
    const _activeFields = new Set(_getCompareFields());
    const statDefs = _statDefsAll.filter(sd => sd.key === 'type' || _activeFields.has(sd.key));

    function fmtCell(val, fmt) {
        if (val == null || val === '') return '&mdash;';
        if (fmt === 'int') return fmtVal(val);
        if (fmt === 'num') return fmtNum(val);
        if (fmt === 'sci') return fmtSci(val);
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

    // Build header row (variable names) — per side. The left-side header is
    // clickable for fine-drill selection; the right-side header just mirrors
    // the column name without selection (selection is keyed off left col).
    function colHeadersForSide(side) {
        return sorted.map((c, ci) => {
            const dispName = side === 'left' ? c.left_col : (c.right_col || c.left_col);
            const icon = _compHasDiff(c) ? '<span style="color:var(--jp-warn-color0);">&#9888;</span> ' : '';
            if (side !== 'left') {
                return `<th style="text-align:right; font-size:11px;">${icon}${esc(dispName)}</th>`;
            }
            const cellKey = `${name}|${c.left_col}|${vlabel}`;
            const sel = selectedCells.has(cellKey) ? ' cell-selected' : '';
            const colJs = JSON.stringify(c.left_col);
            const vJs = JSON.stringify(vlabel);
            return `<th class="cell-selectable${sel}" data-cell-key="${cellKey}" `
                + `title="Click to select for fine drill-down" `
                + `onclick="toggleCellSelection(${JSON.stringify(name)}, ${colJs}, ${vJs})" `
                + `style="text-align:right; font-size:11px; cursor:pointer; user-select:none;">`
                + `${icon}${esc(dispName)}</th>`;
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
    const preset = _currentPreset();
    const params = new URLSearchParams();
    if (from) params.set('from_date', from);
    if (to) params.set('to_date', to);
    if (preset) params.set('preset', preset);
    const fields = _getCompareFields();
    if (fields.length < COMPARE_FIELDS.length) {
        params.set('compare_fields', fields.join(','));
    }
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

// Scientific notation with fixed significant figures. Used for hash_sum
// so a SAS-emitted integer like "100336545671234568" and an Athena-emitted
// "1.00336545E17" render in the same form for visual comparison.
// 11 sig figs (toExponential(10)) is inside DOUBLE's exact range (15-17),
// so identical underlying values always render to the same string. With
// the 16-bit MD5 hash, billion-row sums stay <= 2^53 (exact integer in
// DOUBLE) so the display matches whenever the underlying data matches.
function fmtSci(n) {
    if (n == null || n === '') return '—';
    const num = Number(n);
    if (isNaN(num)) return String(n);
    if (num === 0) return '0';
    return num.toExponential(10);
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
        // Collect live annotations for every pair currently rendered, so
        // unsaved edits flow into the downloaded HTML alongside the
        // saved-config fallback.
        const live_pairs = {};
        for (const p of pairsData) {
            const name = p.pair_name;
            if (!document.getElementById(`cc-comment-left-${name}`)) continue;
            live_pairs[name] = {
                comment_left:  document.getElementById(`cc-comment-left-${name}`)?.value  || '',
                comment_right: document.getElementById(`cc-comment-right-${name}`)?.value || '',
                time_left:     document.getElementById(`cc-time-left-${name}`)?.value     || '',
                time_right:    document.getElementById(`cc-time-right-${name}`)?.value    || '',
            };
        }
        const res = await fetch('/api/compare/col/export/html', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                from_date: document.getElementById('global-from')?.value || '',
                to_date: document.getElementById('global-to')?.value || '',
                title: document.getElementById('global-title')?.value || '',
                subtitle: document.getElementById('global-subtitle')?.value || '',
                pairs: live_pairs,
                preset: _currentPreset(),
                compare_fields: _getCompareFields(),
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
                preset: _currentPreset(),
                compare_fields: _getCompareFields(),
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
