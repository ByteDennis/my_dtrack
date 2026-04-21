// col_gen.js - Column generation page logic

let pairs = [];

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', () => {
    loadPairs();
    loadGlobalSettings();
    initModalHandlers();
});

// ---------------------------------------------------------------------------
// Navigation
// ---------------------------------------------------------------------------
function navigateToStep(step) {
    const routes = {
        pairs: '/pairs', load_row: '/load_row', row_compare: '/row_compare',
        col_mapping: '/col_mapping', col_gen: '/col_gen',
        load_col: '/load_col', col_compare: '/col_compare',
    };
    if (routes[step]) window.location.href = routes[step];
}

function showHelp() {
    alert('Col Gen generates SAS/SQL scripts for extracting column-level statistics.\n\nSelect pairs and click "Generate Col Scripts" to create extraction files.');
}

// ---------------------------------------------------------------------------
// Settings Modal (global defaults)
// ---------------------------------------------------------------------------
function openSettingsModal() {
    document.getElementById('settings-modal').classList.add('active');
}

function closeSettingsModal() {
    document.getElementById('settings-modal').classList.remove('active');
}

function initModalHandlers() {
    document.getElementById('settings-modal').addEventListener('click', (e) => {
        if (e.target.id === 'settings-modal') closeSettingsModal();
    });
}

async function loadGlobalSettings() {
    try {
        const resp = await fetch('/api/config');
        if (!resp.ok) return;
        const config = await resp.json();
        const s = config.settings || {};

        if (s.from_date) document.getElementById('global-from-date').value = s.from_date;
        if (s.to_date) document.getElementById('global-to-date').value = s.to_date;
        if (s.sas_outdir) document.getElementById('global-sas-outdir').value = s.sas_outdir;
        if (s.aws_outdir) document.getElementById('global-aws-outdir').value = s.aws_outdir;
    } catch (e) {
        // Non-critical
    }
}

async function saveGlobalSettings() {
    const settings = {
        from_date: document.getElementById('global-from-date').value || '',
        to_date: document.getElementById('global-to-date').value || '',
        sas_outdir: document.getElementById('global-sas-outdir').value || './sas/',
        aws_outdir: document.getElementById('global-aws-outdir').value || './csv/',
    };
    try {
        await fetch('/api/config', {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({settings}),
        });
    } catch (e) {
        console.error('Failed to save settings:', e);
    }
}

function applySettings() {
    // Apply global from/to/mode to all pairs as defaults
    const globalFrom = document.getElementById('global-from-date').value || '';
    const globalTo = document.getElementById('global-to-date').value || '';
    pairs.forEach(p => {
        if (!p.fromDate) p.fromDate = globalFrom;
        if (!p.toDate) p.toDate = globalTo;
    });
    saveGlobalSettings();
    renderPairs();
    closeSettingsModal();
    showSuccess('Global settings applied to all pairs');
}

function clearGlobalDates() {
    document.getElementById('global-from-date').value = '';
    document.getElementById('global-to-date').value = '';
}

function setQuickDate(days) {
    const today = new Date();
    const fromDate = new Date(today);

    if (typeof days === 'number') {
        fromDate.setDate(today.getDate() - days);
    } else if (days === 'week') {
        const dayOfWeek = today.getDay();
        fromDate.setDate(today.getDate() - dayOfWeek);
    } else if (days === 'month') {
        fromDate.setDate(1);
    }

    document.getElementById('global-from-date').valueAsDate = fromDate;
    document.getElementById('global-to-date').valueAsDate = today;
}

// ---------------------------------------------------------------------------
// Load Pairs (from config, with per-pair settings)
// ---------------------------------------------------------------------------
async function loadPairs() {
    try {
        const [pairsResp, statusResp] = await Promise.all([
            fetch('/api/pairs/list'),
            fetch('/api/status'),
        ]);
        const pairsData = await pairsResp.json();
        const statusData = await statusResp.json();

        const statusMap = {};
        for (const p of (statusData.pairs || [])) {
            statusMap[p.pair_name] = p;
        }

        // Prefer locally-cached patterns (user may have typed but the server
        // save is still in flight or failed). Falls back to what the server
        // has persisted in dtrack.json.
        const lsCache = (() => {
            try { return JSON.parse(localStorage.getItem('dtrack_col_filter') || '{}'); }
            catch { return {}; }
        })();
        pairs = (pairsData.pairs || []).map(p => {
            const serverFilter = p.col_filter || {};
            const cached = lsCache[p.name] || {};
            const include = (cached.include && cached.include.length ? cached.include : serverFilter.include || []);
            const exclude = (cached.exclude && cached.exclude.length ? cached.exclude : serverFilter.exclude || []);
            return {
                name: p.name,
                description: p.description || '',
                left: p.left || {},
                right: p.right || {},
                mode: p.mode || 'incremental',
                vintage: p.vintage || '',
                fromDate: p.fromDate || '',
                toDate: p.toDate || '',
                excludeDates: (p.excludeDates || []).join(', '),
                colFilterInclude: include.join(', '),
                colFilterExclude: exclude.join(', '),
                filterPreview: null,   // {effective_count, total_mapped, pairs, unmapped_matches}
                selected: false,
                expanded: false,
                status: statusMap[p.name] || null,
            };
        });

        renderPairs();
    } catch (e) {
        console.error('Failed to load pairs:', e);
        showError('Failed to load pairs: ' + e.message);
    }
}

// ---------------------------------------------------------------------------
// Render Pairs (with per-pair from/to, vintage, mode)
// ---------------------------------------------------------------------------
const VINTAGE_OPTIONS = ['', 'day', 'week', 'month', 'quarter', 'year', 'all'];
const VINTAGE_LABELS = {
    '': '— none —', day: 'day', week: 'week', month: 'month',
    quarter: 'quarter', year: 'year', all: 'all (single bucket)',
};

function vintageSelect(id, value, extraStyle = '') {
    const opts = VINTAGE_OPTIONS.map(v =>
        `<option value="${v}" ${v === (value || '') ? 'selected' : ''}>${VINTAGE_LABELS[v] || v}</option>`
    ).join('');
    return `<select id="${id}" style="${extraStyle}" onchange="pairFieldChanged()" onclick="event.stopPropagation()">${opts}</select>`;
}

function renderPairs() {
    const container = document.getElementById('pairs-list');
    const emptyState = document.getElementById('pairs-empty');

    if (pairs.length === 0) {
        container.style.display = 'none';
        emptyState.style.display = 'block';
        return;
    }

    container.style.display = 'flex';
    emptyState.style.display = 'none';

    container.innerHTML = pairs.map((pair, index) => {
        const s = pair.status;
        const leftCols = s ? (s.left.col_count || 0) : 0;
        const rightCols = s ? (s.right.col_count || 0) : 0;
        const leftColBadge = leftCols > 0
            ? `<span class="status-badge ready">${leftCols} cols</span>`
            : `<span class="status-badge warning">no cols</span>`;
        const rightColBadge = rightCols > 0
            ? `<span class="status-badge ready">${rightCols} cols</span>`
            : `<span class="status-badge warning">no cols</span>`;

        const modeChecked = pair.mode !== 'full' ? 'checked' : '';

        return `
        <div class="pair-item ${pair.selected ? 'selected' : ''}" id="pair-${index}">
            <div class="pair-header" onclick="togglePair(${index})">
                <input type="checkbox" class="pair-checkbox"
                    ${pair.selected ? 'checked' : ''}
                    onclick="event.stopPropagation(); togglePairSelection(${index})"
                    onchange="togglePairSelection(${index})">
                <span class="pair-expand">${pair.expanded ? '&#9654;' : '&#9660;'}</span>
                <span class="pair-name">${pair.name}</span>
                <div style="display:flex; gap:8px; align-items:center; margin-left:auto;">
                    <label style="display:flex; align-items:center; gap:4px; font-size:12px; font-weight:600; color:var(--jp-ui-font-color1);" onclick="event.stopPropagation()">
                        Vintage:
                        ${vintageSelect(`vintage-${index}`, pair.vintage, 'width:140px; font-size:12px; padding:2px 4px;')}
                    </label>
                    <span id="filter-badge-${index}">${filterCountBadge(pair)}</span>
                    ${leftColBadge}
                    ${rightColBadge}
                </div>
            </div>
            <div class="pair-body ${pair.expanded ? 'expanded' : ''}">
                ${pair.description ? `<div style="margin-bottom:12px; color: var(--jp-ui-font-color2); font-size:13px;">${_escapeHtml(pair.description)}</div>` : ''}
                <div class="pair-info">
                    <div class="pair-side-info">
                        <div class="pair-side-title">Left</div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Source:</span>
                            <span class="pair-info-value">${pair.left.source || ''}</span>
                        </div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Table:</span>
                            <span class="pair-info-value">${pair.left.table || ''}</span>
                        </div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Columns:</span>
                            <span class="pair-info-value">${leftCols}</span>
                        </div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Date Type:</span>
                            <span class="pair-info-value">${pair.left.date_type || ''}</span>
                        </div>
                    </div>
                    <div class="pair-side-info">
                        <div class="pair-side-title">Right</div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Source:</span>
                            <span class="pair-info-value">${pair.right.source || ''}</span>
                        </div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Table:</span>
                            <span class="pair-info-value">${pair.right.table || ''}</span>
                        </div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Columns:</span>
                            <span class="pair-info-value">${rightCols}</span>
                        </div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Date Type:</span>
                            <span class="pair-info-value">${pair.right.date_type || ''}</span>
                        </div>
                    </div>
                </div>
                <!-- Per-pair date range, mode, exclude dates -->
                <div style="display:flex; align-items:center; gap:12px; margin-top:10px; font-size:12px; flex-wrap:wrap;" onclick="event.stopPropagation()">
                    <label style="display:flex; align-items:center; gap:4px;">
                        From: <input type="date" id="from-${index}" value="${pair.fromDate || ''}" style="width:130px; font-size:11px;" onchange="pairFieldChanged()">
                    </label>
                    <label style="display:flex; align-items:center; gap:4px;">
                        To: <input type="date" id="to-${index}" value="${pair.toDate || ''}" style="width:130px; font-size:11px;" onchange="pairFieldChanged()">
                    </label>
                    <label style="display:flex; align-items:center; gap:4px;">
                        <input type="checkbox" id="mode-incr-${index}" ${modeChecked} onchange="pairFieldChanged()">
                        Incremental
                    </label>
                </div>
                <div style="margin-top:8px; font-size:12px;" onclick="event.stopPropagation()">
                    <label style="display:flex; align-items:flex-start; gap:4px;">
                        Exclude dates:
                        <input type="text" id="exclude-${index}" value="${pair.excludeDates || ''}"
                            placeholder="e.g. 2025-01-01, 2025-12-25" style="flex:1; font-size:11px;"
                            onchange="pairFieldChanged()">
                    </label>
                </div>

                <!-- COLUMN SELECTION -->
                <div class="col-filter-section" style="margin-top:12px; padding-top:10px; border-top:1px solid var(--jp-border-color1);"
                     onclick="event.stopPropagation()">
                    <div style="font-size:12px; font-weight:600; margin-bottom:6px; color:var(--jp-ui-font-color1);">
                        COLUMN SELECTION
                        <span style="font-weight:400; color:var(--jp-ui-font-color3);">
                            — patterns match LEFT names; RIGHT resolved via col_map
                        </span>
                    </div>
                    <div style="display:flex; gap:12px;">
                        <label style="flex:1; display:flex; flex-direction:column; gap:2px; font-size:11px;">
                            Include (empty = all mapped)
                            <textarea id="filter-include-${index}" rows="2"
                                placeholder="CUST_*, AMT_*, BAL_EUR"
                                style="width:100%; font-family:var(--jp-code-font-family); font-size:11px; resize:vertical;"
                                oninput="onColFilterInput(${index})">${_escapeHtml(pair.colFilterInclude || '')}</textarea>
                        </label>
                        <label style="flex:1; display:flex; flex-direction:column; gap:2px; font-size:11px;">
                            Exclude (applied after include)
                            <textarea id="filter-exclude-${index}" rows="2"
                                placeholder="*_AUDIT*, *_TMP"
                                style="width:100%; font-family:var(--jp-code-font-family); font-size:11px; resize:vertical;"
                                oninput="onColFilterInput(${index})">${_escapeHtml(pair.colFilterExclude || '')}</textarea>
                        </label>
                    </div>
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-top:6px; font-size:11px;">
                        <span id="filter-summary-${index}" style="color:var(--jp-ui-font-color2);">
                            ${filterSummaryText(pair)}
                        </span>
                        <button class="btn-text" onclick="togglePreview(${index})" style="font-size:11px;">
                            <span id="filter-preview-caret-${index}">&#9654;</span> Preview pairs
                        </button>
                    </div>
                    <div id="filter-preview-${index}" style="display:none; margin-top:6px;"></div>
                </div>
            </div>
        </div>`;
    }).join('');

    // Trigger a preview for each expanded pair (shows initial effective count).
    pairs.forEach((p, i) => { if (p.expanded) requestFilterPreview(i); });
}

// ---------------------------------------------------------------------------
// Col-filter header badge + summary line + debounced preview
// ---------------------------------------------------------------------------
function filterCountBadge(pair) {
    const prev = pair.filterPreview;
    if (!prev) return '';
    const n = prev.effective_count;
    const total = prev.total_mapped || 0;
    if (total === 0) return '';
    const isFull = n === total && !(pair.colFilterInclude || pair.colFilterExclude);
    if (isFull) return `<span class="status-badge ready">${total} pairs</span>`;
    const kind = n > 0 ? 'ready' : 'warning';
    return `<span class="status-badge ${kind}">${n}/${total} pairs</span>`;
}

function filterSummaryText(pair) {
    const prev = pair.filterPreview;
    if (!prev) return 'Loading preview...';
    const n = prev.effective_count, total = prev.total_mapped || 0;
    if (total === 0) return 'No col_map for this pair yet — define mappings in Col Mapping first.';
    const extra = prev.unmapped_matches && prev.unmapped_matches.length
        ? ` — ${prev.unmapped_matches.length} pattern match(es) NOT in col_map (ignored)`
        : '';
    return `Effective: ${n} / ${total} mapped pairs${extra}`;
}

const _filterDebounce = {};
const _filterSaveDebounce = {};
const _LS_KEY = 'dtrack_col_filter';  // localStorage namespace

function _lsReadAll() {
    try { return JSON.parse(localStorage.getItem(_LS_KEY) || '{}'); }
    catch { return {}; }
}
function _lsWrite(pairName, include, exclude) {
    const all = _lsReadAll();
    if ((include || []).length || (exclude || []).length) {
        all[pairName] = {include, exclude};
    } else {
        delete all[pairName];
    }
    try { localStorage.setItem(_LS_KEY, JSON.stringify(all)); } catch {}
}

function onColFilterInput(index) {
    const pair = pairs[index];
    pair.colFilterInclude = document.getElementById(`filter-include-${index}`).value;
    pair.colFilterExclude = document.getElementById(`filter-exclude-${index}`).value;

    // Mirror to localStorage immediately (synchronous, survives reload even if
    // the server write is still in flight).
    const parseList = s => (s || '').split(/[,\n;]/).map(t => t.trim()).filter(Boolean);
    _lsWrite(pair.name, parseList(pair.colFilterInclude), parseList(pair.colFilterExclude));

    clearTimeout(_filterDebounce[index]);
    _filterDebounce[index] = setTimeout(() => requestFilterPreview(index), 300);
    // Persist to dtrack.json so patterns survive page reload without
    // needing to click Generate. Slower debounce to avoid chatty writes.
    clearTimeout(_filterSaveDebounce[index]);
    _filterSaveDebounce[index] = setTimeout(() => saveColFilter(index), 1000);
}

async function saveColFilter(index) {
    const pair = pairs[index];
    const parseList = s => (s || '').split(/[,\n;]/).map(t => t.trim()).filter(Boolean);
    const include = parseList(pair.colFilterInclude);
    const exclude = parseList(pair.colFilterExclude);
    const col_filter = (include.length || exclude.length) ? {include, exclude} : null;
    try {
        await fetch(`/api/pairs/${encodeURIComponent(pair.name)}`, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({col_filter}),
        });
    } catch (e) {
        console.error('[col_filter] save failed:', e);
    }
}

async function requestFilterPreview(index) {
    const pair = pairs[index];
    const parseList = s => (s || '').split(/[,\n;]/).map(t => t.trim()).filter(Boolean);
    const include = parseList(pair.colFilterInclude);
    const exclude = parseList(pair.colFilterExclude);
    try {
        const resp = await fetch(`/api/pairs/${encodeURIComponent(pair.name)}/col_filter/preview`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({include, exclude}),
        });
        const data = await resp.json();
        if (!resp.ok || data.error) throw new Error(data.error || 'preview failed');
        pair.filterPreview = data;
        updateFilterSummary(index);
    } catch (e) {
        console.error('[col_filter] preview failed:', e);
    }
}

function updateFilterSummary(index) {
    const pair = pairs[index];
    const sum = document.getElementById(`filter-summary-${index}`);
    if (sum) sum.textContent = filterSummaryText(pair);
    const badge = document.getElementById(`filter-badge-${index}`);
    if (badge) badge.innerHTML = filterCountBadge(pair);
    const preview = document.getElementById(`filter-preview-${index}`);
    if (preview && preview.style.display !== 'none') renderPreviewBody(index);
}

function togglePreview(index) {
    const panel = document.getElementById(`filter-preview-${index}`);
    const caret = document.getElementById(`filter-preview-caret-${index}`);
    if (!panel) return;
    const visible = panel.style.display !== 'none';
    panel.style.display = visible ? 'none' : '';
    if (caret) caret.innerHTML = visible ? '&#9654;' : '&#9660;';
    if (!visible) renderPreviewBody(index);
}

function renderPreviewBody(index) {
    const pair = pairs[index];
    const panel = document.getElementById(`filter-preview-${index}`);
    if (!panel) return;
    const prev = pair.filterPreview;
    if (!prev) { panel.innerHTML = '<div class="empty-message">Loading...</div>'; return; }
    const rows = (prev.pairs || []).slice(0, 20).map(p =>
        `<tr><td style="padding:2px 8px;">${_escapeHtml(p.left)}</td>` +
        `<td style="padding:2px 4px; color:var(--jp-ui-font-color3);">&rarr;</td>` +
        `<td style="padding:2px 8px;">${_escapeHtml(p.right)}${
            p.left.toLowerCase() !== p.right.toLowerCase()
                ? ' <span style="color:var(--jp-warn-color0); font-size:10px;">renamed</span>'
                : ''
        }</td></tr>`
    ).join('');
    const moreMsg = prev.pairs.length > 20
        ? `<div style="color:var(--jp-ui-font-color3); padding:4px 8px; font-size:10px;">
             …first 20 shown, ${prev.pairs.length - 20} more
           </div>` : '';
    const warnMsg = (prev.unmapped_matches || []).length
        ? `<div style="color:var(--jp-warn-color0); padding:6px 8px; font-size:11px; border-top:1px solid var(--jp-border-color1); margin-top:4px;">
             &#9888; ${prev.unmapped_matches.length} pattern match(es) NOT in col_map — ignored:<br>
             <span style="font-family:var(--jp-code-font-family); font-size:10px;">
               ${prev.unmapped_matches.slice(0, 30).map(_escapeHtml).join(', ')}
               ${prev.unmapped_matches.length > 30 ? ', …' : ''}
             </span>
           </div>` : '';
    panel.innerHTML = `
        <div style="border:1px solid var(--jp-border-color1); border-radius:4px; background:var(--jp-layout-color1); font-size:11px;">
            <table style="width:100%; border-collapse:collapse;">
                <thead><tr style="background:var(--jp-layout-color2);">
                    <th style="text-align:left; padding:3px 8px;">LEFT</th>
                    <th></th>
                    <th style="text-align:left; padding:3px 8px;">RIGHT</th>
                </tr></thead>
                <tbody>${rows}</tbody>
            </table>
            ${moreMsg}
            ${warnMsg}
        </div>`;
}

// Read edited per-pair values back into the pairs array
function syncPairFields() {
    pairs.forEach((pair, index) => {
        const fromEl = document.getElementById(`from-${index}`);
        const toEl = document.getElementById(`to-${index}`);
        const modeEl = document.getElementById(`mode-incr-${index}`);
        const vintEl = document.getElementById(`vintage-${index}`);
        const excludeEl = document.getElementById(`exclude-${index}`);

        if (fromEl) pair.fromDate = fromEl.value;
        if (toEl) pair.toDate = toEl.value;
        if (modeEl) pair.mode = modeEl.checked ? 'incremental' : 'full';
        if (vintEl) pair.vintage = vintEl.value;
        if (excludeEl) pair.excludeDates = excludeEl.value;
    });
}

function pairFieldChanged() {
    syncPairFields();
}

function togglePair(index) {
    pairs[index].expanded = !pairs[index].expanded;
    syncPairFields(); // preserve edits before re-render
    renderPairs();
}

function togglePairSelection(index) {
    syncPairFields();
    pairs[index].selected = !pairs[index].selected;
    syncSelectAllCheckbox();
    renderPairs();
}

function selectAllPairs(selected) {
    syncPairFields();
    pairs.forEach(p => p.selected = selected);
    syncSelectAllCheckbox();
    renderPairs();
}

function syncSelectAllCheckbox() {
    const cb = document.getElementById('select-all-pairs');
    if (!cb) return;
    const allSelected = pairs.length > 0 && pairs.every(p => p.selected);
    const someSelected = pairs.some(p => p.selected);
    cb.checked = allSelected;
    cb.indeterminate = someSelected && !allSelected;
}

// ---------------------------------------------------------------------------
// Save per-pair settings to config (so server reads correct vintage/dates)
// ---------------------------------------------------------------------------
async function savePairSettings(selected) {
    const results = await Promise.all(selected.map(async pair => {
        try {
            // Parse exclude dates string into array
            const excludeArr = (pair.excludeDates || '').split(/[,;\s]+/).map(s => s.trim()).filter(Boolean);
            const parseList = s => (s || '').split(/[,\n;]/).map(t => t.trim()).filter(Boolean);
            const include = parseList(pair.colFilterInclude);
            const exclude = parseList(pair.colFilterExclude);
            const col_filter = (include.length || exclude.length) ? {include, exclude} : null;

            await fetch(`/api/pairs/${pair.name}`, {
                method: 'PUT',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    left: pair.left,
                    right: pair.right,
                    mode: pair.mode,
                    vintage: pair.vintage,
                    fromDate: pair.fromDate,
                    toDate: pair.toDate,
                    excludeDates: excludeArr,
                    col_filter,
                }),
            });
            return {name: pair.name, ok: true};
        } catch (e) {
            return {name: pair.name, ok: false, error: e.message};
        }
    }));
    return results;
}

// ---------------------------------------------------------------------------
// Generation (uses per-pair settings)
// ---------------------------------------------------------------------------
async function generateAll() {
    syncPairFields();

    const selected = pairs.filter(p => p.selected);
    if (selected.length === 0) {
        alert('No pairs selected');
        return;
    }

    const log = document.getElementById('generation-log');
    log.innerHTML = '';

    // Fall back to global settings for pairs without per-pair dates
    const globalFrom = document.getElementById('global-from-date')?.value || '';
    const globalTo = document.getElementById('global-to-date')?.value || '';

    // Save per-pair settings to config BEFORE generating
    // so the server reads the correct vintage/dates/mode
    logMessage('Saving pair settings to config...', 'info');
    const saveResults = await savePairSettings(selected);
    const saveFails = saveResults.filter(r => !r.ok);
    if (saveFails.length) {
        saveFails.forEach(r => logMessage(`  WARN: failed to save ${r.name}: ${r.error}`, 'error'));
    }

    logMessage(`Generating col scripts for ${selected.length} pair(s)...`, 'info');
    for (const p of selected) {
        const from = p.fromDate || globalFrom;
        const to = p.toDate || globalTo;
        const v = p.vintage || 'none';
        logMessage(`  ${p.name}: ${from || '...'} → ${to || '...'} | vintage: ${v} | mode: ${p.mode}`, 'info');
    }

    // Detect if any selected pair has AWS source
    const awsSources = new Set(['aws', 'csv']);
    const hasAws = selected.some(p => awsSources.has(p.left?.source) || awsSources.has(p.right?.source));

    // The backend resolves per-pair fromDate/toDate from the config (set by
    // each card's From/To inputs via savePairSettings above). We only pass
    // the GLOBAL defaults here as a fallback for pairs that don't set their
    // own — do not flatten per-pair dates into a single widest range.
    const effectiveFrom = globalFrom;
    const effectiveTo = globalTo;

    try {
        const genBody = {
            type: 'col',
            sas_outdir: document.getElementById('global-sas-outdir')?.value || './sas/',
            aws_outdir: document.getElementById('global-aws-outdir')?.value || './csv/',
            pair_names: selected.map(p => p.name),
        };
        if (effectiveFrom) genBody.from_date = effectiveFrom;
        if (effectiveTo) genBody.to_date = effectiveTo;

        const resp = await fetch('/api/generate', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(genBody),
        });
        const result = await resp.json();
        if (result.ok) {
            if (result.output) {
                result.output.trim().split('\n').forEach(line => {
                    if (!line.trim()) return;
                    let type = 'info';
                    if (line.includes('[BUCKET CHECK]')) {
                        if (/\bPASS\b/.test(line)) type = 'success';
                        else if (/\bWARNING\b/.test(line)) type = 'warning';
                    }
                    logMessage(line, type);
                });
            }
            if (result.sas_file) logMessage(`SAS file: ${result.sas_file}`, 'success');
            if (result.sql_file) logMessage(`SQL file: ${result.sql_file}`, 'success');
            logMessage('Col script generation complete.', 'success');
        } else {
            logMessage(`Generation error: ${result.error}`, 'error');
            return;
        }
    } catch (err) {
        logMessage(`Generation failed: ${err.message}`, 'error');
        return;
    }

    // Show Run AWS button if AWS tables exist
    if (hasAws) {
        const btnDiv = document.createElement('div');
        btnDiv.style.cssText = 'margin:12px 0; display:flex; gap:12px; align-items:center;';
        btnDiv.innerHTML = `<button class="btn-primary" id="run-aws-col-btn">Run AWS Extraction</button>`;
        log.appendChild(btnDiv);

        document.getElementById('run-aws-col-btn').onclick = () => runAwsColExtraction();
    }
}

async function runAwsColExtraction() {
    const btn = document.getElementById('run-aws-col-btn');
    btn.disabled = true;
    btn.textContent = 'Running...';

    logMessage('Running AWS col extraction from extract_col.sql...', 'info');

    try {
        const reqBody = {
            type: 'col',
            outdir: document.getElementById('global-aws-outdir')?.value || './csv/',
        };

        const resp = await fetch('/api/extract/run-sql', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(reqBody),
        });

        const reader = resp.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
            const {done, value} = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, {stream: true});

            const parts = buffer.split('\n\n');
            buffer = parts.pop();

            for (const part of parts) {
                let eventType = 'message';
                let data = '';
                for (const line of part.split('\n')) {
                    if (line.startsWith('event: ')) eventType = line.slice(7);
                    else if (line.startsWith('data: ')) data = line.slice(6);
                }
                if (!data) continue;
                const msg = JSON.parse(data);

                if (eventType === 'progress') {
                    const tblProg = msg.table_total
                        ? ` (${msg.table} ${msg.table_done}/${msg.table_total})`
                        : '';
                    const status = msg.ok
                        ? `${msg.rows} rows, ${msg.elapsed}s`
                        : `FAIL: ${msg.error}`;
                    logMessage(`[${msg.done}/${msg.total}] ${msg.name}: ${status}${tblProg}`, msg.ok ? 'info' : 'error');
                    btn.textContent = `Running ${msg.done}/${msg.total}...`;
                } else if (eventType === 'done') {
                    if (msg.ok) {
                        logMessage(`AWS col extraction complete: ${msg.succeeded}/${msg.total} succeeded`, 'success');
                        btn.textContent = 'Done';
                        btn.style.background = 'var(--jp-success-color1)';
                    } else if (msg.results) {
                        const failed = msg.results.filter(r => !r.ok);
                        failed.forEach(r => logMessage(`  FAILED: ${r.name} — ${r.error}`, 'error'));
                        logMessage(`${msg.succeeded}/${msg.total} succeeded, ${msg.failed} failed`, 'error');
                        btn.textContent = 'Retry';
                        btn.disabled = false;
                    } else {
                        logMessage(`AWS error: ${msg.error}`, 'error');
                        btn.textContent = 'Retry';
                        btn.disabled = false;
                    }
                }
            }
        }
    } catch (err) {
        logMessage(`AWS request failed: ${err.message}`, 'error');
        btn.textContent = 'Retry';
        btn.disabled = false;
    }
}

// ---------------------------------------------------------------------------
// Log
// ---------------------------------------------------------------------------
function clearLog() {
    const log = document.getElementById('generation-log');
    log.innerHTML = '<div class="log-empty">No files generated yet.</div>';
}

function logMessage(message, type = 'info') {
    const log = document.getElementById('generation-log');
    const empty = log.querySelector('.log-empty');
    if (empty) empty.remove();

    const time = new Date().toLocaleTimeString();
    const className = type === 'success' ? 'log-success'
        : type === 'error' ? 'log-error'
        : type === 'warning' ? 'log-warning'
        : '';

    const entry = document.createElement('div');
    entry.className = 'log-entry';
    entry.innerHTML = `
        <span class="log-time">${time}</span>
        <span class="log-message ${className}">${message}</span>
    `;
    log.appendChild(entry);
    log.scrollTop = log.scrollHeight;
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------
function _escapeHtml(str) {
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function showSuccess(message) {
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed; top: 20px; right: 20px; z-index: 2000;
        background: #e8f5e9; color: #2e7d32; border: 1px solid #a5d6a7;
        padding: 12px 20px; border-radius: 6px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        font-size: 13px;
    `;
    notification.textContent = message;
    document.body.appendChild(notification);
    setTimeout(() => notification.remove(), 3000);
}

function showError(message) {
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed; top: 20px; right: 20px; z-index: 2000;
        background: #fbe9e7; color: #c62828; border: 1px solid #ef9a9a;
        padding: 12px 20px; border-radius: 6px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        font-size: 13px;
    `;
    notification.textContent = message;
    document.body.appendChild(notification);
    setTimeout(() => notification.remove(), 5000);
}
