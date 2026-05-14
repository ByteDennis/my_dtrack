// pairs.js - Pair management and row generation

let pairs = [];
let editingPairIndex = -1;

// UI constants are served by GET /api/constants (source of truth:
// dtrack/constants.py). Kept mutable so initConstants() can populate them
// before initSourceDropdowns() reads them. When adding a new date format,
// update dtrack/constants.py — NOT this file.
let DATA_SOURCES = [];
let CONNECTION_MACROS = {};
let DATE_COLUMN_TYPES = [];

async function initConstants() {
    const resp = await fetch('/api/constants');
    if (!resp.ok) throw new Error(`GET /api/constants failed: ${resp.status}`);
    const data = await resp.json();
    DATA_SOURCES = data.DATA_SOURCES || [];
    CONNECTION_MACROS = data.CONNECTION_MACROS || {};
    DATE_COLUMN_TYPES = data.DATE_COLUMN_TYPES || [];
}

// Initialize on page load. initConstants must resolve before
// initSourceDropdowns so the source/date-type <select> options render.
document.addEventListener('DOMContentLoaded', async () => {
    try {
        await initConstants();
    } catch (e) {
        console.error(e);
        alert('Failed to load UI constants — dropdowns will be empty. See console.');
    }
    loadPairs();
    loadGlobalSettings();
    initModalHandlers();
    initDropHandlers();
    initSourceDropdowns();
    initTestingMode();
});

// Cached retry settings per source -- shows the user's saved value when
// they switch the dropdown without re-fetching. Reset every time the
// modal opens so we don't show stale data from a prior modal session.
let _retrySettings = {ora_count: 3, ora_sleep: 30, hdp_count: 3, hdp_sleep: 30};

async function openSettingsModal() {
    // Load current global settings each time so the modal reflects what's
    // actually persisted in dtrack.json.metadata.
    try {
        const resp = await fetch('/api/global_settings');
        const data = await resp.json();
        const v = data && data.approx_distinct;
        const radio = v === true ? 'approx' : 'exact';  // null/false → exact
        const el = document.querySelector(`input[name="global-approx"][value="${radio}"]`);
        if (el) el.checked = true;

        // Retry knobs -- null/missing => fall back to 3 attempts × 30s.
        _retrySettings = {
            ora_count: (data && data.ora_retry_count) ?? 3,
            ora_sleep: (data && data.ora_retry_sleep) ?? 30,
            hdp_count: (data && data.hdp_retry_count) ?? 3,
            hdp_sleep: (data && data.hdp_retry_sleep) ?? 30,
        };
        // Default dropdown to Hadoop (more common transient-failure source).
        const sel = document.getElementById('global-retry-source');
        if (sel) sel.value = 'hdp';
        onRetrySourceChanged();
    } catch (e) { /* leave defaults */ }
    document.getElementById('settings-modal').classList.add('active');
}

// Source dropdown changed -- save the currently-displayed values for
// the previous source into the cache, then load the new source's values
// into the inputs.
let _retryCurrentSource = 'hdp';
function onRetrySourceChanged() {
    // Save the inputs for the previously-selected source first.
    const countEl = document.getElementById('global-retry-count');
    const sleepEl = document.getElementById('global-retry-sleep');
    if (countEl && sleepEl && _retryCurrentSource) {
        _retrySettings[`${_retryCurrentSource}_count`] = Number(countEl.value) || 3;
        _retrySettings[`${_retryCurrentSource}_sleep`] = Number(sleepEl.value) || 30;
    }
    const newSource = document.getElementById('global-retry-source').value;
    if (countEl) countEl.value = _retrySettings[`${newSource}_count`] ?? 3;
    if (sleepEl) sleepEl.value = _retrySettings[`${newSource}_sleep`] ?? 30;
    _retryCurrentSource = newSource;
}

function closeSettingsModal() {
    document.getElementById('settings-modal').classList.remove('active');
}

// Testing Mode
async function initTestingMode() {
    try {
        const resp = await fetch('/api/testing');
        const data = await resp.json();
        const cb = document.getElementById('testing-mode');
        if (cb) cb.checked = data.testing;
        updateTestingStatus(data.testing, data.config_path);
    } catch (e) {
        // ignore — testing API may not exist in older versions
    }
}

async function toggleTestingMode(enabled) {
    try {
        const resp = await fetch('/api/testing', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({enabled}),
        });
        const data = await resp.json();
        if (resp.ok && data.ok) {
            updateTestingStatus(data.testing, data.config_path);
            await loadPairs();
            showSuccess(enabled ? 'Testing mode enabled — using mock data' : 'Testing mode disabled — using real config');
        } else {
            document.getElementById('testing-mode').checked = !enabled;
            const reason = data.error || data.detail || `HTTP ${resp.status}`;
            showError(`Testing mode failed: ${reason}`);
        }
    } catch (e) {
        document.getElementById('testing-mode').checked = !enabled;
        showError(`Testing mode failed: ${e.message}`);
    }
}

function updateTestingStatus(testing, configPath) {
    const statusDiv = document.getElementById('testing-status');
    if (!statusDiv) return;
    if (testing) {
        statusDiv.style.display = 'block';
        statusDiv.style.color = '#2e7d32';
        statusDiv.textContent = `Active — config: ${configPath}`;
    } else {
        statusDiv.style.display = 'none';
    }
}

async function applyGlobalSettings() {
    const mode = document.querySelector('input[name="global-mode"]:checked').value;
    const fromDate = document.getElementById('global-from-date').value;
    const toDate = document.getElementById('global-to-date').value;

    // Update in-memory state first so the UI shows the change immediately.
    pairs.forEach(pair => {
        pair.mode = mode;
        pair.dateRangeMode = 'global';
        pair.fromDate = fromDate;
        pair.toDate = toDate;
    });

    saveGlobalSettings();

    // Persist global stats-accuracy to dtrack.json.metadata.approx_distinct
    // independently of the per-pair fields below.
    const approxSel = document.querySelector('input[name="global-approx"]:checked');
    const approx = approxSel && approxSel.value === 'approx';

    // Capture the currently-displayed retry values into the cache so the
    // about-to-be-POSTed payload reflects the dropdown's current source.
    onRetrySourceChanged();

    try {
        await fetch('/api/global_settings', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                approx_distinct:  approx,
                ora_retry_count:  _retrySettings.ora_count,
                ora_retry_sleep:  _retrySettings.ora_sleep,
                hdp_retry_count:  _retrySettings.hdp_count,
                hdp_retry_sleep:  _retrySettings.hdp_sleep,
            }),
        });
    } catch (e) {
        showError(`Saving global settings failed: ${e.message}`);
    }

    // Persist the per-pair changes to dtrack.json. Without this, the JS-side
    // mutation above is invisible to /api/generate, which re-reads the file
    // from disk and would emit SQL using the previous per-pair date range.
    // Run all PUTs in parallel; if any fail we surface the first error.
    let saveErr = null;
    await Promise.all(pairs.map(async (pair) => {
        try {
            const resp = await fetch(`/api/pairs/${encodeURIComponent(pair.name)}`, {
                method: 'PUT',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    mode: pair.mode,
                    dateRangeMode: pair.dateRangeMode,
                    fromDate: pair.fromDate,
                    toDate: pair.toDate,
                }),
            });
            if (!resp.ok) {
                const data = await resp.json().catch(() => ({}));
                saveErr = saveErr || `${pair.name}: ${data.error || resp.status}`;
            }
        } catch (e) {
            saveErr = saveErr || `${pair.name}: ${e.message}`;
        }
    }));

    renderPairs();
    closeSettingsModal();
    if (saveErr) {
        showError(`Apply to all pairs partial fail: ${saveErr}`);
    } else {
        showSuccess(`Global settings applied to ${pairs.length} pair${pairs.length===1?'':'s'} (saved to dtrack.json)`);
    }
}

function updateEffectiveRange() {
    const fromDate = document.getElementById('pair-from-date').value;
    const toDate = document.getElementById('pair-to-date').value;
    const infoDiv = document.getElementById('effective-range-info');

    // Get global dates
    const globalFrom = document.getElementById('global-from-date')?.value || 'not set';
    const globalTo = document.getElementById('global-to-date')?.value || 'not set';

    if (fromDate && toDate) {
        // Custom dates set
        infoDiv.innerHTML = `ℹ️ Current: ${fromDate} to ${toDate} (custom)`;
    } else if (fromDate || toDate) {
        // Partial custom dates
        const effectiveFrom = fromDate || globalFrom;
        const effectiveTo = toDate || globalTo;
        infoDiv.innerHTML = `ℹ️ Current: ${effectiveFrom} to ${effectiveTo} (mixed)`;
    } else {
        // Using global
        if (globalFrom !== 'not set' && globalTo !== 'not set') {
            infoDiv.innerHTML = `ℹ️ Current: ${globalFrom} to ${globalTo} (using global default)`;
        } else {
            infoDiv.innerHTML = `ℹ️ Current: Using global default`;
        }
    }
}

function showHelp() {
    alert('Help documentation coming soon!');
}

// Initialize source dropdowns
function initSourceDropdowns() {
    const leftSource = document.getElementById('left-source');
    const rightSource = document.getElementById('right-source');

    DATA_SOURCES.forEach(src => {
        leftSource.add(new Option(src.label, src.value));
        rightSource.add(new Option(src.label, src.value));
    });

    // Set default selections
    leftSource.value = 'oracle';
    rightSource.value = 'aws';

    // Initialize connection options
    updateConnOptions('left');
    updateConnOptions('right');

    // Initialize date type dropdowns
    const leftDateType = document.getElementById('left-date-type');
    const rightDateType = document.getElementById('right-date-type');

    DATE_COLUMN_TYPES.forEach(type => {
        leftDateType.add(new Option(type.label, type.value));
        rightDateType.add(new Option(type.label, type.value));
    });

    // Merge custom date_types from config into dropdowns
    _loadCustomDateTypes(leftDateType, rightDateType);

    // Set default to 'date'
    leftDateType.value = 'date';
    rightDateType.value = 'date';
}

async function _loadCustomDateTypes(leftSelect, rightSelect) {
    try {
        const resp = await fetch('/api/config');
        if (!resp.ok) return;
        const config = await resp.json();
        const customTypes = config.date_types || {};

        // Add custom types after built-in types
        for (const [key, cfg] of Object.entries(customTypes)) {
            // Skip if already a built-in type
            if (DATE_COLUMN_TYPES.some(t => t.value === key)) continue;
            leftSelect.add(new Option(cfg.label || key, key));
            rightSelect.add(new Option(cfg.label || key, key));
        }

        // Add "+ Custom" option at the end
        leftSelect.add(new Option('+ Custom...', '__custom__'));
        rightSelect.add(new Option('+ Custom...', '__custom__'));

        // Wire up custom type creation on selection
        leftSelect.addEventListener('change', () => _onDateTypeChange(leftSelect, rightSelect));
        rightSelect.addEventListener('change', () => _onDateTypeChange(rightSelect, leftSelect));
    } catch (e) {
        // Non-critical — built-in types still work
        console.error('Failed to load custom date types:', e);
    }
}

async function _onDateTypeChange(changedSelect, otherSelect) {
    if (changedSelect.value !== '__custom__') return;

    const label = prompt('Custom date type label (e.g. "Monthly Number"):');
    if (!label) {
        changedSelect.value = 'date';
        return;
    }
    const key = prompt('Type key (e.g. "my_monthly_num", no spaces):');
    if (!key || !key.match(/^[a-z0-9_]+$/)) {
        alert('Key must be lowercase alphanumeric with underscores only.');
        changedSelect.value = 'date';
        return;
    }
    const category = prompt('Category (date, number, or string):');
    if (!['date', 'number', 'string'].includes(category)) {
        alert('Category must be one of: date, number, string');
        changedSelect.value = 'date';
        return;
    }
    const format = prompt('Format pattern (e.g. YYYYMM, YYYYMMDD, YYYY-MM-DD):');
    if (!format) {
        changedSelect.value = 'date';
        return;
    }

    const dateTransform = prompt('SQL date_transform expression (optional, use {col} placeholder):') || undefined;
    const parseToDate = prompt('SQL parse_to_date expression (optional, use {col} placeholder):') || undefined;

    const newType = {label, category, format};
    if (dateTransform) newType.date_transform = dateTransform;
    if (parseToDate) newType.parse_to_date = parseToDate;

    // Save to config via PUT /api/config
    try {
        const resp = await fetch('/api/config', {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({date_types: {[key]: newType}}),
        });
        if (!resp.ok) {
            const data = await resp.json();
            alert(`Failed to save custom type: ${data.error || 'unknown error'}`);
            changedSelect.value = 'date';
            return;
        }

        // Add the new option to both selects (before the "+ Custom..." option)
        const customOptL = changedSelect.querySelector('option[value="__custom__"]');
        const customOptR = otherSelect.querySelector('option[value="__custom__"]');
        const newOptL = new Option(label, key);
        const newOptR = new Option(label, key);
        changedSelect.insertBefore(newOptL, customOptL);
        otherSelect.insertBefore(newOptR, customOptR);

        changedSelect.value = key;
        showSuccess(`Custom date type "${label}" saved`);
    } catch (e) {
        alert(`Failed to save custom type: ${e.message}`);
        changedSelect.value = 'date';
    }
}

function updateConnOptions(side) {
    const sourceSelect = document.getElementById(`${side}-source`);
    const connSelect = document.getElementById(`${side}-conn`);
    const source = sourceSelect.value;

    // Clear existing options
    connSelect.innerHTML = '';

    // Add options for this source
    const options = CONNECTION_MACROS[source] || [];
    if (options.length === 0) {
        connSelect.add(new Option('—', ''));
        connSelect.disabled = true;
    } else {
        options.forEach(opt => {
            connSelect.add(new Option(opt, opt));
        });
        connSelect.disabled = false;
    }
}

// Global Settings — load from config, save back on apply
async function loadGlobalSettings() {
    try {
        const resp = await fetch('/api/config');
        if (!resp.ok) return;
        const config = await resp.json();
        const s = config.settings || {};

        // Mode
        const modeVal = s.mode || 'incremental';
        const modeRadio = document.querySelector(`input[name="global-mode"][value="${modeVal}"]`);
        if (modeRadio) modeRadio.checked = true;

        // Date range
        if (s.from_date) document.getElementById('global-from-date').value = s.from_date;
        if (s.to_date) document.getElementById('global-to-date').value = s.to_date;

        // Output dirs
        if (s.sas_outdir) document.getElementById('global-sas-outdir').value = s.sas_outdir;
        if (s.aws_outdir) document.getElementById('global-aws-outdir').value = s.aws_outdir;

        // Parallelism
        if (s.njob_left != null) document.getElementById('global-njob-left').value = s.njob_left;
        if (s.njob_right != null) document.getElementById('global-njob-right').value = s.njob_right;
    } catch (e) {
        // Non-critical — use hardcoded defaults
    }
}

async function saveGlobalSettings() {
    const settings = {
        mode: document.querySelector('input[name="global-mode"]:checked').value,
        from_date: document.getElementById('global-from-date').value || '',
        to_date: document.getElementById('global-to-date').value || '',
        sas_outdir: document.getElementById('global-sas-outdir').value || './sas/',
        aws_outdir: document.getElementById('global-aws-outdir').value || './csv/',
        njob_left: parseInt(document.getElementById('global-njob-left').value) || 4,
        njob_right: parseInt(document.getElementById('global-njob-right').value) || 4,
    };
    try {
        await fetch('/api/config', {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({settings}),
        });
    } catch (e) {
        console.error('Failed to save global settings:', e);
    }
}

function clearGlobalDates() {
    document.getElementById('global-from-date').value = '';
    document.getElementById('global-to-date').value = '';
}

function clearPairDates() {
    document.getElementById('pair-from-date').value = '';
    document.getElementById('pair-to-date').value = '';
    updateEffectiveRange();
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

function loadJsonConfig() {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.json';
    input.onchange = async () => {
        const file = input.files[0];
        if (!file) return;
        if (!confirm(`Load "${file.name}"? This will wipe current pairs and replace with the new config.`)) return;

        try {
            const formData = new FormData();
            formData.append('file', file);
            const resp = await fetch('/api/config/upload', {method: 'POST', body: formData});
            const data = await resp.json();

            if (resp.ok && data.ok) {
                await loadPairs();
                showSuccess(`Loaded ${data.pairs} pairs from ${file.name} (${data.registered} registered in DB)`);
            } else {
                showError(`Load JSON failed: ${data.error || data.detail || 'HTTP ' + resp.status}`);
            }
        } catch (err) {
            showError(`Load JSON failed: ${err.message}`);
        }
    };
    input.click();
}

// Pair Management
async function loadPairs() {
    try {
        const response = await fetch('/api/pairs/list');
        const data = await response.json();

        if (response.ok && data.pairs) {
            pairs = data.pairs;
            renderPairs();
        } else {
            showError(`Load pairs failed: ${data.error || data.detail || 'HTTP ' + response.status}`);
        }
    } catch (error) {
        console.error('Failed to load pairs:', error);
        showError(`Load pairs failed: ${error.message}`);
    }
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

    container.innerHTML = pairs.map((pair, index) => `
        <div class="pair-item ${pair.selected ? 'selected' : ''}" id="pair-${index}">
            <div class="pair-header" onclick="togglePair(${index})">
                <input type="checkbox" class="pair-checkbox"
                    ${pair.selected ? 'checked' : ''}
                    onclick="event.stopPropagation(); togglePairSelection(${index})"
                    onchange="togglePairSelection(${index})">
                <span class="pair-expand">${pair.expanded ? '▶' : '▼'}</span>
                <span class="pair-name">${pair.name}</span>
                <div class="pair-actions" onclick="event.stopPropagation()">
                    <button onclick="editPair(${index})">Edit</button>
                    <button class="btn-danger" onclick="deletePair(${index})">Delete</button>
                    <button onclick="copyPair(${index})">Copy</button>
                    <button onclick="exportPair(${index})">Export</button>
                </div>
            </div>
            <div class="pair-body ${pair.expanded ? 'expanded' : ''}">
                ${pair.description ? `<div style="margin-bottom:12px; color: var(--jp-ui-font-color2); font-size:13px;">${pair.description}</div>` : ''}
                <div class="pair-info">
                    <div class="pair-side-info">
                        <div class="pair-side-title">Left</div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Source:</span>
                            <span class="pair-info-value">${pair.left.source}</span>
                        </div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Table:</span>
                            <span class="pair-info-value">${pair.left.table}</span>
                        </div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Conn:</span>
                            <span class="pair-info-value">${pair.left.conn_macro}</span>
                        </div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Date Col:</span>
                            <span class="pair-info-value">${pair.left.date_col}</span>
                        </div>
                        ${pair.left.where ? `<div class="pair-info-row">
                            <span class="pair-info-label">WHERE:</span>
                            <span class="pair-info-value">${pair.left.where}</span>
                        </div>` : ''}
                    </div>
                    <div class="pair-side-info">
                        <div class="pair-side-title">Right</div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Source:</span>
                            <span class="pair-info-value">${pair.right.source}</span>
                        </div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Table:</span>
                            <span class="pair-info-value">${pair.right.table}</span>
                        </div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Conn:</span>
                            <span class="pair-info-value">${pair.right.conn_macro}</span>
                        </div>
                        <div class="pair-info-row">
                            <span class="pair-info-label">Date Col:</span>
                            <span class="pair-info-value">${pair.right.date_col}</span>
                        </div>
                        ${pair.right.where ? `<div class="pair-info-row">
                            <span class="pair-info-label">WHERE:</span>
                            <span class="pair-info-value">${pair.right.where}</span>
                        </div>` : ''}
                    </div>
                </div>
            </div>
        </div>
    `).join('');
}


function togglePair(index) {
    pairs[index].expanded = !pairs[index].expanded;
    renderPairs();
}

// Persist a {name: skip} mapping to the server. Pairs page is the global
// switchboard -- once toggled here, /load_row, /row_compare, /load_col,
// /col_compare, and /col_mapping all hide skipped pairs automatically.
async function _persistPairSkip(updates) {
    try {
        const response = await fetch('/api/pairs/skip', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({pairs: updates}),
        });
        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            console.error('Failed to persist pair skip:', err);
        }
    } catch (e) {
        console.error('Failed to persist pair skip:', e);
    }
}

function togglePairSelection(index) {
    pairs[index].selected = !pairs[index].selected;
    pairs[index].skip = !pairs[index].selected;
    syncSelectAllCheckbox();
    renderPairs();
    _persistPairSkip({[pairs[index].name]: pairs[index].skip});
}

function selectAllPairs(selected) {
    const updates = {};
    pairs.forEach(p => {
        p.selected = selected;
        p.skip = !selected;
        updates[p.name] = p.skip;
    });
    syncSelectAllCheckbox();
    renderPairs();
    _persistPairSkip(updates);
}

function syncSelectAllCheckbox() {
    const cb = document.getElementById('select-all-pairs');
    if (!cb) return;
    const allSelected = pairs.length > 0 && pairs.every(p => p.selected);
    const someSelected = pairs.some(p => p.selected);
    cb.checked = allSelected;
    cb.indeterminate = someSelected && !allSelected;
}

// Modal Handlers
function initModalHandlers() {
    // Close modal on background click
    document.getElementById('pair-modal').addEventListener('click', (e) => {
        if (e.target.id === 'pair-modal') {
            closePairModal();
        }
    });

    document.getElementById('preview-modal').addEventListener('click', (e) => {
        if (e.target.id === 'preview-modal') {
            closePreviewModal();
        }
    });

    document.getElementById('settings-modal').addEventListener('click', (e) => {
        if (e.target.id === 'settings-modal') {
            closeSettingsModal();
        }
    });
}

function showAddPairModal() {
    editingPairIndex = -1;
    document.getElementById('modal-title').textContent = 'Add New Pair';
    clearPairForm();
    document.getElementById('query-preview-panel').style.display = 'none';
    document.getElementById('pair-modal').classList.add('active');
    updateEffectiveRange();
}

function editPair(index) {
    editingPairIndex = index;
    const pair = pairs[index];

    document.getElementById('modal-title').textContent = `Edit Pair: ${pair.name}`;

    // Fill form with pair data
    document.getElementById('pair-name').value = pair.name;
    document.getElementById('pair-desc').value = pair.description || '';

    // Left side
    document.getElementById('left-source').value = pair.left.source;
    updateConnOptions('left'); // Update conn options before setting value
    document.getElementById('left-conn').value = pair.left.conn_macro;
    document.getElementById('left-table').value = pair.left.table;
    document.getElementById('left-date-col').value = pair.left.date_col;
    document.getElementById('left-date-type').value = pair.left.date_type || 'date';
    document.getElementById('left-where').value = pair.left.where || '';
    document.getElementById('left-cte').value = pair.left.processed || '';

    // Right side
    document.getElementById('right-source').value = pair.right.source;
    updateConnOptions('right'); // Update conn options before setting value
    document.getElementById('right-conn').value = pair.right.conn_macro;
    document.getElementById('right-table').value = pair.right.table;
    document.getElementById('right-date-col').value = pair.right.date_col;
    document.getElementById('right-date-type').value = pair.right.date_type || 'date';
    document.getElementById('right-where').value = pair.right.where || '';
    document.getElementById('right-cte').value = pair.right.processed || '';

    // Mode and date range
    document.getElementById('pair-incremental').checked = (pair.mode !== 'full');
    document.getElementById('pair-from-date').value = pair.fromDate || '';
    document.getElementById('pair-to-date').value = pair.toDate || '';

    // Stats accuracy: missing field → inherit; true → approx; false → exact.
    const approxRaw = pair.approx_distinct;
    const approxRadio = approxRaw === true ? 'approx'
                      : approxRaw === false ? 'exact'
                      : 'inherit';
    const approxEl = document.querySelector(`input[name="pair-approx"][value="${approxRadio}"]`);
    if (approxEl) approxEl.checked = true;

    document.getElementById('query-preview-panel').style.display = 'none';
    document.getElementById('pair-modal').classList.add('active');

    // Update effective range display
    updateEffectiveRange();
}

function closePairModal() {
    document.getElementById('pair-modal').classList.remove('active');
}

function clearPairForm() {
    document.getElementById('pair-name').value = '';
    document.getElementById('pair-desc').value = '';

    ['left', 'right'].forEach(side => {
        document.getElementById(`${side}-source`).value = side === 'left' ? 'oracle' : 'aws';
        document.getElementById(`${side}-conn`).value = '';
        document.getElementById(`${side}-table`).value = '';
        document.getElementById(`${side}-date-col`).value = '';
        document.getElementById(`${side}-where`).value = '';
        document.getElementById(`${side}-cte`).value = '';
    });

    document.getElementById('pair-incremental').checked = true;
    document.getElementById('pair-from-date').value = '';
    document.getElementById('pair-to-date').value = '';
}

async function savePair() {
    const pairData = {
        pair_name: document.getElementById('pair-name').value.trim(),
        description: document.getElementById('pair-desc').value.trim(),
        left: {
            source: document.getElementById('left-source').value,
            conn_macro: document.getElementById('left-conn').value.trim(),
            table: document.getElementById('left-table').value.trim(),
            date_col: document.getElementById('left-date-col').value.trim(),
            date_type: document.getElementById('left-date-type').value,
            where: document.getElementById('left-where').value.trim(),
            processed: document.getElementById('left-cte').value.trim()
        },
        right: {
            source: document.getElementById('right-source').value,
            conn_macro: document.getElementById('right-conn').value.trim(),
            table: document.getElementById('right-table').value.trim(),
            date_col: document.getElementById('right-date-col').value.trim(),
            date_type: document.getElementById('right-date-type').value,
            where: document.getElementById('right-where').value.trim(),
            processed: document.getElementById('right-cte').value.trim()
        },
        mode: document.getElementById('pair-incremental').checked ? 'incremental' : 'full',
        fromDate: document.getElementById('pair-from-date').value,
        toDate: document.getElementById('pair-to-date').value,
    };

    // Stats accuracy: 'inherit' → null on the wire (clears the per-pair key
    // so the global setting takes over); 'exact' → false; 'approx' → true.
    const approxSel = document.querySelector('input[name="pair-approx"]:checked');
    const approxVal = approxSel ? approxSel.value : 'inherit';
    pairData.approx_distinct = approxVal === 'inherit' ? null
                             : approxVal === 'approx';

    // Validation
    if (!pairData.pair_name) {
        alert('Pair name is required');
        return;
    }
    if (!pairData.left.table || !pairData.right.table) {
        alert('Table names are required for both sides');
        return;
    }
    if (!pairData.left.date_col || !pairData.right.date_col) {
        alert('Date columns are required for both sides');
        return;
    }

    try {
        const url = editingPairIndex >= 0 ? `/api/pairs/${pairData.pair_name}` : '/api/pairs';
        const method = editingPairIndex >= 0 ? 'PUT' : 'POST';

        const response = await fetch(url, {
            method,
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(pairData)
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Failed to save pair');
        }

        closePairModal();
        await loadPairs();
        showSuccess(`Pair "${pairData.pair_name}" saved successfully`);
    } catch (error) {
        console.error('Save pair error:', error);
        showError(error.message);
    }
}

async function deletePair(index) {
    const pair = pairs[index];
    if (!confirm(`Delete pair "${pair.name}"?`)) return;

    try {
        const response = await fetch(`/api/pairs/${pair.name}`, {method: 'DELETE'});
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Failed to delete pair');
        }

        // Reload pairs immediately
        await loadPairs();
        showSuccess(`Pair "${pair.name}" deleted`);
    } catch (error) {
        console.error('Delete error:', error);
        showError(error.message);
    }
}

async function copyPair(index) {
    const src = pairs[index];
    // Pick a name that's unique on the server side. The naive
    // `${name}_copy` collides if the user copies the same pair twice; add
    // a numeric suffix until we find an unused slot.
    let newName = `${src.name}_copy`;
    const existing = new Set(pairs.map(p => p.name));
    if (existing.has(newName)) {
        let i = 2;
        while (existing.has(`${src.name}_copy${i}`)) i++;
        newName = `${src.name}_copy${i}`;
    }

    // POST the clone to /api/pairs immediately so it's persisted on the
    // server. Without this, savePair() would later issue PUT on a name
    // the server has never seen and return "pair not found". The body
    // mirrors what savePair sends: pair_name + left/right + the common
    // pair-level fields.
    const body = {
        pair_name:    newName,
        description:  src.description || '',
        left:         JSON.parse(JSON.stringify(src.left  || {})),
        right:        JSON.parse(JSON.stringify(src.right || {})),
        mode:         src.mode || 'incremental',
        fromDate:     src.fromDate || '',
        toDate:       src.toDate  || '',
        vintage:      src.vintage || '',
        col_map:      src.col_map || {},
        col_filter:   src.col_filter || null,
        approx_distinct: (typeof src.approx_distinct === 'boolean') ? src.approx_distinct : null,
        excludeDates: src.excludeDates || [],
        dateRangeMode: src.dateRangeMode || 'global',
        overlap:      src.overlap ?? 7,
    };

    try {
        const resp = await fetch('/api/pairs', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body),
        });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            throw new Error(err.error || `HTTP ${resp.status}`);
        }
        // Reload from the server so the new pair has the canonical state
        // (including any defaults the POST endpoint filled in).
        await loadPairs();
        showSuccess(`Copied "${src.name}" → "${newName}"`);
    } catch (e) {
        showError(`Copy failed: ${e.message}`);
    }
}

async function exportPair(index) {
    const pair = pairs[index];

    // Create clean export object with field names as they appear in JSON
    const exportData = {
        name: pair.name,
        description: pair.description || "",
        left: {
            source: pair.left.source,
            conn_macro: pair.left.conn_macro,
            table: pair.left.table,
            date_col: pair.left.date_col,
            date_type: pair.left.date_type || "date",
            where: pair.left.where || "",
            processed: pair.left.processed || ""
        },
        right: {
            source: pair.right.source,
            conn_macro: pair.right.conn_macro,
            table: pair.right.table,
            date_col: pair.right.date_col,
            date_type: pair.right.date_type || "date",
            where: pair.right.where || "",
            processed: pair.right.processed || ""
        },
        mode: pair.mode || "incremental",
        fromDate: pair.fromDate || "",
        toDate: pair.toDate || ""
    };

    const jsonString = JSON.stringify(exportData, null, 2);

    try {
        await navigator.clipboard.writeText(jsonString);
        showSuccess(`Pair "${pair.name}" exported to clipboard as JSON`);
    } catch (err) {
        // Fallback: show in alert if clipboard API fails
        alert('Copy this JSON:\n\n' + jsonString);
    }
}

async function toggleQueryPreview() {
    const panel = document.getElementById('query-preview-panel');
    const visible = panel.style.display !== 'none';

    if (visible) {
        panel.style.display = 'none';
        return;
    }

    // Resolve dates
    const globalFrom = document.getElementById('global-from-date')?.value || '';
    const globalTo = document.getElementById('global-to-date')?.value || '';
    const fromDate = document.getElementById('pair-from-date').value || globalFrom || '';
    const toDate = document.getElementById('pair-to-date').value || globalTo || '';

    const body = {
        pair_name: document.getElementById('pair-name').value || 'pair',
        left: {
            source: document.getElementById('left-source').value,
            conn_macro: document.getElementById('left-conn').value,
            table: document.getElementById('left-table').value,
            name: document.getElementById('left-table').value.toLowerCase(),
            date_col: document.getElementById('left-date-col').value,
            date_type: document.getElementById('left-date-type').value,
            where: document.getElementById('left-where').value,
            processed: document.getElementById('left-cte').value,
        },
        right: {
            source: document.getElementById('right-source').value,
            conn_macro: document.getElementById('right-conn').value,
            table: document.getElementById('right-table').value,
            name: document.getElementById('right-table').value.toLowerCase(),
            date_col: document.getElementById('right-date-col').value,
            date_type: document.getElementById('right-date-type').value,
            where: document.getElementById('right-where').value,
            processed: document.getElementById('right-cte').value,
        },
        fromDate,
        toDate,
        njob_left: parseInt(document.getElementById('global-njob-left')?.value) || 0,
        njob_right: parseInt(document.getElementById('global-njob-right')?.value) || 0,
    };

    try {
        const resp = await fetch('/api/preview', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body),
        });
        const data = await resp.json();
        if (!resp.ok) {
            document.getElementById('query-preview-left').textContent =
                `Error: ${data.error || data.detail || 'Preview failed'}`;
            document.getElementById('query-preview-right').textContent = '';
        } else {
            document.getElementById('query-preview-left').textContent =
                data.left ? `${data.left.sql}\n\nOutput: ${data.left.output_file}` : '(no left config)';
            document.getElementById('query-preview-right').textContent =
                data.right ? `${data.right.sql}\n\nOutput: ${data.right.output_file}` : '(no right config)';
        }
    } catch (e) {
        document.getElementById('query-preview-left').textContent = `Error: ${e.message}`;
        document.getElementById('query-preview-right').textContent = '';
    }

    panel.style.display = 'block';
}

// Generation

async function previewQueries() {
    const selected = pairs.filter(p => p.selected);
    if (selected.length === 0) {
        alert('No pairs selected');
        return;
    }

    const modal = document.getElementById('preview-modal');
    const content = document.getElementById('preview-content');

    const globalFrom = document.getElementById('global-from-date')?.value || '';
    const globalTo = document.getElementById('global-to-date')?.value || '';

    content.innerHTML = '<div style="padding:20px; color:var(--jp-ui-font-color2);">Loading previews...</div>';
    modal.classList.add('active');

    // Fetch previews from backend in parallel
    const results = await Promise.all(selected.map(async pair => {
        const fromDate = pair.fromDate || globalFrom || '';
        const toDate = pair.toDate || globalTo || '';
        try {
            const resp = await fetch('/api/preview', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    pair_name: pair.name,
                    left: pair.left,
                    right: pair.right,
                    fromDate, toDate,
                    njob_left: parseInt(document.getElementById('global-njob-left')?.value) || 0,
                    njob_right: parseInt(document.getElementById('global-njob-right')?.value) || 0,
                }),
            });
            const data = await resp.json();
            return {pair, data, error: resp.ok ? null : (data.error || 'Preview failed')};
        } catch (e) {
            return {pair, data: null, error: e.message};
        }
    }));

    let html = '';
    for (const {pair, data, error} of results) {
        const leftSql = error ? `Error: ${error}` : (data?.left?.sql || '(no left config)');
        const rightSql = error ? '' : (data?.right?.sql || '(no right config)');
        const leftFile = data?.left?.output_file || '';
        const rightFile = data?.right?.output_file || '';

        html += `
            <div style="margin-bottom: 24px;">
                <h3 style="font-size: 14px; font-weight: 600; color: var(--jp-brand-color1); margin-bottom: 12px;">
                    ${pair.name}
                </h3>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px;">
                    <div>
                        <div style="font-weight: 600; font-size: 12px; color: var(--jp-ui-font-color1); margin-bottom: 8px;">
                            LEFT (${pair.left.source})
                        </div>
                        <pre style="background: var(--jp-layout-color1); padding: 12px; border-radius: 4px; border: 1px solid var(--jp-border-color0); font-size: 11px; overflow-x: auto; margin: 0;">${_escapeHtml(leftSql)}</pre>
                        ${leftFile ? `<div style="margin-top: 8px; font-size: 11px; color: var(--jp-ui-font-color2);">Output: ${leftFile}</div>` : ''}
                    </div>
                    <div>
                        <div style="font-weight: 600; font-size: 12px; color: var(--jp-ui-font-color1); margin-bottom: 8px;">
                            RIGHT (${pair.right.source})
                        </div>
                        <pre style="background: var(--jp-layout-color1); padding: 12px; border-radius: 4px; border: 1px solid var(--jp-border-color0); font-size: 11px; overflow-x: auto; margin: 0;">${_escapeHtml(rightSql)}</pre>
                        ${rightFile ? `<div style="margin-top: 8px; font-size: 11px; color: var(--jp-ui-font-color2);">Output: ${rightFile}</div>` : ''}
                    </div>
                </div>
            </div>
        `;
    }
    content.innerHTML = html;
}

function _escapeHtml(str) {
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function closePreviewModal() {
    document.getElementById('preview-modal').classList.remove('active');
}

function generateFromPreview() {
    closePreviewModal();
    generateAll();
}

async function generateAll() {
    const selected = pairs.filter(p => p.selected);
    if (selected.length === 0) {
        alert('No pairs selected');
        return;
    }

    const log = document.getElementById('generation-log');
    log.innerHTML = '';

    const globalFrom = document.getElementById('global-from-date')?.value || '';
    const globalTo = document.getElementById('global-to-date')?.value || '';

    // --- Step 1: Fetch all previews ---
    logMessage(`Generating queries for ${selected.length} pair(s)...`, 'info');

    const results = await Promise.all(selected.map(async pair => {
        const fromDate = pair.fromDate || globalFrom || '';
        const toDate = pair.toDate || globalTo || '';
        try {
            const resp = await fetch('/api/preview', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    pair_name: pair.name,
                    left: pair.left,
                    right: pair.right,
                    fromDate, toDate,
                    njob_left: parseInt(document.getElementById('global-njob-left')?.value) || 0,
                    njob_right: parseInt(document.getElementById('global-njob-right')?.value) || 0,
                }),
            });
            const data = await resp.json();
            return {pair, data, error: resp.ok ? null : (data.error || 'Preview failed')};
        } catch (e) {
            return {pair, data: null, error: e.message};
        }
    }));

    // --- Step 2: Detect platforms ---
    const awsSources = new Set(['aws', 'csv']);
    const oracleSources = new Set(['oracle']);
    const hadoopSources = new Set(['hadoop']);
    const hasAws = selected.some(p => awsSources.has(p.left?.source) || awsSources.has(p.right?.source));
    const hasOracle = selected.some(p => oracleSources.has(p.left?.source) || oracleSources.has(p.right?.source));
    const hasHadoop = selected.some(p => hadoopSources.has(p.left?.source) || hadoopSources.has(p.right?.source));
    // "SAS" header now covers SAS-local only (Oracle and Hadoop both moved to gen-first plain SQL extraction).
    const hasSas = selected.some(p => {
        const ls = p.left?.source, rs = p.right?.source;
        return (ls && !awsSources.has(ls) && !oracleSources.has(ls) && !hadoopSources.has(ls)) ||
               (rs && !awsSources.has(rs) && !oracleSources.has(rs) && !hadoopSources.has(rs));
    });

    // --- Step 3: Render two-column review (SAS/Hadoop | AWS/Athena/Oracle) ---
    // Oracle shares the AWS column visually because both are Python-direct
    // gen-then-run flows; the button label switches based on which platform's
    // file was actually generated.
    const reviewDiv = document.createElement('div');
    reviewDiv.id = 'generate-review';
    reviewDiv.innerHTML = `
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:16px;">
            <div style="font-weight:600; font-size:13px; color:var(--jp-ui-font-color1); border-bottom:1px solid var(--jp-border-color0); padding-bottom:6px;">
                SAS / Hadoop
            </div>
            <div style="display:flex; justify-content:space-between; align-items:center; font-weight:600; font-size:13px; color:var(--jp-ui-font-color1); border-bottom:1px solid var(--jp-border-color0); padding-bottom:6px;">
                <span>AWS / Oracle</span>
                <span style="display:flex; gap:6px;">
                    ${hasAws ? `<button class="btn-primary" id="run-aws-btn" style="font-size:11px; padding:3px 10px;">Run AWS Extraction</button>` : ''}
                    ${hasOracle ? `<button class="btn-primary" id="run-oracle-btn" style="font-size:11px; padding:3px 10px;">Run Oracle Extraction</button>` : ''}
                    ${hasHadoop ? `<button class="btn-primary" id="run-hadoop-btn" style="font-size:11px; padding:3px 10px;">Run Hadoop Extraction</button>` : ''}
                </span>
            </div>
        </div>
    `;

    for (const {pair, data, error} of results) {
        const leftSql = error ? `Error: ${error}` : (data?.left?.sql || '(no SAS/Oracle config)');
        const rightSql = error ? '' : (data?.right?.sql || '(no AWS config)');
        const leftFile = data?.left?.output_file || '';
        const rightFile = data?.right?.output_file || '';

        const pairDiv = document.createElement('div');
        pairDiv.style.cssText = 'margin-bottom:20px;';
        pairDiv.innerHTML = `
            <div style="font-size:13px; font-weight:600; color:var(--jp-brand-color1); margin-bottom:8px;">
                ${_escapeHtml(pair.name)}
            </div>
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:16px;">
                <div>
                    <pre style="background:var(--jp-layout-color1); padding:10px; border-radius:4px; border:1px solid var(--jp-border-color0); font-size:11px; overflow-x:auto; margin:0; white-space:pre-wrap; max-height:240px; overflow-y:auto;">${_escapeHtml(leftSql)}</pre>
                    ${leftFile ? `<div style="margin-top:4px; font-size:10px; color:var(--jp-ui-font-color2);">Output: ${_escapeHtml(leftFile)}</div>` : ''}
                </div>
                <div>
                    <pre style="background:var(--jp-layout-color1); padding:10px; border-radius:4px; border:1px solid var(--jp-border-color0); font-size:11px; overflow-x:auto; margin:0; white-space:pre-wrap; max-height:240px; overflow-y:auto;">${_escapeHtml(rightSql)}</pre>
                    ${rightFile ? `<div style="margin-top:4px; font-size:10px; color:var(--jp-ui-font-color2);">Output: ${_escapeHtml(rightFile)}</div>` : ''}
                </div>
            </div>
        `;
        reviewDiv.appendChild(pairDiv);
    }
    log.appendChild(reviewDiv);

    // --- Step 4: Generate SAS + SQL files (no extraction yet) ---
    logMessage('Generating files...', 'info');
    try {
        const genBody = {
            type: 'row',
            sas_outdir: document.getElementById('global-sas-outdir')?.value || './sas/',
            aws_outdir: document.getElementById('global-aws-outdir')?.value || './csv/',
            pair_names: selected.map(p => p.name),
        };
        if (globalFrom) genBody.from_date = globalFrom;
        if (globalTo) genBody.to_date = globalTo;

        const resp = await fetch('/api/generate', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(genBody),
        });
        const result = await resp.json();
        if (result.ok) {
            if (result.output) {
                result.output.trim().split('\n').forEach(line => {
                    if (line.trim()) logMessage(line, 'info');
                });
            }
            if (result.sas_file) logMessage(`SAS file: ${result.sas_file}`, 'success');
            if (result.sql_file) logMessage(`SQL file (AWS): ${result.sql_file}`, 'success');
            if (result.oracle_sql_file) logMessage(`SQL file (Oracle): ${result.oracle_sql_file}`, 'success');
            if (result.hadoop_sql_file) logMessage(`SQL file (Hadoop): ${result.hadoop_sql_file}`, 'success');
        } else {
            logMessage(`File generation error: ${result.error}`, 'error');
        }
    } catch (err) {
        logMessage(`File generation failed: ${err.message}`, 'error');
    }

    // --- Step 5: Wire up Run buttons.
    // While a run is in flight the button toggles to "Cancel X" with a red
    // background and stays clickable; clicking again aborts the fetch (the
    // server-side ThreadPool keeps draining whatever's already submitted,
    // but no more progress is reported). On completion the button resets
    // to its original "Run X Extraction" label so it can be triggered again.
    async function _runExtraction(platform, btnId) {
        const btn = document.getElementById(btnId);
        if (!btn) return;
        const _LABELS = {oracle: 'Oracle', hadoop: 'Hadoop', aws: 'AWS'};
        const label = _LABELS[platform] || 'AWS';
        // Oracle and Hadoop both write to the LEFT outdir (sas/) since both are
        // gen-first plain SQL flows; AWS keeps the RIGHT outdir.
        const writesLeft = (platform === 'oracle' || platform === 'hadoop');
        const outdirInput = writesLeft ? 'global-sas-outdir' : 'global-aws-outdir';
        const defaultDir  = writesLeft ? './sas/' : './csv/';
        const fname = 'extract_row.sql';
        const originalText = btn.textContent;
        const originalBg = btn.style.background;
        const originalColor = btn.style.color;
        let controller = null;
        let activeRunId = null;
        btn.onclick = async () => {
            if (controller) {
                // Second click = cancel: tell the server to stop dispatching
                // queued blocks, then abort the SSE stream client-side.
                if (activeRunId) {
                    try {
                        await fetch(`/api/extract/cancel/${activeRunId}`, {method: 'POST'});
                    } catch (e) { /* server may already be done */ }
                }
                controller.abort();
                return;
            }
            controller = new AbortController();
            btn.textContent = `Cancel ${label}`;
            btn.style.background = '#dc3545';
            btn.style.color = '#fff';
            const outdir = document.getElementById(outdirInput)?.value || defaultDir;
            // Oracle (left) reads its own worker count; AWS (right) reads
            // global-njob-right. Mirrors the Left/Right outdir convention.
            const njobInput = writesLeft ? 'global-njob-left' : 'global-njob-right';
            logMessage(`Running ${label} extraction from ${outdir}${fname}...`, 'info');
            try {
                const reqBody = {
                    type: 'row',
                    platform,
                    outdir,
                };
                const nj = parseInt(document.getElementById(njobInput)?.value);
                if (nj > 0) reqBody.max_workers = nj;

                const resp = await fetch('/api/extract/run-sql', {
                    method: 'POST',
                    signal: controller.signal,
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

                        if (eventType === 'run_id') {
                            activeRunId = msg.run_id;
                        } else if (eventType === 'start') {
                            const eta = _fmtEta(msg.eta_sec);
                            const etaPart = eta ? ` | eta ${eta}` : '';
                            logMessage(`[start | running=${msg.running}${etaPart}] ${msg.name}`, 'info');
                        } else if (eventType === 'progress') {
                            const status = msg.cancelled
                                ? 'cancelled (not submitted)'
                                : (msg.ok ? `${msg.rows} dates, ${msg.elapsed}s` : `FAIL: ${msg.error}`);
                            const eta = _fmtEta(msg.eta_sec);
                            const etaPart = eta ? ` | eta ${eta}` : '';
                            const runningPart = msg.running != null ? ` | running=${msg.running}` : '';
                            logMessage(`[${msg.done}/${msg.total}${runningPart}${etaPart}] ${msg.name}: ${status}`, msg.ok ? 'info' : 'error');
                            if (msg.csv_path) {
                                logMessage(`  -> CSV written: ${msg.csv_path}`, 'success');
                            }
                        } else if (eventType === 'done') {
                            if (msg.ok) {
                                logMessage(`${label} extraction complete: ${msg.succeeded}/${msg.total} succeeded`, 'success');
                            } else if (msg.results) {
                                const failed = msg.results.filter(r => !r.ok);
                                failed.forEach(r => logMessage(`  FAILED: ${r.name} — ${r.error}`, 'error'));
                                logMessage(`${msg.succeeded}/${msg.total} succeeded, ${msg.failed} failed`, 'error');
                            } else {
                                logMessage(`${label} error: ${msg.error}`, 'error');
                            }
                        }
                    }
                }
            } catch (err) {
                if (err.name === 'AbortError') {
                    logMessage(`${label} extraction cancelled (queued blocks short-circuited; in-flight queries finish their query but no new ones are sent)`, 'info');
                } else {
                    logMessage(`${label} request failed: ${err.message}`, 'error');
                }
            } finally {
                controller = null;
                activeRunId = null;
                btn.textContent = originalText;
                btn.style.background = originalBg;
                btn.style.color = originalColor;
            }
        };
    }
    if (hasAws)    _runExtraction('aws',    'run-aws-btn');
    if (hasOracle) _runExtraction('oracle', 'run-oracle-btn');
    if (hasHadoop) _runExtraction('hadoop', 'run-hadoop-btn');
}

function _configBasename() {
    // Derive a display name for the config path (the server knows the real path)
    return 'pairs.json';
}

function clearLog() {
    const log = document.getElementById('generation-log');
    log.innerHTML = '<div class="log-empty">No files generated yet.</div>';
}

// Format an ETA in seconds as a short human-readable string.
// null/undefined/0 -> '' so the caller can decide whether to append it.
function _fmtEta(sec) {
    if (sec == null || !isFinite(sec) || sec < 1) return '';
    sec = Math.round(sec);
    if (sec < 60)    return `${sec}s`;
    if (sec < 3600)  return `${Math.floor(sec / 60)}m ${sec % 60}s`;
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    return `${h}h ${m}m`;
}


function logMessage(message, type = 'info') {
    const log = document.getElementById('generation-log');
    const empty = log.querySelector('.log-empty');
    if (empty) empty.remove();

    const time = new Date().toLocaleTimeString();
    const className = type === 'success' ? 'log-success' : type === 'error' ? 'log-error' : '';

    const entry = document.createElement('div');
    entry.className = 'log-entry';
    entry.innerHTML = `
        <span class="log-time">${time}</span>
        <span class="log-message ${className}">${message}</span>
    `;
    log.appendChild(entry);
    log.scrollTop = log.scrollHeight;
}

function logFile(filename) {
    const log = document.getElementById('generation-log');
    const empty = log.querySelector('.log-empty');
    if (empty) empty.remove();

    const entry = document.createElement('div');
    entry.className = 'log-file';
    entry.innerHTML = `
        <span class="log-file-name">${filename}</span>
        <div class="log-file-actions">
            <button onclick="viewFile('${filename}')">View</button>
            <button onclick="editFile('${filename}')">Edit</button>
            <button onclick="runFile('${filename}')">Run</button>
        </div>
    `;
    log.appendChild(entry);
    log.scrollTop = log.scrollHeight;
}

function viewFile(filename) {
    alert(`View ${filename} - coming soon!`);
}

function editFile(filename) {
    alert(`Edit ${filename} - coming soon!`);
}

function runFile(filename) {
    alert(`Run ${filename} - coming soon!`);
}

// Utility
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

// Drag and drop handlers
function initDropHandlers() {
    // Add drag and drop for file uploads if needed
}
