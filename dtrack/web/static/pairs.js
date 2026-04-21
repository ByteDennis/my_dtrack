// pairs.js - Pair management and row generation

let pairs = [];
let editingPairIndex = -1;

// Constants (should match constants.py)
const DATA_SOURCES = [
    {value: "oracle", label: "SAS/Oracle"},
    {value: "hadoop", label: "SAS/Hadoop"},
    {value: "sas", label: "SAS Dataset"},
    {value: "aws", label: "AWS/Athena"},
    {value: "csv", label: "CSV"},
];

const CONNECTION_MACROS = {
    "oracle": ["pb23", "pb30"],
    "hadoop": ["hdp", "hadoop_prod"],
    "sas": ["work", "sasuser"],
    "aws": ["analytics_db", "warehouse_db", "mydb"],
    "csv": [],
};

// Step 3 of adding a new date format: add {value, label} here.
// See base.py DATE_TYPE_FORMATS comment block for full instructions.
const DATE_COLUMN_TYPES = [
    {value: "date", label: "Date"},
    {value: "timestamp", label: "Timestamp"},
    {value: "datetime", label: "DateTime"},
    {value: "num", label: "Number (YYYYMMDD)"},
    {value: "num_yyyymm", label: "Number (YYYYMM)"},
    {value: "string_dash", label: "String (YYYY-MM-DD)"},
    {value: "string_compact", label: "String (YYYYMMDD)"},
    {value: "string_mon", label: "String (DDMONYYYY)"},
    {value: "string_mon_dash", label: "String (DD-MON-YYYY)"},
    {value: "string_us", label: "String (MM/DD/YYYY)"},
];

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    loadPairs();
    loadGlobalSettings();
    initModalHandlers();
    initDropHandlers();
    initSourceDropdowns();
    initTestingMode();
});

// Navigation
function navigateToStep(step) {
    const routes = {
        'pairs': '/pairs',
        'load_row': '/load_row',
        'row_compare': '/row_compare',
        'col_mapping': '/col_mapping',
        'col_gen': '/col_gen',
        'load_col': '/load_col',
        'col_compare': '/col_compare'
    };
    if (routes[step]) {
        window.location.href = routes[step];
    }
}

function openSettingsModal() {
    document.getElementById('settings-modal').classList.add('active');
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

function applyGlobalSettings() {
    const mode = document.querySelector('input[name="global-mode"]:checked').value;
    const fromDate = document.getElementById('global-from-date').value;
    const toDate = document.getElementById('global-to-date').value;

    pairs.forEach(pair => {
        pair.mode = mode;
        pair.dateRangeMode = 'global';
        pair.fromDate = fromDate;
        pair.toDate = toDate;
    });

    saveGlobalSettings();
    renderPairs();
    closeSettingsModal();
    showSuccess('Global settings applied to all pairs');
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

function togglePairSelection(index) {
    pairs[index].selected = !pairs[index].selected;
    syncSelectAllCheckbox();
    renderPairs();
}

function selectAllPairs(selected) {
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

function copyPair(index) {
    const pair = pairs[index];
    const newPair = JSON.parse(JSON.stringify(pair));
    newPair.name = `${pair.name}_copy`;
    pairs.push(newPair);
    renderPairs();
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
    const hasSas = selected.some(p => !awsSources.has(p.left?.source) || !awsSources.has(p.right?.source));
    const hasAws = selected.some(p => awsSources.has(p.left?.source) || awsSources.has(p.right?.source));

    // --- Step 3: Render two-column review (SAS/Oracle | AWS/Athena) ---
    const reviewDiv = document.createElement('div');
    reviewDiv.id = 'generate-review';
    reviewDiv.innerHTML = `
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:16px;">
            <div style="font-weight:600; font-size:13px; color:var(--jp-ui-font-color1); border-bottom:1px solid var(--jp-border-color0); padding-bottom:6px;">
                SAS / Oracle
            </div>
            <div style="display:flex; justify-content:space-between; align-items:center; font-weight:600; font-size:13px; color:var(--jp-ui-font-color1); border-bottom:1px solid var(--jp-border-color0); padding-bottom:6px;">
                <span>AWS / Athena</span>
                ${hasAws ? `<button class="btn-primary" id="run-aws-btn" style="font-size:11px; padding:3px 10px;">Run AWS Extraction</button>` : ''}
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
            if (result.sql_file) logMessage(`SQL file: ${result.sql_file}`, 'success');
        } else {
            logMessage(`File generation error: ${result.error}`, 'error');
        }
    } catch (err) {
        logMessage(`File generation failed: ${err.message}`, 'error');
    }

    // --- Step 5: Wire up Run AWS button (reads extract_row.sql, streams progress) ---
    if (hasAws) {
        const btn = document.getElementById('run-aws-btn');
        btn.onclick = async () => {
            btn.disabled = true;
            btn.textContent = 'Running...';

            logMessage('Running AWS extraction from extract_row.sql...', 'info');
            try {
                const reqBody = {
                    type: 'row',
                    outdir: document.getElementById('global-aws-outdir')?.value || './csv/',
                };
                const nj = parseInt(document.getElementById('global-njob-right')?.value);
                if (nj > 0) reqBody.max_workers = nj;

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

                    // Parse SSE events from buffer
                    const parts = buffer.split('\n\n');
                    buffer = parts.pop(); // keep incomplete chunk

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
                            const status = msg.ok
                                ? `${msg.rows} dates, ${msg.elapsed}s`
                                : `FAIL: ${msg.error}`;
                            logMessage(`[${msg.done}/${msg.total}] ${msg.name}: ${status}`, msg.ok ? 'info' : 'error');
                            btn.textContent = `Running ${msg.done}/${msg.total}...`;
                        } else if (eventType === 'done') {
                            if (msg.ok) {
                                logMessage(`AWS extraction complete: ${msg.succeeded}/${msg.total} succeeded`, 'success');
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
        };
    }
}

function _configBasename() {
    // Derive a display name for the config path (the server knows the real path)
    return 'pairs.json';
}

function clearLog() {
    const log = document.getElementById('generation-log');
    log.innerHTML = '<div class="log-empty">No files generated yet.</div>';
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
