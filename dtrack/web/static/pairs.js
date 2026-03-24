// pairs.js - Pair management and row generation

let pairs = [];
let editingPairIndex = -1;

// Constants (should match constants.py)
const DATA_SOURCES = [
    {value: "pcds", label: "SAS/Oracle"},
    {value: "hadoop", label: "SAS/Hadoop"},
    {value: "oracle", label: "Oracle Direct"},
    {value: "sas", label: "SAS Dataset"},
    {value: "aws", label: "AWS/Athena"},
    {value: "csv", label: "CSV"},
];

const CONNECTION_MACROS = {
    "pcds": ["pcds", "pb23", "pb30"],
    "hadoop": ["hdp", "hadoop_prod"],
    "oracle": ["pcds", "pb23", "pb30"],
    "sas": ["work", "sasuser"],
    "aws": ["analytics_db", "warehouse_db", "mydb"],
    "csv": [],
};

const DATE_COLUMN_TYPES = [
    {value: "date", label: "Date"},
    {value: "timestamp", label: "Timestamp"},
    {value: "datetime", label: "DateTime"},
    {value: "num", label: "Number (SAS date)"},
    {value: "string_dash", label: "String (YYYY-MM-DD)"},
    {value: "string_compact", label: "String (YYYYMMDD)"},
];

const VINTAGE_PRESETS = {
    "pcds": {
        "day": "{col}",
        "week": "TRUNC({col}, 'IW')",
        "month": "TRUNC({col}, 'MM')",
        "quarter": "TRUNC({col}, 'Q')",
        "year": "TRUNC({col}, 'YYYY')",
    },
    "hadoop": {
        "day": "{col}",
        "week": "TRUNC({col}, 'IW')",
        "month": "TRUNC({col}, 'MM')",
        "quarter": "TRUNC({col}, 'Q')",
        "year": "TRUNC({col}, 'YYYY')",
    },
    "oracle": {
        "day": "{col}",
        "week": "TRUNC({col}, 'IW')",
        "month": "TRUNC({col}, 'MM')",
        "quarter": "TRUNC({col}, 'Q')",
        "year": "TRUNC({col}, 'YYYY')",
    },
    "aws": {
        "day": "{col}",
        "week": "DATE_TRUNC('week', {col})",
        "month": "DATE_TRUNC('month', {col})",
        "quarter": "DATE_TRUNC('quarter', {col})",
        "year": "DATE_TRUNC('year', {col})",
    },
    "sas": {
        "day": "{col}",
        "week": "intnx('week.2', {col}, 0, 'b')",
        "month": "intnx('month', {col}, 0, 'b')",
        "quarter": "intnx('qtr', {col}, 0, 'b')",
        "year": "intnx('year', {col}, 0, 'b')",
    },
};

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    loadPairs();
    initDateDefaults();
    initModalHandlers();
    initDropHandlers();
    initSourceDropdowns();
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

    renderPairs();
    closeSettingsModal();
    showSuccess('Global settings applied to all pairs');
}

function showVintageTips() {
    const tips = `Vintage Function Examples:

Oracle/SAS Oracle/SAS Hadoop:
• Day: column_name
• Week: TRUNC(column_name, 'IW')
• Month: TRUNC(column_name, 'MM')
• Quarter: TRUNC(column_name, 'Q')
• Year: TRUNC(column_name, 'YYYY')

AWS/Athena:
• Day: column_name
• Week: DATE_TRUNC('week', column_name)
• Month: DATE_TRUNC('month', column_name)
• Quarter: DATE_TRUNC('quarter', column_name)
• Year: DATE_TRUNC('year', column_name)

SAS Dataset:
• Day: column_name
• Week: intnx('week.2', column_name, 0, 'b')
• Month: intnx('month', column_name, 0, 'b')
• Quarter: intnx('qtr', column_name, 0, 'b')
• Year: intnx('year', column_name, 0, 'b')

Leave empty or use column name directly if no bucketing needed.`;

    alert(tips);
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
    leftSource.value = 'pcds';
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

    // Set default to 'date'
    leftDateType.value = 'date';
    rightDateType.value = 'date';
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

// Date Handling
function initDateDefaults() {
    // Start with empty dates (no restriction)
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

// Pair Management
async function loadPairs() {
    try {
        const response = await fetch('/api/pairs/list');
        const data = await response.json();

        if (data.pairs) {
            pairs = data.pairs;
            renderPairs();
        }
    } catch (error) {
        console.error('Failed to load pairs:', error);
        showError('Failed to load pairs from database');
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
                        ${pair.left.vintage ? `<div class="pair-info-row">
                            <span class="pair-info-label">Vintage:</span>
                            <span class="pair-info-value">${pair.left.vintage}</span>
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
                        ${pair.right.vintage ? `<div class="pair-info-row">
                            <span class="pair-info-label">Vintage:</span>
                            <span class="pair-info-value">${pair.right.vintage}</span>
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
    document.getElementById('left-vintage').value = pair.left.vintage || '';
    document.getElementById('left-cte').value = pair.left.processed || '';

    // Right side
    document.getElementById('right-source').value = pair.right.source;
    updateConnOptions('right'); // Update conn options before setting value
    document.getElementById('right-conn').value = pair.right.conn_macro;
    document.getElementById('right-table').value = pair.right.table;
    document.getElementById('right-date-col').value = pair.right.date_col;
    document.getElementById('right-date-type').value = pair.right.date_type || 'date';
    document.getElementById('right-where').value = pair.right.where || '';
    document.getElementById('right-vintage').value = pair.right.vintage || '';
    document.getElementById('right-cte').value = pair.right.processed || '';

    // Mode and date range
    document.getElementById('pair-incremental').checked = (pair.mode !== 'full');
    document.getElementById('pair-from-date').value = pair.fromDate || '';
    document.getElementById('pair-to-date').value = pair.toDate || '';

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
        document.getElementById(`${side}-source`).value = side === 'left' ? 'pcds' : 'aws';
        document.getElementById(`${side}-conn`).value = '';
        document.getElementById(`${side}-table`).value = '';
        document.getElementById(`${side}-date-col`).value = '';
        document.getElementById(`${side}-where`).value = '';
        document.getElementById(`${side}-vintage`).value = '';
        document.getElementById(`${side}-cte`).value = '';
    });

    document.getElementById('pair-incremental').checked = true;
    document.getElementById('pair-from-date').value = '';
    document.getElementById('pair-to-date').value = '';
}

async function savePair() {
    const pairData = {
        name: document.getElementById('pair-name').value.trim(),
        description: document.getElementById('pair-desc').value.trim(),
        left: {
            source: document.getElementById('left-source').value,
            conn_macro: document.getElementById('left-conn').value.trim(),
            table: document.getElementById('left-table').value.trim(),
            date_col: document.getElementById('left-date-col').value.trim(),
            date_type: document.getElementById('left-date-type').value,
            where: document.getElementById('left-where').value.trim(),
            vintage: document.getElementById('left-vintage').value.trim(),
            processed: document.getElementById('left-cte').value.trim()
        },
        right: {
            source: document.getElementById('right-source').value,
            conn_macro: document.getElementById('right-conn').value.trim(),
            table: document.getElementById('right-table').value.trim(),
            date_col: document.getElementById('right-date-col').value.trim(),
            date_type: document.getElementById('right-date-type').value,
            where: document.getElementById('right-where').value.trim(),
            vintage: document.getElementById('right-vintage').value.trim(),
            processed: document.getElementById('right-cte').value.trim()
        },
        mode: document.getElementById('pair-incremental').checked ? 'incremental' : 'full',
        fromDate: document.getElementById('pair-from-date').value,
        toDate: document.getElementById('pair-to-date').value,
    };

    // Validation
    if (!pairData.name) {
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
        const url = editingPairIndex >= 0 ? `/api/pairs/${pairData.name}` : '/api/pairs';
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
        showSuccess(`Pair "${pairData.name}" saved successfully`);
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
            vintage: pair.left.vintage || "",
            processed: pair.left.processed || ""
        },
        right: {
            source: pair.right.source,
            conn_macro: pair.right.conn_macro,
            table: pair.right.table,
            date_col: pair.right.date_col,
            date_type: pair.right.date_type || "date",
            where: pair.right.where || "",
            vintage: pair.right.vintage || "",
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

function setVintage(side, type) {
    const input = document.getElementById(`${side}-vintage`);
    const source = document.getElementById(`${side}-source`).value;
    const dateCol = document.getElementById(`${side}-date-col`).value || 'date_col';

    const templates = VINTAGE_PRESETS[source] || VINTAGE_PRESETS['pcds'];
    const template = templates[type];

    if (template) {
        input.value = template.replace(/\{col\}/g, dateCol);
    } else {
        input.value = dateCol;
    }
}

function testPair() {
    alert('Test query preview coming soon!');
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

    // Get global dates for context
    const globalFrom = document.getElementById('global-from-date')?.value || '';
    const globalTo = document.getElementById('global-to-date')?.value || '';

    let html = '';

    selected.forEach(pair => {
        const fromDate = pair.fromDate || globalFrom || '2024-01-01';
        const toDate = pair.toDate || globalTo || '2024-12-31';
        const mode = pair.mode || 'incremental';

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
                        <pre style="background: var(--jp-layout-color1); padding: 12px; border-radius: 4px; border: 1px solid var(--jp-border-color0); font-size: 11px; overflow-x: auto; margin: 0;">${generateQueryPreview(pair, 'left', fromDate, toDate, mode)}</pre>
                        <div style="margin-top: 8px; font-size: 11px; color: var(--jp-ui-font-color2);">
                            Output: ${pair.name}_left_row.${pair.left.source === 'aws' ? 'sql' : 'sas'}
                        </div>
                    </div>

                    <div>
                        <div style="font-weight: 600; font-size: 12px; color: var(--jp-ui-font-color1); margin-bottom: 8px;">
                            RIGHT (${pair.right.source})
                        </div>
                        <pre style="background: var(--jp-layout-color1); padding: 12px; border-radius: 4px; border: 1px solid var(--jp-border-color0); font-size: 11px; overflow-x: auto; margin: 0;">${generateQueryPreview(pair, 'right', fromDate, toDate, mode)}</pre>
                        <div style="margin-top: 8px; font-size: 11px; color: var(--jp-ui-font-color2);">
                            Output: ${pair.name}_right_row.${pair.right.source === 'aws' ? 'sql' : 'sas'}
                        </div>
                    </div>
                </div>
            </div>
        `;
    });

    content.innerHTML = html;
    modal.classList.add('active');
}

function generateQueryPreview(pair, side, fromDate, toDate, mode) {
    const config = pair[side];
    const vintage = config.vintage || config.date_col;
    const whereClause = config.where || '';

    if (config.source === 'aws') {
        // AWS/Athena SQL
        let sql = `SELECT\n  ${vintage} AS date_value,\n  COUNT(*) AS row_count\n`;
        sql += `FROM ${config.conn_macro}.${config.table}\n`;

        const conditions = [];
        if (fromDate) conditions.push(`${config.date_col} >= DATE '${fromDate}'`);
        if (toDate) conditions.push(`${config.date_col} <= DATE '${toDate}'`);
        if (whereClause) conditions.push(`(${whereClause})`);

        if (conditions.length > 0) {
            sql += `WHERE ${conditions.join('\n  AND ')}\n`;
        }

        sql += `GROUP BY ${vintage}\n`;
        sql += `ORDER BY date_value;`;

        return sql;
    } else {
        // SAS/Oracle
        let sas = `proc sql;\n`;
        sas += `  %${config.conn_macro}\n`;
        sas += `  create table work.${pair.name}_${side}_row as\n`;
        sas += `  select * from connection to oracle (\n`;
        sas += `    SELECT\n`;
        sas += `      ${vintage} AS date_value,\n`;
        sas += `      COUNT(*) AS row_count\n`;
        sas += `    FROM ${config.table}\n`;

        const conditions = [];
        if (fromDate) conditions.push(`${config.date_col} >= DATE '${fromDate}'`);
        if (toDate) conditions.push(`${config.date_col} <= DATE '${toDate}'`);
        if (whereClause) conditions.push(`(${whereClause})`);

        if (conditions.length > 0) {
            sas += `    WHERE ${conditions.join('\n      AND ')}\n`;
        }

        sas += `    GROUP BY ${vintage}\n`;
        sas += `  );\n`;
        sas += `  disconnect from oracle;\n`;
        sas += `quit;`;

        return sas;
    }
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

    // Resolve dates: pair-level overrides, then global, then empty
    const globalFrom = document.getElementById('global-from-date')?.value || '';
    const globalTo = document.getElementById('global-to-date')?.value || '';

    // Group selected pairs by platform (sas-like vs aws)
    const awsSources = new Set(['aws']);
    const hasAws = selected.some(p => awsSources.has(p.left.source) || awsSources.has(p.right.source));
    const hasSas = selected.some(p => !awsSources.has(p.left.source) || !awsSources.has(p.right.source));

    // Build CLI command strings for display
    const commands = [];

    if (hasSas) {
        const fromArg = globalFrom ? ` --from ${globalFrom}` : '';
        const toArg = globalTo ? ` --to ${globalTo}` : '';
        const cmd = `dtrack gen-sas ${_configBasename()} ./sas/ --type row${fromArg}${toArg}`;
        commands.push({platform: 'sas', cmd, label: 'SAS/Oracle'});
    }

    if (hasAws) {
        const fromArg = globalFrom ? ` --from ${globalFrom}` : '';
        const toArg = globalTo ? ` --to ${globalTo}` : '';
        const cmd = `dtrack gen-aws ${_configBasename()} ./csv/ --type row${fromArg}${toArg}`;
        commands.push({platform: 'aws', cmd, label: 'AWS/Athena'});
    }

    // Show commands and confirm/cancel buttons
    logMessage(`${selected.length} pair(s) selected — review commands below:`, 'info');

    commands.forEach(({cmd, label}) => {
        const entry = document.createElement('div');
        entry.style.cssText = 'margin-bottom:8px;';
        entry.innerHTML = `
            <div style="font-size:11px; color: var(--jp-ui-font-color2); margin-bottom:2px;">${label}</div>
            <pre class="log-command">${cmd}</pre>
        `;
        log.appendChild(entry);
    });

    // Add confirm/cancel buttons
    const btnRow = document.createElement('div');
    btnRow.className = 'log-entry';
    btnRow.style.cssText = 'display:flex; gap:8px; padding:8px 0;';
    btnRow.innerHTML = `
        <button class="btn-primary" id="confirm-generate">Confirm</button>
        <button class="btn-secondary" id="cancel-generate">Cancel</button>
    `;
    log.appendChild(btnRow);
    log.scrollTop = log.scrollHeight;

    // Wait for user action
    const action = await new Promise(resolve => {
        document.getElementById('confirm-generate').onclick = () => resolve('confirm');
        document.getElementById('cancel-generate').onclick = () => resolve('cancel');
    });

    btnRow.remove();

    if (action === 'cancel') {
        logMessage('Cancelled.', 'info');
        return;
    }

    // Execute each platform command
    for (const {platform, cmd, label} of commands) {
        logMessage(`Running ${label} extraction...`, 'info');

        try {
            const body = {
                platform,
                type: 'row',
                outdir: platform === 'aws' ? './csv/' : './sas/',
            };
            if (globalFrom) body.from_date = globalFrom;
            if (globalTo) body.to_date = globalTo;

            const resp = await fetch('/api/extract', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(body),
            });

            const result = await resp.json();

            if (result.ok) {
                // Show backend output line by line
                if (result.output) {
                    result.output.trim().split('\n').forEach(line => {
                        if (line.trim()) logMessage(line, 'info');
                    });
                }
                logMessage(`${label} extraction complete.`, 'success');
            } else {
                logMessage(`${label} error: ${result.error}`, 'error');
                if (result.output) {
                    result.output.trim().split('\n').forEach(line => {
                        if (line.trim()) logMessage(line, 'error');
                    });
                }
            }
        } catch (err) {
            logMessage(`${label} request failed: ${err.message}`, 'error');
        }
    }

    logMessage('Done.', 'success');
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
