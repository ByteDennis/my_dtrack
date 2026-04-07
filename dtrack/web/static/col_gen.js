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
// Settings Modal
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
    saveGlobalSettings();
    closeSettingsModal();
    showSuccess('Settings saved');
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
// Load Pairs
// ---------------------------------------------------------------------------
async function loadPairs() {
    try {
        const [pairsResp, statusResp] = await Promise.all([
            fetch('/api/pairs/list'),
            fetch('/api/status'),
        ]);
        const pairsData = await pairsResp.json();
        const statusData = await statusResp.json();

        // Build a lookup from status data
        const statusMap = {};
        for (const p of (statusData.pairs || [])) {
            statusMap[p.pair_name] = p;
        }

        pairs = (pairsData.pairs || []).map(p => ({
            name: p.name,
            description: p.description || '',
            left: p.left || {},
            right: p.right || {},
            selected: false,
            expanded: false,
            status: statusMap[p.name] || null,
        }));

        renderPairs();
    } catch (e) {
        console.error('Failed to load pairs:', e);
        showError('Failed to load pairs: ' + e.message);
    }
}

// ---------------------------------------------------------------------------
// Render Pairs
// ---------------------------------------------------------------------------
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

        return `
        <div class="pair-item ${pair.selected ? 'selected' : ''}" id="pair-${index}">
            <div class="pair-header" onclick="togglePair(${index})">
                <input type="checkbox" class="pair-checkbox"
                    ${pair.selected ? 'checked' : ''}
                    onclick="event.stopPropagation(); togglePairSelection(${index})"
                    onchange="togglePairSelection(${index})">
                <span class="pair-expand">${pair.expanded ? '&#9654;' : '&#9660;'}</span>
                <span class="pair-name">${pair.name}</span>
                <div style="display:flex; gap:8px; margin-left:auto;">
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
                    </div>
                </div>
            </div>
        </div>`;
    }).join('');
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

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------
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

    logMessage(`Generating col scripts for ${selected.length} pair(s)...`, 'info');

    try {
        const genBody = {
            type: 'col',
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
            logMessage('Col script generation complete.', 'success');
        } else {
            logMessage(`Generation error: ${result.error}`, 'error');
        }
    } catch (err) {
        logMessage(`Generation failed: ${err.message}`, 'error');
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
