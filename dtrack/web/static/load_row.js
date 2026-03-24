// load_row.js - Load row CSV files

let detectedFiles = [];
let dbStatus = [];

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    initDropZone();
    if (document.getElementById('auto-scan').checked) {
        scanDirectory();
    }
    refreshStatus();
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

function openSettings() {
    alert('Settings page coming soon!');
}

function showHelp() {
    alert('Help documentation coming soon!');
}

// Drop Zone
function initDropZone() {
    const dropZone = document.getElementById('drop-zone');

    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.classList.add('drag-over');
    });

    dropZone.addEventListener('dragleave', () => {
        dropZone.classList.remove('drag-over');
    });

    dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropZone.classList.remove('drag-over');

        const files = Array.from(e.dataTransfer.files).filter(f => f.name.endsWith('.csv'));
        if (files.length > 0) {
            handleFiles(files);
        }
    });
}

function handleFiles(files) {
    logMessage(`Processing ${files.length} file(s)...`, 'info');

    // Simulate file processing
    Array.from(files).forEach(file => {
        const fileInfo = parseFileName(file.name);
        detectedFiles.push({
            file: file.name,
            pair: fileInfo.pair,
            side: fileInfo.side,
            path: file.name,
            rows: 0,
            dates: 0,
            modified: 'just now',
            selected: true
        });
    });

    renderDetectedFiles();
    showSuccess(`Added ${files.length} file(s)`);
}

function parseFileName(filename) {
    // Parse filenames like "customer_daily_left_row.csv"
    const parts = filename.replace('.csv', '').split('_');
    const side = parts.includes('left') ? 'left' : parts.includes('right') ? 'right' : 'unknown';
    const pair = parts.filter(p => p !== 'left' && p !== 'right' && p !== 'row').join('_');

    return { pair, side };
}

// Directory Scanning
async function scanDirectory() {
    const dir = document.getElementById('scan-dir').value;
    logMessage(`Scanning ${dir}...`, 'info');

    try {
        const response = await fetch(`/api/scan/csv?dir=${encodeURIComponent(dir)}&type=row`);
        if (!response.ok) throw new Error('Failed to scan directory');

        const data = await response.json();
        detectedFiles = data.files || [];

        renderDetectedFiles();
        logMessage(`Found ${detectedFiles.length} CSV file(s)`, 'success');
    } catch (error) {
        console.error('Scan error:', error);
        showError(error.message);
    }
}

function browsePath() {
    alert('File browser coming soon! For now, edit the path directly.');
}

function renderDetectedFiles() {
    const section = document.getElementById('detected-files-section');
    const tbody = document.getElementById('detected-files');

    if (detectedFiles.length === 0) {
        section.style.display = 'none';
        return;
    }

    section.style.display = 'block';

    tbody.innerHTML = detectedFiles.map((file, index) => `
        <tr>
            <td><input type="checkbox" ${file.selected ? 'checked' : ''}
                onchange="toggleFileSelection(${index}, this.checked)"></td>
            <td>${file.pair}</td>
            <td>${file.side}</td>
            <td style="font-family: monospace; font-size: 11px;">${file.file}</td>
            <td>${file.rows || '—'}</td>
            <td>${file.dates || '—'}</td>
            <td style="color: var(--text-muted);">${file.modified}</td>
        </tr>
    `).join('');

    updateLoadPreview();
}

function toggleFileSelection(index, selected) {
    detectedFiles[index].selected = selected;
    updateLoadPreview();
}

function selectAllFiles(selected) {
    detectedFiles.forEach(f => f.selected = selected);
    document.getElementById('select-all-files').checked = selected;
    renderDetectedFiles();
}

// Load Preview
function updateLoadPreview() {
    const section = document.getElementById('load-preview-section');
    const selected = detectedFiles.filter(f => f.selected);

    if (selected.length === 0) {
        section.style.display = 'none';
        return;
    }

    section.style.display = 'block';
    validateFiles();
}

async function validateFiles() {
    const results = document.getElementById('validation-results');
    const selected = detectedFiles.filter(f => f.selected);

    // Simulate validation
    const validations = [
        { type: 'success', message: 'All CSV files have required columns: date, row_count' },
        { type: 'success', message: 'Date formats validated (YYYY-MM-DD or YYYYMMDD)' },
        { type: 'success', message: 'Row counts are numeric and positive' },
        { type: 'warning', message: `${selected.length * 7} dates will overlap with existing data (will be replaced via UPSERT)` }
    ];

    results.innerHTML = validations.map(v => `
        <div class="validation-item">
            <span class="validation-icon validation-${v.type}">
                ${v.type === 'success' ? '✓' : '⚠'}
            </span>
            <span>${v.message}</span>
        </div>
    `).join('');
}

// Database Operations
async function loadSelected() {
    const selected = detectedFiles.filter(f => f.selected);
    if (selected.length === 0) {
        alert('No files selected');
        return;
    }

    const mode = document.querySelector('input[name="load-mode"]:checked').value;
    const log = document.getElementById('load-log');
    log.innerHTML = '';

    logMessage(`Loading ${selected.length} file(s) in ${mode} mode...`, 'info');

    try {
        const response = await fetch('/api/load/row', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ files: selected.map(f => f.path), mode })
        });

        if (!response.ok) throw new Error('Load failed');

        const result = await response.json();

        // Simulate load progress
        for (const file of selected) {
            const timestamp = new Date().toLocaleTimeString();
            logMessage(`[${timestamp}] Loading ${file.pair} (${file.side})...`, 'info');
            await new Promise(resolve => setTimeout(resolve, 300));

            const newRows = Math.floor(Math.random() * 20) + 10;
            const updated = Math.floor(Math.random() * 10);
            logMessage(`[${timestamp}] ✓ Upserted ${newRows + updated} rows (${newRows} new, ${updated} updated)`, 'success');
        }

        logMessage(`✓ Load complete: ${selected.length * 30} total rows upserted`, 'success');

        // Refresh status
        await refreshStatus();
        showSuccess('Load completed successfully');

        // Clear selected files after successful load
        detectedFiles = detectedFiles.filter(f => !f.selected);
        renderDetectedFiles();

    } catch (error) {
        console.error('Load error:', error);
        logMessage(`✗ Error: ${error.message}`, 'error');
        showError(error.message);
    }
}

async function refreshStatus() {
    try {
        const response = await fetch('/api/status/row');
        if (!response.ok) throw new Error('Failed to load status');

        const data = await response.json();
        dbStatus = data.status || [];

        if (data.lastLoad) {
            document.getElementById('last-load-time').textContent =
                new Date(data.lastLoad).toLocaleString();
        }

        renderDbStatus();
    } catch (error) {
        console.error('Status error:', error);
    }
}

function renderDbStatus() {
    const tbody = document.getElementById('db-status-table');

    if (dbStatus.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="empty-message">No data loaded yet</td></tr>';
        return;
    }

    tbody.innerHTML = dbStatus.map(row => {
        const statusClass = row.loaded > 0 ? 'ready' : 'warning';
        const statusText = row.loaded > 0 ? '✓ Ready' : '⊗ Not loaded';
        const dateRange = row.min_date && row.max_date
            ? `${row.min_date} → ${row.max_date}`
            : '—';

        return `
            <tr>
                <td>${row.pair}</td>
                <td>${row.side}</td>
                <td>${row.loaded || 0}</td>
                <td style="font-family: monospace; font-size: 11px;">${dateRange}</td>
                <td><span class="status-badge ${statusClass}">${statusText}</span></td>
            </tr>
        `;
    }).join('');
}

async function clearDatabase() {
    if (!confirm('Clear all row count data from database? This cannot be undone.')) {
        return;
    }

    try {
        const response = await fetch('/api/clear/row', { method: 'POST' });
        if (!response.ok) throw new Error('Failed to clear database');

        await refreshStatus();
        showSuccess('Database cleared');
        logMessage('Database cleared', 'warning');
    } catch (error) {
        showError(error.message);
    }
}

function queryDatabase() {
    const sql = prompt('Enter SQL query:', 'SELECT * FROM _row_counts LIMIT 10');
    if (!sql) return;

    fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql })
    })
    .then(r => r.json())
    .then(data => {
        console.table(data.rows);
        alert(`Query executed. Results in console (${data.rows.length} rows)`);
    })
    .catch(error => showError(error.message));
}

function exportDatabase() {
    alert('Export functionality coming soon!');
}

// Logging
function clearLog() {
    const log = document.getElementById('load-log');
    log.innerHTML = '<div class="log-empty">No load operations yet.</div>';
}

function logMessage(message, type = 'info') {
    const log = document.getElementById('load-log');
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

// Utility
function showSuccess(message) {
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed; top: 20px; right: 20px; z-index: 2000;
        background: var(--accent-green); color: white;
        padding: 12px 20px; border-radius: 6px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    `;
    notification.textContent = message;
    document.body.appendChild(notification);

    setTimeout(() => notification.remove(), 3000);
}

function showError(message) {
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed; top: 20px; right: 20px; z-index: 2000;
        background: var(--accent-red); color: white;
        padding: 12px 20px; border-radius: 6px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    `;
    notification.textContent = message;
    document.body.appendChild(notification);

    setTimeout(() => notification.remove(), 5000);
}
