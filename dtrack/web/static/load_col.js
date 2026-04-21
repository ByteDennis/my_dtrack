// load_col.js - Col stats CSV drop, match, load

let fileEntries = [];   // {file, name, rows, matched, tableName, side, pairName, selected, fileType}
let knownTables = [];   // [{table_left, table_right, pair_name, source_left, source_right}]

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', async () => {
    initDropZone();
    await loadKnownTables();
    await refreshDbStatus();
});

function navigateToStep(step) {
    const routes = {
        pairs: '/pairs', load_row: '/load_row', row_compare: '/row_compare',
        col_mapping: '/col_mapping', col_gen: '/col_gen',
        load_col: '/load_col', col_compare: '/col_compare',
    };
    if (routes[step]) window.location.href = routes[step];
}

// ---------------------------------------------------------------------------
// Known tables from DB
// ---------------------------------------------------------------------------
async function loadKnownTables() {
    try {
        const resp = await fetch('/api/status');
        const data = await resp.json();
        knownTables = (data.pairs || []).map(p => ({
            pair_name: p.pair_name,
            table_left: p.table_left,
            table_right: p.table_right,
            source_left: p.source_left || '',
            source_right: p.source_right || '',
        }));

        // Also load from config
        const cfgResp = await fetch('/api/pairs/list');
        const cfgData = await cfgResp.json();
        const existing = new Set(knownTables.map(t => t.pair_name));
        for (const p of (cfgData.pairs || [])) {
            if (existing.has(p.name)) continue;
            const leftSource = p.left?.source || '';
            const rightSource = p.right?.source || '';
            knownTables.push({
                pair_name: p.name,
                table_left: leftSource ? `${leftSource}_${p.name}` : p.name,
                table_right: rightSource ? `${rightSource}_${p.name}` : p.name,
                source_left: leftSource,
                source_right: rightSource,
            });
        }
    } catch (e) {
        console.error('Failed to load tables:', e);
    }
}

// ---------------------------------------------------------------------------
// Database status
// ---------------------------------------------------------------------------
async function refreshDbStatus() {
    const el = document.getElementById('db-summary');
    try {
        const resp = await fetch('/api/status');
        const data = await resp.json();
        const pairs = data.pairs || [];

        if (!pairs.length) {
            el.innerHTML = '<div class="empty-message">No pairs configured</div>';
            return;
        }

        // Also fetch col stats summary (includes date history)
        let colStatus = {};
        try {
            const colResp = await fetch('/api/status/col');
            const colData = await colResp.json();
            colStatus = colData.status || {};
        } catch (e) {
            // Non-critical
        }

        // Fetch per-pair config for vintage info
        let pairConfigs = {};
        try {
            const cfgResp = await fetch('/api/pairs/list');
            const cfgData = await cfgResp.json();
            for (const pc of (cfgData.pairs || [])) {
                pairConfigs[pc.name] = pc;
            }
        } catch (e) {
            // Non-critical
        }

        function colInfo(tableName) {
            const info = colStatus[tableName];
            if (!info) return { count: 0, dates: [], min: null, max: null };
            return {
                count: info.count || 0,
                dates: info.dates || [],
                min: info.min_date || null,
                max: info.max_date || null,
            };
        }

        function dateBadge(info) {
            if (!info.dates.length) return '<span class="file-meta">no dates</span>';
            const n = info.dates.length;
            return `<span class="file-meta">${n} date${n !== 1 ? 's' : ''}: ${info.min} &rarr; ${info.max}</span>`;
        }

        const rows = pairs.map(p => {
            const lc = p.left.col_count || 0;
            const rc = p.right.col_count || 0;
            const lInfo = colInfo(p.table_left);
            const rInfo = colInfo(p.table_right);
            const cfg = pairConfigs[p.pair_name] || {};
            const lVintage = (cfg.left || {}).vintage || '';
            const rVintage = (cfg.right || {}).vintage || '';
            const lColBadge = lc > 0
                ? `<span class="status-badge ready">${lc}</span>`
                : `<span class="status-badge warning">0</span>`;
            const rColBadge = rc > 0
                ? `<span class="status-badge ready">${rc}</span>`
                : `<span class="status-badge warning">0</span>`;
            const lStatsBadge = lInfo.count > 0
                ? `<span class="status-badge ready">${lInfo.count.toLocaleString()}</span>`
                : `<span class="status-badge warning">0</span>`;
            const rStatsBadge = rInfo.count > 0
                ? `<span class="status-badge ready">${rInfo.count.toLocaleString()}</span>`
                : `<span class="status-badge warning">0</span>`;

            return `<tr>
                <td rowspan="2" style="vertical-align:middle; font-weight:600;">${p.pair_name}</td>
                <td>L</td>
                <td style="text-align:center;">${lColBadge}</td>
                <td style="text-align:right;">${lStatsBadge}</td>
                <td>${dateBadge(lInfo)}</td>
                <td><span class="file-meta">${lVintage || 'all'}</span></td>
            </tr><tr>
                <td>R</td>
                <td style="text-align:center;">${rColBadge}</td>
                <td style="text-align:right;">${rStatsBadge}</td>
                <td>${dateBadge(rInfo)}</td>
                <td><span class="file-meta">${rVintage || 'all'}</span></td>
            </tr>`;
        }).join('');

        el.innerHTML = `
            <table class="data-table compact">
                <thead><tr>
                    <th>Pair</th><th></th><th style="text-align:center;">Cols</th><th style="text-align:right;">Col Stats</th><th>Date Range (DB)</th><th>Vintage</th>
                </tr></thead>
                <tbody>${rows}</tbody>
            </table>`;
    } catch (e) {
        el.innerHTML = '<div class="empty-message">Failed to load status</div>';
    }
}

// ---------------------------------------------------------------------------
// Drop zone
// ---------------------------------------------------------------------------
function initDropZone() {
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');

    dropZone.addEventListener('dragover', e => {
        e.preventDefault();
        dropZone.classList.add('drag-over');
    });
    dropZone.addEventListener('dragleave', () => {
        dropZone.classList.remove('drag-over');
    });
    dropZone.addEventListener('drop', e => {
        e.preventDefault();
        dropZone.classList.remove('drag-over');
        const csvFiles = Array.from(e.dataTransfer.files).filter(f => f.name.endsWith('.csv'));
        if (csvFiles.length) processFiles(csvFiles);
    });

    document.getElementById('browse-btn').addEventListener('click', () => fileInput.click());
    dropZone.addEventListener('click', e => {
        if (e.target === dropZone || e.target.closest('.drop-zone-content') && e.target.tagName !== 'BUTTON') {
            fileInput.click();
        }
    });
    fileInput.addEventListener('change', () => {
        const csvFiles = Array.from(fileInput.files).filter(f => f.name.endsWith('.csv'));
        if (csvFiles.length) processFiles(csvFiles);
        fileInput.value = '';
    });

    document.getElementById('drop-more-btn').addEventListener('click', () => fileInput.click());
    document.getElementById('load-btn').addEventListener('click', loadSelected);
    document.getElementById('scan-folder-btn').addEventListener('click', scanFolder);
    document.getElementById('clear-log-btn').addEventListener('click', () => {
        document.getElementById('run-log').innerHTML = '';
        document.getElementById('log-section').style.display = 'none';
    });
}

// ---------------------------------------------------------------------------
// Scan server-side folder
// ---------------------------------------------------------------------------
async function scanFolder() {
    const folder = document.getElementById('folder-path').value.trim();
    if (!folder) return;

    const btn = document.getElementById('scan-folder-btn');
    btn.disabled = true;
    btn.textContent = 'Scanning...';

    try {
        const resp = await fetch(`/api/scan/folder?dir=${encodeURIComponent(folder)}`);
        const data = await resp.json();

        if (!resp.ok) {
            showToast(data.error || 'Scan failed', 'error');
            return;
        }

        const files = data.files || [];
        if (!files.length) {
            showToast('No CSV files found in folder', 'error');
            return;
        }

        for (const f of files) {
            if (fileEntries.some(e => e.name === f.name)) continue;

            const fileType = detectFileType(f.name);
            if (fileType === 'skip') continue;  // ignore _row.csv, _columns.csv, etc.

            const match = autoMatch(f.name);
            fileEntries.push({
                file: null,
                serverPath: f.path,
                name: f.name,
                rows: f.rows,
                matched: match.matched,
                tableName: match.tableName,
                side: match.side,
                pairName: match.pairName,
                selected: match.matched,
                fileType: fileType,
            });
        }

        renderFileList();
        document.getElementById('match-section').style.display = '';
        showToast(`Found ${files.length} CSV file(s)`);
    } catch (e) {
        showToast(e.message, 'error');
    } finally {
        btn.disabled = false;
        btn.textContent = 'Scan';
    }
}

// ---------------------------------------------------------------------------
// Process dropped files
// ---------------------------------------------------------------------------
async function processFiles(files) {
    for (const file of files) {
        if (fileEntries.some(e => e.name === file.name)) continue;

        const fileType = detectFileType(file.name);
        if (fileType === 'skip') continue;  // ignore _row.csv, _columns.csv, etc.

        const { rows } = await parseCSV(file);
        const match = autoMatch(file.name);

        fileEntries.push({
            file,
            name: file.name,
            rows,
            matched: match.matched,
            tableName: match.tableName,
            side: match.side,
            pairName: match.pairName,
            selected: match.matched,
            fileType: fileType,
        });
    }

    renderFileList();
    document.getElementById('match-section').style.display = '';
}

function parseCSV(file) {
    return new Promise(resolve => {
        const reader = new FileReader();
        reader.onload = e => {
            const lines = e.target.result.split('\n').filter(l => l.trim());
            resolve({ rows: Math.max(0, lines.length - 1) });
        };
        reader.readAsText(file);
    });
}

// ---------------------------------------------------------------------------
// Detect file type from filename
// ---------------------------------------------------------------------------
function detectFileType(filename) {
    const lower = filename.toLowerCase();
    // Load-col page only handles *_col.csv. _columns.csv belongs to
    // col_mapping; _row.csv to load_row; everything else is skipped.
    if (lower.endsWith('_col.csv')) {
        return 'col_stats';
    }
    return 'skip';
}

// ---------------------------------------------------------------------------
// Auto-match filename to known DB table
// ---------------------------------------------------------------------------
function autoMatch(filename) {
    const stripped = filename.replace(/\.csv$/i, '');
    if (!/_col$/i.test(stripped)) {
        return { matched: false, tableName: '', side: '', pairName: '' };
    }
    const stem = stripped.replace(/_col$/i, '');

    const noMatch = { matched: false, tableName: '', side: '', pairName: '' };

    for (const pair of knownTables) {
        if (stem.toLowerCase() === pair.table_left.toLowerCase()) {
            return { matched: true, tableName: pair.table_left, side: 'left', pairName: pair.pair_name };
        }
        if (stem.toLowerCase() === pair.table_right.toLowerCase()) {
            return { matched: true, tableName: pair.table_right, side: 'right', pairName: pair.pair_name };
        }
    }

    for (const pair of knownTables) {
        if (stem.toLowerCase().startsWith(pair.table_left.toLowerCase())) {
            return { matched: true, tableName: pair.table_left, side: 'left', pairName: pair.pair_name };
        }
        if (stem.toLowerCase().startsWith(pair.table_right.toLowerCase())) {
            return { matched: true, tableName: pair.table_right, side: 'right', pairName: pair.pair_name };
        }
    }

    return noMatch;
}

function getAvailableTableOptions(currentIdx) {
    const taken = new Set();
    fileEntries.forEach((e, idx) => {
        if (idx !== currentIdx && e.tableName) {
            taken.add(e.tableName.toLowerCase());
        }
    });

    const opts = [];
    for (const pair of knownTables) {
        if (!taken.has(pair.table_left.toLowerCase())) {
            opts.push({ value: `${pair.table_left}|left|${pair.pair_name}`, label: `${pair.table_left} (${pair.pair_name} left)` });
        }
        if (!taken.has(pair.table_right.toLowerCase())) {
            opts.push({ value: `${pair.table_right}|right|${pair.pair_name}`, label: `${pair.table_right} (${pair.pair_name} right)` });
        }
    }
    return opts;
}

// ---------------------------------------------------------------------------
// Render matched file list
// ---------------------------------------------------------------------------
function renderFileList() {
    const container = document.getElementById('file-list');

    container.innerHTML = fileEntries.map((entry, idx) => {
        const matchBadge = entry.matched
            ? `<span class="status-badge ready">matched</span>`
            : `<span class="status-badge warning">unmatched</span>`;

        const typeBadge = entry.fileType === 'columns'
            ? `<span class="status-badge" style="background:#e3f2fd; color:#1565c0;">metadata</span>`
            : `<span class="status-badge" style="background:#f3e5f5; color:#7b1fa2;">col stats</span>`;

        const targetLine = entry.matched
            ? `<span class="file-target">&rarr; ${entry.tableName} <span class="file-side">(${entry.side})</span></span>`
            : buildDropdown(idx, getAvailableTableOptions(idx));

        return `
        <div class="file-card ${entry.matched ? '' : 'unmatched'}">
            <div class="file-card-header">
                <label class="file-checkbox">
                    <input type="checkbox" ${entry.selected ? 'checked' : ''}
                           ${!entry.matched && !entry.tableName ? 'disabled' : ''}
                           onchange="toggleFile(${idx}, this.checked)">
                </label>
                <span class="file-name">${entry.name}</span>
                ${typeBadge}
                ${matchBadge}
                <button class="btn-text btn-remove" onclick="removeFile(${idx})" title="Remove">&times;</button>
            </div>
            <div class="file-card-body">
                ${targetLine}
                <span class="file-meta">${entry.rows.toLocaleString()} rows</span>
            </div>
        </div>`;
    }).join('');

    updateLoadButton();
}

function buildDropdown(idx, tableOpts) {
    const options = tableOpts.map(o =>
        `<option value="${o.value}">${o.label}</option>`
    ).join('');
    return `<select class="manual-match-select" onchange="manualMatch(${idx}, this.value)">
        <option value="">-- select table --</option>
        ${options}
    </select>`;
}

function toggleFile(idx, checked) {
    fileEntries[idx].selected = checked;
    updateLoadButton();
}

function removeFile(idx) {
    fileEntries.splice(idx, 1);
    if (fileEntries.length === 0) {
        document.getElementById('match-section').style.display = 'none';
    } else {
        renderFileList();
    }
}

function manualMatch(idx, value) {
    if (!value) {
        fileEntries[idx].matched = false;
        fileEntries[idx].tableName = '';
        fileEntries[idx].side = '';
        fileEntries[idx].pairName = '';
        fileEntries[idx].selected = false;
    } else {
        const [tableName, side, pairName] = value.split('|');
        fileEntries[idx].matched = true;
        fileEntries[idx].tableName = tableName;
        fileEntries[idx].side = side;
        fileEntries[idx].pairName = pairName;
        fileEntries[idx].selected = true;
    }
    renderFileList();
}

function updateLoadButton() {
    const selected = fileEntries.filter(e => e.selected && e.tableName);
    const btn = document.getElementById('load-btn');
    btn.disabled = selected.length === 0;
    btn.textContent = `Load Selected (${selected.length})`;
}

// ---------------------------------------------------------------------------
// Load selected files
// ---------------------------------------------------------------------------
async function loadSelected() {
    const selected = fileEntries.filter(e => e.selected && e.tableName);
    if (!selected.length) return;

    const btn = document.getElementById('load-btn');
    btn.disabled = true;
    btn.textContent = 'Loading...';

    const logSection = document.getElementById('log-section');
    const logEl = document.getElementById('run-log');
    logSection.style.display = '';
    logEl.innerHTML = '';

    logEntry(`Loading ${selected.length} file(s)...`);

    let successCount = 0;

    for (const entry of selected) {
        logEntry(`Loading ${entry.name} -> ${entry.tableName} (${entry.side})...`);

        try {
            let resp;
            const isColumnFile = entry.fileType === 'columns';

            if (entry.file) {
                const formData = new FormData();
                formData.append('file', entry.file);
                formData.append('table_name', entry.tableName);

                if (isColumnFile) {
                    // Column metadata file
                    const source = entry.side === 'left'
                        ? knownTables.find(t => t.table_left === entry.tableName)?.source_left
                        : knownTables.find(t => t.table_right === entry.tableName)?.source_right;
                    formData.append('source', source || '');
                    resp = await fetch('/api/load/columns/upload', { method: 'POST', body: formData });
                } else {
                    // Col stats file
                    formData.append('mode', 'upsert');
                    resp = await fetch('/api/load/col-stats/upload', { method: 'POST', body: formData });
                }
            } else if (entry.serverPath) {
                if (isColumnFile) {
                    const source = entry.side === 'left'
                        ? knownTables.find(t => t.table_left === entry.tableName)?.source_left
                        : knownTables.find(t => t.table_right === entry.tableName)?.source_right;
                    resp = await fetch('/api/load/columns/path', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            path: entry.serverPath,
                            table_name: entry.tableName,
                            source: source || '',
                        }),
                    });
                } else {
                    resp = await fetch('/api/load/col-stats/path', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            path: entry.serverPath,
                            table_name: entry.tableName,
                            mode: 'upsert',
                        }),
                    });
                }
            } else {
                logEntry(`  SKIP ${entry.name}: no file or path`, 'error');
                continue;
            }

            const data = await resp.json();

            if (data.ok) {
                if (isColumnFile) {
                    logEntry(`  OK ${entry.tableName}: ${data.loaded} columns loaded`, 'success');
                } else {
                    logEntry(`  OK ${entry.tableName}: ${data.loaded} stat rows loaded`, 'success');
                }
                successCount++;
            } else {
                logEntry(`  FAIL ${entry.name}: ${data.error || 'Unknown error'}`, 'error');
            }
        } catch (e) {
            logEntry(`  FAIL ${entry.name}: ${e.message}`, 'error');
        }
    }

    logEntry(`Done: ${successCount}/${selected.length} files loaded`, successCount === selected.length ? 'success' : 'error');

    await refreshDbStatus();
    fileEntries = [];
    document.getElementById('match-section').style.display = 'none';
    btn.textContent = 'Load Selected (0)';
    btn.disabled = true;
}

// ---------------------------------------------------------------------------
// Log
// ---------------------------------------------------------------------------
function logEntry(message, type = 'info') {
    const logEl = document.getElementById('run-log');
    const time = new Date().toLocaleTimeString();
    const cls = type === 'success' ? 'log-success' : type === 'error' ? 'log-error' : '';
    const entry = document.createElement('div');
    entry.className = 'log-entry';
    entry.innerHTML = `<span class="log-time">${time}</span> <span class="log-message ${cls}">${message}</span>`;
    logEl.appendChild(entry);
    logEl.scrollTop = logEl.scrollHeight;
}

// ---------------------------------------------------------------------------
// Toast
// ---------------------------------------------------------------------------
function showToast(message, type = 'success') {
    const el = document.createElement('div');
    const bg = type === 'success' ? 'var(--jp-success-color0)' : 'var(--jp-error-color0)';
    el.style.cssText = `
        position:fixed; top:20px; right:20px; z-index:2000;
        background:${bg}; color:white; padding:12px 20px;
        border-radius:6px; box-shadow:0 4px 12px rgba(0,0,0,0.3);
        font-size:13px;
    `;
    el.textContent = message;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 3000);
}
