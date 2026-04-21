// csv_loader.js — shared drop-zone + scan + auto-match + load UI used by
// load_row, load_col, and col_mapping pages. Each page instantiates it with
// a suffix-specific config (what to match, where to POST) and gets the same
// UX and DOM contract (ids: folder-path, scan-folder-btn, drop-zone,
// file-input, browse-btn, match-section, file-list, load-btn, run-log,
// log-section, clear-log-btn, drop-more-btn).

(function (global) {
    'use strict';

    function createCsvLoader(cfg) {
        /*
         * cfg = {
         *   suffix:          'row' | 'col' | 'columns',
         *   uploadEndpoint:  URL for multipart upload (POST, field: file, table_name, mode?, source?)
         *   pathEndpoint:    URL for server-side path load (POST json: {path, table_name, mode?, source?})
         *   loadVerb:        noun shown in the UI & log (e.g., 'row counts', 'columns', 'col stats')
         *   extraFormFields: (entry, knownTables) -> {source?} — optional per-entry form extras
         *   extraBodyFields: (entry, knownTables) -> {source?} — optional per-entry JSON body extras
         *   afterLoad:       async () => void — called once after all loads finish (e.g. refresh DB summary)
         *   knownTables:     ref holder {current: []} so the host page can refresh the list
         * }
         */
        const state = {
            fileEntries: [],
            knownTables: cfg.knownTables || { current: [] },
        };
        const SUFFIX_RE = new RegExp(`_${cfg.suffix}$`, 'i');

        // ── Auto-match filename to known pair qname, suffix-locked ───────
        function autoMatch(filename) {
            const noMatch = { matched: false, tableName: '', side: '', pairName: '' };
            const stripped = filename.replace(/\.csv$/i, '');
            if (!SUFFIX_RE.test(stripped)) return noMatch;
            const stem = stripped.replace(SUFFIX_RE, '');

            for (const pair of state.knownTables.current) {
                if (stem.toLowerCase() === (pair.table_left || '').toLowerCase()) {
                    return { matched: true, tableName: pair.table_left, side: 'left', pairName: pair.pair_name };
                }
                if (stem.toLowerCase() === (pair.table_right || '').toLowerCase()) {
                    return { matched: true, tableName: pair.table_right, side: 'right', pairName: pair.pair_name };
                }
            }
            for (const pair of state.knownTables.current) {
                if (stem.toLowerCase().startsWith((pair.table_left || '').toLowerCase())) {
                    return { matched: true, tableName: pair.table_left, side: 'left', pairName: pair.pair_name };
                }
                if (stem.toLowerCase().startsWith((pair.table_right || '').toLowerCase())) {
                    return { matched: true, tableName: pair.table_right, side: 'right', pairName: pair.pair_name };
                }
            }
            return noMatch;
        }

        function getAvailableTableOptions(currentIdx) {
            const taken = new Set();
            state.fileEntries.forEach((e, idx) => {
                if (idx !== currentIdx && e.tableName) taken.add(e.tableName.toLowerCase());
            });
            const opts = [];
            for (const pair of state.knownTables.current) {
                if (pair.table_left && !taken.has(pair.table_left.toLowerCase())) {
                    opts.push({ value: `${pair.table_left}|left|${pair.pair_name}`, label: `${pair.table_left} (${pair.pair_name} left)` });
                }
                if (pair.table_right && !taken.has(pair.table_right.toLowerCase())) {
                    opts.push({ value: `${pair.table_right}|right|${pair.pair_name}`, label: `${pair.table_right} (${pair.pair_name} right)` });
                }
            }
            return opts;
        }

        function parseCSV(file) {
            return new Promise(resolve => {
                const reader = new FileReader();
                reader.onload = e => {
                    const lines = e.target.result.split('\n').filter(l => l.trim());
                    if (lines.length < 2) { resolve({ rows: 0, minDate: '', maxDate: '' }); return; }
                    const header = lines[0].split(',').map(h => h.trim().toLowerCase());
                    const dateIdx = header.findIndex(h =>
                        ['dt', 'date', 'rpg_dt', 'eff_dt', 'run_date', 'snap_dt', 'snapshot_dt', 'date_value'].includes(h)
                    );
                    const rows = lines.length - 1;
                    let minDate = '', maxDate = '';
                    if (dateIdx >= 0) {
                        const dates = [];
                        for (let i = 1; i < lines.length; i++) {
                            const cols = lines[i].split(',');
                            if (cols[dateIdx]) dates.push(cols[dateIdx].trim());
                        }
                        if (dates.length) { dates.sort(); minDate = dates[0]; maxDate = dates[dates.length - 1]; }
                    }
                    resolve({ rows, minDate, maxDate });
                };
                reader.readAsText(file);
            });
        }

        // ── Rendering ─────────────────────────────────────────────────────
        // DOM matches the existing .file-card / .file-card-header / .file-card-body
        // styles in style_v2.css so all three pages look identical.
        function renderFileList() {
            const container = document.getElementById('file-list');
            if (!container) return;
            const globalKey = `__csvLoader_${cfg.suffix}`;

            container.innerHTML = state.fileEntries.map((entry, idx) => {
                const dateRange = entry.minDate && entry.maxDate
                    ? `${entry.minDate} to ${entry.maxDate}` : 'no dates detected';
                const matchBadge = entry.matched
                    ? `<span class="status-badge ready">matched</span>`
                    : `<span class="status-badge warning">unmatched</span>`;
                const targetLine = entry.matched
                    ? `<span class="file-target">&rarr; ${entry.tableName} <span class="file-side">(${entry.side})</span></span>`
                    : buildDropdown(idx, getAvailableTableOptions(idx));
                const disabled = !entry.matched && !entry.tableName ? 'disabled' : '';

                return `
                <div class="file-card ${entry.matched ? '' : 'unmatched'}">
                    <div class="file-card-header">
                        <label class="file-checkbox">
                            <input type="checkbox" ${entry.selected ? 'checked' : ''} ${disabled}
                                onchange="window.${globalKey}.toggleEntry(${idx}, this.checked)">
                        </label>
                        <span class="file-name">${entry.name}</span>
                        ${matchBadge}
                        <button class="btn-text btn-remove" title="Remove"
                            onclick="window.${globalKey}.removeEntry(${idx})">&times;</button>
                    </div>
                    <div class="file-card-body">
                        ${targetLine}
                        <span class="file-meta">${(entry.rows || 0).toLocaleString()} rows &middot; ${dateRange}</span>
                    </div>
                </div>`;
            }).join('');

            updateLoadButton();
        }

        function buildDropdown(idx, options) {
            const globalKey = `__csvLoader_${cfg.suffix}`;
            const opts = ['<option value="">-- select table --</option>']
                .concat(options.map(o => `<option value="${o.value}">${o.label}</option>`));
            return `<select class="manual-match-select"
                onchange="window.${globalKey}.onTargetPicked(${idx}, this.value)">${opts.join('')}</select>`;
        }

        function onTargetPicked(idx, value) {
            const entry = state.fileEntries[idx];
            if (!value) {
                entry.matched = false; entry.tableName = ''; entry.side = '';
                entry.pairName = ''; entry.selected = false;
            } else {
                const [tableName, side, pairName] = value.split('|');
                entry.matched = true; entry.tableName = tableName; entry.side = side;
                entry.pairName = pairName; entry.selected = true;
            }
            renderFileList();
        }

        function toggleEntry(idx, checked) {
            state.fileEntries[idx].selected = checked;
            updateLoadButton();
        }

        function removeEntry(idx) {
            state.fileEntries.splice(idx, 1);
            if (!state.fileEntries.length) {
                document.getElementById('match-section').style.display = 'none';
            } else {
                renderFileList();
            }
        }

        function updateLoadButton() {
            const n = state.fileEntries.filter(e => e.selected && e.tableName).length;
            const btn = document.getElementById('load-btn');
            if (!btn) return;
            btn.disabled = n === 0;
            btn.textContent = `Load Selected (${n})`;
        }

        async function processFiles(files) {
            for (const file of files) {
                if (state.fileEntries.some(e => e.name === file.name)) continue;
                const { rows, minDate, maxDate } = await parseCSV(file);
                const match = autoMatch(file.name);
                state.fileEntries.push({
                    file, name: file.name, rows, minDate, maxDate,
                    matched: match.matched, tableName: match.tableName,
                    side: match.side, pairName: match.pairName, selected: match.matched,
                });
            }
            renderFileList();
            document.getElementById('match-section').style.display = '';
        }

        async function scanFolder() {
            const folder = document.getElementById('folder-path').value.trim();
            console.log(`[csv_loader:${cfg.suffix}] scan clicked, folder="${folder}"`);
            if (!folder) { showToast('Folder path is empty', 'error'); return; }
            const btn = document.getElementById('scan-folder-btn');
            btn.disabled = true; btn.textContent = 'Scanning...';
            try {
                const resp = await fetch(`/api/scan/folder?dir=${encodeURIComponent(folder)}`);
                const data = await resp.json();
                console.log(`[csv_loader:${cfg.suffix}] /api/scan/folder ->`, data);
                if (!resp.ok) { showToast(data.error || 'Scan failed', 'error'); return; }
                const files = (data.files || []).filter(f => SUFFIX_RE.test(f.name.replace(/\.csv$/i, '')));
                if (!files.length) {
                    const total = (data.files || []).length;
                    showToast(`No *_${cfg.suffix}.csv in folder` +
                              (total ? ` (${total} other CSV seen, suffix mismatch)` : ''), 'error');
                    return;
                }
                for (const f of files) {
                    if (state.fileEntries.some(e => e.name === f.name)) continue;
                    const match = autoMatch(f.name);
                    state.fileEntries.push({
                        file: null, serverPath: f.path, name: f.name,
                        rows: f.rows, minDate: f.minDate, maxDate: f.maxDate,
                        matched: match.matched, tableName: match.tableName,
                        side: match.side, pairName: match.pairName, selected: match.matched,
                    });
                }
                renderFileList();
                document.getElementById('match-section').style.display = '';
                showToast(`Found ${files.length} *_${cfg.suffix}.csv`);
            } catch (e) {
                console.error(`[csv_loader:${cfg.suffix}] scan error:`, e);
                showToast(e.message, 'error');
            } finally {
                btn.disabled = false; btn.textContent = 'Scan';
            }
        }

        // ── Load (upload or path-based) ──────────────────────────────────
        async function loadSelected() {
            const selected = state.fileEntries.filter(e => e.selected && e.tableName);
            if (!selected.length) return;

            const btn = document.getElementById('load-btn');
            if (btn) { btn.disabled = true; btn.textContent = 'Loading...'; }
            const logSection = document.getElementById('log-section');
            const logEl = document.getElementById('run-log');
            if (logSection) logSection.style.display = '';
            if (logEl) logEl.innerHTML = '';
            logEntry(`Loading ${selected.length} file(s)...`);

            let successCount = 0;
            for (const entry of selected) {
                logEntry(`Loading ${entry.name} -> ${entry.tableName} (${entry.side})...`);
                try {
                    let resp;
                    if (entry.file) {
                        const form = new FormData();
                        form.append('file', entry.file);
                        form.append('table_name', entry.tableName);
                        form.append('mode', 'upsert');
                        const extras = (cfg.extraFormFields || (() => ({})))(entry, state.knownTables.current);
                        for (const [k, v] of Object.entries(extras)) form.append(k, v);
                        resp = await fetch(cfg.uploadEndpoint, { method: 'POST', body: form });
                    } else if (entry.serverPath) {
                        const body = { path: entry.serverPath, table_name: entry.tableName, mode: 'upsert' };
                        const extras = (cfg.extraBodyFields || (() => ({})))(entry, state.knownTables.current);
                        Object.assign(body, extras);
                        resp = await fetch(cfg.pathEndpoint, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(body),
                        });
                    } else {
                        logEntry(`  SKIP ${entry.name}: no file or server path`, 'error');
                        continue;
                    }
                    const data = await resp.json();
                    if (!data.ok) {
                        logEntry(`  FAIL ${entry.name}: ${data.error || 'unknown error'}`, 'error');
                        continue;
                    }
                    const msg = cfg.formatLoadResult
                        ? cfg.formatLoadResult(data, entry)
                        : `${entry.tableName}: loaded`;
                    logEntry(`  OK ${msg}`, 'success');
                    successCount++;
                } catch (e) {
                    logEntry(`  FAIL ${entry.name}: ${e.message}`, 'error');
                }
            }
            logEntry(`Done: ${successCount}/${selected.length} files loaded`,
                     successCount === selected.length ? 'success' : 'error');

            if (cfg.afterLoad) await cfg.afterLoad();

            // Clear the match list now that the run is done
            state.fileEntries = [];
            const matchSection = document.getElementById('match-section');
            if (matchSection) matchSection.style.display = 'none';
            if (btn) { btn.textContent = 'Load Selected (0)'; btn.disabled = true; }
        }

        function logEntry(msg, type) {
            const log = document.getElementById('run-log');
            if (!log) return;
            const time = new Date().toLocaleTimeString();
            const cls = type === 'success' ? 'log-success' : type === 'error' ? 'log-error' : '';
            const div = document.createElement('div');
            div.className = 'log-entry';
            div.innerHTML = `<span class="log-time">${time}</span> <span class="log-message ${cls}">${msg}</span>`;
            log.appendChild(div);
            log.scrollTop = log.scrollHeight;
        }

        function showToast(msg, kind) {
            const el = document.createElement('div');
            el.textContent = msg;
            el.style.cssText = `position:fixed; top:20px; right:20px; padding:10px 16px;
                background:${kind === 'error' ? '#fbe9e7' : '#e8f5e9'};
                color:${kind === 'error' ? '#c62828' : '#2e7d32'};
                border:1px solid ${kind === 'error' ? '#ef9a9a' : '#a5d6a7'};
                border-radius:6px; z-index:2000; font-size:13px;`;
            document.body.appendChild(el);
            setTimeout(() => el.remove(), 3000);
        }

        // ── Init: bind DOM events ────────────────────────────────────────
        function init() {
            const dropZone = document.getElementById('drop-zone');
            const fileInput = document.getElementById('file-input');
            const browseBtn = document.getElementById('browse-btn');
            const scanBtn = document.getElementById('scan-folder-btn');
            const loadBtn = document.getElementById('load-btn');
            const dropMore = document.getElementById('drop-more-btn');
            const clearLog = document.getElementById('clear-log-btn');

            if (dropZone) {
                dropZone.addEventListener('click', e => {
                    if (e.target === dropZone || (e.target.closest('.drop-zone-content') && e.target.tagName !== 'BUTTON')) {
                        if (fileInput) fileInput.click();
                    }
                });
                dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('drag-over'); });
                dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));
                dropZone.addEventListener('drop', e => {
                    e.preventDefault();
                    dropZone.classList.remove('drag-over');
                    const csv = Array.from(e.dataTransfer.files).filter(f => f.name.endsWith('.csv'));
                    if (csv.length) processFiles(csv);
                });
            }
            if (browseBtn && fileInput) {
                browseBtn.addEventListener('click', e => { e.stopPropagation(); fileInput.click(); });
                fileInput.addEventListener('change', () => {
                    processFiles(Array.from(fileInput.files));
                    fileInput.value = '';
                });
            }
            if (dropMore && fileInput) dropMore.addEventListener('click', () => fileInput.click());
            if (loadBtn) loadBtn.addEventListener('click', loadSelected);
            if (scanBtn) scanBtn.addEventListener('click', scanFolder);
            if (clearLog) clearLog.addEventListener('click', () => {
                document.getElementById('run-log').innerHTML = '';
                document.getElementById('log-section').style.display = 'none';
            });
        }

        const api = {
            init, state, processFiles, scanFolder, renderFileList,
            toggleEntry, removeEntry, onTargetPicked,
        };
        global[`__csvLoader_${cfg.suffix}`] = api;
        return api;
    }

    // Shared helper — every page auto-matches against the same union of
    // DB-registered and config-only pairs, so a pair that's in dtrack.json
    // but not yet in _table_pairs still resolves.
    async function loadKnownTables() {
        const tables = [];
        try {
            const resp = await fetch('/api/status');
            const data = await resp.json();
            for (const p of (data.pairs || [])) {
                tables.push({
                    pair_name: p.pair_name,
                    table_left: p.table_left,
                    table_right: p.table_right,
                    source_left: p.source_left || '',
                    source_right: p.source_right || '',
                });
            }
        } catch (e) { console.error('Failed to load /api/status:', e); }

        try {
            const cfgResp = await fetch('/api/pairs/list');
            const cfgData = await cfgResp.json();
            const existing = new Set(tables.map(t => t.pair_name));
            for (const p of (cfgData.pairs || [])) {
                if (existing.has(p.name)) continue;
                const leftSource = p.left?.source || '';
                const rightSource = p.right?.source || '';
                // Match backend _derive_side_name: name from `table`, fallback to pair_name
                const deriveName = (sideCfg) => {
                    const t = (sideCfg?.table || '').trim();
                    if (!t) return p.name;
                    return t.toLowerCase().replace(/[^a-z0-9_]+/g, '_').replace(/^_+|_+$/g, '') || p.name;
                };
                const leftName = deriveName(p.left);
                const rightName = deriveName(p.right);
                tables.push({
                    pair_name: p.name,
                    table_left: leftSource ? `${leftSource}_${leftName}` : leftName,
                    table_right: rightSource ? `${rightSource}_${rightName}` : rightName,
                    source_left: leftSource,
                    source_right: rightSource,
                });
            }
        } catch (e) { console.error('Failed to load /api/pairs/list:', e); }

        return tables;
    }

    global.createCsvLoader = createCsvLoader;
    global.loadKnownTables = loadKnownTables;
})(window);
