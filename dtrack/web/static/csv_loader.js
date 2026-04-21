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
        function renderFileList() {
            const container = document.getElementById('file-list');
            if (!container) return;

            container.innerHTML = state.fileEntries.map((entry, idx) => {
                const dateRange = entry.minDate && entry.maxDate
                    ? `${entry.minDate} to ${entry.maxDate}` : 'no dates detected';
                const matchBadge = entry.matched
                    ? `<span class="status-badge ready">matched</span>`
                    : `<span class="status-badge warning">unmatched</span>`;
                const targetLine = entry.matched
                    ? `<span class="file-target">&rarr; ${entry.tableName} <span class="file-side">(${entry.side})</span></span>`
                    : buildDropdown(idx, getAvailableTableOptions(idx));

                return `
                <div class="file-match-row ${entry.selected ? 'selected' : ''}">
                    <input type="checkbox" ${entry.selected ? 'checked' : ''}
                        onchange="window.__csvLoader_${cfg.suffix}.toggleEntry(${idx}, this.checked)">
                    <div class="file-info">
                        <div class="file-name">
                            <span class="file-icon">&#128196;</span>
                            <span>${entry.name}</span>
                            ${matchBadge}
                        </div>
                        <div class="file-details">
                            ${targetLine}
                            <span class="file-meta">${(entry.rows || 0).toLocaleString()} rows</span>
                            <span class="file-meta">${dateRange}</span>
                        </div>
                    </div>
                    <button class="btn-text" onclick="window.__csvLoader_${cfg.suffix}.removeEntry(${idx})">&times;</button>
                </div>`;
            }).join('');

            const nSelected = state.fileEntries.filter(e => e.selected).length;
            const btn = document.getElementById('load-btn');
            if (btn) {
                btn.disabled = nSelected === 0;
                btn.textContent = `Load Selected (${nSelected})`;
            }
        }

        function buildDropdown(idx, options) {
            if (!options.length) return '<span class="file-meta">no free tables</span>';
            const opts = ['<option value="">-- pick target table --</option>']
                .concat(options.map(o => `<option value="${o.value}">${o.label}</option>`));
            return `<select class="file-target-select"
                onchange="window.__csvLoader_${cfg.suffix}.onTargetPicked(${idx}, this.value)">${opts.join('')}</select>`;
        }

        function onTargetPicked(idx, value) {
            if (!value) return;
            const [tableName, side, pairName] = value.split('|');
            const entry = state.fileEntries[idx];
            entry.tableName = tableName;
            entry.side = side;
            entry.pairName = pairName;
            entry.matched = true;
            entry.selected = true;
            renderFileList();
        }

        function toggleEntry(idx, checked) {
            state.fileEntries[idx].selected = checked;
            renderFileList();
        }

        function removeEntry(idx) {
            state.fileEntries.splice(idx, 1);
            renderFileList();
            if (!state.fileEntries.length) {
                document.getElementById('match-section').style.display = 'none';
            }
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
            if (!folder) return;
            const btn = document.getElementById('scan-folder-btn');
            btn.disabled = true; btn.textContent = 'Scanning...';
            try {
                const resp = await fetch(`/api/scan/folder?dir=${encodeURIComponent(folder)}`);
                const data = await resp.json();
                if (!resp.ok) { showToast(data.error || 'Scan failed', 'error'); return; }
                const files = (data.files || []).filter(f => SUFFIX_RE.test(f.name.replace(/\.csv$/i, '')));
                if (!files.length) { showToast(`No *_${cfg.suffix}.csv in folder`, 'error'); return; }
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
                showToast(e.message, 'error');
            } finally {
                btn.disabled = false; btn.textContent = 'Scan';
            }
        }

        // ── Load (upload or path-based) ──────────────────────────────────
        async function loadSelected() {
            const selected = state.fileEntries.filter(e => e.selected && e.matched);
            if (!selected.length) return;
            const logSection = document.getElementById('log-section');
            if (logSection) logSection.style.display = '';
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
                    }
                    const data = await resp.json();
                    if (!resp.ok) throw new Error(data.error || 'Upload failed');
                    logEntry(`  OK: ${JSON.stringify(data).slice(0, 160)}`);
                    successCount++;
                } catch (e) {
                    logEntry(`  ERROR: ${e.message}`, 'error');
                }
            }
            logEntry(`Done: ${successCount}/${selected.length} succeeded.`);
            if (cfg.afterLoad) await cfg.afterLoad();
        }

        function logEntry(msg, type) {
            const log = document.getElementById('run-log');
            if (!log) return;
            const div = document.createElement('div');
            div.className = 'log-entry' + (type === 'error' ? ' log-error' : '');
            div.textContent = `[${new Date().toLocaleTimeString()}] ${msg}`;
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
                dropZone.addEventListener('click', () => fileInput && fileInput.click());
                dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('dragover'); });
                dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
                dropZone.addEventListener('drop', e => {
                    e.preventDefault();
                    dropZone.classList.remove('dragover');
                    processFiles(Array.from(e.dataTransfer.files).filter(f => f.name.endsWith('.csv')));
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

    global.createCsvLoader = createCsvLoader;
})(window);
