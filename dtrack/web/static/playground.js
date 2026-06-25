/* playground.js — ad-hoc SQL runner with timing + history */

// Cache of history SQL keyed by run id, populated on each pgLoadHistory.
// Lets the per-row Copy button avoid stuffing multi-line SQL into HTML attrs
// (which would need quote-safe escaping).
const _pgHistorySqlById = {};

async function pgCopyHistorySql(id, btn) {
    const sql = _pgHistorySqlById[id] || '';
    if (!sql) return;
    try {
        await navigator.clipboard.writeText(sql);
    } catch (e) {
        // Fallback for non-HTTPS / older browsers.
        const ta = document.createElement('textarea');
        ta.value = sql;
        ta.style.position = 'fixed'; ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select(); document.execCommand('copy');
        document.body.removeChild(ta);
    }
    if (!btn) return;
    const orig = btn.textContent;
    btn.textContent = '✓';
    setTimeout(() => { btn.textContent = orig; }, 900);
}

let _pgLastResult = null;
let _pgHadoopProfiles = {libname: {}, procsql: {}};
let _pgConnMacros = {oracle: [], aws: [], hadoop: [], databricks: []};

// Form fields we persist across page reloads / server restarts. Values are
// stored under `dtrack-playground:<id>` in localStorage; saved on every
// input/change event, restored on DOMContentLoaded after pgLoadConstants
// has populated the dynamic dropdowns.
const _PG_PERSIST_IDS = [
    'pg-conn', 'pg-row-cap', 'pg-use-cache', 'pg-sql',
    'pg-sas-kind', 'pg-sas-method', 'pg-sas-conn',
    'pg-sas-profile',
    'pg-sas-server', 'pg-sas-host', 'pg-sas-schema', 'pg-sas-port',
    'pg-sas-jar-path', 'pg-sas-config-path',
];
const _PG_LS_PREFIX = 'dtrack-playground:';

function _pgSaveField(id) {
    const el = document.getElementById(id);
    if (!el) return;
    const v = (el.type === 'checkbox') ? (el.checked ? '1' : '0') : el.value;
    try { localStorage.setItem(_PG_LS_PREFIX + id, v); } catch (e) {}
}

function _pgRestoreField(id) {
    const el = document.getElementById(id);
    if (!el) return;
    let v;
    try { v = localStorage.getItem(_PG_LS_PREFIX + id); } catch (e) { return; }
    if (v === null) return;
    if (el.type === 'checkbox') { el.checked = (v === '1'); return; }
    // For <select>, only restore if the option exists (set by constants load).
    if (el.tagName === 'SELECT' && !Array.from(el.options).some(o => o.value === v)) return;
    el.value = v;
}

function _pgRestoreAll() {
    for (const id of _PG_PERSIST_IDS) _pgRestoreField(id);
    // Engine radio is per-radio, not single id.
    let savedEngine;
    try { savedEngine = localStorage.getItem(_PG_LS_PREFIX + 'pg-engine'); } catch (e) {}
    if (savedEngine) {
        const radio = document.querySelector(`input[name="pg-engine"][value="${savedEngine}"]`);
        if (radio) radio.checked = true;
    }
}

function _pgWirePersistence() {
    for (const id of _PG_PERSIST_IDS) {
        const el = document.getElementById(id);
        if (!el) continue;
        const evt = (el.tagName === 'SELECT' || el.type === 'number' || el.type === 'checkbox') ? 'change' : 'input';
        el.addEventListener(evt, () => _pgSaveField(id));
    }
    document.querySelectorAll('input[name="pg-engine"]').forEach(r => {
        r.addEventListener('change', () => {
            try { localStorage.setItem(_PG_LS_PREFIX + 'pg-engine', _pgEngine()); } catch (e) {}
        });
    });
}

document.addEventListener('DOMContentLoaded', async () => {
    pgLoadHistory();
    await pgLoadConstants();   // populates Hadoop profile + Oracle macro selects
    _pgRestoreAll();           // restore saved values (after dropdowns are populated)
    pgEngineChanged();         // refresh visible blocks for restored engine
    _pgWirePersistence();
});

async function pgLoadConstants() {
    try {
        const res = await fetch('/api/constants');
        const data = await res.json();
        _pgHadoopProfiles.libname = data.HADOOP_LIBNAME_PROFILES || {};
        _pgHadoopProfiles.procsql = data.HADOOP_PROCSQL_PROFILES || {};
        _pgConnMacros = data.CONNECTION_MACROS || {oracle: [], aws: [], hadoop: [], databricks: []};
        pgSasRepopulateProfiles();

        // SAS-side Oracle macro dropdown (kept as <select>; macros are fixed).
        const sel = document.getElementById('pg-sas-conn');
        const ora = _pgConnMacros.oracle || [];
        if (sel) {
            sel.innerHTML = ora.map(m => `<option value="${m}">${m}</option>`).join('');
            if (ora.length) sel.value = ora[0];
        }
        // Main conn datalist (editable with suggestions) — populated by
        // pgEngineChanged based on whichever engine is currently selected.
        pgRepopulateConnDatalist();
    } catch (e) {
        console.error('pgLoadConstants failed:', e);
    }
}

function pgRepopulateConnDatalist() {
    const dl = document.getElementById('pg-conn-list');
    if (!dl) return;
    const eng = _pgEngine();
    const key = eng === 'athena' ? 'aws'
              : eng === 'databricks' ? 'databricks'
              : eng === 'oracle' ? 'oracle'
              : eng === 'hadoop' ? 'hadoop'
              : null;
    const macros = key ? (_pgConnMacros[key] || []) : [];
    dl.innerHTML = macros.map(m => `<option value="${m}">`).join('');
}

function pgSasRepopulateProfiles() {
    // Refill the Hadoop profile dropdown with the set matching the current
    // method (libname vs procsql have different profile catalogs because
    // the two statements take different params on this user's box).
    const sel = document.getElementById('pg-sas-profile');
    if (!sel) return;
    const method = document.getElementById('pg-sas-method').value;
    const profiles = _pgHadoopProfiles[method] || {};
    sel.innerHTML = '<option value="">(custom)</option>';
    for (const name of Object.keys(profiles)) {
        const opt = document.createElement('option');
        opt.value = name;
        opt.textContent = name;
        sel.appendChild(opt);
    }
    const first = Object.keys(profiles)[0];
    if (first) {
        sel.value = first;
        pgSasProfileChanged();
    }
}

function pgSasProfileChanged() {
    const method = document.getElementById('pg-sas-method').value;
    const name = document.getElementById('pg-sas-profile').value;
    const p = (_pgHadoopProfiles[method] || {})[name];
    if (!p) return;
    if (p.server      != null) document.getElementById('pg-sas-server').value      = p.server;
    if (p.host        != null) document.getElementById('pg-sas-host').value        = p.host;
    if (p.schema      != null) document.getElementById('pg-sas-schema').value      = p.schema;
    if (p.port        != null) document.getElementById('pg-sas-port').value        = p.port;
    if (p.jar_path    != null) document.getElementById('pg-sas-jar-path').value    = p.jar_path;
    if (p.config_path != null) document.getElementById('pg-sas-config-path').value = p.config_path;
}

function _pgEsc(s) {
    if (s == null) return '';
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function _pgEngine() {
    return document.querySelector('input[name="pg-engine"]:checked').value;
}

function pgEngineChanged() {
    const eng = _pgEngine();
    const isSAS = eng === 'sas';
    document.getElementById('pg-sas-controls').style.display = isSAS ? 'flex' : 'none';
    document.getElementById('pg-download-btn').style.display = 'none';

    // Athena/Oracle still use the single conn input. SAS hides it because
    // its connection details live in the SAS controls block below.
    const connRow = document.getElementById('pg-conn').parentElement.parentElement;
    connRow.style.display = isSAS ? 'none' : '';

    const sqlBox = document.getElementById('pg-sql');
    if (isSAS) {
        sqlBox.placeholder = "SAS code body — wrapped between scaffold lines.\n" +
            "  LIBNAME mode: write SAS code (libref `ora` or `hdp` is mounted).\n" +
            "  PROC SQL CONNECT: write native passthrough SQL.";
        pgSasKindChanged();
    } else {
        const label = document.getElementById('pg-conn-label');
        const conn = document.getElementById('pg-conn');
        // Refill the conn datalist from CONNECTION_MACROS for the new engine.
        pgRepopulateConnDatalist();
        if (eng === 'athena') {
            label.textContent = 'Athena database (optional)';
            conn.placeholder = 'my_database';
            sqlBox.placeholder = 'SELECT COUNT(*) FROM trades WHERE trade_date = DATE \'2026-05-01\'\n' +
                '\n' +
                '-- Tip: for column names that start with a digit or contain\n' +
                '-- special chars, use double quotes — e.g. SELECT "123_col" FROM t.\n' +
                '-- Single quotes \'\' make the value a string literal, not an identifier.';
        } else if (eng === 'databricks') {
            label.textContent = 'Databricks catalog.schema (optional)';
            conn.placeholder = 'my_catalog.my_schema';
            sqlBox.placeholder = "SELECT COUNT(*) FROM catalog.schema.trades WHERE trade_date = DATE '2026-05-01'\n" +
                '\n-- Auth via DB_PROFILE + DB_HTTPS env vars (.env).\n' +
                '-- Three-part table refs: catalog.schema.table.';
        } else if (eng === 'hadoop') {
            label.textContent = 'Hive ODBC DSN (default Hive_Prod)';
            conn.placeholder = 'Hive_Prod';
            sqlBox.placeholder = "SELECT COUNT(*) FROM schema.tbl WHERE trade_date >= '2026-05-01'\n" +
                "\n-- Connects via pyodbc.connect('DSN=<name>', autocommit=True).\n" +
                "-- SSL_CERT_FILE is temporarily unset around the connect call.";
        } else if (eng === 'csv') {
            label.textContent = 'CSV file path (conn_macro)';
            conn.placeholder = '/path/to/file.csv';
            sqlBox.placeholder = "SELECT COUNT(*) FROM data WHERE amt > 0\n" +
                "\n-- DuckDB runs in-process on the SERVER. The CSV path above is\n" +
                "-- registered as the view `data`; you can also write\n" +
                "--   SELECT * FROM '/path/to/file.csv'  (read_csv_auto).\n" +
                "-- Only the capped result rows reach the browser (no in-browser load).";
        } else {
            label.textContent = 'Oracle macro (e.g. pb23)';
            conn.placeholder = 'pb23';
            sqlBox.placeholder = "SELECT COUNT(*) FROM owner.tbl WHERE trade_date >= DATE '2026-05-01'";
        }
    }
}

function pgSasKindChanged() {
    const kind = document.getElementById('pg-sas-kind').value;
    document.getElementById('pg-sas-oracle-inline').style.display = kind === 'oracle' ? 'flex' : 'none';
    document.getElementById('pg-sas-hadoop-inline').style.display = kind === 'hadoop' ? 'flex' : 'none';
    document.getElementById('pg-sas-hadoop').style.display        = kind === 'hadoop' ? 'flex' : 'none';
}

async function pgRun() {
    const engine = _pgEngine();
    const sql = document.getElementById('pg-sql').value;
    const btn = document.getElementById('pg-run-btn');
    document.getElementById('pg-download-btn').style.display = 'none';

    if (engine === 'sas') return pgRunSAS();

    if (!sql.trim()) { _pgSetStatus('SQL is empty.', 'error'); return; }
    const conn = document.getElementById('pg-conn').value.trim();
    if (engine === 'oracle' && !conn) { _pgSetStatus('Oracle requires a connection macro.', 'error'); return; }
    if (engine === 'csv' && !conn) { _pgSetStatus('CSV requires a file path.', 'error'); return; }

    const rowCap = parseInt(document.getElementById('pg-row-cap').value || '20', 10);
    const useCache = document.getElementById('pg-use-cache').checked;
    _pgSetStatus('Running…');
    btn.disabled = true;
    document.getElementById('pg-cancel-btn').style.display = '';

    try {
        const res = await fetch('/api/playground/run', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({engine, sql: sql.trim(), conn, row_cap: rowCap, use_cache: useCache}),
        });
        const data = await res.json();
        if (data && data.cancelled) {
            _pgSetStatus(`✗ cancelled after ${data.wall_sec ?? '?'}s`, 'error');
            _pgRenderError('Cancelled by user.');
        } else if (!res.ok || !data.ok) {
            _pgSetStatus(`Error: ${data.error || res.status}`, 'error');
            _pgRenderError(data.error || `HTTP ${res.status}`);
        } else {
            _pgLastResult = data;
            const tag = data.cached ? ' · cached' : '';
            _pgSetStatus(`✓ ${data.n_rows_total} row${data.n_rows_total===1?'':'s'} · ${data.elapsed_sec}s${tag}`, 'ok');
            _pgRenderResult(data);
        }
    } catch (e) {
        _pgSetStatus(`Network error: ${e.message}`, 'error');
        _pgRenderError(e.message);
    } finally {
        btn.disabled = false;
        document.getElementById('pg-cancel-btn').style.display = 'none';
        pgLoadHistory();
    }
}

async function pgCancel() {
    _pgSetStatus('Cancelling…');
    try {
        const res = await fetch('/api/playground/cancel_running', {method: 'POST'});
        const data = await res.json();
        const ids = (data && data.cancelled_ids) || [];
        if (ids.length) {
            _pgSetStatus(`Cancel signalled for #${ids.join(', #')}`, 'error');
        } else {
            _pgSetStatus('Nothing was running.', 'error');
        }
    } catch (e) {
        _pgSetStatus(`Cancel failed: ${e.message}`, 'error');
    }
}

let _pgLastSAS = null;

async function pgRunSAS() {
    const kind   = document.getElementById('pg-sas-kind').value;
    const method = document.getElementById('pg-sas-method').value;
    const sql    = document.getElementById('pg-sql').value;

    const reqBody = {kind, method, sql};
    if (kind === 'oracle') {
        reqBody.conn_macro = document.getElementById('pg-sas-conn').value.trim();
        if (!reqBody.conn_macro) { _pgSetStatus('Oracle needs a macro (e.g. pb23).', 'error'); return; }
    } else {
        reqBody.server      = document.getElementById('pg-sas-server').value.trim();
        reqBody.host        = document.getElementById('pg-sas-host').value.trim();
        reqBody.schema      = document.getElementById('pg-sas-schema').value.trim();
        reqBody.port        = parseInt(document.getElementById('pg-sas-port').value || '10000', 10);
        reqBody.jar_path    = document.getElementById('pg-sas-jar-path').value.trim() || undefined;
        reqBody.config_path = document.getElementById('pg-sas-config-path').value.trim() || undefined;
        for (const f of ['server', 'host', 'schema']) {
            if (!reqBody[f]) { _pgSetStatus(`Hadoop needs \`${f}\`.`, 'error'); return; }
        }
    }

    _pgSetStatus('Generating SAS…');
    const btn = document.getElementById('pg-run-btn');
    btn.disabled = true;

    try {
        const res = await fetch('/api/playground/sas', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(reqBody),
        });
        const data = await res.json();
        if (!res.ok || !data.ok) {
            _pgSetStatus(`Error: ${data.error || res.status}`, 'error');
            _pgRenderError(data.error || `HTTP ${res.status}`);
        } else {
            _pgLastSAS = data;
            _pgSetStatus(`✓ ${data.filename} (${data.content.length.toLocaleString()} chars)`, 'ok');
            _pgRenderSAS(data);
            document.getElementById('pg-download-btn').style.display = '';
        }
    } catch (e) {
        _pgSetStatus(`Network error: ${e.message}`, 'error');
        _pgRenderError(e.message);
    } finally {
        btn.disabled = false;
        pgLoadHistory();
    }
}

function pgDownloadSAS() {
    if (!_pgLastSAS) return;
    _pgDownloadBlob(_pgLastSAS.content, _pgLastSAS.filename, 'text/plain');
}

function _pgRenderSAS(data) {
    const card = document.getElementById('pg-result-card');
    card.style.display = '';
    document.getElementById('pg-result-meta').textContent =
        `${data.filename} · ${data.content.length.toLocaleString()} chars · fill in %let oracle_usr / oracle_pwd, then submit in SAS`;
    document.getElementById('pg-result').innerHTML =
        `<pre style="font-family:var(--jp-code-font-family,monospace); font-size:12px; white-space:pre-wrap; margin:0;">${_pgEsc(data.content)}</pre>`;
}

function _pgSetStatus(text, kind) {
    const el = document.getElementById('pg-status');
    if (!el) return;
    el.textContent = text;
    el.style.color = kind === 'error' ? 'var(--jp-error-color0)'
                   : kind === 'ok'    ? 'var(--jp-success-color0)'
                   : 'var(--jp-ui-font-color2)';
}

function _pgRenderResult(data) {
    const card = document.getElementById('pg-result-card');
    card.style.display = '';
    const meta = document.getElementById('pg-result-meta');

    const parts = [
        `${data.n_rows_total} row${data.n_rows_total===1?'':'s'}`,
        `${data.columns.length} col${data.columns.length===1?'':'s'}`,
    ];
    if (data.engine_sec != null) {
        parts.push(`engine ${data.engine_sec}s`);
        if (data.total_sec != null && data.total_sec !== data.engine_sec) {
            parts.push(`total ${data.total_sec}s`);
        }
        parts.push(`wall ${data.wall_sec}s`);
    } else {
        parts.push(`wall ${data.wall_sec ?? data.elapsed_sec}s`);
    }
    if (data.data_scanned_bytes != null) {
        parts.push(`scanned ${_pgFmtBytes(data.data_scanned_bytes)}`);
    }
    if (data.query_id) {
        parts.push(`query_id <code>${_pgEsc(data.query_id)}</code>`);
    }
    parts.push(`showing first ${data.n_rows_returned}`);
    if (data.cached) parts.push('<span style="color:var(--jp-success-color0);">cached</span>');
    meta.innerHTML = parts.join(' · ');

    const cols = data.columns || [];
    const rows = data.rows || [];
    if (!cols.length) {
        document.getElementById('pg-result').innerHTML = '<div class="empty-message">No columns returned.</div>';
        return;
    }
    const head = '<thead><tr>' + cols.map(c => `<th>${_pgEsc(c)}</th>`).join('') + '</tr></thead>';
    const body = '<tbody>' + rows.map(r =>
        '<tr>' + r.map(v => `<td>${_pgEsc(v)}</td>`).join('') + '</tr>'
    ).join('') + '</tbody>';
    document.getElementById('pg-result').innerHTML = `<table class="data-table compact">${head}${body}</table>`;
}

function _pgRenderError(msg) {
    const card = document.getElementById('pg-result-card');
    card.style.display = '';
    document.getElementById('pg-result-meta').textContent = '';
    document.getElementById('pg-result').innerHTML =
        `<pre style="color:var(--jp-error-color0); white-space:pre-wrap;">${_pgEsc(msg)}</pre>`;
}

function pgDownloadResultCSV() {
    if (!_pgLastResult || !_pgLastResult.cache_key) {
        _pgSetStatus('No result to download.', 'error');
        return;
    }
    // Stream from server so we never hold the full result in JS memory —
    // browser writes straight to disk via native download.
    const a = document.createElement('a');
    a.href = `/api/playground/download/${_pgLastResult.cache_key}`;
    a.download = '';
    document.body.appendChild(a);
    a.click();
    a.remove();
}

function _pgFmtBytes(n) {
    if (n == null) return '—';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let v = Number(n), i = 0;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
    return `${v.toFixed(v >= 100 ? 0 : v >= 10 ? 1 : 2)} ${units[i]}`;
}

function _pgDownloadBlob(content, filename, mime) {
    const blob = new Blob([content], {type: mime || 'application/octet-stream'});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
}

// ---------------------------------------------------------------------------
// History
// ---------------------------------------------------------------------------
async function pgLoadHistory() {
    const el = document.getElementById('pg-history');
    try {
        const res = await fetch('/api/playground/history');
        const data = await res.json();
        const runs = data.runs || [];
        if (!runs.length) {
            el.innerHTML = '<div class="empty-message">No runs yet. Submit a query above.</div>';
            return;
        }
        const head = `<thead><tr>
            <th style="width:40px;">#</th>
            <th style="width:60px;">engine</th>
            <th style="width:50px;">status</th>
            <th style="width:120px;">conn</th>
            <th style="width:160px;">when (UTC)</th>
            <th style="width:80px; text-align:right;">elapsed</th>
            <th style="width:80px; text-align:right;">rows</th>
            <th>note</th>
            <th>sql</th>
            <th style="width:80px;"></th>
        </tr></thead>`;
        // Refresh the per-row SQL cache so the Copy buttons can read full SQL
        // without HTML-attribute escaping headaches.
        for (const k of Object.keys(_pgHistorySqlById)) delete _pgHistorySqlById[k];
        for (const r of runs) _pgHistorySqlById[r.id] = r.sql || '';
        const body = '<tbody>' + runs.map(r => _pgRenderHistoryRow(r)).join('') + '</tbody>';
        el.innerHTML = `<table class="data-table compact">${head}${body}</table>`;
    } catch (e) {
        el.innerHTML = `<div class="empty-message" style="color:var(--jp-error-color0);">Failed to load history: ${_pgEsc(e.message)}</div>`;
    }
}

function _pgRenderHistoryRow(r) {
    const elapsed = r.elapsed_sec == null ? '—' : `${r.elapsed_sec}s`;
    const rows    = r.n_rows == null ? '—' : Number(r.n_rows).toLocaleString();
    const sqlSnip = (r.sql || '').slice(0, 120).replace(/\s+/g, ' ');
    const statusBadge = r.status === 'ok'
        ? '<span class="status-badge ready">ok</span>'
        : `<span class="status-badge warning" title="${_pgEsc(r.error_msg || '')}">err</span>`;
    return `<tr>
        <td>${r.id}</td>
        <td>${_pgEsc(r.engine)}</td>
        <td>${statusBadge}</td>
        <td>${_pgEsc(r.conn || '')}</td>
        <td style="font-size:11px;">${_pgEsc(r.ts_utc)}</td>
        <td style="text-align:right;">${elapsed}</td>
        <td style="text-align:right;">${rows}</td>
        <td>
            <input type="text" id="pg-note-${r.id}" value="${_pgEsc(r.note || '')}"
                   placeholder="add note…" style="width:100%; font-size:11px;"
                   oninput="pgDebouncedNote(${r.id})">
        </td>
        <td>
            <code style="font-size:11px;" title="${_pgEsc(r.sql || '')}">${_pgEsc(sqlSnip)}</code>
        </td>
        <td>
            <button class="btn-text" onclick="pgCopyHistorySql(${r.id}, this)" title="Copy SQL to clipboard">⧉</button>
            <button class="btn-text" onclick="pgDeleteRun(${r.id})" title="Delete run">×</button>
        </td>
    </tr>`;
}

const _pgNoteTimers = {};
function pgDebouncedNote(id) {
    clearTimeout(_pgNoteTimers[id]);
    _pgNoteTimers[id] = setTimeout(() => pgSyncNote(id), 600);
}

async function pgSyncNote(id) {
    const el = document.getElementById(`pg-note-${id}`);
    if (!el) return;
    try {
        await fetch(`/api/playground/history/${id}`, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({note: el.value}),
        });
    } catch (e) {
        console.error('note sync failed:', e);
    }
}

async function pgDeleteRun(id) {
    if (!confirm(`Delete run #${id}?`)) return;
    try {
        const res = await fetch(`/api/playground/history/${id}`, {method: 'DELETE'});
        if (res.ok) pgLoadHistory();
    } catch (e) {
        console.error('delete failed:', e);
    }
}

async function pgClearHistory() {
    if (!confirm('Delete every row from _playground_history?\n\nThis cannot be undone.')) return;
    try {
        const res = await fetch('/api/playground/history/clear', {method: 'POST'});
        const data = await res.json();
        if (res.ok) {
            _pgSetStatus(`Cleared ${data.deleted} history row${data.deleted === 1 ? '' : 's'}.`, 'ok');
            pgLoadHistory();
        } else {
            _pgSetStatus(`Clear failed: ${data.error || res.status}`, 'error');
        }
    } catch (e) {
        _pgSetStatus(`Clear failed: ${e.message}`, 'error');
    }
}

async function pgClearCache() {
    try {
        const res = await fetch('/api/playground/cache/clear', {method: 'POST'});
        const data = await res.json();
        if (res.ok) {
            _pgSetStatus(`Cleared ${data.cleared} cached result${data.cleared === 1 ? '' : 's'}.`, 'ok');
        } else {
            _pgSetStatus(`Clear failed: ${data.error || res.status}`, 'error');
        }
    } catch (e) {
        _pgSetStatus(`Clear failed: ${e.message}`, 'error');
    }
}

async function pgClearExtractionCache() {
    if (!confirm('Delete every .cache.db file under known outdirs (csv/, sas/, …)?\n\nFuture extractor runs will re-execute every block — Athena scans will be re-billed.')) return;
    try {
        const res = await fetch('/api/playground/cache/clear-extraction', {method: 'POST'});
        const data = await res.json();
        if (res.ok) {
            const n = (data.deleted || []).length;
            _pgSetStatus(`Deleted ${n} cache file${n === 1 ? '' : 's'}.`, 'ok');
        } else {
            _pgSetStatus(`Clear failed: ${data.error || res.status}`, 'error');
        }
    } catch (e) {
        _pgSetStatus(`Clear failed: ${e.message}`, 'error');
    }
}
