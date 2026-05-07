/* playground.js — ad-hoc SQL runner with timing + history */

function navigateToStep(step) {
    window.location.href = '/' + step;
}

let _pgLastResult = null;

document.addEventListener('DOMContentLoaded', () => {
    pgEngineChanged();
    pgLoadHistory();
});

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
    document.getElementById('pg-sql-wrap').style.display = isSAS ? 'none' : '';
    document.getElementById('pg-sas-controls').style.display = isSAS ? 'flex' : 'none';
    document.getElementById('pg-run-btn').style.display = isSAS ? 'none' : '';
    document.getElementById('pg-sas-btn').style.display = isSAS ? '' : 'none';

    const label = document.getElementById('pg-conn-label');
    const conn = document.getElementById('pg-conn');
    if (eng === 'athena') {
        label.textContent = 'Athena database (optional)';
        conn.placeholder = 'my_database';
    } else if (eng === 'oracle') {
        label.textContent = 'Oracle macro (e.g. pb23)';
        conn.placeholder = 'pb23';
    } else {
        label.textContent = 'SAS engine — uses dtrack config';
        conn.placeholder = '(uses loaded config_path)';
    }
}

async function pgRun() {
    const engine = _pgEngine();
    const sql = document.getElementById('pg-sql').value.trim();
    const conn = document.getElementById('pg-conn').value.trim();
    const rowCap = parseInt(document.getElementById('pg-row-cap').value || '200', 10);

    if (!sql) { _pgSetStatus('SQL is empty.', 'error'); return; }
    if (engine === 'oracle' && !conn) { _pgSetStatus('Oracle requires a connection macro.', 'error'); return; }

    _pgSetStatus('Running…');
    const btn = document.getElementById('pg-run-btn');
    btn.disabled = true;

    try {
        const res = await fetch('/api/playground/run', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({engine, sql, conn, row_cap: rowCap}),
        });
        const data = await res.json();
        if (!res.ok || !data.ok) {
            _pgSetStatus(`Error: ${data.error || res.status}`, 'error');
            _pgRenderError(data.error || `HTTP ${res.status}`);
        } else {
            _pgLastResult = data;
            _pgSetStatus(`✓ ${data.n_rows_total} row${data.n_rows_total===1?'':'s'} · ${data.elapsed_sec}s`, 'ok');
            _pgRenderResult(data);
        }
    } catch (e) {
        _pgSetStatus(`Network error: ${e.message}`, 'error');
        _pgRenderError(e.message);
    } finally {
        btn.disabled = false;
        pgLoadHistory();
    }
}

async function pgGenerateSAS() {
    const type = document.getElementById('pg-sas-type').value;
    const from_date = document.getElementById('pg-sas-from').value;
    const to_date   = document.getElementById('pg-sas-to').value;
    const vintage   = document.getElementById('pg-sas-vintage').value;

    _pgSetStatus('Generating SAS…');
    const btn = document.getElementById('pg-sas-btn');
    btn.disabled = true;

    try {
        const res = await fetch('/api/playground/sas', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({type, from_date, to_date, vintage}),
        });
        const data = await res.json();
        if (!res.ok || !data.ok) {
            _pgSetStatus(`SAS gen failed: ${data.error || res.status}`, 'error');
        } else {
            _pgSetStatus(`✓ generated ${data.filename} (${data.content.length.toLocaleString()} chars)`, 'ok');
            _pgDownloadBlob(data.content, data.filename, 'text/plain');
        }
    } catch (e) {
        _pgSetStatus(`Network error: ${e.message}`, 'error');
    } finally {
        btn.disabled = false;
        pgLoadHistory();
    }
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
    meta.textContent = `${data.n_rows_total} row${data.n_rows_total===1?'':'s'} · ${data.columns.length} col${data.columns.length===1?'':'s'} · elapsed ${data.elapsed_sec}s · showing first ${data.n_rows_returned}`;

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
    if (!_pgLastResult) { _pgSetStatus('No result to download.', 'error'); return; }
    const cols = _pgLastResult.columns || [];
    const rows = _pgLastResult.rows || [];
    const csvCell = v => {
        const s = v == null ? '' : String(v);
        return /[",\n\r]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
    };
    const lines = [cols.map(csvCell).join(',')];
    for (const r of rows) lines.push(r.map(csvCell).join(','));
    _pgDownloadBlob(lines.join('\n'), 'playground_result.csv', 'text/csv');
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
            <th style="width:120px;">conn</th>
            <th style="width:160px;">when (UTC)</th>
            <th style="width:80px; text-align:right;">elapsed</th>
            <th style="width:80px; text-align:right;">rows</th>
            <th>note</th>
            <th>sql</th>
            <th style="width:60px;"></th>
        </tr></thead>`;
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
        <td>${_pgEsc(r.engine)} ${statusBadge}</td>
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
