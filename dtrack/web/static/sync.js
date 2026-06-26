/* sync.js — diff dtrack.json vs SQLite per pair, push either direction */

function _syncEsc(s) {
    if (s == null) return '';
    return escHtml(s);
}

function _syncFmt(v) {
    if (v == null) return '<span style="color:var(--jp-ui-font-color2);">—</span>';
    if (typeof v === 'object' && Object.keys(v).length === 0) return '<span style="color:var(--jp-ui-font-color2);">empty</span>';
    return `<pre style="margin:0; font-size:11px; max-height:200px; overflow:auto;">${_syncEsc(JSON.stringify(v, null, 2))}</pre>`;
}

function _syncStatusBadge(status) {
    const label = {
        match:     ['ok',      'in sync'],
        differ:    ['warning', 'differ'],
        json_only: ['warning', 'json only'],
        db_only:   ['warning', 'db only'],
        empty:     ['ready',   'empty'],
    }[status] || ['ready', status];
    const cls = label[0] === 'ok' ? 'ready' : (label[0] === 'warning' ? 'warning' : 'ready');
    return `<span class="status-badge ${cls}">${label[1]}</span>`;
}

document.addEventListener('DOMContentLoaded', syncRefresh);

async function syncRefresh() {
    const root = document.getElementById('sync-content');
    root.innerHTML = '<div class="empty-message">Loading…</div>';
    try {
        const res = await fetch('/api/sync/inspect');
        const data = await res.json();
        if (!res.ok) {
            root.innerHTML = `<div class="empty-message" style="color:var(--jp-error-color0);">${_syncEsc(data.error || res.status)}</div>`;
            return;
        }
        const pairs = data.pairs || [];
        if (!pairs.length) {
            root.innerHTML = '<div class="empty-message">No pairs configured.</div>';
            return;
        }

        const summary = `<div style="font-size:11px; color:var(--jp-ui-font-color2); margin-bottom:8px;">
            config: <code>${_syncEsc(data.config_path)}</code> &nbsp;|&nbsp;
            db: <code>${_syncEsc(data.db_path)}</code>
        </div>`;

        root.innerHTML = summary + pairs.map(_syncRenderPair).join('');
    } catch (e) {
        root.innerHTML = `<div class="empty-message" style="color:var(--jp-error-color0);">Failed: ${_syncEsc(e.message)}</div>`;
    }
}

function _syncRenderPair(p) {
    const rows = p.fields.map(f => {
        // Action buttons depend on status — push either way for differ;
        // mirror the populated side onto the empty one for *_only.
        let actions = '';
        if (f.status === 'differ' || f.status === 'json_only') {
            actions += `<button class="btn-text" onclick="syncPush('${_syncEsc(p.pair_name)}','${f.name}','json_to_db')" title="Copy json value into db">json → db</button>`;
        }
        if (f.status === 'differ' || f.status === 'db_only') {
            actions += ` <button class="btn-text" onclick="syncPush('${_syncEsc(p.pair_name)}','${f.name}','db_to_json')" title="Copy db value into dtrack.json">db → json</button>`;
        }
        if (!actions) actions = '<span style="color:var(--jp-ui-font-color2);">—</span>';

        return `<tr>
            <td style="font-weight:600; vertical-align:top;">${f.name}</td>
            <td style="vertical-align:top;">${_syncStatusBadge(f.status)}</td>
            <td style="vertical-align:top; max-width:320px;">${_syncFmt(f.json)}</td>
            <td style="vertical-align:top; max-width:320px;">${_syncFmt(f.db)}</td>
            <td style="vertical-align:top; white-space:nowrap;">${actions}</td>
        </tr>`;
    }).join('');

    const anyDiffer = p.fields.some(f => f.status === 'differ' || f.status === 'json_only' || f.status === 'db_only');
    const headerBadge = anyDiffer
        ? '<span class="status-badge warning">drift</span>'
        : '<span class="status-badge ready">clean</span>';

    return `<details class="rc-details" ${anyDiffer ? 'open' : ''}>
        <summary class="rc-details-summary">
            <span style="font-weight:600;">${_syncEsc(p.pair_name)}</span>
            <span class="rc-details-counts">${headerBadge}</span>
        </summary>
        <div class="rc-details-body">
            <table class="data-table compact">
                <thead><tr>
                    <th>Field</th><th>Status</th>
                    <th>dtrack.json</th><th>SQLite db</th>
                    <th>Push</th>
                </tr></thead>
                <tbody>${rows}</tbody>
            </table>
        </div>
    </details>`;
}

async function syncPush(pair, field, direction) {
    if (!confirm(`Push ${field} ${direction.replace('_', ' ')} for ${pair}?`)) return;
    try {
        const res = await fetch('/api/sync/push', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({pair, field, direction}),
        });
        const data = await res.json();
        if (!res.ok) {
            alert(`Push failed: ${data.error || res.status}`);
            return;
        }
        await syncRefresh();
    } catch (e) {
        alert(`Network error: ${e.message}`);
    }
}

async function syncReloadJson() {
    try {
        const res = await fetch('/api/sync/reload_json', {method: 'POST'});
        const data = await res.json();
        if (!res.ok) {
            alert(`Reload failed: ${data.error || res.status}`);
            return;
        }
        await syncRefresh();
    } catch (e) {
        alert(`Network error: ${e.message}`);
    }
}
