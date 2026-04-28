// load_row.js — page-specific glue for Load Row.
// Generic drop-zone / scan / match / upload lives in csv_loader.js; this
// file owns the DB-summary panel at the top and the per-pair column-upload
// affordance inside it.

const knownTablesRef = { current: [] };   // shared with csv_loader

document.addEventListener('DOMContentLoaded', async () => {
    initLoader();
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

// ─────────────────────────────────────────────────────────────────────
// Known tables (for the shared loader's auto-match)
// ─────────────────────────────────────────────────────────────────────
async function loadKnownTables() {
    try {
        const resp = await fetch('/api/status');
        const data = await resp.json();
        // Hide pairs toggled off on /pairs (skip=true).
        const tables = (data.pairs || []).filter(p => !p.skip).map(p => ({
            pair_name: p.pair_name,
            table_left: p.table_left, table_right: p.table_right,
            source_left: p.source_left || '', source_right: p.source_right || '',
        }));

        // Also include pairs in config that aren't registered in the DB yet
        const cfgResp = await fetch('/api/pairs/list');
        const cfgData = await cfgResp.json();
        const existing = new Set(tables.map(t => t.pair_name));
        for (const p of (cfgData.pairs || [])) {
            if (p.skip) continue;
            if (existing.has(p.name)) continue;
            const leftSource = p.left?.source || '';
            const rightSource = p.right?.source || '';
            tables.push({
                pair_name: p.name,
                table_left: leftSource ? `${leftSource}_${p.name}` : p.name,
                table_right: rightSource ? `${rightSource}_${p.name}` : p.name,
                source_left: leftSource, source_right: rightSource,
            });
        }
        knownTablesRef.current = tables;
    } catch (e) { console.error('Failed to load tables:', e); }
}

// ─────────────────────────────────────────────────────────────────────
// Shared CSV loader — only config differs between load_row / load_col /
// col_mapping. Suffix is what's strictly matched in filenames.
// ─────────────────────────────────────────────────────────────────────
function initLoader() {
    createCsvLoader({
        suffix: 'row',
        uploadEndpoint: '/api/load/row/upload',
        pathEndpoint: '/api/load/row/path',
        knownTables: knownTablesRef,
        afterLoad: refreshDbStatus,
        formatLoadResult: (data, entry) =>
            `${entry.tableName}: ${data.loaded} rows ` +
            `(${data.new_dates ?? 0} new, ${data.updated_dates ?? 0} updated)`,
    }).init();
}

// ─────────────────────────────────────────────────────────────────────
// DB summary panel (load_row-specific)
// ─────────────────────────────────────────────────────────────────────
async function refreshDbStatus() {
    const el = document.getElementById('db-summary');
    try {
        const resp = await fetch('/api/status');
        const data = await resp.json();
        const pairs = (data.pairs || []).filter(p => !p.skip);
        if (!pairs.length) {
            el.innerHTML = '<div class="empty-message">No pairs configured</div>';
            return;
        }

        const rows = pairs.map(p => {
            const lr = p.left.row_count || 0;
            const rr = p.right.row_count || 0;
            const lc = p.left.col_count || 0;
            const rc = p.right.col_count || 0;
            const ld = p.left.min_date && p.left.max_date
                ? `${p.left.min_date} &rarr; ${p.left.max_date}` : '&mdash;';
            const rd = p.right.min_date && p.right.max_date
                ? `${p.right.min_date} &rarr; ${p.right.max_date}` : '&mdash;';
            const lBadge = lr > 0
                ? `<span class="status-badge ready">${lr.toLocaleString()}</span>`
                : `<span class="status-badge warning">0</span>`;
            const rBadge = rr > 0
                ? `<span class="status-badge ready">${rr.toLocaleString()}</span>`
                : `<span class="status-badge warning">0</span>`;
            const lColBadge = lc > 0
                ? `<span class="status-badge ready">${lc}</span>`
                : `<button class="btn-text" style="font-size:11px;" onclick="uploadColCsv('${p.table_left}', '${p.source_left}')">upload</button>`;
            const rColBadge = rc > 0
                ? `<span class="status-badge ready">${rc}</span>`
                : `<button class="btn-text" style="font-size:11px;" onclick="uploadColCsv('${p.table_right}', '${p.source_right}')">upload</button>`;
            const safeName = p.pair_name.replace(/'/g, "\\'");

            return `<tr>
                <td rowspan="2" style="vertical-align:middle; text-align:center;">
                    <button class="btn btn-sm btn-danger" onclick="deletePairFromDb('${safeName}')" title="Delete pair and all data">&times;</button>
                </td>
                <td rowspan="2" style="vertical-align:middle; font-weight:600;">${p.pair_name}</td>
                <td>L</td>
                <td style="text-align:right;">${lBadge}</td>
                <td class="date-cell">${ld}</td>
                <td style="text-align:center;">${lColBadge}</td>
            </tr><tr>
                <td>R</td>
                <td style="text-align:right;">${rBadge}</td>
                <td class="date-cell">${rd}</td>
                <td style="text-align:center;">${rColBadge}</td>
            </tr>`;
        }).join('');

        el.innerHTML = `
            <table class="data-table compact">
                <thead><tr>
                    <th style="width:3em;"></th><th>Pair</th><th></th>
                    <th style="text-align:right;">Rows</th><th>Date Range</th>
                    <th style="text-align:center;">Cols</th>
                </tr></thead>
                <tbody>${rows}</tbody>
            </table>`;
    } catch (e) {
        el.innerHTML = '<div class="empty-message">Failed to load status</div>';
    }
}

// Per-pair ad-hoc columns upload from the DB summary (used before a full
// Load Row has been done — lets you seed _column_meta for one side).
function uploadColCsv(tableName, source) {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.csv';
    input.onchange = async () => {
        const file = input.files[0];
        if (!file) return;
        const form = new FormData();
        form.append('file', file);
        form.append('table_name', tableName);
        form.append('source', source);
        try {
            const resp = await fetch('/api/load/columns/upload', { method: 'POST', body: form });
            const data = await resp.json();
            if (!resp.ok) throw new Error(data.error || 'Upload failed');
            await refreshDbStatus();
        } catch (e) {
            alert('Column upload failed: ' + e.message);
        }
    };
    input.click();
}

async function deletePairFromDb(pairName) {
    if (!confirm(`Delete pair "${pairName}" and all its data?`)) return;
    try {
        const resp = await fetch(`/api/pairs/${encodeURIComponent(pairName)}?purge=1`, {method: 'DELETE'});
        const data = await resp.json();
        if (!resp.ok) throw new Error(data.error || 'Delete failed');
        await refreshDbStatus();
        await loadKnownTables();
    } catch (e) {
        alert('Delete failed: ' + e.message);
    }
}
