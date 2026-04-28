// load_col.js — page-specific glue for Load Col.
// Generic drop-zone / scan / match / upload lives in csv_loader.js; this
// file owns the per-pair col-stats summary panel at the top.

const knownTablesRef = { current: [] };

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

function initLoader() {
    createCsvLoader({
        suffix: 'col',
        uploadEndpoint: '/api/load/col-stats/upload',
        pathEndpoint: '/api/load/col-stats/path',
        knownTables: knownTablesRef,
        afterLoad: refreshDbStatus,
        formatLoadResult: (data, entry) =>
            `${entry.tableName}: ${data.loaded} stat rows loaded`,
    }).init();
}

// ─────────────────────────────────────────────────────────────────────
// DB status panel (load_col-specific: col counts + col stats summary)
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

        // Col stats summary keyed by qname
        let colStatus = {};
        try {
            const colResp = await fetch('/api/status/col');
            const colData = await colResp.json();
            colStatus = colData.status || {};
        } catch (e) { /* non-critical */ }

        // Per-pair config for vintage info
        let pairConfigs = {};
        try {
            const cfgResp = await fetch('/api/pairs/list');
            const cfgData = await cfgResp.json();
            for (const pc of (cfgData.pairs || [])) pairConfigs[pc.name] = pc;
        } catch (e) { /* non-critical */ }

        const colInfo = t => colStatus[t] || { count: 0, dates: [], min_date: null, max_date: null };
        const dateBadge = info => {
            const dates = info.dates || [];
            if (!dates.length) return '<span class="file-meta">no dates</span>';
            return `<span class="file-meta">${dates.length} date${dates.length !== 1 ? 's' : ''}: ${info.min_date || info.min} &rarr; ${info.max_date || info.max}</span>`;
        };

        const rows = pairs.map(p => {
            const lc = p.left.col_count || 0;
            const rc = p.right.col_count || 0;
            const lInfo = colInfo(p.table_left);
            const rInfo = colInfo(p.table_right);
            const cfg = pairConfigs[p.pair_name] || {};
            const pairVintage = cfg.vintage || '';
            const lColBadge = lc > 0
                ? `<span class="status-badge ready">${lc}</span>`
                : `<span class="status-badge warning">0</span>`;
            const rColBadge = rc > 0
                ? `<span class="status-badge ready">${rc}</span>`
                : `<span class="status-badge warning">0</span>`;
            const lStatsBadge = (lInfo.count || 0) > 0
                ? `<span class="status-badge ready">${(lInfo.count || 0).toLocaleString()}</span>`
                : `<span class="status-badge warning">0</span>`;
            const rStatsBadge = (rInfo.count || 0) > 0
                ? `<span class="status-badge ready">${(rInfo.count || 0).toLocaleString()}</span>`
                : `<span class="status-badge warning">0</span>`;

            return `<tr>
                <td rowspan="2" style="vertical-align:middle; font-weight:600;">${p.pair_name}</td>
                <td>L</td>
                <td style="text-align:center;">${lColBadge}</td>
                <td style="text-align:right;">${lStatsBadge}</td>
                <td>${dateBadge(lInfo)}</td>
                <td rowspan="2" style="vertical-align:middle;"><span class="file-meta">${pairVintage || 'all'}</span></td>
            </tr><tr>
                <td>R</td>
                <td style="text-align:center;">${rColBadge}</td>
                <td style="text-align:right;">${rStatsBadge}</td>
                <td>${dateBadge(rInfo)}</td>
            </tr>`;
        }).join('');

        el.innerHTML = `
            <table class="data-table compact">
                <thead><tr>
                    <th>Pair</th><th></th><th style="text-align:center;">Cols</th>
                    <th style="text-align:right;">Col Stats</th>
                    <th>Date Range (DB)</th><th>Vintage</th>
                </tr></thead>
                <tbody>${rows}</tbody>
            </table>`;
    } catch (e) {
        el.innerHTML = '<div class="empty-message">Failed to load status</div>';
    }
}
