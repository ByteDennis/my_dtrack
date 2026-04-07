// benchmark.js — SQL Benchmark page logic

// ---------------------------------------------------------------------------
// Strategy definitions
// ---------------------------------------------------------------------------

const DEFAULT_STRATEGIES = [
    {
        id: 'index-where',
        label: 'Index WHERE',
        desc: 'TRUNC(date) vs date range',
        needs: ['date'],
        oracleSlow: (t, d) =>
            `SELECT COUNT(*) FROM ${t}\nWHERE TRUNC(${d}) = DATE '2024-01-15'`,
        oracleFast: (t, d) =>
            `SELECT COUNT(*) FROM ${t}\nWHERE ${d} >= DATE '2024-01-15'\n  AND ${d} < DATE '2024-01-16'`,
        awsSql: (t, d) =>
            `SELECT COUNT(*) FROM ${t}\nWHERE ${d} >= DATE '2024-01-15'\n  AND ${d} < DATE '2024-01-16'`,
        sasSlowName: 'slow_trunc',
        sasFastName: 'fast_range',
        sasSlow: () =>
            `SELECT COUNT(*) as cnt FROM &bench_table\n        WHERE TRUNC(&bench_datecol) = DATE '2024-01-15'`,
        sasFast: () =>
            `SELECT COUNT(*) as cnt FROM &bench_table\n        WHERE &bench_datecol >= DATE '2024-01-15' AND &bench_datecol < DATE '2024-01-16'`,
    },
    {
        id: 'index-groupby',
        label: 'Index GROUP BY',
        desc: 'TRUNC GROUP BY vs direct',
        needs: ['date'],
        oracleSlow: (t, d) =>
            `SELECT TRUNC(${d}) AS dt, COUNT(*)\nFROM ${t}\nGROUP BY TRUNC(${d})\nORDER BY 1`,
        oracleFast: (t, d) =>
            `SELECT ${d} AS dt, COUNT(*)\nFROM ${t}\nGROUP BY ${d}\nORDER BY 1`,
        awsSql: (t, d) =>
            `SELECT ${d} AS dt, COUNT(*)\nFROM ${t}\nGROUP BY ${d}\nORDER BY 1`,
        sasSlowName: 'slow_trunc_groupby',
        sasFastName: 'fast_direct_groupby',
        sasSlow: () =>
            `SELECT TRUNC(&bench_datecol) AS dt, COUNT(*) as cnt\n        FROM &bench_table GROUP BY TRUNC(&bench_datecol) ORDER BY 1`,
        sasFast: () =>
            `SELECT &bench_datecol AS dt, COUNT(*) as cnt\n        FROM &bench_table GROUP BY &bench_datecol ORDER BY 1`,
    },
    {
        id: 'cte-simple',
        label: 'CTE Simple',
        desc: 'CTE overhead vs direct query',
        needs: ['date', 'num'],
        oracleSlow: (t, d, n) =>
            `WITH means AS (\n  SELECT AVG(${n || '1'}) AS avg_val, COUNT(*) AS cnt\n  FROM ${t}\n)\nSELECT * FROM means`,
        oracleFast: (t, d, n) =>
            `SELECT AVG(${n || '1'}) AS avg_val, COUNT(*) AS cnt\nFROM ${t}`,
        awsSql: (t, d, n) =>
            `SELECT AVG(${n || '1'}) AS avg_val, COUNT(*) AS cnt\nFROM ${t}`,
        sasSlowName: 'with_cte',
        sasFastName: 'direct',
        sasSlow: () =>
            `WITH means AS (SELECT AVG(&bench_numcol) as avg_val, COUNT(*) as cnt FROM &bench_table)\n        SELECT * FROM means`,
        sasFast: () =>
            `SELECT AVG(&bench_numcol) as avg_val, COUNT(*) as cnt FROM &bench_table`,
    },
    {
        id: 'parallel-query',
        label: 'Parallel Query',
        desc: 'Serial vs PARALLEL(4)',
        needs: ['date', 'num'],
        oracleSlow: (t, d, n) =>
            `SELECT /*+ NO_PARALLEL */ ${d}, COUNT(*), AVG(${n || '1'}) AS avg_val\nFROM ${t}\nGROUP BY ${d}`,
        oracleFast: (t, d, n) =>
            `SELECT /*+ PARALLEL(t, 4) */ ${d}, COUNT(*), AVG(${n || '1'}) AS avg_val\nFROM ${t} t\nGROUP BY ${d}`,
        awsSql: (t, d, n) =>
            `SELECT ${d}, COUNT(*), AVG(${n || '1'}) AS avg_val\nFROM ${t}\nGROUP BY ${d}`,
        sasSlowName: 'serial',
        sasFastName: 'parallel_4',
        sasSlow: () =>
            `SELECT /*+ NO_PARALLEL */ &bench_datecol, COUNT(*) as cnt, AVG(&bench_numcol) AS avg_val\n        FROM &bench_table GROUP BY &bench_datecol`,
        sasFast: () =>
            `SELECT /*+ PARALLEL(t, 4) */ &bench_datecol, COUNT(*) as cnt, AVG(&bench_numcol) AS avg_val\n        FROM &bench_table t GROUP BY &bench_datecol`,
    },
    {
        id: 'partition-prune',
        label: 'Partition Prune',
        desc: 'TO_CHAR vs date range',
        needs: ['date'],
        oracleSlow: (t, d) =>
            `SELECT COUNT(*) FROM ${t}\nWHERE TO_CHAR(${d}, 'YYYY-MM') = '2024-01'`,
        oracleFast: (t, d) =>
            `SELECT COUNT(*) FROM ${t}\nWHERE ${d} >= DATE '2024-01-01'\n  AND ${d} < DATE '2024-02-01'`,
        awsSql: (t, d) =>
            `SELECT COUNT(*) FROM ${t}\nWHERE ${d} >= DATE '2024-01-01'\n  AND ${d} < DATE '2024-02-01'`,
        sasSlowName: 'slow_to_char',
        sasFastName: 'fast_date_range',
        sasSlow: () =>
            `SELECT COUNT(*) as cnt FROM &bench_table\n        WHERE TO_CHAR(&bench_datecol, 'YYYY-MM') = '2024-01'`,
        sasFast: () =>
            `SELECT COUNT(*) as cnt FROM &bench_table\n        WHERE &bench_datecol >= DATE '2024-01-01' AND &bench_datecol < DATE '2024-02-01'`,
    },
    {
        id: 'result-cache',
        label: 'Result Cache',
        desc: 'With vs without RESULT_CACHE',
        needs: ['date', 'num'],
        oracleSlow: (t, d, n) =>
            `SELECT ${d}, COUNT(*), AVG(${n || '1'}) AS avg_val\nFROM ${t}\nGROUP BY ${d}`,
        oracleFast: (t, d, n) =>
            `SELECT /*+ RESULT_CACHE */ ${d}, COUNT(*), AVG(${n || '1'}) AS avg_val\nFROM ${t}\nGROUP BY ${d}`,
        awsSql: (t, d, n) =>
            `SELECT ${d}, COUNT(*), AVG(${n || '1'}) AS avg_val\nFROM ${t}\nGROUP BY ${d}`,
        sasSlowName: 'no_cache',
        sasFastName: 'result_cache',
        sasSlow: () =>
            `SELECT &bench_datecol, COUNT(*) as cnt, AVG(&bench_numcol) AS avg_val\n        FROM &bench_table GROUP BY &bench_datecol`,
        sasFast: () =>
            `SELECT /*+ RESULT_CACHE */ &bench_datecol, COUNT(*) as cnt, AVG(&bench_numcol) AS avg_val\n        FROM &bench_table GROUP BY &bench_datecol`,
    },
    {
        id: 'array-size',
        label: 'Array Size',
        desc: 'Fetch arraysize 100 vs 5000',
        needs: ['date'],
        oracleSlow: (t) =>
            `-- arraysize = 100\nSELECT * FROM ${t} WHERE ROWNUM <= 200000`,
        oracleFast: (t) =>
            `-- arraysize = 5000\nSELECT * FROM ${t} WHERE ROWNUM <= 200000`,
        awsSql: (t) =>
            `SELECT * FROM ${t} LIMIT 200000`,
        sasSlowName: 'arraysize_100',
        sasFastName: 'arraysize_5000',
        sasSlow: () =>
            `SELECT * FROM &bench_table WHERE ROWNUM <= 200000`,
        sasFast: () =>
            `SELECT * FROM &bench_table WHERE ROWNUM <= 200000`,
    },
];

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

let benchPairs = [];
let strategies = [...DEFAULT_STRATEGIES];
let customStrategies = [];
let editingPairIdx = -1;
let resultsData = {};  // { pairName: { strategyId: { oracle, aws, sas } } }

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

document.addEventListener('DOMContentLoaded', () => {
    loadState();
    renderPairs();
    renderStrategies();
});

function loadState() {
    try {
        const saved = localStorage.getItem('dtrack_bench_state');
        if (saved) {
            const state = JSON.parse(saved);
            benchPairs = state.pairs || [];
            customStrategies = state.customStrategies || [];
            resultsData = state.results || {};
        }
    } catch (e) { /* ignore */ }
}

function saveState() {
    try {
        localStorage.setItem('dtrack_bench_state', JSON.stringify({
            pairs: benchPairs,
            customStrategies,
            results: resultsData,
        }));
    } catch (e) { /* ignore */ }
}

// ---------------------------------------------------------------------------
// Pair management
// ---------------------------------------------------------------------------

function renderPairs() {
    const list = document.getElementById('bench-pairs-list');
    const empty = document.getElementById('bench-pairs-empty');

    if (benchPairs.length === 0) {
        list.innerHTML = '';
        empty.style.display = '';
        return;
    }
    empty.style.display = 'none';

    list.innerHTML = benchPairs.map((p, i) => `
        <div class="bench-pair-card">
            <div class="pair-info">
                <div class="pair-label">${esc(p.name)}</div>
                <div class="pair-detail">
                    <strong>LEFT:</strong> ${esc(p.left.table)} &middot; ${esc(p.left.dateCol)}
                    ${p.left.numCol ? ' &middot; ' + esc(p.left.numCol) : ''}
                    ${p.left.catCol ? ' &middot; ' + esc(p.left.catCol) : ''}
                    &middot; <em>${esc(p.left.connMacro)}</em>
                    &nbsp;&nbsp;|&nbsp;&nbsp;
                    <strong>RIGHT:</strong> ${esc(p.right.table)} &middot; ${esc(p.right.dateCol)}
                    ${p.right.numCol ? ' &middot; ' + esc(p.right.numCol) : ''}
                    ${p.right.catCol ? ' &middot; ' + esc(p.right.catCol) : ''}
                    &middot; <em>${esc(p.right.connMacro)}</em>
                </div>
            </div>
            <div class="pair-actions">
                <button class="btn-text" onclick="editBenchPair(${i})">Edit</button>
                <button class="btn-text" style="color:var(--jp-error-color0);" onclick="deleteBenchPair(${i})">Del</button>
            </div>
        </div>
    `).join('');
}

function showAddPairModal() {
    editingPairIdx = -1;
    document.getElementById('bench-modal-title').textContent = 'Add Benchmark Pair';
    document.getElementById('bench-pair-name').value = '';
    ['table', 'date-col', 'num-col', 'cat-col'].forEach(f => {
        document.getElementById(`bench-left-${f}`).value = '';
        document.getElementById(`bench-right-${f}`).value = '';
    });
    document.getElementById('bench-left-conn').value = 'pb23';
    document.getElementById('bench-right-conn').value = 'analytics_db';
    document.getElementById('bench-pair-modal').classList.add('active');
}

function editBenchPair(idx) {
    editingPairIdx = idx;
    const p = benchPairs[idx];
    document.getElementById('bench-modal-title').textContent = 'Edit Benchmark Pair';
    document.getElementById('bench-pair-name').value = p.name;
    document.getElementById('bench-left-table').value = p.left.table;
    document.getElementById('bench-left-date-col').value = p.left.dateCol;
    document.getElementById('bench-left-num-col').value = p.left.numCol || '';
    document.getElementById('bench-left-cat-col').value = p.left.catCol || '';
    document.getElementById('bench-left-conn').value = p.left.connMacro;
    document.getElementById('bench-right-table').value = p.right.table;
    document.getElementById('bench-right-date-col').value = p.right.dateCol;
    document.getElementById('bench-right-num-col').value = p.right.numCol || '';
    document.getElementById('bench-right-cat-col').value = p.right.catCol || '';
    document.getElementById('bench-right-conn').value = p.right.connMacro;
    document.getElementById('bench-pair-modal').classList.add('active');
}

function closePairModal() {
    document.getElementById('bench-pair-modal').classList.remove('active');
}

function saveBenchPair() {
    const name = document.getElementById('bench-pair-name').value.trim();
    if (!name) return alert('Pair name is required');

    const pair = {
        name,
        left: {
            table: document.getElementById('bench-left-table').value.trim(),
            dateCol: document.getElementById('bench-left-date-col').value.trim(),
            numCol: document.getElementById('bench-left-num-col').value.trim(),
            catCol: document.getElementById('bench-left-cat-col').value.trim(),
            connMacro: document.getElementById('bench-left-conn').value,
        },
        right: {
            table: document.getElementById('bench-right-table').value.trim(),
            dateCol: document.getElementById('bench-right-date-col').value.trim(),
            numCol: document.getElementById('bench-right-num-col').value.trim(),
            catCol: document.getElementById('bench-right-cat-col').value.trim(),
            connMacro: document.getElementById('bench-right-conn').value,
        },
    };

    if (!pair.left.table || !pair.left.dateCol || !pair.right.table || !pair.right.dateCol) {
        return alert('Table name and date column are required for both sides');
    }

    if (editingPairIdx >= 0) {
        benchPairs[editingPairIdx] = pair;
    } else {
        benchPairs.push(pair);
    }

    saveState();
    renderPairs();
    closePairModal();
}

function deleteBenchPair(idx) {
    if (!confirm(`Delete pair "${benchPairs[idx].name}"?`)) return;
    const name = benchPairs[idx].name;
    benchPairs.splice(idx, 1);
    delete resultsData[name];
    saveState();
    renderPairs();
}

async function importPairsFromPipeline() {
    try {
        const res = await fetch('/api/pairs/list');
        if (!res.ok) throw new Error(await res.text());
        const data = await res.json();

        let count = 0;
        for (const p of (data.pairs || data)) {
            const name = p.pair_name || p.name;
            if (benchPairs.some(bp => bp.name === name)) continue;

            const leftCfg = p.left || p.table_left_config || {};
            const rightCfg = p.right || p.table_right_config || {};
            benchPairs.push({
                name,
                left: {
                    table: leftCfg.table || leftCfg.name || '',
                    dateCol: leftCfg.date_col || leftCfg.date_column || '',
                    numCol: leftCfg.num_col || '',
                    catCol: leftCfg.cat_col || '',
                    connMacro: leftCfg.conn_macro || 'pb23',
                },
                right: {
                    table: rightCfg.table || rightCfg.name || '',
                    dateCol: rightCfg.date_col || rightCfg.date_column || '',
                    numCol: rightCfg.num_col || '',
                    catCol: rightCfg.cat_col || '',
                    connMacro: rightCfg.conn_macro || 'analytics_db',
                },
            });
            count++;
        }

        saveState();
        renderPairs();
        alert(count > 0 ? `Imported ${count} pair(s) from pipeline.` : 'No new pairs to import.');
    } catch (e) {
        alert('Could not load pairs from pipeline: ' + e.message);
    }
}

// ---------------------------------------------------------------------------
// Strategy management
// ---------------------------------------------------------------------------

function getAllStrategies() {
    return [
        ...DEFAULT_STRATEGIES,
        ...customStrategies.map(cs => ({
            id: cs.id,
            label: cs.label,
            desc: 'Custom strategy',
            needs: ['date'],
            custom: true,
        })),
    ];
}

function renderStrategies() {
    const grid = document.getElementById('strategies-grid');
    const all = getAllStrategies();
    grid.innerHTML = all.map(s => `
        <label class="strategy-item ${s.custom ? 'custom' : ''}">
            <input type="checkbox" data-strategy="${s.id}" checked>
            <span class="strat-name">${esc(s.label)}</span>
            <span class="strat-desc">${esc(s.desc)}</span>
        </label>
    `).join('');
}

function selectAllStrategies(checked) {
    document.querySelectorAll('#strategies-grid input[type="checkbox"]').forEach(cb => {
        cb.checked = checked;
    });
}

function getSelectedStrategyIds() {
    const ids = [];
    document.querySelectorAll('#strategies-grid input[type="checkbox"]:checked').forEach(cb => {
        ids.push(cb.dataset.strategy);
    });
    return ids;
}

function addCustomStrategy() {
    const input = document.getElementById('custom-strategy-name');
    const name = input.value.trim();
    if (!name) return;

    const id = 'custom-' + name.toLowerCase().replace(/[^a-z0-9]+/g, '-');
    if (getAllStrategies().some(s => s.id === id)) {
        return alert('Strategy already exists');
    }

    customStrategies.push({ id, label: name });
    saveState();
    renderStrategies();
    input.value = '';
}

// ---------------------------------------------------------------------------
// SQL Preview
// ---------------------------------------------------------------------------

function previewSQL() {
    if (benchPairs.length === 0) return alert('Add at least one benchmark pair first.');
    const selectedIds = getSelectedStrategyIds();
    if (selectedIds.length === 0) return alert('Select at least one strategy.');

    const section = document.getElementById('sql-preview-section');
    const tabsEl = document.getElementById('sql-preview-tabs');
    const contentEl = document.getElementById('sql-preview-content');

    // Build tabs
    tabsEl.innerHTML = benchPairs.map((p, i) =>
        `<button class="sql-tab ${i === 0 ? 'active' : ''}" onclick="switchSqlTab(this, 'sql-pair-${i}')">${esc(p.name)}</button>`
    ).join('');

    // Build content for each pair
    contentEl.innerHTML = benchPairs.map((p, i) => {
        const strats = getAllStrategies().filter(s => selectedIds.includes(s.id));
        const blocks = strats.map(s => {
            if (s.custom) {
                return `<div class="sql-block">
                    <h4>${esc(s.label)} (custom — enter SQL manually)</h4>
                    <pre>-- No auto-generated SQL for custom strategies</pre>
                </div>`;
            }
            const oSlow = s.oracleSlow(p.left.table, p.left.dateCol, p.left.numCol, p.left.catCol);
            const oFast = s.oracleFast(p.left.table, p.left.dateCol, p.left.numCol, p.left.catCol);
            const aws = s.awsSql(p.right.table, p.right.dateCol, p.right.numCol, p.right.catCol);
            return `<div class="sql-block">
                <h4>${esc(s.label)} — ${esc(s.desc)}</h4>
                <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:12px;">
                    <div>
                        <div style="font-size:10px; font-weight:600; color:var(--jp-error-color0); margin-bottom:4px;">ORACLE (Slow)</div>
                        <pre>${esc(oSlow)}</pre>
                    </div>
                    <div>
                        <div style="font-size:10px; font-weight:600; color:var(--jp-success-color0); margin-bottom:4px;">ORACLE (Fast)</div>
                        <pre>${esc(oFast)}</pre>
                    </div>
                    <div>
                        <div style="font-size:10px; font-weight:600; color:var(--jp-brand-color0); margin-bottom:4px;">AWS (Athena)</div>
                        <pre>${esc(aws)}</pre>
                    </div>
                </div>
            </div>`;
        }).join('');

        return `<div class="sql-tab-content ${i === 0 ? 'active' : ''}" id="sql-pair-${i}">${blocks}</div>`;
    }).join('');

    section.style.display = '';
}

function switchSqlTab(btn, contentId) {
    btn.closest('.card').querySelectorAll('.sql-tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    btn.closest('.card').querySelectorAll('.sql-tab-content').forEach(c => c.classList.remove('active'));
    document.getElementById(contentId).classList.add('active');
}

// ---------------------------------------------------------------------------
// SAS Code generation
// ---------------------------------------------------------------------------

function generateSAS() {
    if (benchPairs.length === 0) return alert('Add at least one benchmark pair first.');
    const selectedIds = getSelectedStrategyIds();
    if (selectedIds.length === 0) return alert('Select at least one strategy.');

    const iterations = document.getElementById('bench-iterations').value || 5;
    const section = document.getElementById('sas-code-section');
    const tabsEl = document.getElementById('sas-tabs');
    const contentEl = document.getElementById('sas-code-content');

    tabsEl.innerHTML = benchPairs.map((p, i) =>
        `<button class="sql-tab ${i === 0 ? 'active' : ''}" onclick="switchSqlTab(this, 'sas-pair-${i}')">${esc(p.name)}</button>`
    ).join('');

    contentEl.innerHTML = benchPairs.map((p, i) => {
        const code = buildSASCode(p, selectedIds, iterations);
        return `<div class="sql-tab-content ${i === 0 ? 'active' : ''}" id="sas-pair-${i}">
            <div class="sas-code-block" id="sas-code-${i}">${highlightSAS(code)}</div>
        </div>`;
    }).join('');

    section.style.display = '';
}

function buildSASCode(pair, selectedIds, iterations) {
    const strats = DEFAULT_STRATEGIES.filter(s => selectedIds.includes(s.id));
    const l = pair.left;
    const timestamp = new Date().toISOString().split('T')[0];

    let experiments = '';
    for (const s of strats) {
        const expName = s.id.replace(/-/g, '_');
        experiments += `\n/* ---- ${s.label}: ${s.desc} ---- */\n`;
        experiments += `%bench_run(experiment=${expName}, variant=${s.sasSlowName},\n`;
        experiments += `    sql=%str(\n        ${s.sasSlow()}\n    ))\n\n`;
        experiments += `%bench_run(experiment=${expName}, variant=${s.sasFastName},\n`;
        experiments += `    sql=%str(\n        ${s.sasFast()}\n    ))\n\n`;
    }

    return `/* dtrack SQL Benchmark -- SAS Code
   Pair:      ${pair.name}
   Table:     ${l.table}
   Generated: ${timestamp}

   Usage:
     1. Edit the connection settings below for your environment
     2. Run this SAS program
     3. Results saved to &out_dir./bench_sas_${pair.name}.csv
*/

/* ---- Configuration ---- */
%let bench_table   = ${l.table};
%let bench_datecol = ${l.dateCol};
%let bench_numcol  = ${l.numCol || 'AMOUNT'};
%let bench_catcol  = ${l.catCol || 'STATUS'};

%let iterations    = ${iterations};
%let out_dir       = .;

/* ---- Connection: edit to match your environment ---- */
/* Connection macro: ${l.connMacro} */
%let ora_user    = &SYSUSERID;
%let ora_pass    = %sysget(ORACLE_PASSWORD);
%let ora_path    = host:1521/service_name;  /* edit this */

%macro ora_connect;
    connect to oracle (user="&ora_user" password="&ora_pass" path="&ora_path");
%mend;

/* ---- Results dataset ---- */
data _bench_results;
    length experiment $32 variant $32 run 8 elapsed_sec 8 rows 8 table_name $128;
    stop;
run;

/* ---- Timer macro ---- */
%macro bench_run(experiment=, variant=, sql=);
    %do _run = 1 %to &iterations;
        %let _t0 = %sysfunc(datetime());

        proc sql noprint;
            %ora_connect
            create table _bench_tmp as
            select * from connection to oracle (
                &sql
            );
            disconnect from oracle;
        quit;

        %let _t1 = %sysfunc(datetime());
        %let _elapsed = %sysevalf(&_t1 - &_t0);
        %let _nobs = 0;
        %if %sysfunc(exist(_bench_tmp)) %then %do;
            proc sql noprint;
                select count(*) into :_nobs from _bench_tmp;
            quit;
        %end;

        data _bench_row;
            length experiment $32 variant $32 table_name $128;
            experiment  = "&experiment";
            variant     = "&variant";
            run         = &_run;
            elapsed_sec = &_elapsed;
            rows        = &_nobs;
            table_name  = "&bench_table";
        run;

        proc append base=_bench_results data=_bench_row force; run;
        proc delete data=_bench_tmp _bench_row; run;

        %put NOTE: [&experiment/&variant] run &_run: &_elapsed.s (&_nobs rows);
    %end;
%mend;


/* ==================================================================
   EXPERIMENTS
   ================================================================== */
${experiments}

/* ==================================================================
   Export results
   ================================================================== */
proc export data=_bench_results
    outfile="&out_dir./bench_sas_${pair.name}.csv"
    dbms=csv replace;
run;

%put NOTE: Benchmark complete. Results in &out_dir./bench_sas_${pair.name}.csv;
proc print data=_bench_results; run;
proc delete data=_bench_results; run;
`;
}

function highlightSAS(code) {
    // Simple SAS syntax highlighting
    return esc(code)
        .replace(/(\/\*[\s\S]*?\*\/)/g, '<span class="sas-comment">$1</span>')
        .replace(/(%\w+)/g, '<span class="sas-macro">$1</span>')
        .replace(/\b(proc|data|run|quit|select|from|where|group by|order by|as|into|set)\b/gi,
            '<span class="sas-keyword">$1</span>');
}

function copySASCode() {
    const active = document.querySelector('#sas-code-content .sql-tab-content.active .sas-code-block');
    if (!active) return;
    const text = active.textContent;
    navigator.clipboard.writeText(text).then(() => {
        alert('SAS code copied to clipboard.');
    });
}

function downloadSAS() {
    const active = document.querySelector('#sas-code-content .sql-tab-content.active .sas-code-block');
    if (!active) return;
    const text = active.textContent;
    // Find active pair index
    const activeTab = document.querySelector('#sas-tabs .sql-tab.active');
    const pairName = activeTab ? activeTab.textContent : 'benchmark';
    const blob = new Blob([text], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `bench_sas_${pairName}.sas`;
    a.click();
    URL.revokeObjectURL(url);
}

// ---------------------------------------------------------------------------
// Results table
// ---------------------------------------------------------------------------

function showResultsTable() {
    if (benchPairs.length === 0) return alert('Add at least one benchmark pair first.');
    const selectedIds = getSelectedStrategyIds();
    if (selectedIds.length === 0) return alert('Select at least one strategy.');

    const section = document.getElementById('results-section');
    const contentEl = document.getElementById('results-content');

    contentEl.innerHTML = benchPairs.map((p) => {
        const strats = getAllStrategies().filter(s => selectedIds.includes(s.id));
        const pairResults = resultsData[p.name] || {};

        const rows = strats.map(s => {
            const r = pairResults[s.id] || {};
            const oracleVal = r.oracle != null ? r.oracle : '';
            const awsVal = r.aws != null ? r.aws : '';
            const sasVal = r.sas != null ? r.sas : '';
            return `<tr>
                <td>${esc(s.label)}</td>
                <td>${esc(s.desc || '')}</td>
                <td><input type="number" step="0.001" min="0" value="${oracleVal}"
                    onchange="updateResult('${esc(p.name)}','${s.id}','oracle',this.value)"></td>
                <td><input type="number" step="0.001" min="0" value="${awsVal}"
                    onchange="updateResult('${esc(p.name)}','${s.id}','aws',this.value)"></td>
                <td class="sas-cell" data-pair="${esc(p.name)}" data-strat="${s.id}">
                    <input type="number" step="0.001" min="0" value="${sasVal}"
                        onchange="updateResult('${esc(p.name)}','${s.id}','sas',this.value)"
                        placeholder="est." style="${sasVal === '' ? 'color:var(--jp-ui-font-color3);' : ''}">
                </td>
                <td class="ratio-cell" data-pair="${esc(p.name)}" data-strat="${s.id}" data-type="sas-oracle"></td>
                <td class="ratio-cell" data-pair="${esc(p.name)}" data-strat="${s.id}" data-type="aws-oracle"></td>
            </tr>`;
        }).join('');

        return `<div style="margin-bottom:24px;">
            <div class="pair-results-header" onclick="this.nextElementSibling.style.display = this.nextElementSibling.style.display === 'none' ? '' : 'none'; this.querySelector('.toggle-icon').classList.toggle('open');">
                <span class="toggle-icon open">&#9654;</span>
                <h3>${esc(p.name)}</h3>
                <span style="font-size:11px; color:var(--jp-ui-font-color2);">
                    ${esc(p.left.table)} &#8596; ${esc(p.right.table)}
                </span>
            </div>
            <div>
                <table class="results-table">
                    <thead>
                        <tr>
                            <th style="width:140px;">Strategy</th>
                            <th style="width:180px;">Description</th>
                            <th style="width:100px;">Oracle (s)</th>
                            <th style="width:100px;">AWS (s)</th>
                            <th style="width:100px;">SAS (s)</th>
                            <th style="width:90px;">SAS/Oracle</th>
                            <th style="width:90px;">AWS/Oracle</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        </div>`;
    }).join('');

    section.style.display = '';
    recalcAllResults();
}

function updateResult(pairName, stratId, field, value) {
    if (!resultsData[pairName]) resultsData[pairName] = {};
    if (!resultsData[pairName][stratId]) resultsData[pairName][stratId] = {};

    resultsData[pairName][stratId][field] = value !== '' ? parseFloat(value) : null;
    saveState();
    recalcResults(pairName, stratId);
}

function recalcResults(pairName, stratId) {
    const r = (resultsData[pairName] || {})[stratId] || {};
    const multiplier = parseFloat(document.getElementById('sas-multiplier').value) || 20;

    const oracle = r.oracle;
    const aws = r.aws;
    let sas = r.sas;

    // Estimate SAS if not provided
    const sasCell = document.querySelector(`.sas-cell[data-pair="${pairName}"][data-strat="${stratId}"] input`);
    if (sasCell && sas == null && oracle != null) {
        sasCell.placeholder = (oracle * multiplier).toFixed(1) + ' (est.)';
        sasCell.style.color = 'var(--jp-ui-font-color3)';
        sas = oracle * multiplier;  // for ratio calc
    } else if (sasCell && sas != null) {
        sasCell.style.color = '';
    }

    // SAS/Oracle ratio
    const sasOracleCell = document.querySelector(`.ratio-cell[data-pair="${pairName}"][data-strat="${stratId}"][data-type="sas-oracle"]`);
    if (sasOracleCell) {
        if (oracle != null && oracle > 0 && sas != null) {
            const ratio = sas / oracle;
            const cls = ratio > 5 ? 'slow' : ratio < 2 ? 'fast' : 'neutral';
            sasOracleCell.innerHTML = `<span class="ratio ${cls}">${ratio.toFixed(1)}x</span>`;
        } else {
            sasOracleCell.innerHTML = '<span class="ratio neutral">--</span>';
        }
    }

    // AWS/Oracle ratio
    const awsOracleCell = document.querySelector(`.ratio-cell[data-pair="${pairName}"][data-strat="${stratId}"][data-type="aws-oracle"]`);
    if (awsOracleCell) {
        if (oracle != null && oracle > 0 && aws != null) {
            const ratio = aws / oracle;
            const cls = ratio > 3 ? 'slow' : ratio < 1 ? 'fast' : 'neutral';
            awsOracleCell.innerHTML = `<span class="ratio ${cls}">${ratio.toFixed(1)}x</span>`;
        } else {
            awsOracleCell.innerHTML = '<span class="ratio neutral">--</span>';
        }
    }
}

function recalcAllResults() {
    for (const pairName in resultsData) {
        for (const stratId in resultsData[pairName]) {
            recalcResults(pairName, stratId);
        }
    }
    // Also recalc for pairs/strats with no data yet (to update estimates)
    document.querySelectorAll('.ratio-cell').forEach(cell => {
        const pn = cell.dataset.pair;
        const sid = cell.dataset.strat;
        recalcResults(pn, sid);
    });
}

function clearAllResults() {
    if (!confirm('Clear all timing results?')) return;
    resultsData = {};
    saveState();
    showResultsTable();
}

// ---------------------------------------------------------------------------
// Import / Export results
// ---------------------------------------------------------------------------

function importResults() {
    document.getElementById('import-json').value = '';
    document.getElementById('import-modal').classList.add('active');
}

function closeImportModal() {
    document.getElementById('import-modal').classList.remove('active');
}

function doImportResults() {
    const text = document.getElementById('import-json').value.trim();
    if (!text) return alert('Paste JSON data first.');

    try {
        const data = JSON.parse(text);
        if (!Array.isArray(data)) throw new Error('Expected an array');

        let imported = 0;
        for (const row of data) {
            const exp = (row.experiment || '').replace(/_/g, '-');
            const table = row.table || '';
            const elapsed = row.elapsed_sec;
            const variant = row.variant || '';

            // Match to a pair by table name
            const pair = benchPairs.find(p =>
                p.left.table === table || p.right.table === table ||
                p.left.table.toLowerCase() === table.toLowerCase() ||
                p.right.table.toLowerCase() === table.toLowerCase()
            );
            if (!pair) continue;

            // Match to a strategy
            const strat = getAllStrategies().find(s => s.id === exp);
            if (!strat) continue;

            if (!resultsData[pair.name]) resultsData[pair.name] = {};
            if (!resultsData[pair.name][strat.id]) resultsData[pair.name][strat.id] = {};

            // Determine if oracle or aws based on table match
            const isLeft = pair.left.table === table || pair.left.table.toLowerCase() === table.toLowerCase();

            // Only use the "fast" variant for the result
            const isFast = !variant.startsWith('slow') && !variant.startsWith('no_') && variant !== 'serial' &&
                           variant !== 'fetchone_loop' && variant !== 'with_cte' && variant !== 'arraysize_100';

            if (isFast) {
                if (isLeft) {
                    resultsData[pair.name][strat.id].oracle = elapsed;
                } else {
                    resultsData[pair.name][strat.id].aws = elapsed;
                }
                imported++;
            }
        }

        saveState();
        closeImportModal();

        if (imported > 0) {
            showResultsTable();
            alert(`Imported ${imported} result(s).`);
        } else {
            alert('No matching results found. Ensure table names match your benchmark pairs.');
        }
    } catch (e) {
        alert('Invalid JSON: ' + e.message);
    }
}

function exportResults() {
    const out = [];
    for (const pairName in resultsData) {
        const pair = benchPairs.find(p => p.name === pairName);
        if (!pair) continue;
        for (const stratId in resultsData[pairName]) {
            const r = resultsData[pairName][stratId];
            out.push({
                pair: pairName,
                strategy: stratId,
                oracle_table: pair.left.table,
                aws_table: pair.right.table,
                oracle_sec: r.oracle,
                aws_sec: r.aws,
                sas_sec: r.sas,
            });
        }
    }

    const blob = new Blob([JSON.stringify(out, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'bench_results_comparison.json';
    a.click();
    URL.revokeObjectURL(url);
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

function esc(s) {
    if (s == null) return '';
    const div = document.createElement('div');
    div.textContent = String(s);
    return div.innerHTML;
}
