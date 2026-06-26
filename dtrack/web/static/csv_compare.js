/* csv_compare.js — string-exact diff between two uploaded CSVs */

let _cvLeftInfo  = null;
let _cvRightInfo = null;

function _cvEsc(s) {
    if (s == null) return '';
    return escHtml(s);
}

async function cvInspect() {
    const lf = document.getElementById('cv-left').files[0];
    const rf = document.getElementById('cv-right').files[0];
    document.getElementById('cv-left-info').textContent  = lf ? lf.name : 'no file';
    document.getElementById('cv-right-info').textContent = rf ? rf.name : 'no file';
    if (!lf || !rf) {
        document.getElementById('cv-cols-card').style.display = 'none';
        return;
    }

    const fd = new FormData();
    fd.append('left', lf);
    fd.append('right', rf);
    try {
        const res = await fetch('/api/csv_compare/inspect', {method: 'POST', body: fd});
        const data = await res.json();
        if (!res.ok) {
            document.getElementById('cv-cols-card').style.display = '';
            document.getElementById('cv-status').textContent = `Inspect failed: ${data.error || res.status}`;
            document.getElementById('cv-status').style.color = 'var(--jp-error-color0)';
            return;
        }
        _cvLeftInfo  = data.left;
        _cvRightInfo = data.right;
        document.getElementById('cv-left-info').textContent  = `${lf.name} — ${data.left.n_rows.toLocaleString()} rows, ${data.left.columns.length} cols`;
        document.getElementById('cv-right-info').textContent = `${rf.name} — ${data.right.n_rows.toLocaleString()} rows, ${data.right.columns.length} cols`;
        _cvRenderColumnGrids(data.left.columns, data.right.columns);
        document.getElementById('cv-cols-card').style.display = '';
        document.getElementById('cv-status').textContent = '';
    } catch (e) {
        document.getElementById('cv-status').textContent = `Network error: ${e.message}`;
        document.getElementById('cv-status').style.color = 'var(--jp-error-color0)';
    }
}

function _cvRenderColumnGrids(leftCols, rightCols) {
    // Each row: [✓] left_col   →   [right_col ▾]
    // The right dropdown defaults to the same name when it exists on the
    // right side, otherwise to the first available right column. Lets the
    // user pair columns whose names differ across the two CSVs.
    const _opt = (cols, sel) => '<option value="">(skip)</option>' +
        cols.map(c => `<option value="${_cvEsc(c)}"${c===sel?' selected':''}>${_cvEsc(c)}</option>`).join('');

    const _pickRight = (leftName) => rightCols.includes(leftName) ? leftName : (rightCols[0] || '');

    const pkGrid = document.getElementById('cv-pk-grid');
    pkGrid.innerHTML = leftCols.map(c => {
        const def = _pickRight(c);
        return `<div style="display:flex; gap:8px; align-items:center; font-size:12px; padding:1px 0;">
            <label style="display:flex; gap:4px; align-items:center; min-width:160px;">
                <input type="checkbox" name="cv-pk" value="${_cvEsc(c)}" data-left="${_cvEsc(c)}">
                ${_cvEsc(c)}
            </label>
            <span style="color:var(--jp-ui-font-color2);">→ right:</span>
            <select name="cv-pk-right" data-left="${_cvEsc(c)}" style="min-width:140px;">${_opt(rightCols, def)}</select>
        </div>`;
    }).join('');

    const cmpGrid = document.getElementById('cv-compare-grid');
    cmpGrid.innerHTML = leftCols.map(c => {
        const def = _pickRight(c);
        return `<div style="display:flex; gap:8px; align-items:center; font-size:12px; padding:1px 0;">
            <label style="display:flex; gap:4px; align-items:center; min-width:160px;">
                <input type="checkbox" name="cv-cmp" value="${_cvEsc(c)}" data-left="${_cvEsc(c)}">
                ${_cvEsc(c)}
            </label>
            <span style="color:var(--jp-ui-font-color2);">→ right:</span>
            <select name="cv-cmp-right" data-left="${_cvEsc(c)}" style="min-width:140px;">${_opt(rightCols, def)}</select>
        </div>`;
    }).join('');
}

function _cvCheckedPairs(checkboxName, selectName) {
    // Returns [{left, right}, ...] for every checked checkbox whose paired
    // dropdown has a non-empty selection.
    const out = [];
    const checked = document.querySelectorAll(`input[name="${checkboxName}"]:checked`);
    for (const cb of checked) {
        const left = cb.dataset.left;
        const sel = document.querySelector(`select[name="${selectName}"][data-left="${left.replace(/"/g, '\\"')}"]`);
        const right = sel ? sel.value : left;
        if (!right) continue;
        out.push({left, right});
    }
    return out;
}

async function cvRun() {
    const lf = document.getElementById('cv-left').files[0];
    const rf = document.getElementById('cv-right').files[0];
    if (!lf || !rf) return;

    const pkPairs  = _cvCheckedPairs('cv-pk',  'cv-pk-right');
    const cmpPairs = _cvCheckedPairs('cv-cmp', 'cv-cmp-right');
    const nEx = parseInt(document.getElementById('cv-n-examples').value || '10', 10);

    const status = document.getElementById('cv-status');
    if (!pkPairs.length)  { status.textContent = 'Select at least one primary-key column.'; status.style.color='var(--jp-error-color0)'; return; }
    if (!cmpPairs.length) { status.textContent = 'Select at least one column to compare.'; status.style.color='var(--jp-error-color0)'; return; }
    status.textContent = 'Running…'; status.style.color = 'var(--jp-ui-font-color2)';

    const fd = new FormData();
    fd.append('left', lf);
    fd.append('right', rf);
    fd.append('pk_cols',           pkPairs.map(p => p.left).join(','));
    fd.append('pk_cols_right',     pkPairs.map(p => p.right).join(','));
    fd.append('compare_cols',      cmpPairs.map(p => p.left).join(','));
    fd.append('compare_cols_right', cmpPairs.map(p => p.right).join(','));
    fd.append('n_examples', String(nEx));

    try {
        const res = await fetch('/api/csv_compare/run', {method: 'POST', body: fd});
        const data = await res.json();
        if (!res.ok) {
            status.textContent = `Compare failed: ${data.error || res.status}`;
            status.style.color = 'var(--jp-error-color0)';
            return;
        }
        status.textContent = '';
        _cvRenderResult(data, pkPairs.map(p => p.left));
    } catch (e) {
        status.textContent = `Network error: ${e.message}`;
        status.style.color = 'var(--jp-error-color0)';
    }
}

function _cvRenderResult(data, pkCols) {
    document.getElementById('cv-result-card').style.display = '';

    // Run log: surface every backend step (parse, normalize, merge, per-col)
    // so the user sees what happened instead of just "running…".
    const logs = data.logs || [];
    let logHtml = '';
    if (logs.length) {
        const body = logs.map(l => _cvEsc(l)).join('\n');
        logHtml = `<details class="rc-details" open>
            <summary class="rc-details-summary">
                <span style="font-weight:600;">Run log</span>
                <span class="rc-details-counts">${logs.length} entries</span>
            </summary>
            <div class="rc-details-body">
                <pre style="font-family:var(--jp-code-font-family,monospace); font-size:11px; white-space:pre-wrap; margin:0;">${body}</pre>
            </div>
        </details>`;
    }

    const s = data.summary;
    let normNote = '';
    if (Array.isArray(data.pk_normalizations) && data.pk_normalizations.length) {
        const items = data.pk_normalizations
            .map(n => `<code>${_cvEsc(n.column)}</code> — ${_cvEsc(n.applied)}`)
            .join('; ');
        normNote = `<div style="font-size:11px; color:var(--jp-ui-font-color2); margin-top:6px;">
            Auto-normalized PK: ${items}
        </div>`;
    }
    document.getElementById('cv-summary').innerHTML = `
        <div class="rc-summary-chips">
            <span class="rc-chip match">${s.matched.toLocaleString()} matched</span>
            <span class="rc-chip left-only">${s.only_left.toLocaleString()} L-only</span>
            <span class="rc-chip right-only">${s.only_right.toLocaleString()} R-only</span>
            <span class="rc-chip mismatch">${s.total_mismatches.toLocaleString()} mismatches across ${s.cols_with_mismatches} col${s.cols_with_mismatches===1?'':'s'}</span>
        </div>
        ${normNote}
        ${logHtml}
    `;

    document.getElementById('cv-only-side').innerHTML =
        _cvRenderOnlySide('Left-only keys',  data.only_left_examples,  pkCols, s.only_left) +
        _cvRenderOnlySide('Right-only keys', data.only_right_examples, pkCols, s.only_right);

    const cols = data.columns || [];
    document.getElementById('cv-per-col').innerHTML = cols.map(c => _cvRenderCol(c, pkCols)).join('');
}

function _cvRenderOnlySide(title, examples, pkCols, total) {
    if (!total) return '';
    const head = '<thead><tr>' + pkCols.map(c => `<th>${_cvEsc(c)}</th>`).join('') + '</tr></thead>';
    const body = '<tbody>' + examples.map(ex =>
        '<tr>' + pkCols.map(c => `<td>${_cvEsc(ex[c])}</td>`).join('') + '</tr>'
    ).join('') + '</tbody>';
    return `
    <details class="rc-details">
        <summary class="rc-details-summary">
            <span style="font-weight:600;">${title}</span>
            <span class="rc-details-counts">${total.toLocaleString()} total — showing first ${examples.length}</span>
        </summary>
        <div class="rc-details-body">
            <table class="data-table compact">${head}${body}</table>
        </div>
    </details>`;
}

function _cvRenderCol(col, pkCols) {
    if (col.skipped) {
        return `<details class="rc-details">
            <summary class="rc-details-summary">
                <span style="font-weight:600;">${_cvEsc(col.name)}</span>
                <span class="rc-details-counts">skipped — ${_cvEsc(col.reason || '')}</span>
            </summary>
        </details>`;
    }

    const open = col.n_unmatched > 0 ? 'open' : '';
    const head = '<thead><tr>' +
        pkCols.map(c => `<th>${_cvEsc(c)}</th>`).join('') +
        '<th>LEFT</th><th>RIGHT</th></tr></thead>';
    const body = '<tbody>' + (col.examples || []).map(ex =>
        '<tr>' +
        pkCols.map(c => `<td>${_cvEsc(ex.pk[c])}</td>`).join('') +
        `<td>${_cvEsc(ex.left)}</td><td>${_cvEsc(ex.right)}</td>` +
        '</tr>'
    ).join('') + '</tbody>';

    const statusText = col.n_unmatched === 0
        ? '<span style="color:var(--jp-success-color0);">all matched</span>'
        : `<span style="color:var(--jp-warn-color0);">${col.n_unmatched.toLocaleString()} unmatched</span>`;

    return `<details class="rc-details" ${open}>
        <summary class="rc-details-summary">
            <span style="font-weight:600;">${_cvEsc(col.name)}</span>
            <span class="rc-details-counts">${statusText}</span>
        </summary>
        <div class="rc-details-body">
            ${col.n_unmatched > 0
                ? `<table class="data-table compact">${head}${body}</table>`
                : '<div class="empty-message">No mismatches in this column.</div>'}
        </div>
    </details>`;
}
