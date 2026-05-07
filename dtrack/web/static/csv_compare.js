/* csv_compare.js — string-exact diff between two uploaded CSVs */

function navigateToStep(step) {
    window.location.href = '/' + step;
}

let _cvLeftInfo  = null;
let _cvRightInfo = null;

function _cvEsc(s) {
    if (s == null) return '';
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
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
    const both = leftCols.filter(c => rightCols.includes(c));
    const all  = Array.from(new Set([...leftCols, ...rightCols]));

    const pkGrid = document.getElementById('cv-pk-grid');
    pkGrid.innerHTML = both.length
        ? both.map(c => `<label style="font-size:12px;"><input type="checkbox" name="cv-pk" value="${_cvEsc(c)}"> ${_cvEsc(c)}</label>`).join('')
        : '<span class="empty-message">No common columns between the two files.</span>';

    const cmpGrid = document.getElementById('cv-compare-grid');
    cmpGrid.innerHTML = all.map(c => {
        const onLeft  = leftCols.includes(c);
        const onRight = rightCols.includes(c);
        const flag = (onLeft && onRight) ? ''
                    : !onLeft  ? ' <span style="color:var(--jp-warn-color0); font-size:10px;">(R only)</span>'
                                : ' <span style="color:var(--jp-warn-color0); font-size:10px;">(L only)</span>';
        return `<label style="font-size:12px;"><input type="checkbox" name="cv-cmp" value="${_cvEsc(c)}"> ${_cvEsc(c)}${flag}</label>`;
    }).join('');
}

function _cvCheckedValues(name) {
    return Array.from(document.querySelectorAll(`input[name="${name}"]:checked`)).map(el => el.value);
}

async function cvRun() {
    const lf = document.getElementById('cv-left').files[0];
    const rf = document.getElementById('cv-right').files[0];
    if (!lf || !rf) return;

    const pkCols  = _cvCheckedValues('cv-pk');
    const cmpCols = _cvCheckedValues('cv-cmp');
    const nEx = parseInt(document.getElementById('cv-n-examples').value || '10', 10);

    const status = document.getElementById('cv-status');
    if (!pkCols.length)  { status.textContent = 'Select at least one primary-key column.'; status.style.color='var(--jp-error-color0)'; return; }
    if (!cmpCols.length) { status.textContent = 'Select at least one column to compare.'; status.style.color='var(--jp-error-color0)'; return; }
    status.textContent = 'Running…'; status.style.color = 'var(--jp-ui-font-color2)';

    const fd = new FormData();
    fd.append('left', lf);
    fd.append('right', rf);
    fd.append('pk_cols', pkCols.join(','));
    fd.append('compare_cols', cmpCols.join(','));
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
        _cvRenderResult(data, pkCols);
    } catch (e) {
        status.textContent = `Network error: ${e.message}`;
        status.style.color = 'var(--jp-error-color0)';
    }
}

function _cvRenderResult(data, pkCols) {
    document.getElementById('cv-result-card').style.display = '';

    const s = data.summary;
    document.getElementById('cv-summary').innerHTML = `
        <div class="rc-summary-chips">
            <span class="rc-chip match">${s.matched.toLocaleString()} matched</span>
            <span class="rc-chip left-only">${s.only_left.toLocaleString()} L-only</span>
            <span class="rc-chip right-only">${s.only_right.toLocaleString()} R-only</span>
            <span class="rc-chip mismatch">${s.total_mismatches.toLocaleString()} mismatches across ${s.cols_with_mismatches} col${s.cols_with_mismatches===1?'':'s'}</span>
        </div>
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
