/* s3.js — S3 browser: list / upload / download / delete with localStorage URL history */

const _S3_RECENT_KEY = 'dtrack-s3-recent';
const _S3_RECENT_MAX = 20;

let _s3State = {
    bucket: '',
    prefix: '',
    objects: [],
    folders: [],
    fileQueue: [],
};

// ---------------------------------------------------------------------------
// Remembered URLs (localStorage)
// ---------------------------------------------------------------------------
function _s3RecentLoad() {
    try {
        const raw = localStorage.getItem(_S3_RECENT_KEY);
        return raw ? JSON.parse(raw) : [];
    } catch (e) { return []; }
}

function _s3RecentSave(arr) {
    try { localStorage.setItem(_S3_RECENT_KEY, JSON.stringify(arr.slice(0, _S3_RECENT_MAX))); }
    catch (e) {}
}

function _s3RecentRemember(url) {
    if (!url) return;
    const norm = url.trim();
    if (!norm) return;
    const cur = _s3RecentLoad().filter(u => u !== norm);
    cur.unshift(norm);
    _s3RecentSave(cur);
    _s3RecentRender();
}

function _s3RecentRender() {
    const dl = document.getElementById('s3-url-list');
    const hint = document.getElementById('s3-recent-hint');
    if (!dl) return;
    const recent = _s3RecentLoad();
    dl.innerHTML = recent.map(u => `<option value="${_s3Esc(u)}">`).join('');
    if (hint) {
        hint.textContent = recent.length
            ? `Recent: ${recent.length} URL${recent.length === 1 ? '' : 's'} remembered (open the dropdown)`
            : 'No recent URLs yet — type a bucket and click Go.';
    }
}

function s3ClearHistory() {
    if (!confirm('Forget every remembered S3 URL?')) return;
    _s3RecentSave([]);
    _s3RecentRender();
}

// ---------------------------------------------------------------------------
// URL parsing
// ---------------------------------------------------------------------------
function _s3ParseUrl(url) {
    if (!url) return {bucket: '', prefix: ''};
    let u = url.trim();
    if (u.startsWith('s3://')) u = u.slice(5);
    const slash = u.indexOf('/');
    if (slash === -1) return {bucket: u, prefix: ''};
    return {bucket: u.slice(0, slash), prefix: u.slice(slash + 1)};
}

function _s3BuildUrl(bucket, prefix) {
    return `s3://${bucket}/${prefix || ''}`;
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------
function _s3SetStatus(text, kind) {
    const el = document.getElementById('s3-status');
    if (!el) return;
    el.textContent = text;
    el.style.color = kind === 'error' ? 'var(--jp-error-color0)'
                   : kind === 'ok'    ? 'var(--jp-success-color0)'
                   : 'var(--jp-ui-font-color2)';
}

function _s3Esc(s) {
    if (s == null) return '';
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function _s3FmtBytes(n) {
    if (n == null) return '—';
    const units = ['B','KB','MB','GB','TB'];
    let v = Number(n), i = 0;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
    return `${v.toFixed(v >= 100 ? 0 : v >= 10 ? 1 : 2)} ${units[i]}`;
}

function _s3FmtTs(iso) {
    if (!iso) return '—';
    try { return new Date(iso).toISOString().replace('T', ' ').slice(0, 19); }
    catch (e) { return iso; }
}

// ---------------------------------------------------------------------------
// Creds
// ---------------------------------------------------------------------------
async function s3RefreshCreds() {
    _s3SetStatus('Renewing credentials…');
    try {
        const res = await fetch('/api/s3/creds');
        const d = await res.json();
        if (!d.ok) {
            _s3SetStatus(`Creds error: ${d.error || res.status}`, 'error');
            return;
        }
        const ttl = d.ttl_seconds != null ? `${Math.floor(d.ttl_seconds / 60)}m left` : '(no TTL set)';
        const reg = d.region || 'no region';
        _s3SetStatus(`Creds: ${d.has_keys ? 'set' : 'missing'} · ${ttl} · ${reg}`, d.has_keys ? 'ok' : 'error');
    } catch (e) {
        _s3SetStatus(`Creds error: ${e.message}`, 'error');
    }
}

// ---------------------------------------------------------------------------
// Listing
// ---------------------------------------------------------------------------
async function s3Go() {
    const url = document.getElementById('s3-url').value.trim();
    const {bucket, prefix} = _s3ParseUrl(url);
    if (!bucket) { _s3SetStatus('Enter a bucket URL.', 'error'); return; }
    _s3State.bucket = bucket;
    _s3State.prefix = prefix;
    _s3RecentRemember(_s3BuildUrl(bucket, prefix));
    document.getElementById('s3-upload-btn').disabled = (_s3State.fileQueue.length === 0);
    _s3SetStatus(`Listing s3://${bucket}/${prefix}…`);
    try {
        const res = await fetch(`/api/s3/list?bucket=${encodeURIComponent(bucket)}&prefix=${encodeURIComponent(prefix)}`);
        const d = await res.json();
        if (!d.ok) {
            _s3SetStatus(`List error: ${d.error || res.status}`, 'error');
            return;
        }
        _s3State.objects = d.objects || [];
        _s3State.folders = d.folders || [];
        _s3RenderTable();
        _s3SetStatus(`${d.object_count} object${d.object_count === 1 ? '' : 's'} · ${_s3FmtBytes(d.total_size)}`, 'ok');
    } catch (e) {
        _s3SetStatus(`List error: ${e.message}`, 'error');
    }
}

function _s3RenderTable() {
    const tbody = document.getElementById('s3-tbody');
    const rows = [];

    // Parent link (when we're inside a subprefix)
    if (_s3State.prefix) {
        const parent = _s3State.prefix.replace(/\/$/, '').split('/').slice(0, -1).join('/');
        rows.push(`<tr>
            <td></td>
            <td><a href="#" onclick="s3Navigate('${_s3Esc(parent)}'); return false;">📁 ../</a></td>
            <td></td><td></td><td></td>
        </tr>`);
    }

    for (const f of _s3State.folders) {
        const display = f.key.slice(_s3State.prefix.length);
        rows.push(`<tr>
            <td></td>
            <td><a href="#" onclick="s3Navigate('${_s3Esc(f.key)}'); return false;">📁 ${_s3Esc(display)}</a></td>
            <td></td><td></td><td></td>
        </tr>`);
    }
    for (const o of _s3State.objects) {
        const display = o.key.slice(_s3State.prefix.length);
        rows.push(`<tr>
            <td><input type="checkbox" class="s3-row-check" data-key="${_s3Esc(o.key)}"></td>
            <td>📄 ${_s3Esc(display)}</td>
            <td style="text-align:right;">${_s3FmtBytes(o.size)}</td>
            <td>${_s3FmtTs(o.modified)}</td>
            <td>
                <button class="btn-text" onclick="s3DownloadOne('${_s3Esc(o.key)}')" title="Download">⬇</button>
                <button class="btn-text" onclick="s3DeleteOne('${_s3Esc(o.key)}')" title="Delete">×</button>
            </td>
        </tr>`);
    }

    if (rows.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="empty-message">Empty.</td></tr>';
    } else {
        tbody.innerHTML = rows.join('');
    }
    document.getElementById('s3-check-all').checked = false;
}

function s3Navigate(prefix) {
    document.getElementById('s3-url').value = _s3BuildUrl(_s3State.bucket, prefix);
    s3Go();
}

function s3ToggleAll(checked) {
    document.querySelectorAll('.s3-row-check').forEach(cb => { cb.checked = checked; });
}

function _s3SelectedKeys() {
    return Array.from(document.querySelectorAll('.s3-row-check'))
        .filter(cb => cb.checked).map(cb => cb.dataset.key);
}

// ---------------------------------------------------------------------------
// Download / Delete
// ---------------------------------------------------------------------------
function s3DownloadOne(key) {
    const url = `/api/s3/download?bucket=${encodeURIComponent(_s3State.bucket)}&key=${encodeURIComponent(key)}`;
    const a = document.createElement('a');
    a.href = url; a.download = '';
    document.body.appendChild(a); a.click(); a.remove();
}

function s3DownloadSelected() {
    const keys = _s3SelectedKeys();
    if (keys.length === 0) { _s3SetStatus('Select at least one object.', 'error'); return; }
    // Trigger sequential downloads — browsers throttle parallel auto-downloads.
    keys.forEach((k, i) => setTimeout(() => s3DownloadOne(k), i * 250));
    _s3SetStatus(`Downloading ${keys.length} object${keys.length === 1 ? '' : 's'}…`, 'ok');
}

async function s3DeleteOne(key) {
    if (!confirm(`Delete s3://${_s3State.bucket}/${key} ?`)) return;
    await _s3DoDelete([key]);
}

async function s3DeleteSelected() {
    const keys = _s3SelectedKeys();
    if (keys.length === 0) { _s3SetStatus('Select at least one object.', 'error'); return; }
    if (!confirm(`Delete ${keys.length} object${keys.length === 1 ? '' : 's'} from s3://${_s3State.bucket}/ ?`)) return;
    await _s3DoDelete(keys);
}

async function _s3DoDelete(keys) {
    _s3SetStatus(`Deleting ${keys.length} object${keys.length === 1 ? '' : 's'}…`);
    try {
        const res = await fetch('/api/s3/delete', {
            method: 'DELETE',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({bucket: _s3State.bucket, keys}),
        });
        const d = await res.json();
        if (!d.ok && (d.errors || []).length) {
            _s3SetStatus(`Delete: ${(d.deleted || []).length} ok, ${d.errors.length} failed`, 'error');
        } else {
            _s3SetStatus(`Deleted ${(d.deleted || []).length} object${(d.deleted || []).length === 1 ? '' : 's'}.`, 'ok');
        }
        s3Go();
    } catch (e) {
        _s3SetStatus(`Delete error: ${e.message}`, 'error');
    }
}

// ---------------------------------------------------------------------------
// New folder
// ---------------------------------------------------------------------------
async function s3NewFolder() {
    if (!_s3State.bucket) { _s3SetStatus('Set a bucket first.', 'error'); return; }
    const name = prompt('New folder name:');
    if (!name) return;
    try {
        const res = await fetch('/api/s3/new-folder', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({bucket: _s3State.bucket, prefix: _s3State.prefix, name}),
        });
        const d = await res.json();
        if (!d.ok) { _s3SetStatus(`Folder error: ${d.error}`, 'error'); return; }
        _s3SetStatus(`Created ${d.key}`, 'ok');
        s3Go();
    } catch (e) {
        _s3SetStatus(`Folder error: ${e.message}`, 'error');
    }
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------
function s3OnFiles(files) {
    _s3State.fileQueue = Array.from(files || []);
    _s3RenderQueue();
}

function s3OnDrop(event) {
    s3OnFiles(event.dataTransfer.files);
}

function _s3RenderQueue() {
    const q = document.getElementById('s3-upload-queue');
    if (_s3State.fileQueue.length === 0) {
        q.textContent = '';
        document.getElementById('s3-upload-btn').disabled = true;
        return;
    }
    q.innerHTML = 'Queued: ' + _s3State.fileQueue
        .map(f => `<code>${_s3Esc(f.name)}</code> (${_s3FmtBytes(f.size)})`).join(', ');
    document.getElementById('s3-upload-btn').disabled = !_s3State.bucket;
}

async function s3StartUpload() {
    if (!_s3State.bucket) { _s3SetStatus('Set a bucket first.', 'error'); return; }
    if (_s3State.fileQueue.length === 0) { _s3SetStatus('No files queued.', 'error'); return; }

    const fd = new FormData();
    fd.append('bucket', _s3State.bucket);
    fd.append('prefix', _s3State.prefix);
    fd.append('overwrite', document.getElementById('s3-overwrite').checked ? 'true' : 'false');
    for (const f of _s3State.fileQueue) fd.append('files', f, f.name);

    const btn = document.getElementById('s3-upload-btn');
    const progress = document.getElementById('s3-upload-progress');
    btn.disabled = true;
    progress.textContent = '⏳ uploading…';

    try {
        const xhr = new XMLHttpRequest();
        xhr.upload.addEventListener('progress', e => {
            if (e.lengthComputable) {
                const pct = Math.round(e.loaded / e.total * 100);
                progress.textContent = `▓▓▓▓▓░░░░ ${pct}%`;
            }
        });
        const result = await new Promise((resolve, reject) => {
            xhr.onload = () => {
                try { resolve({status: xhr.status, data: JSON.parse(xhr.responseText)}); }
                catch (e) { reject(new Error('parse error')); }
            };
            xhr.onerror = () => reject(new Error('network'));
            xhr.open('POST', '/api/s3/upload');
            xhr.send(fd);
        });
        if (result.status < 400 && result.data.ok) {
            const n = (result.data.results || []).filter(r => r.ok).length;
            _s3SetStatus(`Uploaded ${n} file${n === 1 ? '' : 's'}.`, 'ok');
            progress.textContent = '✓ done';
            _s3State.fileQueue = [];
            _s3RenderQueue();
            s3Go();
        } else {
            const failed = (result.data.results || []).filter(r => !r.ok);
            _s3SetStatus(`Upload: ${failed.length} failed — ${failed.map(r => r.error).join('; ')}`, 'error');
            progress.textContent = '';
        }
    } catch (e) {
        _s3SetStatus(`Upload error: ${e.message}`, 'error');
        progress.textContent = '';
    } finally {
        btn.disabled = false;
    }
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', () => {
    _s3RecentRender();
    // Restore last URL if any.
    const recent = _s3RecentLoad();
    if (recent.length) {
        document.getElementById('s3-url').value = recent[0];
    }
    s3RefreshCreds();
});
