// common.js — shared utilities across all dtrack pages

// ---------------------------------------------------------------------------
// Refresh DB
// ---------------------------------------------------------------------------
async function refreshDB() {
    const btn = document.getElementById('refresh-db-btn');
    const dot = document.getElementById('db-status');
    if (btn) { btn.disabled = true; btn.textContent = 'Refreshing...'; }
    apiLog('POST /api/init', 'info');

    try {
        const resp = await fetch('/api/init', { method: 'POST' });
        const data = await resp.json();
        if (resp.ok) {
            const msg = `DB refreshed: ${data.action}` +
                (data.metadata_refreshed ? `, ${data.metadata_refreshed} metadata updated` : '');
            if (dot) { dot.style.background = 'var(--jp-success-color0)'; dot.title = msg; }
            if (btn) btn.textContent = 'Done';
            apiLog(msg, 'success');
            if (data.details) apiLog(JSON.stringify(data.details), 'info');
            setTimeout(() => { if (btn) { btn.textContent = 'Refresh DB'; btn.disabled = false; } }, 2000);
        } else {
            if (dot) { dot.style.background = 'var(--jp-error-color0)'; }
            if (btn) { btn.textContent = 'Failed'; btn.disabled = false; }
            apiLog(`Refresh failed: ${data.error || resp.status}`, 'error');
        }
    } catch (e) {
        if (dot) { dot.style.background = 'var(--jp-error-color0)'; }
        if (btn) { btn.textContent = 'Failed'; btn.disabled = false; }
        apiLog(`Refresh error: ${e.message}`, 'error');
    }
}

// ---------------------------------------------------------------------------
// API Log Panel — collapsible log at bottom of every page
// ---------------------------------------------------------------------------
let _logPanel = null;
let _logBody = null;
let _logVisible = false;

function _ensureLogPanel() {
    if (_logPanel) return;

    // Small toggle tab at bottom-right corner
    const tab = document.createElement('div');
    tab.id = 'api-log-tab';
    tab.style.cssText = 'position:fixed; bottom:0; right:16px; z-index:1001; background:var(--jp-layout-color2); border:1px solid var(--jp-border-color0); border-bottom:none; border-radius:6px 6px 0 0; padding:3px 10px; cursor:pointer; font-size:11px; color:var(--jp-ui-font-color2); user-select:none;';
    tab.textContent = 'API Log';
    tab.onclick = () => { _logVisible = !_logVisible; _logPanel.style.display = _logVisible ? '' : 'none'; };
    document.body.appendChild(tab);

    _logPanel = document.createElement('section');
    _logPanel.className = 'card';
    _logPanel.style.cssText = 'position:fixed; bottom:0; left:0; right:0; z-index:1000; max-height:35vh; display:none; flex-direction:column; margin:0; border-radius:8px 8px 0 0; box-shadow:0 -2px 12px rgba(0,0,0,0.2);';
    _logPanel.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:center; padding:6px 12px; border-bottom:1px solid var(--jp-border-color0);">
            <span style="font-size:12px; font-weight:600; text-transform:uppercase; letter-spacing:0.5px;">API Log</span>
            <div style="display:flex; gap:8px; align-items:center;">
                <span id="api-log-count" style="font-size:11px; color:var(--jp-ui-font-color2);">0</span>
                <button class="btn-text" onclick="_clearApiLog();" style="font-size:11px;">Clear</button>
                <button class="btn-text" onclick="_logVisible=false; _logPanel.style.display='none';" style="font-size:11px;">Close</button>
            </div>
        </div>
        <div id="api-log-body" style="overflow-y:auto; flex:1; padding:4px 12px; font-family:var(--jp-code-font-family,monospace); font-size:11px; max-height:30vh;"></div>
    `;
    document.body.appendChild(_logPanel);
    _logBody = document.getElementById('api-log-body');
}

function _clearApiLog() {
    if (_logBody) _logBody.innerHTML = '';
    _logCount = 0;
    const cnt = document.getElementById('api-log-count');
    if (cnt) cnt.textContent = '0';
}

let _logCount = 0;

function apiLog(message, type = 'info') {
    _ensureLogPanel();
    _logCount++;

    const time = new Date().toLocaleTimeString();
    const color = type === 'error' ? 'var(--jp-error-color0)' :
                  type === 'success' ? 'var(--jp-success-color0)' :
                  'var(--jp-ui-font-color1)';
    const entry = document.createElement('div');
    entry.style.cssText = `padding:2px 0; border-bottom:1px solid var(--jp-border-color0); color:${color};`;
    entry.innerHTML = `<span style="color:var(--jp-ui-font-color2);">${time}</span> ${_escHtml(message)}`;
    _logBody.appendChild(entry);
    _logBody.scrollTop = _logBody.scrollHeight;

    const cnt = document.getElementById('api-log-count');
    if (cnt) cnt.textContent = String(_logCount);
}

function _escHtml(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ---------------------------------------------------------------------------
// Intercept all fetch calls to log API requests/responses
// ---------------------------------------------------------------------------
// Initialize log panel tab on load (always visible)
document.addEventListener('DOMContentLoaded', () => _ensureLogPanel());

const _originalFetch = window.fetch;
window.fetch = async function(url, options = {}) {
    const method = (options.method || 'GET').toUpperCase();
    const path = typeof url === 'string' ? url : url.url || '';

    // Only log /api/ calls
    if (!path.includes('/api/')) {
        return _originalFetch.apply(this, arguments);
    }

    const shortPath = path.split('?')[0].replace('/api/', '');
    const t0 = performance.now();

    try {
        const resp = await _originalFetch.apply(this, arguments);
        const ms = Math.round(performance.now() - t0);
        const clone = resp.clone();
        const contentType = resp.headers.get('content-type') || '';

        if (!resp.ok) {
            // Error: full detail
            let bodySnippet = '';
            if (options.body) {
                try { bodySnippet = typeof options.body === 'string' ? options.body.slice(0, 300) : ''; }
                catch(e) {}
            }
            apiLog(`${method} ${shortPath} → ${resp.status} (${ms}ms)`, 'error');
            if (bodySnippet) apiLog(`  req: ${bodySnippet}`, 'error');

            if (contentType.includes('json')) {
                clone.json().then(data => {
                    if (data.error) apiLog(`  error: ${data.error}`, 'error');
                    if (data.output) {
                        data.output.trim().split('\n').forEach(line => {
                            if (line.trim()) apiLog(`  ${line}`, 'error');
                        });
                    }
                }).catch(() => {});
            }
        } else {
            // Success: one-line summary
            if (contentType.includes('json')) {
                clone.json().then(data => {
                    // Build a short summary from response keys
                    const keys = Object.keys(data);
                    let summary = '';
                    if (data.ok !== undefined) summary += data.ok ? 'ok' : 'FAIL';
                    if (data.loaded !== undefined) summary += ` loaded=${data.loaded}`;
                    if (data.matching !== undefined) summary += ` matching=${data.matching}`;
                    if (data.excluded !== undefined) summary += ` excluded=${data.excluded}`;
                    if (data.updated !== undefined) summary += ` updated=${data.updated}`;
                    if (data.action) summary += ` ${data.action}`;
                    if (data.error) summary += ` error: ${data.error}`;
                    if (!summary) summary = keys.slice(0, 4).join(',');
                    apiLog(`${method} ${shortPath} → 200 (${ms}ms) ${summary}`, 'info');
                    // Show server stdout if present
                    if (data.output) {
                        data.output.trim().split('\n').forEach(line => {
                            if (line.trim()) apiLog(`  ${line}`, 'info');
                        });
                    }
                }).catch(() => {
                    apiLog(`${method} ${shortPath} → 200 (${ms}ms)`, 'info');
                });
            } else {
                apiLog(`${method} ${shortPath} → ${resp.status} (${ms}ms)`, 'info');
            }
        }

        return resp;
    } catch (e) {
        apiLog(`${method} ${shortPath} → NETWORK ERROR: ${e.message}`, 'error');
        throw e;
    }
};
