// common.js — shared utilities across all dtrack pages

// ---------------------------------------------------------------------------
// Top-nav step dropdown — single source of truth for routes used by every
// page's <select id="step-selector">. Adding a new page = one entry here.
// ---------------------------------------------------------------------------
const STEP_ROUTES = {
    'pairs': '/pairs',
    'load_row': '/load_row',
    'row_compare': '/row_compare',
    'col_mapping': '/col_mapping',
    'col_gen': '/col_gen',
    'load_col': '/load_col',
    'col_compare': '/col_compare',
    'csv_compare': '/csv_compare',
    'playground': '/playground',
    'sync': '/sync',
};


// ===========================================================================
// Global pair selector
// ===========================================================================
//
// A sticky, cross-page set of "selected pair names" backed by localStorage.
// Pages that have per-page pair-pickers can consult this to pre-tick the
// right checkboxes on load, and write back when the user toggles. The goal
// is one source of truth so flipping pages doesn't lose your selection.
//
// API:
//   getGlobalSelectedPairs()        -> string[]  (the persisted set)
//   setGlobalSelectedPairs(arr)     -> void      (replace with arr)
//   toggleGlobalPair(name, on)      -> void      (add/remove one)
//   isGlobalPairSelected(name)      -> bool
//   clearGlobalSelectedPairs()      -> void
//   selectAllGlobalPairs(allNames)  -> void
//   onGlobalPairSelectionChange(fn) -> unsubscribe()
//
// Pages can listen for cross-tab updates via the storage event the
// browser fires when localStorage changes -- this implementation forwards
// those to subscribers so a selector tweak in one tab updates a peer tab.
//
// Empty selection means "all pairs" (the default before the user has
// touched anything). Pages should treat `[]` as "no narrow set yet, use
// whatever the API would return without filtering."

const _GLOBAL_PAIRS_KEY = 'dtrack-global-selected-pairs';
const _globalPairsSubscribers = new Set();

function getGlobalSelectedPairs() {
    try {
        const raw = localStorage.getItem(_GLOBAL_PAIRS_KEY);
        if (!raw) return [];
        const arr = JSON.parse(raw);
        return Array.isArray(arr) ? arr : [];
    } catch (e) { return []; }
}

function setGlobalSelectedPairs(arr) {
    const clean = Array.isArray(arr) ? [...new Set(arr.map(String))] : [];
    try { localStorage.setItem(_GLOBAL_PAIRS_KEY, JSON.stringify(clean)); }
    catch (e) {}
    _notifyGlobalPairsChange(clean);
}

function toggleGlobalPair(name, on) {
    const cur = new Set(getGlobalSelectedPairs());
    if (on) cur.add(name); else cur.delete(name);
    setGlobalSelectedPairs([...cur]);
}

function isGlobalPairSelected(name) {
    return getGlobalSelectedPairs().includes(name);
}

function clearGlobalSelectedPairs() {
    setGlobalSelectedPairs([]);
}

function selectAllGlobalPairs(allNames) {
    setGlobalSelectedPairs(allNames || []);
}

function onGlobalPairSelectionChange(fn) {
    _globalPairsSubscribers.add(fn);
    return () => _globalPairsSubscribers.delete(fn);
}

function _notifyGlobalPairsChange(arr) {
    for (const fn of _globalPairsSubscribers) {
        try { fn(arr); } catch (e) { /* swallow */ }
    }
    _renderGlobalPairsBadge();
}

// Cross-tab sync: when localStorage changes in another tab the browser
// fires a `storage` event in this tab. Forward to subscribers + refresh
// the badge so the header count stays accurate without a reload.
window.addEventListener('storage', (e) => {
    if (e.key === _GLOBAL_PAIRS_KEY) {
        _notifyGlobalPairsChange(getGlobalSelectedPairs());
    }
});

function _renderGlobalPairsBadge() {
    const badge = document.getElementById('global-pairs-badge');
    if (!badge) return;
    const arr = getGlobalSelectedPairs();
    badge.textContent = arr.length === 0 ? 'all' : String(arr.length);
}

// First render after DOM loads (in case the layout includes the badge).
document.addEventListener('DOMContentLoaded', _renderGlobalPairsBadge);

// ---- Modal-based picker -----------------------------------------------------
// openGlobalPairPicker(allPairsList) shows a modal with a checkbox per pair.
// `allPairsList` is [{name, ...}, ...]; the picker only needs `.name`. Each
// page that has the full pair list typically fetches it for its own purposes
// and can pass it in. The modal mounts itself lazily so pages without it
// don't pay any cost.

async function openGlobalPairPicker(allPairsList) {
    if (!allPairsList) {
        // Fetch the list if the caller didn't supply one. Use
        // /api/status (cheap; returns {pairs:[{pair_name,...}]}) which
        // every page already calls. Earlier this hit /api/pairs which
        // doesn't exist; the modal then rendered empty.
        try {
            const res = await fetch('/api/status');
            if (res.ok) {
                const data = await res.json();
                allPairsList = (data.pairs || []).filter(p => !p.skip);
            }
        } catch (e) { allPairsList = []; }
    }
    if (!Array.isArray(allPairsList)) allPairsList = [];
    const selected = new Set(getGlobalSelectedPairs());
    let modal = document.getElementById('global-pair-picker-modal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'global-pair-picker-modal';
        modal.className = 'modal';
        modal.style.cssText = 'display:flex; position:fixed; top:0; left:0; right:0; bottom:0; background:rgba(0,0,0,0.45); align-items:center; justify-content:center; z-index:9999;';
        modal.innerHTML = `
            <div class="modal-content" style="max-width:520px; max-height:70vh; display:flex; flex-direction:column; background:var(--jp-layout-color0, white); border-radius:6px; padding:0; box-shadow:0 4px 16px rgba(0,0,0,0.2);">
                <div class="modal-header" style="padding:14px 18px; border-bottom:1px solid var(--jp-border-color0); display:flex; justify-content:space-between; align-items:center;">
                    <h3 style="margin:0; font-size:14px;">Global Pair Selection</h3>
                    <button class="modal-close" onclick="closeGlobalPairPicker()" style="border:none; background:transparent; cursor:pointer; font-size:18px;">&times;</button>
                </div>
                <div class="modal-body" id="global-pair-picker-body" style="padding:14px 18px; overflow:auto; flex:1;">
                </div>
                <div class="modal-footer" style="padding:12px 18px; border-top:1px solid var(--jp-border-color0); display:flex; gap:8px; justify-content:space-between;">
                    <div>
                        <button class="btn-text" onclick="_gppToggleAll(true)">Select all</button>
                        <button class="btn-text" onclick="_gppToggleAll(false)">Clear</button>
                    </div>
                    <button class="btn-primary" onclick="closeGlobalPairPicker()">Done</button>
                </div>
            </div>
        `;
        document.body.appendChild(modal);
    } else {
        modal.style.display = 'flex';
    }
    const body = document.getElementById('global-pair-picker-body');
    body.innerHTML = allPairsList.map(p => {
        const name = p.name || p.pair_name || '';
        const checked = selected.has(name) ? 'checked' : '';
        return `<label style="display:flex; align-items:center; gap:8px; padding:4px 0; cursor:pointer; font-size:13px;">
            <input type="checkbox" data-pair="${name}" ${checked} onchange="_gppOnToggle(this)">
            ${name}
        </label>`;
    }).join('');
}

function closeGlobalPairPicker() {
    const modal = document.getElementById('global-pair-picker-modal');
    if (modal) modal.style.display = 'none';
}

function _gppOnToggle(input) {
    toggleGlobalPair(input.dataset.pair, input.checked);
}

function _gppToggleAll(on) {
    const inputs = document.querySelectorAll(
        '#global-pair-picker-body input[type="checkbox"]');
    const names = [];
    inputs.forEach(inp => {
        inp.checked = on;
        if (on) names.push(inp.dataset.pair);
    });
    setGlobalSelectedPairs(names);
}

function navigateToStep(step) {
    const url = STEP_ROUTES[step];
    if (url) window.location.href = url;
}

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
                    if (data.mapped !== undefined) summary += ` mapped=${data.mapped}`;
                    if (data.json_synced !== undefined) summary += ` json=${data.json_synced ? 'yes' : 'NO'}`;
                    if (data.json_error) summary += ` json_err: ${data.json_error}`;
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
