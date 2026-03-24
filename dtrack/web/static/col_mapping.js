/* col_mapping.js — Column Mapping page logic */

// ---------------------------------------------------------------------------
// SVG icons
// ---------------------------------------------------------------------------
const ICON = {
    chevron: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"/></svg>`,
    details: `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg>`,
};

// ---------------------------------------------------------------------------
// Navigation
// ---------------------------------------------------------------------------
function navigateToStep(step) {
    window.location.href = '/' + step;
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let pairsData = [];
let mappingCache = {};  // pair_name -> {mappings, rules, sources, left_columns, right_columns}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', async () => {
    await loadPairs();
});

async function loadPairs() {
    try {
        const res = await fetch('/api/status');
        const data = await res.json();
        pairsData = data.pairs || [];
        renderAccordion();
    } catch (e) {
        document.getElementById('pairs-accordion').innerHTML =
            '<div class="empty-message">Failed to load pairs.</div>';
    }
}

// ---------------------------------------------------------------------------
// Render accordion
// ---------------------------------------------------------------------------
function renderAccordion() {
    const container = document.getElementById('pairs-accordion');
    if (!pairsData.length) {
        container.innerHTML = '<div class="empty-message">No pairs configured.</div>';
        return;
    }
    container.innerHTML = pairsData.map(p => pairAccordionHTML(p)).join('');
}

function pairAccordionHTML(p) {
    const name = p.pair_name;
    return `
    <div class="rc-pair" id="pair-${name}">
        <div class="rc-pair-header" onclick="togglePair('${name}')">
            <span class="pair-expand">${ICON.chevron}</span>
            <span class="pair-name">${name}</span>
            <span class="rc-pair-status" id="status-${name}">
                <span class="status-badge warning">${p.col_mappings || 0} mapped</span>
            </span>
        </div>
        <div class="rc-pair-body" id="body-${name}">
            <div class="empty-message">Expand to load columns...</div>
        </div>
    </div>`;
}

// ---------------------------------------------------------------------------
// Accordion toggle — auto-load on first expand
// ---------------------------------------------------------------------------
function togglePair(name) {
    const el = document.getElementById(`pair-${name}`);
    const wasExpanded = el.classList.contains('expanded');
    el.classList.toggle('expanded');

    if (!wasExpanded && !mappingCache[name]) {
        loadColumns(name);
    }
}

// ---------------------------------------------------------------------------
// Load columns for a pair
// ---------------------------------------------------------------------------
async function loadColumns(name) {
    const body = document.getElementById(`body-${name}`);
    body.innerHTML = '<div class="empty-message">Loading columns...</div>';

    try {
        const res = await fetch(`/api/pairs/${name}/columns`);
        if (!res.ok) throw new Error(await res.text());
        const data = await res.json();

        // Build initial state
        const existingMappings = data.col_mappings || {};
        const autoMatch = data.auto_match || {};
        const rulesData = data.col_rules || {};
        const rules = rulesData.rules || [];
        const sources = rulesData.sources || {};

        // Merge existing + auto-matched
        const allMappings = { ...existingMappings };
        const allSources = { ...sources };
        for (const [left, right] of Object.entries(autoMatch)) {
            if (!(left in allMappings)) {
                allMappings[left] = right;
                allSources[left] = 'auto';
            }
        }

        mappingCache[name] = {
            mappings: allMappings,
            rules: rules,
            sources: allSources,
            left_columns: data.left_columns || {},
            right_columns: data.right_columns || {},
            source_left: data.source_left || 'left',
            source_right: data.source_right || 'right',
        };

        renderPairBody(name);
        updateStatusBadge(name);

        // Auto-sync if auto-match added new mappings
        if (Object.keys(autoMatch).length > 0) {
            debouncedSync(name);
        }
    } catch (e) {
        body.innerHTML = `<div class="empty-message" style="color:var(--jp-error-color0);">Error: ${e.message}</div>`;
    }
}

// ---------------------------------------------------------------------------
// Status badge
// ---------------------------------------------------------------------------
function updateStatusBadge(name) {
    const cache = mappingCache[name];
    if (!cache) return;

    const nMapped = Object.keys(cache.mappings).length;
    const nLeft = Object.keys(cache.left_columns).length;
    const nRight = Object.keys(cache.right_columns).length;
    const mappedRight = new Set(Object.values(cache.mappings));
    const unmappedLeft = Object.keys(cache.left_columns).filter(c => !(c in cache.mappings));
    const unmappedRight = Object.keys(cache.right_columns).filter(c => !mappedRight.has(c));
    const nUnmatched = unmappedLeft.length + unmappedRight.length;

    const el = document.getElementById(`status-${name}`);
    if (nUnmatched === 0) {
        el.innerHTML = `<span class="status-badge ready">${nMapped} mapped</span>`;
    } else {
        el.innerHTML = `<span class="status-badge warning">${nMapped} mapped, ${nUnmatched} unmatched</span>`;
    }
}

// ---------------------------------------------------------------------------
// Render pair body
// ---------------------------------------------------------------------------
function renderPairBody(name) {
    const body = document.getElementById(`body-${name}`);
    const cache = mappingCache[name];
    if (!cache) return;

    const { mappings, sources, left_columns, right_columns, rules } = cache;

    // Compute mapped and unmatched
    const mappedRight = new Set(Object.values(mappings));
    const unmappedLeft = Object.keys(left_columns).filter(c => !(c in mappings)).sort();
    const unmappedRight = Object.keys(right_columns).filter(c => !mappedRight.has(c)).sort();
    const nMapped = Object.keys(mappings).length;
    const nUnmatched = unmappedLeft.length + unmappedRight.length;

    // Summary
    let html = `<div class="cm-summary">${nMapped} mapped &middot; ${nUnmatched} unmatched</div>`;

    // Mapped section
    html += `
    <details class="rc-details" open>
        <summary class="rc-details-summary">
            ${ICON.details}
            <span>Mapped Columns (${nMapped})</span>
        </summary>
        <div class="rc-details-body">
            <div class="cm-table-wrap">
            <table class="data-table compact">
                <thead><tr>
                    <th>Left Column</th><th>L Type</th>
                    <th style="text-align:center; width:30px;"></th>
                    <th>Right Column</th><th>R Type</th>
                    <th>Source</th><th style="text-align:center; width:40px;">Unmap</th>
                </tr></thead>
                <tbody>
                ${Object.entries(mappings).sort((a,b) => a[0].localeCompare(b[0])).map(([left, right]) => {
                    const src = sources[left] || 'manual';
                    const srcClass = src.startsWith('rule') ? 'rule' : src;
                    return `<tr>
                        <td class="cm-col-name">${esc(left)}</td>
                        <td class="cm-col-type">${esc(left_columns[left] || '')}</td>
                        <td style="text-align:center; color:var(--jp-ui-font-color3);">&rarr;</td>
                        <td class="cm-col-name">${esc(right)}</td>
                        <td class="cm-col-type">${esc(right_columns[right] || '')}</td>
                        <td><span class="cm-source cm-source-${srcClass}">${esc(src)}</span></td>
                        <td style="text-align:center;">
                            <button class="btn-text cm-unmap-btn" onclick="unmapColumn('${name}', '${escAttr(left)}')" title="Unmap">&times;</button>
                        </td>
                    </tr>`;
                }).join('')}
                </tbody>
            </table>
            </div>
        </div>
    </details>`;

    // Unmatched section
    if (nUnmatched > 0) {
        html += `
        <details class="rc-details" open>
            <summary class="rc-details-summary">
                ${ICON.details}
                <span>Unmatched (${nUnmatched})</span>
            </summary>
            <div class="rc-details-body">
                <div class="cm-table-wrap">
                <table class="data-table compact">
                    <thead><tr>
                        <th>Side</th><th>Column</th><th>Type</th><th>Map to</th>
                    </tr></thead>
                    <tbody>
                    ${unmappedLeft.map(col => {
                        return `<tr>
                            <td><span class="cm-side-badge cm-side-left">LEFT</span></td>
                            <td class="cm-col-name">${esc(col)}</td>
                            <td class="cm-col-type">${esc(left_columns[col] || '')}</td>
                            <td>
                                <select class="cm-map-select" onchange="manualMap('${name}', '${escAttr(col)}', this.value, 'left')">
                                    <option value="">-- select right col --</option>
                                    ${unmappedRight.map(r => `<option value="${escAttr(r)}">${esc(r)} (${esc(right_columns[r] || '')})</option>`).join('')}
                                </select>
                            </td>
                        </tr>`;
                    }).join('')}
                    ${unmappedRight.map(col => {
                        return `<tr>
                            <td><span class="cm-side-badge cm-side-right">RIGHT</span></td>
                            <td class="cm-col-name">${esc(col)}</td>
                            <td class="cm-col-type">${esc(right_columns[col] || '')}</td>
                            <td>
                                <select class="cm-map-select" onchange="manualMap('${name}', this.value, '${escAttr(col)}', 'right')">
                                    <option value="">-- select left col --</option>
                                    ${unmappedLeft.map(l => `<option value="${escAttr(l)}">${esc(l)} (${esc(left_columns[l] || '')})</option>`).join('')}
                                </select>
                            </td>
                        </tr>`;
                    }).join('')}
                    </tbody>
                </table>
                </div>
            </div>
        </details>`;
    }

    // Rules section
    html += `
    <details class="rc-details">
        <summary class="rc-details-summary">
            ${ICON.details}
            <span>Rules (${rules.length})</span>
        </summary>
        <div class="rc-details-body">
            <div class="cm-table-wrap">
            <table class="data-table compact" id="rules-table-${name}">
                <thead><tr>
                    <th>Pattern Left</th><th>Pattern Right</th><th>Type</th>
                    <th style="text-align:center;">Matches</th>
                    <th style="text-align:center; width:80px;">Actions</th>
                </tr></thead>
                <tbody>
                ${rules.map((rule, idx) => ruleRowHTML(name, rule, idx)).join('')}
                </tbody>
            </table>
            </div>
            <div style="margin-top:8px; display:flex; gap:8px;">
                <button class="btn-secondary" onclick="addRule('${name}')">+ Add Rule</button>
                <button class="btn-primary" onclick="applyRules('${name}')">Apply Rules</button>
                <button class="btn-danger" onclick="clearAllMappings('${name}')">Clear All</button>
            </div>
        </div>
    </details>`;

    body.innerHTML = html;
}

function ruleRowHTML(name, rule, idx) {
    return `<tr>
        <td><input type="text" class="cm-rule-input" value="${escAttr(rule.pattern_left || '')}"
            data-pair="${name}" data-idx="${idx}" data-field="pattern_left"
            onchange="updateRule('${name}', ${idx}, 'pattern_left', this.value)"></td>
        <td><input type="text" class="cm-rule-input" value="${escAttr(rule.pattern_right || '')}"
            data-pair="${name}" data-idx="${idx}" data-field="pattern_right"
            onchange="updateRule('${name}', ${idx}, 'pattern_right', this.value)"></td>
        <td>
            <select class="cm-rule-type" onchange="updateRule('${name}', ${idx}, 'type', this.value)">
                <option value="wildcard" ${rule.type === 'wildcard' ? 'selected' : ''}>wildcard</option>
                <option value="regex" ${rule.type === 'regex' ? 'selected' : ''}>regex</option>
            </select>
        </td>
        <td style="text-align:center;" id="rule-matches-${name}-${idx}">
            <button class="btn-text" onclick="testRule('${name}', ${idx})">Test</button>
        </td>
        <td style="text-align:center;">
            <button class="btn-text cm-unmap-btn" onclick="deleteRule('${name}', ${idx})">&times;</button>
        </td>
    </tr>`;
}

// ---------------------------------------------------------------------------
// Manual mapping
// ---------------------------------------------------------------------------
function manualMap(name, leftCol, rightCol, fromSide) {
    if (!leftCol || !rightCol) return;
    const cache = mappingCache[name];
    if (!cache) return;

    cache.mappings[leftCol] = rightCol;
    cache.sources[leftCol] = 'manual';
    renderPairBody(name);
    updateStatusBadge(name);
    debouncedSync(name);
}

function unmapColumn(name, leftCol) {
    const cache = mappingCache[name];
    if (!cache) return;

    delete cache.mappings[leftCol];
    delete cache.sources[leftCol];
    renderPairBody(name);
    updateStatusBadge(name);
    debouncedSync(name);
}

// ---------------------------------------------------------------------------
// Rules
// ---------------------------------------------------------------------------
function addRule(name) {
    const cache = mappingCache[name];
    if (!cache) return;

    cache.rules.push({ pattern_left: '', pattern_right: '', type: 'wildcard' });
    renderPairBody(name);
}

function updateRule(name, idx, field, value) {
    const cache = mappingCache[name];
    if (!cache || !cache.rules[idx]) return;

    cache.rules[idx][field] = value;
}

function deleteRule(name, idx) {
    const cache = mappingCache[name];
    if (!cache) return;

    cache.rules.splice(idx, 1);
    // Re-derive sources (remove references to deleted/shifted rule indices)
    for (const [col, src] of Object.entries(cache.sources)) {
        if (src.startsWith('rule:')) {
            const ruleIdx = parseInt(src.split(':')[1]);
            if (ruleIdx === idx) {
                cache.sources[col] = 'manual';
            } else if (ruleIdx > idx) {
                cache.sources[col] = `rule:${ruleIdx - 1}`;
            }
        }
    }
    renderPairBody(name);
    debouncedSync(name);
}

function testRule(name, idx) {
    const cache = mappingCache[name];
    if (!cache || !cache.rules[idx]) return;

    const rule = cache.rules[idx];
    const mappedRight = new Set(Object.values(cache.mappings));
    const unmappedLeft = Object.keys(cache.left_columns).filter(c => !(c in cache.mappings));
    const unmappedRight = Object.keys(cache.right_columns).filter(c => !mappedRight.has(c));

    const result = applyColumnRulesJS([rule], unmappedLeft, unmappedRight);
    const n = Object.keys(result.mappings).length;

    const el = document.getElementById(`rule-matches-${name}-${idx}`);
    if (n > 0) {
        const preview = Object.entries(result.mappings).map(([l,r]) => `${l} &rarr; ${r}`).join('<br>');
        el.innerHTML = `<span class="cm-source cm-source-rule" title="${esc(JSON.stringify(result.mappings))}">${n} match</span>`;
        el.title = Object.entries(result.mappings).map(([l,r]) => `${l} -> ${r}`).join('\n');
    } else {
        el.innerHTML = '<span style="color:var(--jp-ui-font-color3);">0</span>';
    }
}

function applyRules(name) {
    const cache = mappingCache[name];
    if (!cache || !cache.rules.length) return;

    const mappedRight = new Set(Object.values(cache.mappings));
    const unmappedLeft = Object.keys(cache.left_columns).filter(c => !(c in cache.mappings));
    const unmappedRight = Object.keys(cache.right_columns).filter(c => !mappedRight.has(c));

    const result = applyColumnRulesJS(cache.rules, unmappedLeft, unmappedRight);

    for (const [left, right] of Object.entries(result.mappings)) {
        cache.mappings[left] = right;
    }
    Object.assign(cache.sources, result.sources);

    renderPairBody(name);
    updateStatusBadge(name);
    debouncedSync(name);
}

function clearAllMappings(name) {
    const cache = mappingCache[name];
    if (!cache) return;

    cache.mappings = {};
    cache.sources = {};
    renderPairBody(name);
    updateStatusBadge(name);
    debouncedSync(name);
}

// ---------------------------------------------------------------------------
// Client-side rule evaluation (mirrors Python apply_column_rules)
// ---------------------------------------------------------------------------
function applyColumnRulesJS(rules, unmappedLeft, unmappedRight) {
    const newMappings = {};
    const ruleSources = {};
    const rightSet = new Set(unmappedRight);

    for (let ruleIdx = 0; ruleIdx < rules.length; ruleIdx++) {
        const rule = rules[ruleIdx];
        const patLeft = rule.pattern_left || '';
        const patRight = rule.pattern_right || '';
        const ruleType = rule.type || 'wildcard';

        for (const leftCol of unmappedLeft) {
            if (leftCol in newMappings) continue;

            if (ruleType === 'wildcard') {
                const transformed = wildcardTransform(leftCol, patLeft, patRight);
                if (transformed && rightSet.has(transformed)) {
                    newMappings[leftCol] = transformed;
                    ruleSources[leftCol] = `rule:${ruleIdx}`;
                    rightSet.delete(transformed);
                }
            } else if (ruleType === 'regex') {
                try {
                    const re = new RegExp(patLeft, 'i');
                    const m = leftCol.match(re);
                    if (m && m[0] === leftCol) {
                        const transformed = leftCol.replace(re, patRight);
                        if (rightSet.has(transformed)) {
                            newMappings[leftCol] = transformed;
                            ruleSources[leftCol] = `rule:${ruleIdx}`;
                            rightSet.delete(transformed);
                        }
                    }
                } catch (e) {
                    // invalid regex, skip
                }
            }
        }
    }

    return { mappings: newMappings, sources: ruleSources };
}

function wildcardTransform(value, patFrom, patTo) {
    if (!patFrom.includes('*')) {
        return value === patFrom ? patTo : null;
    }

    // Convert fnmatch * pattern to regex
    let regex = '';
    for (const ch of patFrom) {
        if (ch === '*') regex += '(.*)';
        else regex += ch.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    const m = value.match(new RegExp(`^${regex}$`, 'i'));
    if (!m) return null;

    return patTo.replace('*', m[1]);
}

// ---------------------------------------------------------------------------
// Auto-sync (debounced save to server)
// ---------------------------------------------------------------------------
const _syncTimers = {};

function debouncedSync(name) {
    clearTimeout(_syncTimers[name]);
    _syncTimers[name] = setTimeout(() => syncPair(name), 600);
}

async function syncPair(name) {
    const cache = mappingCache[name];
    if (!cache) return;

    try {
        await fetch(`/api/pairs/${name}/col-mappings`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                mappings: cache.mappings,
                rules: {
                    rules: cache.rules,
                    sources: cache.sources,
                },
            }),
        });
        notify('Saved', 'success');
    } catch (e) {
        console.error(`Sync failed for ${name}:`, e);
        notify(`Save failed: ${e.message}`, 'error');
    }
}

// ---------------------------------------------------------------------------
// CSV Export
// ---------------------------------------------------------------------------
function downloadCSV(name) {
    window.open(`/api/pairs/${name}/col-mappings/csv`, '_blank');
}

async function downloadAllCSV() {
    // Download CSV for each pair that has mappings
    for (const p of pairsData) {
        if (mappingCache[p.pair_name] && Object.keys(mappingCache[p.pair_name].mappings).length > 0) {
            downloadCSV(p.pair_name);
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function esc(s) {
    if (!s) return '';
    return String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function escAttr(s) {
    if (!s) return '';
    return String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/"/g, '&quot;');
}

function notify(msg, type = 'success') {
    const el = document.getElementById('db-status');
    if (el) {
        el.style.background = type === 'error' ? 'var(--jp-error-color0)' : 'var(--jp-success-color0)';
        el.title = msg;
        setTimeout(() => { el.style.background = 'var(--jp-success-color0)'; }, 2000);
    }
}
