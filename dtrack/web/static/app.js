/* dtrack web UI — vanilla JS */

const $id = (id) => document.getElementById(id);
const output = $id("output");

function log(text) {
    output.textContent += text + "\n";
    output.scrollTop = output.scrollHeight;
}

function clearOutput() {
    output.textContent = "";
}

function setButtonLoading(btn, loading) {
    if (loading) {
        btn.classList.add("running");
        btn.disabled = true;
    } else {
        btn.classList.remove("running");
        btn.disabled = false;
    }
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

async function loadStatus() {
    try {
        const res = await fetch("/api/status");
        const data = await res.json();
        renderStatus(data);
    } catch (e) {
        log("Error loading status: " + e.message);
    }
}

function renderStatus(data) {
    const tbody = $id("status-body");
    const empty = $id("status-empty");

    if (!data.pairs || data.pairs.length === 0) {
        tbody.innerHTML = "";
        empty.style.display = "block";
        return;
    }
    empty.style.display = "none";

    // Auto-fill from-date: max_date_loaded + 1 day across all pairs
    let latestDate = null;
    for (const p of data.pairs) {
        for (const side of [p.left, p.right]) {
            if (side.max_date && (!latestDate || side.max_date > latestDate)) {
                latestDate = side.max_date;
            }
        }
    }
    if (latestDate) {
        const next = nextDay(latestDate);
        const fromInput = $id("from-date");
        if (!fromInput.value) {
            fromInput.value = next;
        }
    }

    tbody.innerHTML = data.pairs.map((p) => {
        const leftLoaded = formatDateRange(p.left);
        const rightLoaded = formatDateRange(p.right);
        const skipClass = p.skip ? ' style="opacity:0.4"' : "";
        return `<tr${skipClass}>
            <td>${esc(p.pair_name)}</td>
            <td>${esc(p.table_left)}</td>
            <td>${leftLoaded}</td>
            <td>${esc(p.table_right)}</td>
            <td>${rightLoaded}</td>
            <td>${p.col_mappings}</td>
            <td><button class="btn-delete" onclick="deletePair('${esc(p.pair_name)}')" title="Remove pair">x</button></td>
        </tr>`;
    }).join("");
}

function esc(s) {
    return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}

function formatDateRange(side) {
    if (!side.min_date && !side.max_date) {
        return '<span class="not-loaded">(not loaded)</span>';
    }
    const min = side.min_date || "?";
    const max = side.max_date || "?";
    return `<span class="date-loaded">${min} &rarr; ${max}</span>`;
}

function nextDay(dateStr) {
    try {
        const d = new Date(dateStr + "T00:00:00");
        d.setDate(d.getDate() + 1);
        return d.toISOString().slice(0, 10);
    } catch {
        return dateStr;
    }
}

// ---------------------------------------------------------------------------
// Config upload
// ---------------------------------------------------------------------------

async function uploadConfig(input) {
    const file = input.files[0];
    if (!file) return;

    clearOutput();
    log("Uploading config: " + file.name);

    const formData = new FormData();
    formData.append("file", file);

    try {
        const res = await fetch("/api/config/upload", {
            method: "POST",
            body: formData,
        });
        const data = await res.json();
        if (data.ok) {
            log(`Config loaded: ${data.pairs} pairs, ${data.registered} registered in DB`);
            loadStatus();
        } else {
            log("ERROR: " + (data.error || "unknown"));
        }
    } catch (e) {
        log("Error: " + e.message);
    }
    input.value = "";
}

// ---------------------------------------------------------------------------
// Pair management
// ---------------------------------------------------------------------------

function toggleAddPair() {
    const form = $id("add-pair-form");
    form.style.display = form.style.display === "none" ? "block" : "none";
}

async function addPair() {
    const pairName = $id("pair-name").value.trim();
    if (!pairName) { log("Pair name is required"); return; }

    const body = {
        pair_name: pairName,
        left: {
            source: $id("left-source").value.trim(),
            table: $id("left-table").value.trim(),
            conn_macro: $id("left-conn").value.trim(),
            date_col: $id("left-datecol").value.trim(),
        },
        right: {
            source: $id("right-source").value.trim(),
            table: $id("right-table").value.trim(),
            conn_macro: $id("right-conn").value.trim(),
            date_col: $id("right-datecol").value.trim(),
        },
    };

    if (!body.left.table || !body.right.table) {
        log("Both left and right table names are required");
        return;
    }

    clearOutput();
    log("Adding pair: " + pairName);

    try {
        const res = await fetch("/api/pairs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
        const data = await res.json();
        if (data.ok) {
            log(`Pair added: ${data.pair_name} (${data.table_left} / ${data.table_right})`);
            toggleAddPair();
            // Clear form
            for (const id of ["pair-name","left-source","left-table","left-conn","left-datecol",
                              "right-source","right-table","right-conn","right-datecol"]) {
                $id(id).value = "";
            }
            loadStatus();
        } else {
            log("ERROR: " + (data.error || "unknown"));
        }
    } catch (e) {
        log("Error: " + e.message);
    }
}

async function deletePair(pairName) {
    if (!confirm(`Remove pair "${pairName}" from config?`)) return;

    clearOutput();
    try {
        const res = await fetch(`/api/pairs/${encodeURIComponent(pairName)}`, {
            method: "DELETE",
        });
        const data = await res.json();
        if (data.ok) {
            log("Deleted pair: " + data.deleted);
            loadStatus();
        } else {
            log("ERROR: " + (data.error || "unknown"));
        }
    } catch (e) {
        log("Error: " + e.message);
    }
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

async function runInit() {
    clearOutput();
    log("Initializing database...");
    try {
        const res = await fetch("/api/init", { method: "POST" });
        const data = await res.json();
        log("Action: " + data.action);
        if (data.details) {
            for (const [k, v] of Object.entries(data.details)) {
                log("  " + k + ": " + v);
            }
        }
        loadStatus();
    } catch (e) {
        log("Error: " + e.message);
    }
}

async function runExtract(platform, type) {
    const btn = event.target;
    setButtonLoading(btn, true);
    clearOutput();
    log(`Extracting ${type} via ${platform}...`);

    const body = {
        platform,
        type,
        from_date: $id("from-date").value || null,
        to_date: $id("to-date").value || null,
    };

    try {
        const res = await fetch("/api/extract", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
        const data = await res.json();
        if (data.output) log(data.output);
        if (!data.ok) log("ERROR: " + (data.error || "unknown"));
    } catch (e) {
        log("Error: " + e.message);
    } finally {
        setButtonLoading(btn, false);
    }
}

async function runLoad(type) {
    const btn = event.target;
    setButtonLoading(btn, true);
    clearOutput();
    log(`Loading ${type} data...`);

    try {
        const res = await fetch("/api/load", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ type }),
        });
        const data = await res.json();
        if (data.output) log(data.output);
        if (!data.ok) log("ERROR: " + (data.error || "unknown"));
        loadStatus();
    } catch (e) {
        log("Error: " + e.message);
    } finally {
        setButtonLoading(btn, false);
    }
}

async function runCompare(type) {
    const btn = event.target;
    setButtonLoading(btn, true);
    clearOutput();
    log(`Comparing ${type}...`);

    try {
        const res = await fetch(`/api/compare/${type}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                from_date: $id("from-date").value || null,
                to_date: $id("to-date").value || null,
            }),
        });
        const data = await res.json();
        if (data.output) log(data.output);
        if (!data.ok) log("ERROR: " + (data.error || "unknown"));
    } catch (e) {
        log("Error: " + e.message);
    } finally {
        setButtonLoading(btn, false);
    }
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

async function runQuery() {
    const sql = $id("sql-input").value.trim();
    if (!sql) return;

    const resultDiv = $id("query-result");
    resultDiv.innerHTML = '<span class="muted">Running...</span>';

    try {
        const res = await fetch("/api/query", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ sql }),
        });
        const data = await res.json();

        if (data.error) {
            resultDiv.innerHTML = `<span style="color:#e94560">Error: ${esc(data.error)}</span>`;
            return;
        }

        // Write operations (DELETE, UPDATE, INSERT) return a message
        if (data.message) {
            resultDiv.innerHTML = `<span class="muted">${esc(data.message)}</span>`;
            return;
        }

        if (!data.rows || data.rows.length === 0) {
            resultDiv.innerHTML = '<span class="muted">(no rows)</span>';
            return;
        }

        let html = "<table><thead><tr>";
        for (const col of data.columns) {
            html += `<th>${esc(col)}</th>`;
        }
        html += "</tr></thead><tbody>";
        for (const row of data.rows) {
            html += "<tr>";
            for (const col of data.columns) {
                const val = row[col];
                html += `<td>${val !== null && val !== undefined ? esc(String(val)) : ""}</td>`;
            }
            html += "</tr>";
        }
        html += `</tbody></table><p class="muted">(${data.rows.length} row${data.rows.length !== 1 ? "s" : ""})</p>`;
        resultDiv.innerHTML = html;
    } catch (e) {
        resultDiv.innerHTML = `<span style="color:#e94560">Error: ${esc(e.message)}</span>`;
    }
}

// ---------------------------------------------------------------------------
// Annotations (where, time, comment)
// ---------------------------------------------------------------------------

function populatePairSelect(pairs) {
    const select = $id("annotate-pair");
    // Keep first option
    select.innerHTML = '<option value="">-- select --</option>';
    for (const p of pairs) {
        select.innerHTML += `<option value="${esc(p.pair_name)}">${esc(p.pair_name)}</option>`;
    }
}

async function loadAnnotations() {
    const pairName = $id("annotate-pair").value;
    const form = $id("annotate-form");
    if (!pairName) {
        form.style.display = "none";
        return;
    }

    try {
        const res = await fetch(`/api/pairs/${encodeURIComponent(pairName)}/annotations`);
        const data = await res.json();
        if (data.error) {
            log("Error: " + data.error);
            return;
        }

        const wm = data.where_map || {};
        $id("ann-where-left").value = wm.left || "";
        $id("ann-where-right").value = wm.right || "";

        const tm = data.time_map || {};
        $id("ann-time-row-left").value = (tm.row || {}).left || "";
        $id("ann-time-row-right").value = (tm.row || {}).right || "";
        $id("ann-time-col-left").value = (tm.col || {}).left || "";
        $id("ann-time-col-right").value = (tm.col || {}).right || "";

        const cm = data.comment_map || {};
        $id("ann-comment-row-left").value = (cm.row || {}).left || "";
        $id("ann-comment-row-right").value = (cm.row || {}).right || "";
        $id("ann-comment-col-left").value = (cm.col || {}).left || "";
        $id("ann-comment-col-right").value = (cm.col || {}).right || "";

        form.style.display = "block";
    } catch (e) {
        log("Error loading annotations: " + e.message);
    }
}

async function saveAnnotations() {
    const pairName = $id("annotate-pair").value;
    if (!pairName) return;

    const body = {
        where_map: {
            left: $id("ann-where-left").value,
            right: $id("ann-where-right").value,
        },
        time_map: {
            row: {
                left: $id("ann-time-row-left").value,
                right: $id("ann-time-row-right").value,
            },
            col: {
                left: $id("ann-time-col-left").value,
                right: $id("ann-time-col-right").value,
            },
        },
        comment_map: {
            row: {
                left: $id("ann-comment-row-left").value,
                right: $id("ann-comment-row-right").value,
            },
            col: {
                left: $id("ann-comment-col-left").value,
                right: $id("ann-comment-col-right").value,
            },
        },
    };

    try {
        const res = await fetch(`/api/pairs/${encodeURIComponent(pairName)}/annotations`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
        const data = await res.json();
        if (data.ok) {
            log(`Annotations saved for ${pairName}`);
        } else {
            log("ERROR: " + (data.error || "unknown"));
        }
    } catch (e) {
        log("Error: " + e.message);
    }
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

// Patch loadStatus to also populate the annotate pair dropdown
const _origRenderStatus = renderStatus;
renderStatus = function(data) {
    _origRenderStatus(data);
    if (data.pairs) {
        populatePairSelect(data.pairs);
    }
};

loadStatus();
