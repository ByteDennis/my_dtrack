# 12 — Frontend (`web/static/*.js` + Jinja templates)

dtrack's frontend has **no framework**. No React, no Vue, no build step. Every
page is a Jinja-rendered HTML file that loads one or two `<script>` tags
from `web/static/`. Pages share a thin runtime in `common.js` (nav,
global pair selector, log panel, fetch helpers).

This tutorial walks through:

1. The shared runtime (`common.js`, `_layout.html`).
2. The per-page scripts.
3. How SSE is consumed.
4. How state persists (localStorage + the project DB).

## 1. `_layout.html` — the shell

Every page extends `_layout.html`. It defines:

- The top nav (uses `STEP_ROUTES` from `common.js`).
- A `<select id="step-selector">` for keyboard-friendly step navigation.
- The "log panel" toggle and container.
- The page title block (`{% block page_title %}`) and content block
  (`{% block content %}`).

Per-page templates set `current_page = "…"` so the nav highlights the right
step.

## 2. `common.js` — the shared runtime

### 2.1 `STEP_ROUTES`

Single source of truth for which slug maps to which route. Adding a new
page means adding a row here and creating `web/templates/<slug>.html`.

### 2.2 Global pair selector

`getGlobalSelectedPairs / setGlobalSelectedPairs / toggleGlobalPair /
selectAllGlobalPairs / onGlobalPairSelectionChange` — a sticky, cross-page,
cross-tab set of "the user picked these pairs". Backed by
`localStorage['dtrack-global-selected-pairs']`. Empty set means **all
pairs**.

Subscribers register a callback and get notified on every change, including
changes in other browser tabs (via the `storage` event). Used by
`pairs.js`, `col_gen.js`, `row_compare.js`, `col_compare.js` to keep the
"selected pairs" badge in sync.

`openGlobalPairPicker` renders a modal with checkboxes; `closeGlobalPairPicker`
dismisses it. Both are wired to a button in the nav bar.

### 2.3 Log panel

`_logPanel`, `_logBody`, `_logVisible` — DOM refs for the slide-up log
drawer. `logMessage(text, level)` is the public API every page uses to
print to the panel. Levels: `info` / `success` / `warning` / `error`.

The panel is collapsed by default; the user clicks a chevron to expand.

### 2.4 `refreshDB()`

POSTs `/api/refresh-metadata`, then reloads the page. Wired to the
"Refresh DB" button visible on every page.

### 2.5 `navigateToStep(step)`

Sets `window.location.href = STEP_ROUTES[step]`. The dropdown's `onchange`
calls this.

## 3. Per-page JS

### 3.1 `pairs.js` — the row-gen page

The longest JS file (~1650 lines). Three responsibilities:

1. **Pair list rendering** — fetches `/api/pairs/list`, renders cards with
   per-pair status badges (`/api/status/row`, `/api/status/col`),
   edit / delete buttons, drag-to-reorder.
2. **Preview + Generate flow** — POSTs `/api/extract/dry-run` to preview
   SQL, `/api/generate` to write `extract_*.sql/sas`. Renders the two-column
   review (SAS/Hadoop | AWS/Oracle/Databricks). Detection logic at lines
   ~1199–1212 decides which "Run X Extraction" buttons to show based on
   each pair's source.
3. **Run extraction** — `_runExtraction(platform, btnId)` POSTs to
   `/api/extract/run-sql`, reads the SSE stream with a manual
   `reader.read()` loop, parses each `event: …` block, and dispatches by
   eventType (`run_id` / `log` / `start` / `progress` / `done`).

The Databricks button we recently added lives in this file. The CSV runner
is special because it may need two passes (left + right outdirs); the
`_runCsvExtraction` helper composes both.

### 3.2 `row_compare.js` — the row-compare page

Fetches `/api/compare/row/{pair}`. Renders matching / non-matching /
excluded date sets as three columns the user can drag dates between. Saves
back via `PUT /api/compare/row/{pair}`. Auto-saves the matching set to
`_row_comparison` so `col_gen` can use the same bucket alignment.

### 3.3 `col_mapping.js` — the column-mapping page

Fetches `/api/pairs/{pair}/columns`. Renders left and right columns side by
side with auto-match suggestions (fuzzy name + wildcard transforms). The
trash button posts to `/api/pairs/{pair}/col_rejected` so deletions persist
across page reloads.

### 3.4 `col_gen.js` — the column-gen page

~1300 lines. Layout:

- Preset radio (`quick` / `standard` / `detailed` / `custom`). `custom`
  reveals a checkbox grid for the seven per-stat toggles.
- "Generate files" button → `/api/generate`. Logs `[BUCKET CHECK]` lines
  from the response (PASS green, WARNING amber).
- "Run X Extraction" / "Resume X" buttons per detected platform — same
  SSE consumer as `pairs.js`. The Databricks button we wired routes
  here too.

### 3.5 `col_compare.js` — the column-compare page

Renders the per-pair col-stat diff grid. Each cell is a small badge with
left/right values and a color-coded verdict. Clicking a cell opens a
drill-down panel that POSTs `/api/extract/dry-run` with the
`{pair, col, vintage}` filter, returning the SQL that re-extracts just that
cell. The user can copy the SQL into the Playground.

### 3.6 `load_row.js`, `load_col.js`, `csv_loader.js`

Drag-and-drop CSV uploaders. `csv_loader.js` is the shared upload helper
(progress bar, chunked POST). The row / col variants pre-fill the table
qname from the filename and POST to `/api/load/row/upload` or
`/api/load/col-stats/upload`.

### 3.7 `csv_compare.js`

Drives the `/csv_compare` page: pick two CSVs, POST `/api/csv_compare/inspect`
to confirm schema, POST `/api/csv_compare/run` to render the diff. Output
is rendered fully on the client — no DB writes.

### 3.8 `playground.js`

Covered in detail by our recent work. Lives at ~550 lines. Three parts:

1. **Form persistence** — every input the user fills (engine, conn, SQL,
   SAS controls, **use-cache toggle**, row cap, …) is mirrored into
   `localStorage['dtrack-playground:<id>']`. The `_PG_PERSIST_IDS` array
   is the list; the restore happens after `pgLoadConstants` fills dropdowns.
2. **Query stack** — `pgRun()` posts to `/api/playground/run`, handles
   cache hits / cancellations / errors, renders the result preview
   (`_pgRenderResult`). `pgCancel()` posts to
   `/api/playground/cancel_running`.
3. **History** — `pgLoadHistory()` fetches `/api/playground/history` and
   renders a per-row table with notes, copy-SQL, delete. Debounced
   note saves go to `PUT /api/playground/history/{id}`.

The download button **streams from the server** (`/api/playground/download/{cache_key}`)
so the browser never holds the full result in JS memory.

### 3.9 `sync.js`

Three-way diff between `dtrack.json`, `_table_pairs`, and `_metadata`.
Shows two columns ("JSON-only", "DB-only") and a button per side to
reconcile. The implementation is straight `fetch` + `innerHTML` — no
framework.

## 4. The SSE consumer pattern

Every page that consumes SSE has the same shape:

```javascript
const resp = await fetch('/api/extract/run-sql', {
    method: 'POST', signal: controller.signal, headers: …, body: JSON.stringify(reqBody),
});
const reader = resp.body.getReader();
const decoder = new TextDecoder();
let buffer = '';
while (true) {
    const {done, value} = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, {stream: true});
    const parts = buffer.split('\n\n');
    buffer = parts.pop();      // last chunk may be incomplete
    for (const part of parts) {
        let eventType = 'message', data = '';
        for (const line of part.split('\n')) {
            if (line.startsWith('event: ')) eventType = line.slice(7);
            else if (line.startsWith('data: ')) data = line.slice(6);
        }
        if (!data) continue;
        const msg = JSON.parse(data);

        if (eventType === 'run_id') { /* wire cancel */ }
        else if (eventType === 'log') logMessage(msg.line, msg.level || 'info');
        else if (eventType === 'start') { /* eta */ }
        else if (eventType === 'progress') { /* per-block update */ }
        else if (eventType === 'done') { /* summary */ }
    }
}
```

The `\n\n` split honors the SSE wire format. The `buffer = parts.pop()`
keeps a half-message at the boundary intact across `read()` calls. This is
duplicated in `pairs.js`, `col_gen.js`, and (in the CSV-only variant)
`_runCsvColSide` — the boilerplate is small enough we haven't yet pulled
it into a `common.js` helper.

The `controller = new AbortController()` + `signal: controller.signal`
pattern is paired with a manual `POST /api/extract/cancel/<run_id>` call.
The client aborts its own stream **after** telling the server to stop, so
the SSE half-message buffer doesn't get re-rendered after cancel.

## 5. Persistence across page loads

Two backends:

| Where             | What it stores                                                 |
| ----------------- | -------------------------------------------------------------- |
| `localStorage`    | UI ephemera: form values, expanded/collapsed panels, last-selected preset, global pair selection. Prefix `dtrack-*` everywhere. |
| Project DB        | Authoritative state: pair mappings, comparisons, playground history. |

The convention: if you want it back on next page load, write to
`localStorage`; if you want it back in another browser, write to the DB
via an `/api/...` call.

## 6. Templates that hold non-trivial markup

Most templates are mostly forms + empty divs that the JS fills. A few
hold meaningful content:

- `_layout.html` — global shell (nav, log, page title).
- `pairs.html` — the pair-list scaffold with placeholder cards.
- `col_gen.html` — the preset radio + custom-stats grid + run-buttons row.
- `playground.html` — the engine radio + connection inputs + SQL textarea
  + result/history cards.
- `col_compare.html` — the diff grid scaffolding (the grid itself is
  rendered by JS).
- `csv_compare.html` — drag-drop upload widgets.

The rest are ~30 lines of HTML each.

## 7. Style

Two stylesheets get loaded by `_layout.html`:

- A Jupyter-classic-derived palette (`--jp-*` CSS variables) so the
  pages match what users expect inside a Jupyter environment.
- Page-level inline styles for one-off bits.

There's no global CSS preprocessor — all styling is hand-written CSS in
either `<style>` tags or inline `style="…"` attrs.

## 8. What the frontend deliberately doesn't do

- **No client-side routing.** Every navigation is a full page reload.
  Simpler state model, no virtual-DOM cache to invalidate.
- **No reactive framework.** When a page mutates state, it calls a
  re-render function that rebuilds the relevant `innerHTML`. No diffing.
- **No bundling.** Every JS file is shipped as-is. Reload picks up the
  new version immediately.
- **No client-side auth.** dtrack is a developer tool, not a multi-tenant
  service. The CLI runs locally; the web app binds to `localhost` by
  default.

That's the whole frontend. The takeaway: every interactive feature is a
short JS file with a single concern. When adding a new page, copy
`load_row.html` + `load_row.js` as a starting skeleton and grow from there.

---

This concludes the tutorial series. Reading the 12 files in order will
give a working developer a complete mental model of dtrack — from the
SQLite schema up through the SSE stream the col-gen page consumes. Open
issues, tutorial drift, and roadmap items live in `example/nbs/`'s plain
text files; the canonical reference is always the code.
