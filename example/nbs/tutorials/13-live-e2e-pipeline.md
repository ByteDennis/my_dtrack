# 13 — Live end-to-end pipeline (`just e2e`)

This tutorial covers the `just e2e` recipe: a single entry point that drives
the **whole comparison pipeline against real backends** and emits the two
HTML reports. It also documents *why* the stages run in the order they do
(the ordering is load-bearing, not cosmetic) and the skip-cache that lets you
re-run cheaply.

> The granular recipes (`gen-aws`, `load-row`, `compare-row`, …) still exist
> for running one step by hand. `e2e` is the orchestrator that chains them in
> the correct order with the right flags.

---

## 1. Quick start

```sh
# full pipeline against the default config (testing/config.json)
just e2e all

# point at your own live config
just config=./my_live.json e2e all

# only the row half, or only the col half
just e2e row
just e2e col            # REQUIRES a prior `just e2e row` / `all` (see §4)

# skip the column auto-mapping step
just e2e all --skip-mapping
```

Outputs land under `{{e2edir}}/output/` (default `e2edir = testing`):

```
testing/output/compare_row.html
testing/output/compare_col.html
```

`type` defaults to `all`, so `just e2e` == `just e2e all`.

---

## 2. What each `type` does

| `type` | stages |
|--------|--------|
| `row`  | extract-row → load-row → **compare-row** (writes `_row_comparison`) |
| `col`  | load-columns → extract-col → load-col → match-columns → **compare-col** |
| `all`  | the `row` chain, then the `col` chain |

Under the hood every step is a `dtrack` subcommand. The recipe just sequences
them and gates the expensive ones (§5). The hand-written equivalent of
`just e2e all` is:

```sh
DB=testing/project.db ; CFG=testing/config.json ; OUT=testing/output

# --- row ---
dtrack run "$DB" --config "$CFG" --outdir testing --type row -y
#   = extract-row -> load-row -> compare-row  (compare_row.html + _row_comparison)

# --- col ---
dtrack load-columns "$DB" --config "$CFG"                 # discover -> _column_meta
dtrack run "$DB" --config "$CFG" --outdir testing --type col --skip-compare -y
#   = extract-col -> load-col   (col SQL filtered to matching dates)
dtrack match-columns "$DB" --config "$CFG" --yes          # auto col_map
dtrack compare-col "$DB" --config "$CFG" --html "$OUT/compare_col.html" -y
```

---

## 3. Why row must finish before col

This is the single most important rule and it is **enforced by the code, not
the docs**:

1. `compare-row` persists its result to the `_row_comparison` table —
   including the `matching_dates` list (the dates where left and right row
   counts agreed). See `save_row_comparison` in `dtrack/db.py`.
2. Column extraction **reads those matching dates back** and restricts its
   SQL to them: `compute_date_filter` → `get_row_comparison`
   (`dtrack/platforms/base.py`). So column stats are only ever computed on
   dates where the two sides already agreed on row counts — comparing column
   distributions on a partition that is missing half its rows would be noise.

Therefore col extraction is **meaningless until row-compare has run**. `all`
guarantees this by running the row chain first; running `just e2e col` on its
own assumes a prior `row`/`all` populated `_row_comparison` (it does **not**
re-run the row half for you — by design).

> ⚠️ The repo's `manuscript.txt` shows a single `dtrack run --type both`
> (extract everything, then compare). That ordering is **inconsistent with
> the current code** and appears outdated — col extraction there would run
> before any `_row_comparison` exists. Trust the code; `e2e` follows it.

---

## 4. Column mapping (`--skip-mapping`)

The col chain has a column-mapping step that some setups need and some don't.

- `match-columns` reads column names/types **only** from the `_column_meta`
  table and writes a `col_map` (left-col → right-col) used by `compare-col`.
- `_column_meta` is populated by `dtrack load-columns` (a live column-discovery
  query). Note: column *extraction* writes `_column_meta` only on
  **databricks/hadoop**, **not** on athena/oracle — so the explicit
  `load-columns` step is what makes auto-mapping work everywhere. That is why
  the col chain runs `load-columns` before `match-columns`.

**`--skip-mapping`** skips *both* `load-columns` and `match-columns`. The
pipeline then relies on:

- the `col_map` you hand-wrote in the config, plus
- `compare-col`'s fallback: when a column has no mapping it is compared against
  the **same-named** column on the other side (`compare_column_stats` in
  `dtrack/compare.py`).

Use `--skip-mapping` only when your config already carries `columns` for each
side (extraction needs them) and, ideally, a `col_map`. Without per-side
`columns` *and* without `load-columns`, col extraction prints
`No columns specified for …` and emits nothing.

---

## 5. The skip-cache (`.done` sentinels)

Re-running the whole pipeline shouldn't re-pull from the backend every time.
The three **expensive, backend-hitting** stages are cached:

| stage | sentinel |
|-------|----------|
| row extract   | `{{e2edir}}/.state/row.extract.done` |
| col discover  | `{{e2edir}}/.state/col.discover.done` |
| col extract   | `{{e2edir}}/.state/col.extract.done` |

Rules:

- A sentinel is written **only after that stage's command exits 0**, so a
  failed/interrupted extraction leaves no stamp and re-runs next time.
- While the sentinel exists, the stage is **skipped** (you'll see
  `>>> [row] extract: cached`).
- The **cheap local steps always re-run**: load, compare, match. This is
  deliberate — it means the DB is repopulated from the cached CSVs (even after
  a DB reset) and the HTML reports are always regenerated fresh.

Overrides:

```sh
just force=1 e2e all      # ignore the cache; re-run every expensive stage once
just e2e-clean            # delete {{e2edir}}/.state so the next run is fresh
```

This is intentionally **existence-only** caching: editing the config does *not*
auto-invalidate the cache. After changing the config (new table, new date
window, different where-clause) re-pull with `force=1` or `e2e-clean`.

---

## 6. Variables you can override

All are plain `just` variables (`just name=value e2e …`):

| var | default | meaning |
|-----|---------|---------|
| `db`     | `testing/project.db`   | SQLite project DB |
| `config` | `testing/config.json`  | unified pairs config (source of truth) |
| `e2edir` | `testing`              | base dir; outputs go to `e2edir/{csv,sas,output}`, cache to `e2edir/.state` |
| `force`  | `""`                   | set to `1` to bypass the skip-cache |

Example — a fully isolated live run:

```sh
just db=./prod.db config=./prod.json e2edir=./run-2026q2 e2e all
```

---

## 7. Prerequisites & gotchas

- **Credentials / env.** Live extraction needs the same env wiring as any
  `dtrack gen-*` (Oracle `<macro>_pwd`, Athena `AWS_*`, etc.). Run
  `just doctor` to verify resolution.
- **`columns` in the config.** Col extraction needs columns from *somewhere*:
  per-side `columns` in the config, or `_column_meta` from `load-columns`.
  The non-`--skip-mapping` path runs `load-columns`, so a bare config works;
  the `--skip-mapping` path needs `columns` pre-written.
- **`col` alone needs a prior `row` run** (see §3).
- **Skip-cache is existence-only** — `force=1` / `e2e-clean` after config edits.
- The pipeline is non-interactive (`-y` / `match-columns --yes`); it keeps
  auto-matched columns and never blocks on a prompt.
