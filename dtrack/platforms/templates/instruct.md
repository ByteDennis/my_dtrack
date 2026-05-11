# SAS column-extraction templates — what they do and how

This folder contains the SAS templates Python fills in to produce
`extract_col.sas` — the script that pulls column statistics from
Oracle or Hadoop/Hive and writes one CSV per table.

This README is aimed at someone who has written a `proc sql` statement
before but hasn't worked with multi-file macro pipelines or `call
execute`. Every concept used here is explained inline. The two files
this README covers in detail:

- `column_oracle.sas` — Oracle passthrough column extraction.
- `column_hadoop.sas` — Hive/Hadoop passthrough column extraction.

The SAS macro `_col_categorical_freq` (defined in
`platforms/oracle.py` and embedded into the generated script) is also
explained — it post-processes the freq table that the templates pull.


## What the script does, in one paragraph

For each (table, column) pair in your `dtrack.json`, the script:

1. Opens a passthrough connection to Oracle or Hadoop.
2. Issues a `SELECT … FROM <table>` that returns column statistics
   (count, missing, distinct, min, max, mean, std, top-10 values).
3. Saves the result to a permanent SAS dataset `cache._cs_<dsname>`.
4. After every column on a table is processed, exports
   `cache._cs_<dsname>` to `<qname>_col.csv` for the dtrack web UI
   to load.


## What the final CSV looks like (example)

After extracting the `AMT` and `STATUS` columns from a Hadoop table
called `db.transactions` over four years with `vintage=year`, the
exported `hadoop_transactions_col.csv` would contain rows like:

```
dt           column_name     col_type      n_total   n_missing  n_unique   mean       std        min_val   max_val   top_10                                length_avg  length_max  hash_sum
2022-01-01   AMT             numeric       9_842_173 12         8_421_553  185.32     421.04     0         99999.99                                                                
2023-01-01   AMT             numeric       9_991_044 8          8_534_120  187.10     419.88     0         99999.99                                                                
2024-01-01   AMT             numeric       10_115_887 5         8_660_002  189.45     422.10     0         99999.99                                                                
2025-01-01   AMT             numeric       2_103_400 2          1_855_120  188.92     420.55     0         99999.99                                                                
2022-01-01   STATUS          categorical   9_842_173 4_201       3         OPEN=...   CLOSED=... OPEN=5234 PENDING=10  OPEN(5234); CLOSED(2419); PENDING(2189)
2023-01-01   STATUS          categorical   9_991_044 4_355       3         OPEN=...   CLOSED=... OPEN=...  PENDING=... OPEN(5301); CLOSED(2502); PENDING(2188)
...
```

Read this row by row:

- `dt` — the **vintage bucket label**. With `vintage=year`, it's the
  year's January 1st as a string. With `vintage=month`, it's a per-
  month bucket. With `vintage=all`, it's literally `"all"`.
- `column_name` — the column's name. Same row's value across all dt
  buckets for one column.
- `col_type` — `numeric` or `categorical` (determined from the
  Oracle/Hive DDL type).
- `n_total` — `COUNT(*)` for that bucket (including rows where the
  column is NULL).
- `n_missing` — number of rows where the column is NULL.
- `n_unique` — number of distinct non-null values (or HyperLogLog
  approximation if `approx_distinct` is enabled).
- `mean` / `std` — for numeric: arithmetic mean/std. For categorical:
  rank-weighted on alphabetically-sorted distinct values (a
  permutation invariant — the same set of (value, cnt) pairs in any
  order produces the same mean/std).
- `min_val` / `max_val` — for numeric: scalar min/max. For
  categorical: `"<alpha-first value>=<its count>"` /
  `"<alpha-last value>=<its count>"`. The `=count` suffix is the
  "weight" of the boundary value; it's stripped during column-compare
  so a SAS `SF=1283` and an Athena `SF=1280` match on the value.
- `top_10` — semicolon-separated `value(count)` pairs for the ten
  most frequent values. Numeric columns leave this blank.
- `length_avg` / `length_max` / `hash_sum` — coarse structural drift
  signals, only meaningful on categorical columns. `length_*` is the
  weighted-by-count average and max of `LENGTH(value)`. `hash_sum` is
  a portable 16-bit MD5-truncated weighted checksum that AGREES
  cross-platform on identical data (the same recipe runs on Athena's
  side, so a Hadoop `SUM(cnt × hash16)` matches an Athena
  `SUM(cnt × hash16)` byte-for-byte for the same input rows).

The dtrack web UI loads this CSV into the `_col_stats` table; the
`col_compare` page reads from there to render side-by-side
comparisons.


## How the file is structured

A generated `extract_col.sas` is built by concatenating, in order:

1. **Credentials block** (from `template.sas`) — SAS macro variables
   for Oracle/Hadoop login, set from `.env` values Python read at
   generation time. Looks like `%let iamusr = alice; %let pb23_pwd
   = …;`.
2. **`%let _coarse_mode = …;` + retry knobs** — global toggles that
   the column macros consult at run time.
3. **`%macro pb23 …`** — the PROC SQL CONNECT macro. Expanding
   `%pb23` inside a `proc sql` body emits the `connect to oracle
   (user=… orapw=… path=…)` statement.
4. **Helper macros** — `_export_col_stats`, `_col_categorical_freq`,
   `_run_one_ora_table`, `_run_one_hdp_table`.
5. **The driver datasets** (`_ora_col_map`, `_hdp_col_map`) — one
   row per (table, column, vintage-bucket) for Oracle, or per (table,
   column) for Hadoop.
6. **`%_run_one_ora_table(qname=…, dsname=…)`** invocations — one per
   table. Each kicks off the full pipeline for that table.

Sections 5 and 6 are emitted by Python from your `dtrack.json`. Sections
1–4 are mostly fixed code from the templates.


## The driver dataset — what `_ora_col_map` is for

```sas
data _ora_col_map;
    length qname $64 dsname $32 conn_macro $32
           col_name $64 col_expr $2000 col_type $12
           date_col $64 vintage $12 vintage_expr $500
           dt_label $32
           date_from $64 date_to $64
           from_table $500 where_clause $2000
           n_unique_expr $500;
    qname='oracle_transactions'; dsname='oracle_transac'; conn_macro='pb23';
    col_name='AMT';  col_expr='AMT'; col_type='numeric';
    date_col='DT';   vintage='year'; vintage_expr=`'2022-01-01'`;
    dt_label='2022-01-01';
    date_from='DATE ''2022-01-01'''; date_to='DATE ''2025-12-31''';
    from_table='db.transactions';
    where_clause='(DT BETWEEN DATE ''2022-01-01'' AND DATE ''2022-12-31'')';
    n_unique_expr='COUNT(DISTINCT AMT)'; output;
    /* … one row per (column × vintage bucket) … */
run;
```

Each row is a **work order**: "here is one (column, bucket) to extract,
along with everything the per-column macro needs to know to build the
right SQL". The `length` statement declares each variable's width up
front so SAS doesn't infer it from the first row (which would truncate
later longer values — see comment in the template).

Why this shape? Two reasons:

1. **Inspectable.** Before running the script you can
   `proc print data=_ora_col_map(keep=qname col_name dt_label
   where_clause);` and eyeball every bucket to verify the date
   bounds and WHERE fragments are correct.
2. **Macro-driven.** A single per-column macro reads these fields and
   emits the SQL — no per-row hard-coded SQL block in the script,
   even though we have hundreds of (column, bucket) pairs.


## The dispatcher — `data _null_; … call execute(…); run;`

```sas
data _null_;
    set _ora_col_map;
    where qname = "&qname";
    length _cmd $4000;
    _cmd = cats(
        'data _null_;',
        ' call symputx("_qname", ',        quote(strip(qname)),        ');',
        ' call symputx("_col_name", ',     quote(strip(col_name)),     ');',
        ' call symputx("_dt_label", ',     quote(strip(dt_label)),     ');',
        ' …',
        ' call symputx("_where_clause", ', quote(strip(where_clause)), ');',
        ' run; ',
        '%nrstr(%_col_oracle)();'
    );
    call execute(_cmd);
run;
```

This is one of the more SAS-idiomatic patterns and deserves a slow read.

- `data _null_;` — a data step that doesn't produce a dataset; it's
  here purely for its side effects.
- `set _ora_col_map; where qname = "&qname";` — iterates one row at
  a time over the work orders for this table.
- For each row, build a string `_cmd` (up to 4000 chars). The string
  contains literal SAS code — a small `data _null_` step that sets
  macro variables from the row's values, plus a call to the
  `%_col_oracle()` macro.
- `quote(strip(col_name))` wraps the value in double quotes
  for safe embedding in SAS code. The `cats(...)` glues everything
  into a single string.
- `call execute(_cmd);` doesn't *run* `_cmd` immediately — it
  **queues** it to run after the enclosing data step finishes.
- `%nrstr(%_col_oracle)()` — the `%nrstr` masks the `%` so SAS
  doesn't try to expand `_col_oracle` *during* the data step's
  compile phase. The actual `%_col_oracle()` invocation only
  triggers once the data step ends and call_execute drains its
  queue.

Net effect: SAS first builds N command strings (one per work order),
then runs them in order. Each command sets macro vars from its row
and invokes `%_col_oracle()`. So the per-column macro sees its row's
data via `&_qname`, `&_col_name`, `&_dt_label`, etc.

Why this dance? Because PROC SQL passthrough can't be parameterized
inline — Oracle/Hive expects literal SQL, not bind variables. So we
splice values into the SQL via macro substitution, one row at a time.


## The per-column macro — `%_col_oracle()`

This is where the actual database query lives. Annotated walkthrough
of the macro body for ONE work order (one column, one bucket):

```sas
%macro _col_oracle();
    %local _cache _rc _t0 _elapsed _attempt _max_attempts _sleep_secs;
    %let _cache = cache._cs_&_dsname;            /* permanent SAS dataset for this table */
    %let _t0    = %sysfunc(datetime());          /* timer start for the WARNING- DONE banner */

    /* Resume support: if (column_name, dt) is already in the cache from
       a previous run, skip this bucket. Set SAS_COL_REDO=1 when generating
       the SAS to force a full re-extract. */
    %local _already_done;  %let _already_done = 0;
    %if &_col_redo = 0 and %sysfunc(exist(&_cache)) %then %do;
        proc sql noprint;
            select count(*) into :_already_done trimmed
            from &_cache
            where strip(column_name) = "&_col_name"
              and strip(dt)          = "&_dt_label";
        quit;
    %end;
    %if &_already_done > 0 %then %do;
        %put NOTE: already in cache -- skipping (resume);
        %return;   /* exit the macro, the next column's call execute fires */
    %end;
```

**What `%return;` does:** like Python's `return` inside a function — it
exits the macro early without running anything below. The dispatcher's
next queued call_execute snippet then runs the next column.

```sas
%_ora_retry_loop:                              /* label for %goto on transient failures */
    %let _attempt = %eval(&_attempt + 1);
    proc sql;
        %&_conn_macro                          /* expands to: connect to oracle (user=…) */
        %if %upcase(&_col_type) = NUMERIC %then %do;
            create table _c_one as
            select * from connection to oracle (
                SELECT &_vintage_expr AS dt,    /* literal '2022-01-01' for this bucket */
                       CAST(&_col_name_lit AS VARCHAR2(64)) AS column_name,
                       'numeric' AS col_type,
                       COUNT(*) AS n_total,
                       COUNT(*) - COUNT(&_col_expr) AS n_missing,
                       &_n_unique_expr AS n_unique,
                       &_mean_expr AS mean,
                       &_std_expr  AS std,
                       TO_CHAR(MIN(&_col_expr)) AS min_val,
                       TO_CHAR(MAX(&_col_expr)) AS max_val,
                       CAST('' AS VARCHAR2(4000)) AS top_10,
                       CAST('' AS VARCHAR2(32))   AS length_avg,
                       CAST('' AS VARCHAR2(32))   AS length_max,
                       CAST('' AS VARCHAR2(64))   AS hash_sum
                FROM &_from_table
                WHERE &_where_clause
            );
        %end;
        %else %do;
            /* Categorical: pull a (dt, p_col, cnt) freq table only;
               SAS post-processes via %_col_categorical_freq. */
            create table _ora_freq_ as
            select * from connection to oracle (
                SELECT &_vintage_expr AS dt,
                       SUBSTR(TO_CHAR(&_col_expr), 1, 200) AS p_col,
                       COUNT(*) AS cnt
                FROM &_from_table
                WHERE &_where_clause
                GROUP BY SUBSTR(TO_CHAR(&_col_expr), 1, 200)
            );
        %end;
        disconnect from oracle;
    quit;
```

Important things to understand here:

- **Macro expansion happens before SQL parsing.** `&_col_name` is
  replaced with the column name (e.g. `AMT`) by SAS *before* the
  resulting SQL is sent to Oracle. So Oracle sees
  `SELECT '2022-01-01' AS dt, CAST('AMT' AS VARCHAR2(64)) AS column_name, …`,
  not the `&_…` placeholders.
- **`from connection to oracle (…)`** is the SQL passthrough syntax.
  Everything inside the parentheses is sent to Oracle verbatim;
  SAS doesn't parse it. This lets you use Oracle-specific functions
  (`TRUNC`, `STDDEV_SAMP`, `ORA_HASH`, …) that SAS PROC SQL doesn't
  recognize.
- **Numeric vs categorical branches.** Numeric pulls a one-row
  aggregate; categorical pulls a freq table and SAS does the rest.

```sas
    %let _rc = &SYSERR;                        /* return code from previous proc step */
    %if &_rc > 4 %then %do;                    /* > 4 means error (not just NOTE/WARNING) */
        %if &_attempt < &_max_attempts %then %do;
            %put WARNING: attempt &_attempt of &_max_attempts failed -- sleeping;
            options obs=max nosyntaxcheck;     /* clear SAS's "skip the rest" flag */
            data _null_; rc = sleep(&_sleep_secs, 1); run;
            %goto _ora_retry_loop;             /* jump back to the label */
        %end;
        %put WARNING: skipping column after &_max_attempts attempts;
        %return;
    %end;
```

`%goto _ora_retry_loop` jumps back to the label set earlier — the same
control-flow construct as `goto` in C. The retry loop wraps the entire
proc sql block; after sleep, it re-runs from the label.

```sas
    /* Categorical only: post-process the freq table SAS-side */
    %if %upcase(&_col_type) NE NUMERIC %then %do;
        %_col_categorical_freq(freq_ds=_ora_freq_, col=&_col_name, out_ds=_c_one);
        proc delete data=_ora_freq_; run;
    %end;

    /* Normalize widths -- ensure length statement applies to _c_one
       even if the SQL output inferred shorter widths. */
    data _c_one;
        length dt $32 column_name $64 col_type $12 top_10 $4000
               length_avg $32 length_max $32 hash_sum $32;
        set _c_one;
    run;

    /* Append this column's row(s) to the cache. proc append doesn't
       overwrite -- it accumulates one column's stats at a time
       across all the call_execute invocations. */
    proc append base=&_cache data=_c_one force; run;
    proc delete data=_c_one; run;

    %let _elapsed = %sysevalf(%sysfunc(datetime()) - &_t0);
    %put WARNING- ==== [&_qname / &_col_name] DONE in %sysfunc(putn(&_elapsed, 8.2))s ====;
%mend _col_oracle;
```

- **`force` on proc append** lets the append succeed even if `_c_one`
  and `&_cache` have small schema differences (length mismatch, extra
  column). Without `force`, a column-length disagreement aborts the
  whole step.
- **`WARNING-`** (the dash prefix) prints in the log as a plain NOTE
  but in a color that stands out — useful for marking timing
  milestones without polluting the actual WARNING channel.


## The categorical post-processor — `%_col_categorical_freq`

Defined as a Python-side string and embedded once near the top of the
generated script. The full body is in
`platforms/oracle.py:_SAS_COL_HELPERS`.

It takes the freq table the macro just pulled (`_ora_freq_`, with
columns `dt, p_col, cnt`) and produces a single-row output (`_c_one`)
matching the schema of the numeric path.

Key steps it performs:

1. **Per-dt totals.** `n_total = SUM(cnt)`, `n_missing = SUM(cnt)
   where p_col is missing`, `n_unique = count of non-missing distinct
   values`.
2. **Alpha rank.** Sorts distinct values by `upcase(strip(p_col))`,
   then `p_col`, assigning `rnk = 1..k` per dt bucket.
3. **Rank-weighted mean/std.** `mean = SUM(cnt × rnk) / SUM(cnt)`.
   `std = sqrt(SUM(cnt × rnk²) / SUM(cnt) - mean²)`. This is a
   **permutation-invariant** statistic — the same set of (value, cnt)
   pairs in any order produces the same mean/std, but two different
   sets produce different stats. Useful as a drift signal.
4. **Alpha-first / alpha-last min/max.** The value at `rnk = 1` is
   the min, at `rnk = k` is the max, each emitted as
   `"value=count"`.
5. **Top-10.** Sorts the freq table by descending `cnt` and joins the
   first ten as `"value(count); value(count); …"`.

The macro then merges these by `dt`, sets the literal `column_name =
"&col"`, and outputs to `_c_one`. The caller appends `_c_one` to the
cache.


## How Hadoop differs from Oracle

`column_hadoop.sas` is structurally identical to `column_oracle.sas`,
just with `connect to hadoop (…)` instead of `connect to oracle (…)`
and Hive's SQL dialect inside the passthrough block (`CAST(x AS
STRING)`, `STDDEV_SAMP`, etc.).

The one big difference is the **DB-side GROUP BY**: Oracle uses
per-bucket queries (one passthrough per (column × bucket)) to avoid
`ORA-01652` TEMP-tablespace exhaustion. Hadoop uses ONE passthrough
per column with `GROUP BY vintage_expr` returning all buckets in one
shot, because Hive's per-task local-disk spill doesn't have Oracle's
hard TEMP wall. You can see this in the SQL:

```sql
-- Oracle, per-bucket
SELECT '2022-01-01' AS dt, COUNT(*), …
FROM big_t WHERE dt BETWEEN DATE '2022-01-01' AND DATE '2022-12-31'

-- Hadoop, all-buckets-in-one
SELECT concat(substr(dt,1,4),'-01-01') AS dt, COUNT(*), …
FROM big_t WHERE dt BETWEEN '20220101' AND '20251231'
GROUP BY concat(substr(dt,1,4),'-01-01')
```

The resume check is the same idea but slightly different keying:

- Oracle: skip if `(column_name, dt)` already in cache (per-bucket).
- Hadoop: skip if `column_name` already has *any* row in cache (the
  single Hive query produces all-or-nothing across buckets, so seeing
  any row means the previous run completed the column).


## Export — `%_export_col_stats`

After all columns for a table have processed, the table's
`_run_one_*_table` macro calls `%_export_col_stats(dsname=…, qname=…,
outdir=…)`:

```sas
%macro _export_col_stats(dsname=, qname=, outdir=);
    %if %sysfunc(exist(cache._cs_&dsname)) %then %do;
        data _colstats_;
            set cache._cs_&dsname;
            length dt_fmt $10;
            if vtype(dt)='N' then dt_fmt = put(dt, yymmdd10.);
            else dt_fmt = strip(vvalue(dt));
            drop dt; rename dt_fmt = dt;
        run;
        proc export data=_colstats_
            outfile="&outdir./&qname._col.csv"
            dbms=csv replace;
        run;
        proc delete data=_colstats_; run;
        %put NOTE: Exported col stats for &qname;
    %end;
    %else %do;
        %put WARNING: No cache found at cache._cs_&_cds -- nothing exported for &qname;
    %end;
%mend _export_col_stats;
```

- Copies the cache to a temp dataset `_colstats_`.
- Normalizes `dt` so numeric SAS dates print as `YYYY-MM-DD`. For our
  Oracle/Hadoop paths, `dt` is already a character variable holding
  the bucket label, so the `else` branch runs and `dt_fmt` is just a
  trimmed copy.
- `proc export` writes `<qname>_col.csv` with one row per (column,
  bucket).
- The cache stays put so re-runs can resume.


## Per-table run macros — `%_run_one_ora_table` / `%_run_one_hdp_table`

These are the entry points Python emits at the bottom of the script,
one per qualified table name. Body of `_run_one_ora_table`:

```sas
%macro _run_one_ora_table(qname=, dsname=);
    %_table_start_banner(qname=&qname, dsname=&dsname);

    /* Wipe cache only on forced redo (env SAS_COL_REDO=1). */
    %if &_col_redo = 1 %then %do;
        %if %sysfunc(exist(cache._cs_&dsname)) %then %do;
            proc delete data=cache._cs_&dsname; run;
        %end;
    %end;

    /* Create empty cache if missing; resume preserves existing rows. */
    %if not %sysfunc(exist(cache._cs_&dsname)) %then %do;
        data cache._cs_&dsname;
            length dt $32 column_name $64 col_type $12 top_10 $4000
                   mean $32 std $32 min_val $400 max_val $400
                   length_avg $32 length_max $32 hash_sum $32
                   n_total 8 n_missing 8 n_unique 8;
            stop;        /* stop; produces an empty dataset with this schema */
        run;
    %end;

    /* Count the work for the progress display */
    %local _n_total;
    proc sql noprint;
        select count(*) into :_n_total trimmed
        from _ora_col_map where qname = "&qname";
    quit;

    /* Drain the dispatcher: queues all per-column macro calls */
    data _null_;
        set _ora_col_map;
        where qname = "&qname";
        length _cmd $4000;
        _cmd = cats('data _null_;', /* … see "The dispatcher" section above … */);
        call execute(_cmd);
    run;

    %_table_done_footer(qname=&qname, dsname=&dsname);
%mend _run_one_ora_table;
```

The `_table_start_banner` / `_table_done_footer` macros print start
and DONE lines (with timing) at the open-code level. They take args
explicitly rather than relying on macro vars so the banners
resolve at macro-invoke time — no dependence on call_execute
ordering.


## SAS concepts cheat sheet

Quick reference for the SAS idioms used throughout these templates.

### Macro language vs. data step language

There are TWO interpreters running:

- **Macro language** — `%let`, `%macro`, `%if`, `%do`, `%goto`,
  `%sysfunc`, `&macro_var`. Runs FIRST, before SAS executes anything.
  Its job is to produce SAS code as text. The macro language can't
  read row values from a dataset directly.
- **Data step language** — `data _null_; set …; run;`,
  `if … then …`, `call symputx`, `call execute`. Runs SECOND,
  on actual data. Can read row values and pass them back to the
  macro layer via `call symputx`.

`call execute` is the bridge between them: a data step reads a row,
builds a string containing macro-language code, and queues it for
the macro layer to run later.

### Macro variable scope

- `%local x;` — declares a macro variable visible only inside the
  current macro. Avoids leaking state to other macros.
- `%global x;` — declares a macro variable visible everywhere.
- `&x` — substitute the value of macro variable `x`.
- `&&x&n` — double `&&` is for indirect resolution (rarely needed
  here).
- `%symexist(x)` — true if macro variable `x` has been declared.
- `%superq(x)` — get the value of `x` without re-resolving any `&` /
  `%` in it (useful when the value contains quotes or special chars).

### `proc append base=X data=Y force; run;`

Adds the rows of `Y` to the end of `X`. With `force`, schema
mismatches are tolerated (lengths get padded/truncated, missing
columns become missing values). Without `force`, schema mismatches
abort the step. This is the accumulation primitive used across all
the per-column work orders in one table.

### `proc sql; … connect to oracle (…); … from connection to oracle (…); disconnect; quit;`

PROC SQL passthrough. The text inside `connection to oracle (…)` is
sent to Oracle verbatim; SAS doesn't parse it. Anything outside
those parentheses (inside the proc sql block) is regular SAS SQL.
You can mix the two: `create table _c_one as select * from
connection to oracle (SELECT … FROM …);` — outer is SAS, inner is
Oracle.

### `proc sql noprint; select … into :name trimmed from …; quit;`

Run a SAS SQL query and stuff the single returned value into the
macro variable `:name`. Useful for "how many rows match this
filter?" lookups (used by the resume check).

### `&SYSERR`

A SAS automatic macro variable holding the return code of the most
recent step. `0` = success, `>4` = error worth reacting to. Always
read it immediately after the step; later steps reset it.

### `options obs=max nosyntaxcheck;`

When a proc step errors, SAS often silently puts itself into
"syntax-check only" mode for the rest of the program — subsequent
steps compile but don't actually run, which makes debugging
miserable. This option resets it so the retry loop's next attempt
actually executes.

### Quoting inside passthrough SQL

Inside `connection to oracle (…)`, SQL single quotes need to be
**doubled** when they're inside a SAS string literal. So the SAS
source

```sas
where_clause='(DT BETWEEN DATE ''2022-01-01'' AND DATE ''2022-12-31'')';
```

produces the macro var value

```
(DT BETWEEN DATE '2022-01-01' AND DATE '2022-12-31')
```

which Python ultimately spliced into the driver dataset with each
single quote internally doubled.


## Common errors and what they mean

| Symptom in log | Likely cause |
|---|---|
| `ORA-01652: unable to extend temp segment` | Oracle TEMP tablespace exhausted on a wide `COUNT(DISTINCT)` or `GROUP BY`. Reduce date range, enable `approx_distinct`, or rely on the per-bucket query splitting already in place. |
| `ORA-01858: non-numeric character was found where a numeric was expected` | Comparison between a NUMBER column and a DATE literal (or vice versa). Usually means `_column_meta` reported a different type than the column actually has, or the column has sentinel values like `-999` being compared with DATE literals. |
| `ERROR: error trying to establish connection: couldn't open client transport with JDBC Uri …` | Transient Hive connection failure. The retry loop catches this — re-runs the proc step after the configured cooldown. |
| `WARNING: attempt N of M failed -- sleeping` | The retry loop firing. If you see this and then a successful DONE banner on a later attempt, no action needed. If all M attempts fail, the column is skipped (logged as `skipping column`). |
| `WARNING: No cache found at cache._cs_<dsname> -- nothing exported` | The per-column macros never appended anything for this table. Probably all columns errored out or all were already in cache before run. Check the `_col_redo` flag and the cache contents. |
| Numeric column appears with `min_val=-999, max_val=...` and skewed `mean` | The column has sentinel values being treated as real data. Add `-999` to the column's data via a `where` clause exclusion, or enable `NULL_SENTINELS` in `constants.py` to translate them to NULL at extraction time. |
| `SYSERR=1012` (or similar) after a proc step | Generic error code; the WARNING line preceding it carries the real diagnostic. The retry loop will fire if it's > 4. |


## How to inspect what the script will do before running

The driver dataset is left in WORK after the run macros execute, so:

```sas
proc print data=_ora_col_map(keep=qname col_name dt_label where_clause); run;
```

shows every (table, column, bucket) the script will pull and the WHERE
clause that narrows each one. If any look wrong (wrong date format,
missing column), fix the dtrack config and re-generate before running.

You can also pull just one column for testing by editing
`_ora_col_map` to keep only that row:

```sas
data _ora_col_map; set _ora_col_map; where col_name = 'AMT'; run;
```

…before invoking `%_run_one_ora_table(qname=…, dsname=…);`.


## Knobs you can adjust

These macro variables are emitted at the very top of the generated
script (Python writes `%let …;` lines from `dtrack.json` /
`constants.py` / env). You can edit the values directly in the
generated SAS, no regenerate needed:

| Macro var | Default | Effect |
|---|---|---|
| `_coarse_mode` | 0 | When 1, blanks `mean / std / top_10` in the output CSV (coarse-stats mode). |
| `_col_redo` | 0 | When 1, wipes `cache._cs_<dsname>` at the start of each table run. Default keeps the cache so an interrupted run can resume. |
| `_ora_retry_count` | 3 | Max attempts on transient Oracle connection failure per column query. |
| `_ora_retry_sleep` | 30 | Seconds between Oracle retries. |
| `_hdp_retry_count` | 3 | Max attempts on transient Hadoop connection failure. |
| `_hdp_retry_sleep` | 30 | Seconds between Hadoop retries. |

To force a full re-extract from the command line:

```bash
SAS_COL_REDO=1 dtrack gen-sas …
```

To bump Hadoop retry tolerance:

```bash
DTRACK_HDP_RETRY_COUNT=5 DTRACK_HDP_RETRY_SLEEP=60 dtrack gen-sas …
```

Or edit the `%let` lines at the top of the generated `extract_col.sas`
directly before running it.
