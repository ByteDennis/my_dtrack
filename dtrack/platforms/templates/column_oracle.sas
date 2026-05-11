/* Oracle column-stats extraction (passthrough, all aggregation in DB).
   The SQL template is emitted ONCE in the %_col_oracle macro; the driver
   dataset carries only the per-row variables (col_name, col_type, dt_label,
   from_table, where_clause). This avoids blowing up the generated .sas file
   with a full SQL string per (col, bucket) combination.

   Placeholders:
     {COL_REDO}         - 0 or 1
     {ORA_COL_ROWS}     - inline data-step 'output;' rows feeding _ora_col_map
*/

%let _col_redo = /*{COL_REDO}*/;

/* Driver dataset. One row per (qname, col_name) -- vintage bucketing runs
   DB-side via GROUP BY &_vintage_expr.  Inspect with
       proc print data=_ora_col_map(keep=qname col_name date_col vintage
                                         vintage_expr date_from date_to);
   BEFORE running to verify the truncation expression matches the column's
   date type (e.g. DATE col -> TO_CHAR(TRUNC(col,'YYYY'),'YYYY-MM-DD'),
   string_compact -> concat(substr(col,1,4),'-01-01'), etc.). */
data _ora_col_map;
    length qname $64 dsname $32 conn_macro $32
           col_name $64 col_expr $2000 col_type $12
           date_col $64 vintage $12 vintage_expr $500
           dt_label $32
           date_from $64 date_to $64
           from_table $500 where_clause $2000
           n_unique_expr $500;
/*{ORA_COL_ROWS}*/
run;

%macro _col_oracle();
    /* Dispatcher sets these macro vars per (qname, col_name) before
       invoking this macro:
         _qname, _dsname, _conn_macro, _col_name, _col_type,
         _date_col, _vintage, _vintage_expr,
         _from_table, _date_from, _date_to, _where_clause
       One SQL returns every vintage bucket for this column. */
    %local _cache _rc _t0 _elapsed _attempt _max_attempts _sleep_secs;
    %let _cache = cache._cs_&_dsname;
    %let _t0 = %sysfunc(datetime());
    /* Retry knobs for transient connection failures on Oracle (long
       extractions sometimes hit "couldn't open client transport" /
       network hiccups). After _max_attempts the macro gives up with a
       WARNING and returns so the rest of the extraction continues.
       Caller can override via _ora_retry_count / _ora_retry_sleep
       macro vars set globally at the top of the SAS file (Python
       injects defaults). */
    %if %symexist(_ora_retry_count) %then %let _max_attempts = &_ora_retry_count;
    %else %let _max_attempts = 3;
    %if %symexist(_ora_retry_sleep) %then %let _sleep_secs = &_ora_retry_sleep;
    %else %let _sleep_secs = 30;
    %let _attempt = 0;
    %let _rc = 0;

    %put NOTE: ---- [&_x_idx/&_n_total &_qname / &_col_name] (&_col_type) oracle stats pull START ----;
    %put NOTE:      FROM    : &_from_table;
    %put NOTE:      DATE    : &_date_col IN [&_date_from , &_date_to];
    %put NOTE:      VINTAGE : &_vintage  (expr: &_vintage_expr);
    %put NOTE:      WHERE   : &_where_clause;
    %put NOTE:      BUCKET  : &_dt_label;

    /* Resume support: if (column_name, dt) is already in the cache from
       a previous run, skip this bucket. _col_redo=1 disables the skip
       (set SAS_COL_REDO=1 in the env when regenerating the SAS to force
       a full re-extract). */
    %local _already_done;
    %let _already_done = 0;
    %if &_col_redo = 0 and %sysfunc(exist(&_cache)) %then %do;
        proc sql noprint;
            select count(*) into :_already_done trimmed
            from &_cache
            where strip(column_name) = "&_col_name"
              and strip(dt)          = "&_dt_label";
        quit;
    %end;
    %if &_already_done > 0 %then %do;
        %put NOTE: ---- [&_x_idx/&_n_total &_qname / &_col_name / &_dt_label] already in cache -- skipping (resume) ----;
        %return;
    %end;

%_ora_retry_loop:
    %let _attempt = %eval(&_attempt + 1);
    proc sql;
        %&_conn_macro
        %if %upcase(&_col_type) = NUMERIC %then %do;
            /* &_col_expr is the sentinel-translated column expression
               (CASE WHEN col IN (-999, ...) THEN NULL ELSE col END), built
               in Python via apply_null_sentinels and threaded through the
               driver. Aggregates use it so sentinel values collapse to
               NULL; the literal output name still uses &_col_name_lit.

               In coarse mode (&_coarse_mode = 1) mean / std are blanked
               in the SELECT so the output CSV matches Athena's coarse
               SQL schema (top_10 / length_* / hash_sum are already
               empty on numeric -- length depends on storage formatting,
               hash on the function family). */
            %local _mean_expr _std_expr;
            %if &_coarse_mode = 1 %then %do;
                %let _mean_expr = CAST('' AS VARCHAR2(32));
                %let _std_expr  = CAST('' AS VARCHAR2(32));
            %end;
            %else %do;
                %let _mean_expr = TO_CHAR(AVG(&_col_expr));
                %let _std_expr  = TO_CHAR(STDDEV_SAMP(&_col_expr));
            %end;
            create table _c_one as
            select * from connection to oracle (
                SELECT &_vintage_expr AS dt,
                       /* CAST(literal AS VARCHAR2(64)) so Oracle's metadata
                          tells SAS the column is $64 -- without it, SAS
                          infers length from the literal width (e.g. $17
                          for 'PSTBU_MASDECI_TXT') and proc append freezes
                          the base cache at that width, truncating later
                          longer column names to fit. */
                       CAST(&_col_name_lit AS VARCHAR2(64)) AS column_name,
                       'numeric' AS col_type,
                       COUNT(*) AS n_total,
                       COUNT(*) - COUNT(&_col_expr) AS n_missing,
                       &_n_unique_expr AS n_unique,
                       &_mean_expr AS mean,
                       &_std_expr AS std,
                       TO_CHAR(MIN(&_col_expr)) AS min_val,
                       TO_CHAR(MAX(&_col_expr)) AS max_val,
                       CAST('' AS VARCHAR2(4000)) AS top_10,
                       /* length_avg / length_max left empty on numeric:
                          LENGTH(TO_CHAR(num)) is dominated by storage
                          type (NUMBER displays differently than DOUBLE),
                          so cross-platform compares pick up presentation
                          drift rather than data drift. Only emit length
                          stats on categorical columns. */
                       CAST('' AS VARCHAR2(32)) AS length_avg,
                       CAST('' AS VARCHAR2(32)) AS length_max,
                       /* hash_sum left empty: Oracle ORA_HASH and Athena
                          xxhash64 sums never agree on identical data, so
                          cross-platform Phase-1 compares would always
                          flag hash mismatch. Comparison treats empty as
                          match=None (unknown) instead of a false diff. */
                       CAST('' AS VARCHAR2(64)) AS hash_sum
                FROM &_from_table
                WHERE &_where_clause
                /* Per-bucket query: _vintage_expr is a SQL string literal
                   (e.g. '2022-01-01'), not a DB function, so there's no
                   per-row truncation expression to group by. The WHERE
                   above already narrows to this bucket -- one row of
                   aggregates comes back, no GROUP BY needed on Oracle. */
            );
        %end;
        %else %do;
            /* Categorical: pull only the (dt, p_col, cnt) freq table.
               Alpha-rank-weighted mean/std, alpha-bound min/max, and
               top_10 are computed SAS-side via _col_categorical_freq.
               Same algorithm as the Athena path -- numerical values
               match across engines on identical data.

               &_col_expr feeds the freq aggregation so sentinel string
               values (e.g. 'NA', '#N/A') are mapped to NULL upstream and
               folded into the missing-row bucket instead of forming
               their own categories.

               Per-bucket query: _vintage_expr is the bucket label as a
               SQL string literal. WHERE narrows to one bucket, so the
               GROUP BY drops the vintage and keeps only the freq key. */
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

    %let _rc = &SYSERR;
    %if &_rc > 4 %then %do;
        %if &_attempt < &_max_attempts %then %do;
            %put WARNING: [&_qname/&_col_name] oracle pull attempt &_attempt of &_max_attempts failed (SYSERR=&_rc) -- sleeping &_sleep_secs.s before retry;
            options obs=max nosyntaxcheck;
            data _null_; rc = sleep(&_sleep_secs, 1); run;
            %goto _ora_retry_loop;
        %end;
        %put WARNING: [&_qname/&_col_name/&_dt_label] stats SQL failed after &_max_attempts attempts (SYSERR=&_rc) -- skipping column;
        options obs=max nosyntaxcheck;
        %return;
    %end;

    /* Categorical: post-process freq -> final stats SAS-side. */
    %if %upcase(&_col_type) NE NUMERIC %then %do;
        %_col_categorical_freq(freq_ds=_ora_freq_, col=&_col_name, out_ds=_c_one);
        proc delete data=_ora_freq_; run;
    %end;

    /* Normalize widths + ensure top_10 / length / hash slots exist.
       column_name $64 (not $32) so long Oracle identifiers don't get
       silently truncated when the base cache freezes its width on the
       first proc append. */
    data _c_one;
        length dt $32 column_name $64 col_type $12 top_10 $4000
               length_avg $32 length_max $32 hash_sum $32;
        set _c_one;
    run;
    proc append base=&_cache data=_c_one force; run;
    proc delete data=_c_one; run;

    %let _elapsed = %sysevalf(%sysfunc(datetime()) - &_t0);
    %put WARNING- ==== [&_x_idx/&_n_total &_qname / &_col_name] (&_col_type, vintage=&_vintage) DONE in %sysfunc(putn(&_elapsed, 8.2))s ====;
%mend _col_oracle;

/* Per-(qname) helper -- banner runs in open code, then the inner data step
   dispatches one %_col_oracle() call per row (filtered to this qname) via
   call execute; queue drains when the data step ends, then the footer
   runs in open code. Open-code banner/footer means their macro-var
   references (&qname, &dsname) are resolved at macro-invoke time with the
   args Python passed -- no reliance on call-execute symputx ordering. */
%macro _run_one_ora_table(qname=, dsname=);
    %_table_start_banner(qname=&qname, dsname=&dsname);

    /* Cache lifecycle:
         _col_redo = 1 (env SAS_COL_REDO=1)  -> wipe and start fresh.
         _col_redo = 0 (default)             -> keep existing cache so
             an interrupted run can resume. _col_oracle below checks
             (column_name, dt) against the cache before each pull and
             skips already-completed (col, bucket) pairs. */
    %if &_col_redo = 1 %then %do;
        %if %sysfunc(exist(cache._cs_&dsname)) %then %do;
            proc delete data=cache._cs_&dsname; run;
        %end;
    %end;

    /* Pre-create the cache with explicit lengths so proc append cannot
       freeze column_name (or any other field) to a short width inferred
       from the first row. Without this, a short first column name like
       'AMT' could cap column_name at $3 on the base dataset, and the
       next pull's 'PSTBU_MASDECI_TXT' / 'PSTBU_MASDECI_SETID' would be
       truncated to fit. Conditional so resume keeps any existing rows
       intact; only creates a fresh schema-locked empty dataset when the
       cache doesn't exist yet. */
    %if not %sysfunc(exist(cache._cs_&dsname)) %then %do;
        data cache._cs_&dsname;
            length dt $32 column_name $64 col_type $12 top_10 $4000
                   mean $32 std $32 min_val $400 max_val $400
                   length_avg $32 length_max $32 hash_sum $32
                   n_total 8 n_missing 8 n_unique 8;
            stop;
        run;
    %end;

    /* Total column count for this table -- drives x/n progress display in
       the per-column start banner. */
    %local _n_total;
    proc sql noprint;
        select count(*) into :_n_total trimmed
        from _ora_col_map where qname = "&qname";
    quit;

    data _null_;
        set _ora_col_map;
        where qname = "&qname";
        length _cmd $4000;
        _cmd = cats(
            'data _null_;',
            ' call symputx("_qname", ',        quote(strip(qname)),        ');',
            ' call symputx("_dsname", ',       quote(strip(dsname)),       ');',
            ' call symputx("_conn_macro", ',   quote(strip(conn_macro)),   ');',
            ' call symputx("_col_name", ',     quote(strip(col_name)),     ');',
            ' call symputx("_col_name_lit", ', quote(cats("'", strip(col_name), "'")), ');',
            ' call symputx("_col_expr", ',     quote(strip(col_expr)),     ');',
            ' call symputx("_dt_label", ',     quote(strip(dt_label)),     ');',
            ' call symputx("_col_type", ',     quote(strip(col_type)),     ');',
            ' call symputx("_date_col", ',     quote(strip(date_col)),     ');',
            ' call symputx("_vintage", ',      quote(strip(vintage)),      ');',
            ' call symputx("_vintage_expr", ', quote(strip(vintage_expr)), ');',
            ' call symputx("_from_table", ',   quote(strip(from_table)),   ');',
            ' call symputx("_date_from", ',    quote(strip(date_from)),    ');',
            ' call symputx("_date_to", ',      quote(strip(date_to)),      ');',
            ' call symputx("_where_clause", ', quote(strip(where_clause)), ');',
            ' call symputx("_n_unique_expr", ', quote(strip(n_unique_expr)), ');',
            ' call symputx("_x_idx", ',        quote(strip(put(_n_, best.))), ');',
            ' call symputx("_n_total", "',     "&_n_total",                   '");',
            ' run; ',
            '%nrstr(%_col_oracle)();'
        );
        call execute(_cmd);
    run;

    %_table_done_footer(qname=&qname, dsname=&dsname);
%mend _run_one_ora_table;

/* Python emits one %_run_one_ora_table() per qname below. */
/*{ORA_RUN_CALLS}*/

/* _ora_col_map intentionally NOT deleted -- keep it around for post-run
   inspection (e.g., proc print to re-check date_from/date_to formatting). */
