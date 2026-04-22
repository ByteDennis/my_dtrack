/* Hadoop/Hive column-stats extraction (passthrough, all aggregation in DB).
   The SQL template is emitted ONCE in the %_col_hadoop macro; the driver
   dataset carries only per-row variables (col_name, col_type, dt_label,
   from_table, where_clause) to keep the generated .sas file compact.

   Placeholders:
     {COL_REDO}         - 0 or 1
     {HDP_COL_ROWS}     - inline data-step 'output;' rows feeding _hdp_col_map
*/

%let _col_redo = /*{COL_REDO}*/;

/* Driver dataset. One row per (qname, col_name) -- vintage bucketing is
   done DB-side via GROUP BY &_vintage_expr.  Inspect with
       proc print data=_hdp_col_map(keep=qname col_name date_col vintage
                                         vintage_expr date_from date_to);
   BEFORE running to verify the truncation expression matches the Hive
   column's date type (string_compact YYYYMMDD, string_dash YYYY-MM-DD,
   native DATE, etc.). */
data _hdp_col_map;
    length qname $64 dsname $32 conn_macro $32
           col_name $64 col_type $12
           date_col $64 vintage $12 vintage_expr $500
           date_from $64 date_to $64
           from_table $500 where_clause $2000;
/*{HDP_COL_ROWS}*/
run;

%macro _col_hadoop();
    %local _cache _rc _t0 _elapsed;
    %let _cache = cache._cs_&_dsname;
    %let _t0 = %sysfunc(datetime());

    %put NOTE: ---- [&_qname / &_col_name] (&_col_type) hadoop stats pull START ----;
    %put NOTE:      FROM    : &_from_table;
    %put NOTE:      DATE    : &_date_col IN [&_date_from , &_date_to];
    %put NOTE:      VINTAGE : &_vintage  (expr: &_vintage_expr);
    %put NOTE:      WHERE   : &_where_clause;

    proc sql inobs=max;
        connect to hadoop (%&_conn_macro);
        %if %upcase(&_col_type) = NUMERIC %then %do;
            create table _c_one as
            select * from connection to hadoop (
                SELECT &_vintage_expr AS dt,
                       &_col_name_lit AS column_name,
                       'numeric' AS col_type,
                       COUNT(*) AS n_total,
                       COUNT(*) - COUNT(&_col_name) AS n_missing,
                       COUNT(DISTINCT &_col_name) AS n_unique,
                       CAST(AVG(CAST(&_col_name AS DOUBLE)) AS STRING) AS mean,
                       CAST(STDDEV_SAMP(CAST(&_col_name AS DOUBLE)) AS STRING) AS std,
                       CAST(MIN(&_col_name) AS STRING) AS min_val,
                       CAST(MAX(&_col_name) AS STRING) AS max_val,
                       CAST('' AS STRING) AS top_10
                FROM &_from_table
                WHERE &_where_clause
                GROUP BY &_vintage_expr
            );
        %end;
        %else %do;
            /* Categorical, two-stage:
                 freq   -- GROUP BY (dt, col) -> one row per (bucket, value)
                           with its count. This IS the expensive step for
                           high-cardinality columns, but it produces one row
                           per distinct value so it doesn't OOM on its own;
                           what crashed before was the windowed ranking +
                           collect_list/sort_array chain layered on top of
                           freq. Keeping freq, dropping everything above it.
                 outer  -- aggregate cnt per dt to get stats of the
                           frequency distribution (how skewed, how uniform,
                           etc.). mean/std/min/max describe the distribution
                           of per-value counts; n_total / n_missing /
                           n_unique describe the column as a whole.
               Top-10 is emitted as empty string -- ranking/top-N was the
               part that kept blowing up (Tez reducer OOM, collect_list
               finalization bugs, SMB plan errors). */
            create table _c_one as
            select * from connection to hadoop (
                WITH freq AS (
                    SELECT &_vintage_expr AS dt,
                           SUBSTR(CAST(&_col_name AS STRING), 1, 200) AS p_col,
                           COUNT(*) AS cnt
                    FROM &_from_table
                    WHERE &_where_clause
                    GROUP BY &_vintage_expr,
                             SUBSTR(CAST(&_col_name AS STRING), 1, 200)
                )
                SELECT dt,
                       &_col_name_lit AS column_name,
                       'categorical' AS col_type,
                       SUM(cnt) AS n_total,
                       COALESCE(SUM(CASE WHEN p_col IS NULL THEN cnt ELSE 0 END), 0) AS n_missing,
                       SUM(CASE WHEN p_col IS NOT NULL THEN 1 ELSE 0 END) AS n_unique,
                       CAST(AVG(CAST(cnt AS DOUBLE)) AS STRING) AS mean,
                       CAST(STDDEV_SAMP(CAST(cnt AS DOUBLE)) AS STRING) AS std,
                       CAST(MIN(cnt) AS STRING) AS min_val,
                       CAST(MAX(cnt) AS STRING) AS max_val,
                       CAST('' AS STRING) AS top_10
                FROM freq
                GROUP BY dt
            );
        %end;
        disconnect from hadoop;
    quit;

    %let _rc = &SYSERR;
    %if &_rc > 4 %then %do;
        %put WARNING: [&_qname/&_col_name/&_dt_label] stats SQL failed (SYSERR=&_rc) -- skipping;
        options obs=max nosyntaxcheck;
        %return;
    %end;

    data _c_one;
        length dt $32 column_name $32 col_type $12 top_10 $4000;
        set _c_one;
    run;
    proc append base=&_cache data=_c_one force; run;
    proc delete data=_c_one; run;

    %let _elapsed = %sysevalf(%sysfunc(datetime()) - &_t0);
    %put WARNING- ==== [&_qname / &_col_name] (&_col_type, vintage=&_vintage) DONE in %sysfunc(putn(&_elapsed, 8.2))s ====;
%mend _col_hadoop;

/* Per-(qname) helper -- banner in open code, per-col dispatch via
   call execute, footer in open code. Banner/footer see their args
   directly so there's no call-execute / symputx ordering risk. */
%macro _run_one_hdp_table(qname=, dsname=);
    %_table_start_banner(qname=&qname, dsname=&dsname);

    data _null_;
        set _hdp_col_map;
        where qname = "&qname";
        length _cmd $4000;
        _cmd = cats(
            'data _null_;',
            ' call symputx("_qname", ',        quote(strip(qname)),        ');',
            ' call symputx("_dsname", ',       quote(strip(dsname)),       ');',
            ' call symputx("_conn_macro", ',   quote(strip(conn_macro)),   ');',
            ' call symputx("_col_name", ',     quote(strip(col_name)),     ');',
            ' call symputx("_col_name_lit", ', quote(cats("'", strip(col_name), "'")), ');',
            ' call symputx("_col_type", ',     quote(strip(col_type)),     ');',
            ' call symputx("_date_col", ',     quote(strip(date_col)),     ');',
            ' call symputx("_vintage", ',      quote(strip(vintage)),      ');',
            ' call symputx("_vintage_expr", ', quote(strip(vintage_expr)), ');',
            ' call symputx("_from_table", ',   quote(strip(from_table)),   ');',
            ' call symputx("_date_from", ',    quote(strip(date_from)),    ');',
            ' call symputx("_date_to", ',      quote(strip(date_to)),      ');',
            ' call symputx("_where_clause", ', quote(strip(where_clause)), ');',
            ' run; ',
            '%nrstr(%_col_hadoop)();'
        );
        call execute(_cmd);
    run;

    %_table_done_footer(qname=&qname, dsname=&dsname);
%mend _run_one_hdp_table;

/* Python emits one %_run_one_hdp_table() per qname below. */
/*{HDP_RUN_CALLS}*/

/* _hdp_col_map kept for post-run inspection. */
