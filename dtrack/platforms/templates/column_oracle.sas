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
           col_name $64 col_type $12
           date_col $64 vintage $12 vintage_expr $500
           date_from $64 date_to $64
           from_table $500 where_clause $2000;
/*{ORA_COL_ROWS}*/
run;

%macro _col_oracle();
    /* Dispatcher sets these macro vars per (qname, col_name) before
       invoking this macro:
         _qname, _dsname, _conn_macro, _col_name, _col_type,
         _date_col, _vintage, _vintage_expr,
         _from_table, _date_from, _date_to, _where_clause
       One SQL returns every vintage bucket for this column. */
    %local _cache _rc _t0 _elapsed;
    %let _cache = cache._cs_&_dsname;
    %let _t0 = %sysfunc(datetime());

    %put NOTE: ---- [&_qname / &_col_name] (&_col_type) oracle stats pull START ----;
    %put NOTE:      FROM    : &_from_table;
    %put NOTE:      DATE    : &_date_col IN [&_date_from , &_date_to];
    %put NOTE:      VINTAGE : &_vintage  (expr: &_vintage_expr);
    %put NOTE:      WHERE   : &_where_clause;

    proc sql;
        %&_conn_macro
        %if %upcase(&_col_type) = NUMERIC %then %do;
            create table _c_one as
            select * from connection to oracle (
                SELECT &_vintage_expr AS dt,
                       &_col_name_lit AS column_name,
                       'numeric' AS col_type,
                       COUNT(*) AS n_total,
                       COUNT(*) - COUNT(&_col_name) AS n_missing,
                       COUNT(DISTINCT &_col_name) AS n_unique,
                       TO_CHAR(AVG(&_col_name)) AS mean,
                       TO_CHAR(STDDEV_SAMP(&_col_name)) AS std,
                       TO_CHAR(MIN(&_col_name)) AS min_val,
                       TO_CHAR(MAX(&_col_name)) AS max_val,
                       CAST('' AS VARCHAR2(4000)) AS top_10
                FROM &_from_table
                WHERE &_where_clause
                GROUP BY &_vintage_expr
            );
        %end;
        %else %do;
            /* Categorical: per-value SUBSTR caps width to 200 so per-bucket
               LISTAGG stays under Oracle's 4000-char cap (plus ON OVERFLOW
               TRUNCATE as a belt-and-braces; Oracle 12.2+).  Freq table is
               grouped by (dt, p_col); rank partitioned by dt; LISTAGG
               grouped by dt -> one row per bucket. */
            create table _c_one as
            select * from connection to oracle (
                WITH freq_raw_ AS (
                    SELECT &_vintage_expr AS dt,
                           SUBSTR(TO_CHAR(&_col_name), 1, 200) AS p_col,
                           COUNT(*) AS value_freq
                    FROM &_from_table
                    WHERE &_where_clause
                    GROUP BY &_vintage_expr, SUBSTR(TO_CHAR(&_col_name), 1, 200)
                ), ranked_ AS (
                    SELECT dt, p_col, value_freq,
                           ROW_NUMBER() OVER (PARTITION BY dt
                                              ORDER BY value_freq DESC, p_col ASC) AS rn
                    FROM freq_raw_
                ), stats_ AS (
                    SELECT dt,
                           SUM(value_freq) AS n_total,
                           COALESCE(MAX(CASE WHEN p_col IS NULL
                                             THEN value_freq END), 0) AS n_missing,
                           SUM(CASE WHEN p_col IS NOT NULL THEN 1 ELSE 0 END) AS n_unique,
                           TO_CHAR(AVG(value_freq)) AS mean,
                           TO_CHAR(STDDEV_SAMP(value_freq)) AS std,
                           TO_CHAR(MIN(value_freq)) AS min_val,
                           TO_CHAR(MAX(value_freq)) AS max_val
                    FROM freq_raw_ GROUP BY dt
                ), top_ AS (
                    SELECT dt,
                           LISTAGG(p_col || '(' || value_freq || ')', '; '
                                   ON OVERFLOW TRUNCATE '...' WITH COUNT)
                             WITHIN GROUP (ORDER BY value_freq DESC, p_col ASC) AS top_10
                    FROM ranked_ WHERE rn <= 10 AND p_col IS NOT NULL
                    GROUP BY dt
                )
                SELECT s.dt,
                       &_col_name_lit AS column_name,
                       'categorical' AS col_type,
                       s.n_total, s.n_missing, s.n_unique,
                       s.mean, s.std, s.min_val, s.max_val,
                       COALESCE(t.top_10, '') AS top_10
                FROM stats_ s LEFT JOIN top_ t ON s.dt = t.dt
            );
        %end;
        disconnect from oracle;
    quit;

    %let _rc = &SYSERR;
    %if &_rc > 4 %then %do;
        %put WARNING: [&_qname/&_col_name/&_dt_label] stats SQL failed (SYSERR=&_rc) -- skipping;
        options obs=max nosyntaxcheck;
        %return;
    %end;

    /* Normalize widths + ensure top_10 slot exists. */
    data _c_one;
        length dt $32 column_name $32 col_type $12 top_10 $4000;
        set _c_one;
    run;
    proc append base=&_cache data=_c_one force; run;
    proc delete data=_c_one; run;

    %let _elapsed = %sysevalf(%sysfunc(datetime()) - &_t0);
    %put WARNING- ==== [&_qname / &_col_name] (&_col_type, vintage=&_vintage) DONE in %sysfunc(putn(&_elapsed, 8.2))s ====;
%mend _col_oracle;

/* Per-(qname) helper -- banner runs in open code, then the inner data step
   dispatches one %_col_oracle() call per row (filtered to this qname) via
   call execute; queue drains when the data step ends, then the footer
   runs in open code. Open-code banner/footer means their macro-var
   references (&qname, &dsname) are resolved at macro-invoke time with the
   args Python passed -- no reliance on call-execute symputx ordering. */
%macro _run_one_ora_table(qname=, dsname=);
    %_table_start_banner(qname=&qname, dsname=&dsname);

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
            ' call symputx("_col_type", ',     quote(strip(col_type)),     ');',
            ' call symputx("_date_col", ',     quote(strip(date_col)),     ');',
            ' call symputx("_vintage", ',      quote(strip(vintage)),      ');',
            ' call symputx("_vintage_expr", ', quote(strip(vintage_expr)), ');',
            ' call symputx("_from_table", ',   quote(strip(from_table)),   ');',
            ' call symputx("_date_from", ',    quote(strip(date_from)),    ');',
            ' call symputx("_date_to", ',      quote(strip(date_to)),      ');',
            ' call symputx("_where_clause", ', quote(strip(where_clause)), ');',
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
