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

    %put NOTE: ---- [&_x_idx/&_n_total &_qname / &_col_name] (&_col_type) oracle stats pull START ----;
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
            /* Categorical: pull only the (dt, p_col, cnt) freq table.
               Alpha-rank-weighted mean/std, alpha-bound min/max, and
               top_10 are computed SAS-side via _col_categorical_freq.
               Same algorithm as the Athena path -- numerical values
               match across engines on identical data. */
            create table _ora_freq_ as
            select * from connection to oracle (
                SELECT &_vintage_expr AS dt,
                       SUBSTR(TO_CHAR(&_col_name), 1, 200) AS p_col,
                       COUNT(*) AS cnt
                FROM &_from_table
                WHERE &_where_clause
                GROUP BY &_vintage_expr,
                         SUBSTR(TO_CHAR(&_col_name), 1, 200)
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

    /* Categorical: post-process freq -> final stats SAS-side. */
    %if %upcase(&_col_type) NE NUMERIC %then %do;
        %_col_categorical_freq(freq_ds=_ora_freq_, col=&_col_name, out_ds=_c_one);
        proc delete data=_ora_freq_; run;
    %end;

    /* Normalize widths + ensure top_10 slot exists. */
    data _c_one;
        length dt $32 column_name $32 col_type $12 top_10 $4000;
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

    /* Wipe any stale cache from prior runs. cache._cs_<dsname> is a
       permanent SAS dataset; each per-column step proc-appends to it.
       Without this clear, rerunning the script would double up the rows
       in the exported CSV. */
    %if %sysfunc(exist(cache._cs_&dsname)) %then %do;
        proc delete data=cache._cs_&dsname; run;
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
            ' call symputx("_col_type", ',     quote(strip(col_type)),     ');',
            ' call symputx("_date_col", ',     quote(strip(date_col)),     ');',
            ' call symputx("_vintage", ',      quote(strip(vintage)),      ');',
            ' call symputx("_vintage_expr", ', quote(strip(vintage_expr)), ');',
            ' call symputx("_from_table", ',   quote(strip(from_table)),   ');',
            ' call symputx("_date_from", ',    quote(strip(date_from)),    ');',
            ' call symputx("_date_to", ',      quote(strip(date_to)),      ');',
            ' call symputx("_where_clause", ', quote(strip(where_clause)), ');',
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
