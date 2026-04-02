/* Oracle SQL Performance Benchmark — SAS Companion
   Runs the same SQL as oracle_bench.py via SAS PROC SQL passthrough
   so you can compare SAS vs Python execution times.

   Usage:
     1. Edit the %let variables below for your environment
     2. Run this SAS program
     3. Results saved to &out_dir/bench_sas_results.csv
     4. Load into bench_analysis.ipynb alongside Python results

   Matches experiments: index_where, index_groupby, cte_materialize,
     cte_simple, parallel_query, partition_pruning, result_cache,
     dtrack_row_count, dtrack_col_stats, dtrack_top10
*/

/* ---- Configuration ---- */
%let ora_user    = &SYSUSERID;           /* or hardcode */
%let ora_pass    = %sysget(ORACLE_PASSWORD);
%let ora_path    = host:1521/service_name;  /* edit this */

/* Table/column settings — change these to test different tables */
%let bench_table   = SCHEMA.TABLE_NAME;
%let bench_datecol = LOAD_DT;
%let bench_numcol  = AMOUNT;
%let bench_catcol  = STATUS;

%let iterations  = 5;
%let out_dir     = .;

/* ---- Connection macro ---- */
%macro ora_connect;
    connect to oracle (user="&ora_user" password="&ora_pass" path="&ora_path");
%mend;

/* ---- Results dataset ---- */
data _bench_results;
    length experiment $32 variant $32 run 8 elapsed_sec 8 rows 8 table_name $128;
    stop;
run;

/* ---- Timer macro ---- */
%macro bench_run(experiment=, variant=, sql=);
    %do _run = 1 %to &iterations;
        %let _t0 = %sysfunc(datetime());

        proc sql noprint;
            %ora_connect
            create table _bench_tmp as
            select * from connection to oracle (
                &sql
            );
            disconnect from oracle;
        quit;

        %let _t1 = %sysfunc(datetime());
        %let _elapsed = %sysevalf(&_t1 - &_t0);
        %let _nobs = 0;
        %if %sysfunc(exist(_bench_tmp)) %then %do;
            proc sql noprint;
                select count(*) into :_nobs from _bench_tmp;
            quit;
        %end;

        data _bench_row;
            length experiment $32 variant $32 table_name $128;
            experiment  = "&experiment";
            variant     = "&variant";
            run         = &_run;
            elapsed_sec = &_elapsed;
            rows        = &_nobs;
            table_name  = "&bench_table";
        run;

        proc append base=_bench_results data=_bench_row force; run;
        proc delete data=_bench_tmp _bench_row; run;

        %put NOTE: [&experiment/&variant] run &_run: &_elapsed.s (&_nobs rows);
    %end;
%mend;


/* ==================================================================
   EXPERIMENTS — same SQL as oracle_bench.py
   ================================================================== */

/* ---- 1. index_where ---- */
%bench_run(experiment=index_where, variant=slow_trunc,
    sql=%str(
        SELECT COUNT(*) as cnt FROM &bench_table
        WHERE TRUNC(&bench_datecol) = DATE '2024-01-15'
    ))

%bench_run(experiment=index_where, variant=fast_range,
    sql=%str(
        SELECT COUNT(*) as cnt FROM &bench_table
        WHERE &bench_datecol >= DATE '2024-01-15' AND &bench_datecol < DATE '2024-01-16'
    ))


/* ---- 2. index_groupby ---- */
%bench_run(experiment=index_groupby, variant=slow_trunc_groupby,
    sql=%str(
        SELECT TRUNC(&bench_datecol) AS dt, COUNT(*) as cnt
        FROM &bench_table GROUP BY TRUNC(&bench_datecol) ORDER BY 1
    ))

%bench_run(experiment=index_groupby, variant=fast_direct_groupby,
    sql=%str(
        SELECT &bench_datecol AS dt, COUNT(*) as cnt
        FROM &bench_table GROUP BY &bench_datecol ORDER BY 1
    ))


/* ---- 3. cte_materialize ---- */
%bench_run(experiment=cte_materialize, variant=no_hint,
    sql=%str(
        WITH daily_avg AS (
            SELECT &bench_datecol, AVG(&bench_numcol) as avg_val
            FROM &bench_table GROUP BY &bench_datecol
        )
        SELECT d.&bench_datecol, d.avg_val, COUNT(*) as cnt
        FROM daily_avg d JOIN &bench_table s ON s.&bench_datecol = d.&bench_datecol
        GROUP BY d.&bench_datecol, d.avg_val
    ))

%bench_run(experiment=cte_materialize, variant=materialize_hint,
    sql=%str(
        WITH daily_avg AS (
            SELECT /*+ MATERIALIZE */ &bench_datecol, AVG(&bench_numcol) as avg_val
            FROM &bench_table GROUP BY &bench_datecol
        )
        SELECT d.&bench_datecol, d.avg_val, COUNT(*) as cnt
        FROM daily_avg d JOIN &bench_table s ON s.&bench_datecol = d.&bench_datecol
        GROUP BY d.&bench_datecol, d.avg_val
    ))


/* ---- 4. cte_simple ---- */
%bench_run(experiment=cte_simple, variant=with_cte,
    sql=%str(
        WITH means AS (SELECT AVG(&bench_numcol) as avg_val, COUNT(*) as cnt FROM &bench_table)
        SELECT * FROM means
    ))

%bench_run(experiment=cte_simple, variant=direct,
    sql=%str(
        SELECT AVG(&bench_numcol) as avg_val, COUNT(*) as cnt FROM &bench_table
    ))


/* ---- 5. parallel_query ---- */
%bench_run(experiment=parallel_query, variant=serial,
    sql=%str(
        SELECT /*+ NO_PARALLEL */ &bench_datecol, COUNT(*) as cnt, AVG(&bench_numcol) AS avg_val
        FROM &bench_table GROUP BY &bench_datecol
    ))

%bench_run(experiment=parallel_query, variant=parallel_4,
    sql=%str(
        SELECT /*+ PARALLEL(t, 4) */ &bench_datecol, COUNT(*) as cnt, AVG(&bench_numcol) AS avg_val
        FROM &bench_table t GROUP BY &bench_datecol
    ))


/* ---- 6. partition_pruning ---- */
%bench_run(experiment=partition_pruning, variant=slow_to_char,
    sql=%str(
        SELECT COUNT(*) as cnt FROM &bench_table
        WHERE TO_CHAR(&bench_datecol, 'YYYY-MM') = '2024-01'
    ))

%bench_run(experiment=partition_pruning, variant=fast_date_range,
    sql=%str(
        SELECT COUNT(*) as cnt FROM &bench_table
        WHERE &bench_datecol >= DATE '2024-01-01' AND &bench_datecol < DATE '2024-02-01'
    ))


/* ---- 7. result_cache ---- */
%bench_run(experiment=result_cache, variant=no_cache,
    sql=%str(
        SELECT &bench_datecol, COUNT(*) as cnt, AVG(&bench_numcol) AS avg_val
        FROM &bench_table GROUP BY &bench_datecol
    ))

%bench_run(experiment=result_cache, variant=result_cache,
    sql=%str(
        SELECT /*+ RESULT_CACHE */ &bench_datecol, COUNT(*) as cnt, AVG(&bench_numcol) AS avg_val
        FROM &bench_table GROUP BY &bench_datecol
    ))


/* ---- 8. dtrack_row_count ---- */
%bench_run(experiment=dtrack_row_count, variant=trunc_groupby,
    sql=%str(
        SELECT TRUNC(&bench_datecol) AS date_value, COUNT(*) AS row_count
        FROM &bench_table GROUP BY TRUNC(&bench_datecol)
    ))

%bench_run(experiment=dtrack_row_count, variant=direct_groupby,
    sql=%str(
        SELECT &bench_datecol AS date_value, COUNT(*) AS row_count
        FROM &bench_table GROUP BY &bench_datecol
    ))

%bench_run(experiment=dtrack_row_count, variant=parallel_groupby,
    sql=%str(
        SELECT /*+ PARALLEL(t, 4) */ TRUNC(&bench_datecol) AS date_value, COUNT(*) AS row_count
        FROM &bench_table t GROUP BY TRUNC(&bench_datecol)
    ))


/* ---- 9. dtrack_col_stats ---- */
%bench_run(experiment=dtrack_col_stats, variant=serial_stats,
    sql=%str(
        SELECT &bench_datecol AS dt, '&bench_numcol' AS column_name, 'numeric' AS col_type,
            COUNT(*) AS n_total,
            SUM(CASE WHEN &bench_numcol IS NULL THEN 1 ELSE 0 END) AS n_missing,
            COUNT(DISTINCT &bench_numcol) AS n_unique,
            AVG(&bench_numcol) AS mean, STDDEV(&bench_numcol) AS std,
            MIN(&bench_numcol) AS min_val, MAX(&bench_numcol) AS max_val
        FROM &bench_table WHERE 1=1 GROUP BY &bench_datecol
    ))

%bench_run(experiment=dtrack_col_stats, variant=parallel_stats,
    sql=%str(
        SELECT /*+ PARALLEL(t, 4) */ &bench_datecol AS dt, '&bench_numcol' AS column_name,
            'numeric' AS col_type,
            COUNT(*) AS n_total,
            SUM(CASE WHEN &bench_numcol IS NULL THEN 1 ELSE 0 END) AS n_missing,
            COUNT(DISTINCT &bench_numcol) AS n_unique,
            AVG(&bench_numcol) AS mean, STDDEV(&bench_numcol) AS std,
            MIN(&bench_numcol) AS min_val, MAX(&bench_numcol) AS max_val
        FROM &bench_table t WHERE 1=1 GROUP BY &bench_datecol
    ))


/* ---- 10. dtrack_top10 ---- */
%bench_run(experiment=dtrack_top10, variant=serial_top10,
    sql=%str(
        SELECT dt, val, cnt FROM (
            SELECT &bench_datecol AS dt, CAST(&bench_catcol AS VARCHAR(200)) AS val,
                   COUNT(*) AS cnt,
                   ROW_NUMBER() OVER (PARTITION BY &bench_datecol ORDER BY COUNT(*) DESC) AS rn
            FROM &bench_table WHERE &bench_catcol IS NOT NULL
            GROUP BY &bench_datecol, &bench_catcol
        ) WHERE rn <= 10
    ))

%bench_run(experiment=dtrack_top10, variant=parallel_top10,
    sql=%str(
        SELECT dt, val, cnt FROM (
            SELECT /*+ PARALLEL(t, 4) */ &bench_datecol AS dt,
                   CAST(&bench_catcol AS VARCHAR(200)) AS val,
                   COUNT(*) AS cnt,
                   ROW_NUMBER() OVER (PARTITION BY &bench_datecol ORDER BY COUNT(*) DESC) AS rn
            FROM &bench_table t WHERE &bench_catcol IS NOT NULL
            GROUP BY &bench_datecol, &bench_catcol
        ) WHERE rn <= 10
    ))


/* ==================================================================
   Export results
   ================================================================== */
proc export data=_bench_results
    outfile="&out_dir./bench_sas_results.csv"
    dbms=csv replace;
run;

%put NOTE: Benchmark complete. Results in &out_dir./bench_sas_results.csv;

proc print data=_bench_results; run;

proc delete data=_bench_results; run;
