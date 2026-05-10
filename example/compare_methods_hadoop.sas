/*============================================================================
 * compare_methods_hadoop.sas
 *
 * Side-by-side benchmark of dtrack's two Hadoop column-stats paths:
 *   (A) PROC SQL CONNECT TO HADOOP   <-- what gen-col emits today
 *   (B) LIBNAME hadoop + proc sql    <-- what connect_method=libname emits
 *
 * Both paths compute the same column statistics (n_total, n_missing,
 * n_unique, mean, std, min_val, max_val) on the same table / column /
 * date range, then PROC PRINT the results next to each other and log
 * the elapsed time per path. The intent is to confirm whether the
 * libname rewrite is worth the migration cost on YOUR cluster.
 *
 * Fill in the macro variables in the CONFIG block, then submit.
 *
 * Tip: leave `options sastrace=...` on so the SAS log shows which
 * SQL Hadoop actually executed. If the libname path's optimizer
 * pushed aggregation down, you'll see a single Hive SQL with COUNT()
 * / AVG() / etc. If it streamed raw rows back to SAS, you'll see a
 * SELECT <col> FROM ... and the work is happening locally — that's
 * the case where libname loses to PROC SQL CONNECT.
 *============================================================================*/

/*-------- CONFIG ----------------------------------------------------------*/
%let hdp_server = REPLACE.WITH.YOUR.HOST;       /* e.g. hadoop.example.com */
%let hdp_realm  = REPLACE.WITH.YOUR.REALM;      /* Kerberos REALM, used in principal */
%let hdp_schema = REPLACE_WITH_YOUR_SCHEMA;     /* Hive schema/db name */
%let hdp_table  = v_ecm_strtgy_rta_input;       /* table to query */
%let target_col = amt_purch_ctd;                /* column to compute stats on */
%let date_col   = ptn_val_txt;                  /* date / partition column */
%let date_from  = 20230101;                     /* literal as the column stores it */
%let date_to    = 20251231;
%let truststore = /sas/bin/Software/Hadoop/OSH/PROD/truststore.jks;
%let jar_path   = /sas/bin/Software/Hadoop/OSH/Jars;
%let cfg_path   = /sas/bin/Software/Hadoop/OSH/PROD;

/*-------- common SAS_HADOOP_* options needed by both paths ---------------*/
options set=SAS_HADOOP_JAR_PATH    "&jar_path";
options set=SAS_HADOOP_CONFIG_PATH "&cfg_path";

/* sastrace prints the actual Hive SQL each path emits — read the log to
   see what got pushed down. */
options sastrace=',,,d' sastraceloc=saslog nostsuffix sql_ip_trace=note;

/*============================================================================
 * (A) PROC SQL CONNECT TO HADOOP — what dtrack gen-col emits today.
 *     Aggregation is in the inner native-SQL block, so Hive does all the
 *     work and only one row comes back over the wire.
 *============================================================================*/

%let _t0 = %sysfunc(datetime());

%let hadoop_cro_ded = %str(server="&hdp_server" port=10000 schema=&hdp_schema
    subprotocol=hive2 LOGIN_TIMEOUT=300
    uri='jdbc:hive2://&hdp_server:10000/default;principal=hive/&hdp_server@&hdp_realm;ssl=true;sslTrustStore=&truststore;');

proc sql;
    connect to hadoop (&hadoop_cro_ded);
    create table connect_stats as
    select * from connection to hadoop (
        SELECT 'all'            AS dt,
               '&target_col'    AS column_name,
               'numeric'        AS col_type,
               COUNT(*)                               AS n_total,
               COUNT(*) - COUNT(&target_col)          AS n_missing,
               COUNT(DISTINCT &target_col)            AS n_unique,
               CAST(AVG(&target_col)         AS STRING) AS mean,
               CAST(STDDEV_SAMP(&target_col) AS STRING) AS std,
               CAST(MIN(&target_col)         AS STRING) AS min_val,
               CAST(MAX(&target_col)         AS STRING) AS max_val
        FROM &hdp_schema..&hdp_table
        WHERE &date_col BETWEEN '&date_from' AND '&date_to'
    );
    disconnect from hadoop;
quit;

%let _t1 = %sysfunc(datetime());
%let _elapsed_connect = %sysevalf(&_t1 - &_t0);
%put NOTE: ============================================================;
%put NOTE: (A) PROC SQL CONNECT path elapsed: %sysfunc(putn(&_elapsed_connect, 8.2)) s;
%put NOTE: ============================================================;

/*============================================================================
 * (B) LIBNAME hadoop + proc sql — what connect_method=libname emits.
 *     Same logical query, but written in SAS proc sql against the libref
 *     so SAS/ACCESS to Hive plans the pushdown. Verify in the log that
 *     SAS sent a single SQL with COUNT/AVG/STDDEV/etc. (good) rather
 *     than streaming raw rows then aggregating locally (bad).
 *============================================================================*/

libname hdp hadoop server="&hdp_server"
    port=10000 schema=&hdp_schema subprotocol=hive2 LOGIN_TIMEOUT=300
    uri='jdbc:hive2://&hdp_server:10000/usrf;principal=hive/&hdp_server@&hdp_realm;ssl=true;sslTrustStore=&truststore;';

%let _t0 = %sysfunc(datetime());

proc sql;
    create table libname_stats as
    select 'all'            as dt          length=32,
           "&target_col"    as column_name length=32,
           'numeric'        as col_type    length=32,
           count(*)                                   as n_total,
           count(*) - count(&target_col)              as n_missing,
           count(distinct &target_col)                as n_unique,
           strip(put(avg(&target_col), best32.))      as mean    length=200,
           strip(put(std(&target_col), best32.))      as std     length=200,
           strip(put(min(&target_col), best32.))      as min_val length=200,
           strip(put(max(&target_col), best32.))      as max_val length=200
    from hdp.&hdp_table
    where &date_col between '&date_from' and '&date_to';
quit;

%let _t1 = %sysfunc(datetime());
%let _elapsed_libname = %sysevalf(&_t1 - &_t0);
%put NOTE: ============================================================;
%put NOTE: (B) LIBNAME path elapsed: %sysfunc(putn(&_elapsed_libname, 8.2)) s;
%put NOTE: ============================================================;

libname hdp clear;

/*============================================================================
 * (C) Side-by-side. PROC PRINT both result sets so the numbers can be
 *     eyeballed for parity, plus a final NOTE summarizing wall time.
 *============================================================================*/

data _comparison;
    length method $20;
    set connect_stats(in=a) libname_stats(in=b);
    if a then method = '(A) PROC SQL';
    else if b then method = '(B) LIBNAME';
run;

proc print data=_comparison noobs;
    var method dt column_name col_type
        n_total n_missing n_unique mean std min_val max_val;
    title "Hadoop column stats: PROC SQL CONNECT vs LIBNAME -- &hdp_table..&target_col";
run;
title;

%put NOTE: ##############################################################;
%put NOTE: # TIMING SUMMARY (lower is better);
%put NOTE: #   (A) PROC SQL CONNECT : %sysfunc(putn(&_elapsed_connect, 8.2)) s;
%put NOTE: #   (B) LIBNAME          : %sysfunc(putn(&_elapsed_libname, 8.2)) s;
%put NOTE: # Speed ratio LIBNAME/CONNECT : %sysfunc(putn(%sysevalf(&_elapsed_libname / &_elapsed_connect), 8.2));
%put NOTE: ##############################################################;
%put NOTE: ;
%put NOTE: Read the sastrace lines above to confirm aggregation pushdown:;
%put NOTE:   - Look for `ACCESS ENGINE: SQL_IP_TRACE: passing down query` (good);
%put NOTE:   - Beware `Could not pass down ... falling back` (bad -- libname loses);
