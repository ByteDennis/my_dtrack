options nomprint nomlogic nosymbolgen encoding=asciiany;

/* Credentials (filled by Python from dtrack.conf) */
%let iamusr = MOCK_PCDS_USR;
%let prefix = x01f5bc1;
%let email_to = MOCK_EMAIL_TO;
%let out_dir = .;
libname sas_lib "WORK";
/* Cache library for extraction staging (prefix-namespaced) */
options dlcreatedir;
libname cache "WORK/x01f5bc1";


/* Connection macros (Python inserts here) */
%macro pb30;
  connect to oracle(user="&iamusr" orapw="MOCK_PB30_PWD" path="@pcbs_mkt_comnn_30"
    buffsize=5000 preserve_comments);
%mend pb30;
%macro pcds;
  connect to oracle(user="&iamusr" orapw="MOCK_PCDS_PWD" path="@pcds_svc"
    buffsize=5000 preserve_comments);
%mend pcds;

/* pull_data macro (REDO=1 default = always re-pull)
   user=/pwd= override credentials (e.g. temp access account) */
%MACRO pull_data(SQL_QUERY, SQL_TBL, REDO=1, server=pcds, user=, pwd=);
  %IF %SYSFUNC(exist(&SQL_TBL.)) AND %eval(&REDO = 0) %THEN %DO;
    %PUT WARNING: &SQL_TBL. already exists, skip;
  %END;
  %ELSE %DO;
    proc sql;
      %IF %length(&user) > 0 %THEN %DO;
        connect to oracle(user="&user" orapw="&pwd" path="@&server"
          buffsize=5000 preserve_comments);
      %END;
      %ELSE %DO;
        %&server
      %END;
      create table &SQL_TBL. as select * from connection to oracle
      ( &SQL_QUERY. );
      disconnect from oracle;
    quit;
  %END;
%MEND pull_data;

/* Time tracking */
%macro start_timer();
  %global _timer_start;
  %let _timer_start = %sysfunc(datetime());
%mend start_timer;

%macro log_time(table=, step=, outpath=);
  %local _now _elapsed;  /* Prevent namespace pollution */
  %let _now = %sysfunc(datetime());
  %let _elapsed = %sysevalf(&_now - &_timer_start);
  %put NOTE: [TIMER] &table. &step.: %sysfunc(putn(&_elapsed, time8.)) elapsed;
  proc sql noprint;
    insert into _timing values("&table.", "&step.",
      "%sysfunc(putn(&_timer_start, datetime20.))",
      "%sysfunc(putn(&_now, datetime20.))",
      &_elapsed);
  quit;
  %let _timer_start = %sysfunc(datetime());
%mend log_time;

/* Email notification */
%macro send_email(subject=, body=);
  %if %length(&email_to) > 0 %then %do;
    filename mymail email "&email_to"
      subject="&subject";
    data _null_;
      file mymail;
      put "&body";
    run;
    filename mymail clear;
  %end;
  %else %put WARNING: email_to not set, skipping email.;
%mend send_email;

/* Initialize timing dataset */
data _timing;
  length table $64 step $16 start $20 end $20 elapsed_sec 8;
  stop;
run;

/* Capture job start time */
%global _job_start;
%let _job_start = %sysfunc(datetime());

/* ========== TABLE MACROS (Python inserts here) ========== */
/* Column metadata discovery for SAS datasets */
/* Column metadata discovery for SAS dataset
   Placeholders:
     {SN}          - SAS-safe table name
     {QNAME}       - qualified name for CSV output
     {SOURCE}      - source identifier
     {TABLE}       - table name
     {SAS_DATASET} - SAS dataset reference
*/

/* Metadata: sas_acct_snap from saslib.ACCT_SNAP */
proc contents data=saslib.ACCT_SNAP out=_meta_acct_snap noprint; run;

proc sql;
    create table _colmeta_acct_snap as
    select
        'sas' as source length=32,
        'acct_snap' as table length=128,
        name as column_name length=64,
        case
            when type = 1 and (upcase(format) like '%DATETIME%'
                or upcase(format) like '%TIME%'
                or upcase(informat) like '%DATETIME%'
                or upcase(informat) like '%TIME%')
                then 'DATETIME'
            when type = 1 and (upcase(format) like '%DATE%'
                or upcase(format) like '%DDMMYY%'
                or upcase(format) like '%MMDDYY%'
                or upcase(format) like '%YYMMDD%'
                or upcase(informat) like '%DATE%')
                then 'DATE'
            when type = 1 then 'NUMBER'
            when type = 2 then cats('VARCHAR(', length, ')')
            else 'UNKNOWN'
        end as data_type length=32
    from _meta_acct_snap
    order by name;
quit;

proc export data=_colmeta_acct_snap outfile="&out_dir./sas_acct_snap_meta.csv"
    dbms=csv replace;
    putnames=yes;
run;
proc delete data=_meta_acct_snap _colmeta_acct_snap; run;

/* --- Row extraction (data-driven) --- */
/* Oracle row extraction (passthrough)
   Placeholders:
     {CTE_VARS}      - %let _cteN = WITH ... AS (...); statements (or empty)
     {ROW_REDO}      - 0 or 1
     {ORA_DATALINES} - table|dsname|qname|date_expr|conn_macro|idx|where_clause rows
*/


%let _row_redo = 0;

data _ora_map;
    length table $128 dsname $32 qname $64 date_expr $200 conn_macro $32 idx $4 where_clause $500;
    infile datalines dlm='|' truncover;
    input table $ dsname $ qname $ date_expr $ conn_macro $ idx $ where_clause $;
    datalines;
CUST_DAILY_VW|pcds_ts_cust_daily|pcds_ts_cust_daily|RPT_TS|pcds|1|RPT_TS <= TIMESTAMP ''2026-03-17 00:00:00''
ACCT_MASTER|pcds_acct_master|pcds_acct_master|ACCT_DT|pcds|2|ACCT_DT <= DATE ''2026-03-17''
POS_DAILY|oracle_pos_daily|oracle_pos_daily|POS_DT|pb30|4|POS_DT <= DATE ''2026-03-17''
MONTH_SUMMARY|oracle_month_summary|oracle_month_summary|MONTH_FLAG|pb30|5|MONTH_FLAG <= 20260317
;
run;

%macro _row_oracle(table=, dsname=, qname=, date_expr=, conn_macro=, where_clause=, idx=);
    %local _outpath _cte_val;
    %let _outpath = &out_dir./&qname._row.csv;

    %if &_row_redo = 0 and %sysfunc(exist(cache.rc_&dsname)) %then %do;
        %put NOTE: Cached rc_&dsname found - skipping;
        proc export data=cache.rc_&dsname outfile="&_outpath" dbms=csv replace; run;
        %return;
    %end;

    %if %symexist(_cte&idx) %then %let _cte_val = &&_cte&idx;
    %else %let _cte_val = ;

    %put NOTE: [&qname] SQL: &_cte_val select &date_expr as date_value, count(*) as row_count from &table where &where_clause group by &date_expr;
    %start_timer();
    proc sql;
        %&conn_macro
        create table cache.rc_&dsname as
        select * from connection to oracle (
            &_cte_val
            select &date_expr as date_value, count(*) as row_count
            from &table
            %if %length(&where_clause) > 0 %then where &where_clause;
            group by &date_expr
        );
        disconnect from oracle;
    quit;

    proc export data=cache.rc_&dsname outfile="&_outpath" dbms=csv replace; run;
    %log_time(table=&qname, step=row, outpath=&out_dir.);
%mend _row_oracle;

data _null_;
    set _ora_map;
    length _cmd $2000;
    _cmd = cats(
        '%nrstr(%_row_oracle)(',
        'table=', strip(table),
        ', dsname=', strip(dsname),
        ', qname=', strip(qname),
        ', date_expr=', strip(date_expr),
        ', conn_macro=', strip(conn_macro),
        ', where_clause=', strip(where_clause),
        ', idx=', strip(idx),
        ')'
    );
    call execute(_cmd);
run;
proc delete data=_ora_map; run;


/* SAS dataset row extraction (local proc sql)
   Placeholders:
     {ROW_REDO}      - 0 or 1 (only used when no Oracle block precedes)
     {SAS_DATALINES} - table|dsname|qname|date_expr|where_clause rows
*/



data _sas_map;
    length table $128 dsname $32 qname $64 date_expr $200 where_clause $500;
    infile datalines dlm='|' truncover;
    input table $ dsname $ qname $ date_expr $ where_clause $;
    datalines;
saslib.ACCT_SNAP|sas_acct_snap|sas_acct_snap|SNAP_DTTM|SNAP_DTTM <= '17MAR2026:00:00:00'dt
;
run;

%macro _row_sas(table=, dsname=, qname=, date_expr=, where_clause=);
    %local _outpath;
    %let _outpath = &out_dir./&qname._row.csv;

    %if &_row_redo = 0 and %sysfunc(exist(cache.rc_&dsname)) %then %do;
        %put NOTE: Cached rc_&dsname found - skipping;
        proc export data=cache.rc_&dsname outfile="&_outpath" dbms=csv replace; run;
        %return;
    %end;

    %put NOTE: [&qname] SQL: select &date_expr as date_value, count(*) as row_count from &table where &where_clause group by &date_expr;
    %start_timer();
    proc sql;
        create table cache.rc_&dsname as
        select &date_expr as date_value, count(*) as row_count
        from &table
        %if %length(&where_clause) > 0 %then where &where_clause;
        group by &date_expr;
    quit;

    proc export data=cache.rc_&dsname outfile="&_outpath" dbms=csv replace; run;
    %log_time(table=&qname, step=row, outpath=&out_dir.);
%mend _row_sas;

data _null_;
    set _sas_map;
    length _cmd $2000;
    _cmd = cats(
        '%nrstr(%_row_sas)(',
        'table=', strip(table),
        ', dsname=', strip(dsname),
        ', qname=', strip(qname),
        ', date_expr=', strip(date_expr),
        ', where_clause=', strip(where_clause),
        ')'
    );
    call execute(_cmd);
run;
proc delete data=_sas_map; run;


/* ========== RUNNER (Python inserts here) ========== */
/* Row extraction driven by table_date_map (macros above) */

%let _job_end = %sysfunc(datetime());
%let _job_elapsed = %sysevalf(&_job_end - &_job_start);
/* %send_email(subject=dtrack row extraction complete, body=Row extraction finished. Elapsed: %sysfunc(putn(%nrstr(&_job_elapsed), time8.)). Output: &out_dir.); */


/* Export timing log */
proc export data=_timing outfile="./_timing.csv" dbms=csv replace; run;
