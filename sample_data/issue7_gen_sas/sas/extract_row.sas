options mprint symbolgen;

/* Credentials (filled by Python from .env) */
%let iamusr = my_oracle_user;
%let prefix = 7d17bd27;
%let email_to = user@example.com;
%let out_dir = /path/to/mps/folder;
libname sas_lib "/path/to/sas/lib";

/* Connection macros */
%macro pcds;
  connect to oracle(user="&iamusr" orapw="my_oracle_password" path="@pcds"
    buffsize=5000 preserve_comments);
%mend pcds;
%macro pb23;
  connect to oracle(user="&iamusr" orapw="my_pb23_password" path="@p_uscb_comms_svc"
    buffsize=5000 preserve_comments);
%mend pb23;

/* pull_data macro (REDO=1 default = always re-pull) */
%MACRO pull_data(SQL_QUERY, SQL_TBL, REDO=1, server=pcds);
  %IF %SYSFUNC(exist(&SQL_TBL.)) AND %eval(&REDO = 0) %THEN %DO;
    %PUT WARNING: &SQL_TBL. already exists, skip;
  %END;
  %ELSE %DO;
    proc sql;
      %&server
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
/* --- cust_daily: CUST_DAILY --- */
%macro get_rowcounts_cust_daily();
    %pull_data(%str(SELECT TRUNC(TRUNC(RPT_DT), ''IW'') AS date_value, COUNT(*) AS row_count FROM CUST_DAILY WHERE STATUS = ''A'' GROUP BY TRUNC(TRUNC(RPT_DT), ''IW'')), sas_lib.&prefix._rc_cust_daily, server=pcds);

    proc export data=sas_lib.&prefix._rc_cust_daily
        outfile=&out_dir./pcds_cust_daily_row.csv
        dbms=csv replace;
    run;

    proc delete data=sas_lib.&prefix._rc_cust_daily; run;
%mend get_rowcounts_cust_daily;

/* --- txn_monthly: TXN_MONTHLY --- */
%macro get_rowcounts_txn_monthly();
    %pull_data(%str(SELECT MONTH_DT AS date_value, COUNT(*) AS row_count FROM TXN_MONTHLY  GROUP BY MONTH_DT), sas_lib.&prefix._rc_txn_monthly, server=pb23);

    proc export data=sas_lib.&prefix._rc_txn_monthly
        outfile=&out_dir./oracle_txn_monthly_row.csv
        dbms=csv replace;
    run;

    proc delete data=sas_lib.&prefix._rc_txn_monthly; run;
%mend get_rowcounts_txn_monthly;


/* ========== RUNNER (Python inserts here) ========== */
%start_timer();
%get_rowcounts_cust_daily();
%log_time(table=cust_daily, step=row, outpath=&out_dir.);

%start_timer();
%get_rowcounts_txn_monthly();
%log_time(table=txn_monthly, step=row, outpath=&out_dir.);

%let _job_end = %sysfunc(datetime());
%let _job_elapsed = %sysevalf(&_job_end - &_job_start);
%send_email(subject=dtrack row extraction complete, body=Row extraction finished. Elapsed: %sysfunc(putn(%nrstr(&_job_elapsed), time8.)). Output: &out_dir.);


/* Export timing log */
proc export data=_timing outfile="/path/to/mps/folder/_timing.csv" dbms=csv replace; run;
