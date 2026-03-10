options mprint symbolgen;

/* Credentials (filled by Python from dtrack.conf) */
%let iamusr = {pcds_usr};
%let prefix = {prefix};
%let email_to = {email_to};
%let out_dir = {out_dir};
libname sas_lib "{sas_lib}";
{user_credentials}

/* Connection macros (Python inserts here) */
{conn_macros}

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
{table_macros}

/* ========== RUNNER (Python inserts here) ========== */
{runner}

/* Export timing log */
proc export data=_timing outfile="{out_dir}/_timing.csv" dbms=csv replace; run;
