options mprint symbolgen;

/* Credentials (filled by Python from .env) */
%let iamusr = my_oracle_user;
%let prefix = 1be0c16c;
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
%macro get_colstats_cust_daily();
    %put NOTE: ===== OPTIMIZED EXTRACTION (single scan) ====;
    %put NOTE: Table: cust_daily (CUST_DAILY);
    %put NOTE: Columns: 4 numeric + 3 categorical = 7 total;
    %put NOTE: Method: Single table scan (not 7 separate scans);

    /* ===== Single table scan for all 7 columns ===== */

    %let _sql_main = SELECT /*+ PARALLEL(3) */ 
        TRUNC(TRUNC(RPT_DT), ''IW'') AS dt,
        COUNT(*) AS n_total,
        COUNT(AMT) AS AMT_not_null,
        COUNT(DISTINCT AMT) AS AMT_n_unique,
        AVG(AMT) AS AMT_mean,
        STDDEV(AMT) AS AMT_std,
        MIN(AMT) AS AMT_min,
        MAX(AMT) AS AMT_max,
        COUNT(BATCH_NO) AS BATCH_NO_not_null,
        COUNT(DISTINCT BATCH_NO) AS BATCH_NO_n_unique,
        AVG(BATCH_NO) AS BATCH_NO_mean,
        STDDEV(BATCH_NO) AS BATCH_NO_std,
        MIN(BATCH_NO) AS BATCH_NO_min,
        MAX(BATCH_NO) AS BATCH_NO_max,
        COUNT(CUSTOMER_ID) AS CUSTOMER_ID_not_null,
        COUNT(DISTINCT CUSTOMER_ID) AS CUSTOMER_ID_n_unique,
        AVG(CUSTOMER_ID) AS CUSTOMER_ID_mean,
        STDDEV(CUSTOMER_ID) AS CUSTOMER_ID_std,
        MIN(CUSTOMER_ID) AS CUSTOMER_ID_min,
        MAX(CUSTOMER_ID) AS CUSTOMER_ID_max,
        COUNT(OLD_SYSTEM_ID) AS OLD_SYSTEM_ID_not_null,
        COUNT(DISTINCT OLD_SYSTEM_ID) AS OLD_SYSTEM_ID_n_unique,
        AVG(OLD_SYSTEM_ID) AS OLD_SYSTEM_ID_mean,
        STDDEV(OLD_SYSTEM_ID) AS OLD_SYSTEM_ID_std,
        MIN(OLD_SYSTEM_ID) AS OLD_SYSTEM_ID_min,
        MAX(OLD_SYSTEM_ID) AS OLD_SYSTEM_ID_max,
        SUM(CASE WHEN LEGACY_FLAG IS NULL THEN 1 ELSE 0 END) AS LEGACY_FLAG_n_missing,
        COUNT(DISTINCT LEGACY_FLAG) AS LEGACY_FLAG_n_unique,
        MIN(LEGACY_FLAG) AS LEGACY_FLAG_min,
        MAX(LEGACY_FLAG) AS LEGACY_FLAG_max,
        SUM(CASE WHEN REGION IS NULL THEN 1 ELSE 0 END) AS REGION_n_missing,
        COUNT(DISTINCT REGION) AS REGION_n_unique,
        MIN(REGION) AS REGION_min,
        MAX(REGION) AS REGION_max,
        SUM(CASE WHEN STATUS IS NULL THEN 1 ELSE 0 END) AS STATUS_n_missing,
        COUNT(DISTINCT STATUS) AS STATUS_n_unique,
        MIN(STATUS) AS STATUS_min,
        MAX(STATUS) AS STATUS_max
    FROM CUST_DAILY
    WHERE 1=1 AND ((STATUS = ''''A'''') AND TRUNC(RPT_DT) IN (DATE ''''2023-01-09'''', DATE ''''2023-03-06'''', DATE ''''2023-08-14'''', DATE ''''2023-09-04'''', DATE ''''2023-09-18'''', DATE ''''2023-10-30'''', DATE ''''2023-12-25'''', DATE ''''2024-02-05'''', DATE ''''2024-04-29'''', DATE ''''2024-10-28'''', DATE ''''2024-11-11'''', DATE ''''2024-11-25'''', DATE ''''2024-12-09'''', DATE ''''2024-12-30''''))
    GROUP BY TRUNC(TRUNC(RPT_DT), ''IW'');
    %pull_data(&_sql_main, _wide_cust_daily, server=pcds);

    /* Reshape from wide to long format */
    data _colstats_cust_daily;
        set _wide_cust_daily;
        length column_name $32 col_type $12 min_val $100 max_val $100;

        column_name = 'AMT';
        col_type = 'numeric';
        n_missing = n_total - AMT_not_null;
        n_unique = AMT_n_unique;
        mean = AMT_mean;
        std = AMT_std;
        min_val = strip(put(AMT_min, best32.));
        max_val = strip(put(AMT_max, best32.));
        top_10 = '';
        output;

        column_name = 'BATCH_NO';
        col_type = 'numeric';
        n_missing = n_total - BATCH_NO_not_null;
        n_unique = BATCH_NO_n_unique;
        mean = BATCH_NO_mean;
        std = BATCH_NO_std;
        min_val = strip(put(BATCH_NO_min, best32.));
        max_val = strip(put(BATCH_NO_max, best32.));
        top_10 = '';
        output;

        column_name = 'CUSTOMER_ID';
        col_type = 'numeric';
        n_missing = n_total - CUSTOMER_ID_not_null;
        n_unique = CUSTOMER_ID_n_unique;
        mean = CUSTOMER_ID_mean;
        std = CUSTOMER_ID_std;
        min_val = strip(put(CUSTOMER_ID_min, best32.));
        max_val = strip(put(CUSTOMER_ID_max, best32.));
        top_10 = '';
        output;

        column_name = 'OLD_SYSTEM_ID';
        col_type = 'numeric';
        n_missing = n_total - OLD_SYSTEM_ID_not_null;
        n_unique = OLD_SYSTEM_ID_n_unique;
        mean = OLD_SYSTEM_ID_mean;
        std = OLD_SYSTEM_ID_std;
        min_val = strip(put(OLD_SYSTEM_ID_min, best32.));
        max_val = strip(put(OLD_SYSTEM_ID_max, best32.));
        top_10 = '';
        output;

        column_name = 'LEGACY_FLAG';
        col_type = 'categorical';
        n_missing = LEGACY_FLAG_n_missing;
        n_unique = LEGACY_FLAG_n_unique;
        mean = .;
        std = .;
        min_val = LEGACY_FLAG_min;
        max_val = LEGACY_FLAG_max;
        top_10 = '';
        output;

        column_name = 'REGION';
        col_type = 'categorical';
        n_missing = REGION_n_missing;
        n_unique = REGION_n_unique;
        mean = .;
        std = .;
        min_val = REGION_min;
        max_val = REGION_max;
        top_10 = '';
        output;

        column_name = 'STATUS';
        col_type = 'categorical';
        n_missing = STATUS_n_missing;
        n_unique = STATUS_n_unique;
        mean = .;
        std = .;
        min_val = STATUS_min;
        max_val = STATUS_max;
        top_10 = '';
        output;

        keep dt column_name col_type n_total n_missing n_unique mean std min_val max_val top_10;
    run;

    /* ===== Single top-10 query for all 3 categorical columns ===== */

    %let _sql_topn = SELECT %str(%'')LEGACY_FLAG%str(%'') AS column_name, dt, val, cnt FROM (
        SELECT TRUNC(TRUNC(RPT_DT), ''IW'') AS dt, LEGACY_FLAG AS val, COUNT(*) AS cnt,
               ROW_NUMBER() OVER (PARTITION BY TRUNC(TRUNC(RPT_DT), ''IW'') ORDER BY COUNT(*) DESC) AS rn
        FROM CUST_DAILY
        WHERE LEGACY_FLAG IS NOT NULL AND ((STATUS = ''''A'''') AND TRUNC(RPT_DT) IN (DATE ''''2023-01-09'''', DATE ''''2023-03-06'''', DATE ''''2023-08-14'''', DATE ''''2023-09-04'''', DATE ''''2023-09-18'''', DATE ''''2023-10-30'''', DATE ''''2023-12-25'''', DATE ''''2024-02-05'''', DATE ''''2024-04-29'''', DATE ''''2024-10-28'''', DATE ''''2024-11-11'''', DATE ''''2024-11-25'''', DATE ''''2024-12-09'''', DATE ''''2024-12-30''''))
        GROUP BY TRUNC(TRUNC(RPT_DT), ''IW''), LEGACY_FLAG
    ) WHERE rn <= 10
    UNION ALL
    SELECT %str(%'')REGION%str(%'') AS column_name, dt, val, cnt FROM (
        SELECT TRUNC(TRUNC(RPT_DT), ''IW'') AS dt, REGION AS val, COUNT(*) AS cnt,
               ROW_NUMBER() OVER (PARTITION BY TRUNC(TRUNC(RPT_DT), ''IW'') ORDER BY COUNT(*) DESC) AS rn
        FROM CUST_DAILY
        WHERE REGION IS NOT NULL AND ((STATUS = ''''A'''') AND TRUNC(RPT_DT) IN (DATE ''''2023-01-09'''', DATE ''''2023-03-06'''', DATE ''''2023-08-14'''', DATE ''''2023-09-04'''', DATE ''''2023-09-18'''', DATE ''''2023-10-30'''', DATE ''''2023-12-25'''', DATE ''''2024-02-05'''', DATE ''''2024-04-29'''', DATE ''''2024-10-28'''', DATE ''''2024-11-11'''', DATE ''''2024-11-25'''', DATE ''''2024-12-09'''', DATE ''''2024-12-30''''))
        GROUP BY TRUNC(TRUNC(RPT_DT), ''IW''), REGION
    ) WHERE rn <= 10
    UNION ALL
    SELECT %str(%'')STATUS%str(%'') AS column_name, dt, val, cnt FROM (
        SELECT TRUNC(TRUNC(RPT_DT), ''IW'') AS dt, STATUS AS val, COUNT(*) AS cnt,
               ROW_NUMBER() OVER (PARTITION BY TRUNC(TRUNC(RPT_DT), ''IW'') ORDER BY COUNT(*) DESC) AS rn
        FROM CUST_DAILY
        WHERE STATUS IS NOT NULL AND ((STATUS = ''''A'''') AND TRUNC(RPT_DT) IN (DATE ''''2023-01-09'''', DATE ''''2023-03-06'''', DATE ''''2023-08-14'''', DATE ''''2023-09-04'''', DATE ''''2023-09-18'''', DATE ''''2023-10-30'''', DATE ''''2023-12-25'''', DATE ''''2024-02-05'''', DATE ''''2024-04-29'''', DATE ''''2024-10-28'''', DATE ''''2024-11-11'''', DATE ''''2024-11-25'''', DATE ''''2024-12-09'''', DATE ''''2024-12-30''''))
        GROUP BY TRUNC(TRUNC(RPT_DT), ''IW''), STATUS
    ) WHERE rn <= 10;
    %pull_data(&_sql_topn, _topn_all_cust_daily, server=pcds);

    proc sort data=_topn_all_cust_daily; by column_name dt descending cnt; run;

    data _topn_agg_cust_daily(keep=column_name dt top_10);
        length top_10 $2000;
        set _topn_all_cust_daily;
        by column_name dt;
        retain top_10;
        if first.dt then top_10 = catx('', strip(val), '(', strip(put(cnt, best.)), ')');
        else top_10 = catx('; ', top_10, catx('', strip(val), '(', strip(put(cnt, best.)), ')'));
        if last.dt then output;
    run;

    proc sort data=_colstats_cust_daily; by column_name dt; run;
    proc sort data=_topn_agg_cust_daily; by column_name dt; run;

    data _colstats_cust_daily;
        merge _colstats_cust_daily(in=a) _topn_agg_cust_daily(in=b);
        by column_name dt;
        if a;
        if not b then top_10 = '';
    run;

    proc export data=_colstats_cust_daily
        outfile=&out_dir./pcds_cust_daily_col.csv
        dbms=csv replace;
    run;

    proc datasets lib=work nolist;
        delete _colstats_cust_daily _wide_cust_daily _topn_all_cust_daily _topn_agg_cust_daily;
    quit;

    %put NOTE: ===== EXTRACTION COMPLETE: cust_daily ====;

%mend get_colstats_cust_daily;
/* --- txn_monthly: TXN_MONTHLY --- */
%macro get_colstats_txn_monthly();
    %put NOTE: ===== OPTIMIZED EXTRACTION (single scan) ====;
    %put NOTE: Table: txn_monthly (TXN_MONTHLY);
    %put NOTE: Columns: 3 numeric + 4 categorical = 7 total;
    %put NOTE: Method: Single table scan (not 7 separate scans);

    /* ===== Single table scan for all 7 columns ===== */

    %let _sql_main = SELECT /*+ PARALLEL(3) */ 
        MONTH_DT AS dt,
        COUNT(*) AS n_total,
        COUNT(ACCT_ID) AS ACCT_ID_not_null,
        COUNT(DISTINCT ACCT_ID) AS ACCT_ID_n_unique,
        AVG(ACCT_ID) AS ACCT_ID_mean,
        STDDEV(ACCT_ID) AS ACCT_ID_std,
        MIN(ACCT_ID) AS ACCT_ID_min,
        MAX(ACCT_ID) AS ACCT_ID_max,
        COUNT(LEGACY_BATCH_ID) AS LEGACY_BATCH_ID_not_null,
        COUNT(DISTINCT LEGACY_BATCH_ID) AS LEGACY_BATCH_ID_n_unique,
        AVG(LEGACY_BATCH_ID) AS LEGACY_BATCH_ID_mean,
        STDDEV(LEGACY_BATCH_ID) AS LEGACY_BATCH_ID_std,
        MIN(LEGACY_BATCH_ID) AS LEGACY_BATCH_ID_min,
        MAX(LEGACY_BATCH_ID) AS LEGACY_BATCH_ID_max,
        COUNT(TXN_AMT) AS TXN_AMT_not_null,
        COUNT(DISTINCT TXN_AMT) AS TXN_AMT_n_unique,
        AVG(TXN_AMT) AS TXN_AMT_mean,
        STDDEV(TXN_AMT) AS TXN_AMT_std,
        MIN(TXN_AMT) AS TXN_AMT_min,
        MAX(TXN_AMT) AS TXN_AMT_max,
        SUM(CASE WHEN CHANNEL IS NULL THEN 1 ELSE 0 END) AS CHANNEL_n_missing,
        COUNT(DISTINCT CHANNEL) AS CHANNEL_n_unique,
        MIN(CHANNEL) AS CHANNEL_min,
        MAX(CHANNEL) AS CHANNEL_max,
        SUM(CASE WHEN OLD_PROCESSING_FLAG IS NULL THEN 1 ELSE 0 END) AS OLD_PROCESSING_FLAG_n_missing,
        COUNT(DISTINCT OLD_PROCESSING_FLAG) AS OLD_PROCESSING_FLAG_n_unique,
        MIN(OLD_PROCESSING_FLAG) AS OLD_PROCESSING_FLAG_min,
        MAX(OLD_PROCESSING_FLAG) AS OLD_PROCESSING_FLAG_max,
        SUM(CASE WHEN SRC_SYSTEM_CODE IS NULL THEN 1 ELSE 0 END) AS SRC_SYSTEM_CODE_n_missing,
        COUNT(DISTINCT SRC_SYSTEM_CODE) AS SRC_SYSTEM_CODE_n_unique,
        MIN(SRC_SYSTEM_CODE) AS SRC_SYSTEM_CODE_min,
        MAX(SRC_SYSTEM_CODE) AS SRC_SYSTEM_CODE_max,
        SUM(CASE WHEN TXN_TYPE IS NULL THEN 1 ELSE 0 END) AS TXN_TYPE_n_missing,
        COUNT(DISTINCT TXN_TYPE) AS TXN_TYPE_n_unique,
        MIN(TXN_TYPE) AS TXN_TYPE_min,
        MAX(TXN_TYPE) AS TXN_TYPE_max
    FROM TXN_MONTHLY
    WHERE 1=1 AND (MONTH_DT IN (202002, 202004, 202006, 202007, 202009, 202010, 202012, 202101, 202102, 202104, 202105, 202201, 202202, 202203, 202204, 202206, 202208, 202210, 202211, 202212, 202302, 202303, 202304, 202305, 202306, 202308, 202309, 202310, 202311, 202312, 202401, 202402, 202403, 202405, 202406, 202408, 202409, 202411, 202412, 202501, 202503, 202507, 202509, 202510, 202512, 202601, 202602, 202603, 202604, 202605, 202606, 202609, 202610, 202612, 202701, 202703, 202704, 202705, 202706, 202708, 202710, 202802, 202803, 202804, 202805, 202806, 202808, 202809, 202810, 202811, 202812, 202902, 202903, 202904, 202906, 202907, 202908, 202909, 202910, 202912, 203001, 203003, 203004, 203007, 203009, 203010, 203011, 203012, 203101, 203102, 203103, 203105, 203108, 203109, 203110, 203111, 203112, 203201, 203205, 203206))
    GROUP BY MONTH_DT;
    %pull_data(&_sql_main, _wide_txn_monthly, server=pb23);

    /* Reshape from wide to long format */
    data _colstats_txn_monthly;
        set _wide_txn_monthly;
        length column_name $32 col_type $12 min_val $100 max_val $100;

        column_name = 'ACCT_ID';
        col_type = 'numeric';
        n_missing = n_total - ACCT_ID_not_null;
        n_unique = ACCT_ID_n_unique;
        mean = ACCT_ID_mean;
        std = ACCT_ID_std;
        min_val = strip(put(ACCT_ID_min, best32.));
        max_val = strip(put(ACCT_ID_max, best32.));
        top_10 = '';
        output;

        column_name = 'LEGACY_BATCH_ID';
        col_type = 'numeric';
        n_missing = n_total - LEGACY_BATCH_ID_not_null;
        n_unique = LEGACY_BATCH_ID_n_unique;
        mean = LEGACY_BATCH_ID_mean;
        std = LEGACY_BATCH_ID_std;
        min_val = strip(put(LEGACY_BATCH_ID_min, best32.));
        max_val = strip(put(LEGACY_BATCH_ID_max, best32.));
        top_10 = '';
        output;

        column_name = 'TXN_AMT';
        col_type = 'numeric';
        n_missing = n_total - TXN_AMT_not_null;
        n_unique = TXN_AMT_n_unique;
        mean = TXN_AMT_mean;
        std = TXN_AMT_std;
        min_val = strip(put(TXN_AMT_min, best32.));
        max_val = strip(put(TXN_AMT_max, best32.));
        top_10 = '';
        output;

        column_name = 'CHANNEL';
        col_type = 'categorical';
        n_missing = CHANNEL_n_missing;
        n_unique = CHANNEL_n_unique;
        mean = .;
        std = .;
        min_val = CHANNEL_min;
        max_val = CHANNEL_max;
        top_10 = '';
        output;

        column_name = 'OLD_PROCESSING_FLAG';
        col_type = 'categorical';
        n_missing = OLD_PROCESSING_FLAG_n_missing;
        n_unique = OLD_PROCESSING_FLAG_n_unique;
        mean = .;
        std = .;
        min_val = OLD_PROCESSING_FLAG_min;
        max_val = OLD_PROCESSING_FLAG_max;
        top_10 = '';
        output;

        column_name = 'SRC_SYSTEM_CODE';
        col_type = 'categorical';
        n_missing = SRC_SYSTEM_CODE_n_missing;
        n_unique = SRC_SYSTEM_CODE_n_unique;
        mean = .;
        std = .;
        min_val = SRC_SYSTEM_CODE_min;
        max_val = SRC_SYSTEM_CODE_max;
        top_10 = '';
        output;

        column_name = 'TXN_TYPE';
        col_type = 'categorical';
        n_missing = TXN_TYPE_n_missing;
        n_unique = TXN_TYPE_n_unique;
        mean = .;
        std = .;
        min_val = TXN_TYPE_min;
        max_val = TXN_TYPE_max;
        top_10 = '';
        output;

        keep dt column_name col_type n_total n_missing n_unique mean std min_val max_val top_10;
    run;

    /* ===== Single top-10 query for all 4 categorical columns ===== */

    %let _sql_topn = SELECT %str(%'')CHANNEL%str(%'') AS column_name, dt, val, cnt FROM (
        SELECT MONTH_DT AS dt, CHANNEL AS val, COUNT(*) AS cnt,
               ROW_NUMBER() OVER (PARTITION BY MONTH_DT ORDER BY COUNT(*) DESC) AS rn
        FROM TXN_MONTHLY
        WHERE CHANNEL IS NOT NULL AND (MONTH_DT IN (202002, 202004, 202006, 202007, 202009, 202010, 202012, 202101, 202102, 202104, 202105, 202201, 202202, 202203, 202204, 202206, 202208, 202210, 202211, 202212, 202302, 202303, 202304, 202305, 202306, 202308, 202309, 202310, 202311, 202312, 202401, 202402, 202403, 202405, 202406, 202408, 202409, 202411, 202412, 202501, 202503, 202507, 202509, 202510, 202512, 202601, 202602, 202603, 202604, 202605, 202606, 202609, 202610, 202612, 202701, 202703, 202704, 202705, 202706, 202708, 202710, 202802, 202803, 202804, 202805, 202806, 202808, 202809, 202810, 202811, 202812, 202902, 202903, 202904, 202906, 202907, 202908, 202909, 202910, 202912, 203001, 203003, 203004, 203007, 203009, 203010, 203011, 203012, 203101, 203102, 203103, 203105, 203108, 203109, 203110, 203111, 203112, 203201, 203205, 203206))
        GROUP BY MONTH_DT, CHANNEL
    ) WHERE rn <= 10
    UNION ALL
    SELECT %str(%'')OLD_PROCESSING_FLAG%str(%'') AS column_name, dt, val, cnt FROM (
        SELECT MONTH_DT AS dt, OLD_PROCESSING_FLAG AS val, COUNT(*) AS cnt,
               ROW_NUMBER() OVER (PARTITION BY MONTH_DT ORDER BY COUNT(*) DESC) AS rn
        FROM TXN_MONTHLY
        WHERE OLD_PROCESSING_FLAG IS NOT NULL AND (MONTH_DT IN (202002, 202004, 202006, 202007, 202009, 202010, 202012, 202101, 202102, 202104, 202105, 202201, 202202, 202203, 202204, 202206, 202208, 202210, 202211, 202212, 202302, 202303, 202304, 202305, 202306, 202308, 202309, 202310, 202311, 202312, 202401, 202402, 202403, 202405, 202406, 202408, 202409, 202411, 202412, 202501, 202503, 202507, 202509, 202510, 202512, 202601, 202602, 202603, 202604, 202605, 202606, 202609, 202610, 202612, 202701, 202703, 202704, 202705, 202706, 202708, 202710, 202802, 202803, 202804, 202805, 202806, 202808, 202809, 202810, 202811, 202812, 202902, 202903, 202904, 202906, 202907, 202908, 202909, 202910, 202912, 203001, 203003, 203004, 203007, 203009, 203010, 203011, 203012, 203101, 203102, 203103, 203105, 203108, 203109, 203110, 203111, 203112, 203201, 203205, 203206))
        GROUP BY MONTH_DT, OLD_PROCESSING_FLAG
    ) WHERE rn <= 10
    UNION ALL
    SELECT %str(%'')SRC_SYSTEM_CODE%str(%'') AS column_name, dt, val, cnt FROM (
        SELECT MONTH_DT AS dt, SRC_SYSTEM_CODE AS val, COUNT(*) AS cnt,
               ROW_NUMBER() OVER (PARTITION BY MONTH_DT ORDER BY COUNT(*) DESC) AS rn
        FROM TXN_MONTHLY
        WHERE SRC_SYSTEM_CODE IS NOT NULL AND (MONTH_DT IN (202002, 202004, 202006, 202007, 202009, 202010, 202012, 202101, 202102, 202104, 202105, 202201, 202202, 202203, 202204, 202206, 202208, 202210, 202211, 202212, 202302, 202303, 202304, 202305, 202306, 202308, 202309, 202310, 202311, 202312, 202401, 202402, 202403, 202405, 202406, 202408, 202409, 202411, 202412, 202501, 202503, 202507, 202509, 202510, 202512, 202601, 202602, 202603, 202604, 202605, 202606, 202609, 202610, 202612, 202701, 202703, 202704, 202705, 202706, 202708, 202710, 202802, 202803, 202804, 202805, 202806, 202808, 202809, 202810, 202811, 202812, 202902, 202903, 202904, 202906, 202907, 202908, 202909, 202910, 202912, 203001, 203003, 203004, 203007, 203009, 203010, 203011, 203012, 203101, 203102, 203103, 203105, 203108, 203109, 203110, 203111, 203112, 203201, 203205, 203206))
        GROUP BY MONTH_DT, SRC_SYSTEM_CODE
    ) WHERE rn <= 10
    UNION ALL
    SELECT %str(%'')TXN_TYPE%str(%'') AS column_name, dt, val, cnt FROM (
        SELECT MONTH_DT AS dt, TXN_TYPE AS val, COUNT(*) AS cnt,
               ROW_NUMBER() OVER (PARTITION BY MONTH_DT ORDER BY COUNT(*) DESC) AS rn
        FROM TXN_MONTHLY
        WHERE TXN_TYPE IS NOT NULL AND (MONTH_DT IN (202002, 202004, 202006, 202007, 202009, 202010, 202012, 202101, 202102, 202104, 202105, 202201, 202202, 202203, 202204, 202206, 202208, 202210, 202211, 202212, 202302, 202303, 202304, 202305, 202306, 202308, 202309, 202310, 202311, 202312, 202401, 202402, 202403, 202405, 202406, 202408, 202409, 202411, 202412, 202501, 202503, 202507, 202509, 202510, 202512, 202601, 202602, 202603, 202604, 202605, 202606, 202609, 202610, 202612, 202701, 202703, 202704, 202705, 202706, 202708, 202710, 202802, 202803, 202804, 202805, 202806, 202808, 202809, 202810, 202811, 202812, 202902, 202903, 202904, 202906, 202907, 202908, 202909, 202910, 202912, 203001, 203003, 203004, 203007, 203009, 203010, 203011, 203012, 203101, 203102, 203103, 203105, 203108, 203109, 203110, 203111, 203112, 203201, 203205, 203206))
        GROUP BY MONTH_DT, TXN_TYPE
    ) WHERE rn <= 10;
    %pull_data(&_sql_topn, _topn_all_txn_monthly, server=pb23);

    proc sort data=_topn_all_txn_monthly; by column_name dt descending cnt; run;

    data _topn_agg_txn_monthly(keep=column_name dt top_10);
        length top_10 $2000;
        set _topn_all_txn_monthly;
        by column_name dt;
        retain top_10;
        if first.dt then top_10 = catx('', strip(val), '(', strip(put(cnt, best.)), ')');
        else top_10 = catx('; ', top_10, catx('', strip(val), '(', strip(put(cnt, best.)), ')'));
        if last.dt then output;
    run;

    proc sort data=_colstats_txn_monthly; by column_name dt; run;
    proc sort data=_topn_agg_txn_monthly; by column_name dt; run;

    data _colstats_txn_monthly;
        merge _colstats_txn_monthly(in=a) _topn_agg_txn_monthly(in=b);
        by column_name dt;
        if a;
        if not b then top_10 = '';
    run;

    proc export data=_colstats_txn_monthly
        outfile=&out_dir./oracle_txn_monthly_col.csv
        dbms=csv replace;
    run;

    proc datasets lib=work nolist;
        delete _colstats_txn_monthly _wide_txn_monthly _topn_all_txn_monthly _topn_agg_txn_monthly;
    quit;

    %put NOTE: ===== EXTRACTION COMPLETE: txn_monthly ====;

%mend get_colstats_txn_monthly;

/* ========== RUNNER (Python inserts here) ========== */
%start_timer();
%get_colstats_cust_daily();
%log_time(table=cust_daily, step=col, outpath=&out_dir.);
%send_email(subject=dtrack col done: cust_daily, body=Table cust_daily col extraction complete. Output: &out_dir./pcds_cust_daily_col.csv);

%start_timer();
%get_colstats_txn_monthly();
%log_time(table=txn_monthly, step=col, outpath=&out_dir.);
%send_email(subject=dtrack col done: txn_monthly, body=Table txn_monthly col extraction complete. Output: &out_dir./oracle_txn_monthly_col.csv);


/* Export timing log */
proc export data=_timing outfile="/path/to/mps/folder/_timing.csv" dbms=csv replace; run;
