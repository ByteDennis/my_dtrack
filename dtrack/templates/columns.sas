/* Column statistics template - data-driven extraction
   Placeholders (replaced by Python):
     {SN}                  - SAS-safe table name
     {NAME}                - display name
     {TABLE}               - source table
     {QNAME}               - qualified name for CSV output
     {N_NUMERIC}           - count of numeric columns
     {N_CATEGORICAL}       - count of categorical columns
     {N_COLS}              - total non-date columns
     {REDO}                - 0 or 1
     {PULL_STMT}           - pull data statement (SAS proc sql or %pull_data)
     {CACHE_CHECK_START}   - %if exist ... %else %do (or empty if REDO=1)
     {CACHE_CHECK_END}     - %end (or empty if REDO=1)
     {COL_MAP_ROWS}        - column assignment statements
     {VINTAGE_CALLS}       - data _null_ + %_process_vintage() per vintage
     {STACK_CACHES}        - data step to stack vintage caches
*/

%macro get_colstats_/*{SN}*/();
    %put NOTE: ===== LOCAL COMPUTE EXTRACTION ====;
    %put NOTE: Table: /*{NAME}*/ (/*{TABLE}*/);
    %put NOTE: Columns: /*{N_NUMERIC}*/ numeric + /*{N_CATEGORICAL}*/ categorical = /*{N_COLS}*/ total;
    %put NOTE: REDO=/*{REDO}*/ (1=force re-pull, 0=use cached stats);

    /* ---- Reusable macro: numeric column statistics ---- */
    /* Output: dt, column_name, col_type, col_count, col_distinct, col_max, col_min,
               col_avg, col_std, col_sum, col_sum_sq, col_freq, col_missing */
    %macro _col_numeric(raw_ds=, col=, out_ds=);
        proc sql noprint;
            create table &out_ds as
            select dt,
                "&col" as column_name length=32,
                'numeric' as col_type length=32,
                count(&col) as col_count,
                count(distinct &col) as col_distinct,
                max(&col) as col_max,
                min(&col) as col_min,
                avg(&col) as col_avg,
                std(&col) as col_std,
                sum(&col) as col_sum,
                sum(&col * &col) as col_sum_sq,
                '' as col_freq length=2000,
                count(*) - count(&col) as col_missing
            from &raw_ds
            group by dt;
        quit;
    %mend _col_numeric;

    /* ---- Reusable macro: categorical column statistics ---- */
    /* Output: same schema as numeric, with col_freq = top-10 semicolon list */
    %macro _col_categorical(raw_ds=, col=, out_ds=);
        /* Frequency table */
        proc sql noprint;
            create table _freq_raw_ as
            select dt, &col as p_col, count(*) as value_freq
            from &raw_ds
            group by dt, &col;
        quit;

        /* Top 10 concatenation per dt */
        proc sort data=_freq_raw_ out=_freq_sorted_;
            by dt descending value_freq p_col;
        run;
        data _t10_(keep=dt col_freq);
            length col_freq $2000 _entry $200;
            set _freq_sorted_; by dt;
            where p_col is not missing;
            retain col_freq _rn;
            if first.dt then do; col_freq = ''; _rn = 0; end;
            if _rn < 10 then do;
                _rn + 1;
                _entry = catx('', strip(vvalue(p_col)), '(', strip(put(value_freq, best.)), ')');
                if col_freq = '' then col_freq = _entry;
                else col_freq = catx('; ', col_freq, _entry);
            end;
            if last.dt then output;
        run;

        /* Aggregate stats from frequency table */
        proc sql noprint;
            create table _agg_ as
            select dt,
                "&col" as column_name length=32,
                'categorical' as col_type length=32,
                sum(value_freq) as col_count,
                count(value_freq) as col_distinct,
                max(value_freq) as col_max,
                min(value_freq) as col_min,
                avg(value_freq) as col_avg,
                std(value_freq) as col_std,
                sum(value_freq) as col_sum,
                sum(value_freq * value_freq) as col_sum_sq
            from _freq_raw_
            group by dt;
        quit;

        /* Missing count per dt */
        proc sql noprint;
            create table _miss_ as
            select dt, coalesce(value_freq, 0) as col_missing
            from _freq_raw_
            where p_col is missing;
        quit;

        /* Merge all together */
        proc sort data=_agg_; by dt; run;
        proc sort data=_t10_; by dt; run;
        proc sort data=_miss_; by dt; run;
        data &out_ds;
            merge _agg_(in=a) _t10_(in=b) _miss_(in=c);
            by dt;
            if a;
            if not b then col_freq = '';
            if not c then col_missing = 0;
        run;

        proc datasets lib=work nolist; delete _freq_raw_ _freq_sorted_ _t10_ _agg_ _miss_; quit;
    %mend _col_categorical;

    /* ---- Reusable macro: process one vintage ---- */
    %macro _process_vintage(raw_ds=, cache_ds=);
/*{CACHE_CHECK_START}*/
        %put NOTE: Pulling data into &raw_ds;
        %put NOTE: SQL: &_full_sql;
/*{PULL_STMT}*/
        data _null_;
            set _col_map;
            length _cmd $2000;
            if col_type = 'numeric' then
                _cmd = cats('%nrstr(%_col_numeric)(raw_ds=', "&raw_ds",
                            ', col=', strip(col_name),
                            ', out_ds=_cstat_', strip(put(_n_, 3.)), ')');
            else
                _cmd = cats('%nrstr(%_col_categorical)(raw_ds=', "&raw_ds",
                            ', col=', strip(col_name),
                            ', out_ds=_cstat_', strip(put(_n_, 3.)), ')');
            call execute(_cmd);
        run;
        data &cache_ds; set _cstat_:; run;
        proc delete data=&raw_ds; run;
        proc datasets lib=work nolist; delete _cstat_:; quit;
/*{CACHE_CHECK_END}*/
    %mend _process_vintage;

    /* ---- Column metadata ---- */
    data _col_map;
        length col_name $32 col_type $12;
/*{COL_MAP_ROWS}*/
    run;

    /* ---- Drive all vintages ---- */
/*{VINTAGE_CALLS}*/

    /* ---- Stack and export ---- */
/*{STACK_CACHES}*/
    proc datasets lib=work nolist; delete _col_map; quit;

    proc export data=_colstats_/*{SN}*/
        outfile="&out_dir.//*{QNAME}*/_col.csv"
        dbms=csv replace;
    run;

    proc delete data=_colstats_/*{SN}*/; run;
    %put NOTE: ===== EXTRACTION COMPLETE: /*{NAME}*/ ====;

%mend get_colstats_/*{SN}*/;
