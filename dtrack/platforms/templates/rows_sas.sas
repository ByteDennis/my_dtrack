/* SAS dataset row extraction (local proc sql)
   Placeholders:
     {ROW_REDO}      - 0 or 1 (only used when no Oracle block precedes)
     {SAS_DATALINES} - table|dsname|qname|date_expr|where_clause rows
*/

/*{ROW_REDO}*/

data _sas_map;
    length table $128 dsname $32 qname $64 date_expr $200 where_clause $500;
    infile datalines dlm='|' truncover;
    input table $ dsname $ qname $ date_expr $ where_clause $;
    datalines;
/*{SAS_DATALINES}*/
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

    %if &SYSERR > 4 %then %do;
        %put ERROR: [&qname] Row extraction failed (SYSERR=&SYSERR) - skipping to next table;
        options obs=max nosyntaxcheck;
        %return;
    %end;

    proc export data=cache.rc_&dsname outfile="&_outpath" dbms=csv replace; run;
    %log_time(table=&qname, step=row, outpath=&out_dir.);

    /* Column metadata: proc contents on SAS dataset → _columns.csv */
    %local _colpath;
    %let _colpath = &out_dir./&qname._columns.csv;
    %if %sysfunc(fileexist(&_colpath)) = 0 %then %do;
        proc contents data=&table out=_m_&dsname noprint; run;
        proc sql;
            create table _c_&dsname as
            select
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
            from _m_&dsname
            order by varnum;
        quit;
        proc export data=_c_&dsname outfile="&_colpath" dbms=csv replace; run;
        proc delete data=_m_&dsname _c_&dsname; run;
    %end;
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
