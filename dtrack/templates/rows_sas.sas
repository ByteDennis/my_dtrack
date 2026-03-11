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
