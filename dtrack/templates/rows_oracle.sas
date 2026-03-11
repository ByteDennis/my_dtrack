/* Oracle row extraction (passthrough)
   Placeholders:
     {CTE_VARS}      - %let _cteN = WITH ... AS (...); statements (or empty)
     {ROW_REDO}      - 0 or 1
     {ORA_DATALINES} - table|dsname|qname|date_expr|conn_macro|idx|where_clause rows
*/

/*{CTE_VARS}*/
%let _row_redo = /*{ROW_REDO}*/;

data _ora_map;
    length table $128 dsname $32 qname $64 date_expr $200 conn_macro $32 idx $4 where_clause $500;
    infile datalines dlm='|' truncover;
    input table $ dsname $ qname $ date_expr $ conn_macro $ idx $ where_clause $;
    datalines;
/*{ORA_DATALINES}*/
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
