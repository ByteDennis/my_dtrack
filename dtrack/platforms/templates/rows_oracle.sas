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
    %local _outpath _cte_val _rc;
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

    %let _rc = &SYSERR;
    %if &_rc > 4 %then %do;
        %put ERROR: [&qname] Row extraction failed (SYSERR=&_rc) - skipping to next table;
        options obs=max nosyntaxcheck;
        %return;
    %end;

    proc export data=cache.rc_&dsname outfile="&_outpath" dbms=csv replace; run;
    %log_time(table=&qname, step=row, outpath=&out_dir.);

    /* Column metadata: fetch 1 row, proc contents -> _columns.csv */
    %local _colpath;
    %let _colpath = &out_dir./&qname._columns.csv;
    %if %sysfunc(fileexist(&_colpath)) = 0 %then %do;
        proc sql;
            %&conn_macro
            create table _s_&dsname as
            select * from connection to oracle (
                &_cte_val
                select * from &table where rownum <= 1
            );
            disconnect from oracle;
        quit;

        %if &SYSERR > 4 %then %do;
            %put WARNING: [&qname] Column metadata fetch failed - skipping metadata;
            options obs=max nosyntaxcheck;
            %return;
        %end;

        proc contents data=_s_&dsname out=_m_&dsname noprint; run;
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
        proc delete data=_s_&dsname _m_&dsname _c_&dsname; run;
    %end;
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
