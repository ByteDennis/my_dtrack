/* Column metadata discovery for SAS dataset
   Placeholders:
     {SN}          - SAS-safe table name
     {QNAME}       - qualified name for CSV output
     {SOURCE}      - source identifier
     {TABLE}       - table name
     {SAS_DATASET} - SAS dataset reference
*/

/* Metadata: /*{QNAME}*/ from /*{SAS_DATASET}*/ */
proc contents data=/*{SAS_DATASET}*/ out=_meta_/*{SN}*/ noprint; run;

proc sql;
    create table _colmeta_/*{SN}*/ as
    select
        '/*{SOURCE}*/' as source length=32,
        '/*{TABLE}*/' as table length=128,
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
    from _meta_/*{SN}*/
    order by name;
quit;

proc export data=_colmeta_/*{SN}*/ outfile="&out_dir.//*{QNAME}*/_meta.csv"
    dbms=csv replace;
    putnames=yes;
run;
proc delete data=_meta_/*{SN}*/ _colmeta_/*{SN}*/; run;
