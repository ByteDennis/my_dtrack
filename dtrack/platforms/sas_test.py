"""Generate small SAS scripts to verify libname / PROC SQL CONNECT setups.

These are not part of the extraction pipeline — they're standalone snippets
the user downloads from the Playground and pastes into SAS to confirm a
connection works. Credentials are emitted as `%let` macros at the top of
each script so secrets never need to live in the downloaded file.
"""

from typing import Optional


_SAS_HADOOP_OPTIONS = (
    'options set=SAS_HADOOP_JAR_PATH    "/sas/bin/Software/Hadoop/OSH/Jars";\n'
    'options set=SAS_HADOOP_CONFIG_PATH "/sas/bin/Software/Hadoop/OSH/PROD";\n'
)
_SAS_HADOOP_TRUSTSTORE = "/sas/bin/Software/Hadoop/OSH/PROD/truststore.jks"


def oracle_libname_test(svc: str, table: str) -> str:
    """libname-form connection to Oracle, prints first 10 rows of `table`."""
    return f"""\
/* Oracle libname connection test
   Fill in the %let macros below, then submit. */

%let oracle_usr = YOUR_USER;
%let oracle_pwd = YOUR_PASSWORD;
%let svc        = {svc};
%let target     = {table};

libname ora oracle user=&oracle_usr password=&oracle_pwd path=&svc;

proc print data=ora.&target(obs=10); run;
proc contents data=ora.&target; run;

libname ora clear;
"""


def oracle_procsql_test(svc: str, table: str) -> str:
    """PROC SQL CONNECT TO ORACLE — prints first 10 rows of `table`."""
    return f"""\
/* Oracle PROC SQL CONNECT test
   Fill in the %let macros below, then submit. */

%let oracle_usr = YOUR_USER;
%let oracle_pwd = YOUR_PASSWORD;
%let svc        = {svc};
%let target     = {table};

proc sql;
    connect to oracle as ora
        (user=&oracle_usr password=&oracle_pwd path=&svc);

    select * from connection to ora
        (select * from &target where rownum <= 10);

    disconnect from ora;
quit;
"""


def hadoop_libname_test(server: str, host: str, schema: str, table: str,
                       port: int = 10000,
                       truststore: Optional[str] = None) -> str:
    """libname-form connection to Hadoop/Hive — prints first 10 rows of `table`."""
    ts = truststore or _SAS_HADOOP_TRUSTSTORE
    return f"""\
/* Hadoop/Hive libname connection test (Kerberos + SSL)
   Fill in the %let macros below, then submit. */

%let hdp_server = {server};
%let hdp_host   = {host};
%let schema     = {schema};
%let target     = {table};

{_SAS_HADOOP_OPTIONS}
libname hdp hadoop server="&hdp_server"
    port={port} schema=&schema subprotocol=hive2 LOGIN_TIMEOUT=300
    uri='jdbc:hive2://&hdp_server:{port}/usrf;principal=hive/&hdp_server@&hdp_host;ssl=true;sslTrustStore={ts};';

proc print data=hdp.&target(obs=10); run;
proc contents data=hdp.&target; run;

libname hdp clear;
"""


def hadoop_procsql_test(server: str, host: str, schema: str, table: str,
                       port: int = 10000,
                       truststore: Optional[str] = None) -> str:
    """PROC SQL CONNECT TO HADOOP — prints first 10 rows of `table`."""
    ts = truststore or _SAS_HADOOP_TRUSTSTORE
    return f"""\
/* Hadoop PROC SQL CONNECT test (Kerberos + SSL)
   Fill in the %let macros below, then submit. */

%let hdp_server = {server};
%let hdp_host   = {host};
%let schema     = {schema};
%let target     = {table};

{_SAS_HADOOP_OPTIONS}
proc sql;
    connect to hadoop as hdp
        (server="&hdp_server" port={port} schema=&schema subprotocol=hive2
         uri='jdbc:hive2://&hdp_server:{port}/usrf;principal=hive/&hdp_server@&hdp_host;ssl=true;sslTrustStore={ts};');

    select * from connection to hdp
        (select * from &target limit 10);

    disconnect from hdp;
quit;
"""


def gen_test_connect(kind: str, method: str, *,
                     svc: Optional[str] = None,
                     table: str,
                     server: Optional[str] = None,
                     host: Optional[str] = None,
                     schema: Optional[str] = None,
                     port: int = 10000,
                     truststore: Optional[str] = None) -> str:
    """Dispatch on (kind, method) and return SAS source.

    kind   in {"oracle", "hadoop"}
    method in {"libname", "procsql"}
    """
    kind = (kind or "").lower()
    method = (method or "").lower()

    if kind == "oracle":
        if not svc:
            raise ValueError("Oracle test requires `svc` (service name).")
        if method == "libname":
            return oracle_libname_test(svc, table)
        if method == "procsql":
            return oracle_procsql_test(svc, table)
    elif kind == "hadoop":
        for name, val in (("server", server), ("host", host), ("schema", schema)):
            if not val:
                raise ValueError(f"Hadoop test requires `{name}`.")
        if method == "libname":
            return hadoop_libname_test(server, host, schema, table, port=port,
                                       truststore=truststore)
        if method == "procsql":
            return hadoop_procsql_test(server, host, schema, table, port=port,
                                       truststore=truststore)
    raise ValueError(f"Unsupported (kind={kind!r}, method={method!r}).")
