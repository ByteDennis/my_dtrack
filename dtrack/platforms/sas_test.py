"""Generate runnable SAS scripts that wrap a user-supplied body with the
right LIBNAME or PROC SQL CONNECT scaffolding.

Used by the Playground SAS engine: the user writes SAS code (libname mode)
or native passthrough SQL (procsql mode) in the editor, picks Oracle or
Hadoop and a connection method, and gets back a complete `.sas` file ready
to submit. Credentials are emitted as `%let oracle_usr / %let oracle_pwd`
macros at the top so secrets never live in the downloaded file.
"""

from typing import Optional


_SAS_HADOOP_OPTIONS = (
    'options set=SAS_HADOOP_JAR_PATH    "/sas/bin/Software/Hadoop/OSH/Jars";\n'
    'options set=SAS_HADOOP_CONFIG_PATH "/sas/bin/Software/Hadoop/OSH/PROD";'
)
_SAS_HADOOP_TRUSTSTORE = "/sas/bin/Software/Hadoop/OSH/PROD/truststore.jks"

# Default body when the user leaves the editor empty — a sane "does the
# connection work" check that exercises the libref/connect alias.
_DEFAULT_LIBNAME_BODY = "/* Type SAS here. The libref `{lib}` is mounted. */"
_DEFAULT_PROCSQL_BODY = "select 1 from dual"


def _libname_body(body: Optional[str], lib: str) -> str:
    return body if body and body.strip() else _DEFAULT_LIBNAME_BODY.format(lib=lib)


def _procsql_body(body: Optional[str], alias: str) -> str:
    """Wrap user's native SQL with `select * from connection to <alias> (...)`.

    This is the standard PROC SQL passthrough form — the user types only the
    inner native Oracle/Hive SELECT, and we wrap it once so the result is a
    valid SAS dataset. If the body looks like the user already wrote the
    full wrapper themselves (`from connection to`), we pass it through
    unchanged.
    """
    s = (body or "").strip() or _DEFAULT_PROCSQL_BODY
    if "from connection to" in s.lower():
        return s
    # Indent the user's SQL one level inside the wrapper.
    indented = "\n        ".join(s.splitlines())
    return f"select * from connection to {alias} (\n        {indented}\n    );"


def oracle_libname_script(svc: str, body: Optional[str] = None) -> str:
    """LIBNAME mount to Oracle + user body + clear."""
    return f"""\
/* Oracle libname — fill in credentials, then submit. */
%let oracle_usr = YOUR_USER;
%let oracle_pwd = YOUR_PASSWORD;

libname ora oracle user=&oracle_usr password=&oracle_pwd path={svc};

{_libname_body(body, "ora")}

libname ora clear;
"""


def oracle_procsql_script(svc: str, body: Optional[str] = None) -> str:
    """PROC SQL CONNECT to Oracle + user body + disconnect."""
    return f"""\
/* Oracle PROC SQL CONNECT — fill in credentials, then submit. */
%let oracle_usr = YOUR_USER;
%let oracle_pwd = YOUR_PASSWORD;

proc sql;
    connect to oracle as ora
        (user=&oracle_usr password=&oracle_pwd path={svc});

    {_procsql_body(body, "ora")}

    disconnect from ora;
quit;
"""


def hadoop_libname_script(server: str, host: str, schema: str,
                          body: Optional[str] = None,
                          port: int = 10000,
                          truststore: Optional[str] = None) -> str:
    """LIBNAME mount to Hadoop/Hive (Kerberos + SSL) + user body + clear."""
    ts = truststore or _SAS_HADOOP_TRUSTSTORE
    return f"""\
/* Hadoop/Hive libname (Kerberos + SSL). */
{_SAS_HADOOP_OPTIONS}

libname hdp hadoop server="{server}"
    port={port} schema={schema} subprotocol=hive2 LOGIN_TIMEOUT=300
    uri='jdbc:hive2://{server}:{port}/usrf;principal=hive/{server}@{host};ssl=true;sslTrustStore={ts};';

{_libname_body(body, "hdp")}

libname hdp clear;
"""


def hadoop_procsql_script(server: str, host: str, schema: str,
                          body: Optional[str] = None,
                          port: int = 10000,
                          truststore: Optional[str] = None) -> str:
    """PROC SQL CONNECT to Hadoop (Kerberos + SSL) + user body + disconnect."""
    ts = truststore or _SAS_HADOOP_TRUSTSTORE
    return f"""\
/* Hadoop PROC SQL CONNECT (Kerberos + SSL). */
{_SAS_HADOOP_OPTIONS}

proc sql;
    connect to hadoop as hdp
        (server="{server}" port={port} schema={schema} subprotocol=hive2
         uri='jdbc:hive2://{server}:{port}/usrf;principal=hive/{server}@{host};ssl=true;sslTrustStore={ts};');

    {_procsql_body(body, "hdp")}

    disconnect from hdp;
quit;
"""


def gen_sas_script(kind: str, method: str, *,
                   body: Optional[str] = None,
                   svc: Optional[str] = None,
                   server: Optional[str] = None,
                   host: Optional[str] = None,
                   schema: Optional[str] = None,
                   port: int = 10000,
                   truststore: Optional[str] = None) -> str:
    """Dispatch on (kind, method) and return SAS source wrapping `body`.

    kind   in {"oracle", "hadoop"}
    method in {"libname", "procsql"}
    """
    kind = (kind or "").lower()
    method = (method or "").lower()

    if kind == "oracle":
        if not svc:
            raise ValueError("Oracle script requires `svc` (service name).")
        if method == "libname":
            return oracle_libname_script(svc, body)
        if method == "procsql":
            return oracle_procsql_script(svc, body)
    elif kind == "hadoop":
        for name, val in (("server", server), ("host", host), ("schema", schema)):
            if not val:
                raise ValueError(f"Hadoop script requires `{name}`.")
        if method == "libname":
            return hadoop_libname_script(server, host, schema, body,
                                          port=port, truststore=truststore)
        if method == "procsql":
            return hadoop_procsql_script(server, host, schema, body,
                                          port=port, truststore=truststore)
    raise ValueError(f"Unsupported (kind={kind!r}, method={method!r}).")


# Backward-compat alias so older callers/tests still work.
gen_test_connect = gen_sas_script
