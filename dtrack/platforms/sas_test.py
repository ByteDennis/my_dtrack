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
_DEFAULT_LIBNAME_BODY = "select count(*) from {lib}.YOUR_TABLE"
_DEFAULT_PROCSQL_BODY = "select 1 from dual"


def _looks_like_sql_select(s: str) -> bool:
    """True if `s` looks like a bare SELECT statement (no proc sql / data step)."""
    head = s.lstrip().lower()
    return head.startswith("select") or head.startswith("with ")


def _looks_like_sas_block(s: str) -> bool:
    """True if `s` looks like SAS code that already wraps itself
    (proc sql / data step / proc print / etc) — pass through unchanged."""
    head = s.lstrip().lower()
    return any(head.startswith(p) for p in ("proc ", "data ", "%macro", "%let"))


def _libname_body(body: Optional[str], lib: str) -> str:
    """Wrap a body for libname mode.

    Bare SELECT  → wrap with `proc sql; <body>; quit;` so the libref gets used.
    proc/data    → pass through unchanged (already a SAS block).
    Empty editor → wrap a sane `select count(*) from <lib>.YOUR_TABLE` probe.
    Anything else → pass through (treat as multi-statement SAS).
    """
    s = (body or "").strip()
    if not s:
        s = _DEFAULT_LIBNAME_BODY.format(lib=lib)
        return _wrap_proc_sql(s)
    if _looks_like_sas_block(s):
        return s
    if _looks_like_sql_select(s):
        return _wrap_proc_sql(s)
    return s


def _wrap_proc_sql(sql: str) -> str:
    s = sql.rstrip().rstrip(";")
    indented = "\n    ".join(s.splitlines())
    return f"proc sql;\n    {indented};\nquit;"


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


def _oracle_cred_block(user: Optional[str], password: Optional[str]) -> str:
    """Emit the %let credential macros, baking in env values when provided.

    When both user/password are passed in (resolved server-side from .env),
    the script is ready to submit. When either is None, falls back to
    YOUR_USER / YOUR_PASSWORD placeholders the user fills in by hand.
    """
    u = user or "YOUR_USER"
    p = password or "YOUR_PASSWORD"
    note = "" if (user and password) else "  /* edit and submit */"
    return f"%let oracle_usr = {u};{note}\n%let oracle_pwd = {p};{note}"


def oracle_libname_script(svc: str, body: Optional[str] = None, *,
                          user: Optional[str] = None,
                          password: Optional[str] = None) -> str:
    """LIBNAME mount to Oracle + user body + clear."""
    return f"""\
/* Oracle libname */
{_oracle_cred_block(user, password)}

libname ora oracle user=&oracle_usr password=&oracle_pwd path={svc};

{_libname_body(body, "ora")}

libname ora clear;
"""


def oracle_procsql_script(svc: str, body: Optional[str] = None, *,
                          user: Optional[str] = None,
                          password: Optional[str] = None) -> str:
    """PROC SQL CONNECT to Oracle + user body + disconnect."""
    return f"""\
/* Oracle PROC SQL CONNECT */
{_oracle_cred_block(user, password)}

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
    """LIBNAME mount to Hadoop/Hive (Kerberos + SSL) + user body + clear.

    Matches the format used on this user's box — URI path is `/usrf;`,
    libname `hdp` is the libref. Profile values come from
    `HADOOP_LIBNAME_PROFILES`.
    """
    ts = truststore or _SAS_HADOOP_TRUSTSTORE
    return f"""\
/* Hadoop/Hive connection (LIBNAME) */
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
    """PROC SQL CONNECT to Hadoop (Kerberos + SSL) + user body + disconnect.

    Matches the format used on this user's box — uses %let macros to
    parameterize the connect string, URI path is `/default;` (different from
    libname's `/usrf`), and the connect call takes a single &-var arg.
    Profile values come from `HADOOP_PROCSQL_PROFILES` and may differ from
    the libname profile (notably the server).
    """
    ts = truststore or _SAS_HADOOP_TRUSTSTORE
    return f"""\
/* Hadoop PROC SQL CONNECT (Kerberos + SSL) */
%let hdp_server_osh = {server};
%let hadoop_cro_ded = %str(server="&hdp_server_osh" port={port} schema={schema} subprotocol=hive2 LOGIN_TIMEOUT=300
    uri='jdbc:hive2://{server}:{port}/default;principal=hive/{server}@{host};ssl=true;sslTrustStore={ts};');

proc sql;
    connect to hadoop (&hadoop_cro_ded);

    {_procsql_body(body, "hadoop")}

    disconnect from hadoop;
quit;
"""


def gen_sas_script(kind: str, method: str, *,
                   body: Optional[str] = None,
                   svc: Optional[str] = None,
                   user: Optional[str] = None,
                   password: Optional[str] = None,
                   server: Optional[str] = None,
                   host: Optional[str] = None,
                   schema: Optional[str] = None,
                   port: int = 10000,
                   truststore: Optional[str] = None) -> str:
    """Dispatch on (kind, method) and return SAS source wrapping `body`.

    kind   in {"oracle", "hadoop"}
    method in {"libname", "procsql"}

    Oracle credentials (user, password) are baked into the %let macros at
    the top when supplied — caller is expected to read them from the dtrack
    process environment (e.g. PCDS_USR + <macro>_pwd).
    """
    kind = (kind or "").lower()
    method = (method or "").lower()

    if kind == "oracle":
        if not svc:
            raise ValueError("Oracle script requires `svc` (service name).")
        if method == "libname":
            return oracle_libname_script(svc, body, user=user, password=password)
        if method == "procsql":
            return oracle_procsql_script(svc, body, user=user, password=password)
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
