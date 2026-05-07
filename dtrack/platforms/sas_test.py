"""Generate runnable SAS scripts that wrap a user-supplied body with the
right LIBNAME or PROC SQL CONNECT scaffolding.

Used by the Playground SAS engine: the user writes SAS code (libname mode)
or native passthrough SQL (procsql mode) in the editor, picks Oracle or
Hadoop and a connection method, and gets back a complete `.sas` file ready
to submit. Credentials are emitted as `%let oracle_usr / %let oracle_pwd`
macros at the top so secrets never live in the downloaded file.
"""

from typing import Optional


_SAS_HADOOP_JAR_PATH    = "/sas/bin/Software/Hadoop/OSH/Jars"
_SAS_HADOOP_CONFIG_PATH = "/sas/bin/Software/Hadoop/OSH/PROD"
_SAS_HADOOP_TRUSTSTORE  = "/sas/bin/Software/Hadoop/OSH/PROD/truststore.jks"


def _sas_hadoop_options(jar_path: Optional[str] = None,
                       config_path: Optional[str] = None) -> str:
    """Build the `options set=SAS_HADOOP_*_PATH` block for the script.

    Both paths default to the values used in this user's environment;
    profiles override per-machine via the `jar_path` / `config_path` fields.
    """
    j = jar_path    or _SAS_HADOOP_JAR_PATH
    c = config_path or _SAS_HADOOP_CONFIG_PATH
    return (
        f'options set=SAS_HADOOP_JAR_PATH    "{j}";\n'
        f'options set=SAS_HADOOP_CONFIG_PATH "{c}";'
    )

# Default body when the user leaves the editor empty — a sane "does the
# connection work" check that exercises the libref/connect alias.
_DEFAULT_LIBNAME_BODY = "select count(*) from {lib}.YOUR_TABLE"
_DEFAULT_PROCSQL_BODY = "select 1 from dual"


def _looks_like_sql_select(s: str) -> bool:
    """True if `s` looks like a bare SELECT statement (no proc sql / data step)."""
    head = s.lstrip().lower()
    return head.startswith("select") or head.startswith("with ")


def _convert_sql_date_literals_to_sas(sql: str) -> str:
    """Rewrite ANSI/Oracle DATE / TIMESTAMP literals to SAS literals.

    Examples:
        DATE '2026-05-04'              → '04MAY2026'd
        DATE('2026-05-04')             → '04MAY2026'd
        TIMESTAMP '2026-05-04 12:34:56' → '04MAY2026:12:34:56'dt
        TIMESTAMP('2026-05-04 12:34:56') → '04MAY2026:12:34:56'dt

    Plain ISO strings (e.g. '2026-05-04' without the DATE prefix) are left
    alone — rewriting them blind would clobber non-date strings that happen
    to match the pattern.
    """
    import re
    from datetime import datetime as _dt

    def _date_repl(m):
        try:
            d = _dt.strptime(m.group(1), "%Y-%m-%d")
            return f"'{d.strftime('%d%b%Y').upper()}'d"
        except ValueError:
            return m.group(0)

    def _ts_repl(m):
        try:
            d = _dt.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            return f"'{d.strftime('%d%b%Y').upper()}:{d.strftime('%H:%M:%S')}'dt"
        except ValueError:
            return m.group(0)

    s = sql
    # TIMESTAMP first so DATE doesn't snag the date portion of a TIMESTAMP literal.
    s = re.sub(r"TIMESTAMP\s*\(\s*'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})'\s*\)", _ts_repl, s, flags=re.IGNORECASE)
    s = re.sub(r"TIMESTAMP\s+'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})'", _ts_repl, s, flags=re.IGNORECASE)
    s = re.sub(r"DATE\s*\(\s*'(\d{4}-\d{2}-\d{2})'\s*\)", _date_repl, s, flags=re.IGNORECASE)
    s = re.sub(r"DATE\s+'(\d{4}-\d{2}-\d{2})'", _date_repl, s, flags=re.IGNORECASE)
    return s


def _qualify_with_libref(sql: str, libref: str) -> str:
    """Prefix bare table refs after `from` / `join` with `<libref>.<name>`.

    SAS proc sql against a mounted libref needs `<libref>.<table>` to read
    from the remote DB; a bare `from MYTBL` would look in WORK and fail.
    This regex inserts the libref only when the next identifier doesn't
    already contain a dot (i.e. isn't already qualified).

    Limitations:
      - CTE aliases (e.g. `with cte as (...) select * from cte`) get
        wrongly prefixed. If the body starts with `with`, the caller
        skips this rewrite — see `_libname_body`.
      - Subqueries in parentheses are unaffected (regex requires an
        identifier directly after `from`/`join`).
    """
    import re
    pattern = re.compile(
        r"(\b(?:from|join)\s+)([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*\.)",
        re.IGNORECASE,
    )
    return pattern.sub(rf"\1{libref}.\2", sql)


def _looks_like_sas_block(s: str) -> bool:
    """True if `s` looks like SAS code that already wraps itself
    (proc sql / data step / proc print / etc) — pass through unchanged."""
    head = s.lstrip().lower()
    return any(head.startswith(p) for p in ("proc ", "data ", "%macro", "%let"))


def _libname_body(body: Optional[str], lib: str) -> str:
    """Wrap a body for libname mode.

    Bare SELECT  → auto-qualify table refs with `<lib>.` and wrap in
                   `proc sql; ...; quit;` so the libref gets used.
                   Skipped for `with` (CTE) bodies because alias names
                   can't be safely distinguished from real tables.
    proc/data    → pass through unchanged (already a SAS block).
    Empty editor → emit a `select count(*) from <lib>.YOUR_TABLE` probe.
    Anything else → pass through (treat as multi-statement SAS).
    """
    s = (body or "").strip()
    if not s:
        s = _DEFAULT_LIBNAME_BODY.format(lib=lib)
        return _wrap_proc_sql(s)
    if _looks_like_sas_block(s):
        return s
    if _looks_like_sql_select(s):
        head = s.lstrip().lower()
        if not head.startswith("with "):
            s = _qualify_with_libref(s, lib)
        # Rewrite ANSI/Oracle DATE / TIMESTAMP literals to SAS form so
        # `where dt = DATE '2026-05-04'` becomes `where dt = '04MAY2026'd`.
        # SAS proc sql against a libref doesn't understand the ANSI form.
        s = _convert_sql_date_literals_to_sas(s)
        # SAS doesn't grok `limit N` (Hive) or `rownum <= N` (Oracle).
        # Pull them out of the query and put `outobs=N` on the proc sql.
        s, outobs = _extract_row_limit(s)
        return _wrap_proc_sql(s, outobs=outobs)
    return s


def _wrap_proc_sql(sql: str, outobs: Optional[int] = None) -> str:
    s = sql.rstrip().rstrip(";")
    indented = "\n    ".join(s.splitlines())
    obs_attr = f" outobs={outobs}" if outobs else ""
    return f"proc sql{obs_attr};\n    {indented};\nquit;"


def _extract_row_limit(sql: str):
    """Find a `limit N` or `rownum <= N` / `rownum < N` clause and pull it out.

    SAS proc sql doesn't recognize either Hive-style `limit N` or Oracle's
    `rownum`. The standard SAS equivalent is `proc sql outobs=N;`. This
    function returns `(cleaned_sql, N)` after stripping the clause, or
    `(sql, None)` if neither pattern is present.

    Handles:
        ... limit 5           → strip, outobs=5
        ... where rownum <= 5 → strip the WHERE; outobs=5
        ... where rownum < 6  → strip; outobs=5  (strictly-less subtracts 1)
        ... where x=1 and rownum <= 5 → strip just the AND/rownum bit
    """
    import re

    # `LIMIT N` — must be at end of the statement (ignoring trailing ; / ws).
    m = re.search(r"\s+limit\s+(\d+)\s*;?\s*$", sql, re.IGNORECASE)
    if m:
        return sql[:m.start()], int(m.group(1))

    # `ROWNUM <op> N` joined to a predicate by AND, or as the lone WHERE.
    # We capture the full slice to remove (including the connector) so the
    # surrounding query stays well-formed.
    pat_and  = re.compile(r"\s+and\s+rownum\s*(<=|<)\s*(\d+)", re.IGNORECASE)
    pat_only = re.compile(r"\s+where\s+rownum\s*(<=|<)\s*(\d+)", re.IGNORECASE)

    m = pat_and.search(sql)
    if m:
        n = int(m.group(2))
        if m.group(1) == "<":
            n -= 1
        return sql[:m.start()] + sql[m.end():], n

    m = pat_only.search(sql)
    if m:
        n = int(m.group(2))
        if m.group(1) == "<":
            n -= 1
        # Drop the entire `where rownum <op> N` clause.
        return sql[:m.start()] + sql[m.end():], n

    return sql, None


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


def _oracle_cred_block(conn_macro: str,
                       user: Optional[str], password: Optional[str]) -> str:
    """Emit `%let iamusr = ...;` and `%let <macro>_pwd = ...;` matching the
    SAS macros the existing extraction templates already use. When env values
    aren't supplied, falls back to YOUR_USER / YOUR_PASSWORD placeholders.
    """
    u = user or "YOUR_USER"
    p = password or "YOUR_PASSWORD"
    note = "" if (user and password) else "  /* edit and submit */"
    return f"%let iamusr = {u};{note}\n%let {conn_macro}_pwd = {p};{note}"


def oracle_libname_script(svc: str, body: Optional[str] = None, *,
                          conn_macro: str = "pcdslib",
                          user: Optional[str] = None,
                          password: Optional[str] = None) -> str:
    """LIBNAME mount to Oracle + user body + clear.

    The libref is `conn_macro` (e.g. `pb23` → `libname pb23 oracle ...`).
    Connection params follow the format used by extraction.sas:
    `user="&iamusr" orapw="&<macro>_pwd" path="@<svc>"`.
    """
    return f"""\
/* Oracle libname */
{_oracle_cred_block(conn_macro, user, password)}

libname {conn_macro} oracle user="&iamusr" orapw="&{conn_macro}_pwd" path="@{svc}";

{_libname_body(body, conn_macro)}

libname {conn_macro} clear;
"""


def oracle_procsql_script(svc: str, body: Optional[str] = None, *,
                          conn_macro: str = "pb23",
                          user: Optional[str] = None,
                          password: Optional[str] = None) -> str:
    """PROC SQL CONNECT to Oracle + user body + disconnect.

    Matches extraction.sas format: `connect to oracle(user="&iamusr"
    orapw="&<macro>_pwd" path="@<svc>" buffsize=5000 preserve_comments)`.
    No alias — the unaliased default means body wraps with `from connection
    to oracle (...)`.
    """
    return f"""\
/* Oracle PROC SQL CONNECT */
{_oracle_cred_block(conn_macro, user, password)}

proc sql;
    connect to oracle (user="&iamusr" orapw="&{conn_macro}_pwd" path="@{svc}"
        buffsize=5000 preserve_comments);

    {_procsql_body(body, "oracle")}

    disconnect from oracle;
quit;
"""


def hadoop_libname_script(server: str, host: str, schema: str,
                          body: Optional[str] = None,
                          port: int = 10000,
                          truststore: Optional[str] = None,
                          jar_path: Optional[str] = None,
                          config_path: Optional[str] = None) -> str:
    """LIBNAME mount to Hadoop/Hive (Kerberos + SSL) + user body + clear.

    Matches the format used on this user's box — URI path is `/usrf;`,
    libname `hdp` is the libref. Profile values come from
    `HADOOP_LIBNAME_PROFILES` (server, host, schema, port, truststore,
    jar_path, config_path).
    """
    ts = truststore or _SAS_HADOOP_TRUSTSTORE
    return f"""\
/* Hadoop/Hive connection (LIBNAME) */
{_sas_hadoop_options(jar_path, config_path)}

libname hdp hadoop server="{server}"
    port={port} schema={schema} subprotocol=hive2 LOGIN_TIMEOUT=300
    uri='jdbc:hive2://{server}:{port}/usrf;principal=hive/{server}@{host};ssl=true;sslTrustStore={ts};';

{_libname_body(body, "hdp")}

libname hdp clear;
"""


def hadoop_procsql_script(server: str, host: str, schema: str,
                          body: Optional[str] = None,
                          port: int = 10000,
                          truststore: Optional[str] = None,
                          jar_path: Optional[str] = None,
                          config_path: Optional[str] = None) -> str:
    """PROC SQL CONNECT to Hadoop (Kerberos + SSL) + user body + disconnect.

    Matches the format used on this user's box — uses %let macros to
    parameterize the connect string, URI path is `/default;` (different from
    libname's `/usrf`), and the connect call takes a single &-var arg.
    Profile values come from `HADOOP_PROCSQL_PROFILES` and may differ from
    the libname profile (notably the server, jar_path, config_path).
    """
    ts = truststore or _SAS_HADOOP_TRUSTSTORE
    return f"""\
/* Hadoop PROC SQL CONNECT (Kerberos + SSL) */
{_sas_hadoop_options(jar_path, config_path)}

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
                   conn_macro: str = "pb23",
                   user: Optional[str] = None,
                   password: Optional[str] = None,
                   server: Optional[str] = None,
                   host: Optional[str] = None,
                   schema: Optional[str] = None,
                   port: int = 10000,
                   truststore: Optional[str] = None,
                   jar_path: Optional[str] = None,
                   config_path: Optional[str] = None) -> str:
    """Dispatch on (kind, method) and return SAS source wrapping `body`.

    kind   in {"oracle", "hadoop"}
    method in {"libname", "procsql"}

    For Oracle, `conn_macro` is the macro name (pb23, pb30, …) — used both
    as the libref in libname mode AND as the `<macro>_pwd` SAS macro suffix
    in both modes. Credentials (user, password) are baked into %let macros
    at the top of the file when supplied; otherwise YOUR_USER / YOUR_PASSWORD
    placeholders.
    """
    kind = (kind or "").lower()
    method = (method or "").lower()

    if kind == "oracle":
        if not svc:
            raise ValueError("Oracle script requires `svc` (service name).")
        if method == "libname":
            return oracle_libname_script(svc, body, conn_macro=conn_macro,
                                         user=user, password=password)
        if method == "procsql":
            return oracle_procsql_script(svc, body, conn_macro=conn_macro,
                                         user=user, password=password)
    elif kind == "hadoop":
        for name, val in (("server", server), ("host", host), ("schema", schema)):
            if not val:
                raise ValueError(f"Hadoop script requires `{name}`.")
        if method == "libname":
            return hadoop_libname_script(
                server, host, schema, body,
                port=port, truststore=truststore,
                jar_path=jar_path, config_path=config_path,
            )
        if method == "procsql":
            return hadoop_procsql_script(
                server, host, schema, body,
                port=port, truststore=truststore,
                jar_path=jar_path, config_path=config_path,
            )
    raise ValueError(f"Unsupported (kind={kind!r}, method={method!r}).")


# Backward-compat alias so older callers/tests still work.
gen_test_connect = gen_sas_script
