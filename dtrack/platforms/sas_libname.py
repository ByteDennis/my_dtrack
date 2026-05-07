"""SAS LIBNAME-header generation for Oracle and Hadoop.

Emits the LIBNAME statements (and the accompanying SAS_HADOOP_*_PATH options
for Hadoop) that map a remote schema to a SAS libref. Once the libref is
mounted, downstream SAS code can reference remote tables as plain SAS
datasets — `ora.MYTABLE`, `hdp.MYTABLE` — and use SAS-syntax SQL functions
(datepart, intnx, count, nmiss, std, ...) instead of platform-specific SQL.

The emitted scripts use SAS macro variables for credentials so secrets stay
out of the generated file:
  %let oracle_usr = …;
  %let oracle_pwd = …;
"""

from typing import Iterable, Optional


SAS_HADOOP_OPTIONS = (
    'options set=SAS_HADOOP_JAR_PATH    "/sas/bin/Software/Hadoop/OSH/Jars";\n'
    'options set=SAS_HADOOP_CONFIG_PATH "/sas/bin/Software/Hadoop/OSH/PROD";\n'
)
SAS_HADOOP_TRUSTSTORE = "/sas/bin/Software/Hadoop/OSH/PROD/truststore.jks"


def oracle_libname(alias: str, svc: str,
                   usr_macro: str = "&oracle_usr",
                   pwd_macro: str = "&oracle_pwd") -> str:
    """LIBNAME line for an Oracle service. `alias` becomes the libref."""
    return f"libname {alias} oracle user={usr_macro} password={pwd_macro} path={svc};\n"


def hadoop_libname(alias: str, server: str, host: str, schema: str,
                   port: int = 10000,
                   truststore: Optional[str] = None) -> str:
    """LIBNAME line for a Hadoop/Hive schema (Kerberos + SSL)."""
    ts = truststore or SAS_HADOOP_TRUSTSTORE
    return (
        f'libname {alias} hadoop server="{server}"\n'
        f'    port={port} schema={schema} subprotocol=hive2 LOGIN_TIMEOUT=300\n'
        f"    uri='jdbc:hive2://{server}:{port}/usrf;"
        f"principal=hive/{server}@{host};ssl=true;sslTrustStore={ts};';\n"
    )


def credential_macros() -> str:
    """Top-of-file %let block for credentials. User fills these in before run."""
    return (
        "%let oracle_usr = YOUR_USER;\n"
        "%let oracle_pwd = YOUR_PASSWORD;\n"
    )


def libname_prelude(oracle_aliases: Iterable[tuple] = (),
                    hadoop_aliases: Iterable[tuple] = (),
                    include_macros: bool = True) -> str:
    """Build a single SAS preamble that mounts every libref the script needs.

    Args:
        oracle_aliases: iterable of (alias, svc) pairs.
        hadoop_aliases: iterable of dicts with keys
            {alias, server, host, schema, port?, truststore?}.
        include_macros: prepend %let oracle_usr/pwd macros.

    Returns the SAS source as a single string (newline-terminated).
    """
    parts = []
    if include_macros:
        parts.append(credential_macros())

    ora_lst = list(oracle_aliases)
    hdp_lst = list(hadoop_aliases)

    if ora_lst:
        parts.append("/* Oracle libname mounts */")
        for alias, svc in ora_lst:
            parts.append(oracle_libname(alias, svc).rstrip())

    if hdp_lst:
        parts.append("/* Hadoop/Hive libname mounts */")
        parts.append(SAS_HADOOP_OPTIONS.rstrip())
        for spec in hdp_lst:
            parts.append(hadoop_libname(
                spec['alias'], spec['server'], spec['host'], spec['schema'],
                port=int(spec.get('port', 10000)),
                truststore=spec.get('truststore'),
            ).rstrip())

    return "\n".join(parts) + ("\n" if parts else "")


def libname_clear(aliases: Iterable[str]) -> str:
    """Emit `libname X clear;` for each alias — call at end of script."""
    return "\n".join(f"libname {a} clear;" for a in aliases) + "\n"
