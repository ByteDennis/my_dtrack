# dtrack: the Left-side CLI (python -m dtrack / python -m dtrack_left / dtrack)
#
# Commands
#   left-run      --config c.json --run-s3 s3://.../runs [--run-id]  # version 2
#   row-check     --config c.json --run-s3 s3://.../runs [--run-id] [--pair P ...] [--workers 4]
#   col-check     --run-s3 s3://.../runs [--run-id] [--pair P] [--work-dir] [--pull-workers 2]
#                 [--stat-workers 1] [--wip 200] [--wait-timeout 1800] [--poll-interval 3]
#   tools         materialize, doctor, rollback, blog, and dbx helpers
#                 (the installed dtrack CLI also adds the local key-check helper)
#   Every command: --log-level debug|info|warning|error (stderr); stdout is one JSON line.
#   .env is loaded (cwd upwards) before options parse; shell variables win over .env.
#
# Config file (JSON): {"connections": {...optional profiles...}, "pairs": {"<pair>": {...}}}
#   connections.<name>   fields merged under a side that says "conn_macro": "<name>" (side fields win)
#
# Left side (pair.left), "source" one of:
#   oracle   table | schema+table | query; conn_macro; where
#            credentials: user/password/dsn fields, *_env names, <macro>_USR/_PWD/_DSN, service
#            (field or DTRACK_ORACLE_MACROS="macro:service,..") resolved via LDAP_DSN, then
#            ORACLE_USER/ORACLE_PASSWORD/ORACLE_DSN, PCDS_USR; names are used as written
#            (unquoted: Oracle folds them to upper case); quote reserved names yourself, "\"DATE\""
#   file     path (glob ok) + format csv|txt|tsv|json|ndjson|jsonl|parquet (default from suffix);
#   (csv)    options {read_csv/read_json args}; query with {src}; date_format (strptime) -> date
#   duckdb   path to a .duckdb file or s3:// / https:// URL; table | schema.table | db.schema.table
#            | query; the database must not be held open read-write by another process
#   athena   database|schema, s3_staging_dir, region (dtrack[athena])
#   hadoop   disabled until identifier quoting and Kerberos/ODBC auth are fixed
#
# Right side (pair.right): "source": "databricks", "kind" one of:
#   table    catalog+schema+table | table "c.s.t" | conn_macro "c.s" + table   (default kind)
#   query    query (any SELECT)
#   files    path + format [+ options, schema_hints, query with {src}]   ("volume_raw" = files)
#   plus where; names are case-insensitive in Databricks, but col_map right names must match
#   the DESCRIBE spelling (column types are looked up by exact name)
#
# Both sides: date_col (required) and date_type
#   date     native DATE/TIMESTAMP values, compared by calendar day
#   string   probes YYYY-MM-DD or YYYYMMDD text
#   number   numeric YYYYMMDD values
#   sas      probes DATE9, SAS datetime text, or numeric days since 1960-01-01
#   Left and Right may differ. Preflight reads at most 16 non-null values for string/sas,
#   rejects empty or mixed formats, then publishes the concrete internal spec for later stages.
#
# Stage 1 settings (changing them needs rollback --stage row, or a new run):
#   left, right, fromDate, toDate ("YYYY-MM-DD", inclusive), date_synonyms {"3000-01-01": "2026-07-01"}
#   gate (Right job / dtrack_local --gate): auto (>= 1 matching partition) | manual | ratio:<0..1>
# Stage 2 settings (rollback --stage col --config c.json rebuilds the manifest, no recount):
#   col_map              {"LEFT": "right"} | "maps/p.csv" (left,right[,type]) | "maps/p.json",
#                        paths relative to the config file; date_col is dropped from it
#   col_type_overrides   {"LEFT": "numeric" | "categorical"}   (CSV type column fills it)
#   mapping type         may instead name a registered ColumnType; `sas_datetime_date` compares
#                        Left SAS datetime text with a Right native date by calendar day
#   vintage              day (default) | week | month | quarter | year   -> one window per bucket
#   key                  "ORDER_ID" or "A,B"; right names via col_map, else the same name
#   key_families         {"KEY": family} hash normalisation, keyed by the Left key name; family is
#                        a name: string (default, trim+lower) | exact | numeric (007, +7.00 -> 7) |
#                        date | digits, or a step list ["trim", "upper", "strip_prefix:ID-"], or
#                        {"family"?, "transform"?, "left_transform"?, "right_transform"?}
#                        date keys may use {"family":"date_type", "left_date_type":...,
#                        "right_date_type":...}; date_col keys inherit the side date types automatically
#                        (sides default to transform; steps and families: common/hashing.py)
#   sample               missing / "" / false = all rows | 0 < x <= 1 fraction by key hash |
#                        N > 0 fixed N keys per window chosen by the Left side (both need key)
#   strict               true: text compared lower-cased and decimal-collapsed
#   skip                 true: the pair is ignored
#
# Large local CSV: `dtrack tools materialize --config c.json --pair P --delete-source`
#   writes verified date-partitioned/hash-sorted Parquet, updates config, then removes the CSV.

from __future__ import annotations

import click
from dotenv import find_dotenv, load_dotenv

from .. import __version__
from .blog import blog_group
from .checks import col_check_command, left_run_command, row_check_command
from .ops import dbx_group
from .options import CONTEXT_SETTINGS, DistributedGroup
from .tools import (
    doctor_command,
    materialize_command,
    rollback_command,
    tools_group,
)


# >>> dtrack: the Left-side CLI; .env loads before any subcommand parses its options (shell variables win) <<< #
@click.group(cls=DistributedGroup, context_settings=CONTEXT_SETTINGS)
@click.version_option(__version__, "-v", "--version", prog_name="dtrack")
def cli():
    load_dotenv(find_dotenv(usecwd=True))


for command in (
    left_run_command,
    row_check_command,
    col_check_command,
    tools_group,
):
    cli.add_command(command)

for command in (
    materialize_command,
    doctor_command,
    rollback_command,
    blog_group,
    dbx_group,
):
    tools_group.add_command(command)


# >>> console entry point: click parsing with the command's return value as the exit code <<< #
def main(argv: list[str] | None = None) -> int:
    try:
        code = cli.main(args=argv, prog_name="dtrack", standalone_mode=False)
    except click.exceptions.Exit as exc:
        return exc.exit_code
    except click.ClickException as exc:
        exc.show()
        return exc.exit_code
    except click.Abort:
        click.echo("Aborted!", err=True)
        return 1
    return code if isinstance(code, int) else 0
