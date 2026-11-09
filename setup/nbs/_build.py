from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent


def markdown(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": [text + "\n"]}


def code(text: str) -> dict:
    lines = text.strip().splitlines()
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [line + "\n" for line in lines],
    }


def notebook(description: str, *cells: str) -> dict:
    return {
        "cells": [markdown(description), *(code(cell) for cell in cells)],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


TOOLS = "from setup.nbs.inspect_tools import show, show_methods"


NOTEBOOKS = {
    "00_execution_map.ipynb": notebook(
        "本地只读。沿主入口查看 CLI、Left、Right 和发布阶段的调用边界。",
        f"""{TOOLS}
from pair_recon.app.cli import main, main_program, execute
from pair_recon.app.runner import launch, run_left
from pair_recon_right.engine.job import run

# 先看入口，再沿 launch 进入两侧执行器
show(main, main_program, execute, launch, run_left, run)""",
    ),
    "01_environment_catalog.ipynb": notebook(
        "本地只读。环境变量只负责定位配置与 Databricks 资源，表名由 catalog schema 统一派生。",
        f"""{TOOLS}
from pair_recon.app.config import load_environment, env_value, env_boolean, local_output_root
from pair_recon.core.metadata.constant import CatalogTables, catalog_tables, validate_catalog_schema

show(load_environment, env_value, env_boolean, local_output_root, validate_catalog_schema, catalog_tables)
show_methods(CatalogTables, 'table', 'pair_meta', 'column_map', 'run_status', 'run_error')""",
        """from pair_recon.core.metadata.constant import catalog_tables

# 纯函数示例，不连接 Databricks
catalog_tables('catalog.schema')""",
    ),
    "02_workbook_metadata.ipynb": notebook(
        "本地可运行。查看 pairs.xlsx 的创建、读取、差异判断、配置构建和远程 upsert 入口。",
        f"""{TOOLS}
from pair_recon.core.metadata.workbook import create_pair_workbook, read_pair_workbook
from pair_recon.core.metadata.workbook import pair_configuration_signature, changed_pair_configurations
from pair_recon.core.metadata.workbook import upsert_pair_configuration
from pair_recon.core.metadata.configuration import parse_key_columns, build_pair_config

show(create_pair_workbook, read_pair_workbook, pair_configuration_signature)
show(changed_pair_configurations, upsert_pair_configuration, parse_key_columns, build_pair_config)""",
        """from pair_recon.core.metadata.configuration import parse_key_columns

# key 类型在 workbook 边界解析，运行时使用规范化结构
parse_key_columns(['ORDER_ID[numeric]', 'EFF_DT[date]'])""",
    ),
    "03_cli_prompt_defaults.ipynb": notebook(
        "本地只读。Prompt 负责收集运行选项并保存 YAML，metadata 字段只显示而不再次询问。",
        f"""{TOOLS}
from pair_recon.app.cli import PromptSession, prompts, persistent_settings
from pair_recon.app.cli import synchronize_pair_workbook, synchronize_key_types
from pair_recon.app.cli import select_last_failed_run, add_mismatched_partitions

show_methods(PromptSession, 'begin', 'answer', 'select', 'select_many', 'confirm', 'worker_budget')
show(prompts, persistent_settings, synchronize_pair_workbook, synchronize_key_types)
show(select_last_failed_run, add_mismatched_partitions)""",
    ),
    "04_launch_lifecycle.ipynb": notebook(
        "本地只读。launch 建立 run 状态、启动 Right job、执行 Left、监控失败并发布最终状态。",
        f"""{TOOLS}
from pair_recon.app.runner import launch, run_left, run_outcome, right_job_params
from pair_recon.app.runner import RightJobMonitor, StageWatchdog
from pair_recon.app.runner import record_runtime_failure, make_run_id

show(launch, run_left, run_outcome, right_job_params, record_runtime_failure, make_run_id)
show_methods(RightJobMonitor, 'start', 'request_cancel', 'failure', 'raise_if_failed', 'result')
show_methods(StageWatchdog, 'start', 'check', 'close')""",
    ),
    "05_volume_store.ipynb": notebook(
        "本地只读。Store 封装 Volume 文件协议；真实 API 调用需要 Databricks 凭据，本页不会发起网络请求。",
        f"""{TOOLS}
from pair_recon.core.backends.store import ApiVolumeStore, retry_transient, volume_failure_streak
from pair_recon_left.backends.store import VolumeStore as LeftVolumeStore
from pair_recon_right.engine.backends.store import VolumeStore as RightVolumeStore

show(retry_transient, volume_failure_streak)
show_methods(ApiVolumeStore, 'child', 'put_bytes', 'put_json', 'get_bytes', 'get_json', 'exists', 'list_keys', 'delete_keys', 'clear')
show_methods(LeftVolumeStore, 'put_bytes', 'get_bytes', 'exists')
show_methods(RightVolumeStore, 'put_bytes', 'get_bytes', 'exists')""",
    ),
    "06_protocol_files.ipynb": notebook(
        "本地可运行。Protocol 定义运行目录、信号文件、unit batch、result batch 和状态文档的唯一命名。",
        f"""{TOOLS}
from pair_recon_left.common.model.protocol import plan_path, mapping_path, unit_batch_path, result_batch_path
from pair_recon_left.common.model.protocol import unit_batch_document, result_batch_document
from pair_recon_left.common.model.protocol import list_plans, list_units, list_results, list_failures
from pair_recon_left.common.model.protocol import write_run_status, write_left_state, write_right_state
from pair_recon_left.common.model.protocol import refresh_units_status, col_check_close_document

show(plan_path, mapping_path, unit_batch_path, result_batch_path)
show(unit_batch_document, result_batch_document, list_plans, list_units, list_results, list_failures)
show(write_run_status, write_left_state, write_right_state, refresh_units_status, col_check_close_document)""",
        """from pair_recon_left.common.model.protocol import plan_path, unit_batch_path, result_batch_path

# 路径函数是纯函数，可安全观察命名规则
print(plan_path('ORDERS'))
print(unit_batch_path(1, 64))
print(result_batch_path([{'unit_id': 'ORDERS:2026-01:AMOUNT'}]))""",
    ),
    "07_left_sources.ipynb": notebook(
        "本地只读。SqlSource 统一查询与采样接口，Oracle、Hadoop 和 CSV 仅实现连接与方言差异。",
        f"""{TOOLS}
from pair_recon_left.backends.sources import SqlSource, CsvSource, OracleSource, HadoopSource
from pair_recon_left.backends.sources import source_from_config, solve_ldap, oracle_macro_services

show(source_from_config, solve_ldap, oracle_macro_services)
show_methods(SqlSource, 'relation', 'query', 'query_to_csv', 'measure_query_rate', 'health_check', 'partition_counts', 'pull_window_sql', 'sample_keys')
show_methods(CsvSource, 'reader', 'query_to_csv', 'health_check')
show_methods(OracleSource, 'resolve_dsn', 'connect', 'query_to_csv', 'key_types')
show_methods(HadoopSource, 'connect')""",
    ),
    "08_dates_partitions.ipynb": notebook(
        "本地可运行。DateType 处理物理日期格式，partition 决定 day、month 或 all 的聚合与过滤。",
        f"""{TOOLS}
from pair_recon_left.common.sql.dates import DateType, NativeDateType, StringDateType, NumericDateType, SasDateType
from pair_recon_left.common.sql.dates import resolve_date_type, canonical_date, bucket_date, partition_key
from pair_recon_left.common.sql.dates import aggregate_partition_counts, partition_where, bounds_where

show(resolve_date_type, canonical_date, bucket_date, partition_key, aggregate_partition_counts)
show(partition_where, bounds_where)
show_methods(DateType, 'detect', 'predicate', 'normalize', 'partition_key', 'normalize_sql', 'spec')
show_methods(NativeDateType, 'day', 'normalize_sql')
show_methods(StringDateType, 'detect', 'normalize', 'normalize_sql', 'spec')
show_methods(NumericDateType, 'normalize', 'normalize_sql', 'partition_key', 'spec')
show_methods(SasDateType, 'detect', 'normalize', 'normalize_sql', 'spec')""",
        """from pair_recon_left.common.sql.dates import resolve_date_type

# 同一个接口覆盖 date、compact string、numeric YYYYMM 和 SAS datetime
for spec in ('date', {'type': 'string', 'format': 'compact'}, {'type': 'number', 'format': 'month'}, {'type': 'sas', 'format': 'datetime'}):
    print(spec, '->', resolve_date_type(spec).spec(portable=True))""",
    ),
    "09_hashing_key_families.ipynb": notebook(
        "本地可运行。KeyFamily 先规范化 sampled key，再编码复合键并计算稳定 hash。",
        f"""{TOOLS}
from pair_recon_left.common.model.hashing import canonical_numeric, strip_datetime
from pair_recon_left.common.model.hashing import canonical_normalization_rules, active_normalization_rules
from pair_recon_left.common.model.hashing import KeyFamily, DateKeyFamily, StringFamily, ExactFamily
from pair_recon_left.common.model.hashing import NumericFamily, DateFamily, MonthFamily, DigitsFamily
from pair_recon_left.common.model.hashing import key_family, composite_key, hash32, selected_fraction

show(canonical_numeric, strip_datetime, canonical_normalization_rules, active_normalization_rules)
show_methods(KeyFamily, 'normalize', 'sql', 'normalize_sql', 'spec')
show_methods(DateKeyFamily, 'normalize', 'normalize_sql', 'sql', 'sides', 'spec')
show_methods(StringFamily, 'normalize_value', 'normalize_value_sql')
show_methods(NumericFamily, 'normalize_value', 'normalize_value_sql')
show_methods(MonthFamily, 'normalize_value', 'normalize_value_sql')
show(key_family, composite_key, hash32, selected_fraction)""",
        """from pair_recon_left.common.model.hashing import key_family, composite_key, hash32

# 数字文本和日期时间先规范化，再参与左右一致的 composite hash
family = key_family('numeric')
values = [family.normalize(value) for value in ('0035', '35.000', '0.0050100000')]
print(values)
joined = composite_key(values, [family] * len(values), 'left')
print(joined, hash32(joined))""",
    ),
    "10_sampling_sql.ipynb": notebook(
        "本地可运行。Sampling SQL 在不同方言生成同一复合键、MD5 bucket 和百分比谓词。",
        f"""{TOOLS}
from pair_recon_left.common.sql.sampling import normalize_sql, composite_sql, hash_hex_sql
from pair_recon_left.common.sql.sampling import hash32_sql, sample_predicate_sql, fixed_keys_sql, sample_csv

show(normalize_sql, composite_sql, hash_hex_sql, hash32_sql)
show(sample_predicate_sql, fixed_keys_sql, sample_csv)""",
        """from pair_recon_left.common.sql.sampling import sample_predicate_sql

# 只生成 SQL，不连接源数据库
plan = {'mode': 'fraction', 'rate': 0.01, 'threshold': 42949672}
print(sample_predicate_sql(plan, ['ORDER_ID'], ['numeric'], 'oracle', 'left'))
print(sample_predicate_sql(plan, ['order_id'], ['numeric'], 'spark', 'right'))""",
    ),
    "11_row_check_manifest.ipynb": notebook(
        "本地只读。Left row check 写分区计数；Right 对齐计数；manifest 只为匹配分区选择 vintage 和 columns。",
        f"""{TOOLS}
from pair_recon_left.common.checks.row_check import active_pairs, reuse_row_check, publish_row_check_results, run_row_check
from pair_recon_left.common.model.manifest import sample_plan, select_vintage_windows, statistics_plan
from pair_recon_left.common.model.manifest import build_manifest, validate_manifest, mapping_csv_bytes

show(active_pairs, reuse_row_check, publish_row_check_results, run_row_check)
show(sample_plan, select_vintage_windows, statistics_plan)
show(build_manifest, validate_manifest, mapping_csv_bytes)""",
    ),
    "12_left_column_pipeline.ipynb": notebook(
        "本地只读。Left column pipeline 等待 row gate，按 vintage 拉样本，计算统计分片并发布 unit batches。",
        f"""{TOOLS}
from pair_recon_left.common.checks.col_check import wait_for_row_result, wait_for_row_check_result
from pair_recon_left.common.checks.col_check import publish_fixed_keys, iter_windows_round_robin
from pair_recon_left.common.checks.col_check import ColCheckPipeline, LeftRuntime

show(wait_for_row_result, wait_for_row_check_result, publish_fixed_keys, iter_windows_round_robin)
show_methods(ColCheckPipeline, 'run')
show_methods(LeftRuntime, 'source', 'pull_window', 'compute_stats')""",
    ),
    "13_statistics_duckdb.ipynb": notebook(
        "本地可运行。统计阶段从 CSV 推断类型，在 DuckDB 中计算 checks，并按 shard 返回结果。",
        f"""{TOOLS}
from pair_recon_left.common.runtime.stats import infer_column_type, column_statistics
from pair_recon_left.common.runtime.stats import execute_reporting, stats_batch, compute_csv_statistics
from pair_recon_left.common.runtime.duck import worker_count, settings, connect
from pair_recon_left.common.runtime.progress import StatisticsProgress

show(infer_column_type, column_statistics, execute_reporting, stats_batch, compute_csv_statistics)
show(worker_count, settings, connect)
show_methods(StatisticsProgress, 'start', 'update', 'close')""",
    ),
    "14_stop_audit_logging.ipynb": notebook(
        "本地只读。StopSignal 传播取消，Heartbeat 证明活性，SqlAudit 记录实际 SQL，日志使用配置时区。",
        f"""{TOOLS}
from pair_recon_left.common.runtime.stop import StopSignal, Heartbeat, beat, canceller
from pair_recon_left.common.model.audit import SqlAudit
from pair_recon_left.common.runtime.run_logging import ZonedFormatter, configure_console_logging, configure_left_logging

show(beat, canceller, configure_console_logging, configure_left_logging)
show_methods(StopSignal, 'triggered', 'check', 'trigger', 'guard')
show_methods(Heartbeat, 'tick')
show_methods(SqlAudit, 'record', 'refresh')
show_methods(ZonedFormatter, 'formatTime')""",
    ),
    "15_right_job_row_check.ipynb": notebook(
        "本地只读。Right job 先等待 Left row document，再查询 Right partitions、生成 gate 并拒绝未排除的差异。",
        f"""{TOOLS}
from pair_recon_right.engine.job import smoke, wait_for_left_row_data, require_plan_units, summary, run
from pair_recon_right.engine.checks.row_check import reconcile_partitions, summarize_partitions
from pair_recon_right.engine.checks.row_check import evaluate_gate, reconcile_pair, require_exclusions, run_row_check

show(smoke, wait_for_left_row_data, require_plan_units, summary, run)
show(reconcile_partitions, summarize_partitions, evaluate_gate)
show(reconcile_pair, require_exclusions, run_row_check)""",
    ),
    "16_right_column_scheduler.ipynb": notebook(
        "本地只读。DrainScheduler 管理 ready unit、Right 预取和重试；ColCheck 并行计算并批量提交结果。",
        f"""{TOOLS}
from pair_recon_right.engine.checks.col_check import DrainScheduler, ColCheck
from pair_recon_right.engine.checks.col_check import compare_statistics, result_detail
from pair_recon_right.engine.checks.col_check import require_left_heartbeat, require_same_total

show(compare_statistics, result_detail, require_left_heartbeat, require_same_total)
show_methods(DrainScheduler, 'add_manifest', 'add_unit', 'mark_right_ready', 'mark_prefetch_failed', 'next_task', 'requeue_unit')
show_methods(ColCheck, 'poll', 'run_task', 'complete', 'run')""",
    ),
    "17_right_targets.ipynb": notebook(
        "本地只读。CsvTarget 可本地执行；SparkTarget 的真实查询必须在 Databricks Spark 环境运行。",
        f"""{TOOLS}
from pair_recon_right.engine.backends.targets import CsvTarget, SparkTarget, check_date_column

show(check_date_column)
show_methods(CsvTarget, 'health_check', 'partition_counts', 'count_keys', 'window_statistics')
show_methods(SparkTarget, 'relation', 'date_type', 'health_check', 'partition_counts', 'count_keys')
show_methods(SparkTarget, 'window_count', 'release_window', 'window_statistics', 'window_sample', 'window_rows')""",
    ),
    "18_attribute_checks.ipynb": notebook(
        "本地可运行。AttributeCheck 将 Left 和 Right 统计值转成 match 或 difference；row diff 另生成明细工作簿。",
        f"""{TOOLS}
from pair_recon_left.common.model.attribute_checks import AttributeCheck, DifferenceCheck, FloatCheck, ExtremeCheck
from pair_recon_left.common.model.attribute_checks import attribute_check_names, get_attribute_check
from pair_recon_left.common.checks.row_diff import compare_rows, row_difference_workbook

show(attribute_check_names, get_attribute_check)
show_methods(AttributeCheck, 'compare', 'result_match')
show_methods(DifferenceCheck, 'compare')
show_methods(FloatCheck, 'compare')
show_methods(ExtremeCheck, 'compare')
show(compare_rows, row_difference_workbook)""",
        """from pair_recon_left.common.model.attribute_checks import attribute_check_names, get_attribute_check

# 每个 check 由 registry 获取，Left 和 Right 使用同一比较规则
for name in attribute_check_names():
    print(name, type(get_attribute_check(name)).__name__)""",
    ),
    "19_reports_publish.ipynb": notebook(
        "本地只读。报告收集 protocol 结果并生成 Excel/HTML；catalog publish 把状态、错误和窗口写入 Unity Catalog。",
        f"""{TOOLS}
from pair_recon_right.engine.report.build import collect, build_report
from pair_recon_right.engine.report.excel import write_row_sheet, write_col_sheet, build_workbook
from pair_recon_right.engine.report.html import row_count_section, column_stats_section, summary_section
from pair_recon_right.catalog import run_state, status_run_values, result_summary, error_rows, window_rows, publish

show(collect, build_report, write_row_sheet, write_col_sheet, build_workbook)
show(row_count_section, column_stats_section, summary_section)
show(run_state, status_run_values, result_summary, error_rows, window_rows, publish)""",
    ),
    "20_catalog_progress.ipynb": notebook(
        "本地只读。Catalog 通过 SQL Warehouse 更新 metadata 和 run tables；ProgressSampler 周期读取 protocol 快照。",
        f"""{TOOLS}
from pair_recon.app.catalog import Catalog
from pair_recon.core.progress import ProgressSampler, progress_snapshot

show(progress_snapshot)
show_methods(ProgressSampler, 'capture', 'start', 'stop')
show_methods(Catalog, 'normalization_rules', 'configuration_rows', 'column_mappings', 'pairs')
show_methods(Catalog, 'run', 'latest_run', 'checked_windows', 'progress', 'status', 'fail', 'finish')""",
    ),
    "21_local_debug_samples.ipynb": notebook(
        "本地只读。debug 模式复用已发布的 Right statistics 在本地 reconciliation，并只持久化差异样本。",
        f"""{TOOLS}
from pair_recon.app.runner import StoredRightStatistics, run_local_reconciliation
from pair_recon.app.runner import publish_row_differences, publish_difference_samples, cleanup_left_pull_files

show_methods(StoredRightStatistics, 'window_statistics')
show(run_local_reconciliation, publish_row_differences, publish_difference_samples, cleanup_left_pull_files)""",
    ),
    "22_resume_last_failed.ipynb": notebook(
        "本地只读。resume 复用同一 run checkpoint；last-failed 只重算失败项，再与非失败结果合并生成完整报告。",
        f"""{TOOLS}
from pair_recon.app.runner import retry_failed, clear_resume_outputs, checked_vintages, log_vintage_coverage
from pair_recon.app.cli import select_last_failed_run
from pair_recon_left.common.checks.row_check import reuse_row_check

show(retry_failed, clear_resume_outputs, checked_vintages, log_vintage_coverage)
show(select_last_failed_run, reuse_row_check)""",
    ),
    "23_timing_smoke_batch.ipynb": notebook(
        "本地可运行但会访问 Left source。Timing 受 PAIR_RECON_TIMING 限制；smoke 和 batch 分别做快速校验与顺序执行。",
        f"""{TOOLS}
from pair_recon.app.timing import observation_seconds, left_pull_timing
from pair_recon.app.cli.smoke import smoke
from pair_recon.app.cli.batch import result_state, classify, run_pair, summary_table

show(observation_seconds, left_pull_timing, smoke)
show(result_state, classify, run_pair, summary_table)""",
        """from pair_recon.app.timing import observation_seconds

# 只解析时间，不连接 Left source
print(observation_seconds('true'))
print(observation_seconds('60'))""",
    ),
    "24_sas_sample.ipynb": notebook(
        "本地只读。SAS sample 模式生成原生 SQL 和 request，验证外部 CSV 后回到普通 CSV Left 流程。",
        f"""{TOOLS}
from pair_recon.core.backends.sas_sample import prepare_left_sample
from pair_recon.core.sas import export_bundle, execute

show(prepare_left_sample, export_bundle, execute)""",
    ),
    "25_mapping_discovery.ipynb": notebook(
        "本地可导入。Oracle、Hadoop、CSV discovery 查询真实源；Databricks discovery 和 upsert 需要 SQL Warehouse。",
        f"""{TOOLS}
from pair_recon.core.metadata.mapping import discover_columns, discover_relations
from pair_recon.core.metadata.mapping import discover_oracle_columns, discover_hadoop_columns, discover_csv_columns
from pair_recon.core.metadata.mapping import discover_databricks_columns, discover_databricks_partition
from pair_recon.core.metadata.mapping import automatch_columns, get_date_column_type, upsert_column_mappings

show(discover_columns, discover_relations)
show(discover_oracle_columns, discover_hadoop_columns, discover_csv_columns)
show(discover_databricks_columns, discover_databricks_partition)
show(automatch_columns, get_date_column_type, upsert_column_mappings)""",
    ),
    "26_databricks_sdk_runtime.ipynb": notebook(
        "需要 Databricks。默认只展示源码；设置 RUN_LIVE 为 True 后才使用 databricks.sdk 检查用户和 SQL Warehouse。",
        f"""{TOOLS}
from pair_recon.app.runner import workspace, workspace_auth_type, StatementSql, resolve_job_id
from pair_recon.app.config import load_environment, env_value

show(workspace, workspace_auth_type, resolve_job_id)
show_methods(StatementSql, 'execute')""",
        """from pair_recon.app.config import load_environment, env_value
from pair_recon.app.runner import workspace

RUN_LIVE = False  # 改成 True 才访问 Databricks
if RUN_LIVE:
    load_environment()
    client = workspace(env_value('DBX_PROFILE', default='corp'))
    print(client.current_user.me().user_name)""",
    ),
    "27_databricks_notebook_entry.ipynb": notebook(
        "需要 Databricks。Right notebook 依赖 dbutils、Spark 和 task parameters，本地只显示文件，不执行入口。",
        """from setup.nbs.inspect_tools import show_file

# 直接查看 Databricks notebook 入口，避免本地 import 触发 dbutils 依赖
show_file('src/pair_recon_right/notebook.py')""",
    ),
    "28_config_validation.ipynb": notebook(
        "本地可运行。配置层合并 Excel、YAML 与运行选项，再解析连接宏并执行完整结构验证。",
        f"""{TOOLS}
from pair_recon.app.config import load_defaults, load_config, save_defaults, run_config
from pair_recon.app.runner import pair_config
from pair_recon_left.cli.config import resolve_connections, validate_config

show(load_defaults, load_config, save_defaults, run_config)
show(pair_config, resolve_connections, validate_config)""",
    ),
    "29_query_windows_platforms.ipynb": notebook(
        "本地可运行。Query window 将日期谓词注入自定义 SQL；DateSqlPlatform 为各数据库生成等价日期表达式。",
        f"""{TOOLS}
from pair_recon_left.common.sql.query_windows import normalize_query_windows, render_query_windows
from pair_recon_left.common.sql.date_platforms import DateSqlPlatform, OracleDateSqlPlatform
from pair_recon_left.common.sql.date_platforms import SparkDateSqlPlatform, HadoopDateSqlPlatform, DuckDbDateSqlPlatform
from pair_recon_left.common.sql.date_platforms import date_platform

show(normalize_query_windows, render_query_windows, date_platform)
show_methods(DateSqlPlatform, 'text', 'format_day', 'native_day', 'parse_text', 'generic_day', 'epoch_value', 'epoch_day')
show_methods(OracleDateSqlPlatform, 'native_day', 'parse_text', 'epoch_day')
show_methods(SparkDateSqlPlatform, 'native_day', 'parse_text', 'epoch_day')
show_methods(HadoopDateSqlPlatform, 'generic_day')
show_methods(DuckDbDateSqlPlatform, 'native_day', 'parse_text', 'epoch_day')""",
    ),
    "30_mail_volume_logging.ipynb": notebook(
        "本地只读。Outlook 通知在 run 结束后发送；Databricks Right 日志通过 VolumeLogHandler 写入运行目录。",
        f"""{TOOLS}
from pair_recon.mail import send_outlook
from pair_recon_right.engine.backends.volume_log import VolumeLogHandler

show(send_outlook)
show_methods(VolumeLogHandler, 'emit', 'flush', 'close')""",
    ),
    "31_dbx_deploy_jobs.ipynb": notebook(
        "需要 Databricks。默认只展示 SDK 调用；deploy 负责上传 Right package、创建或重定向 job。",
        f"""{TOOLS}
from pair_recon_dbx.deploy import remote_package, right_source, right_target, right_notebook
from pair_recon_dbx.deploy import upload, repoint_job, job_spec, create_job, sync_right
from pair_recon_dbx.runtime import client, login, resolve_job_id, list_runs, run_detail

show(remote_package, right_source, right_target, right_notebook)
show(upload, repoint_job, job_spec, create_job, sync_right)
show(client, login, resolve_job_id, list_runs, run_detail)""",
    ),
    "32_dbx_workspace_cache.ipynb": notebook(
        "需要 Databricks。默认只展示源码；workspace 命令管理远程文件，cache 命令通过 SQL Warehouse 查询或删除缓存索引。",
        f"""{TOOLS}
from pair_recon_dbx.workspace import upload, download, ls, copy, move, rm, resolve_workspace_path
from pair_recon_dbx.cache import show as show_cache, explain, peek, format_explain, format_peek
from pair_recon_dbx.runtime import run_sql, format_sql_result, warehouse_start

show(upload, download, ls, copy, move, rm, resolve_workspace_path)
show(show_cache, explain, peek, format_explain, format_peek)
show(run_sql, format_sql_result, warehouse_start)""",
    ),
}


# Keep the examples in the generator so rebuilding the notebooks preserves them.
NOTEBOOKS["01_environment_catalog.ipynb"]["cells"] = [
    markdown("Explore environment settings and catalog table names with isolated, local examples. The examples do not contact Databricks."),
    code(f"""{TOOLS}
from pair_recon.app.config import load_environment, env_value, env_boolean, local_output_root
from pair_recon.core.metadata.constant import CatalogTables, catalog_tables, validate_catalog_schema

show(load_environment, env_value, env_boolean, local_output_root, validate_catalog_schema, catalog_tables)
show_methods(CatalogTables, 'table', 'pair_meta', 'column_map', 'run_status', 'run_error')"""),
    markdown("## `load_environment`\n\nLoad a temporary `.pair_recon.env`. Existing process variables take precedence; the environment is restored after the example."),
    code('''import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

with TemporaryDirectory() as directory:
    root = Path(directory)
    (root / ".pair_recon.env").write_text("PAIR_RECON_TOY_VALUE=from_file\\n")
    with patch("pair_recon.app.config.find_dotenv", return_value=""), patch.dict(os.environ, {}, clear=True):
        print("loaded file:", load_environment(root).name)
        print("value:", os.environ["PAIR_RECON_TOY_VALUE"])
# loaded file: .pair_recon.env
# value: from_file'''),
    markdown("## `env_value` and `env_boolean`\n\nThe first non-empty variable wins. Boolean values accept common true/false spellings."),
    code('''with patch.dict(os.environ, {"TOY_PRIMARY": "  ", "TOY_FALLBACK": " hello ", "TOY_ENABLED": "yes"}, clear=True):
    print(env_value("TOY_PRIMARY", "TOY_FALLBACK", default="missing"))
    print(env_value("TOY_UNKNOWN", default="missing"))
    print(env_boolean("TOY_ENABLED"))
    print(env_boolean("TOY_UNKNOWN", default=False))
# hello
# missing
# True
# False'''),
    markdown("## `local_output_root`\n\nThe environment variable takes precedence over the default work directory. The selected directory is created."),
    code('''with TemporaryDirectory() as directory:
    target = Path(directory) / "toy-output"
    with patch.dict(os.environ, {"PAIR_RECON_OUTPUT_DIR": str(target)}, clear=True):
        result = local_output_root({"work_dir": "unused"})
        print("selected:", result.name)
        print("created:", result.is_dir())
# selected: toy-output
# created: True'''),
    markdown("## `validate_catalog_schema` and `catalog_tables`\n\nA valid namespace contains exactly a catalog and schema. Invalid inputs raise `ValueError`."),
    code('''print(validate_catalog_schema("  demo.recon  "))
tables = catalog_tables("demo.recon")
print(type(tables).__name__, tables.catalog_schema)
try:
    validate_catalog_schema("demo")
except ValueError as error:
    print(type(error).__name__, str(error))
# demo.recon
# CatalogTables demo.recon
# ValueError catalog schema must be catalog.schema'''),
    markdown("## `CatalogTables.table` and table properties\n\nThe method accepts known PairRecon table names. The properties return fully qualified names."),
    code('''print("table:", tables.table("pair_recon_pair_meta"))
print("pair_meta:", tables.pair_meta)
print("column_map:", tables.column_map)
print("run_status:", tables.run_status)
print("run_error:", tables.run_error)
try:
    tables.table("unknown")
except ValueError as error:
    print(type(error).__name__, str(error))
# table: demo.recon.pair_recon_pair_meta
# pair_meta: demo.recon.pair_recon_pair_meta
# column_map: demo.recon.pair_recon_column_map
# run_status: demo.recon.pair_recon_run_status
# run_error: demo.recon.pair_recon_run_error
# ValueError unknown PairRecon table: unknown'''),
]

NOTEBOOKS["02_workbook_metadata.ipynb"]["cells"][0] = markdown(
    "Explore workbook creation, loading, configuration comparison, and metadata upserts using temporary files and a mock SQL client."
)
NOTEBOOKS["02_workbook_metadata.ipynb"]["cells"].extend([
    markdown("## Create and read a workbook\n\nThe workbook is created in a temporary directory. Reading the empty template returns an empty list."),
    code('''from pathlib import Path
from tempfile import TemporaryDirectory

with TemporaryDirectory() as directory:
    path = Path(directory) / "pairs.xlsx"
    created = create_pair_workbook(path)
    print("created:", created.name, created.exists())
    print("rows:", read_pair_workbook(path))'''),
    markdown("## Parse key columns\n\nA list of typed key specifications becomes a list of names and a type mapping."),
    code('''keys = ["ORDER_ID[numeric]", "EFF_DT[date]"]
print("input:", keys)
print("output:", parse_key_columns(keys))'''),
    markdown("## Build and compare a pair configuration\n\nThis small legacy-style row supplies `config_json`. The comparison normalizes the row into a signature before detecting changes."),
    code('''from pprint import pprint

row = {"pair_name": "ORDERS", "enabled": True, "key_columns": ["ORDER_ID"], "config_json": {"left": {}, "right": {}}}
print("input row:")
pprint(row)
print("built config:")
pprint(build_pair_config(row))
print("signature:")
pprint(pair_configuration_signature(row))
print("new pair:", [item["pair_name"] for item in changed_pair_configurations([row], [])])
print("unchanged:", changed_pair_configurations([row], [row]))'''),
    markdown("## Upsert with a mock SQL client\n\nThe client records the SQL and parameters; no database is contacted. The input row is the same one used above."),
    code('''from unittest.mock import Mock

sql = Mock()
result = upsert_pair_configuration(sql, "demo.recon.pair_recon_pair_meta", [row])
print("result:", result)
print("SQL calls:", sql.execute.call_count)
for call in sql.execute.call_args_list:
    statement, parameters = call.args
    print("target:", statement.splitlines()[0])
    print("pair_name:", parameters["pair_name"])'''),
])

NOTEBOOKS["03_cli_prompt_defaults.ipynb"]["cells"][0] = markdown(
    "Explore CLI prompts, workbook synchronization and run-default precedence. Examples use local values; no Databricks connection is made."
)
NOTEBOOKS["03_cli_prompt_defaults.ipynb"]["cells"].extend([
    markdown("## Resolve saved and catalog defaults\n\nSaved values normally take precedence. When catalog metadata is authoritative, catalog-owned values and run defaults take precedence, including an absent resettable `use_modular`."),
    code('''from pprint import pprint
from pair_recon.app.config import resolve_run_defaults

metadata = {"partition": "month", "_run_defaults": {"sample": 0.1}}
saved = {"partition": "day", "sample": 0.5, "use_modular": True}
print("metadata:"); pprint(metadata)
print("saved:"); pprint(saved)
print("workbook precedence:"); pprint(resolve_run_defaults(metadata, saved))
print("catalog precedence:"); pprint(resolve_run_defaults(metadata, saved, catalog_authoritative=True))'''),
    markdown("## Workbook synchronization skips disabled pair mappings\n\nA temporary file and mocks show which rows are synchronized; the example does not connect to a database."),
    code('''from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

rows = [{"pair_name": "ON", "enabled": True}, {"pair_name": "OFF", "enabled": False}]
catalog = Mock()
catalog.configuration_rows.return_value = []
with TemporaryDirectory() as directory:
    path = Path(directory) / "pairs.xlsx"
    path.touch()
    with patch("pair_recon.app.cli.read_pair_workbook", return_value=rows), \\
         patch("pair_recon.app.cli.changed_pair_configurations", return_value=[]), \\
         patch("pair_recon.app.cli.synchronize_key_types", return_value=1) as sync:
        loaded, result = synchronize_pair_workbook(Mock(), catalog, "demo.recon.pair_recon_pair_meta", path)
        print("input pairs:", [row["pair_name"] for row in loaded])
        print("mapping rows:", result["mapping_rows"])
        print("synchronized pairs:", [call.args[2]["pair_name"] for call in sync.call_args_list])'''),
])

NOTEBOOKS["03_cli_prompt_defaults.ipynb"]["cells"].extend([
    markdown("## `PromptSession` input methods\n\nMock the terminal helpers so the example is deterministic. The recorded answers demonstrate `begin`, `answer`, `select`, `select_many`, `confirm`, and `worker_budget`."),
    code('''from unittest.mock import patch

session = PromptSession()
session.begin()
with patch("pair_recon.app.cli.ask", return_value="alex") as ask_mock, \\
     patch("pair_recon.app.cli.choose", return_value="orders") as choose_mock, \\
     patch("pair_recon.app.cli.choose_many", return_value=["min", "max"]) as many_mock, \\
     patch("pair_recon.app.cli.yes_no", return_value=True) as confirm_mock, \\
     patch("pair_recon.app.cli.positive_int", return_value=4) as workers_mock:
    print("answer:", session.answer("Owner", "guest"), "input:", ask_mock.call_args)
    print("select:", session.select("Pair", ["orders", "items"], "items"), "input:", choose_mock.call_args)
    print("select_many:", session.select_many("Checks", ["min", "max"], ["min"]), "input:", many_mock.call_args)
    print("confirm:", session.confirm("Continue", False), "input:", confirm_mock.call_args)
    print("worker_budget:", session.worker_budget("Workers", 2), "input:", workers_mock.call_args)
print("stored answers:", session.answers)
print("cursor before begin:", session.cursor)
session.begin()
print("cursor after begin:", session.cursor)'''),
    markdown("## Persist settings and select the latest failed run\n\nTransient command flags are removed from saved settings. A mock catalog supplies the previous run."),
    code('''from unittest.mock import Mock
settings = {"pair": "orders", "owner": "alex", "run_id": "old", "force": True, "_internal": 1}
print("input settings:", settings)
print("persistent settings:", persistent_settings(settings))
catalog = Mock()
catalog.latest_run.return_value = {"run_id": "orders-alex-20261109", "state": "failed"}
print("latest run:", catalog.latest_run.return_value)
print("action:", select_last_failed_run(settings, catalog))
print("selected run:", settings["run_id"])
print("catalog input:", catalog.latest_run.call_args)'''),
    markdown("## Synchronize key types without a database\n\nThe mock catalog provides existing mappings; the mocked upsert exposes the exact rows and overwrite mode that would be sent."),
    code('''from unittest.mock import Mock, patch
catalog = Mock()
catalog.column_map_table = "demo.recon.pair_recon_column_map"
catalog.column_mappings.return_value = []
row = {"pair_name": "orders", "key_columns": ["ORDER_ID[numeric]"], "left_date_col": "ORDER_DT", "right_date_col": "order_date"}
with patch("pair_recon.app.cli.upsert_column_mappings") as upsert:
    count = synchronize_key_types(Mock(), catalog, row)
    print("input:", row)
    print("changed mappings:", count)
    print("upsert target:", upsert.call_args.args[1])
    print("upsert pair:", upsert.call_args.args[2])
    print("upsert mappings:", upsert.call_args.args[3])
    print("upsert options:", upsert.call_args.kwargs)'''),
    markdown("## Add mismatched partitions\n\nMock the error parser to make the partition input explicit; the real function updates both settings and pair configuration."),
    code('''settings = {"pair": "orders", "excluded_partitions": ["2026-01-01"]}
config = {"pairs": {"orders": {}}}
with patch("pair_recon.app.cli.mismatched_partitions", return_value=["2026-01-02"]):
    print("input settings:", settings)
    print("added:", add_mismatched_partitions(settings, config, RuntimeError("toy row mismatch")))
    print("updated settings:", settings)
    print("updated configuration:", config)'''),
])

NOTEBOOKS["25_mapping_discovery.ipynb"]["cells"][0] = markdown(
    "Explore column discovery and mapping updates. The example uses a mock SQL client and does not contact a database."
)
NOTEBOOKS["25_mapping_discovery.ipynb"]["cells"].extend([
    markdown("## Replace or patch mappings\n\n`overwrite=True` replaces the entire mapping table, including rows for other pairs. `overwrite=False` patches the selected pair using MERGE. This example only records SQL calls."),
    code('''from unittest.mock import Mock
import json

mapping = [{"left_column": "ORDER_ID", "right_column": "order_id", "data_type": "numeric"}]
for overwrite in (True, False):
    sql = Mock()
    result = upsert_column_mappings(sql, "demo.recon.pair_recon_column_map", "ORDERS", mapping, overwrite=overwrite)
    statement, parameters = sql.execute.call_args.args
    print("input:", mapping, "overwrite:", overwrite)
    print("result:", result)
    print("SQL:", statement.splitlines()[0])
    print("SQL input:", json.loads(parameters["mappings_json"]))

sql = Mock()
result = upsert_column_mappings(sql, "demo.recon.pair_recon_column_map", "ORDERS", [], overwrite=False)
print("empty patch:", result["rows"], "SQL calls:", sql.execute.call_count)'''),
])

NOTEBOOKS["04_launch_lifecycle.ipynb"]["cells"][0] = markdown(
    "Explore the local launcher, the Right job monitor, and cooperative stop handling. The example simulates a Right failure locally."
)
NOTEBOOKS["04_launch_lifecycle.ipynb"]["cells"].extend([
    markdown("## A Right failure triggers the local stop signal\n\nA mock remote job fails immediately. The monitor runs in a local thread; no Databricks job is submitted."),
    code('''from unittest.mock import Mock
from pair_recon_left.common.runtime.stop import StopSignal, StopRequested

remote = Mock()
remote.run_id = 123
remote.result.side_effect = RuntimeError("toy Right failure")
stop = StopSignal()
monitor = RightJobMonitor(remote, stop=stop).start()
monitor.done.wait(timeout=5)
print("run id:", monitor.run_id)
print("monitor state:", monitor.final_state)
print("stop triggered:", stop.triggered)
try:
    stop.check()
except StopRequested as error:
    print("Left sees:", type(error).__name__, str(error))'''),
])

NOTEBOOKS["08_dates_partitions.ipynb"]["cells"].extend([
    markdown("## Date and partition helpers\n\nThese examples show concrete date values and generated SQL; no database is contacted."),
    code('''print("canonical_date:", "2026-01-02", "->", canonical_date("2026-01-02"))
print("partition_key:", "2026-01-02", "month ->", partition_key("2026-01-02", "month"))
print("partition_where:", partition_where("ORDER_DT", ["2026-01-02"], dialect="oracle", date_type="date", grain="day"))
print("bounds_where:", bounds_where("ORDER_DT", dialect="oracle", date_type="date", from_date="2026-01-01", to_date="2026-01-31"))'''),
    markdown("## Date type methods\n\nEach object receives a representative value and emits its portable specification, normalized value, partition key and SQL. Detection resolves `auto` formats from concrete values."),
    code('''examples = [
    (NativeDateType(), "2026-01-02"),
    (StringDateType(format="compact"), "20260102"),
    (NumericDateType(format="day"), 20260102),
    (SasDateType(format="days"), 24108),
]
for date_type, value in examples:
    print("\\nclass:", type(date_type).__name__, "input:", value)
    print("spec:", date_type.spec(portable=True))
    print("normalize:", date_type.normalize(value))
    try:
        print("partition_key:", date_type.partition_key(value))
    except ValueError as error:
        print("partition_key error:", str(error))
    print("predicate:", date_type.predicate("ORDER_DT", "oracle"))
    print("normalize_sql:", date_type.normalize_sql("ORDER_DT", "oracle"))
    print("day:", date_type.day("ORDER_DT", "oracle"))
for date_type, values in [(StringDateType(), ["20260102"]), (SasDateType(), [24108])]:
    print("detect:", type(date_type).__name__, values, "->", date_type.detect(values).spec(portable=True))'''),
])

DESCRIPTIONS = {
    "00": "Follow the CLI, Left and Right execution entry points.",
    "05": "Inspect Volume storage and its retry behavior without a remote connection.",
    "06": "Explore protocol paths and batch documents shared by Left and Right.",
    "07": "Inspect Left data sources and their query interfaces.",
    "08": "Explore date types, partition keys and date predicates.",
    "09": "Explore key normalization and stable composite hashes.",
    "10": "Generate sampling SQL for different database dialects.",
    "11": "Inspect row checks, sampling plans and manifests.",
    "12": "Inspect the Left column pipeline and row-result gate.",
    "13": "Explore local statistics and DuckDB execution.",
    "14": "Explore cooperative stop signals, heartbeats and SQL auditing.",
    "15": "Explore Right-side partition reconciliation and row gates.",
    "16": "Inspect Right-side column scheduling and statistics comparison.",
    "17": "Inspect CSV and Spark target interfaces.",
    "18": "Explore attribute checks and row-difference reports.",
    "19": "Inspect report generation and catalog publishing.",
    "20": "Inspect catalog operations and progress snapshots.",
    "21": "Inspect local reconciliation and difference samples.",
    "22": "Explore checkpoint reuse and failed-run selection.",
    "23": "Explore timing, smoke checks and batch execution.",
    "24": "Inspect SAS sample preparation and bundle export.",
    "26": "Inspect Databricks SDK integration; the live example is disabled by default.",
    "27": "Read the Databricks notebook entry point without executing it locally.",
    "28": "Explore configuration loading and validation.",
    "29": "Inspect query windows and date SQL platforms.",
    "30": "Inspect Outlook notifications and Volume logging.",
    "31": "Inspect Databricks deployment and job operations.",
    "32": "Inspect Databricks workspace and cache operations.",
}

TOY_EXAMPLES = {
    "00": '''from datetime import datetime, timezone
from pair_recon.app.runner import make_run_id
print("input:", "ORDERS", "alex", "UTC", "2026-01-02")
print("run id:", make_run_id("ORDERS", "alex", "UTC", now=datetime(2026, 1, 2, tzinfo=timezone.utc)))''',
    "05": '''calls = []
def operation():
    calls.append("attempt")
    if len(calls) == 1:
        raise ConnectionError("temporary")
    return "ok"
from unittest.mock import patch
with patch("pair_recon.core.backends.store.time.sleep"):
    print("result:", retry_transient("toy", operation))
print("attempts:", len(calls))''',
    "06": '''from pprint import pprint
units = [{"unit_id": "ORDERS:2026-01:AMOUNT", "pair": "ORDERS", "window": "2026-01"}]
print("input units:", units)
pprint(unit_batch_document(1, units, left_ready_at="2026-01-01T00:00:00Z"))
pprint(result_batch_document([{"unit_id": units[0]["unit_id"], "verdict": "match"}], committed_at="2026-01-01T00:01:00Z"))''',
    "07": '''config = {"source": "csv", "path": "orders.csv"}
source = source_from_config(config)
print("input:", config)
print("source type:", type(source).__name__)
print("source config:", source.config)''',
    "08": '''counts = {"2026-01-01": 2, "2026-01-02": 3, "2026-02-01": 4}
print("input:", counts)
print("monthly:", aggregate_partition_counts(counts, "month"))
print("bucket:", bucket_date("2026-01-02", "month"))''',
    "09": '''for value in ("0035", "35.000", "0.0050100000"):
    print(repr(value), "->", canonical_numeric(value))
print("date text:", strip_datetime("2026-01-01 12:30:00"))''',
    "11": '''from pprint import pprint
for raw in (0.1, 100):
    try:
        print("sample input:", raw); pprint(sample_plan(raw))
    except (TypeError, ValueError) as error:
        print(type(error).__name__, str(error))
print("statistics input:", ["min", "max"])
print("statistics output:", statistics_plan(["min", "max"]))''',
    "12": '''manifests = [{"pair": "ORDERS", "windows": [{"bucket": "2026-01"}, {"bucket": "2026-02"}]}, {"pair": "ITEMS", "windows": [{"bucket": "2026-01"}]}]
print("input:", manifests)
print("round robin:", [(m["pair"], w["bucket"]) for m, w in iter_windows_round_robin(manifests)])''',
    "13": '''from pprint import pprint
import pandas as pd
values = pd.Series(["10", "20", None])
print("input:", values.tolist())
print("inferred:", infer_column_type(2, 2, 2))
pprint(column_statistics(values))''',
    "14": '''from unittest.mock import Mock
from pair_recon_left.common.runtime.stop import StopRequested
handle = Mock()
stop = StopSignal()
try:
    with stop.guard(handle):
        print("initial:", stop.triggered)
        stop.trigger("toy failure")
        print("cancel calls:", handle.cancel.call_count)
        stop.check()
except StopRequested as error:
    print(type(error).__name__, str(error))''',
    "15": '''from pprint import pprint
left = {"2026-01-01": 2, "2026-01-02": 1}
right = {"2026-01-01": 2, "2026-01-02": 3}
print("left:", left, "right:", right)
partitions = reconcile_partitions(left, right)
pprint(partitions)
print("summary:"); pprint(summarize_partitions(partitions))
print("gate:"); pprint(evaluate_gate(partitions))''',
    "16": '''from pprint import pprint
left = {"n_total": 2}
right = {"n_total": 3}
print("input:", left, right)
pprint(compare_statistics(pair="ORDERS", window="2026-01", column="AMOUNT", left=left, right=right, statistics=["n_total"]))''',
    "17": '''from pair_recon_left.common.sql.dates import resolve_date_type
date_type = resolve_date_type("date")
print("input:", "ORDER_DT", "date", date_type.spec(), "left")
print("result:", check_date_column("ORDER_DT", "date", date_type, "left"))''',
    "18": '''for name in attribute_check_names():
    check = get_attribute_check(name)
    print("input:", name, "output:", type(check).__name__)''',
    "19": '''for outcome in ({"state": "matched"}, {"state": "different"}, {"state": "failed"}):
    print("input:", outcome, "output:", run_state(outcome))''',
    "20": '''from unittest.mock import Mock
store = Mock()
store.exists.return_value = False
store.list_keys.return_value = []
snapshot = progress_snapshot(store, "orders-alex-20261109", "orders", "UTC")
print("input:", {"run_id": "orders-alex-20261109", "pair": "orders", "timezone": "UTC"})
print("snapshot:", {key: snapshot[key] for key in ("state", "units_expected", "units_ready", "units_complete", "units_failed")})''',
    "21": '''from pathlib import Path
from tempfile import TemporaryDirectory
with TemporaryDirectory() as directory:
    print("input:", {"run_dir": "temporary directory", "pair": "ORDERS"})
    print("cleanup result:", cleanup_left_pull_files(Path(directory), "ORDERS"))''',
    "22": '''from unittest.mock import Mock
catalog = Mock()
catalog.latest_run.return_value = {"run_id": "orders-alex-20261109", "state": "failed"}
settings = {"pair": "orders", "owner": "alex"}
print("input:", settings, "latest:", catalog.latest_run.return_value)
print("action:", select_last_failed_run(settings, catalog))
print("updated settings:", settings)
print("catalog call:", catalog.latest_run.call_args)''',
    "23": '''for returncode, state in [(0, "matched"), (0, "different"), (1, "failed")]:
    print("input:", returncode, state, "output:", classify(returncode, state))''',
    "24": '''pair = {"left": {"source": "csv"}}
print("pair:", pair)
print("result:", prepare_left_sample("ORDERS", pair, {}))''',
    "25": '''left = ["ORDER_ID", "AMOUNT", "LEFT_ONLY"]
right = ["order_id", "AMOUNT", "RIGHT_ONLY"]
print("left:", left, "right:", right)
print("matched:", automatch_columns(left, right))''',
    "26": '''from unittest.mock import Mock
client = Mock()
print("input job reference:", "123")
print("job id:", resolve_job_id(client, "123"))
print("SDK list calls:", client.jobs.list.call_count)''',
    "28": '''from pathlib import Path
from tempfile import TemporaryDirectory
with TemporaryDirectory() as directory:
    path = Path(directory) / "defaults.yaml"
    data = {"owner": "alex", "password": "secret"}
    print("input keys:", sorted(data))
    print("saved:", save_defaults(path, data))
    print("loaded:", load_defaults(path))''',
    "29": '''for dialect in ("oracle", "spark", "hadoop", "duckdb"):
    platform = date_platform(dialect)
    print("input:", dialect, "platform:", type(platform).__name__)''',
    "30": '''from unittest.mock import Mock
client = Mock()
message = client.Dispatch.return_value.CreateItem.return_value
result = send_outlook("alex@example.com; sam@example.com", "Toy report", "Hello", client=client)
print("recipients:", message.To)
print("subject:", message.Subject, "body:", message.Body)
print("output:", result, "send calls:", message.Send.call_count)''',
    "31": '''spec = job_spec("toy-job", "/Shared/pair_recon/notebook", "cluster-123")
print("job spec:", spec)''',
    "32": '''result = {"pair": "ORDERS", "artifacts": [], "total_artifacts": 0, "total_size_bytes": 0}
print("input:", result)
print("formatted output:\\n" + format_explain(result))''',
}

for name, document in NOTEBOOKS.items():
    prefix = name[:2]
    if prefix in DESCRIPTIONS:
        document["cells"][0] = markdown(DESCRIPTIONS[prefix])
    if prefix in TOY_EXAMPLES:
        document["cells"].extend([
            markdown("## Toy example\n\nRun this cell locally to inspect the concrete inputs and outputs."),
            code(TOY_EXAMPLES[prefix]),
        ])

NOTEBOOKS["06_protocol_files.ipynb"]["cells"].extend([
    markdown("## Protocol reads and writes with an in-memory store\n\nThe fake store prints the keys and documents that would be persisted. It supports the same methods used by these helpers and performs no network calls."),
    code('''from pprint import pprint

class ToyStore:
    def __init__(self):
        self.documents = {}
        self.files = {}
    def exists(self, key):
        return key in self.documents
    def get_json(self, key):
        return self.documents[key]
    def put_json(self, key, value):
        self.documents[key] = value
        return key
    def list_keys(self, prefix):
        return sorted(key for key in self.documents if key.startswith(prefix))
    def put_bytes(self, key, body, *, content_type=None):
        self.files[key] = body
        return key

store = ToyStore()
print("plan path:", plan_path("ORDERS"))
print("mapping path:", mapping_path("ORDERS"))
print("initial plans:", list_plans(store))
print("initial units:", list_units(store))
print("initial results:", list_results(store))
print("initial failures:", list_failures(store))
print("run status key:", write_run_status(store, {"state": "running"}, timezone_name="UTC"))
print("left state key:", write_left_state(store, {"col_check_closed_at": "2026-01-01T00:00:00Z", "pairs": ["ORDERS"], "expected_units": 0}, timezone_name="UTC"))
print("right state key:", write_right_state(store, {"phase": "waiting"}, timezone_name="UTC"))
print("close document:"); pprint(col_check_close_document(store))
status_key = refresh_units_status(store)
print("units status key:", status_key)
print("units status CSV:", store.files[status_key].decode().strip())
print("stored JSON documents:"); pprint(store.documents)'''),
])

NOTEBOOKS["05_volume_store.ipynb"]["cells"].extend([
    markdown("## `ApiVolumeStore` and Left store with a fake Files API\n\nThe fake records byte payloads in memory. Each input and returned value is printed; no Databricks API is called."),
    code('''import io
from types import SimpleNamespace

class FakeFiles:
    def __init__(self):
        self.data = {}
    def create_directory(self, path):
        pass
    def upload(self, path, content, overwrite):
        self.data[path] = content.read()
    def download(self, path):
        if path not in self.data:
            raise FileNotFoundError(path)
        return SimpleNamespace(contents=io.BytesIO(self.data[path]))
    def get_metadata(self, path):
        if path not in self.data:
            raise FileNotFoundError(path)
        return SimpleNamespace(path=path)
    def list_directory_contents(self, path):
        return [SimpleNamespace(path=key, is_directory=False) for key in self.data if key.startswith(path.rstrip("/") + "/")]
    def delete(self, path):
        del self.data[path]

files = FakeFiles()
store = ApiVolumeStore("/Volumes/demo/recon/runs", files=files)
child = store.child("toy-run")
print("child root:", child.root)
print("put bytes:", child.put_bytes("sample.txt", b"hello"))
print("get bytes:", child.get_bytes("sample.txt"))
print("put json:", child.put_json("state.json", {"state": "ready"}))
print("get json:", child.get_json("state.json"))
print("exists:", child.exists("state.json"), child.exists("missing.json"))
print("list keys:", child.list_keys(""))
print("delete keys:", child.delete_keys(["sample.txt"]))
print("clear remaining:", child.clear())
print("remaining fake files:", files.data)
left = LeftVolumeStore("/Volumes/demo/recon/left", files=files)
print("Left put:", left.put_bytes("toy.txt", b"left"))
print("Left get:", left.get_bytes("toy.txt"), "exists:", left.exists("toy.txt"))
print("failure streak after successful calls:", volume_failure_streak())'''),
    markdown("## Right `VolumeStore` with a temporary directory\n\nRight accesses files through a mounted directory. A temporary local directory makes the filesystem behavior visible without using a real Volume."),
    code('''from tempfile import TemporaryDirectory
with TemporaryDirectory() as directory:
    right = RightVolumeStore(directory)
    print("input bytes:", b"right")
    print("put:", right.put_bytes("toy.txt", b"right"))
    print("get:", right.get_bytes("toy.txt"))
    print("exists:", right.exists("toy.txt"), right.exists("missing.txt"))'''),
])

NOTEBOOKS["04_launch_lifecycle.ipynb"]["cells"].extend([
    markdown("## Right job parameters and outcome\n\nInputs are ordinary dictionaries. A mock Jobs API supplies the notebook result as JSON."),
    code('''from pprint import pprint
from types import SimpleNamespace
from unittest.mock import Mock
from datetime import datetime, timezone

settings = {"volume_root": "/Volumes/demo/recon/runs", "owner": "alex", "timezone": "UTC", "pair_table": "demo.recon.pair_recon_pair_meta", "status_table": "demo.recon.pair_recon_run_status", "error_table": "demo.recon.pair_recon_run_error", "right_workers": 2}
run_id = make_run_id("orders", "alex", "UTC", now=datetime(2026, 1, 2, tzinfo=timezone.utc))
print("run id input:", "orders", "alex", "UTC", "2026-01-02")
print("run id output:", run_id)
print("settings:"); pprint(settings)
print("job parameters:"); pprint(right_job_params(settings, "orders", run_id, {"sample": 0.1}))
jobs = Mock()
jobs.get_run_output.return_value = SimpleNamespace(notebook_output=SimpleNamespace(result='{"state":"matched","count":3}'))
run = SimpleNamespace(run_id=42, tasks=[])
print("notebook output input:", jobs.get_run_output.return_value.notebook_output.result)
print("run outcome:", run_outcome(jobs, run))
print("SDK call:", jobs.get_run_output.call_args)
store = Mock()
record_runtime_failure(store, "orders", "right-job", RuntimeError("toy failure"))
print("failure artifact input:", store.put_json.call_args)'''),
    markdown("## Monitor cancellation and error propagation\n\nThese objects are local. `request_cancel` marks a caller-initiated cancellation; `failure`, `raise_if_failed`, and `result` expose a detected failure."),
    code('''from unittest.mock import Mock
remote = Mock()
remote.run_id = 123
monitor = RightJobMonitor(remote)
print("failure before error:", monitor.failure())
monitor.request_cancel()
print("cancel requested:", monitor.cancel_requested.is_set())
monitor.error = RuntimeError("toy failure")
monitor.done.set()
print("failure:", monitor.failure())
for method in (monitor.raise_if_failed, monitor.result):
    try:
        method()
    except RuntimeError as error:
        print(method.__name__, "raised:", str(error))'''),
    markdown("## `StageWatchdog.check`, `start`, and `close`\n\nThe mock store returns no state; timeouts are disabled for a deterministic no-stall example. The daemon is closed immediately."),
    code('''from unittest.mock import Mock
from pair_recon_left.common.runtime.stop import StopSignal
store = Mock()
store.exists.return_value = False
watchdog = StageWatchdog(store, StopSignal(), interval=60)
watchdog.fetch_stall = 0
watchdog.first_result = 0
watchdog.state_stall = 0
watchdog.volume_failures = 0
print("check output:", watchdog.check(now=100))
watchdog.start()
print("thread alive:", watchdog.thread.is_alive())
watchdog.close()
watchdog.thread.join(timeout=1)
print("thread stopped:", not watchdog.thread.is_alive())'''),
])


def main() -> None:
    HERE.mkdir(parents=True, exist_ok=True)
    for name, value in NOTEBOOKS.items():
        (HERE / name).write_text(
            json.dumps(value, ensure_ascii=False, indent=1) + "\n",
            encoding="utf-8",
        )
    print(f"wrote {len(NOTEBOOKS)} notebooks to {HERE}")


if __name__ == "__main__":
    main()
