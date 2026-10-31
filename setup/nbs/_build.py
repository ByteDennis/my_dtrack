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
