# dtrack 测试指南

## 测试结构

| 路径 | 覆盖内容 | 运行环境 |
|------|----------|----------|
| `tests/*.py` | 纯 Python 单元测试(SQL 生成、引号、parity、db CRUD) | 所有平台 |
| `tests/csv/` | CSV → DuckDB 流程(无外部后端) | 所有平台 |
| `tests/api/` | FastAPI HTTP 层测试(Starlette `TestClient`) | 仅 Windows 开发机 |
| `tests/e2e/` | 完整 CLI 流程,**mock** 后端(`DTRACK_MOCK` 复制 fixture CSV) | 仅 Windows 开发机 |
| `tests/integration/` | **真实**连后端、跑全流程(extract→load→compare) | 手动开启(`--run-live`) |

`tests/api/` 和 `tests/e2e/` 由各自 `conftest.py` 的 `pytest_collection_modifyitems`(`platform.system() == "Windows"`)限制为只在 Windows 跑;其它平台会被 collect 但 **skip**。

## 在 Windows 上跑全套

> **关键坑**:测试框架**不会加载** `.env`。`.env` 只有 `dtrack serve` / CLI 会加载(`dtrack/cli.py`)。API 测试直接建 `TestClient` 绕过了它 —— 没进进程环境的后端变量就会缺失(例如报 `No Oracle user for 'pcds': set PCDS_USR`)。

两种办法让变量对 pytest 可见:

**办法 A —— 跑之前先导出**(最简单):
```bat
set PCDS_USR=...
set PCDS_PWD=...
python -m pytest tests/api tests/e2e -q
```

**办法 B —— 让测试自动加载 `.env`**:新建 `tests/conftest.py`:
```python
import os
from dotenv import dotenv_values

def pytest_configure(config):
    for fname in ("dtrack.conf", ".env", "dtrack.env"):
        if os.path.exists(fname):
            for k, v in dotenv_values(fname).items():
                if v is not None:
                    os.environ.setdefault(k, v)
            break
```

随时用 `dtrack doctor` 查看实际解析到的 env-file、Oracle macro、AWS 变量。

## 各后端需要的环境变量(live 模式)

用到哪个后端就配哪个。

### Oracle(`conn_macro` = `pb23`、`pb30`、`pcds` …)
| 变量 | 用途 |
|------|------|
| `PCDS_USR`、`PCDS_PWD` | 共享 Oracle 用户/密码(所有 macro 的兜底) |
| `<MACRO>_USR`、`<MACRO>_PWD` | 单 macro 覆盖,如 `PB23_PWD`(优先于 `PCDS_*`) |
| `LDAP_DSN` | 可选,LDAP DSN 模板 |
| `DTRACK_ORACLE_MACROS` / `_FILE` | 在内置 `pb23`、`pb30` 之外加 macro→service |

### AWS / Athena(`source: aws`)
| 变量 | 用途 |
|------|------|
| `AWS_USR`、`AWS_PWD`、`AWS_HOST` | 公司代理(`http://usr:pwd@host:8080`) |
| `AWS_TOKEN_URL`、`AWS_ARN_URL` | 取 token + 临时凭证的端点 |
| `AWS_DEFAULT_REGION` | Athena region |
| `AWS_S3_STAGING_DIR`、`AWS_S3_WORK_GROUP` | Athena 输出位置 / workgroup |

### Databricks(`source: databricks`)
| 变量 | 用途 |
|------|------|
| `DB_PROFILE` | `~/.databrickscfg` 里的 profile 名 |
| `DB_HTTPS` | SQL warehouse 的 HTTP Path |
| `DB_DEFAULT_CATALOG` | `conn_macro` 只有 schema 时前置成 `catalog.schema` |
| `AWS_USR`、`AWS_PWD`、`AWS_HOST` | 复用作 HTTPS 代理 |

### Hadoop(`source: hadoop`)
| 变量 | 用途 |
|------|------|
| `OUT_DIR`、`SEED`、`EMAIL_TO` | 输出目录、采样种子、通知邮箱 |
| `HADOOP_DSN` | Hive / Hadoop ODBC DSN |
| `ODBCINI`、`ODBCSYSINI` | ODBC 配置文件位置 |

> **不用 mock**:所有 `DTRACK_*_MOCK`(`DTRACK_MOCK`、`DTRACK_ORACLE_MOCK` …)都**别设** —— 设了任何一个都会把对应后端转去读 fixture CSV。

## 跑测试

```bat
:: 跨平台单元 + CSV 测试(快,无后端)
python -m pytest -q

:: Windows 上全套(api + e2e 自动开启)
python -m pytest tests -q

:: 单个文件 / 单个用例
python -m pytest tests/api/test_compare.py -q
```

## Live 集成测试(真实后端)

`tests/integration/` 是**唯一**真连服务器、用**真实表**跑**全流程(extract → load → compare)**的测试,仿 `tests/e2e/` 的结构 —— **一个阶段一个文件**(`test_01_extract_row.py` … `test_06_compare_col.py`),共享 fixture(`combo` 矩阵参数化、`live_workdir` 沙盒、`pipe` 各阶段操作)都在 `conftest.py`。**row 和 col 两条线都覆盖**(col 那条正好跑到本次改的引号/反引号 SQL)。断言是 live 容忍的:每边 extract 要有真实行、两边都 load 进 `_row_counts` / `_col_stats`、`compare-row` / `compare-col` 要产出 HTML —— 不校验精确行数。

覆盖矩阵 **左 ∈ {oracle, hadoop} × 右 ∈ {athena, databricks}**。**默认 skip**(pytest 标记 `live`、`databricks`),用 CLI 参数开启,**不用环境变量**:

```bat
:: oracle/hadoop x athena(databricks 组合仍跳过)
python -m pytest tests/integration --run-live

:: 把慢的 databricks 组合也跑上
python -m pytest tests/integration --run-live --run-databricks
```

### 改成 live 的步骤

1. **后端凭证**:连接变量写进 `.env`(见上),并确保进了进程环境(`set` 导出,或办法 B 的自动加载 `tests/conftest.py`)。所有 `DTRACK_*_MOCK` 都别设。

2. **填 `LIVE_TABLES`**:改 `tests/integration/conftest.py` 顶部的 dict,给每个平台一张真实存在的表:
```python
LIVE_TABLES = {
    "oracle":     {"table": "SCHEMA.REAL_TABLE", "conn_macro": "pb23", "date_col": "RPT_DT", "date_type": "date"},
    "hadoop":     {"table": "db.real_table", "conn_macro": "hive_prod", "date_col": "rpt_dt", "date_type": "string_dash"},
    "athena":     {"table": "real_table", "conn_macro": "analytics_db", "date_col": "rpt_dt", "date_type": "string_dash"},
    "databricks": {"table": "real_table", "conn_macro": "main.analytics", "date_col": "rpt_dt", "date_type": "date"},
}
```

3. **仅 databricks**:配好 `DB_PROFILE`(`%USERPROFILE%\.databrickscfg` 里的 profile)+ `DB_HTTPS`(SQL Warehouses → Connection details → HTTP Path)。跑前验证:
```bat
python -c "from databricks.sdk.core import Config; c=Config(profile='myprofile'); print(c.host); print(bool(c.authenticate()))"
```
若看到 *"Databricks Connect disabled (local mode)"*,那是 **`databricks-connect`** 包遮住了 `databricks.sql`;dtrack 用 `databricks-sql-connector` —— `pip uninstall databricks-connect` 后 `pip install "dtrack[databricks]"`。

4. **运行**:加 `--run-live`(warehouse 能连后再加 `--run-databricks`,它冷启动慢)。

> **hadoop 走直连 ODBC**(`source: hadoop`,经 `extract_hadoop` / pyodbc);没有独立的 `gen-hadoop` 命令,测试直接调 `extract_hadoop`。
>
> **不要**改 `_E2E_PAIRS`(e2e)或 `_seed_pairs`(api)来"转 live":e2e 强制 `DTRACK_MOCK` 且断言 fixture 行数,api 又容忍 `200`/`500`。用这个 integration 文件。

## JavaScript 测试

前端 `dtrack/web/static/*.js` 的单元测试在 `tests/csv/js/`(`*.test.mjs`),用 **Node 内置 runner**(`node:test`)—— 无 `package.json` / npm / jest。需 **Node 21+**(`--test` glob 支持)。

```bat
node --test "tests/csv/js/*.test.mjs"
node --test tests/csv/js/fmt.test.mjs
node --test --watch "tests/csv/js/*.test.mjs"
```

## 覆盖率

```bat
:: 终端报告 + 未覆盖行号
python -m pytest --cov=dtrack --cov-report=term-missing -q

:: HTML 报告 → 打开 htmlcov/index.html
python -m pytest --cov=dtrack --cov-report=html -q
```

> **覆盖率只在测试真跑的地方有意义**:非 Windows 上 api + e2e 被 skip,所以 `cli.py`、`dtrack/web/app.py`、`html_export.py` 等显示近 0% —— 是 skip,不是死代码。要真实数字,在 Windows 上(配好 env)跑。
