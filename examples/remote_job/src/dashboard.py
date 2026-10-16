from __future__ import annotations

RED, ORANGE, BLUE, GREEN, GRAY = "red", "orange", "blue", "green", "gray"
TINT = {
    RED: ("#FDE2E1", "#A4161A"),
    ORANGE: ("#FFE8CC", "#8A4B00"),
    BLUE: ("#DCEEF6", "#075A73"),
    GREEN: ("#D4EDDA", "#155724"),
    GRAY: ("#ECEEF0", "#4A5560"),
}
SOLID = {
    RED: "#E5484D",
    ORANGE: "#F76B15",
    BLUE: "#0B7FA8",
    GREEN: "#30A46C",
    GRAY: "#9BA1A6",
}
STATUSES = [
    ("FAILED", "🔴", RED),
    ("BLOCKED", "🔴", RED),
    ("STALLED", "🟠", ORANGE),
    ("ROW_CHECK", "🔵", BLUE),
    ("COL_CHECK", "🔵", BLUE),
    ("REPORTING", "🔵", BLUE),
    ("NOT_STARTED", "⚪", GRAY),
    ("DONE", "🟢", GREEN),
]
HEALTH = {"LATE": RED, "AT_RISK": ORANGE, "ON_TRACK": GREEN, "NO_TARGET": GRAY}
STATUS_TINTS = {status: color for status, _icon, color in STATUSES}
HOURS_SQL = (
    "CASE WHEN {h} IS NULL THEN 'unknown' WHEN {h} < 24 THEN concat({h}, 'h') "
    "ELSE concat(floor({h} / 24), 'd ', {h} % 24, 'h') END"
)
AGE_SQL = (
    "CASE WHEN {m} IS NULL THEN 'never' WHEN {m} < 60 THEN concat({m}, 'm ago') "
    "WHEN {m} < 1440 THEN concat(floor({m} / 60), 'h ', {m} % 60, 'm ago') "
    "ELSE concat(floor({m} / 1440), 'd ', floor(({m} % 1440) / 60), 'h ago') END"
)


def _case(column, pairs, default):
    whens = "\n".join(f"    WHEN '{value}' THEN '{label}'" for value, label in pairs)
    return f"CASE {column}\n{whens}\n    ELSE {default}\n  END"


def _dataset_sql(schema):
    labels = [
        (status, f"{icon} {status.replace('_', ' ').title()}") for status, icon, _ in STATUSES
    ]
    priorities = [(status, str(index)) for index, (status, _icon, _color) in enumerate(STATUSES)]
    status = f"""SELECT *,
  {_case("status", labels, "status")} AS status_label,
  CAST({_case("status", priorities, str(len(STATUSES)))} AS INT) AS severity,
  {HOURS_SQL.format(h="elapsed_hours")} AS elapsed,
  CASE WHEN eta_ts IS NULL THEN 'unknown'
       ELSE {HOURS_SQL.format(h="timestampdiff(HOUR, current_timestamp(), eta_ts)")} END AS eta,
  {AGE_SQL.format(m="heartbeat_age_minutes")} AS heartbeat_age,
  CASE WHEN heartbeat_at IS NULL OR run_state = 'complete' THEN '-'
       WHEN timeout_in_minutes < 0 THEN concat('overdue ', -timeout_in_minutes, 'm')
       ELSE concat('in ', timeout_in_minutes, 'm') END AS stale_in,
  CAST(status = 'DONE' AS INT) AS is_done,
  CAST(status IN ('ROW_CHECK', 'COL_CHECK', 'REPORTING') AS INT) AS is_running,
  CAST(status IN ('BLOCKED', 'STALLED', 'FAILED') AS INT) AS is_trouble,
  CAST(status = 'NOT_STARTED' AS INT) AS is_waiting
FROM {schema}.v_pair_status"""
    daily = f"SELECT * FROM {schema}.v_pair_daily"
    blockers = f"""SELECT *, resolved_at IS NULL AS is_open,
  timestampdiff(HOUR, opened_at, coalesce(resolved_at, current_timestamp())) AS open_hours
FROM {schema}.dtrack_pair_blocker
WHERE resolved_at IS NULL OR resolved_at >= current_timestamp() - INTERVAL 30 DAYS"""
    return {"ds_status": status, "ds_daily": daily, "ds_blockers": blockers}


def _dataset(name, display, sql):
    lines = sql.split("\n")
    return {
        "name": name,
        "displayName": display,
        "queryLines": [line + "\n" for line in lines[:-1]] + [lines[-1]],
    }


def _frame(title, description=""):
    return {
        "showTitle": True,
        "title": title,
        "showDescription": bool(description),
        "description": description,
    }


def _tints(field, colors):
    return {
        "type": "basic",
        "rules": [
            {
                "condition": {
                    "operand": {"type": "data-value", "value": value},
                    "operator": "=",
                    "fieldName": field,
                },
                "backgroundColor": TINT[color][0],
                "foregroundColor": TINT[color][1],
            }
            for value, color in colors.items()
        ],
    }


def _column(field, title, kind="string", style=None, width=None):
    spec = {"fieldName": field, "displayName": title, "useForSearch": kind == "string"}
    if kind == "integer":
        spec["format"] = {
            "type": "number-plain",
            "abbreviation": "none",
            "decimalPlaces": {"type": "exact", "places": 0},
        }
        spec["contentAlignment"] = "right"
    elif kind == "percent":
        spec["format"] = {
            "type": "number-plain",
            "abbreviation": "none",
            "decimalPlaces": {"type": "exact", "places": 1},
        }
        spec["contentAlignment"] = "right"
    elif kind == "date":
        spec["format"] = {"type": "moment.js", "format": "YYYY-MM-DD"}
    elif kind == "datetime":
        spec["format"] = {"type": "moment.js", "format": "YYYY-MM-DD HH:mm"}
    if kind == "long":
        spec["actions"] = {"hover": {"type": "tooltip", "tooltipTemplate": "{{ @ }}"}}
    if style:
        spec["style"] = style
    if width:
        spec["columnWidth"] = width
    return spec


def _table(name, dataset, title, columns, orders, extra=(), rows=25):
    fields = [column["fieldName"] for column in columns]
    fields.extend(field for field in extra if field not in fields)
    query = {
        "datasetName": dataset,
        "fields": [{"name": field, "expression": f"`{field}`"} for field in fields],
        "disaggregated": True,
        "orders": [
            {"direction": direction, "expression": f"`{field}`"} for field, direction in orders
        ],
    }
    return {
        "name": name,
        "queries": [{"name": "main_query", "query": query}],
        "spec": {
            "version": 2,
            "widgetType": "table",
            "rowsPerPage": rows,
            "displayRowNumbers": False,
            "encodings": {"columns": columns},
            "data": {"queryName": "main_query"},
            "frame": _frame(title),
        },
    }


def _counter(name, dataset, title, aggregate, field):
    alias = f"{aggregate.lower()}({field})"
    query = {
        "datasetName": dataset,
        "fields": [{"name": alias, "expression": f"{aggregate}(`{field}`)"}],
        "disaggregated": False,
    }
    return {
        "name": name,
        "queries": [{"name": "main_query", "query": query}],
        "spec": {
            "version": 2,
            "widgetType": "counter",
            "encodings": {"value": {"fieldName": alias, "displayName": title}},
            "data": {"queryName": "main_query"},
            "frame": _frame(title),
        },
    }


def _bar(name, dataset, title):
    fields = [
        {"name": "wave", "expression": "`wave`"},
        {"name": "status", "expression": "`status`"},
        {"name": "count(pair_name)", "expression": "COUNT(`pair_name`)"},
    ]
    return {
        "name": name,
        "queries": [
            {
                "name": "main_query",
                "query": {"datasetName": dataset, "fields": fields, "disaggregated": False},
            }
        ],
        "spec": {
            "version": 3,
            "widgetType": "bar",
            "encodings": {
                "x": {"fieldName": "wave", "scale": {"type": "categorical"}},
                "y": {"fieldName": "count(pair_name)", "scale": {"type": "quantitative"}},
                "color": {
                    "fieldName": "status",
                    "scale": {
                        "type": "categorical",
                        "mappings": [
                            {"value": status, "color": SOLID[color]}
                            for status, color in STATUS_TINTS.items()
                        ],
                    },
                },
            },
            "frame": _frame(title),
        },
    }


def _line(name, dataset, title, color=None):
    fields = [
        {"name": "day", "expression": "`day`"},
        {"name": "sum(units_done)", "expression": "SUM(`units_done`)"},
    ]
    encodings = {
        "x": {"fieldName": "day", "scale": {"type": "temporal"}},
        "y": {"fieldName": "sum(units_done)", "scale": {"type": "quantitative"}},
    }
    if color:
        fields.append({"name": color, "expression": f"`{color}`"})
        encodings["color"] = {"fieldName": color, "scale": {"type": "categorical"}}
    return {
        "name": name,
        "queries": [
            {
                "name": "main_query",
                "query": {"datasetName": dataset, "fields": fields, "disaggregated": False},
            }
        ],
        "spec": {
            "version": 3,
            "widgetType": "line",
            "encodings": encodings,
            "frame": _frame(title),
        },
    }


def _filter(name, title, field, datasets, kind="filter-multi-select"):
    queries, fields = [], []
    for dataset in datasets:
        query_name = f"filter_{name}_{dataset}"
        queries.append(
            {
                "name": query_name,
                "query": {
                    "datasetName": dataset,
                    "fields": [
                        {"name": field, "expression": f"`{field}`"},
                        {
                            "name": f"{field}_associativity",
                            "expression": "COUNT_IF(`associative_filter_predicate_group`)",
                        },
                    ],
                    "disaggregated": False,
                },
            }
        )
        fields.append({"fieldName": field, "displayName": field, "queryName": query_name})
    return {
        "name": name,
        "queries": queries,
        "spec": {
            "version": 2,
            "widgetType": kind,
            "encodings": {"fields": fields},
            "frame": {"showTitle": True, "title": title},
        },
    }


def _text(name, markdown):
    return {
        "name": name,
        "multilineTextboxSpec": {"lines": [line + "\n" for line in markdown.strip().split("\n")]},
    }


def _at(widget, x, y, width, height):
    return {
        "widget": widget,
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def _page(name, display, layout):
    return {
        "name": name,
        "displayName": display,
        "pageType": "PAGE_TYPE_CANVAS",
        "layoutVersion": "GRID_V1",
        "layout": layout,
    }


def _executive():
    columns = [
        _column("display_name", "Pair", width=180),
        _column("status_label", "Status", style=_tints("status", STATUS_TINTS), width=150),
        _column("started_at", "Started", "datetime"),
        _column("elapsed", "Elapsed", width=90),
        _column("progress_pct", "Progress %", "percent"),
        _column("eta", "ETA", width=100),
        _column("target_done_date", "Target", "date"),
        _column("schedule_health", "Schedule", style=_tints("schedule_health", HEALTH)),
        _column("heartbeat_age", "Heartbeat", width=100),
        _column("top_blocker", "Top blocker", "long", width=320),
    ]
    note = (
        "**Status** uses the latest collector snapshot · **Progress** = 15% row check + "
        "80% units compared + 5% report · the launcher refreshes the collector after every run"
    )
    layout = [
        _at(_text("exec_note", note), 0, 0, 8, 1),
        _at(_filter("wave", "Wave", "wave", ["ds_status"]), 8, 0, 2, 1),
        _at(_filter("owner", "Owner", "owner", ["ds_status"]), 10, 0, 2, 1),
        _at(_counter("total", "ds_status", "Pairs", "COUNT", "pair_name"), 0, 1, 2, 2),
        _at(_counter("done", "ds_status", "Done", "SUM", "is_done"), 2, 1, 2, 2),
        _at(_counter("running", "ds_status", "Running", "SUM", "is_running"), 4, 1, 2, 2),
        _at(_counter("trouble", "ds_status", "Needs attention", "SUM", "is_trouble"), 6, 1, 2, 2),
        _at(_counter("waiting", "ds_status", "Not started", "SUM", "is_waiting"), 8, 1, 2, 2),
        _at(
            _table(
                "pair_table",
                "ds_status",
                "Pair progress",
                columns,
                [("severity", "ASC"), ("priority", "ASC"), ("display_name", "ASC")],
                extra=("status", "priority"),
            ),
            0,
            3,
            12,
            8,
        ),
        _at(_bar("wave_status", "ds_status", "Status by wave"), 0, 11, 6, 5),
        _at(_line("burnup", "ds_daily", "Units completed"), 6, 11, 6, 5),
    ]
    return _page("executive", "Executive summary", layout)


def _detailed():
    pairs = [
        _column("pair_name", "Pair", width=160),
        _column("run_id", "Run", width=200),
        _column("status_label", "Status", style=_tints("status", STATUS_TINTS), width=150),
        _column("row_days_match", "Match", "integer"),
        _column("row_days_count_diff", "Count diff", "integer"),
        _column("row_days_left_only", "Left only", "integer"),
        _column("row_days_right_only", "Right only", "integer"),
        _column("units_expected", "Expected", "integer"),
        _column("units_published", "Published", "integer"),
        _column("units_compared", "Compared", "integer"),
        _column("units_dead", "Dead", "integer"),
        _column("units_diff", "Diff", "integer"),
        _column("heartbeat_at", "Heartbeat", "datetime"),
        _column("last_result_at", "Last result", "datetime"),
        _column("run_root", "Run root", "long", width=300),
    ]
    blockers = [
        _column("pair_name", "Pair", width=160),
        _column("severity", "Severity"),
        _column("stage", "Stage"),
        _column("category", "Category"),
        _column("title", "Title", "long", width=320),
        _column("owner", "Owner"),
        _column("opened_at", "Opened", "datetime"),
        _column("open_hours", "Open hours", "integer"),
        _column("resolved_at", "Resolved", "datetime"),
    ]
    layout = [
        _at(
            _filter("pair", "Pair", "pair_name", ["ds_status", "ds_blockers", "ds_daily"]),
            0,
            0,
            3,
            1,
        ),
        _at(_filter("status", "Status", "status", ["ds_status"]), 3, 0, 3, 1),
        _at(
            _filter("day", "History", "day", ["ds_daily"], "filter-date-range-picker"),
            6,
            0,
            3,
            1,
        ),
        _at(
            _table(
                "pair_detail",
                "ds_status",
                "Pair detail",
                pairs,
                [("severity", "ASC"), ("pair_name", "ASC")],
                extra=("status",),
                rows=50,
            ),
            0,
            1,
            12,
            8,
        ),
        _at(
            _line("history", "ds_daily", "Units completed per pair", color="pair_name"), 0, 9, 12, 5
        ),
        _at(
            _table(
                "blockers",
                "ds_blockers",
                "Blockers",
                blockers,
                [("is_open", "DESC"), ("severity", "ASC"), ("opened_at", "DESC")],
                extra=("is_open",),
            ),
            0,
            14,
            12,
            7,
        ),
    ]
    return _page("detailed", "Detailed", layout)


def build(schema: str) -> dict:
    sql = _dataset_sql(schema)
    return {
        "datasets": [
            _dataset("ds_status", "pair_status", sql["ds_status"]),
            _dataset("ds_daily", "pair_daily", sql["ds_daily"]),
            _dataset("ds_blockers", "pair_blockers", sql["ds_blockers"]),
        ],
        "pages": [_executive(), _detailed()],
        "uiSettings": {
            "theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"},
            "applyModeEnabled": False,
        },
    }
