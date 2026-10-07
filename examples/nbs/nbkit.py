from __future__ import annotations

import ast
import html
import importlib.util
import io
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tokenize
from pathlib import Path

from IPython.display import HTML, display
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import get_lexer_by_name

from dtrack_left.checks.col_check import (
    ColCheckPipeline,
    LeftRuntime,
    publish_fixed_keys,
    wait_for_manifests,
)
from dtrack_left.checks.row_check import run_row_check as left_row_check
from dtrack_local.csv_target import CsvAnchorTarget
from dtrack_right.backends.store import VolumeStore
from dtrack_right.checks.col_check import ColCheck
from dtrack_right.checks.row_check import run_row_check as right_row_check

ROOT = Path(__file__).resolve().parents[2]
WORK = ROOT / "examples/nbs/.work"
_spec = importlib.util.spec_from_file_location("support", ROOT / "tests/support.py")
support = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(support)


BAD = {"diff", "count_diff", "left_only", "right_only", "PARTIAL", "FAILED", "False"}
GOOD = {"match", "DONE", "True"}
CSS = """
<style>
.dt-table { border-collapse: collapse; margin: 6px 0; font-size: 12.5px;
  font-family: var(--vscode-editor-font-family, monospace); }
.dt-table th { text-align: left; padding: 5px 10px; font-weight: 600;
  border-bottom: 2px solid var(--vscode-focusBorder, #4e94ce);
  background: var(--vscode-editorWidget-background, rgba(127,127,127,.12)); }
.dt-table td { padding: 4px 10px; border-bottom: 1px solid var(--vscode-panel-border, rgba(127,127,127,.25)); }
.dt-table tr:nth-child(even) td { background: rgba(127,127,127,.06); }
.dt-table td.num { text-align: right; font-variant-numeric: tabular-nums; }
.dt-table td.bad { color: #f14c4c; font-weight: 600; }
.dt-table td.good { color: #23a55a; }
.dt-table td.none { opacity: .45; }
.dt-code { margin: 6px 0; padding: 8px 12px; border-radius: 6px; font-size: 12.5px; line-height: 1.45;
  font-family: var(--vscode-editor-font-family, monospace); white-space: pre-wrap; word-break: break-word;
  background: var(--vscode-textCodeBlock-background, rgba(127,127,127,.12));
  border-left: 3px solid var(--vscode-focusBorder, #4e94ce); }
.dt-code pre { margin: 0; white-space: pre-wrap; background: transparent !important; }
.dt-label { font-size: 11px; opacity: .65; margin: 8px 0 -2px; font-family: var(--vscode-font-family, sans-serif); }
</style>
"""
CSS += "<style>" + HtmlFormatter(style="friendly").get_style_defs(".vscode-light .dt-code")
CSS += HtmlFormatter(style="github-dark").get_style_defs(".vscode-dark .dt-code") + "</style>"


# >>> a DataFrame as a themed HTML table; verdict-like values coloured, numbers right-aligned <<< #
def table(frame, index: bool = False) -> None:
    frame = frame.reset_index() if index else frame
    head = "".join(f"<th>{html.escape(str(column))}</th>" for column in frame.columns)
    rows = []
    for values in frame.itertuples(index=False):
        cells = []
        for value in values:
            text = (
                ""
                if value is None or (isinstance(value, float) and math.isnan(value))
                else str(value)
            )
            text = "" if text in {"<NA>", "NaT", "nan", "None"} else text
            kind = "bad" if text in BAD else "good" if text in GOOD else ""
            kind = kind or (
                "num" if isinstance(value, int | float) and not isinstance(value, bool) else ""
            )
            kind = kind or ("none" if not text else "")
            cells.append(f'<td class="{kind}">{html.escape(text)}</td>')
        rows.append("<tr>" + "".join(cells) + "</tr>")
    display(HTML(f'{CSS}<table class="dt-table"><tr>{head}</tr>{"".join(rows)}</table>'))


# >>> text as a highlighted block: json, sql, python, or plain text for listings <<< #
def code(text: str, lang: str = "text", label: str | None = None) -> None:
    body = highlight(str(text).rstrip("\n"), get_lexer_by_name(lang), HtmlFormatter(nowrap=True))
    title = f'<div class="dt-label">{html.escape(label)}</div>' if label else ""
    display(HTML(f'{CSS}{title}<div class="dt-code"><pre>{body}</pre></div>'))


# >>> a clean scratch folder per notebook under examples/nbs/.work/<name> <<< #
def sandbox(name: str) -> Path:
    folder = WORK / name
    shutil.rmtree(folder, ignore_errors=True)
    folder.mkdir(parents=True)
    return folder


# >>> the synthetic e2e data (left CSVs as the "Oracle", right CSVs as the "Databricks table") <<< #
def dataset(folder: Path):
    summary = support.synthetic_data(folder / "data")
    return folder / "data", support.pairs_config(folder / "data" / "left"), summary


# >>> every object under a store folder, like `aws s3 ls --recursive` <<< #
def tree(root: Path, prefix: str = "") -> None:
    root = Path(root)
    lines = [
        f"{path.stat().st_size:>7}  {path.relative_to(root).as_posix()}"
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.relative_to(root).as_posix().startswith(prefix)
    ]
    code("\n".join(lines) or "(empty)", label=f"store: {prefix or '/'}")


def show(value, label: str | None = None) -> None:
    code(json.dumps(value, indent=2, sort_keys=True, default=str), "json", label)


# >>> the Right side's stand-in target: one CSV per table under data/right <<< #
def target_factory(data: Path, folder: Path):
    return lambda config: CsvAnchorTarget(config, data / "right", folder / "right_work")


# >>> Stage 1 on both sides: left counts, right reconcile + gate + manifest <<< #
def stage_one(folder: Path, run_id: str = "nb"):
    data, config, _summary = dataset(folder)
    store = VolumeStore(folder / "runs" / run_id)
    left_row_check(config, store, run_id=run_id)
    right_row_check(store, target_factory(data, folder))
    return data, store


# >>> Stage 2 on both sides: left units, right comparisons; returns the result rows <<< #
def stage_two(folder: Path, data: Path, store):
    manifests = wait_for_manifests(store, timeout=1, poll_interval=0)
    for manifest in manifests:
        publish_fixed_keys(store, manifest, folder / "left_work")
    runtime = LeftRuntime(folder / "left_work", store)
    published = ColCheckPipeline(
        store, pull_window=runtime.pull_window, compute_stats=runtime.compute_stats
    ).run(manifests)
    results = ColCheck(store, target_factory(data, folder), poll_interval=0).run()
    return published, results


# >>> run pytest in a subprocess from the repo root; show the command and its summary line <<< #
def pytest(*args: str, **env: str) -> int:
    done = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "--color=no", "-p", "no:cacheprovider", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, **env},
    )
    lines = [line for line in done.stdout.splitlines() if line.strip()]
    summary = lines[-1] if lines else done.stderr.strip()[-300:]
    code(f"$ pytest {' '.join(args)}\n{summary}", label=f"exit {done.returncode}")
    return done.returncode


# >>> every file under src/, tests/ and docs/notes that mentions a name: the blast radius of a change <<< #
def impact(term: str, roots=("src", "tests", "docs/notes", "examples/nbs")) -> None:
    rows = []
    for root in roots:
        for path in sorted((ROOT / root).rglob("*")):
            if (
                path.suffix not in {".py", ".md"}
                or "__pycache__" in path.parts
                or ".work" in path.parts
            ):
                continue
            numbers = [
                n
                for n, line in enumerate(path.read_text().splitlines(), 1)
                if term.lower() in line.lower()
            ]
            if numbers:
                rows.append(
                    {
                        "file": path.relative_to(ROOT).as_posix(),
                        "hits": len(numbers),
                        "lines": ", ".join(map(str, numbers[:8])),
                    }
                )
    import pandas as pd

    table(pd.DataFrame(rows, columns=["file", "hits", "lines"]))


# >>> the repo rules that tests or conventions enforce, measured now <<< #
def guardrails() -> None:
    import pandas as pd

    from dtrack_left.common import hashing as left_hashing
    from dtrack_left.common.sampling import sample_predicate_sql as left_sample_predicate
    from dtrack_right.common import hashing as right_hashing
    from dtrack_right.common.sampling import sample_predicate_sql as right_sample_predicate

    src = ROOT / "src"
    folders = [d for d in src.rglob("*") if d.is_dir() and "__pycache__" not in d.parts]
    widest = max(folders, key=lambda d: len(list(d.glob("*.py"))))
    longest = max(src.rglob("*.py"), key=lambda p: len(p.read_text().splitlines()))
    right = src / "dtrack_right/common"
    expected_right = {
        "__init__.py",
        "audit.py",
        "date_platforms.py",
        "dates.py",
        "hashing.py",
        "manifest.py",
        "sampling.py",
    }
    actual_right = {path.name for path in right.glob("*.py")}
    inventory_delta = sorted(expected_right ^ actual_right)
    normalization_cases = [
        ("  Ab C  ", "string", "left"),
        ("001.0", "numeric", "left"),
        (None, "string", "right"),
    ]
    normalization_parity = all(
        left_hashing.normalize_value(value, family, side)
        == right_hashing.normalize_value(value, family, side)
        for value, family, side in normalization_cases
    )
    sample_plans = [
        ({"mode": "fraction", "rate": 0.25}, None),
        ({"mode": "fixed"}, [["001.0"], ["2"]]),
    ]
    sampling_parity = all(
        left_sample_predicate(plan, ["id"], ["numeric"], "spark", side, key_tuples)
        == right_sample_predicate(plan, ["id"], ["numeric"], "spark", side, key_tuples)
        for plan, key_tuples in sample_plans
        for side in ("left", "right")
    )
    comments, docstrings = [], []
    for path in src.rglob("*.py"):
        text = path.read_text()
        if path.name != "notebook.py":
            for token in tokenize.generate_tokens(io.StringIO(text).readline):
                header = re.fullmatch(r"# >>> .+ <<< #", token.string.strip())
                if token.type == tokenize.COMMENT and not header and "noqa" not in token.string:
                    comments.append(path.relative_to(ROOT).as_posix())
        for node in ast.walk(ast.parse(text)):
            kinds = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)
            if isinstance(node, kinds) and ast.get_docstring(node):
                docstrings.append(path.relative_to(ROOT).as_posix())
    rows = [
        {
            "rule": "at most 5 .py files per src folder",
            "measured": f"{len(list(widest.glob('*.py')))} in {widest.relative_to(ROOT).as_posix()}",
            "ok": str(len(list(widest.glob("*.py"))) <= 5),
        },
        {
            "rule": "no module of thousands of lines",
            "measured": f"{len(longest.read_text().splitlines())} lines, {longest.relative_to(ROOT).as_posix()}",
            "ok": str(len(longest.read_text().splitlines()) < 500),
        },
        {
            "rule": "Right common/ contains only the Spark runtime subset",
            "measured": ", ".join(inventory_delta) or "expected inventory",
            "ok": str(not inventory_delta),
        },
        {
            "rule": "normalisation and Spark sampling behavior match Left",
            "measured": f"normalize={normalization_parity}, sample={sampling_parity}",
            "ok": str(normalization_parity and sampling_parity),
        },
        {
            "rule": "only `# >>> ... <<< #` comments (notebook.py exempt)",
            "measured": f"{len(comments)} other comments",
            "ok": str(not comments),
        },
        {
            "rule": "no docstrings",
            "measured": ", ".join(docstrings) or "none",
            "ok": str(not docstrings),
        },
    ]
    table(pd.DataFrame(rows))
