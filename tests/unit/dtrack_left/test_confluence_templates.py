import json
from pathlib import Path

import pytest

from dtrack_left.confluence_templates import build_template_page

ROOT = Path(__file__).resolve().parents[3]


def test_business_validation_template_computes_metrics_and_artifact_links():
    page = build_template_page(
        ROOT / "templates/confluence/business-validation-overall.xhtml",
        ROOT / "templates/confluence/business-validation-overall.example.json",
        attachment_names=["orders-biz-pvt.xlsx"],
    )
    body = page["body"]
    assert page["title"] == "Business Validation Overall Status"
    assert "Tech PVT Started</strong></p><h2>3 / 3" in body
    assert "Biz PVT Passed</strong></p><h2>1 / 3" in body
    assert 'href="https://reports.example/orders/tech.csv"' in body
    assert 'ri:filename="orders-biz-pvt.xlsx"' in body
    assert "main.finance.payments" in body and "NOT PASS" in body


def test_template_escapes_json_values_and_rejects_raw_user_fields(tmp_path):
    data = tmp_path / "data.json"
    data.write_text(json.dumps({"title": "<script>x</script>", "tables": []}))
    safe = tmp_path / "safe.xhtml"
    safe.write_text("<h1>{{title}}</h1>")
    assert "&lt;script&gt;" in build_template_page(safe, data)["body"]

    raw = tmp_path / "raw.xhtml"
    raw.write_text("<h1>{{{title}}}</h1>")
    with pytest.raises(ValueError, match="not generated safe XHTML"):
        build_template_page(raw, data)
