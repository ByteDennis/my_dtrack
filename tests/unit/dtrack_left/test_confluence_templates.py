import json
from pathlib import Path

import pytest

from dtrack_left.confluence_templates import (
    build_stored_template_page,
    build_template_page,
    merge_template_data,
)

ROOT = Path(__file__).resolve().parents[3]


def test_business_validation():
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


def test_template_escapes(tmp_path):
    data = tmp_path / "data.json"
    data.write_text(json.dumps({"title": "<script>x</script>", "tables": []}))
    safe = tmp_path / "safe.xhtml"
    safe.write_text("<h1>{{title}}</h1>")
    assert "&lt;script&gt;" in build_template_page(safe, data)["body"]

    raw = tmp_path / "raw.xhtml"
    raw.write_text("<h1>{{{title}}}</h1>")
    with pytest.raises(ValueError, match="not generated safe XHTML"):
        build_template_page(raw, data)


def test_template_state(tmp_path):
    template = tmp_path / "page.xhtml"
    template.write_text(
        "{{#tables}}<p>{{name}} {{{tech_pvt.artifacts_xhtml}}} "
        "{{{biz_pvt.status_xhtml}}}</p>{{/tables}}"
    )
    data = tmp_path / "data.json"
    data.write_text(
        json.dumps(
            {
                "title": "Validation",
                "tables": [
                    {
                        "name": "Orders",
                        "table": "main.orders",
                        "tech_pvt": {"artifacts": []},
                        "biz_pvt": {},
                    }
                ],
            }
        )
    )
    state = build_template_page(template, data)["template_state"]
    merged = merge_template_data(
        state["data"],
        append_tables=[{"name": "Customers", "table": "main.customers"}],
        add_evidence=[
            {
                "table": "Orders",
                "phase": "tech_pvt",
                "label": "Orders evidence",
                "attachment": "orders.xlsx",
            }
        ],
        set_status=["Orders.business_pvt=pass", "Customers=not pass"],
    )
    page = build_stored_template_page({**state, "data": merged})
    assert len(page["pairs"]) == 2
    assert 'ri:filename="orders.xlsx"' in page["body"]
    assert "PASS" in page["body"]
    assert page["template_state"]["data"]["tables"][1]["status"] == "NOT PASS"


def test_template_rejects(tmp_path):
    document = {"tables": [{"name": "Orders", "table": "main.orders"}]}
    with pytest.raises(ValueError, match="already exists"):
        merge_template_data(document, append_tables=[{"name": "Orders"}])
    with pytest.raises(ValueError, match="matched 0"):
        merge_template_data(document, set_status=["Missing=PASS"])
