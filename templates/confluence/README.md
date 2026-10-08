# Confluence templates

Preview the business-validation template without contacting Confluence:

```bash
dtrack tools blog preview \
  --template templates/confluence/business-validation-overall.xhtml \
  --data templates/confluence/business-validation-overall.example.json
```

Publish it privately to the current user by default:

```bash
dtrack tools blog publish \
  --template templates/confluence/business-validation-overall.xhtml \
  --data validation.json \
  --space DTRACK \
  --attach orders-biz-pvt.xlsx
```

Use `--space-visible` to inherit space visibility instead. Artifact entries accept either an
external `url` or a Confluence `attachment` filename. Edit the XHTML layout freely; ordinary
`{{field}}` values are escaped, while generated `{{{...}}}` fields are restricted to safe status
and artifact fragments.

Template publication also stores versioned structured state on the page. Later changes merge that
state and redraw the exact page id:

```bash
dtrack tools blog update --page-id 123 \
  --append-table new-table.json \
  --add-evidence new-evidence.json \
  --set-status 'Orders.business_pvt=PASS' \
  --attach orders-signoff.xlsx
```

`--append-table` accepts one table object or a list. Evidence requires `table`,
`phase: tech_pvt|business_pvt`, and either an `artifact`, an `artifacts` list, or inline
`label` plus `url|attachment`. `--set-status Orders=PASS` changes the overall status; suffix it
with `.tech_pvt` or `.business_pvt` to change a phase. A page must first be published with
`--template` and `--data`; report-backed pages do not have editable template state.
