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
