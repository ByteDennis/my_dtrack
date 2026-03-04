# GitHub Issues for dtrack

Create these 5 consolidated issues on GitHub: https://github.com/ByteDennis/my_dtrack/issues/new

---

## Issue 1: Core Functionality Testing

**Title:** Test core functionality: init, load-row, load-col, show commands

**Labels:** testing, core

**Description:**

Comprehensive testing of all basic dtrack functionality.

### Test Coverage:

**Database & Loading:**
- [ ] `init` creates database with correct schema
- [ ] `load-row` with different date formats (ISO, SAS datetime, YYYYMM, US)
- [ ] `load-row` with different vintages (day/week/month/quarter/year)
- [ ] `load-row` with different modes (replace/append/upsert)
- [ ] `load-row` from single CSV and folder with multiple CSVs
- [ ] `load-col` computes correct statistics for numeric columns
- [ ] `load-col` computes correct statistics for categorical columns
- [ ] `load-col` with date range filters (--from-date, --to-date)
- [ ] Column auto-detection (date_var, count columns)
- [ ] Missing value handling
- [ ] Metadata tracking (source, db, source_table, vintage)

**Statistics:**
- [ ] Numeric: n_total, n_missing, n_unique, mean, std, min, max
- [ ] Categorical: n_total, n_missing, n_unique, min, max, top_10
- [ ] Type detection threshold (>90% numeric → numeric, else categorical)

**Display Commands:**
- [ ] `list` shows all tables with metadata
- [ ] `show` displays row counts correctly
- [ ] `show-stats` displays column statistics correctly
- [ ] `show-stats` with column and date filters

**Edge Cases:**
- [ ] Invalid date formats show clear errors
- [ ] Empty CSV files handled gracefully
- [ ] Very large numbers formatted correctly
- [ ] Unicode column names work
- [ ] Duplicate dates in CSV aggregated correctly

### Test Data:
Use sample_data/ folder and create additional test CSVs as needed.

### Verification:
- [ ] All 44 automated tests passing: `pytest tests/ -v`
- [ ] Manual CLI tests completed
- [ ] Documentation accurate

---

## Issue 2: Implement load-map Command

**Title:** Implement load-map command for paired table loading with column mappings

**Labels:** feature, enhancement

**Description:**

Add `load-map` command to load table pairs from JSON config with column mappings stored in the database.

### Problem:
When comparing Oracle vs AWS tables, column names differ (e.g., `AMT` → `amount`). We need to:
1. Load both tables together as a "pair"
2. Store the column mapping relationship
3. Use this mapping later for comparisons

### Solution:

**New database table:**
```sql
CREATE TABLE _table_pairs (
    pair_name TEXT NOT NULL,
    table_left TEXT NOT NULL,
    table_right TEXT NOT NULL,
    source_left TEXT,
    source_right TEXT,
    col_mappings TEXT,  -- JSON: {"oracle_col": "aws_col"}
    created_at TEXT,
    PRIMARY KEY (pair_name)
);
```

**JSON Config Format:**
```json
{
  "pairs": [
    {
      "name": "customer_daily",
      "tables": {
        "oracle": {
          "file": "data/customer_daily_oracle.csv",
          "source": "oracle",
          "db": "prod_db",
          "table_name": "customer_daily_oracle",
          "date_col": "eff_dt",
          "vintage": "day"
        },
        "aws": {
          "file": "data/customer_daily_aws.csv",
          "source": "aws",
          "db": "analytics_db",
          "table_name": "customer_daily_aws",
          "date_col": "run_date",
          "vintage": "day"
        }
      },
      "col_map": {
        "AMT": "amount",
        "CUST_STATUS": "customer_status"
      }
    }
  ]
}
```

**Commands:**
```bash
# Load paired tables (row counts)
dtrack load-map myproject.db config.json --type row

# Load paired tables (column stats)
dtrack load-map myproject.db config.json --type col

# List all registered pairs
dtrack list-pairs myproject.db
```

### Implementation Tasks:
- [ ] Add `_table_pairs` table to schema
- [ ] Implement JSON config parsing
- [ ] Implement `load-map` command
- [ ] Load both tables from config
- [ ] Store pair relationship and column mappings
- [ ] Implement `list-pairs` command
- [ ] Add tests for load-map functionality
- [ ] Add example config files to sample_data/

### Acceptance Criteria:
- [ ] Can load table pairs from JSON config
- [ ] Column mappings stored in database
- [ ] `list-pairs` shows all registered pairs
- [ ] Both row and col data types supported
- [ ] Tests passing

---

## Issue 3: Implement compare-row and compare-col Commands

**Title:** Add comparison commands for row counts and column statistics

**Labels:** feature, enhancement

**Description:**

Implement commands to compare data between table pairs.

### Compare-Row Command

Compare row counts between two tables.

**Usage:**
```bash
# Compare by table names
dtrack compare-row myproject.db customer_daily_oracle customer_daily_aws

# Compare using registered pair name
dtrack compare-row myproject.db --pair customer_daily

# With date range filter
dtrack compare-row myproject.db --pair customer_daily \
  --from-date 2025-01-01 --to-date 2025-12-31
```

**Output:**
```
Comparing customer_daily_oracle vs customer_daily_aws
=================================================

oracle: 2022-01-01 to 2022-12-31 | 365 dates | total: 1,234,567
aws:    2022-01-01 to 2022-12-31 | 365 dates | total: 1,234,480

Only in oracle (0 dates):
  (none)

Only in aws (0 dates):
  (none)

Matching (364 dates):
  2022-01-01: 3,400
  2022-01-02: 3,380
  ... (362 more)

Mismatched (1 date):
  2022-03-03: oracle=94, aws=93, diff=-1

Summary: 0 only-left, 0 only-right, 364 match, 1 mismatch
```

### Compare-Col Command

Compare column statistics with automatic column mapping.

**Usage:**
```bash
# Compare using registered pair (uses stored column mapping)
dtrack compare-col myproject.db --pair customer_daily

# Compare with manual column mapping
dtrack compare-col myproject.db oracle_table aws_table \
  --col-map "AMT=amount,CUST_STATUS=customer_status"

# Compare specific columns only
dtrack compare-col myproject.db --pair customer_daily \
  --columns amount,status \
  --from-date 2025-01-01
```

**Column Mapping Resolution (priority order):**
1. `--col-map` parameter (manual override)
2. `--pair` parameter (lookup from `_table_pairs`)
3. Auto-detect pair by table names
4. Exact column name matching (no mapping)

**Output:**
```
Comparing column stats: oracle vs aws
Date range: 2025-01-01 to 2025-12-31
Column mapping: AMT → amount, CUST_STATUS → customer_status
======================================================================

Column: amount (numeric)
  oracle: AMT → aws: amount

                  oracle        aws           diff        % diff
n_total           1,200         1,180         -20         -1.7%
n_missing         5             12            +7          +140%
n_unique          342           338           -4          -1.2%
mean              1,523.45      1,519.20      -4.25       -0.3%
std               234.12        240.88        +6.76       +2.9%
min               10.00         10.00         0.00        0.0%
max               9,999.00      9,850.00      -149.00     -1.5%

Column: status (categorical)
  oracle: CUST_STATUS → aws: customer_status

                  oracle        aws           diff
n_total           1,200         1,180         -20
n_missing         0             3             +3
n_unique          4             4             0

Top 10 frequency comparison:
  ACTIVE          800 (66.7%)   785 (66.5%)   -15
  CLOSED          300 (25.0%)   295 (25.0%)   -5
  PENDING         80 (6.7%)     78 (6.6%)     -2
  ERROR           20 (1.7%)     22 (1.9%)     +2

Summary: 1/2 columns have significant differences (>5% change)
```

### Implementation Tasks:
- [ ] Implement `compare-row` command
- [ ] Implement `compare-col` command
- [ ] Column mapping resolution logic
- [ ] Side-by-side comparison display
- [ ] Percentage difference calculations
- [ ] Handle missing columns gracefully
- [ ] Date range filtering
- [ ] Summary statistics
- [ ] Add comparison tests
- [ ] Handle edge cases (missing dates, different column sets)

### Acceptance Criteria:
- [ ] Row count comparisons work correctly
- [ ] Column stats comparisons work correctly
- [ ] Column mapping from `_table_pairs` works
- [ ] Manual `--col-map` override works
- [ ] Clear output showing differences
- [ ] Date range filters work
- [ ] Tests passing

---

## Issue 4: Add HTML Export for Comparisons

**Title:** Implement HTML report generation for comparison results

**Labels:** feature, enhancement, html

**Description:**

Add HTML export functionality for comparison results based on design in `datatrack-html.md`.

### Commands:
```bash
# Export row comparison to HTML
dtrack compare-row myproject.db --pair customer_daily --html --output report.html

# Export column comparison to HTML
dtrack compare-col myproject.db --pair customer_daily --html --output report.html
```

### HTML Features:

**Row Count Tracker:**
- Summary bar (match/mismatch/only-left/only-right counts)
- Color-coded indicators (green=match, red=mismatch)
- Expandable details with `<details><summary>`
- Date coverage gap analysis
- Mismatched dates table
- Query time information

**Column Stats Tracker:**
- 3-level drill-down:
  1. Table summary (overall match/diff)
  2. Vintage breakdown (per-date summary)
  3. Diff detail (detailed stats for specific dates)
- Side-by-side comparison tables
- Percentage differences highlighted
- Color-coded (green/red/orange)
- Top 10 frequency changes for categorical

**Design Requirements:**
- Single self-contained HTML file
- Inline CSS (no external dependencies)
- Native `<details><summary>` for dropdowns (no JavaScript)
- Responsive table layout
- Works in all modern browsers

### Implementation Tasks:
- [ ] HTML template for row count comparison
- [ ] HTML template for column stats comparison
- [ ] CSS styling (color-coded indicators)
- [ ] Expandable sections
- [ ] Side-by-side comparison tables
- [ ] Percentage highlighting
- [ ] Metadata display (last updated, vintage)
- [ ] Add HTML generation tests
- [ ] Test in multiple browsers

### Acceptance Criteria:
- [ ] HTML export works for row comparisons
- [ ] HTML export works for column comparisons
- [ ] Self-contained single file
- [ ] Color coding works correctly
- [ ] Expandable sections work
- [ ] Readable formatting
- [ ] Tests passing

---

## Issue 5: Documentation and Examples

**Title:** Complete documentation with examples and usage guide

**Labels:** documentation

**Description:**

Create comprehensive documentation for all dtrack features.

### Documentation Tasks:
- [ ] Update README with:
  - Load-map examples
  - Compare commands examples
  - Column mapping examples
  - HTML export examples
  - JSON config format
- [ ] Add example JSON configs to sample_data/
- [ ] Add example comparison scenarios
- [ ] Add troubleshooting section
- [ ] Add FAQ section
- [ ] Add architecture diagram

### Example Scenarios:
1. **Basic workflow**: Load and compare two sources
2. **Batch workflow**: Load multiple table pairs from config
3. **Incremental refresh**: Update specific date ranges
4. **HTML reporting**: Generate comparison reports

### Sample Data:
- [ ] Add paired Oracle/AWS CSV examples
- [ ] Add example JSON configs
- [ ] Add example comparison outputs

### Acceptance Criteria:
- [ ] All commands documented with examples
- [ ] Example configs work correctly
- [ ] Common workflows documented
- [ ] Clear error messages documented
- [ ] Migration guide for existing users

---

## Implementation Order

1. **Issue 1** (Testing) - Ongoing, verify core functionality
2. **Issue 2** (Load-map) - Foundation for comparisons
3. **Issue 3** (Compare commands) - Core comparison logic
4. **Issue 4** (HTML export) - Visualization
5. **Issue 5** (Documentation) - Final polish

Each issue should be completed with tests before moving to the next.
