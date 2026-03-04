# dtrack Implementation Plan

## Problem Statement

When comparing tables from different sources (Oracle vs AWS), we need to:
1. Know which columns map to each other (e.g., `AMT` → `amount`)
2. Identify which table belongs to which source
3. Store this mapping relationship for later comparisons

**The Challenge:** When running `dtrack compare-col myproject.db oracle_table aws_table`, how do we know which columns belong to Oracle and which to AWS?

## Solution: Load-Map with Pairs Configuration

### JSON Config Format

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
        "CUST_STATUS": "customer_status",
        "EFF_DT": "eff_date"
      }
    }
  ]
}
```

### New Database Schema

Add `_table_pairs` table to store pair relationships and column mappings:

```sql
CREATE TABLE _table_pairs (
    pair_name TEXT NOT NULL,
    table_left TEXT NOT NULL,
    table_right TEXT NOT NULL,
    source_left TEXT,
    source_right TEXT,
    col_mappings TEXT,  -- JSON: {"oracle_col": "aws_col", ...}
    created_at TEXT,
    PRIMARY KEY (pair_name)
);
```

### New Commands

#### 1. Load-Map Command
```bash
dtrack load-map <project.db> <config.json>
```

**What it does:**
- Reads JSON config with table pairs
- Loads row counts OR column stats from both CSV files
- Stores the pair relationship and column mappings in `_table_pairs`
- Updates `_metadata` for each table

**Example:**
```bash
dtrack load-map myproject.db config.json --type row
dtrack load-map myproject.db config.json --type col
```

#### 2. Compare-Row Command
```bash
dtrack compare-row <project.db> <tableA> <tableB>
dtrack compare-row <project.db> --pair <pair_name>
```

**What it does:**
- Compares row counts between two tables
- If `--pair` used, looks up tables from `_table_pairs`
- Shows: only-left, only-right, matching, mismatched dates

#### 3. Compare-Col Command
```bash
dtrack compare-col <project.db> <tableA> <tableB> [--col-map "A=B,C=D"]
dtrack compare-col <project.db> --pair <pair_name>
```

**What it does:**
- Compares column statistics between two tables
- If `--pair` used, looks up column mappings from `_table_pairs`
- If `--col-map` provided, uses that instead
- Shows side-by-side comparison of all stats

#### 4. List-Pairs Command
```bash
dtrack list-pairs <project.db>
```

**What it does:**
- Lists all registered table pairs
- Shows pair name, left/right tables, sources
- Shows column mappings

### Workflow Example

```bash
# 1. Create config file
cat > config.json << 'EOF'
{
  "pairs": [
    {
      "name": "customer_daily",
      "tables": {
        "oracle": {
          "file": "data/customer_daily_oracle.csv",
          "source": "oracle",
          "table_name": "customer_daily_oracle",
          "date_col": "eff_dt"
        },
        "aws": {
          "file": "data/customer_daily_aws.csv",
          "source": "aws",
          "table_name": "customer_daily_aws",
          "date_col": "run_date"
        }
      },
      "col_map": {
        "AMT": "amount",
        "CUST_STATUS": "customer_status"
      }
    }
  ]
}
EOF

# 2. Load both tables and their mapping
dtrack load-map myproject.db config.json --type row

# 3. List pairs to verify
dtrack list-pairs myproject.db

# 4. Compare using pair name (mapping is automatic)
dtrack compare-row myproject.db --pair customer_daily
dtrack compare-col myproject.db --pair customer_daily

# OR compare directly with manual col-map
dtrack compare-col myproject.db oracle_table aws_table \
  --col-map "AMT=amount,CUST_STATUS=customer_status"
```

## Consolidated GitHub Issues

### Issue 1: Core Testing (Consolidate all basic tests)
**Title:** Test core functionality: init, load-row, load-col, show commands

**Covers:**
- Database initialization
- Date format parsing (ISO, SAS, YYYYMM, US)
- Vintage bucketing (day/week/month/quarter/year)
- Load modes (replace/append/upsert)
- Column type detection (numeric vs categorical)
- Statistics computation
- Missing value handling
- Metadata tracking
- CLI display commands (list, show, show-stats)

### Issue 2: Load-Map Implementation
**Title:** Implement load-map command for paired table loading with column mappings

**Features:**
- JSON config parsing for table pairs
- `_table_pairs` database table
- Load both tables from config
- Store column mappings
- Support for both row and col data types
- `list-pairs` command to view registered pairs

### Issue 3: Compare Commands Implementation
**Title:** Implement compare-row and compare-col commands

**Features:**
- `compare-row`: Compare row counts between tables
  - Show only-left, only-right, match, mismatch
  - Date range filters
  - Summary statistics
- `compare-col`: Compare column statistics
  - Use stored mappings from `_table_pairs` OR `--col-map` override
  - Side-by-side comparison for all stats
  - Percentage differences for numeric stats
  - Frequency comparison for categorical stats
  - Handle missing columns gracefully

### Issue 4: HTML Export
**Title:** Add HTML export for comparison results

**Features:**
- `--html` flag for compare commands
- Row Count Tracker HTML (expandable details, color-coded)
- Column Stats Tracker HTML (3-level drill-down)
- Self-contained single file
- Based on design in datatrack-html.md

### Issue 5: Advanced Features (Future)
**Title:** Batch operations and additional enhancements

**Features:**
- Batch comparisons for multiple pairs
- Export to CSV
- Comparison thresholds and alerts
- Performance optimization for large datasets

## Implementation Priority

1. ✅ **Phase 0**: Core functionality (DONE)
   - init, load-row, load-col, list, show, show-stats
   - 44 tests passing

2. **Phase 1**: Comparison foundation (Issue 2 & 3)
   - Add `_table_pairs` table to schema
   - Implement `load-map` command
   - Implement `compare-row` command
   - Implement `compare-col` command with column mapping
   - Implement `list-pairs` command

3. **Phase 2**: HTML export (Issue 4)
   - HTML templates for row comparison
   - HTML templates for col comparison
   - Expandable sections and color coding

4. **Phase 3**: Advanced features (Issue 5)
   - Batch operations
   - Additional export formats
   - Performance optimizations

## Technical Design Notes

### Column Mapping Resolution Order
When running `compare-col`:
1. Check if `--col-map` parameter provided → use it
2. Check if `--pair` parameter provided → lookup from `_table_pairs`
3. Check if tableA and tableB exist in any pair → use that mapping
4. Fall back to exact column name matching

### Handling Column Mapping in Code
```python
def get_column_mapping(db_path, tableA, tableB, col_map_override=None, pair_name=None):
    """
    Get column mapping between two tables.

    Priority:
    1. col_map_override (from --col-map)
    2. pair_name lookup in _table_pairs
    3. Auto-detect from _table_pairs where tableA and tableB match
    4. Exact name matching (no mapping)
    """
    if col_map_override:
        return parse_col_map(col_map_override)

    if pair_name:
        return get_mapping_from_pair(db_path, pair_name)

    # Try to find pair containing these tables
    pair = find_pair_by_tables(db_path, tableA, tableB)
    if pair:
        return pair['col_mappings']

    # Fall back to exact matching
    return {}  # Empty dict means: match by exact column names
```

### JSON Schema Validation
Add validation for config.json:
- Required fields: name, tables (with oracle/aws/pcds keys)
- Optional fields: col_map, vintage, mode
- Date column required for column stats
- File paths must exist

## Testing Strategy

### Unit Tests
- JSON config parsing
- Column mapping resolution logic
- Comparison logic (row counts, stats)
- HTML generation

### Integration Tests
- Load-map full workflow
- Compare with stored mappings
- Compare with override mappings
- HTML export end-to-end

### Edge Cases
- Column exists in oracle but not aws
- Different data types between sources
- Missing dates in one source
- Empty col_map (exact name matching)
- Case sensitivity in column names
