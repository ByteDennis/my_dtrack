"""SAS dataset platform builder for local SAS datasets ($ prefix)."""

import os
from .base import (
    PlatformBuilder, qualified_name, sas_safe_name, is_numeric_type,
    build_stats_sql, build_top10_sql,
)


class SASBuilder(PlatformBuilder):
    """Builder for local SAS datasets ($ prefix in processed field)."""

    def __init__(self, tbl_cfg, db_path=None):
        super().__init__(tbl_cfg, db_path)
        processed = tbl_cfg.get('processed', '')
        if isinstance(processed, list):
            processed = " ".join(processed)
        self.sas_dataset = processed[1:].strip() if processed.startswith('$') else processed

    def build_row_sql(self, date_filter):
        """Build SAS proc sql for row counts."""
        date_expr = self.date_col
        where = self.tbl_cfg.get('where', '')
        where_clause = f"WHERE {where}" if where else ""
        return f"SELECT {date_expr} AS date_value, COUNT(*) AS row_count FROM {self.sas_dataset} {where_clause} GROUP BY {date_expr}"

    def build_continuous_sql(self, col, col_type, where):
        return build_stats_sql(self.sas_dataset, col, self.date_col, where, "numeric", "oracle")

    def build_categorical_sql(self, col, col_type, where, top_n=10):
        return build_stats_sql(self.sas_dataset, col, self.date_col, where, "categorical", "oracle")

    def generate_extraction(self, outdir, extract_type, **kw):
        """SAS datasets are extracted via gen_sas in OracleBuilder (they share the SAS file).

        This is a no-op - SAS dataset extraction is handled as part of oracle.gen_sas().
        """
        return []
