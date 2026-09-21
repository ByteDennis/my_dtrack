from __future__ import annotations

import os
from typing import Any

from .sql import SqlSource


class OracleSource(SqlSource):
    dialect = "oracle"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.user = config.get("user") or os.getenv(config.get("user_env", "ORACLE_USER"))
        self.password = config.get("password") or os.getenv(
            config.get("password_env", "ORACLE_PASSWORD")
        )
        self.dsn = config.get("dsn") or os.getenv(config.get("dsn_env", "ORACLE_DSN"))

    def connect(self):
        import oracledb

        if not all((self.user, self.password, self.dsn)):
            raise ValueError("Oracle requires user, password, and dsn")
        return oracledb.connect(user=self.user, password=self.password, dsn=self.dsn)
