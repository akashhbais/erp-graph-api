from __future__ import annotations

import duckdb
from pathlib import Path
from typing import Optional


class DatabaseManager:
    _instance: Optional[DatabaseManager] = None
    _connection: Optional[duckdb.DuckDBPyConnection] = None

    def __new__(cls, db_path: Path) -> DatabaseManager:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.db_path = db_path
        return cls._instance

    def connect(self) -> duckdb.DuckDBPyConnection:
        if self._connection is None:
            self._connection = duckdb.connect(str(self.db_path))
        return self._connection

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None

    def is_connected(self) -> bool:
        try:
            if self._connection:
                self._connection.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False


def get_db() -> duckdb.DuckDBPyConnection:
    from backend.app.core.config import settings
    manager = DatabaseManager(settings.DB_PATH)
    return manager.connect()