from __future__ import annotations

import re
from typing import Tuple

from backend.app.core.config import settings


class SQLValidator:
    FORBIDDEN_KEYWORDS: list[str] = [
        "DROP",
        "DELETE",
        "UPDATE",
        "INSERT",
        "ALTER",
        "CREATE",
        "TRUNCATE",
        "EXEC",
        "EXECUTE",
    ]

    @staticmethod
    def validate(sql: str) -> Tuple[bool, str]:
        """Validate SQL query for safety."""
        sql_upper = sql.upper().strip()

        # 1. Must be SELECT
        if not sql_upper.startswith("SELECT"):
            return False, "Only SELECT queries allowed"

        # 2. No forbidden keywords
        for keyword in SQLValidator.FORBIDDEN_KEYWORDS:
            if keyword in sql_upper:
                return False, f"Keyword '{keyword}' not allowed"

        # 3. No SELECT *
        if "SELECT *" in sql_upper:
            return False, "SELECT * not allowed; specify columns explicitly"

        # 4. Extract table names (basic heuristic)
        from_match = re.findall(r'\bFROM\s+(\w+)', sql_upper)
        join_match = re.findall(r'\b(?:JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN)\s+(\w+)', sql_upper)
        tables = set(from_match + join_match)

        # 5. Only allowed tables (CASE-INSENSITIVE)
        allowed_tables_lower = {t.lower() for t in settings.ALLOWED_TABLES}
        tables_lower = {t.lower() for t in tables}
        invalid_tables = tables_lower - allowed_tables_lower
        
        if invalid_tables:
            return False, f"Access denied for tables: {', '.join(invalid_tables)}"

        # 6. No multiple statements
        if ";" in sql and sql.count(";") > 1:
            return False, "Multiple statements not allowed"

        return True, ""

    @staticmethod
    def enforce_limit(sql: str, max_rows: int = 200) -> str:
        """Enforce row limit if not already present."""
        if not sql or not sql.strip():
            return f"SELECT 1 LIMIT {max_rows}"

        s = sql.strip().replace("```sql", "").replace("```", "").strip()
        s = re.sub(r";+\s*$", "", s)  # remove trailing semicolons

        if re.search(r"\blimit\s+\d+\b", s, flags=re.IGNORECASE):
            return s

        return f"{s} LIMIT {max_rows}"