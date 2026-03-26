from __future__ import annotations

import re
from typing import Tuple


class SQLValidator:
    FORBIDDEN = (
        " drop ",
        " delete ",
        " truncate ",
        " alter ",
        " insert ",
        " update ",
        " create ",
        " merge ",
    )

    @staticmethod
    def enforce_limit(sql: str, max_rows: int) -> str:
        if not sql or not sql.strip():
            return f"SELECT 1 AS ok LIMIT {max_rows}"

        s = sql.strip().replace("```sql", "").replace("```", "").strip()
        s = re.sub(r";+\s*$", "", s)

        # Reject multi-statements; keep first statement only
        if ";" in s:
            s = s.split(";")[0].strip()

        if not re.search(r"\blimit\s+\d+\b", s, flags=re.IGNORECASE):
            s = f"{s} LIMIT {max_rows}"
        return s

    @staticmethod
    def validate(sql: str) -> Tuple[bool, str | None]:
        if not sql or not re.match(r"^\s*select\b", sql, flags=re.IGNORECASE):
            return False, "Only SELECT queries are allowed."

        low = f" {sql.lower()} "
        for kw in SQLValidator.FORBIDDEN:
            if kw in low:
                return False, f"Forbidden keyword detected: {kw.strip()}"

        # no union of non-select blocks, no comments used for bypass
        if "--" in sql or "/*" in sql or "*/" in sql:
            return False, "SQL comments are not allowed."

        return True, None