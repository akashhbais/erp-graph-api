from __future__ import annotations

from typing import Any, Dict

from backend.app.core.config import settings
from backend.app.guardrails.sql_validator import SQLValidator
from backend.app.query.sql_generator import SQLGenerator


class QueryService:
    def __init__(self, db_connection: Any) -> None:
        self.con = db_connection

    def execute_question(self, question: str) -> Dict[str, Any]:
        mode = "unknown"
        generator_error = None

        # 1) Generate SQL (template/llm), fallback on error
        try:
            sql, mode = SQLGenerator.generate_with_mode(question)
        except Exception as ex:
            sql = SQLGenerator.generate_fallback(question)
            mode = "fallback"
            generator_error = str(ex)

        # 2) Enforce row limit
        sql = SQLValidator.enforce_limit(sql, settings.MAX_QUERY_ROWS)

        # 3) Validate SQL safety
        valid, error = SQLValidator.validate(sql)
        if not valid:
            return {
                "question": question,
                "error": error,
                "generated_sql": sql,
                "mode": mode,
                "generator_error": generator_error,
                "row_count": 0,
                "rows": [],
            }

        # 4) Execute
        try:
            result = self.con.execute(sql).fetchall()
            columns = [d[0] for d in (self.con.description or [])]
            rows = [dict(zip(columns, row)) for row in result]

            return {
                "question": question,
                "generated_sql": sql,
                "rows": rows,
                "row_count": len(rows),
                "mode": mode,
                "generator_error": generator_error,
            }
        except Exception as ex:
            return {
                "question": question,
                "error": str(ex),
                "generated_sql": sql,
                "mode": mode,
                "generator_error": generator_error,
                "row_count": 0,
                "rows": [],
            }