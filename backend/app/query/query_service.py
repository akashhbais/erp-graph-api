from __future__ import annotations

from typing import Any, Dict

from backend.app.guardrails.sql_validator import SQLValidator
from backend.app.query.sql_generator import SQLGenerator
from backend.app.core.config import settings


class QueryService:
    def __init__(self, db_connection: Any) -> None:
        self.con = db_connection

    def execute_question(self, question: str) -> Dict[str, Any]:
        """End-to-end NL → SQL → result pipeline."""
        try:
            sql = SQLGenerator.generate(question)
            mode = "llm"
        except Exception as ex:
            sql = SQLGenerator.generate_fallback(question)
            mode = "fallback"
            generator_error = str(ex)

        # 2. Enforce limit
        sql = SQLValidator.enforce_limit(sql, settings.MAX_QUERY_ROWS)

        # 3. Validate
        valid, error = SQLValidator.validate(sql)
        if not valid:
            return {
                "question": question,
                "error": error,
                "generated_sql": sql,
            }

        # 4. Execute
        try:
            result = self.con.execute(sql).fetchall()
            columns = [desc[0] for desc in self.con.description]
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
            }