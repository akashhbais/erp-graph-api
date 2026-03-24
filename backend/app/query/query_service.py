from __future__ import annotations

import logging
import os
from typing import Any, Dict

from backend.app.core.config import settings
from backend.app.guardrails.sql_validator import SQLValidator
from backend.app.query.sql_generator import SQLGenerator

logger = logging.getLogger(__name__)


class QueryService:
    def __init__(self, db_connection: Any) -> None:
        self.con = db_connection
        self.allow_fallback = os.getenv("QUERY_ALLOW_FALLBACK", "0") == "1"

    def execute_question(self, question: str) -> Dict[str, Any]:
        mode = "generation_error"
        generator_error = None

        logger.info("Query start question=%s", question)

        # 1) Generate SQL (LLM/template). No silent fallback by default.
        try:
            sql, mode = SQLGenerator.generate_with_mode(question)
            logger.info("Generation success mode=%s sql=%s", mode, sql)
        except Exception as ex:
            generator_error = str(ex)
            logger.exception("Generation failed mode=llm/template error=%s", generator_error)

            if self.allow_fallback:
                sql = SQLGenerator.generate_fallback(question)
                mode = "fallback"
                logger.warning("Using fallback SQL because QUERY_ALLOW_FALLBACK=1")
            else:
                return {
                    "question": question,
                    "error": "SQL generation failed",
                    "generated_sql": None,
                    "mode": mode,
                    "generator_error": generator_error,
                    "row_count": 0,
                    "rows": [],
                }

        # 2) Enforce row limit safely
        sql = SQLValidator.enforce_limit(sql, settings.MAX_QUERY_ROWS)

        # 3) Validate SQL safety
        valid, error = SQLValidator.validate(sql)
        if not valid:
            logger.warning("SQL validation failed error=%s sql=%s", error, sql)
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

            logger.info("Query success mode=%s row_count=%s", mode, len(rows))
            return {
                "question": question,
                "generated_sql": sql,
                "rows": rows,
                "row_count": len(rows),
                "mode": mode,
                "generator_error": generator_error,
            }
        except Exception as ex:
            logger.exception("SQL execution failed sql=%s", sql)
            return {
                "question": question,
                "error": str(ex),
                "generated_sql": sql,
                "mode": mode,
                "generator_error": generator_error,
                "row_count": 0,
                "rows": [],
            }