from __future__ import annotations

import logging
import os
from typing import Any, Dict, List

from backend.app.core.config import settings
from backend.app.guardrails.sql_validator import SQLValidator
from backend.app.query.sql_generator import SQLGenerator

logger = logging.getLogger(__name__)


class QueryService:
    def __init__(self, db_connection: Any) -> None:
        self.con = db_connection
        self.allow_fallback = os.getenv("QUERY_ALLOW_FALLBACK", "1") == "1"
        self.max_repair_attempts = int(os.getenv("QUERY_MAX_REPAIRS", "2"))

    def _schema_snapshot(self) -> str:
        rows = self.con.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema='main'
            ORDER BY table_name, ordinal_position
            """
        ).fetchall()

        grouped: Dict[str, List[str]] = {}
        for t, c in rows:
            grouped.setdefault(t, []).append(c)
        return "\n".join([f"{t}({', '.join(cols)})" for t, cols in grouped.items()])

    @staticmethod
    def _is_repairable_error(error_text: str) -> bool:
        e = (error_text or "").lower()
        return ("binder error" in e) or ("catalog error" in e) or ("parser error" in e)

    def _execute_sql(self, sql: str) -> Dict[str, Any]:
        result = self.con.execute(sql).fetchall()
        columns = [d[0] for d in (self.con.description or [])]
        rows = [dict(zip(columns, row)) for row in result]
        return {"rows": rows, "row_count": len(rows)}

    def execute_question(self, question: str) -> Dict[str, Any]:
        schema = self._schema_snapshot()
        mode_trace: List[str] = []
        generator_error = None

        logger.info("Query start question=%s", question)

        # 1) generate initial SQL
        try:
            sql, mode = SQLGenerator.generate_with_mode(question, schema)
            mode_trace.append(mode)
            logger.info("Generation success mode=%s sql=%s", mode, sql)
        except Exception as ex:
            generator_error = str(ex)
            logger.exception("Generation failed error=%s", generator_error)

            if not self.allow_fallback:
                return {
                    "question": question,
                    "error": "SQL generation failed",
                    "generated_sql": None,
                    "mode": "generation_error",
                    "mode_trace": mode_trace,
                    "generator_error": generator_error,
                    "row_count": 0,
                    "rows": [],
                }

            sql = SQLGenerator.generate_fallback(question, schema)
            mode = "fallback"
            mode_trace.append(mode)

        # 2) execute + repair loop
        attempted_sql = sql
        last_error = None

        for attempt in range(self.max_repair_attempts + 1):
            attempted_sql = SQLValidator.enforce_limit(attempted_sql, settings.MAX_QUERY_ROWS)

            valid, err = SQLValidator.validate(attempted_sql)
            if not valid:
                return {
                    "question": question,
                    "error": err,
                    "generated_sql": attempted_sql,
                    "mode": mode_trace[-1] if mode_trace else "unknown",
                    "mode_trace": mode_trace,
                    "generator_error": generator_error,
                    "row_count": 0,
                    "rows": [],
                }

            try:
                out = self._execute_sql(attempted_sql)
                final_mode = mode_trace[-1] if mode_trace else "unknown"
                logger.info("Query success mode=%s attempts=%s rows=%s", final_mode, attempt + 1, out["row_count"])
                return {
                    "question": question,
                    "generated_sql": attempted_sql,
                    "rows": out["rows"],
                    "row_count": out["row_count"],
                    "mode": final_mode,
                    "mode_trace": mode_trace,
                    "generator_error": generator_error,
                }
            except Exception as ex:
                last_error = str(ex)
                logger.exception("Execution failed attempt=%s sql=%s", attempt + 1, attempted_sql)

                can_repair = attempt < self.max_repair_attempts and self._is_repairable_error(last_error)
                if can_repair:
                    try:
                        attempted_sql = SQLGenerator.repair_sql(
                            question=question,
                            bad_sql=attempted_sql,
                            db_error=last_error,
                            schema_snapshot=schema,
                        )
                        mode_trace.append("llm_repair")
                        continue
                    except Exception as repair_ex:
                        generator_error = f"{generator_error or ''} | repair_failed={repair_ex}".strip(" |")
                        logger.exception("Repair generation failed")
                        break
                break

        # 3) optional fallback if not already fallback
        if self.allow_fallback and (not mode_trace or mode_trace[-1] != "fallback"):
            try:
                fallback_sql = SQLGenerator.generate_fallback(question, schema)
                fallback_sql = SQLValidator.enforce_limit(fallback_sql, settings.MAX_QUERY_ROWS)

                valid, err = SQLValidator.validate(fallback_sql)
                if valid:
                    out = self._execute_sql(fallback_sql)
                    mode_trace.append("fallback")
                    return {
                        "question": question,
                        "generated_sql": fallback_sql,
                        "rows": out["rows"],
                        "row_count": out["row_count"],
                        "mode": "fallback",
                        "mode_trace": mode_trace,
                        "generator_error": generator_error,
                    }
                last_error = err or last_error
            except Exception as ex:
                last_error = str(ex)

        return {
            "question": question,
            "error": last_error or "SQL execution failed",
            "generated_sql": attempted_sql,  # always return attempted query for UI display
            "mode": mode_trace[-1] if mode_trace else "unknown",
            "mode_trace": mode_trace,
            "generator_error": generator_error,
            "row_count": 0,
            "rows": [],
        }