from __future__ import annotations

import logging
import os
from datetime import date, datetime
from decimal import Decimal
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
    def _repairable(err: str) -> bool:
        e = (err or "").lower()
        return any(k in e for k in ["binder error", "catalog error", "parser error", "does not have a column"])

    @staticmethod
    def _json_value(v: Any) -> Any:
        if isinstance(v, Decimal):
            return float(v)
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, bytes):
            return v.decode("utf-8", errors="replace")
        return v

    def _execute(self, sql: str) -> Dict[str, Any]:
        result = self.con.execute(sql).fetchall()
        cols = [d[0] for d in (self.con.description or [])]
        rows = [{c: self._json_value(v) for c, v in zip(cols, r)} for r in result]
        return {"rows": rows, "row_count": len(rows), "columns": cols}

    @staticmethod
    def _best_metric_key(rows: List[Dict[str, Any]]) -> str | None:
        if not rows:
            return None
        preferred = [
            "total_billing_value", "total_amount", "amount", "revenue",
            "order_count", "billing_doc_count", "count"
        ]
        keys = list(rows[0].keys())
        for p in preferred:
            for k in keys:
                if p in k.lower():
                    return k
        for k in keys:
            if isinstance(rows[0].get(k), (int, float)):
                return k
        return None

    @classmethod
    def _answer_text(cls, rows: List[Dict[str, Any]], mode: str) -> str:
        if not rows:
            return f"No matching records found. (mode: {mode})"

        metric = cls._best_metric_key(rows)
        top = rows[0]

        if metric and metric in top:
            # top + quick comparison with next rows
            leader = ", ".join([f"{k}={v}" for k, v in list(top.items())[:2]])
            compare_parts = []
            for r in rows[1:4]:
                name = next((str(v) for k, v in r.items() if k != metric), "item")
                compare_parts.append(f"{name}: {r.get(metric)}")
            compare = "; ".join(compare_parts) if compare_parts else "No comparison rows."
            return (
                f"Top result: {leader}, {metric}={top.get(metric)}.\n"
                f"Comparison: {compare}\n"
                f"(mode: {mode}, rows: {len(rows)})"
            )

        preview = ", ".join([f"{k}={v}" for k, v in list(top.items())[:3]])
        return f"Found {len(rows)} row(s). Top result: {preview}. (mode: {mode})"

    def execute_question(self, question: str) -> Dict[str, Any]:
        schema = self._schema_snapshot()
        mode_trace: List[str] = []
        generator_error = None

        # Generate
        try:
            sql, mode = SQLGenerator.generate_with_mode(question, schema)
            mode_trace.append(mode)
        except Exception as ex:
            generator_error = str(ex)
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
                    "answer_text": "I could not generate SQL for this question.",
                }
            sql = SQLGenerator.generate_fallback(question, schema)
            mode_trace.append("fallback")

        attempted_sql = sql
        last_error = None

        # Execute + repair
        for attempt in range(self.max_repair_attempts + 1):
            attempted_sql = SQLValidator.enforce_limit(attempted_sql, settings.MAX_QUERY_ROWS)

            ok, err = SQLValidator.validate(attempted_sql)
            if not ok:
                return {
                    "question": question,
                    "error": err,
                    "generated_sql": attempted_sql,
                    "mode": mode_trace[-1] if mode_trace else "unknown",
                    "mode_trace": mode_trace,
                    "generator_error": generator_error,
                    "row_count": 0,
                    "rows": [],
                    "answer_text": "Generated SQL failed safety validation.",
                }

            try:
                out = self._execute(attempted_sql)
                final_mode = mode_trace[-1] if mode_trace else "unknown"

                answer_text = SQLGenerator.summarize_answer(
                    question=question,
                    rows=out["rows"],
                    row_count=out["row_count"],
                    mode=final_mode,
                )

                return {
                    "question": question,
                    "generated_sql": attempted_sql,
                    "rows": out["rows"],
                    "columns": out["columns"],
                    "row_count": out["row_count"],
                    "mode": final_mode,
                    "mode_trace": mode_trace,
                    "generator_error": generator_error,
                    "answer_text": answer_text,
                }
            except Exception as ex:
                last_error = str(ex)
                logger.exception("Execution failed attempt=%s sql=%s", attempt + 1, attempted_sql)

                if attempt < self.max_repair_attempts and self._repairable(last_error):
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
                break

        # Final fallback execute
        if self.allow_fallback and (not mode_trace or mode_trace[-1] != "fallback"):
            try:
                fb = SQLGenerator.generate_fallback(question, schema)
                fb = SQLValidator.enforce_limit(fb, settings.MAX_QUERY_ROWS)
                ok, err = SQLValidator.validate(fb)
                if ok:
                    out = self._execute(fb)
                    mode_trace.append("fallback")
                    return {
                        "question": question,
                        "generated_sql": fb,
                        "rows": out["rows"],
                        "columns": out["columns"],
                        "row_count": out["row_count"],
                        "mode": "fallback",
                        "mode_trace": mode_trace,
                        "generator_error": generator_error,
                        "answer_text": self._answer_text(out["rows"], "fallback"),
                    }
                last_error = err or last_error
            except Exception as ex:
                last_error = str(ex)

        return {
            "question": question,
            "error": last_error or "SQL execution failed",
            "generated_sql": attempted_sql,
            "mode": mode_trace[-1] if mode_trace else "unknown",
            "mode_trace": mode_trace,
            "generator_error": generator_error,
            "row_count": 0,
            "rows": [],
            "answer_text": "I could not execute this query. Showing generated SQL for debugging.",
        }