from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple

import httpx
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

ENV_PATH = Path(__file__).resolve().parents[3] / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=False)


class SQLGenerator:
    """
    Production-oriented NL->SQL generator:
    - mode: template | llm | llm_repair | fallback
    - schema-aware prompting
    - strict SELECT-only post-processing
    """

    @staticmethod
    def _llm_config() -> tuple[str, str, str]:
        api_key = (os.getenv("LLM_API_KEY") or "").strip()
        model = (os.getenv("LLM_MODEL") or "llama-3.1-8b-instant").strip()
        base_url = (os.getenv("LLM_BASE_URL") or "https://api.groq.com/openai/v1").strip().rstrip("/")
        logger.info("LLM config key_present=%s model=%s base_url=%s", bool(api_key), model, base_url)
        return api_key, model, base_url

    @staticmethod
    def _strip_code_fences(text: str) -> str:
        return (text or "").replace("```sql", "").replace("```", "").strip()

    @staticmethod
    def _ensure_select_and_limit(sql: str, max_rows: int = 200) -> str:
        s = SQLGenerator._strip_code_fences(sql)
        s = re.sub(r";+\s*$", "", s).strip()

        if not re.match(r"^\s*select\b", s, flags=re.IGNORECASE):
            raise ValueError("LLM returned non-SELECT SQL")

        if not re.search(r"\blimit\s+\d+\b", s, flags=re.IGNORECASE):
            s = f"{s} LIMIT {max_rows}"

        return s

    @staticmethod
    def _schema_tables(schema_snapshot: str) -> List[str]:
        out: List[str] = []
        for line in (schema_snapshot or "").splitlines():
            m = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(", line.strip())
            if m:
                out.append(m.group(1))
        return out

    @staticmethod
    def _has_table(schema_snapshot: str, table_name: str) -> bool:
        return table_name in set(SQLGenerator._schema_tables(schema_snapshot))

    @staticmethod
    def _normalize(q: str) -> str:
        q = q.lower().strip()
        repl = {
            "clients": "customers",
            "invoice": "billing document",
            "invoices": "billing documents",
            "revenue": "billing value",
            "biggest": "highest",
            "least": "lowest",
        }
        for k, v in repl.items():
            q = q.replace(k, v)
        return q

    @staticmethod
    def _template_sql(question: str, schema_snapshot: str) -> str | None:
        q = SQLGenerator._normalize(question)

        # customer with most sales orders
        if "customer" in q and "most sales orders" in q and SQLGenerator._has_table(schema_snapshot, "sales_order"):
            if SQLGenerator._has_table(schema_snapshot, "customer"):
                return """
SELECT so.customer_id, c.customer_name, COUNT(*) AS order_count
FROM sales_order so
LEFT JOIN customer c ON c.customer_id = so.customer_id
GROUP BY so.customer_id, c.customer_name
ORDER BY order_count DESC
LIMIT 10
""".strip()
            return """
SELECT customer_id, COUNT(*) AS order_count
FROM sales_order
GROUP BY customer_id
ORDER BY order_count DESC
LIMIT 10
""".strip()

        # highest billing by customer
        if "highest billing value" in q and "customer" in q and SQLGenerator._has_table(schema_snapshot, "billing_document"):
            if SQLGenerator._has_table(schema_snapshot, "customer"):
                return """
SELECT bd.customer_id, c.customer_name, SUM(bd.total_amount) AS total_billing
FROM billing_document bd
LEFT JOIN customer c ON c.customer_id = bd.customer_id
GROUP BY bd.customer_id, c.customer_name
ORDER BY total_billing DESC
LIMIT 10
""".strip()
            return """
SELECT customer_id, SUM(total_amount) AS total_billing
FROM billing_document
GROUP BY customer_id
ORDER BY total_billing DESC
LIMIT 10
""".strip()

        # products most frequent in billing docs
        if "product" in q and "billing documents" in q and ("most" in q or "frequent" in q):
            if SQLGenerator._has_table(schema_snapshot, "billing_item") and SQLGenerator._has_table(schema_snapshot, "product"):
                return """
SELECT p.product_id, p.product_name, COUNT(*) AS frequency
FROM billing_item bi
JOIN product p ON p.product_id = bi.product_id
GROUP BY p.product_id, p.product_name
ORDER BY frequency DESC
LIMIT 20
""".strip()

        # average billing amount per customer
        if "average billing amount per customer" in q and SQLGenerator._has_table(schema_snapshot, "billing_document"):
            return """
SELECT customer_id, AVG(total_amount) AS avg_billing_amount
FROM billing_document
GROUP BY customer_id
ORDER BY avg_billing_amount DESC
LIMIT 200
""".strip()

        # plant highest order volume
        if "plant" in q and "highest order volume" in q and SQLGenerator._has_table(schema_snapshot, "sales_order_item"):
            return """
SELECT plant_code, SUM(ordered_qty) AS total_order_volume
FROM sales_order_item
GROUP BY plant_code
ORDER BY total_order_volume DESC
LIMIT 10
""".strip()

        return None

    @staticmethod
    def _chat_completion(prompt: str) -> str:
        api_key, model, base_url = SQLGenerator._llm_config()
        if not api_key:
            raise RuntimeError("LLM_API_KEY missing")

        with httpx.Client(timeout=45) as client:
            resp = client.post(
                f"{base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "temperature": 0,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            logger.info("LLM status=%s", resp.status_code)
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]

    @staticmethod
    def generate_llm_sql(question: str, schema_snapshot: str) -> str:
        prompt = f"""
You are generating SQL for DuckDB over an ERP dataset.
Return ONLY SQL. No markdown. No explanation.

STRICT RULES:
1) SELECT only
2) Use ONLY table/column names that exist in this schema snapshot
3) Do not invent columns
4) Use explicit JOIN keys from existing columns only
5) Add LIMIT 200 if not present
6) If question is ambiguous, choose the safest valid interpretation

Schema snapshot:
{schema_snapshot}

Question:
{question}
""".strip()

        content = SQLGenerator._chat_completion(prompt)
        sql = SQLGenerator._ensure_select_and_limit(content, max_rows=200)
        logger.debug("LLM SQL=%s", sql)
        return sql

    @staticmethod
    def repair_sql(question: str, bad_sql: str, db_error: str, schema_snapshot: str) -> str:
        prompt = f"""
Fix this SQL for DuckDB using ONLY existing schema.
Return ONLY corrected SQL.

Question:
{question}

Bad SQL:
{bad_sql}

Error:
{db_error}

Schema snapshot:
{schema_snapshot}

Rules:
- SELECT only
- Keep intent same
- Remove invalid columns/tables
- Add LIMIT 200 if missing
""".strip()

        content = SQLGenerator._chat_completion(prompt)
        sql = SQLGenerator._ensure_select_and_limit(content, max_rows=200)
        logger.debug("Repair SQL=%s", sql)
        return sql

    @staticmethod
    def generate_with_mode(question: str, schema_snapshot: str) -> Tuple[str, str]:
        if os.getenv("ENABLE_TEMPLATES", "1") == "1":
            templ = SQLGenerator._template_sql(question, schema_snapshot)
            if templ:
                logger.info("Generation mode=template")
                return templ, "template"

        sql = SQLGenerator.generate_llm_sql(question, schema_snapshot)
        logger.info("Generation mode=llm")
        return sql, "llm"

    @staticmethod
    def generate_fallback(question: str, schema_snapshot: str) -> str:
        tables = set(SQLGenerator._schema_tables(schema_snapshot))
        q = SQLGenerator._normalize(question)

        if "sales order" in q and "sales_order" in tables:
            return "SELECT customer_id, COUNT(*) AS order_count FROM sales_order GROUP BY customer_id ORDER BY order_count DESC LIMIT 20"
        if "billing" in q and "billing_document" in tables:
            return "SELECT billing_document_id, customer_id, billing_date, total_amount FROM billing_document ORDER BY billing_date DESC LIMIT 20"
        if "product" in q and "product" in tables:
            return "SELECT * FROM product LIMIT 20"

        # guaranteed safe fallback
        return "SELECT table_name FROM information_schema.tables WHERE table_schema='main' LIMIT 20"
