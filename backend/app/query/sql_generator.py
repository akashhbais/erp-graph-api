from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Tuple

import httpx
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load .env explicitly
ENV_PATH = Path(__file__).resolve().parents[3] / ".env"  # c:\GitHub\Task\.env
load_dotenv(dotenv_path=ENV_PATH, override=False)


class SQLGenerator:
    """
    LLM-first SQL generator.
    - No automatic fallback here.
    - Returns (sql, mode) for observability.
    """

    ALLOWED_TABLES = [
        "customer",
        "address",
        "product",
        "sales_order",
        "sales_order_item",
        "delivery",
        "delivery_item",
        "billing_document",
        "billing_item",
        "journal_entry",
        "journal_entry_line",
    ]

    @staticmethod
    def _llm_config() -> tuple[str, str, str]:
        api_key = (os.getenv("LLM_API_KEY") or "").strip()
        model = (os.getenv("LLM_MODEL") or "llama-3.1-8b-instant").strip()
        base_url = (os.getenv("LLM_BASE_URL") or "https://api.groq.com/openai/v1").strip().rstrip("/")

        logger.info(
            "LLM config loaded key_present=%s model=%s base_url=%s",
            bool(api_key),
            model,
            base_url,
        )
        return api_key, model, base_url

    @staticmethod
    def _strip_code_fences(text: str) -> str:
        return text.replace("```sql", "").replace("```", "").strip()

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
    def _template_sql(question: str) -> str | None:
        q = question.lower().strip()

        if "highest billing value" in q and "customer" in q:
            return """
SELECT c.customer_id, c.customer_name, SUM(bd.total_amount) AS total_billing
FROM customer c
JOIN billing_document bd ON bd.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY total_billing DESC
LIMIT 10
""".strip()

        if "products" in q and "billing documents" in q and ("most frequent" in q or "most" in q):
            return """
SELECT p.product_id, p.product_name, COUNT(*) AS billing_doc_count
FROM product p
JOIN billing_item bi ON bi.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY billing_doc_count DESC
LIMIT 20
""".strip()

        return None

    @staticmethod
    def generate_with_mode(question: str) -> Tuple[str, str]:
        """
        Mode priority:
        1) optional templates (if ENABLE_TEMPLATES=1)
        2) LLM
        No fallback here.
        """
        if os.getenv("ENABLE_TEMPLATES", "0") == "1":
            templ = SQLGenerator._template_sql(question)
            if templ:
                logger.info("SQL generation mode=template")
                return templ, "template"

        sql = SQLGenerator.generate_llm_sql(question)
        logger.info("SQL generation mode=llm")
        return sql, "llm"

    @staticmethod
    def generate(question: str) -> str:
        sql, _ = SQLGenerator.generate_with_mode(question)
        return sql

    @staticmethod
    def generate_fallback(question: str) -> str:
        # Used only if QueryService explicitly allows fallback.
        return """
SELECT billing_document_id, customer_id, billing_date, total_amount
FROM billing_document
ORDER BY billing_date DESC
LIMIT 20
""".strip()

    @staticmethod
    def generate_llm_sql(question: str) -> str:
        api_key, model, base_url = SQLGenerator._llm_config()
        if not api_key:
            raise RuntimeError("LLM_API_KEY missing")

        prompt = f"""
You are an ERP SQL generator for DuckDB.
Return ONLY SQL (no markdown, no explanation).

Rules:
- SELECT only
- Use only these tables: {", ".join(SQLGenerator.ALLOWED_TABLES)}
- Use explicit JOIN conditions
- Add LIMIT 200 if not present

Question: {question}
""".strip()

        logger.info(
            "LLM request start provider=%s model=%s key_present=%s",
            "groq/openai-compatible",
            model,
            bool(api_key),
        )

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
            logger.info("LLM response status=%s", resp.status_code)
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]

        sql = SQLGenerator._ensure_select_and_limit(content, max_rows=200)
        logger.debug("Generated SQL: %s", sql)
        return sql