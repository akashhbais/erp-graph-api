from __future__ import annotations

import logging
import os
import re
import json
from pathlib import Path
from typing import List, Tuple

import httpx
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

ENV_PATH = Path(__file__).resolve().parents[3] / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=False)


class SQLGenerator:
    @staticmethod
    def _llm_config() -> tuple[str, str, str]:
        return (
            (os.getenv("LLM_API_KEY") or "").strip(),
            (os.getenv("LLM_MODEL") or "llama-3.1-8b-instant").strip(),
            (os.getenv("LLM_BASE_URL") or "https://api.groq.com/openai/v1").strip().rstrip("/"),
        )

    @staticmethod
    def _strip_sql(text: str) -> str:
        t = (text or "").replace("```sql", "").replace("```", "").strip()
        m = re.search(r"(?is)\bselect\b.*", t)
        return m.group(0).strip() if m else t

    @staticmethod
    def _ensure_select_limit(sql: str, max_rows: int = 200) -> str:
        s = SQLGenerator._strip_sql(sql)
        s = re.sub(r";+\s*$", "", s).strip()
        if not re.match(r"^\s*select\b", s, flags=re.IGNORECASE):
            raise RuntimeError("LLM returned non-SELECT SQL")
        if not re.search(r"\blimit\s+\d+\b", s, flags=re.IGNORECASE):
            s = f"{s} LIMIT {max_rows}"
        return s

    @staticmethod
    def _chat(prompt: str) -> str:
        api_key, model, base_url = SQLGenerator._llm_config()
        if not api_key:
            raise RuntimeError("LLM_API_KEY missing")

        with httpx.Client(timeout=45) as client:
            r = client.post(
                f"{base_url}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"model": model, "temperature": 0, "messages": [{"role": "user", "content": prompt}]},
            )
            logger.info("LLM status=%s", r.status_code)
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"]

    @staticmethod
    def _template_sql(question: str) -> str | None:
        q = question.lower().strip()

        # a) products associated with highest number of billing documents
        if "products" in q and "billing documents" in q and ("highest" in q or "most" in q):
            return """
SELECT p.product_id, p.product_name, COUNT(DISTINCT bi.billing_document_id) AS billing_doc_count
FROM billing_item bi
JOIN product p ON p.product_id = bi.product_id
GROUP BY p.product_id, p.product_name
ORDER BY billing_doc_count DESC
LIMIT 20
""".strip()

        # b) trace full flow for a billing document id
        m = re.search(r"\b(\d{6,})\b", q)
        if m and ("trace" in q or "full flow" in q or "sales order" in q) and "billing" in q:
            bid = m.group(1)
            return f"""
SELECT
  bd.billing_document_id,
  bi.item_no AS billing_item_no,
  bi.sales_order_id,
  bi.sales_order_item_no,
  bi.delivery_id,
  bi.delivery_item_no,
  je.journal_entry_id,
  je.posting_date
FROM billing_document bd
LEFT JOIN billing_item bi ON bi.billing_document_id = bd.billing_document_id
LEFT JOIN journal_entry je ON je.reference_billing_document_id = bd.billing_document_id
WHERE bd.billing_document_id = '{bid}'
ORDER BY bi.item_no, je.posting_date
LIMIT 200
""".strip()

        # c) broken / incomplete flows
        if ("broken" in q or "incomplete" in q or "delivered but not billed" in q or "billed without delivery" in q):
            return """
SELECT
  so.sales_order_id,
  CASE
    WHEN di.delivery_id IS NOT NULL AND bi.billing_document_id IS NULL THEN 'Delivered but not billed'
    WHEN bi.billing_document_id IS NOT NULL AND di.delivery_id IS NULL THEN 'Billed without delivery'
    ELSE 'Other'
  END AS flow_issue
FROM sales_order so
LEFT JOIN delivery_item di ON di.sales_order_id = so.sales_order_id
LEFT JOIN billing_item bi ON bi.sales_order_id = so.sales_order_id
WHERE (di.delivery_id IS NOT NULL AND bi.billing_document_id IS NULL)
   OR (bi.billing_document_id IS NOT NULL AND di.delivery_id IS NULL)
LIMIT 200
""".strip()

        # Strong template: product group by highest billing value (with comparison columns)
        if "product group" in q and ("highest billing value" in q or "generated the highest billing value" in q):
            return """
WITH grp_docs AS (
  SELECT p.product_group, bi.billing_document_id
  FROM billing_item bi
  JOIN product p ON p.product_id = bi.product_id
  GROUP BY p.product_group, bi.billing_document_id
)
SELECT
  gd.product_group,
  COUNT(*) AS billing_doc_count,
  SUM(bd.total_amount) AS total_billing_value
FROM grp_docs gd
JOIN billing_document bd ON bd.billing_document_id = gd.billing_document_id
GROUP BY gd.product_group
ORDER BY total_billing_value DESC
LIMIT 10
""".strip()

        return None

    @staticmethod
    def generate_llm_sql(question: str, schema_snapshot: str) -> str:
        prompt = f"""
You are a DuckDB SQL generator for ERP analytics.
Return ONLY SQL.

Rules:
- SELECT only
- Use only columns/tables from schema snapshot
- Do not invent keys
- Use explicit JOIN conditions
- Add LIMIT 200 if missing

Intent rules (important):
- If question asks "highest/top/most", return TOP 10 sorted DESC by the main metric
- Include at least:
  (a) business dimension column(s) (e.g., product_group/customer/plant)
  (b) main metric column with clear alias (e.g., total_billing_value, order_count)
  (c) one comparison/support metric when meaningful (e.g., billing_doc_count)
- Prefer human-readable aliases

Schema snapshot:
{schema_snapshot}

Question:
{question}
""".strip()
        return SQLGenerator._ensure_select_limit(SQLGenerator._chat(prompt), 200)

    @staticmethod
    def repair_sql(question: str, bad_sql: str, db_error: str, schema_snapshot: str) -> str:
        prompt = f"""
Repair the SQL for DuckDB.
Return ONLY corrected SQL.

Question:
{question}

Failed SQL:
{bad_sql}

Database error:
{db_error}

Schema snapshot:
{schema_snapshot}

Rules:
- Keep business intent
- SELECT only
- Use existing columns only
- Add LIMIT 200 if missing
""".strip()
        return SQLGenerator._ensure_select_limit(SQLGenerator._chat(prompt), 200)

    @staticmethod
    def generate_with_mode(question: str, schema_snapshot: str) -> Tuple[str, str]:
        if os.getenv("ENABLE_TEMPLATES", "1") == "1":
            t = SQLGenerator._template_sql(question)
            if t:
                return t, "template"
        return SQLGenerator.generate_llm_sql(question, schema_snapshot), "llm"

    @staticmethod
    def generate_fallback(question: str, schema_snapshot: str) -> str:
        q = question.lower()
        if "sales order" in q:
            return "SELECT customer_id, COUNT(*) AS order_count FROM sales_order GROUP BY customer_id ORDER BY order_count DESC LIMIT 20"
        if "billing" in q:
            return "SELECT billing_document_id, customer_id, billing_date, total_amount FROM billing_document ORDER BY billing_date DESC LIMIT 20"
        if "product" in q:
            return "SELECT product_id, product_name, product_group FROM product LIMIT 20"
        return "SELECT table_name FROM information_schema.tables WHERE table_schema='main' LIMIT 20"

    @staticmethod
    def summarize_answer(question: str, rows: list[dict], row_count: int, mode: str) -> str:
        """
        Data-grounded summary. Uses only returned rows.
        Falls back to deterministic summary if LLM fails.
        """
        if not rows:
            return f"No matching records found. (mode: {mode})"

        sample = rows[:10]
        prompt = f"""
You are a data analyst assistant.
Write a short, clear, human-readable answer (3-5 lines).
Use ONLY the provided result rows. Do not invent facts.

Question:
{question}

Mode:
{mode}

Row count:
{row_count}

Sample rows (JSON):
{json.dumps(sample, ensure_ascii=False)}

Output style:
- Direct answer first line
- 2-3 key insights
- Mention if this is sampled/top-N data
""".strip()

        try:
            text = SQLGenerator._chat(prompt).strip()
            if text:
                return text
        except Exception:
            pass

        # deterministic fallback summary
        top = sample[0]
        preview = ", ".join([f"{k}={top[k]}" for k in list(top.keys())[:3]])
        return f"Found {row_count} row(s). Top result: {preview}. (mode: {mode})"
