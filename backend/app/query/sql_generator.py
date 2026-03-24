from __future__ import annotations

import os
import re
from typing import Tuple

import httpx


class SQLGenerator:
    SCHEMA_DESCRIPTION: str = """
    Database Schema:
    customer (customer_id, customer_name, segment, country_code, default_address_id)
    address (address_id, line1, city, state, postal_code, country_code)
    product (product_id, product_name, product_group, base_uom)
    sales_order (sales_order_id, customer_id, order_date, status, currency_code, sold_to_address_id, ship_to_address_id)
    sales_order_item (sales_order_id, item_no, product_id, ordered_qty, net_amount, plant_code)
    delivery (delivery_id, customer_id, delivery_date, status, ship_to_address_id)
    delivery_item (delivery_id, item_no, sales_order_id, sales_order_item_no, product_id, delivered_qty)
    billing_document (billing_document_id, customer_id, billing_date, billing_type, currency_code, total_amount)
    billing_item (billing_document_id, item_no, delivery_id, delivery_item_no, sales_order_id, sales_order_item_no, product_id, billed_qty, net_amount)
    journal_entry (journal_entry_id, company_code, fiscal_year, accounting_document, posting_date, document_date, transaction_currency, amount_in_transaction_currency, reference_billing_document_id)
    journal_entry_line (journal_entry_id, line_no, gl_account, debit_credit_indicator, amount_company_code_currency, company_code_currency, cost_center, profit_center, reference_document)
    """

    @staticmethod
    def _llm_config() -> tuple[str, str, str]:
        if os.getenv("FORCE_LOCAL_LLM", "0") == "1":
            # local-only testing mode
            api_key = os.getenv("LOCAL_LLM_API_KEY", "")
            model = os.getenv("LOCAL_LLM_MODEL", "llama-3.1-8b-instant")
            base_url = os.getenv("LOCAL_LLM_BASE_URL", "https://api.groq.com/openai/v1")
            return api_key, model, base_url

        return (
            os.getenv("LLM_API_KEY", ""),
            os.getenv("LLM_MODEL", "llama-3.1-8b-instant"),
            os.getenv("LLM_BASE_URL", "https://api.groq.com/openai/v1"),
        )

    @staticmethod
    def _normalize(q: str) -> str:
        q = q.lower().strip()
        replacements = {
            "clients": "customers",
            "invoice": "billing document",
            "invoices": "billing documents",
            "revenue": "billing value",
            "biggest": "highest",
            "least": "lowest",
        }
        for k, v in replacements.items():
            q = q.replace(k, v)
        return q

    @staticmethod
    def _template_sql(question: str) -> str | None:
        q = SQLGenerator._normalize(question)

        if "highest billing value" in q and "customer" in q:
            return """
            SELECT c.customer_id, c.customer_name, SUM(bd.total_amount) AS total_billing
            FROM customer c
            JOIN billing_document bd ON c.customer_id = bd.customer_id
            GROUP BY c.customer_id, c.customer_name
            ORDER BY total_billing DESC
            LIMIT 10
            """

        if "products" in q and "billing documents" in q and ("most frequent" in q or "most" in q):
            return """
            SELECT p.product_id, p.product_name, COUNT(*) AS frequency, SUM(bi.billed_qty) AS total_qty
            FROM product p
            JOIN billing_item bi ON p.product_id = bi.product_id
            GROUP BY p.product_id, p.product_name
            ORDER BY frequency DESC
            LIMIT 20
            """

        # Trace by billing document id
        m = re.search(r"\b(\d{6,})\b", q)
        if m and ("journal" in q or "accounting" in q):
            billing_id = m.group(1)
            return f"""
            SELECT je.journal_entry_id, je.company_code, je.fiscal_year, je.posting_date, je.amount_in_transaction_currency
            FROM journal_entry je
            WHERE je.reference_billing_document_id = '{billing_id}'
            ORDER BY je.posting_date DESC
            LIMIT 200
            """

        return None

    @staticmethod
    def generate_with_mode(question: str) -> Tuple[str, str]:
        templ = SQLGenerator._template_sql(question)
        if templ:
            return templ.strip(), "template"

        sql = SQLGenerator.generate_llm_sql(question)
        return sql.strip(), "llm"

    @staticmethod
    def generate(question: str) -> str:
        sql, _ = SQLGenerator.generate_with_mode(question)
        return sql

    @staticmethod
    def generate_fallback(question: str) -> str:
        q = SQLGenerator._normalize(question)

        if "customer" in q and ("highest" in q or "top" in q):
            return """
            SELECT c.customer_id, c.customer_name, SUM(bd.total_amount) AS total_billing
            FROM customer c
            JOIN billing_document bd ON c.customer_id = bd.customer_id
            GROUP BY c.customer_id, c.customer_name
            ORDER BY total_billing DESC
            LIMIT 10
            """.strip()

        if "product" in q:
            return """
            SELECT p.product_id, p.product_name, COUNT(*) AS frequency
            FROM product p
            JOIN billing_item bi ON p.product_id = bi.product_id
            GROUP BY p.product_id, p.product_name
            ORDER BY frequency DESC
            LIMIT 20
            """.strip()

        return "SELECT billing_document_id, customer_id, billing_date, total_amount FROM billing_document ORDER BY billing_date DESC LIMIT 20"

    @staticmethod
    def generate_llm_sql(question: str) -> str:
        api_key, model, base_url = SQLGenerator._llm_config()
        if not api_key:
            raise RuntimeError("LLM_API_KEY missing")

        prompt = f"""
You generate SQL for DuckDB.
Return ONLY SQL (no markdown, no explanation).

Rules:
- SELECT only
- Use only these tables:
  customer, address, product, sales_order, sales_order_item, delivery, delivery_item,
  billing_document, billing_item, journal_entry, journal_entry_line
- Prefer explicit JOIN conditions
- Add LIMIT 200 if missing

Question: {question}
        """.strip()

        with httpx.Client(timeout=40) as client:
            r = client.post(
                f"{base_url.rstrip('/')}/chat/completions",
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
            r.raise_for_status()
            text = r.json()["choices"][0]["message"]["content"].strip()

        text = text.replace("```sql", "").replace("```", "").strip()
        if not re.match(r"^\s*select\b", text, re.IGNORECASE):
            raise RuntimeError("LLM returned non-SELECT SQL")
        if " limit " not in text.lower():
            text = text.rstrip(";") + " LIMIT 200"
        return text