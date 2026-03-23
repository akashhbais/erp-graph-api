from __future__ import annotations
import os
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
    def generate(question: str) -> str:
        """Generate SQL from natural language question (placeholder for LLM integration)."""
        # This is a simple fallback; integrate OpenAI/Claude here
        question_lower = question.lower()

        if "highest billing" in question_lower or "top customer" in question_lower:
            return """
            SELECT 
                c.customer_id,
                c.customer_name,
                SUM(bd.total_amount) as total_billing
            FROM customer c
            JOIN billing_document bd ON c.customer_id = bd.customer_id
            GROUP BY c.customer_id, c.customer_name
            ORDER BY total_billing DESC
            LIMIT 10
            """

        if "most frequent product" in question_lower or "products in billing" in question_lower:
            return """
            SELECT 
                p.product_id,
                p.product_name,
                COUNT(*) as frequency,
                SUM(bi.billed_qty) as total_qty
            FROM product p
            JOIN billing_item bi ON p.product_id = bi.product_id
            GROUP BY p.product_id, p.product_name
            ORDER BY frequency DESC
            LIMIT 20
            """

        # Default safe query
        return "SELECT COUNT(*) as total FROM billing_document"

    @staticmethod
    def generate_llm_sql(question: str) -> str:
        """Generate SQL from natural language question using an LLM."""
        api_key = os.getenv("LLM_API_KEY", "").strip()
        base_url = os.getenv("LLM_BASE_URL", "https://api.groq.com/openai/v1").rstrip("/")
        model = os.getenv("LLM_MODEL", "llama-3.1-8b-instant")

        if not api_key:
            raise RuntimeError("LLM_API_KEY missing")

        prompt = f"""
Return ONLY SQL for DuckDB.
Rules:
- SELECT only
- Use ERP tables only
- Add LIMIT 200 if missing
Question: {question}
""".strip()

        with httpx.Client(timeout=40) as client:
            r = client.post(
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
            r.raise_for_status()
            text = r.json()["choices"][0]["message"]["content"].strip()

        # remove markdown fences if model returns them
        text = text.replace("```sql", "").replace("```", "").strip()
        return text