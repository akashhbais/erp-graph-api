from __future__ import annotations

import os
from pathlib import Path


class Settings:
    APP_NAME: str = "ERP Graph API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"

    DB_PATH: Path = Path(os.getenv("APP_DB_PATH", "data/duckdb/app.duckdb"))
    LLM_API_KEY: str = os.getenv("LLM_API_KEY", "")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gpt-4-turbo")

    MAX_NEIGHBORS: int = 50
    MAX_QUERY_ROWS: int = 200

    ALLOWED_TABLES: set[str] = {
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
    }


settings = Settings()