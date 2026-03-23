from __future__ import annotations
import argparse
import logging
from pathlib import Path
from typing import Dict
import duckdb
import pandas as pd

from .loader import load_raw_csvs
from .schema_sql import CANONICAL_DDL
from .edge_builder import refresh_graph_edges
from .model_builder import (
    build_customer, build_address, build_product, build_sales_order, build_sales_order_item,
    build_delivery, build_delivery_item, build_billing_document, build_billing_item,
    build_journal_entry, build_journal_entry_line
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

TABLE_COLUMNS: Dict[str, list[str]] = {
    "customer": ["customer_id","customer_name","segment","country_code","default_address_id"],
    "address": ["address_id","line1","city","state","postal_code","country_code"],
    "product": ["product_id","product_name","product_group","base_uom"],
    "sales_order": ["sales_order_id","customer_id","order_date","status","currency_code","sold_to_address_id","ship_to_address_id"],
    "sales_order_item": ["sales_order_id","item_no","product_id","ordered_qty","net_amount","plant_code"],
    "delivery": ["delivery_id","customer_id","delivery_date","status","ship_to_address_id"],
    "delivery_item": ["delivery_id","item_no","sales_order_id","sales_order_item_no","product_id","delivered_qty"],
    "billing_document": ["billing_document_id","customer_id","billing_date","billing_type","currency_code","total_amount"],
    "billing_item": ["billing_document_id","item_no","delivery_id","delivery_item_no","sales_order_id","sales_order_item_no","product_id","billed_qty","net_amount"],
    "journal_entry": ["journal_entry_id","company_code","fiscal_year","accounting_document","posting_date","document_date","transaction_currency","amount_in_transaction_currency","reference_billing_document_id"],
    "journal_entry_line": ["journal_entry_id","line_no","gl_account","debit_credit_indicator","amount_company_code_currency","company_code_currency","cost_center","profit_center","reference_document"],
}

def _load_table(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> None:
    cols = TABLE_COLUMNS[table]
    if df.empty:
        logger.warning("No rows for %s", table)
        return
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[cols]
    con.register("_tmp_df", df)
    con.execute(f"INSERT INTO {table} ({','.join(cols)}) SELECT {','.join(cols)} FROM _tmp_df")
    con.unregister("_tmp_df")
    logger.info("Inserted %s rows into %s", len(df), table)

def run_ingestion(raw_dir: Path, db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))

    con.execute(CANONICAL_DDL)
    raw = load_raw_csvs(con, raw_dir)

    for t in TABLE_COLUMNS:
        con.execute(f"DELETE FROM {t}")

    _load_table(con, "customer", build_customer(raw))
    _load_table(con, "address", build_address(raw))
    _load_table(con, "product", build_product(raw))
    _load_table(con, "sales_order", build_sales_order(raw))
    _load_table(con, "sales_order_item", build_sales_order_item(raw))
    _load_table(con, "delivery", build_delivery(raw))
    _load_table(con, "delivery_item", build_delivery_item(raw))
    _load_table(con, "billing_document", build_billing_document(raw))
    _load_table(con, "billing_item", build_billing_item(raw))
    _load_table(con, "journal_entry", build_journal_entry(raw))
    _load_table(con, "journal_entry_line", build_journal_entry_line(raw))

    refresh_graph_edges(con)
    con.close()
    logger.info("Ingestion complete")

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--db-path", type=Path, default=Path("data/duckdb/app.duckdb"))
    args = parser.parse_args()
    run_ingestion(args.raw_dir, args.db_path)

if __name__ == "__main__":
    main()
