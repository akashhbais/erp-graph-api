CANONICAL_DDL = """
CREATE TABLE IF NOT EXISTS customer (
    customer_id VARCHAR PRIMARY KEY,
    customer_name VARCHAR,
    segment VARCHAR,
    country_code VARCHAR,
    default_address_id VARCHAR
);
CREATE TABLE IF NOT EXISTS address (
    address_id VARCHAR PRIMARY KEY,
    line1 VARCHAR, city VARCHAR, state VARCHAR, postal_code VARCHAR, country_code VARCHAR
);
CREATE TABLE IF NOT EXISTS product (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR, product_group VARCHAR, base_uom VARCHAR
);
CREATE TABLE IF NOT EXISTS sales_order (
    sales_order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR, order_date DATE, status VARCHAR, currency_code VARCHAR,
    sold_to_address_id VARCHAR, ship_to_address_id VARCHAR
);
CREATE TABLE IF NOT EXISTS sales_order_item (
    sales_order_id VARCHAR, item_no INTEGER, product_id VARCHAR,
    ordered_qty DECIMAL(18,3), net_amount DECIMAL(18,2), plant_code VARCHAR,
    PRIMARY KEY (sales_order_id, item_no)
);
CREATE TABLE IF NOT EXISTS delivery (
    delivery_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR, delivery_date DATE, status VARCHAR, ship_to_address_id VARCHAR
);
CREATE TABLE IF NOT EXISTS delivery_item (
    delivery_id VARCHAR, item_no INTEGER, sales_order_id VARCHAR,
    sales_order_item_no INTEGER, product_id VARCHAR, delivered_qty DECIMAL(18,3),
    PRIMARY KEY (delivery_id, item_no)
);
CREATE TABLE IF NOT EXISTS billing_document (
    billing_document_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR, billing_date DATE, billing_type VARCHAR, currency_code VARCHAR,
    total_amount DECIMAL(18,2)
);
CREATE TABLE IF NOT EXISTS billing_item (
    billing_document_id VARCHAR, item_no INTEGER, delivery_id VARCHAR, delivery_item_no INTEGER,
    sales_order_id VARCHAR, sales_order_item_no INTEGER, product_id VARCHAR,
    billed_qty DECIMAL(18,3), net_amount DECIMAL(18,2),
    PRIMARY KEY (billing_document_id, item_no)
);
CREATE TABLE IF NOT EXISTS journal_entry (
    journal_entry_id VARCHAR PRIMARY KEY,
    company_code VARCHAR, fiscal_year INTEGER, accounting_document VARCHAR,
    posting_date TIMESTAMP, document_date TIMESTAMP, transaction_currency VARCHAR,
    amount_in_transaction_currency DECIMAL(18,2), reference_billing_document_id VARCHAR
);
CREATE TABLE IF NOT EXISTS journal_entry_line (
    journal_entry_id VARCHAR, line_no INTEGER, gl_account VARCHAR, debit_credit_indicator VARCHAR,
    amount_company_code_currency DECIMAL(18,2), company_code_currency VARCHAR,
    cost_center VARCHAR, profit_center VARCHAR, reference_document VARCHAR,
    PRIMARY KEY (journal_entry_id, line_no)
);
CREATE TABLE IF NOT EXISTS graph_edge (
    edge_id VARCHAR PRIMARY KEY, edge_type VARCHAR NOT NULL,
    from_type VARCHAR NOT NULL, from_id VARCHAR NOT NULL,
    to_type VARCHAR NOT NULL, to_id VARCHAR NOT NULL,
    source_table VARCHAR, metadata_json VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

EDGE_REFRESH_SQL = """
DELETE FROM graph_edge;

INSERT INTO graph_edge
SELECT DISTINCT
  'CUSTOMER_PLACES_ORDER|' || customer_id || '|' || sales_order_id,
  'CUSTOMER_PLACES_ORDER', 'customer', customer_id, 'sales_order', sales_order_id,
  'sales_order', NULL, CURRENT_TIMESTAMP
FROM sales_order
WHERE customer_id IS NOT NULL AND sales_order_id IS NOT NULL

UNION ALL
SELECT DISTINCT
  'ORDER_HAS_ITEM|' || sales_order_id || '|' || CAST(item_no AS VARCHAR),
  'ORDER_HAS_ITEM', 'sales_order', sales_order_id, 'sales_order_item', sales_order_id || ':' || CAST(item_no AS VARCHAR),
  'sales_order_item', NULL, CURRENT_TIMESTAMP
FROM sales_order_item
WHERE sales_order_id IS NOT NULL AND item_no IS NOT NULL

UNION ALL
SELECT DISTINCT
  'ORDER_ITEM_FOR_PRODUCT|' || sales_order_id || ':' || CAST(item_no AS VARCHAR) || '|' || product_id,
  'ORDER_ITEM_FOR_PRODUCT', 'sales_order_item', sales_order_id || ':' || CAST(item_no AS VARCHAR), 'product', product_id,
  'sales_order_item', NULL, CURRENT_TIMESTAMP
FROM sales_order_item
WHERE sales_order_id IS NOT NULL AND item_no IS NOT NULL AND product_id IS NOT NULL

UNION ALL
SELECT DISTINCT
  'DELIVERY_HAS_ITEM|' || delivery_id || '|' || CAST(item_no AS VARCHAR),
  'DELIVERY_HAS_ITEM', 'delivery', delivery_id, 'delivery_item', delivery_id || ':' || CAST(item_no AS VARCHAR),
  'delivery_item', NULL, CURRENT_TIMESTAMP
FROM delivery_item
WHERE delivery_id IS NOT NULL AND item_no IS NOT NULL

UNION ALL
SELECT DISTINCT
  'ORDER_ITEM_FULFILLED_BY_DELIVERY_ITEM|' || sales_order_id || ':' || CAST(sales_order_item_no AS VARCHAR) || '|' || delivery_id || ':' || CAST(item_no AS VARCHAR),
  'ORDER_ITEM_FULFILLED_BY_DELIVERY_ITEM', 'sales_order_item', sales_order_id || ':' || CAST(sales_order_item_no AS VARCHAR),
  'delivery_item', delivery_id || ':' || CAST(item_no AS VARCHAR),
  'delivery_item', NULL, CURRENT_TIMESTAMP
FROM delivery_item
WHERE sales_order_id IS NOT NULL AND sales_order_item_no IS NOT NULL
  AND delivery_id IS NOT NULL AND item_no IS NOT NULL

UNION ALL
SELECT DISTINCT
  'BILLING_DOC_HAS_ITEM|' || billing_document_id || '|' || CAST(item_no AS VARCHAR),
  'BILLING_DOC_HAS_ITEM', 'billing_document', billing_document_id, 'billing_item', billing_document_id || ':' || CAST(item_no AS VARCHAR),
  'billing_item', NULL, CURRENT_TIMESTAMP
FROM billing_item
WHERE billing_document_id IS NOT NULL AND item_no IS NOT NULL

UNION ALL
SELECT DISTINCT
  'DELIVERY_ITEM_BILLED_BY_BILLING_ITEM|' || delivery_id || ':' || CAST(delivery_item_no AS VARCHAR) || '|' || billing_document_id || ':' || CAST(item_no AS VARCHAR),
  'DELIVERY_ITEM_BILLED_BY_BILLING_ITEM', 'delivery_item', delivery_id || ':' || CAST(delivery_item_no AS VARCHAR),
  'billing_item', billing_document_id || ':' || CAST(item_no AS VARCHAR),
  'billing_item', NULL, CURRENT_TIMESTAMP
FROM billing_item
WHERE delivery_id IS NOT NULL AND delivery_item_no IS NOT NULL
  AND billing_document_id IS NOT NULL AND item_no IS NOT NULL

UNION ALL
SELECT DISTINCT
  'BILLING_ITEM_FOR_PRODUCT|' || billing_document_id || ':' || CAST(item_no AS VARCHAR) || '|' || product_id,
  'BILLING_ITEM_FOR_PRODUCT', 'billing_item', billing_document_id || ':' || CAST(item_no AS VARCHAR), 'product', product_id,
  'billing_item', NULL, CURRENT_TIMESTAMP
FROM billing_item
WHERE billing_document_id IS NOT NULL AND item_no IS NOT NULL AND product_id IS NOT NULL

UNION ALL
SELECT DISTINCT
  'BILLING_DOC_POSTED_TO_JOURNAL_ENTRY|' || reference_billing_document_id || '|' || journal_entry_id,
  'BILLING_DOC_POSTED_TO_JOURNAL_ENTRY', 'billing_document', reference_billing_document_id, 'journal_entry', journal_entry_id,
  'journal_entry', NULL, CURRENT_TIMESTAMP
FROM journal_entry
WHERE reference_billing_document_id IS NOT NULL AND journal_entry_id IS NOT NULL;
"""
