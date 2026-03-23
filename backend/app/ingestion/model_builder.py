from __future__ import annotations

import re
from typing import Dict, List

import pandas as pd

from .normalize import (
    clean_text,
    normalize_currency,
    normalize_date,
    normalize_decimal,
    normalize_id,
    normalize_int,
    normalize_timestamp,
)


def _norm(c: str) -> str:
    return re.sub(r"[^a-z0-9]", "", c.lower())


def _pick(df: pd.DataFrame, names: List[str]) -> pd.Series:
    cmap = {_norm(c): c for c in df.columns}
    for n in names:
        k = _norm(n)
        if k in cmap:
            return df[cmap[k]]
    return pd.Series([pd.NA] * len(df), dtype="string")


def _src(raw: Dict[str, pd.DataFrame], keys: List[str]) -> pd.DataFrame:
    # exact and raw_ prefixed
    for k in keys:
        if k in raw:
            return raw[k].copy()
        rk = k[4:] if k.startswith("raw_") else f"raw_{k}"
        if rk in raw:
            return raw[rk].copy()

    # fuzzy
    norm_raw = {_norm(rk): rk for rk in raw.keys()}
    for k in keys:
        nk = _norm(k)
        nk2 = _norm(k[4:]) if k.startswith("raw_") else nk
        for nrk, rk in norm_raw.items():
            if nk == nrk or nk2 == nrk or nk in nrk or nk2 in nrk:
                return raw[rk].copy()

    return pd.DataFrame()


def _dedupe(df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    return df.dropna(subset=keys).drop_duplicates(subset=keys)


def build_customer(raw):
    df = _src(raw, ["raw_business_partners", "business_partners"])
    if df.empty:
        return pd.DataFrame(columns=["customer_id", "customer_name", "segment", "country_code", "default_address_id"])

    out = pd.DataFrame(
        {
            "customer_id": normalize_id(_pick(df, ["businessPartner"])),
            "customer_name": clean_text(_pick(df, ["businessPartnerFullName"])),
            "segment": clean_text(_pick(df, ["businessPartnerGrouping"])),
            "country_code": clean_text(_pick(df, ["country"])).str.upper().str[:2],
            "default_address_id": normalize_id(_pick(df, ["addressId"])),
        }
    )
    return _dedupe(out, ["customer_id"])


def build_address(raw):
    df = _src(raw, ["raw_business_partner_addresses", "business_partner_addresses"])
    if df.empty:
        return pd.DataFrame(columns=["address_id", "line1", "city", "state", "postal_code", "country_code"])

    out = pd.DataFrame(
        {
            "address_id": normalize_id(_pick(df, ["addressId"])),
            "line1": clean_text(_pick(df, ["streetName"])),
            "city": clean_text(_pick(df, ["cityName"])),
            "state": clean_text(_pick(df, ["region"])),
            "postal_code": clean_text(_pick(df, ["postalCode"])),
            "country_code": clean_text(_pick(df, ["country"])).str.upper().str[:2],
        }
    )
    return _dedupe(out, ["address_id"])


def build_product(raw):
    df = _src(raw, ["raw_products", "products"])
    if df.empty:
        return pd.DataFrame(columns=["product_id", "product_name", "product_group", "base_uom"])

    # optional description table join
    dfd = _src(raw, ["raw_product_descriptions", "product_descriptions"])
    if not dfd.empty:
        d = pd.DataFrame(
            {
                "product_id": normalize_id(_pick(dfd, ["product"])),
                "product_name": clean_text(_pick(dfd, ["productDescription"])),
            }
        ).dropna(subset=["product_id"]).drop_duplicates(subset=["product_id"])
    else:
        d = pd.DataFrame(columns=["product_id", "product_name"])

    out = pd.DataFrame(
        {
            "product_id": normalize_id(_pick(df, ["product"])),
            "product_group": clean_text(_pick(df, ["productGroup"])),
            "base_uom": clean_text(_pick(df, ["baseUnit"])).str.upper(),
        }
    )
    out = out.merge(d, on="product_id", how="left")
    if "product_name" not in out.columns:
        out["product_name"] = pd.NA
    out = out[["product_id", "product_name", "product_group", "base_uom"]]
    return _dedupe(out, ["product_id"])


def build_sales_order(raw):
    df = _src(raw, ["raw_sales_order_headers", "sales_order_headers"])
    if df.empty:
        return pd.DataFrame(
            columns=["sales_order_id", "customer_id", "order_date", "status", "currency_code", "sold_to_address_id", "ship_to_address_id"]
        )

    out = pd.DataFrame(
        {
            "sales_order_id": normalize_id(_pick(df, ["salesOrder"])),
            "customer_id": normalize_id(_pick(df, ["soldToParty"])),
            "order_date": normalize_date(_pick(df, ["creationDate"])),
            "status": clean_text(_pick(df, ["overallDeliveryStatus"])),
            "currency_code": normalize_currency(_pick(df, ["transactionCurrency"])),
            "sold_to_address_id": normalize_id(_pick(df, ["soldToAddressId", "addressId"])),
            "ship_to_address_id": normalize_id(_pick(df, ["shipToAddressId"])),
        }
    )
    return _dedupe(out, ["sales_order_id"])


def build_sales_order_item(raw):
    df = _src(raw, ["raw_sales_order_items", "sales_order_items"])
    if df.empty:
        return pd.DataFrame(columns=["sales_order_id", "item_no", "product_id", "ordered_qty", "net_amount", "plant_code"])

    out = pd.DataFrame(
        {
            "sales_order_id": normalize_id(_pick(df, ["salesOrder"])),
            "item_no": normalize_int(_pick(df, ["salesOrderItem"])),
            "product_id": normalize_id(_pick(df, ["material"])),
            "ordered_qty": normalize_decimal(_pick(df, ["requestedQuantity"]), 3),
            "net_amount": normalize_decimal(_pick(df, ["netAmount"]), 2),
            "plant_code": clean_text(_pick(df, ["productionPlant"])),
        }
    )
    return _dedupe(out, ["sales_order_id", "item_no"])


def build_delivery(raw):
    df = _src(raw, ["raw_outbound_delivery_headers", "outbound_delivery_headers"])
    if df.empty:
        return pd.DataFrame(columns=["delivery_id", "customer_id", "delivery_date", "status", "ship_to_address_id"])

    out = pd.DataFrame(
        {
            "delivery_id": normalize_id(_pick(df, ["deliveryDocument"])),
            "customer_id": normalize_id(_pick(df, ["shipToParty", "soldToParty", "customer"])),
            "delivery_date": normalize_date(_pick(df, ["creationDate"])),
            "status": clean_text(_pick(df, ["overallGoodsMovementStatus"])),
            "ship_to_address_id": normalize_id(_pick(df, ["shipToAddressId", "addressId"])),
        }
    )
    return _dedupe(out, ["delivery_id"])


def build_delivery_item(raw):
    df = _src(raw, ["raw_outbound_delivery_items", "outbound_delivery_items"])
    if df.empty:
        return pd.DataFrame(columns=["delivery_id", "item_no", "sales_order_id", "sales_order_item_no", "product_id", "delivered_qty"])

    out = pd.DataFrame(
        {
            "delivery_id": normalize_id(_pick(df, ["deliveryDocument"])),
            "item_no": normalize_int(_pick(df, ["deliveryDocumentItem"])),
            "sales_order_id": normalize_id(_pick(df, ["referenceSdDocument"])),
            "sales_order_item_no": normalize_int(_pick(df, ["referenceSdDocumentItem"])),
            "product_id": normalize_id(_pick(df, ["material"])),
            "delivered_qty": normalize_decimal(_pick(df, ["actualDeliveryQuantity"]), 3),
        }
    )
    return _dedupe(out, ["delivery_id", "item_no"])


def build_billing_document(raw):
    df = _src(raw, ["raw_billing_document_headers", "billing_document_headers"])
    if df.empty:
        return pd.DataFrame(columns=["billing_document_id", "customer_id", "billing_date", "billing_type", "currency_code", "total_amount"])

    out = pd.DataFrame(
        {
            "billing_document_id": normalize_id(_pick(df, ["billingDocument"])),
            "customer_id": normalize_id(_pick(df, ["soldToParty"])),
            "billing_date": normalize_date(_pick(df, ["billingDocumentDate"])),
            "billing_type": clean_text(_pick(df, ["billingDocumentType"])),
            "currency_code": normalize_currency(_pick(df, ["transactionCurrency"])),
            "total_amount": normalize_decimal(_pick(df, ["totalNetAmount"]), 2),
        }
    )
    return _dedupe(out, ["billing_document_id"])


def build_billing_item(raw):
    df = _src(raw, ["raw_billing_document_items", "billing_document_items"])
    if df.empty:
        return pd.DataFrame(
            columns=[
                "billing_document_id",
                "item_no",
                "delivery_id",
                "delivery_item_no",
                "sales_order_id",
                "sales_order_item_no",
                "product_id",
                "billed_qty",
                "net_amount",
            ]
        )

    out = pd.DataFrame(
        {
            "billing_document_id": normalize_id(_pick(df, ["billingDocument"])),
            "item_no": normalize_int(_pick(df, ["billingDocumentItem"])),
            "delivery_id": normalize_id(_pick(df, ["referenceDeliveryDocument", "deliveryDocument"])),
            "delivery_item_no": normalize_int(_pick(df, ["referenceDeliveryDocumentItem", "deliveryDocumentItem"])),
            "sales_order_id": normalize_id(_pick(df, ["referenceSdDocument"])),
            "sales_order_item_no": normalize_int(_pick(df, ["referenceSdDocumentItem"])),
            "product_id": normalize_id(_pick(df, ["material"])),
            "billed_qty": normalize_decimal(_pick(df, ["billingQuantity"]), 3),
            "net_amount": normalize_decimal(_pick(df, ["netAmount"]), 2),
        }
    )
    return _dedupe(out, ["billing_document_id", "item_no"])


def build_journal_entry(raw):
    df = _src(raw, ["raw_journal_entry_items_accounts_receivable", "journal_entry_items_accounts_receivable"])
    if df.empty:
        return pd.DataFrame(
            columns=[
                "journal_entry_id",
                "company_code",
                "fiscal_year",
                "accounting_document",
                "posting_date",
                "document_date",
                "transaction_currency",
                "amount_in_transaction_currency",
                "reference_billing_document_id",
            ]
        )

    accounting_document = normalize_id(_pick(df, ["accountingDocument"]))

    out = pd.DataFrame(
        {
            "journal_entry_id": accounting_document,
            "company_code": clean_text(_pick(df, ["companyCode"])),
            "fiscal_year": normalize_int(_pick(df, ["fiscalYear"])),
            "accounting_document": accounting_document,
            "posting_date": normalize_timestamp(_pick(df, ["postingDate"])),
            "document_date": normalize_timestamp(_pick(df, ["documentDate"])),
            "transaction_currency": normalize_currency(_pick(df, ["transactionCurrency"])),
            "amount_in_transaction_currency": normalize_decimal(_pick(df, ["amountInTransactionCurrency"]), 2),
            "reference_billing_document_id": normalize_id(_pick(df, ["referenceDocument"])),
        }
    )
    return _dedupe(out, ["journal_entry_id"])


def build_journal_entry_line(raw):
    df = _src(raw, ["raw_journal_entry_items_accounts_receivable", "journal_entry_items_accounts_receivable"])
    if df.empty:
        return pd.DataFrame(
            columns=[
                "journal_entry_id",
                "line_no",
                "gl_account",
                "debit_credit_indicator",
                "amount_company_code_currency",
                "company_code_currency",
                "cost_center",
                "profit_center",
                "reference_document",
            ]
        )

    out = pd.DataFrame(
        {
            "journal_entry_id": normalize_id(_pick(df, ["accountingDocument"])),
            "line_no": normalize_int(_pick(df, ["accountingDocumentItem", "lineItem"])),
            "gl_account": normalize_id(_pick(df, ["glAccount"])),
            "debit_credit_indicator": clean_text(_pick(df, ["debitCreditCode"])).str[:1],
            "amount_company_code_currency": normalize_decimal(_pick(df, ["amountInCompanyCodeCurrency"]), 2),
            "company_code_currency": normalize_currency(_pick(df, ["companyCodeCurrency"])),
            "cost_center": normalize_id(_pick(df, ["costCenter"])),
            "profit_center": normalize_id(_pick(df, ["profitCenter"])),
            "reference_document": normalize_id(_pick(df, ["referenceDocument"])),
        }
    )
    return _dedupe(out, ["journal_entry_id", "line_no"])
