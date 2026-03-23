from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb


class GraphService:
    NODE_TABLES: Dict[str, Tuple[str, Tuple[str, ...]]] = {
        "customer": ("customer", ("customer_id",)),
        "address": ("address", ("address_id",)),
        "product": ("product", ("product_id",)),
        "sales_order": ("sales_order", ("sales_order_id",)),
        "sales_order_item": ("sales_order_item", ("sales_order_id", "item_no")),
        "delivery": ("delivery", ("delivery_id",)),
        "delivery_item": ("delivery_item", ("delivery_id", "item_no")),
        "billing_document": ("billing_document", ("billing_document_id",)),
        "billing_item": ("billing_item", ("billing_document_id", "item_no")),
        "journal_entry": ("journal_entry", ("journal_entry_id",)),
        "journal_entry_line": ("journal_entry_line", ("journal_entry_id", "line_no")),
    }

    def __init__(self, db_path: Optional[str] = None) -> None:
        self.db_path = Path(db_path or os.getenv("APP_DB_PATH", "data/duckdb/app.duckdb"))

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path))

    def check_connection(self) -> Tuple[bool, Optional[str]]:
        try:
            with self._connect() as con:
                con.execute("SELECT 1").fetchone()
            return True, None
        except Exception as ex:
            return False, str(ex)

    def _infer_type_from_edges(self, con: duckdb.DuckDBPyConnection, node_id: str) -> Optional[str]:
        row = con.execute(
            """
            SELECT node_type
            FROM (
                SELECT from_type AS node_type FROM graph_edge WHERE from_id = ?
                UNION ALL
                SELECT to_type   AS node_type FROM graph_edge WHERE to_id = ?
            )
            LIMIT 1
            """,
            [node_id, node_id],
        ).fetchone()
        return row[0] if row else None

    def _fetch_node_row(
        self,
        con: duckdb.DuckDBPyConnection,
        node_type: str,
        node_id: str,
    ) -> Dict[str, Any]:
        if node_type not in self.NODE_TABLES:
            return {}

        table, pk_cols = self.NODE_TABLES[node_type]
        if len(pk_cols) == 1:
            row = con.execute(
                f"SELECT * FROM {table} WHERE {pk_cols[0]} = ? LIMIT 1",
                [node_id],
            ).fetchone()
            if not row:
                return {}
            cols = [d[0] for d in con.description]
            return dict(zip(cols, row))

        # composite key ids in graph are encoded as "id1:id2"
        parts = node_id.split(":")
        if len(parts) != len(pk_cols):
            return {}
        where_clause = " AND ".join([f"{c} = ?" for c in pk_cols])
        row = con.execute(
            f"SELECT * FROM {table} WHERE {where_clause} LIMIT 1",
            parts,
        ).fetchone()
        if not row:
            return {}
        cols = [d[0] for d in con.description]
        return dict(zip(cols, row))

    def _get_node_with_con(
        self,
        con: duckdb.DuckDBPyConnection,
        node_id: str,
        type_hint: Optional[str] = None,
    ) -> Dict[str, Any]:
        node_type = type_hint or self._infer_type_from_edges(con, node_id)

        if node_type:
            metadata = self._fetch_node_row(con, node_type, node_id)
            return {"id": node_id, "type": node_type, "metadata": metadata}

        # fallback scan for direct PK matches
        for t, (table, pk_cols) in self.NODE_TABLES.items():
            if len(pk_cols) != 1:
                continue
            row = con.execute(
                f"SELECT * FROM {table} WHERE {pk_cols[0]} = ? LIMIT 1",
                [node_id],
            ).fetchone()
            if row:
                cols = [d[0] for d in con.description]
                return {"id": node_id, "type": t, "metadata": dict(zip(cols, row))}

        return {"id": node_id, "type": "unknown", "metadata": {}}

    def get_node(self, node_id: str) -> Dict[str, Any]:
        with self._connect() as con:
            return self._get_node_with_con(con, node_id)

    def get_neighbors(self, node_id: str, limit: int = 50) -> Dict[str, Any]:
        edge_limit = min(max(limit, 1), 50)

        with self._connect() as con:
            center = self._get_node_with_con(con, node_id)

            rows = con.execute(
                """
                SELECT edge_id, edge_type, from_type, from_id, to_type, to_id
                FROM graph_edge
                WHERE from_id = ? OR to_id = ?
                LIMIT ?
                """,
                [node_id, node_id, edge_limit],
            ).fetchall()

            edges: List[Dict[str, Any]] = []
            neighbor_specs: Dict[Tuple[str, str], None] = {}

            for edge_id, edge_type, from_type, from_id, to_type, to_id in rows:
                edges.append(
                    {
                        "id": edge_id,
                        "type": edge_type,
                        "source": from_id,
                        "source_type": from_type,
                        "target": to_id,
                        "target_type": to_type,
                    }
                )

                if from_id == node_id:
                    neighbor_specs[(to_id, to_type)] = None
                else:
                    neighbor_specs[(from_id, from_type)] = None

            neighbors: List[Dict[str, Any]] = [
                self._get_node_with_con(con, nid, ntype) for (nid, ntype) in neighbor_specs.keys()
            ]

            return {
                "node": center,
                "neighbors": neighbors,
                "edges": edges,
            }

    def get_flow(self, billing_document_id: str) -> Dict[str, Any]:
        """Reconstruct ERP flow for a billing document."""
        with self._connect() as con:
            # Get billing document
            bd = con.execute(
                "SELECT * FROM billing_document WHERE billing_document_id = ?",
                [billing_document_id],
            ).fetchone()
            if not bd:
                return {"error": f"Billing document not found: {billing_document_id}"}

            bd_cols = [d[0] for d in con.description]
            bd_data = dict(zip(bd_cols, bd))

            # Get related billing items
            bi_rows = con.execute(
                "SELECT * FROM billing_item WHERE billing_document_id = ?",
                [billing_document_id],
            ).fetchall()
            bi_cols = [d[0] for d in con.description]
            billing_items = [dict(zip(bi_cols, row)) for row in bi_rows]

            # Extract unique sales orders
            so_ids = list({bi["sales_order_id"] for bi in billing_items if bi.get("sales_order_id")})
            sales_orders = []
            for so_id in so_ids:
                ro = con.execute(
                    "SELECT * FROM sales_order WHERE sales_order_id = ?",
                    [so_id],
                ).fetchone()
                if ro:
                    so_cols = [d[0] for d in con.description]
                    sales_orders.append(dict(zip(so_cols, ro)))

            # Extract sales order items
            soi_rows = con.execute(
                f"""
                SELECT soi.* FROM sales_order_item soi
                WHERE soi.sales_order_id IN ({','.join('?' * len(so_ids))})
                """,
                so_ids,
            ).fetchall()
            soi_cols = [d[0] for d in con.description]
            sales_order_items = [dict(zip(soi_cols, row)) for row in soi_rows]

            # Extract deliveries
            delivery_ids = list({bi["delivery_id"] for bi in billing_items if bi.get("delivery_id")})
            deliveries = []
            for d_id in delivery_ids:
                rd = con.execute(
                    "SELECT * FROM delivery WHERE delivery_id = ?",
                    [d_id],
                ).fetchone()
                if rd:
                    d_cols = [d[0] for d in con.description]
                    deliveries.append(dict(zip(d_cols, rd)))

            # Get journal entries linked to this billing doc
            je_rows = con.execute(
                "SELECT * FROM journal_entry WHERE reference_billing_document_id = ?",
                [billing_document_id],
            ).fetchall()
            je_cols = [d[0] for d in con.description]
            journal_entries = [dict(zip(je_cols, row)) for row in je_rows]

            return {
                "billing_document": bd_data,
                "flow": {
                    "sales_orders": sales_orders,
                    "sales_order_items": sales_order_items,
                    "deliveries": deliveries,
                    "billing_items": billing_items,
                    "journal_entries": journal_entries,
                },
            }