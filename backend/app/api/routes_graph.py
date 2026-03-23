from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.graph.graph_service import GraphService
from backend.app.core.database import get_db
import duckdb

router = APIRouter(prefix="/graph", tags=["graph"])


@router.get(
    "/node/{node_id}",
    summary="Get node metadata",
    description="Retrieve metadata and type for a specific ERP node"
)
def get_node(node_id: str, con: duckdb.DuckDBPyConnection = Depends(get_db)) -> dict:
    svc = GraphService()
    node = svc.get_node(node_id)
    if node["type"] == "unknown" and not node.get("metadata"):
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")
    return {"node": node}


@router.get(
    "/neighbors/{node_id}",
    summary="Get connected nodes and edges",
    description="Return all nodes directly connected to the given node with edge relationships"
)
def get_neighbors(node_id: str, con: duckdb.DuckDBPyConnection = Depends(get_db)) -> dict:
    svc = GraphService()
    payload = svc.get_neighbors(node_id=node_id, limit=50)
    if payload["node"]["type"] == "unknown" and not payload["node"].get("metadata"):
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")
    return payload


@router.get(
    "/flow/{billing_document_id}",
    summary="Trace order-to-cash flow",
    description="Reconstruct the complete ERP trace: Sales Order → Delivery → Billing → Journal Entry"
)
def get_flow(billing_document_id: str, con: duckdb.DuckDBPyConnection = Depends(get_db)) -> dict:
    svc = GraphService()
    result = svc.get_flow(billing_document_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get(
    "/sample-flow",
    summary="Get sample order-to-cash flow",
    description="Returns a sample billing document ID and traces its full flow"
)
def get_sample_flow(con: duckdb.DuckDBPyConnection = Depends(get_db)) -> dict:
    """Helper endpoint to find and trace a real billing document."""
    try:
        # Get first billing document
        row = con.execute(
            "SELECT billing_document_id FROM billing_document LIMIT 1"
        ).fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="No billing documents found in database")
        
        billing_id = row[0]
        svc = GraphService()
        result = svc.get_flow(billing_id)
        
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
        
        return result
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


@router.get(
    "/stats",
    summary="Graph database statistics",
    description="Return counts of all entity types and edges"
)
def get_stats(con: duckdb.DuckDBPyConnection = Depends(get_db)) -> dict:
    try:
        stats = {}
        for table in [
            "customer", "address", "product",
            "sales_order", "sales_order_item",
            "delivery", "delivery_item",
            "billing_document", "billing_item",
            "journal_entry", "journal_entry_line"
        ]:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            stats[table] = count

        edge_count = con.execute("SELECT COUNT(*) FROM graph_edge").fetchone()[0]
        stats["edges"] = edge_count

        return {"statistics": stats}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))