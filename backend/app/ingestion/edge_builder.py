import logging
import duckdb
from .schema_sql import EDGE_REFRESH_SQL

logger = logging.getLogger(__name__)

def refresh_graph_edges(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("Refreshing graph edges")
    con.execute(EDGE_REFRESH_SQL)
    n = con.execute("SELECT COUNT(*) FROM graph_edge").fetchone()[0]
    logger.info("graph_edge rows: %s", n)
