from fastapi import APIRouter, Depends

from backend.app.core.database import get_db
from backend.app.core.config import settings
import duckdb

router = APIRouter(tags=["health"])


@router.get(
    "/health",
    summary="Health check",
    description="Verify API and database connectivity"
)
def health(con: duckdb.DuckDBPyConnection = Depends(get_db)) -> dict:
    try:
        con.execute("SELECT 1").fetchone()
        return {
            "status": "ok",
            "database": "connected",
            "db_path": str(settings.DB_PATH),
            "version": settings.APP_VERSION,
            "debug": settings.DEBUG,
        }
    except Exception as ex:
        return {
            "status": "degraded",
            "database": "error",
            "db_path": str(settings.DB_PATH),
            "version": settings.APP_VERSION,
            "error": str(ex),
        }