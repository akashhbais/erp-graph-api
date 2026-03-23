from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.app.guardrails.domain_guard import DomainGuard
from backend.app.query.query_service import QueryService
from backend.app.core.database import get_db
import duckdb

router = APIRouter(prefix="/chat", tags=["chat"])


class QueryRequest(BaseModel):
    question: str = Field(..., description="Natural language question about the ERP dataset")


@router.post(
    "/query",
    summary="Natural language query",
    description="Convert ERP business questions into SQL queries. Includes domain guard and SQL validation.",
    response_description="Generated SQL, results, and metadata"
)
def query(req: QueryRequest, con: duckdb.DuckDBPyConnection = Depends(get_db)) -> dict:
    question = req.question.strip()

    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    # 1. Domain check
    in_domain, msg = DomainGuard.is_in_domain(question)
    if not in_domain:
        raise HTTPException(status_code=400, detail=msg)

    # 2. Execute
    svc = QueryService(con)
    result = svc.execute_question(question)

    if "error" in result:
        if "Access denied" in result.get("error", ""):
            raise HTTPException(status_code=403, detail=result["error"])
        raise HTTPException(status_code=400, detail=result["error"])

    return result