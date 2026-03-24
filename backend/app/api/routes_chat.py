from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
import duckdb

from backend.app.core.database import get_db
from backend.app.guardrails.domain_guard import DomainGuard
from backend.app.query.query_service import QueryService

router = APIRouter(prefix="/chat", tags=["chat"])


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=1, description="Natural language ERP question")


@router.post("/query")
def query(req: QueryRequest, con: duckdb.DuckDBPyConnection = Depends(get_db)) -> dict:
    question = req.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    in_domain, msg = DomainGuard.is_in_domain(question)
    if not in_domain:
        raise HTTPException(status_code=400, detail=msg)

    service = QueryService(con)
    result = service.execute_question(question)

    if result.get("error"):
        # Keep details visible for debugging in production
        raise HTTPException(
            status_code=400,
            detail={
                "message": result.get("error"),
                "mode": result.get("mode"),
                "generator_error": result.get("generator_error"),
                "generated_sql": result.get("generated_sql"),
            },
        )

    return result