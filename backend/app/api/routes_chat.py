from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.app.core.database import get_db
from backend.app.guardrails.domain_guard import DomainGuard
from backend.app.query.query_service import QueryService

router = APIRouter(prefix="/chat", tags=["chat"])


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=1, description="Natural language ERP question")


@router.post("/query")
def query(req: QueryRequest, con: duckdb.DuckDBPyConnection = Depends(get_db)) -> dict:
    q = req.question.strip()
    ok, msg = DomainGuard.is_in_domain(q)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    svc = QueryService(con)
    result = svc.execute_question(q)

    if result.get("error"):
        raise HTTPException(
            status_code=400,
            detail={
                "message": result.get("error"),
                "answer_text": result.get("answer_text"),
                "mode": result.get("mode"),
                "mode_trace": result.get("mode_trace"),
                "generator_error": result.get("generator_error"),
                "generated_sql": result.get("generated_sql"),
            },
        )
    return result