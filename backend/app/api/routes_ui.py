from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter(tags=["ui"])

@router.get("/ui/graph", summary="Graph Explorer UI")
def graph_ui():
    page = Path(__file__).resolve().parents[1] / "static" / "graph.html"
    return FileResponse(page)