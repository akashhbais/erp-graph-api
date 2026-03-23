from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter(tags=["ui"])

@router.get("/", summary="Main UI - Graph + Chat")
def index():
    page = Path(__file__).resolve().parents[1] / "static" / "index.html"
    return FileResponse(page, media_type="text/html")

@router.get("/ui/graph", summary="Graph Explorer (legacy)")
def graph_ui():
    page = Path(__file__).resolve().parents[1] / "static" / "graph.html"
    return FileResponse(page, media_type="text/html")