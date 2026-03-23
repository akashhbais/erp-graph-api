from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.app.api import routes_ui
from backend.app.api.routes_graph import router as graph_router
from backend.app.api.routes_health import router as health_router
from backend.app.api.routes_chat import router as chat_router

app = FastAPI(
    title="ERP Graph API",
    version="1.0.0",
    description="Enterprise Resource Planning Graph Intelligence Platform",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
app.include_router(health_router)
app.include_router(graph_router)
app.include_router(chat_router)
app.include_router(routes_ui.router)