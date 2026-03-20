"""
app/main.py
-----------
FastAPI application entry point.

- CORS: explicit allowlist from settings.cors_origins (.env ALLOWED_ORIGINS)
  No wildcard "*" — production-safe.
- Logging: setup_logging() called at startup, writes to logs/api.log + stdout.
- All routers registered here.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.logging_config import setup_logging

from app.connect import router as connect_router
from app.upload import router as upload_router
from app.file_details import router as details_router
from app.schema_def import router as schema_router
from app.create_table import router as create_router
from app.post_load import router as post_load_router
from app.jobs import router as jobs_router
from app.discovery import router as discovery_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    log = setup_logging()
    log.info("Starting Data Ingestion API")
    log.info("CORS allowed origins: %s", settings.cors_origins)
    yield
    log.info("Shutting down Data Ingestion API")


app = FastAPI(
    title="Data Ingestion API",
    version="1.0.0",
    lifespan=lifespan,
    # Disable docs in production if needed:
    # docs_url=None, redoc_url=None,
)

# ── CORS ──────────────────────────────────────────────────────────────────────
# No wildcard. Origins configured via ALLOWED_ORIGINS in .env.
# To add your enterprise domain:
#   ALLOWED_ORIGINS=http://localhost:8000,https://tools.yourcompany.com
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Content-Type", "X-Session-Token"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(connect_router)
app.include_router(upload_router)
app.include_router(details_router)
app.include_router(schema_router)
app.include_router(create_router)
app.include_router(post_load_router)
app.include_router(jobs_router)
app.include_router(discovery_router)

# ── Static files + index ──────────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="frontend"), name="static")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/")
async def index():
    return FileResponse("frontend/index.html")
