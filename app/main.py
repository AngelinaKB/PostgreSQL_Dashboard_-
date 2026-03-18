from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.upload import router as upload_router
from app.file_details import router as details_router
from app.schema_def import router as schema_router
from app.create_table import router as create_router
from app.post_load import router as post_load_router
from app.jobs import router as jobs_router
from app.discovery import router as discovery_router

app = FastAPI(title="Ingestion Pipeline")

app.include_router(upload_router)
app.include_router(details_router)
app.include_router(schema_router)
app.include_router(create_router)
app.include_router(post_load_router)
app.include_router(jobs_router)
app.include_router(discovery_router)

app.mount("/static", StaticFiles(directory="frontend"), name="static")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/")
async def index():
    return FileResponse("frontend/index.html")
