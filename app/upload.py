import io

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_db
from app.models import StagingFile

router = APIRouter()

ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx"}
ALLOWED_MIME_TYPES = {
    "text/csv",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/octet-stream",  # normalised by extension below
}
CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB


class UploadResponse(BaseModel):
    staging_file_id: str
    filename: str
    content_type: str
    size_bytes: int


@router.post("/upload", response_model=UploadResponse, status_code=201)
async def upload_file(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    import os

    # --- Validate extension ---
    ext = os.path.splitext(file.filename or "")[-1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"Extension '{ext}' not allowed. Use: .csv, .xls, .xlsx",
        )

    # --- Normalise content type ---
    ct = file.content_type or "application/octet-stream"
    if ct == "application/octet-stream":
        ct = {
            ".csv":  "text/csv",
            ".xls":  "application/vnd.ms-excel",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }.get(ext, ct)

    if ct not in ALLOWED_MIME_TYPES:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"Content-Type '{ct}' not supported.",
        )

    # --- Stream file into memory in chunks (never holds full file at once) ---
    buf = io.BytesIO()
    total = 0
    while chunk := await file.read(CHUNK_SIZE):
        total += len(chunk)
        if total > settings.MAX_UPLOAD_SIZE_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail="File exceeds the 50 MB limit.",
            )
        buf.write(chunk)

    file_bytes = buf.getvalue()

    # --- Insert into stg.staging_files ---
    record = StagingFile(
        filename=file.filename,
        content_type=ct,
        size_bytes=total,
        file_bytes=file_bytes,
    )
    db.add(record)
    await db.commit()
    await db.refresh(record)

    return UploadResponse(
        staging_file_id=str(record.id),
        filename=record.filename,
        content_type=record.content_type,
        size_bytes=record.size_bytes,
    )