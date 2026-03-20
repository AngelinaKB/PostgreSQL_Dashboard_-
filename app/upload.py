import io

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_db
from app.models import StagingFile

router = APIRouter()

ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx", ".txt"}
ALLOWED_MIME_TYPES = {
    "text/csv",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/octet-stream",  # normalised by extension below
    "text/plain",
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
            detail=f"Extension '{ext}' not allowed. Use: .csv, .xls, .xlsx, .txt",
        )

    # --- Normalise content type ---
    ct = file.content_type or "application/octet-stream"
    if ct == "application/octet-stream":
        ct = {
            ".csv":  "text/csv",
            ".xls":  "application/vnd.ms-excel",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }.get(ext, ct)
    if ct == "text/plain" and ext == ".txt":
        ct = "text/plain"

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

    # --- For .txt files: reject known non-tabular formats ---
    if ext == ".txt":
        import re as _re
        sample = file_bytes[:2048].decode("utf-8", errors="replace")
        first  = sample.lstrip()[:50]

        # Reject JSON / JSONL
        if first.startswith(("{", "[")):
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail="JSON files are not supported. Rename to .csv if the file contains tabular data.",
            )

        # Reject log files — majority of first 10 lines match log patterns
        log_pat = _re.compile(
            r"^(\d{4}-\d{2}-\d{2}[\sT]|\[\d|\d{2}:\d{2}:\d{2}|(DEBUG|INFO|WARNING|ERROR|CRITICAL|WARN)\s)",
            _re.IGNORECASE,
        )
        first_lines = [l for l in sample.splitlines() if l.strip()][:10]
        if first_lines and sum(1 for l in first_lines if log_pat.match(l.strip())) > len(first_lines) // 2:
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail="Log files are not supported. Only delimited text files with a header row are accepted.",
            )

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

