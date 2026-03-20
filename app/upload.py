import io

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_db
from app.models import StagingFile

router = APIRouter()

ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx", ".txt", ".json", ".jsonl"}
ALLOWED_MIME_TYPES = {
    "text/csv",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/octet-stream",  # normalised by extension below
    "text/plain",
    "application/json",
    "application/x-ndjson",
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
            detail=f"Extension '{ext}' not allowed. Use: .csv, .xls, .xlsx, .txt, .json, .jsonl",
        )

    # --- Normalise content type ---
    ct = file.content_type or "application/octet-stream"
    if ct == "application/octet-stream":
        ct = {
            ".csv":  "text/csv",
            ".xls":  "application/vnd.ms-excel",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".json":  "application/json",
            ".jsonl": "application/x-ndjson",
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

    # --- For .json/.jsonl files: validate they are arrays of objects ---
    if ext in (".json", ".jsonl"):
        import json as _json
        sample = file_bytes[:4096].decode("utf-8", errors="replace").strip()
        try:
            if ext == ".jsonl":
                # JSONL: each line is a JSON object
                lines = [l.strip() for l in sample.splitlines() if l.strip()]
                if not lines:
                    raise ValueError("Empty file.")
                first_obj = _json.loads(lines[0])
                if not isinstance(first_obj, dict):
                    raise ValueError("Each line must be a JSON object ({...}).")
            else:
                # JSON: must be an array of objects
                parsed = _json.loads(file_bytes.decode("utf-8", errors="replace"))
                if not isinstance(parsed, list):
                    raise ValueError("JSON file must be an array ([...]) of objects.")
                if not parsed:
                    raise ValueError("JSON array is empty.")
                if not isinstance(parsed[0], dict):
                    raise ValueError("JSON array must contain objects ({...}), not primitives.")
        except _json.JSONDecodeError as e:
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail=f"Invalid JSON: {e}",
            )
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail=f"Unsupported JSON structure: {e} Only arrays of objects are supported.",
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
