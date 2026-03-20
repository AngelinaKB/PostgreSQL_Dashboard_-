import io

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_db
from app.models import StagingFile
from app.utils import sniff_delimiter

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

    # --- For .txt files, verify the content is actually structured/delimited ---
    if ext == ".txt":
        import csv as _csv
        import re as _re
        sample = file_bytes[:8192].decode("utf-8", errors="replace")
        lines  = [l for l in sample.splitlines() if l.strip()]

        if len(lines) < 2:
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail=".txt file must have at least a header row and one data row.",
            )

        # --- Reject known unstructured formats immediately ---
        first = lines[0].strip()

        # Reject JSON / JSONL
        if first.startswith("{") or first.startswith("["):
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail=(
                    "This looks like a JSON or JSONL file, not a delimited text file. "
                    "Only structured delimited files (CSV-like) are supported."
                ),
            )

        # Reject log files — lines starting with timestamps or log levels
        log_pattern = _re.compile(
            r"^(\d{4}-\d{2}-\d{2}[\sT]|\[\d|\d{2}:\d{2}:\d{2}|"
            r"(DEBUG|INFO|WARNING|ERROR|CRITICAL|WARN)\s)",
            _re.IGNORECASE,
        )
        log_line_count = sum(1 for l in lines[:10] if log_pattern.match(l.strip()))
        if log_line_count > len(lines[:10]) // 2:
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail=(
                    "This looks like a log file, not a delimited text file. "
                    "Only structured delimited files (CSV-like) are supported."
                ),
            )

        # --- Detect delimiter and validate structure ---
        detected_delim = sniff_delimiter(file_bytes)

        try:
            col_counts = []
            for line in lines[:30]:
                row = next(_csv.reader([line], delimiter=detected_delim))
                col_counts.append(len(row))

            header_cols = col_counts[0]

            # Must have at least 2 columns
            if header_cols < 2:
                raise HTTPException(
                    status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                    detail=(
                        ".txt files must have at least 2 delimited columns. "
                        "Plain unstructured text files are not supported."
                    ),
                )

            # All rows (not just most) must match header column count
            # Allow only 1 outlier row for trailing delimiters / blank lines
            data_counts = col_counts[1:]
            inconsistent = sum(1 for c in data_counts if c != header_cols)
            if inconsistent > 1:
                raise HTTPException(
                    status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                    detail=(
                        f"File structure is inconsistent: header has {header_cols} columns "
                        f"but {inconsistent} rows have a different count. "
                        "Only consistently delimited files are supported."
                    ),
                )

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail=f"Could not validate .txt file structure: {e}",
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

