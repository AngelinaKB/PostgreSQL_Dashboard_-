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
        sample = file_bytes[:8192].decode("utf-8", errors="replace")
        lines  = [l for l in sample.splitlines() if l.strip()]

        if len(lines) < 2:
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail=".txt file must have at least a header row and one data row.",
            )

        detected_delim = sniff_delimiter(file_bytes)

        # Count columns per row — structured files have consistent counts
        try:
            col_counts = []
            for line in lines[:20]:  # check up to 20 rows
                row = next(_csv.reader([line], delimiter=detected_delim))
                col_counts.append(len(row))

            header_cols = col_counts[0]

            # Must have at least 2 columns
            if header_cols < 2:
                raise HTTPException(
                    status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                    detail=(
                        ".txt files must be structured delimited text with at least 2 columns. "
                        "Plain unstructured text files are not supported."
                    ),
                )

            # Must be consistent — allow 1 row to differ (trailing delimiter etc.)
            inconsistent = sum(1 for c in col_counts[1:] if c != header_cols)
            if inconsistent > max(1, len(col_counts) // 5):
                raise HTTPException(
                    status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                    detail=(
                        f"The .txt file does not appear to be consistently structured. "
                        f"Header has {header_cols} columns but row counts vary. "
                        "Only delimited structured files are supported."
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

