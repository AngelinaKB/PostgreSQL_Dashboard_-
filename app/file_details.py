"""
app/file_details.py
-------------------
GET /file-details/{id}

Fetches metadata + raw bytes from stg.staging_files, parses the top 3 rows
via pandas, auto-detects CSV delimiter via csv.Sniffer, and returns a JSON
preview response.

Concurrency: each request gets its own DB session (via get_db dependency)
and runs pandas in a thread pool so the async event loop is never blocked.
"""

import asyncio
import csv
import io
import os
from functools import partial
from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models import StagingFile

router = APIRouter()

PREVIEW_ROWS = 3


# ---------------------------------------------------------------------------
# Response schema
# ---------------------------------------------------------------------------

class FileDetailsResponse(BaseModel):
    staging_file_id: str
    filename: str
    content_type: str
    size_bytes: int
    # CSV only
    detected_delimiter: str | None = None
    # Excel only
    extension: str | None = None
    # Preview
    columns: list[str]
    preview_rows: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# Sync parsing helpers (run in thread pool)
# ---------------------------------------------------------------------------

def _sniff_delimiter(raw: bytes) -> str:
    """Use csv.Sniffer on the first 4 KB to detect delimiter."""
    sample = raw[:4096].decode("utf-8", errors="replace")
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except csv.Error:
        return ","  # fallback


def _parse_csv(raw: bytes, delimiter: str) -> tuple[list[str], list[dict]]:
    # First pass: count total rows cheaply
    buf = io.BytesIO(raw)
    total = sum(1 for _ in buf) - 1  # subtract header
    skip = max(0, total - PREVIEW_ROWS)

    # Second pass: read only the last PREVIEW_ROWS rows
    df = pd.read_csv(
        io.BytesIO(raw),
        sep=delimiter,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8",
        skiprows=range(1, skip + 1) if skip > 0 else None,
    )
    df.columns = [str(c).strip() for c in df.columns]
    rows = df.where(pd.notnull(df), None).to_dict(orient="records")
    return list(df.columns), rows


def _parse_excel(raw: bytes, ext: str) -> tuple[list[str], list[dict]]:
    engine = "openpyxl" if ext == ".xlsx" else "xlrd"
    # Read full file to get row count, then slice tail
    # Excel has no cheap line-count — read with nrows=None but minimal cols
    df_full = pd.read_excel(io.BytesIO(raw), engine=engine, dtype=str, usecols=[0])
    total = len(df_full)
    skip = max(0, total - PREVIEW_ROWS)

    df = pd.read_excel(
        io.BytesIO(raw),
        engine=engine,
        dtype=str,
        skiprows=range(1, skip + 1) if skip > 0 else None,
        header=0,
    )
    df.columns = [str(c).strip() for c in df.columns]
    rows = df.where(pd.notnull(df), None).to_dict(orient="records")
    return list(df.columns), rows


def _do_parse(
    raw: bytes,
    content_type: str,
    filename: str,
    delimiter_override: str | None,
) -> dict:
    ext = os.path.splitext(filename)[-1].lower()

    if content_type == "text/csv":
        delimiter = delimiter_override or _sniff_delimiter(raw)
        try:
            columns, rows = _parse_csv(raw, delimiter)
        except Exception as exc:
            raise ValueError(f"Could not parse CSV: {exc}") from exc
        return {
            "detected_delimiter": delimiter,
            "extension": None,
            "columns": columns,
            "preview_rows": rows,
        }

    else:  # Excel
        try:
            columns, rows = _parse_excel(raw, ext)
        except Exception as exc:
            raise ValueError(f"Could not parse Excel file: {exc}") from exc
        return {
            "detected_delimiter": None,
            "extension": ext,
            "columns": columns,
            "preview_rows": rows,
        }


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/file-details/{file_id}",
    response_model=FileDetailsResponse,
    summary="Get file metadata and preview rows",
)
async def get_file_details(
    file_id: str,
    db: AsyncSession = Depends(get_db),
):
    # 1. Fetch record from DB
    from uuid import UUID
    try:
        uid = UUID(file_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="Invalid file ID format.")

    from sqlalchemy import select
    stmt = select(
        StagingFile.id, StagingFile.filename, StagingFile.content_type,
        StagingFile.size_bytes, StagingFile.file_bytes,
        StagingFile.status,
    ).where(StagingFile.id == uid)
    result = await db.execute(stmt)
    record = result.fetchone()
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"File '{file_id}' not found.")
    # Map to object-like access
    class _R:
        pass
    r = _R()
    r.filename, r.content_type, r.size_bytes, r.file_bytes = (
        record.filename, record.content_type, record.size_bytes, record.file_bytes
    )
    record = r

    # 2. Parse in thread pool (pandas is CPU-bound / sync)
    loop = asyncio.get_running_loop()
    try:
        parse_result = await loop.run_in_executor(
            None,
            partial(
                _do_parse,
                record.file_bytes,
                record.content_type,
                record.filename,
                None,
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=str(exc))

    return FileDetailsResponse(
        staging_file_id=str(record.id),
        filename=record.filename,
        content_type=record.content_type,
        size_bytes=record.size_bytes,
        **parse_result,
    )
