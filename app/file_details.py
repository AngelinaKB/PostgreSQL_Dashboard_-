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
from app.utils import sniff_delimiter, fmt_delimiter
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

# _sniff_delimiter imported from app.utils  # fallback


def _parse_csv(raw: bytes, delimiter: str) -> tuple[list[str], list[dict]]:
    df = pd.read_csv(
        io.BytesIO(raw),
        sep=delimiter,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8",
    )
    df.columns = [str(c).strip() for c in df.columns]
    df = df.tail(PREVIEW_ROWS)
    rows = df.where(pd.notnull(df), None).to_dict(orient="records")
    return list(df.columns), rows


def _parse_excel(raw: bytes, ext: str) -> tuple[list[str], list[dict]]:
    engine = "openpyxl" if ext == ".xlsx" else "xlrd"
    df = pd.read_excel(
        io.BytesIO(raw),
        engine=engine,
        dtype=str,
    )
    df.columns = [str(c).strip() for c in df.columns]
    df = df.tail(PREVIEW_ROWS)
    rows = df.where(pd.notnull(df), None).to_dict(orient="records")
    return list(df.columns), rows


def _do_parse(
    raw: bytes,
    content_type: str,
    filename: str,
    delimiter_override: str | None,
) -> dict:
    ext = os.path.splitext(filename)[-1].lower()

    if content_type in ("text/csv",) or ext == ".txt":
        delimiter = delimiter_override or sniff_delimiter(raw)
        try:
            columns, rows = _parse_csv(raw, delimiter)
        except Exception as exc:
            raise ValueError(f"Could not parse file: {exc}") from exc
        return {
            "detected_delimiter": delimiter,
            "extension": ext if ext == ".txt" else None,
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

    record: StagingFile | None = await db.get(StagingFile, uid)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"File '{file_id}' not found.")

    # 2. Parse in thread pool (pandas is CPU-bound / sync)
    loop = asyncio.get_running_loop()
    try:
        parse_result = await loop.run_in_executor(
            None,
            partial(
                _do_parse,
                bytes(record.file_bytes),
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
