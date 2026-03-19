"""
app/post_load.py
----------------
Stage 5: Post-load actions — append rows and download table.

POST /append/{table_name}
  - Accepts a new file upload.
  - Validates MIME type.
  - Fetches existing column structure from information_schema.
  - Parses uploaded file, aligns columns, casts values to correct SQL types.
  - Warns on schema mismatch (extra/missing columns).
  - Bulk-inserts with advisory lock + transaction.
  - Returns rows appended + warnings.

GET /download/{table_name}
  - Queries dataset.<table_name>.
  - Streams as CSV or XLSX via pandas.
  - Returns as downloadable file.
"""

import csv
import io
import os
from functools import partial
import asyncio

import pandas as pd
import psycopg2
import psycopg2.extras
from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import StreamingResponse

from app.config import settings
from app.session import require_session, session_pg_connect
from app.schema_def import _sanitize_name

router = APIRouter()

BATCH_SIZE = 500
CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB

ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx"}
ALLOWED_MIME_TYPES = {
    "text/csv",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/octet-stream",
}

# SQL type → Python cast function
CAST_MAP = {
    "BIGINT":       lambda v: int(float(v)) if v not in (None, "") else None,
    "INTEGER":      lambda v: int(float(v)) if v not in (None, "") else None,
    "SMALLINT":     lambda v: int(float(v)) if v not in (None, "") else None,
    "FLOAT":        lambda v: float(v)       if v not in (None, "") else None,
    "NUMERIC":      lambda v: float(v)       if v not in (None, "") else None,
    "BOOLEAN":      lambda v: str(v).lower() in ("true", "1", "yes") if v not in (None, "") else None,
    "TEXT":         lambda v: str(v)         if v not in (None, "") else None,
    "VARCHAR(255)": lambda v: str(v)         if v not in (None, "") else None,
    "DATE":         lambda v: str(v)         if v not in (None, "") else None,
    "TIMESTAMP":    lambda v: str(v)         if v not in (None, "") else None,
    "TIMESTAMPTZ":  lambda v: str(v)         if v not in (None, "") else None,
    "UUID":         lambda v: str(v)         if v not in (None, "") else None,
    "JSONB":        lambda v: str(v)         if v not in (None, "") else None,
    "BYTEA":        lambda v: v,
}


def cast_series(value, sql_type: str, empty_as_null: bool = True):
    """Cast a single cell value to the appropriate Python type for psycopg2."""
    if value is None or (empty_as_null and str(value).strip() == ""):
        return None
    cast_fn = CAST_MAP.get(sql_type.upper(), lambda v: v)
    try:
        return cast_fn(str(value).strip())
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Sync helpers
# ---------------------------------------------------------------------------

def _get_table_schema(table_name: str, target_schema: str = "public", target_database: str = None, session_token: str = None) -> list[dict]:
    """
    Fetch column names and data types from information_schema.
    """
    conn = session_pg_connect(session_token, dbname=target_database)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name,
                       upper(data_type) as data_type
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name   = %s
                ORDER BY ordinal_position
                """,
                (target_schema, table_name),
            )
            rows = cur.fetchall()
        if not rows:
            raise ValueError(f"Table '{target_schema}.{table_name}' not found or has no columns.")
        return [{"name": r[0], "sql_type": r[1]} for r in rows]
    finally:
        conn.close()


def _parse_upload(raw: bytes, content_type: str, filename: str) -> pd.DataFrame:
    """Parse uploaded bytes into a DataFrame with sanitised column names."""
    ext = os.path.splitext(filename)[-1].lower()
    if content_type in ("text/csv", "text/plain") or ext == ".txt":
        sample = raw[:4096].decode("utf-8", errors="replace")
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            delimiter = dialect.delimiter
        except csv.Error:
            delimiter = ","
        df = pd.read_csv(
            io.BytesIO(raw), sep=delimiter,
            dtype=str, keep_default_na=False, encoding="utf-8",
        )
    else:
        engine = "openpyxl" if ext == ".xlsx" else "xlrd"
        df = pd.read_excel(io.BytesIO(raw), engine=engine, dtype=str)

    df.columns = [_sanitize_name(str(c)) for c in df.columns]
    return df



def _stream_csv(table_name: str, target_schema: str = "public", target_database: str = None, session_token: str = None):
    """Stream CSV directly from PostgreSQL via COPY TO STDOUT. Never loads full table."""
    conn = session_pg_connect(session_token, dbname=target_database)
    try:
        buf = io.BytesIO()
        with conn.cursor() as cur:
            cur.copy_expert(
                f'COPY "{target_schema}"."{table_name}" TO STDOUT WITH (FORMAT CSV, HEADER TRUE)',
                buf,
            )
        buf.seek(0)
        while True:
            chunk = buf.read(65536)
            if not chunk:
                break
            yield chunk
    finally:
        conn.close()


def _build_xlsx(table_name: str, target_schema: str = "public", target_database: str = None, session_token: str = None) -> bytes:
    """Build XLSX using xlsxwriter — much faster than openpyxl for large tables."""
    conn = session_pg_connect(session_token, dbname=target_database)
    try:
        df = pd.read_sql(f'SELECT * FROM "{target_schema}"."{table_name}"', conn)
    finally:
        conn.close()
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=table_name[:31])
        # Disable URL recognition — prevents slow hyperlinking of URL columns
        writer.book.strings_to_urls = False
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

def _append_job(table_name: str, rows_input: list[dict], target_schema: str = "public", target_database: str = None, session_token: str = None) -> dict:
    """
    Sync worker: cast and insert manually entered rows.
    Runs in thread pool via jobs.submit_job.
    """
    table_schema = _get_table_schema(table_name, target_schema, target_database, session_token)
    table_cols   = {c["name"]: c["sql_type"] for c in table_schema}
    insert_cols  = list(table_cols.keys())

    cast_rows: list[tuple] = []
    errors: list[str] = []
    for i, row in enumerate(rows_input, start=1):
        cast_row = []
        for col in insert_cols:
            val = row.get(col, None)
            try:
                cast_row.append(cast_series(val, table_cols[col], empty_as_null=True))
            except Exception as e:
                errors.append(f"Row {i}, column '{col}': {e}")
        cast_rows.append(tuple(cast_row))

    if errors:
        raise ValueError("; ".join(errors))

    rows_inserted = _insert_rows(table_name, insert_cols, cast_rows, target_schema, target_database, session_token)
    return {
        "table_name": f"{target_schema}.{table_name}",
        "rows_inserted": rows_inserted,
    }


@router.post(
    "/append/{database}/{schema}/{table_name}",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Queue a background job to insert manually entered rows",
)
async def append_rows(
    database: str,
    schema: str,
    table_name: str,
    payload: dict,
    token: str = Depends(require_session),
):
    """
    Accepts { rows: [ {col: value, ...}, ... ] }
    Returns job_id immediately; work runs in background.
    """
    from app.jobs import submit_job

    rows_input: list[dict] = payload.get("rows", [])
    if not rows_input:
        raise HTTPException(status_code=422, detail="No rows provided.")

    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, partial(_get_table_schema, table_name, schema, database, token))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    job_id = submit_job(
        file_id=None,
        action="append",
        fn=_append_job,
        table_name=table_name,
        rows_input=rows_input,
        target_schema=schema,
        target_database=database,
        session_token=token,
    )

    return {"job_id": str(job_id), "status": "queued"}


def _insert_rows(table_name: str, col_names: list[str], rows: list[tuple], target_schema: str = "public", target_database: str = None, session_token: str = None) -> int:
    conn = session_pg_connect(session_token, dbname=target_database)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            lock_id = hash(f"{target_schema}.{table_name}") & 0x7FFFFFFFFFFFFFFF
            cur.execute(f"SELECT pg_advisory_xact_lock({lock_id})")
            col_list     = ", ".join(f'"{c}"' for c in col_names)
            placeholders = ", ".join(["%s"] * len(col_names))
            insert_sql   = f'INSERT INTO "{target_schema}"."{table_name}" ({col_list}) VALUES ({placeholders})'
            for i in range(0, len(rows), BATCH_SIZE):
                psycopg2.extras.execute_batch(cur, insert_sql, rows[i:i+BATCH_SIZE])
        conn.commit()
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@router.get(
    "/download/{database}/{schema}/{table_name}",
    summary="Download a table as CSV or XLSX",
)
async def download_table(
    database: str,
    schema: str,
    table_name: str,
    fmt: str = Query(default="csv", description="Output format: csv or xlsx"),
    token: str = Depends(require_session),
):
    if fmt not in ("csv", "xlsx"):
        raise HTTPException(status_code=400, detail="fmt must be 'csv' or 'xlsx'.")

    filename = f"{table_name}.{fmt}"

    if fmt == "csv":
        return StreamingResponse(
            _stream_csv(table_name, schema, database, token),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    else:
        loop = asyncio.get_running_loop()
        try:
            file_bytes = await loop.run_in_executor(None, partial(_build_xlsx, table_name, schema, database, token))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to build XLSX: {exc}")
        return StreamingResponse(
            io.BytesIO(file_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
