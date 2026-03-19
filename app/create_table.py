"""
app/create_table.py
-------------------
Stage 4: Create the curated table and bulk-load rows.

POST /create-table/{file_id}
  - Validates table name (no collision, no reserved words).
  - Generates CREATE TABLE dataset.<name> DDL.
  - Parses the staged file via pandas.
  - Bulk-inserts rows using psycopg2 executemany in batches.
  - Runs ANALYZE after load.
  - Wraps everything in one transaction with a pg_advisory_xact_lock
    to prevent two concurrent requests creating the same table.
  - Updates stg.staging_files.status → 'loaded'.
"""

import asyncio
import csv
import io
import os
import re
from functools import partial
from typing import Any

import pandas as pd
import psycopg2
import psycopg2.extras
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.session import require_session, session_pg_connect
from app.db import get_db
from app.models import StagingFile
from app.schema_def import (
    PG_RESERVED,
    TABLE_NAME_RE,
    VALID_SQL_TYPES,
    _sanitize_name,
)

router = APIRouter()

BATCH_SIZE = 500


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class ColumnInput(BaseModel):
    original_name: str = ""
    new_name: str
    sql_type: str
    is_pk:    bool = False
    not_null: bool = False
    unique:   bool = False

    @field_validator("sql_type")
    @classmethod
    def check_sql_type(cls, v: str) -> str:
        if v not in VALID_SQL_TYPES:
            raise ValueError(f"Invalid SQL type: {v}")
        return v

    @field_validator("new_name")
    @classmethod
    def check_col_name(cls, v: str) -> str:
        v = v.strip()
        if not TABLE_NAME_RE.match(v):
            raise ValueError(f"Invalid column name: '{v}'")
        return v


class CreateTableRequest(BaseModel):
    table_name: str
    columns: list[ColumnInput]
    overwrite: bool = False
    target_database: str = settings.PG_DATABASE
    target_schema: str = "public"

    @field_validator("table_name")
    @classmethod
    def check_table_name(cls, v: str) -> str:
        v = v.strip().lower()
        if not TABLE_NAME_RE.match(v):
            raise ValueError(
                f"Table name '{v}' is invalid. Use lowercase letters, digits, underscores only."
            )
        if v in PG_RESERVED:
            raise ValueError(f"'{v}' is a reserved PostgreSQL keyword.")
        return v

    @field_validator("columns")
    @classmethod
    def at_least_one(cls, v: list) -> list:
        if not v:
            raise ValueError("At least one column is required.")
        if len(v) > 1600:
            raise ValueError(
                f"PostgreSQL supports a maximum of 1,600 columns per table. "
                f"Your file has {len(v)} columns."
            )
        return v


class CreateTableResponse(BaseModel):
    success: bool
    table_name: str
    full_table_name: str
    rows_loaded: int
    warnings: list[str]


# ---------------------------------------------------------------------------
# DDL builder
# ---------------------------------------------------------------------------

def _build_ddl(table_name: str, columns: list[ColumnInput], warnings: list[str], target_schema: str = "public") -> tuple[str, list[ColumnInput]]:
    """
    Build CREATE TABLE DDL. Returns (ddl_string, resolved_columns).
    Resolves duplicate column names with _2, _3 suffixes.
    """
    seen: dict[str, int] = {}
    resolved: list[ColumnInput] = []

    col_defs: list[str] = []
    pk_cols:  list[str] = []

    for col in columns:
        name = col.new_name

        # Deduplicate
        if name in seen:
            seen[name] += 1
            new_name = f"{name}_{seen[name]}"
            warnings.append(f"Duplicate column '{name}' renamed to '{new_name}'.")
            name = new_name
        else:
            seen[name] = 1

        # Reserved keyword warning
        if name in PG_RESERVED:
            warnings.append(f"Column '{name}' is a reserved keyword — consider renaming.")

        # PK enforcement
        not_null = col.not_null
        unique   = col.unique
        is_pk    = col.is_pk
        if is_pk:
            not_null = True
            unique   = False
            pk_cols.append(f'"{name}"')

        parts = [f'"{name}"', col.sql_type]
        if not_null:
            parts.append("NOT NULL")
        if unique and not is_pk:
            parts.append("UNIQUE")

        col_defs.append("    " + " ".join(parts))
        resolved.append(ColumnInput(
            new_name=name,
            sql_type=col.sql_type,
            is_pk=is_pk,
            not_null=not_null,
            unique=unique,
        ))

    if pk_cols:
        col_defs.append(f"    PRIMARY KEY ({', '.join(pk_cols)})")

    ddl = (
        f'CREATE TABLE "{target_schema}"."{table_name}" (\n'
        + ",\n".join(col_defs)
        + "\n);"
    )
    return ddl, resolved


# ---------------------------------------------------------------------------
# File parser (sync, runs in thread pool)
# ---------------------------------------------------------------------------

def _parse_to_rows(
    raw: bytes,
    content_type: str,
    filename: str,
    col_names: list[str],
    original_names: list[str] | None = None,
) -> list[tuple]:
    """
    Parse the staged file and return rows as list-of-tuples
    matching the order of col_names.

    col_names      — user-defined new names (what the table will use)
    original_names — original file header names (before user renaming)
    """
    ext = os.path.splitext(filename)[-1].lower()

    if content_type in ("text/csv", "text/plain") or ext == ".txt":
        sample = raw[:4096].decode("utf-8", errors="replace")
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            delimiter = dialect.delimiter
        except csv.Error:
            delimiter = ","
        df = pd.read_csv(
            io.BytesIO(raw),
            sep=delimiter,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
        )
    else:
        engine = "openpyxl" if ext == ".xlsx" else "xlrd"
        df = pd.read_excel(io.BytesIO(raw), engine=engine, dtype=str)

    # Rename file columns to user-defined new_names positionally
    # This is the most reliable approach — file cols and original_names
    # are always in the same order from the infer-schema step
    if original_names and len(original_names) == len(col_names) and len(original_names) == len(df.columns):
        # Positional rename: file_col[i] → col_names[i]
        df.columns = col_names
    else:
        # Fallback: sanitize and match by name
        df.columns = [_sanitize_name(str(c)) for c in df.columns]

    # Select only columns present in both, in order
    available = [c for c in col_names if c in df.columns]
    if not available:
        raise ValueError(
            f"No matching columns found between file and schema. "
            f"File has: {list(df.columns)}, schema expects: {col_names}"
        )
    df = df[available]

    # Replace NaN with None
    df = df.where(pd.notnull(df), None)

    return [tuple(row) for row in df.itertuples(index=False, name=None)]


# ---------------------------------------------------------------------------
# Sync bulk-load (runs in thread pool via run_in_executor)
# ---------------------------------------------------------------------------

def _create_and_load(
    ddl:             str,
    table_name:      str,
    col_names:       list[str],
    rows:            list[tuple],
    overwrite:       bool = False,
    target_database: str = None,
    target_schema:   str = "public",
    session_token:   str = None,
) -> int:
    """
    Opens a psycopg2 connection to target_database using session credentials.
    Acquires an advisory lock, creates the table, bulk-inserts rows,
    runs ANALYZE, commits. Returns the number of rows inserted.
    """
    conn = session_pg_connect(session_token, dbname=target_database)
    conn.autocommit = False
    full = f'"{target_schema}"."{table_name}"'

    try:
        with conn.cursor() as cur:
            lock_id = hash(f"{target_schema}.{table_name}") & 0x7FFFFFFFFFFFFFFF
            cur.execute(f"SELECT pg_advisory_xact_lock({lock_id})")

            # Pre-flight: verify schema exists
            cur.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                (target_schema,),
            )
            if not cur.fetchone():
                raise ValueError(
                    f"Schema '{target_schema}' does not exist in database '{target_database}'. "
                    "Create it first or pick a different schema."
                )

            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                """,
                (target_schema, table_name),
            )
            exists = cur.fetchone()
            if exists and not overwrite:
                raise ValueError(
                    f"Table '{target_schema}.{table_name}' already exists in '{target_database}'. "
                    "Tick 'Overwrite if exists' and click Create table again to replace it."
                )
            if exists and overwrite:
                cur.execute(f"DROP TABLE {full}")

            # Log DDL for debugging
            import logging as _log
            _log.getLogger("data_ingestion").info(f"Executing DDL:\n{ddl}")

            try:
                cur.execute(ddl)
            except Exception as ddl_exc:
                raise ValueError(f"CREATE TABLE failed: {ddl_exc}\nDDL was:\n{ddl}") from ddl_exc

            # Verify table was actually created before inserting
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
                (target_schema, table_name),
            )
            if not cur.fetchone():
                raise ValueError(
                    f"Table '{target_schema}.{table_name}' was not created. "
                    f"Ensure schema '{target_schema}' exists in database '{target_database}'."
                )

            if rows:
                placeholders = ", ".join(["%s"] * len(col_names))
                insert_sql = (
                    f"INSERT INTO {full} "
                    f'({", ".join(f"{chr(34)}{c}{chr(34)}" for c in col_names)}) '
                    f"VALUES ({placeholders})"
                )
                for i in range(0, len(rows), BATCH_SIZE):
                    psycopg2.extras.execute_batch(cur, insert_sql, rows[i:i+BATCH_SIZE])

            cur.execute(f"ANALYZE {full}")

        conn.commit()
        return len(rows)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Sync worker function (runs in thread pool via jobs.submit_job)
# ---------------------------------------------------------------------------

def _create_table_job(
    file_id_str: str,
    payload_dict: dict,
    session_token: str = None,
) -> dict:
    """
    Full create-table pipeline in one sync function.
    Fetches file bytes, parses, creates table, loads rows, updates status.
    Returns result dict stored in stg.jobs.result.
    """
    import csv as _csv
    import io as _io
    import os as _os
    import pandas as _pd

    conn = settings.pg_connect()
    conn.autocommit = True

    # Fetch staging record
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT filename, content_type, file_bytes FROM stg.staging_files WHERE id = %s",
            (file_id_str,),
        )
        record = cur.fetchone()
    conn.close()

    if not record:
        raise ValueError(f"Staging file '{file_id_str}' not found.")

    # Rebuild payload objects
    columns = [ColumnInput(**c) for c in payload_dict["columns"]]
    table_name = payload_dict["table_name"]
    overwrite  = payload_dict.get("overwrite", False)

    warnings: list[str] = []
    target_schema   = payload_dict.get("target_schema", "public")
    target_database = payload_dict.get("target_database", settings.PG_DATABASE)
    ddl, resolved_cols = _build_ddl(table_name, columns, warnings, target_schema)
    col_names      = [c.new_name      for c in resolved_cols]
    original_names = [c.original_name for c in columns]

    import logging as _log2
    _log2.getLogger("data_ingestion").info(
        f"DDL to execute (db={target_database}, schema={target_schema}):\n{ddl}"
    )
    _log2.getLogger("data_ingestion").info(f"col_names: {col_names}")
    _log2.getLogger("data_ingestion").info(f"original_names: {original_names}")

    # Parse file
    rows = _parse_to_rows(
        bytes(record["file_bytes"]),
        record["content_type"],
        record["filename"],
        col_names,
        original_names,
    )

    # Create table + load
    rows_loaded = _create_and_load(ddl, table_name, col_names, rows, overwrite, target_database, target_schema, session_token)

    # Update staging status
    conn2 = settings.pg_connect()
    conn2.autocommit = True
    with conn2.cursor() as cur:
        cur.execute(
            "UPDATE stg.staging_files SET status = 'loaded' WHERE id = %s",
            (file_id_str,),
        )
    conn2.close()

    return {
        "table_name": table_name,
        "full_table_name": f"{target_schema}.{table_name}",
        "target_database": target_database,
        "target_schema": target_schema,
        "rows_loaded": rows_loaded,
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# Endpoint — now returns job_id immediately
# ---------------------------------------------------------------------------

@router.post(
    "/create-table/{file_id}",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Queue a background job to create dataset table and bulk-load rows",
)
async def create_table(
    file_id: str,
    payload: CreateTableRequest,
    db: AsyncSession = Depends(get_db),  # type: ignore[assignment]
    token: str = Depends(require_session),  # type: ignore[assignment]
) -> dict:
    from uuid import UUID
    from app.jobs import submit_job

    try:
        uid = UUID(file_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid file ID format.")

    record: StagingFile | None = await db.get(StagingFile, uid)
    if record is None:
        raise HTTPException(status_code=404, detail=f"File '{file_id}' not found.")

    if not payload.overwrite:
        loop = asyncio.get_running_loop()
        exists = await loop.run_in_executor(
            None, _table_exists,
            payload.table_name, payload.target_schema, payload.target_database, token
        )
        if exists:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Table '{payload.target_schema}.{payload.table_name}' already exists in '{payload.target_database}'. "
                    "Tick 'Overwrite if exists' and click Create table again to replace it."
                ),
            )

    job_id = submit_job(
        file_id=uid,
        action="create_table",
        fn=_create_table_job,
        file_id_str=file_id,
        payload_dict=payload.model_dump(),
        session_token=token,
    )

    return {"job_id": str(job_id), "status": "queued"}


def _table_exists(table_name: str, target_schema: str = "public", target_database: str = None, session_token: str = None) -> bool:
    conn = session_pg_connect(session_token, dbname=target_database)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
                (target_schema, table_name),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()
