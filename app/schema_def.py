"""
app/schema_def.py
-----------------
Stage 3: Schema definition endpoint.

POST /define-schema/{file_id}
  - Accepts a table name + list of column definitions.
  - Validates table name, column names, PK rules, constraints.
  - Returns validated schema JSON with any warnings.
  - Does NOT create the table yet — that is Stage 4.
"""

import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, field_validator, model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.utils import sniff_delimiter
from app.models import StagingFile

router = APIRouter()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TABLE_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

PG_RESERVED = frozenset({
    "user", "order", "group", "table", "select", "insert", "update", "delete",
    "from", "where", "index", "column", "constraint", "default", "check",
    "primary", "foreign", "references", "unique", "null", "not", "and", "or",
    "in", "is", "as", "on", "join", "left", "right", "inner", "outer",
    "create", "drop", "alter", "view", "schema", "database", "sequence",
    "trigger", "function", "procedure", "return", "end", "begin", "case",
    "when", "then", "else", "cast", "between", "like", "limit", "offset",
    "having", "distinct", "all", "any", "some", "exists", "union", "except",
    "intersect", "with", "recursive", "values", "set", "into", "by",
})

VALID_SQL_TYPES = frozenset({
    "TEXT", "VARCHAR(255)", "INTEGER", "BIGINT", "SMALLINT", "NUMERIC",
    "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP", "TIMESTAMPTZ", "UUID", "JSONB",
    "BYTEA",
})

# Pandas dtype → best SQL type mapping
PANDAS_TO_SQL: dict[str, str] = {
    "int64":          "BIGINT",
    "int32":          "INTEGER",
    "int16":          "SMALLINT",
    "int8":           "SMALLINT",
    "float64":        "FLOAT",
    "float32":        "FLOAT",
    "bool":           "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "object":         "TEXT",
    "string":         "TEXT",
    "category":       "TEXT",
}


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class ColumnDef(BaseModel):
    original_name: str
    new_name: str
    pandas_dtype: str
    sql_type: str
    is_pk: bool = False
    not_null: bool = False
    unique: bool = False

    @field_validator("sql_type")
    @classmethod
    def validate_sql_type(cls, v: str) -> str:
        if v not in VALID_SQL_TYPES:
            raise ValueError(f"Invalid SQL type '{v}'. Must be one of: {sorted(VALID_SQL_TYPES)}")
        return v

    @field_validator("new_name")
    @classmethod
    def validate_col_name(cls, v: str) -> str:
        v = v.strip()
        if not TABLE_NAME_RE.match(v):
            raise ValueError(
                f"Column name '{v}' is invalid. "
                "Use lowercase letters, digits, underscores only. Must start with a letter or underscore."
            )
        return v


class SchemaDefRequest(BaseModel):
    table_name: str
    columns: list[ColumnDef]

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        v = v.strip().lower()
        if not TABLE_NAME_RE.match(v):
            raise ValueError(
                f"Table name '{v}' is invalid. "
                "Use lowercase letters, digits, underscores only. Must start with a letter or underscore. Max 63 chars."
            )
        if v in PG_RESERVED:
            raise ValueError(
                f"'{v}' is a reserved PostgreSQL keyword. Choose a different table name."
            )
        return v

    @field_validator("columns")
    @classmethod
    def at_least_one_column(cls, v: list) -> list:
        if not v:
            raise ValueError("At least one column must be defined.")
        return v


class ColumnDefResponse(BaseModel):
    original_name: str
    new_name: str
    pandas_dtype: str
    sql_type: str
    is_pk: bool
    not_null: bool
    unique: bool


class SchemaDefResponse(BaseModel):
    staging_file_id: str
    table_name: str
    columns: list[ColumnDefResponse]
    warnings: list[str]
    valid: bool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sanitize_name(name: str) -> str:
    """Lowercase, replace spaces/hyphens with underscores, strip non-alnum."""
    s = name.strip().lower()
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    if s and s[0].isdigit():
        s = "_" + s
    return s or "col"


def _default_table_name(filename: str) -> str:
    import os
    base = os.path.splitext(filename)[0]
    name = _sanitize_name(base)
    # Truncate to 63 chars
    return name[:63]


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.post(
    "/define-schema/{file_id}",
    response_model=SchemaDefResponse,
    status_code=status.HTTP_200_OK,
    summary="Validate and return a schema definition for a staged file",
)
async def define_schema(
    file_id: str,
    payload: SchemaDefRequest,
    db: AsyncSession = Depends(get_db),
) -> SchemaDefResponse:
    from uuid import UUID

    # 1. Verify file exists
    try:
        uid = UUID(file_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid file ID format.")

    record: StagingFile | None = await db.get(StagingFile, uid)
    if record is None:
        raise HTTPException(status_code=404, detail=f"File '{file_id}' not found.")

    warnings: list[str] = []
    columns_out: list[ColumnDefResponse] = []

    # 2. Check for reserved table name
    if payload.table_name in PG_RESERVED:
        warnings.append(f"Table name '{payload.table_name}' is a reserved keyword.")

    # 3. Validate PK rules
    pk_columns = [col for col in payload.columns if col.is_pk]
    if len(pk_columns) > 1:
        raise HTTPException(
            status_code=422,
            detail=f"Only one primary key column is allowed. You selected {len(pk_columns)}: {', '.join(c.new_name for c in pk_columns)}."
        )

    # Check for duplicate new_names being used as PK
    all_new_names = [col.new_name for col in payload.columns]
    duplicates = {n for n in all_new_names if all_new_names.count(n) > 1}
    pk_dupes = [col.new_name for col in pk_columns if col.new_name in duplicates]
    if pk_dupes:
        raise HTTPException(
            status_code=422,
            detail=f"Column '{pk_dupes[0]}' is duplicated and cannot be used as a primary key."
        )

    # Check actual data for PK uniqueness (no duplicate values allowed)
    if pk_columns:
        pk_col = pk_columns[0]
        import asyncio, io, os, csv as _csv
        import pandas as pd
        from functools import partial

        def _check_pk_unique(raw: bytes, content_type: str, filename: str, col_original: str) -> int | None:
            """Returns the number of duplicate values if any, else None."""
            ext = os.path.splitext(filename)[-1].lower()
            if ext in (".json", ".jsonl"):
                import json as _json
                if ext == ".jsonl":
                    lines = [l.strip() for l in raw.decode("utf-8", errors="replace").splitlines() if l.strip()]
                    records = [_json.loads(l) for l in lines]
                else:
                    records = _json.loads(raw.decode("utf-8", errors="replace"))
                df = pd.DataFrame(records).astype(str)
            elif content_type in ("text/csv", "text/plain") or ext == ".txt":
                delimiter = sniff_delimiter(raw)
                df = pd.read_csv(io.BytesIO(raw), sep=delimiter, dtype=str, keep_default_na=False, encoding="utf-8")
            else:
                engine = "openpyxl" if ext == ".xlsx" else "xlrd"
                df = pd.read_excel(io.BytesIO(raw), engine=engine, dtype=str)

            df.columns = [str(c).strip() for c in df.columns]
            if col_original not in df.columns:
                return None
            dupes = df[col_original].duplicated().sum()
            return int(dupes) if dupes > 0 else None

        loop = asyncio.get_running_loop()
        dupe_count = await loop.run_in_executor(
            None,
            partial(_check_pk_unique, bytes(record.file_bytes), record.content_type, record.filename, pk_col.original_name),
        )
        if dupe_count:
            raise HTTPException(
                status_code=422,
                detail=f"Column '{pk_col.new_name}' has {dupe_count} duplicate value(s) and cannot be used as a primary key. PK columns must have unique values."
            )

    # 3. Process columns — deduplicate names, apply PK rules
    seen_names: dict[str, int] = {}

    for col in payload.columns:
        new_name = col.new_name
        not_null = col.not_null
        unique    = col.unique
        is_pk     = col.is_pk

        # PK rules: force NOT NULL, disable UNIQUE
        if is_pk:
            if not not_null:
                not_null = True
                warnings.append(
                    f"Column '{new_name}': PK selected — NOT NULL forced."
                )
            if unique:
                unique = False
                warnings.append(
                    f"Column '{new_name}': PK selected — UNIQUE disabled (PK implies uniqueness)."
                )

        # Duplicate name detection
        if new_name in seen_names:
            seen_names[new_name] += 1
            suffixed = f"{new_name}_{seen_names[new_name]}"
            warnings.append(
                f"Duplicate column name '{new_name}' — renamed to '{suffixed}'."
            )
            new_name = suffixed
        else:
            seen_names[new_name] = 1

        # Reserved column name warning
        if new_name in PG_RESERVED:
            warnings.append(
                f"Column name '{new_name}' is a reserved PostgreSQL keyword. Consider renaming it."
            )

        columns_out.append(ColumnDefResponse(
            original_name=col.original_name,
            new_name=new_name,
            pandas_dtype=col.pandas_dtype,
            sql_type=col.sql_type,
            is_pk=is_pk,
            not_null=not_null,
            unique=unique,
        ))

    return SchemaDefResponse(
        staging_file_id=file_id,
        table_name=payload.table_name,
        columns=columns_out,
        warnings=warnings,
        valid=True,
    )


# ---------------------------------------------------------------------------
# Helper endpoint: infer schema from file_id (called on page load)
# ---------------------------------------------------------------------------

@router.get(
    "/infer-schema/{file_id}",
    summary="Infer column names and SQL types from a staged file",
)
async def infer_schema(
    file_id: str,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """
    Re-reads the staged file via pandas, infers dtypes, and returns
    a suggested schema the frontend can pre-populate the editor with.
    """
    import asyncio
    import io
    from functools import partial
    import os
    import pandas as pd
    from uuid import UUID

    try:
        uid = UUID(file_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid file ID format.")

    record: StagingFile | None = await db.get(StagingFile, uid)
    if record is None:
        raise HTTPException(status_code=404, detail=f"File '{file_id}' not found.")

    def _infer(raw: bytes, content_type: str, filename: str) -> dict:
        ext = os.path.splitext(filename)[-1].lower()
        if ext in (".json", ".jsonl"):
            import json as _json
            if ext == ".jsonl":
                lines = [l.strip() for l in raw.decode("utf-8", errors="replace").splitlines() if l.strip()]
                records = [_json.loads(l) for l in lines]
            else:
                records = _json.loads(raw.decode("utf-8", errors="replace"))
            df = pd.DataFrame(records)
        elif content_type in ("text/csv", "text/plain") or ext == ".txt":
            delimiter = sniff_delimiter(raw)
            df = pd.read_csv(io.BytesIO(raw), sep=delimiter, encoding="utf-8")
        else:
            engine = "openpyxl" if ext == ".xlsx" else "xlrd"
            df = pd.read_excel(io.BytesIO(raw), engine=engine)

        col_count = len(df.columns)
        warnings = []
        if col_count > 1600:
            warnings.append(
                f"Your file has {col_count} columns. PostgreSQL supports a maximum of 1,600 — "
                f"please remove {col_count - 1600} column(s) before creating the table."
            )

        columns = []
        for col in df.columns:
            col_str = str(col).strip()
            sanitized = _sanitize_name(col_str)
            dtype_str = str(df[col].dtype)
            sql_type  = PANDAS_TO_SQL.get(dtype_str, "TEXT")
            columns.append({
                "original_name": col_str,
                "new_name":       sanitized,
                "pandas_dtype":   dtype_str,
                "sql_type":       sql_type,
                "is_pk":          False,
                "not_null":       False,
                "unique":         False,
            })

        return {
            "table_name": _default_table_name(filename),
            "columns": columns,
            "warnings": warnings,
        }

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        partial(_infer, bytes(record.file_bytes), record.content_type, record.filename),
    )
    return result

