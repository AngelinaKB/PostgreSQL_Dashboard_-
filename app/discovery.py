"""
app/discovery.py
----------------
GET /databases          — list all databases on the server
GET /schemas/{database} — list all schemas inside a specific database
GET /tables/{database}/{schema} — list all tables inside a schema

Uses user session credentials (X-Session-Token header).
"""

import psycopg2
from fastapi import APIRouter, Depends, HTTPException

from app.session import require_session, session_pg_connect

router = APIRouter()


@router.get("/meta/databases", summary="List all databases on the PostgreSQL server")
def list_databases(token: str = Depends(require_session)) -> list[str]:
    conn = session_pg_connect(token)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT datname
                FROM pg_database
                WHERE datistemplate = false
                  AND datname NOT IN ('postgres', 'template0', 'template1')
                ORDER BY datname
            """)
            return [row[0] for row in cur.fetchall()]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list databases: {exc}")
    finally:
        conn.close()


@router.get("/meta/schemas/{database}", summary="List all schemas inside a database")
def list_schemas(database: str, token: str = Depends(require_session)) -> list[str]:
    try:
        conn = session_pg_connect(token, dbname=database)
    except psycopg2.OperationalError as exc:
        raise HTTPException(status_code=404, detail=f"Cannot connect to database '{database}': {exc}")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT LIKE 'pg_%'
                  AND schema_name != 'information_schema'
                ORDER BY schema_name
            """)
            return [row[0] for row in cur.fetchall()]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list schemas: {exc}")
    finally:
        conn.close()


@router.get("/meta/tables/{database}/{schema}", summary="List all tables inside a schema")
def list_tables(database: str, schema: str, token: str = Depends(require_session)) -> list[str]:
    try:
        conn = session_pg_connect(token, dbname=database)
    except psycopg2.OperationalError as exc:
        raise HTTPException(status_code=404, detail=f"Cannot connect to database '{database}': {exc}")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, (schema,))
            return [row[0] for row in cur.fetchall()]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {exc}")
    finally:
        conn.close()


@router.get("/meta/columns/{database}/{schema}/{table}", summary="List columns of a table")
def list_columns(database: str, schema: str, table: str, token: str = Depends(require_session)) -> list[dict]:  # type: ignore[assignment]
    """Returns column names and SQL types for a given table."""
    try:
        conn = session_pg_connect(token, dbname=database)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, upper(data_type)
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table))
            rows = cur.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail=f"Table '{schema}.{table}' not found or has no columns.")
        return [{"name": r[0], "sql_type": r[1]} for r in rows]
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list columns: {exc}")
    finally:
        conn.close()


@router.get("/meta/preview/{database}/{schema}/{table}", summary="Get last 3 rows of a table")
def preview_table(database: str, schema: str, table: str, token: str = Depends(require_session)) -> dict:  # type: ignore[assignment]
    """Returns the last 3 rows of a table as a preview."""
    try:
        conn = session_pg_connect(token, dbname=database)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    try:
        with conn.cursor() as cur:
            # Get column names
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table))
            columns = [r[0] for r in cur.fetchall()]
            if not columns:
                raise HTTPException(status_code=404, detail=f"Table '{schema}.{table}' not found.")
            # Get last 3 rows
            cur.execute(f'SELECT * FROM "{schema}"."{table}" ORDER BY ctid DESC LIMIT 3')
            raw_rows = cur.fetchall()
            # Reverse so they're in natural order
            raw_rows = list(reversed(raw_rows))
            rows = [
                {columns[i]: (str(v) if v is not None else None) for i, v in enumerate(row)}
                for row in raw_rows
            ]
        return {"columns": columns, "rows": rows}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to preview table: {exc}")
    finally:
        conn.close()

