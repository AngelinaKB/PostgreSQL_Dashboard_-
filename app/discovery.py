"""
app/discovery.py
----------------
Endpoints to auto-discover databases and schemas on the PostgreSQL server.

GET /databases          — list all databases on the server
GET /schemas/{database} — list all schemas inside a specific database

Both connect with the credentials from settings (same PG server, different db).
System databases (postgres, template0, template1) are excluded.
System schemas (pg_*, information_schema) are excluded.
"""

import psycopg2
from fastapi import APIRouter, HTTPException

from app.config import settings

router = APIRouter()


@router.get("/databases", summary="List all databases on the PostgreSQL server")
def list_databases() -> list[str]:
    """
    Returns all user-created databases on the server.
    Excludes: postgres, template0, template1.
    """
    conn = settings.pg_connect()
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


@router.get("/schemas/{database}", summary="List all schemas inside a database")
def list_schemas(database: str) -> list[str]:
    """
    Connects to the specified database and returns all user-created schemas.
    Excludes: pg_*, information_schema, pg_catalog.
    """
    try:
        conn = settings.pg_connect(dbname=database)
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
