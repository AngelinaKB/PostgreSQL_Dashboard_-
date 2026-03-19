"""
app/connect.py
--------------
POST /connect    — test credentials, create session, return token
DELETE /connect  — destroy session
GET  /connect/me — return masked info about current session
"""

import psycopg2
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.session import (
    create_session,
    delete_session,
    get_credentials,
    require_session,
)

router = APIRouter()


class ConnectRequest(BaseModel):
    host:     str = "localhost"
    port:     int = 5432
    user:     str = "postgres"
    password: str
    database: str = "postgres"


class ConnectResponse(BaseModel):
    token:    str
    host:     str
    port:     int
    user:     str
    database: str
    server_version: str


@router.post("/connect", response_model=ConnectResponse)
def connect(req: ConnectRequest):
    """
    Test the provided PostgreSQL credentials.
    On success, create a server-side session and return the token.
    """
    try:
        conn = psycopg2.connect(
            host=req.host,
            port=req.port,
            user=req.user,
            password=req.password,
            dbname=req.database,
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            version_row = cur.fetchone()[0]
            # e.g. "PostgreSQL 15.3 on ..." → "15.3"
            version = version_row.split()[1] if version_row else "unknown"
        conn.close()
    except psycopg2.OperationalError as exc:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=400,
            detail=f"Could not connect: {exc}",
        )

    token = create_session(
        host=req.host,
        port=req.port,
        user=req.user,
        password=req.password,
        database=req.database,
    )

    return ConnectResponse(
        token=token,
        host=req.host,
        port=req.port,
        user=req.user,
        database=req.database,
        server_version=version,
    )


@router.delete("/connect")
def disconnect(token: str = Depends(require_session)):
    """Destroy the active session."""
    delete_session(token)
    return {"detail": "Disconnected."}


@router.get("/connect/me")
def whoami(token: str = Depends(require_session)):
    """Return masked info about the current session (no password)."""
    creds = get_credentials(token)
    return {
        "host":     creds.host,
        "port":     creds.port,
        "user":     creds.user,
        "database": creds.database,
    }
