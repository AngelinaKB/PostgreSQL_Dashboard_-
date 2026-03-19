"""
app/session.py
--------------
Server-side session store for user-provided PostgreSQL credentials.

After a successful /connect, a UUID token is issued and the credentials
are stored in memory. Every subsequent request sends this token via the
X-Session-Token header, and get_session_conn() produces a psycopg2
connection using the stored credentials.

Staging operations (stg.staging_files, stg.jobs) always use settings
(.env credentials) and never touch this module.
"""

import secrets
import threading
from dataclasses import dataclass

import psycopg2
from fastapi import Header, HTTPException

# ── in-memory store ──────────────────────────────────────────────────────────
# { token: SessionCredentials }
_store: dict[str, "SessionCredentials"] = {}
_lock  = threading.Lock()


@dataclass
class SessionCredentials:
    host:     str
    port:     int
    user:     str
    password: str
    database: str          # default database for this session


# ── public API ───────────────────────────────────────────────────────────────

def create_session(host: str, port: int, user: str, password: str, database: str) -> str:
    """Persist credentials and return a new session token."""
    token = secrets.token_urlsafe(32)
    creds = SessionCredentials(
        host=host, port=port, user=user,
        password=password, database=database,
    )
    with _lock:
        _store[token] = creds
    return token


def delete_session(token: str) -> None:
    with _lock:
        _store.pop(token, None)


def get_credentials(token: str) -> SessionCredentials | None:
    with _lock:
        return _store.get(token)


def session_pg_connect(token: str, dbname: str = None) -> psycopg2.extensions.connection:
    """
    Open a psycopg2 connection using the stored credentials for this token.
    dbname overrides the session default database.
    """
    creds = get_credentials(token)
    if creds is None:
        raise HTTPException(status_code=401, detail="Session expired or invalid. Please reconnect.")
    return psycopg2.connect(
        host=creds.host,
        port=creds.port,
        user=creds.user,
        password=creds.password,
        dbname=dbname or creds.database,
    )


# ── FastAPI dependency ────────────────────────────────────────────────────────

def require_session(x_session_token: str = Header(default=None)) -> str:
    """
    FastAPI dependency that extracts and validates the session token.
    Raises 401 if missing or unknown.
    """
    if not x_session_token:
        raise HTTPException(
            status_code=401,
            detail="No session token. Please connect first.",
        )
    if get_credentials(x_session_token) is None:
        raise HTTPException(
            status_code=401,
            detail="Session expired or invalid. Please reconnect.",
        )
    return x_session_token
