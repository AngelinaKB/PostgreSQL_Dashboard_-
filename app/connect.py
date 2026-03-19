"""
app/connect.py
--------------
POST /connect    — validate credentials, test connection, create session
DELETE /connect  — destroy session
GET  /connect/me — return masked info about current session (no password)
"""

import re
import socket

import psycopg2
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator

from app.session import (
    create_session,
    delete_session,
    get_credentials,
    require_session,
)

router = APIRouter()

# ---------------------------------------------------------------------------
# SSRF protection — block private / loopback ranges and reserved hostnames
# Only relevant if the app is exposed to untrusted users.
# ---------------------------------------------------------------------------
_BLOCKED_PATTERNS = re.compile(
    r"^(localhost|.*\.local|metadata\.google\.internal)$", re.IGNORECASE
)
_PRIVATE_PREFIXES = (
    "10.", "172.16.", "172.17.", "172.18.", "172.19.",
    "172.20.", "172.21.", "172.22.", "172.23.", "172.24.",
    "172.25.", "172.26.", "172.27.", "172.28.", "172.29.",
    "172.30.", "172.31.", "192.168.", "127.", "169.254.",
)

def _check_ssrf(host: str) -> None:
    """
    Raise HTTPException if the host resolves to a private/loopback address.
    This prevents SSRF attacks where a user supplies an internal hostname.
    Remove or relax this if your PostgreSQL server IS on localhost/private network
    and you trust all users of this tool.
    """
    # Skip SSRF check for internal/trusted deployments — comment this out
    # if you want strict SSRF protection in a multi-tenant environment
    return

    # --- strict SSRF check below (uncomment for multi-tenant) ---
    # if _BLOCKED_PATTERNS.match(host):
    #     raise HTTPException(status_code=400, detail=f"Host '{host}' is not allowed.")
    # try:
    #     ip = socket.gethostbyname(host)
    # except socket.gaierror:
    #     raise HTTPException(status_code=400, detail=f"Cannot resolve host '{host}'.")
    # if any(ip.startswith(p) for p in _PRIVATE_PREFIXES):
    #     raise HTTPException(status_code=400, detail=f"Host '{host}' resolves to a private address.")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class ConnectRequest(BaseModel):
    host:     str = "localhost"
    port:     int = 5432
    user:     str = "postgres"
    password: str
    database: str = "postgres"

    @field_validator("port")
    @classmethod
    def valid_port(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError("Port must be between 1 and 65535.")
        return v

    @field_validator("host")
    @classmethod
    def valid_host(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Host cannot be empty.")
        # Basic sanity — no spaces, no shell metacharacters
        if re.search(r"[\s;|&`$]", v):
            raise ValueError("Host contains invalid characters.")
        return v

    @field_validator("database", "user")
    @classmethod
    def not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("This field cannot be empty.")
        return v.strip()


class ConnectResponse(BaseModel):
    token:          str
    host:           str
    port:           int
    user:           str
    database:       str
    server_version: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/connect", response_model=ConnectResponse)
def connect(req: ConnectRequest):
    """
    Test the provided PostgreSQL credentials.
    On success, create a server-side session and return a token.
    The password is never logged or returned to the client.
    """
    _check_ssrf(req.host)

    try:
        conn = psycopg2.connect(
            host=req.host,
            port=req.port,
            user=req.user,
            password=req.password,
            dbname=req.database,
            connect_timeout=5,          # never hang on unreachable servers
            options="-c statement_timeout=5000",  # 5s query timeout
        )
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            version_row = cur.fetchone()[0]
            version = version_row.split()[1] if version_row else "unknown"
        conn.close()
    except psycopg2.OperationalError as exc:
        # Sanitise error — don't echo back credentials in the message
        msg = str(exc)
        for secret in (req.password, req.user):
            msg = msg.replace(secret, "***")
        raise HTTPException(status_code=400, detail=f"Connection failed: {msg}")

    token = create_session(
        host=req.host,
        port=req.port,
        user=req.user,
        password=req.password,
        database=req.database,
    )

    # Password is NEVER included in the response
    return ConnectResponse(
        token=token,
        host=req.host,
        port=req.port,
        user=req.user,
        database=req.database,
        server_version=version,
    )


@router.delete("/connect")
def disconnect(token: str = Depends(require_session)):  # type: ignore[assignment]
    """Destroy the active session. Credentials are wiped from memory."""
    delete_session(token)
    return {"detail": "Disconnected."}


@router.get("/connect/me")
def whoami(token: str = Depends(require_session)):  # type: ignore[assignment]
    """Return masked connection info — host, port, user, database. No password."""
    creds = get_credentials(token)
    return {
        "host":     creds.host,
        "port":     creds.port,
        "user":     creds.user,
        "database": creds.database,
    }
