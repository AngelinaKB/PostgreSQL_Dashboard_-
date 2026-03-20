"""
app/jobs.py
-----------
Background job system using Python's ThreadPoolExecutor.

Architecture
------------
- When a user triggers create_table or append, we:
  1. Insert a job record (status=queued) and return the job_id immediately.
  2. Submit the actual work to a ThreadPoolExecutor which runs it in a
     background thread — the HTTP response returns instantly.
  3. The worker updates the job status (running → success/failed) and
     stores result/error in stg.jobs.

- GET /job-status/{job_id} lets the frontend poll for updates.

- No Redis, no Celery — just Python threads. Simple, zero extra infra.
  For 50 MB files this is plenty; the GIL is released during I/O and
  psycopg2 calls so true parallelism is achieved for DB-heavy work.
"""

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Callable

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, HTTPException

logger = logging.getLogger("data_ingestion")

from app.config import settings

router = APIRouter()

# Global thread pool — 4 workers, enough for concurrent ingestion
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="job-worker")


# ---------------------------------------------------------------------------
# Raw psycopg2 helpers (all sync, called from threads)
# ---------------------------------------------------------------------------

def _get_conn():
    return settings.pg_connect()


def create_job(file_id: uuid.UUID | None, action: str) -> uuid.UUID:
    """Insert a queued job and return its ID."""
    job_id = uuid.uuid4()
    conn = _get_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.jobs (id, file_id, action, status, created_at)
                VALUES (%s, %s, %s, 'queued', now())
                """,
                (str(job_id), str(file_id) if file_id else None, action),
            )
    finally:
        conn.close()
    return job_id


def get_job(job_id: uuid.UUID) -> dict | None:
    """Fetch a job record by ID."""
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM stg.jobs WHERE id = %s",
                (str(job_id),),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def _update_job(
    job_id: uuid.UUID,
    status: str,
    result: dict | None = None,
    message: str | None = None,
):
    """Update job status, result, and timestamps."""
    conn = _get_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            if status == "running":
                cur.execute(
                    "UPDATE stg.jobs SET status=%s, started_at=now() WHERE id=%s",
                    (status, str(job_id)),
                )
            else:
                import json
                cur.execute(
                    """
                    UPDATE stg.jobs
                    SET status=%s, finished_at=now(),
                        result=%s, message=%s
                    WHERE id=%s
                    """,
                    (
                        status,
                        json.dumps(result) if result else None,
                        message,
                        str(job_id),
                    ),
                )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Worker wrapper
# ---------------------------------------------------------------------------

def _run_job(job_id: uuid.UUID, fn: Callable, *args, **kwargs):
    """
    Wraps a sync function with job status tracking.
    Called in a thread pool thread — never in the async event loop.
    """
    _update_job(job_id, "running")
    try:
        result = fn(*args, **kwargs)
        _update_job(job_id, "success", result=result)
    except Exception as exc:
        _update_job(job_id, "failed", message=str(exc))
        logger.exception("Job %s crashed: %s", job_id, exc)


def submit_job(
    file_id: uuid.UUID | None,
    action: str,
    fn: Callable,
    *args,
    **kwargs,
) -> uuid.UUID:
    """
    Create a job record and submit fn(*args, **kwargs) to the thread pool.
    Returns the job_id immediately.
    """
    job_id = create_job(file_id, action)
    _executor.submit(_run_job, job_id, fn, *args, **kwargs)
    return job_id


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/job-status/{job_id}",
    summary="Poll the status of a background job",
)
async def job_status(job_id: uuid.UUID) -> dict:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

    # Serialise datetimes to ISO strings
    for key in ("started_at", "finished_at", "created_at"):
        if job.get(key) and hasattr(job[key], "isoformat"):
            job[key] = job[key].isoformat()

    return job
