"""
scripts/create_jobs_table.py
-----------------------------
Run once to create the stg.jobs table.

Usage:
    python scripts/create_jobs_table.py
"""
import psycopg2

conn = psycopg2.connect(
    host="localhost", port=5432,
    user="postgres", password="1234",
    dbname="webmethoddb"
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS stg.jobs (
        id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
        file_id     UUID        REFERENCES stg.staging_files(id) ON DELETE SET NULL,
        action      TEXT        NOT NULL,
        status      TEXT        NOT NULL DEFAULT 'queued'
                        CHECK (status IN ('queued','running','success','failed')),
        started_at  TIMESTAMPTZ,
        finished_at TIMESTAMPTZ,
        result      JSONB,
        message     TEXT,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )
""")
cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status   ON stg.jobs (status)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_file_id  ON stg.jobs (file_id)")

print("✅ stg.jobs table ready")
cur.close()
conn.close()