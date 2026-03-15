# scripts/init_db.py
import psycopg2

conn = psycopg2.connect(
    host="localhost", port=5432,
    user="postgres", password="1234",
    dbname="webmethoddb"
)
conn.autocommit = True
cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS stg.staging_files (
        id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
        filename     TEXT        NOT NULL,
        content_type TEXT        NOT NULL,
        size_bytes   INTEGER     NOT NULL,
        file_bytes   BYTEA       NOT NULL,
        uploaded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        status       TEXT        NOT NULL DEFAULT 'uploaded'
    )
""")
print("✅ stg.staging_files created")
cur.close()
conn.close()
