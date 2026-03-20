from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    # ── PostgreSQL staging DB (stg.staging_files, stg.jobs) ──
    # No defaults — app fails at startup if .env is missing or incomplete.
    PG_HOST:     str
    PG_PORT:     int
    PG_USER:     str
    PG_PASSWORD: str
    PG_DATABASE: str

    MAX_UPLOAD_SIZE_BYTES: int = 52_428_800  # 50 MB — safe to default

    # ── CORS ──
    # Comma-separated allowed origins. No wildcard.
    # Set in .env: ALLOWED_ORIGINS=http://localhost:8000,https://tools.yourcompany.com
    ALLOWED_ORIGINS: str = "http://localhost:8000,http://127.0.0.1:8000"

    @field_validator("PG_HOST", "PG_USER", "PG_PASSWORD", "PG_DATABASE", mode="before")
    @classmethod
    def must_not_be_empty(cls, v: str, info) -> str:
        if not v or not str(v).strip():
            raise ValueError(f"{info.field_name} must be set in .env and cannot be empty.")
        return v

    @property
    def cors_origins(self) -> list[str]:
        return [o.strip() for o in self.ALLOWED_ORIGINS.split(",") if o.strip()]

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.PG_USER}:{self.PG_PASSWORD}"
            f"@{self.PG_HOST}:{self.PG_PORT}/{self.PG_DATABASE}"
        )

    def pg_connect(self, dbname: str = None):
        """Return a psycopg2 connection to any database on the same server."""
        import psycopg2
        return psycopg2.connect(
            host=self.PG_HOST,
            port=self.PG_PORT,
            user=self.PG_USER,
            password=self.PG_PASSWORD,
            dbname=dbname or self.PG_DATABASE,
        )


settings = Settings()
