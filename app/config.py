from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    # ── PostgreSQL staging DB ──
    # No defaults — app fails at startup if .env is missing these.
    PG_HOST:     str
    PG_PORT:     int
    PG_USER:     str
    PG_PASSWORD: str
    PG_DATABASE: str

    MAX_UPLOAD_SIZE_BYTES: int = 52_428_800  # 50 MB

    # CORS — comma-separated allowed origins, no wildcard
    ALLOWED_ORIGINS: str = "http://localhost:8000,http://127.0.0.1:8000"

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
