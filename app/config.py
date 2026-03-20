from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    PG_HOST:     str
    PG_PORT:     int
    PG_USER:     str
    PG_PASSWORD: str
    PG_DATABASE: str

    MAX_UPLOAD_SIZE_BYTES: int   # required in .env, e.g. 52428800 for 50 MB

    ALLOWED_ORIGINS: str         # required in .env, comma-separated

    @property
    def cors_origins(self) -> list[str]:
        return [o.strip().rstrip("/") for o in self.ALLOWED_ORIGINS.split(",") if o.strip()]

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.PG_USER}:{self.PG_PASSWORD}"
            f"@{self.PG_HOST}:{self.PG_PORT}/{self.PG_DATABASE}"
        )

    def pg_connect(self, dbname: str = None):
        import psycopg2
        return psycopg2.connect(
            host=self.PG_HOST,
            port=self.PG_PORT,
            user=self.PG_USER,
            password=self.PG_PASSWORD,
            dbname=dbname or self.PG_DATABASE,
        )


settings = Settings()
