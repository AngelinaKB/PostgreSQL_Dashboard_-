from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    PG_HOST: str = "localhost"
    PG_PORT: int = 5432
    PG_USER: str = "postgres"
    PG_PASSWORD: str = "1234"
    PG_DATABASE: str = "webmethoddb"

    MAX_UPLOAD_SIZE_BYTES: int = 52_428_800  # 50 MB

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
