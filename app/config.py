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


settings = Settings()