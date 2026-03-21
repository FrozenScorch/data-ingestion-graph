"""
Application configuration using Pydantic Settings.
Loads from .env file with environment variable overrides.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_name: str = "ingestion-graph"
    app_env: str = "development"
    app_debug: bool = True
    app_port: int = 8040

    # Database
    database_url: str = "postgresql+asyncpg://ingestion:ingestion_password@localhost:5432/ingestion_db"
    database_pool_size: int = 10
    database_max_overflow: int = 20
    database_pool_timeout: int = 30
    database_pool_recycle: int = 3600
    database_echo: bool = False

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_password: str = ""

    # JWT Authentication
    jwt_secret_key: str = "change-this-secret-in-production"
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 60
    jwt_refresh_token_expire_days: int = 7

    # Admin seed user
    admin_username: str = "admin"
    admin_email: str = "admin@ingestion-graph.local"
    admin_password: str = "admin123"

    # OpenRouter
    openrouter_api_key: str = ""
    openrouter_free_models: str = "z-ai/glm-4.5-air:free,qwen/qwen3.5-9b"

    # CORS
    cors_origins: str = "http://localhost:3000,http://localhost:8040"

    # File storage
    upload_dir: str = "./data/uploads"
    temp_dir: str = "./data/temp"
    max_upload_size_mb: int = 100

    # Logging
    log_level: str = "INFO"

    @property
    def cors_origins_list(self) -> list[str]:
        """Parse CORS origins string into list."""
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

    @property
    def free_models_list(self) -> list[str]:
        """Parse free models string into list."""
        return [model.strip() for model in self.openrouter_free_models.split(",") if model.strip()]

    @property
    def sync_database_url(self) -> str:
        """Convert async database URL to sync for Alembic."""
        return self.database_url.replace("+asyncpg", "")


# Global settings instance
settings = Settings()
