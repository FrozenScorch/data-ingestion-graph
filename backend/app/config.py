"""
Application configuration using Pydantic Settings.
Loads from .env file with environment variable overrides.
"""

import logging

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# Default secret that must never be used in production
_INSECURE_JWT_SECRET = "change-this-secret-in-production"
_INSECURE_CONNECTION_KEY = "change-this-connection-encryption-key"


class Settings(BaseSettings):
    """Application settings loaded from .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_name: str = "enterprise-data-ingestion-graph-studio"
    app_env: str = "development"
    app_debug: bool = True
    app_port: int = 8040

    # Database
    database_url: str = (
        "postgresql+asyncpg://ingestion:ingestion_password@localhost:5432/ingestion_db"
    )
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
    connection_encryption_key: str = ""

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
    max_upload_files_per_request: int = Field(default=20, ge=1, le=100)
    max_upload_request_mb: int = Field(default=250, ge=1)
    max_upload_storage_mb: int = Field(default=1024, ge=1)
    query_artifact_ttl_hours: int = Field(default=168, ge=1)

    # Durable execution worker
    run_worker_enabled: bool = True
    run_worker_concurrency: int = Field(default=1, ge=1, le=32)
    run_worker_poll_seconds: float = Field(default=1.0, ge=0.1, le=60)
    run_worker_lease_seconds: int = Field(default=60, ge=15, le=3600)
    run_worker_heartbeat_seconds: int = Field(default=15, ge=5, le=300)

    # Schedule dispatch and signed webhook ingestion
    trigger_scheduler_enabled: bool = True
    trigger_scheduler_poll_seconds: float = Field(default=5.0, ge=0.1, le=300)
    trigger_scheduler_batch_size: int = Field(default=50, ge=1, le=1000)
    webhook_max_bytes: int = Field(default=1_048_576, ge=1024, le=104_857_600)
    webhook_timestamp_skew_seconds: int = Field(default=300, ge=1, le=3600)
    webhook_delivery_retention_hours: int = Field(default=168, ge=1, le=8760)
    webhook_prune_interval_seconds: int = Field(default=3600, ge=60, le=86400)

    @model_validator(mode="after")
    def validate_worker_lease(self) -> "Settings":
        if self.run_worker_heartbeat_seconds * 2 >= self.run_worker_lease_seconds:
            raise ValueError("run worker heartbeat must be less than half the lease duration")
        return self

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

    def validate_security(self) -> None:
        """
        Validate security-critical settings at startup.

        Raises RuntimeError if JWT_SECRET is still the insecure default
        or shorter than 32 characters (insufficient entropy).
        """
        if self.jwt_secret_key == _INSECURE_JWT_SECRET:
            raise RuntimeError(
                "SECURITY: jwt_secret_key is still set to the default insecure value. "
                "Set a strong secret (>= 32 chars) via the JWT_SECRET_KEY environment "
                "variable or .env file before deploying to production."
            )
        if len(self.jwt_secret_key) < 32:
            raise RuntimeError(
                f"SECURITY: jwt_secret_key is only {len(self.jwt_secret_key)} characters long. "
                "It must be at least 32 characters for sufficient entropy. "
                "Set a stronger secret via the JWT_SECRET_KEY environment variable."
            )
        if (
            self.connection_encryption_key == _INSECURE_CONNECTION_KEY
            or len(self.connection_encryption_key) < 32
        ):
            raise RuntimeError(
                "SECURITY: CONNECTION_ENCRYPTION_KEY must be at least 32 characters "
                "outside development."
            )


# Global settings instance
settings = Settings()
