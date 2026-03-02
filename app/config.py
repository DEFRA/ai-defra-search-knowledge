from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict()
    python_env: str | None = None
    host: str = "127.0.0.1"
    port: int = 8086
    log_config: str | None = None
    mongo_uri: str | None = None
    mongo_database: str = "ai-defra-search-knowledge"
    mongo_truststore: str = "TRUSTSTORE_CDP_ROOT_CA"
    aws_endpoint_url: str | None = None
    http_proxy: HttpUrl | None = None
    enable_metrics: bool = False
    tracing_header: str = "x-cdp-request-id"
    knowledge_upload_bucket: str | None = Field(
        default=None, alias="KNOWLEDGE_UPLOAD_BUCKET_NAME"
    )

    @property
    def upload_bucket_name(self) -> str:
        base = self.knowledge_upload_bucket or "ai-defra-search-knowledge-upload"
        if self.python_env == "development" and not base.endswith("-local"):
            return f"{base}-local"
        return base


config = AppConfig()
