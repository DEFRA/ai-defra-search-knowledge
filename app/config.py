from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresConfig(BaseSettings):
    model_config = SettingsConfigDict()
    host: str = Field(default="postgres", alias="POSTGRES_HOST")
    port: int = Field(5432, alias="POSTGRES_PORT")
    database: str = Field(default="ai_defra_search_data", alias="POSTGRES_DB")
    user: str = Field(default="postgres", alias="POSTGRES_USER")
    password: str | None = Field(default=None, alias="POSTGRES_PASSWORD")
    ssl_mode: str = Field(default="disable", alias="POSTGRES_SSL_MODE")


class BedrockEmbeddingConfig(BaseSettings):
    model_config = SettingsConfigDict()
    model_id: str = Field(
        default="amazon.titan-embed-text-v2:0",
        alias="BEDROCK_EMBEDDING_MODEL_ID",
    )


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict()
    python_env: str | None = None
    host: str = "127.0.0.1"
    port: int = 8086
    log_config: str | None = None
    mongo_uri: str | None = None
    mongo_database: str = "ai-defra-search-knowledge"
    mongo_truststore: str = "TRUSTSTORE_CDP_ROOT_CA"
    aws_region: str = Field(default="eu-west-2", alias="AWS_REGION")
    aws_endpoint_url: str | None = Field(default=None, alias="AWS_ENDPOINT_URL")
    bedrock_endpoint_url: str | None = Field(default=None, alias="BEDROCK_ENDPOINT_URL")
    http_proxy: HttpUrl | None = None
    enable_metrics: bool = False
    tracing_header: str = "x-cdp-request-id"
    knowledge_upload_bucket: str | None = Field(
        default=None, alias="KNOWLEDGE_UPLOAD_BUCKET_NAME"
    )
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    bedrock_embedding: BedrockEmbeddingConfig = Field(
        default_factory=BedrockEmbeddingConfig
    )

    @property
    def upload_bucket_name(self) -> str:
        base = self.knowledge_upload_bucket or "ai-defra-search-knowledge-upload"
        if self.python_env == "development" and not base.endswith("-local"):
            return f"{base}-local"
        return base


config = AppConfig()
