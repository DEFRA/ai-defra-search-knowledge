from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class TimeoutConfig(BaseSettings):
    model_config = SettingsConfigDict()

    # HTTP (httpx) timeouts – seconds
    http_connect_timeout: int = Field(default=5, alias="HTTP_CONNECT_TIMEOUT")
    http_read_timeout: int = Field(default=30, alias="HTTP_READ_TIMEOUT")

    # MongoDB timeouts – milliseconds (PyMongo convention)
    mongo_connect_timeout_ms: int = Field(
        default=5000, alias="MONGO_CONNECT_TIMEOUT_MS"
    )
    mongo_server_selection_timeout_ms: int = Field(
        default=5000, alias="MONGO_SERVER_SELECTION_TIMEOUT_MS"
    )

    # AWS (boto3 / botocore) timeouts – seconds
    aws_connect_timeout: int = Field(default=5, alias="AWS_CONNECT_TIMEOUT")
    aws_read_timeout: int = Field(default=30, alias="AWS_READ_TIMEOUT")

    # PostgreSQL timeouts – seconds
    postgres_connect_timeout: int = Field(default=5, alias="POSTGRES_CONNECT_TIMEOUT")


class PostgresConfig(BaseSettings):
    model_config = SettingsConfigDict()
    host: str = Field(default="postgres", alias="POSTGRES_HOST")
    port: int = Field(5432, alias="POSTGRES_PORT")
    database: str = Field(default="ai_defra_search_knowledge", alias="POSTGRES_DB")
    user: str = Field(default="ai_defra_search_knowledge", alias="POSTGRES_USER")
    password: str | None = Field(default=None, alias="POSTGRES_PASSWORD")
    ssl_mode: str = Field(default="require", alias="POSTGRES_SSL_MODE")
    rds_truststore: str | None = Field(default=None, alias="TRUSTSTORE_RDS_ROOT_CA")


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
    # S3-only (LocalStack). Do not use AWS_ENDPOINT_URL — botocore applies it globally and
    # would route bedrock-runtime to LocalStack, breaking real Bedrock + bearer auth.
    localstack_s3_endpoint_url: str | None = Field(
        default=None, alias="LOCALSTACK_S3_ENDPOINT_URL"
    )
    localstack_access_key: str | None = Field(
        default=None, alias="LOCALSTACK_ACCESS_KEY"
    )
    localstack_secret_access_key: str | None = Field(
        default=None, alias="LOCALSTACK_SECRET_ACCESS_KEY"
    )
    bedrock_endpoint_url: str | None = Field(default=None, alias="BEDROCK_ENDPOINT_URL")
    http_proxy: HttpUrl | None = None
    enable_metrics: bool = False
    tracing_header: str = "x-cdp-request-id"
    api_key: str = Field(default="", alias="AI_DEFRA_SEARCH_KNOWLEDGE_API_KEY")
    knowledge_upload_bucket: str | None = Field(
        default=None, alias="KNOWLEDGE_UPLOAD_BUCKET_NAME"
    )
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    timeouts: TimeoutConfig = Field(default_factory=TimeoutConfig)
    bedrock_embedding: BedrockEmbeddingConfig = Field(
        default_factory=BedrockEmbeddingConfig
    )

    @property
    def upload_bucket_name(self) -> str:
        if self.knowledge_upload_bucket is not None:
            return self.knowledge_upload_bucket
        base = "ai-defra-search-knowledge-upload"
        if self.python_env == "development" and not base.endswith("-local"):
            return f"{base}-local"
        return base


config = AppConfig()
