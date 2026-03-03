import json
from abc import ABC, abstractmethod

import boto3

from app.config import config

_bedrock_client: boto3.client | None = None


def get_bedrock_client() -> boto3.client:
    global _bedrock_client
    if _bedrock_client is None:
        kwargs: dict = {"region_name": config.aws_region}
        if config.bedrock_endpoint_url:
            kwargs["endpoint_url"] = config.bedrock_endpoint_url
        _bedrock_client = boto3.client("bedrock-runtime", **kwargs)
    return _bedrock_client


class AbstractEmbeddingService(ABC):
    @abstractmethod
    def generate_embeddings(self, input_text: str) -> list[float]:
        pass


class BedrockEmbeddingService(AbstractEmbeddingService):
    def __init__(self):
        self._client = get_bedrock_client()

    def generate_embeddings(self, input_text: str) -> list[float]:
        request = {"inputText": input_text}
        response = self._client.invoke_model(
            modelId=config.bedrock_embedding.model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(request),
        )
        body = json.loads(response["body"].read().decode("utf-8"))
        return body["embedding"]
