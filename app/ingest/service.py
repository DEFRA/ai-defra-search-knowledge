import json
import logging
from asyncio import to_thread

from app.common.bedrock import BedrockEmbeddingService
from app.ingest.s3_client import fetch_jsonl_from_s3
from app.ingest.vector_store import insert_vectors

logger = logging.getLogger(__name__)


def _parse_jsonl_chunks(data: bytes) -> list[dict]:
    """Parse JSONL bytes into list of chunk dicts with 'source' and 'text'."""
    chunks: list[dict] = []
    for line in data.decode("utf-8").strip().split("\n"):
        if not line:
            continue
        chunks.append(json.loads(line))
    return chunks


async def ingest_document(
    *,
    bucket: str,
    s3_key: str,
    document_id: str,
    knowledge_group_id: str,
    snapshot_id: str,
) -> int:
    """
    Fetch JSONL from S3, embed each chunk via Bedrock, insert into pgvector.
    Returns number of chunks ingested.
    """
    try:
        data = await to_thread(fetch_jsonl_from_s3, bucket, s3_key)
    except FileNotFoundError:
        logger.warning("No JSONL at s3://%s/%s, trying .jsonl suffix", bucket, s3_key)
        try:
            data = await to_thread(fetch_jsonl_from_s3, bucket, f"{s3_key}.jsonl")
        except FileNotFoundError:
            logger.error(
                "JSONL not found at s3://%s/%s or %s.jsonl", bucket, s3_key, s3_key
            )
            return 0

    chunks = _parse_jsonl_chunks(data)
    if not chunks:
        logger.warning("Empty or invalid JSONL at s3://%s/%s", bucket, s3_key)
        return 0

    embedding_service = BedrockEmbeddingService()
    vectors: list[tuple[str, list[float], str, str, dict | None]] = []

    for chunk in chunks:
        text = chunk.get("text") or chunk.get("content", "")
        if not text:
            continue
        source = chunk.get("source", "")
        embedding = await to_thread(embedding_service.generate_embeddings, text)
        metadata = {"source": source, "knowledge_group_id": knowledge_group_id}
        vectors.append((text, embedding, snapshot_id, document_id, metadata))

    if vectors:
        await insert_vectors(vectors)
    return len(vectors)
