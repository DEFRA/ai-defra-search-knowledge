from logging import getLogger

from sqlalchemy import text

from app.ingest.vector_store import get_connection

logger = getLogger(__name__)

_SEARCH_SQL = text("""
SELECT content,
       source_id AS document_id,
       1.0 - (embedding <=> CAST(:embedding AS vector)) AS similarity_score
FROM knowledge_vectors
WHERE metadata->>'knowledge_group_id' = ANY(:knowledge_group_ids)
ORDER BY embedding <=> CAST(:embedding2 AS vector) ASC
LIMIT :max_results
""")


async def search_vectors(
    embedding: list[float],
    knowledge_group_ids: list[str],
    max_results: int,
) -> list[dict]:
    """Execute cosine-distance vector search against knowledge_vectors.

    Returns a list of dicts with keys: content, similarity_score, document_id.
    Ordered by descending similarity_score (ascending cosine distance).
    Exceptions are propagated to the caller.
    """
    async with get_connection() as conn:
        result = await conn.execute(
            _SEARCH_SQL,
            {
                "embedding": embedding,
                "knowledge_group_ids": knowledge_group_ids,
                "embedding2": embedding,
                "max_results": max_results,
            },
        )
        rows = result.fetchall()

    results = [
        {
            "content": row[0],
            "document_id": row[1],
            "similarity_score": float(row[2]),
        }
        for row in rows
    ]
    logger.info(
        "Vector search returned %d results for %d group(s)",
        len(results),
        len(knowledge_group_ids),
    )
    return results
