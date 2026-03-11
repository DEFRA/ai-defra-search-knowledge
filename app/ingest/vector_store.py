import json
import logging
from contextlib import asynccontextmanager

from sqlalchemy import text

from app.common.postgres import get_sql_engine
from app.config import config

logger = logging.getLogger(__name__)

_INSERT_SQL = text("""
INSERT INTO knowledge_vectors (content, embedding, snapshot_id, source_id, metadata)
VALUES (:content, :embedding::vector, :snapshot_id, :source_id, :metadata::jsonb)
""")


@asynccontextmanager
async def get_connection():
    engine = await get_sql_engine()
    async with engine.connect() as conn:
        yield conn


async def close_pool() -> None:
    from app.common.postgres import close_engine

    await close_engine()


async def insert_vectors(
    vectors: list[tuple[str, list[float], str, str, dict | None]],
) -> None:
    """Insert (content, embedding, snapshot_id, source_id, metadata) into knowledge_vectors."""
    if not vectors:
        return
    engine = await get_sql_engine()
    params = [
        {
            "content": content,
            "embedding": embedding,
            "snapshot_id": snapshot_id,
            "source_id": source_id,
            "metadata": json.dumps(metadata) if metadata is not None else None,
        }
        for content, embedding, snapshot_id, source_id, metadata in vectors
    ]
    try:
        async with engine.connect() as conn:
            await conn.execute(_INSERT_SQL, params)
            await conn.commit()
        logger.info("Inserted %d vectors into knowledge_vectors", len(vectors))
    except Exception as e:
        logger.exception(
            "Postgres connection failed: %s (host=%s port=%s dbname=%s user=%s)",
            e,
            config.postgres.host,
            config.postgres.port,
            config.postgres.database,
            config.postgres.user,
        )
        raise
