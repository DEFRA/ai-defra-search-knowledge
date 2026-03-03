import json
import logging
from contextlib import asynccontextmanager

from psycopg_pool import AsyncConnectionPool

from app.config import config

logger = logging.getLogger(__name__)

_pool: AsyncConnectionPool | None = None


def get_pool() -> AsyncConnectionPool:
    global _pool
    if _pool is None:
        conninfo = (
            f"host={config.postgres.host} port={config.postgres.port} "
            f"dbname={config.postgres.database} user={config.postgres.user}"
        )
        if config.postgres.password:
            conninfo += f" password={config.postgres.password}"
        if config.postgres.ssl_mode != "disable":
            conninfo += f" sslmode={config.postgres.ssl_mode}"
        _pool = AsyncConnectionPool(conninfo=conninfo, min_size=1, max_size=5)
        logger.info("Postgres connection pool created")

    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Postgres connection pool closed")


@asynccontextmanager
async def get_connection():
    pool = get_pool()
    async with pool.connection() as conn:
        yield conn


async def insert_vectors(
    vectors: list[tuple[str, list[float], str, str, dict | None]],
) -> None:
    """Insert (content, embedding, snapshot_id, source_id, metadata) into knowledge_vectors."""
    if not vectors:
        return
    pool = get_pool()
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.executemany(
            """
                INSERT INTO knowledge_vectors (content, embedding, snapshot_id, source_id, metadata)
                VALUES (%s, %s::vector, %s, %s, %s::jsonb)
                """,
            [
                (
                    content,
                    embedding,
                    snapshot_id,
                    source_id,
                    json.dumps(metadata) if metadata is not None else None,
                )
                for content, embedding, snapshot_id, source_id, metadata in vectors
            ],
        )
    logger.info("Inserted %d vectors into knowledge_vectors", len(vectors))
