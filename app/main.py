import os
from contextlib import asynccontextmanager
from logging import getLogger

import uvicorn
from fastapi import FastAPI

from app.common.mongo import get_mongo_client
from app.common.tracing import TraceIdMiddleware
from app.config import config
from app.document.router import router as document_router
from app.example.router import router as example_router
from app.health.router import router as health_router
from app.knowledge_group.router import router as knowledge_group_router

logger = getLogger(__name__)


KNOWLEDGE_GROUPS_COLLECTION = "knowledgeGroups"
DOCUMENTS_COLLECTION = "documents"


async def ensure_knowledge_group_indexes(client):
    db = client.get_database(config.mongo_database)
    await db[KNOWLEDGE_GROUPS_COLLECTION].create_index(
        [("created_by", 1), ("name", 1)],
        unique=True,
    )
    logger.info("Knowledge group unique index (created_by, name) ensured")


async def ensure_document_indexes(client):
    db = client.get_database(config.mongo_database)
    await db[DOCUMENTS_COLLECTION].create_index([("cdp_upload_id", 1)])
    logger.info("Document index (cdp_upload_id) ensured")


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Startup
    client = await get_mongo_client()
    logger.info("MongoDB client connected")
    await ensure_knowledge_group_indexes(client)
    await ensure_document_indexes(client)
    yield
    # Shutdown
    if client:
        await client.close()
        logger.info("MongoDB client closed")


app = FastAPI(lifespan=lifespan)

# Setup middleware
app.add_middleware(TraceIdMiddleware)

# Setup Routes
app.include_router(health_router)
app.include_router(example_router)
app.include_router(knowledge_group_router)
app.include_router(document_router)


def main() -> None:  # pragma: no cover
    if config.http_proxy:
        os.environ["HTTP_PROXY"] = str(config.http_proxy)
        os.environ["HTTPS_PROXY"] = str(config.http_proxy)
    else:
        os.environ.pop("HTTP_PROXY", None)
        os.environ.pop("HTTPS_PROXY", None)

    uvicorn.run(
        "app.main:app",
        host=config.host,
        port=config.port,
        log_config=config.log_config,
        reload=config.python_env == "development",
    )


if __name__ == "__main__":  # pragma: no cover
    main()
