import asyncio
from logging import getLogger
from typing import Annotated

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, Header, HTTPException
from pymongo.asynchronous.database import AsyncDatabase

from app.common.bedrock import BedrockEmbeddingService
from app.common.mongo import get_db
from app.rag.models import RagSearchRequest, RagSearchResult
from app.rag.vector_search import search_vectors

router = APIRouter()
logger = getLogger(__name__)

COLLECTION = "knowledgeGroups"


@router.post("/rag/search")
async def search(
    body: RagSearchRequest,
    user_id: Annotated[str, Header(..., alias="user-id")],
    db: Annotated[AsyncDatabase, Depends(get_db)],
) -> list[RagSearchResult]:
    # Step 1: Validate each knowledge group exists and belongs to this user
    for group_id in body.knowledge_group_ids:
        try:
            object_id = ObjectId(group_id)
        except InvalidId:
            raise HTTPException(
                status_code=404,
                detail=f"Knowledge group '{group_id}' not found",
            ) from None
        doc = await db[COLLECTION].find_one({"_id": object_id, "created_by": user_id})
        if doc is None:
            raise HTTPException(
                status_code=404,
                detail=f"Knowledge group '{group_id}' not found",
            )
    logger.info(
        "All %d knowledge group(s) validated for user %s",
        len(body.knowledge_group_ids),
        user_id,
    )

    # Step 2: Generate embedding via Bedrock
    embedding_service = BedrockEmbeddingService()
    try:
        embedding = await asyncio.to_thread(
            embedding_service.generate_embeddings, body.query
        )
    except Exception:
        logger.error("Bedrock embedding generation failed for query: %s", body.query)
        raise HTTPException(
            status_code=502,
            detail="Embedding generation failed",
        ) from None
    logger.info("Bedrock embedding generated for query")

    # Step 3: Execute vector search in Postgres
    try:
        rows = await search_vectors(
            embedding, body.knowledge_group_ids, body.max_results
        )
    except Exception:
        logger.error(
            "Postgres vector search failed for knowledge_group_ids=%s",
            body.knowledge_group_ids,
        )
        raise HTTPException(
            status_code=502,
            detail="Vector search failed",
        ) from None
    logger.info("Vector search returned %d result(s)", len(rows))

    # Step 4: Serialise and return
    return [
        RagSearchResult(
            content=row["content"],
            similarity_score=row["similarity_score"],
            document_id=row["document_id"],
        )
        for row in rows
    ]
