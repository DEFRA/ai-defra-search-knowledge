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

KNOWLEDGE_GROUPS_COLLECTION = "knowledgeGroups"
DOCUMENTS_COLLECTION = "documents"


@router.post(
    "/rag/search",
    responses={
        404: {
            "description": "Knowledge group not found or not owned by the requesting user"
        },
        502: {
            "description": "Upstream failure — Bedrock embedding or Postgres vector search"
        },
    },
)
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
        doc = await db[KNOWLEDGE_GROUPS_COLLECTION].find_one(
            {"_id": object_id, "created_by": user_id}
        )
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
        logger.error("Bedrock embedding generation failed")
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

    # Step 4: Enrich results with document metadata from MongoDB
    doc_map: dict[str, dict] = {}
    if rows:
        object_ids = []
        for row in rows:
            try:
                object_ids.append(ObjectId(row["document_id"]))
            except (InvalidId, TypeError):
                logger.warning(
                    "Skipping invalid document_id %s during enrichment",
                    row["document_id"],
                )
        cursor = db[DOCUMENTS_COLLECTION].find(
            {"_id": {"$in": object_ids}},
            {"file_name": 1, "s3_key": 1},
        )
        async for doc in cursor:
            doc_map[str(doc["_id"])] = doc
        enriched_count = sum(1 for row in rows if row["document_id"] in doc_map)
        logger.info(
            "Enriched %d/%d result(s) with document metadata",
            enriched_count,
            len(rows),
        )

    # Step 5: Serialise and return
    return [
        RagSearchResult(
            content=row["content"],
            similarity_score=row["similarity_score"],
            document_id=row["document_id"],
            file_name=doc_map.get(row["document_id"], {}).get("file_name", ""),
            s3_key=doc_map.get(row["document_id"], {}).get("s3_key", ""),
        )
        for row in rows
    ]
