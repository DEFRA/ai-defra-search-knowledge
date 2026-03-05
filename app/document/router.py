import asyncio
from datetime import UTC, datetime
from logging import getLogger
from typing import Annotated

from bson import ObjectId
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pymongo.asynchronous.database import AsyncDatabase

from app.common.mongo import get_db
from app.config import config
from app.document.models import Document, DocumentCreate
from app.ingest import ingest_document

router = APIRouter()
logger = getLogger(__name__)

COLLECTION = "documents"
KNOWLEDGE_GROUPS_COLLECTION = "knowledgeGroups"


async def _run_ingest_for_document(
    *,
    db: AsyncDatabase,
    document_id: str,
    s3_key: str,
    knowledge_group_id: str,
    cdp_upload_id: str,
) -> None:
    bucket = config.upload_bucket_name
    snapshot_id = f"{cdp_upload_id}:{s3_key}"
    try:
        await db[COLLECTION].update_one(
            {"_id": ObjectId(document_id)},
            {"$set": {"status": "in_progress"}},
        )
        count = await ingest_document(
            bucket=bucket,
            s3_key=s3_key,
            document_id=document_id,
            knowledge_group_id=knowledge_group_id,
            snapshot_id=snapshot_id,
        )
        await db[COLLECTION].update_one(
            {"_id": ObjectId(document_id)},
            {"$set": {"status": "ready", "chunk_count": count}},
        )
        logger.info("Ingested %d chunks for document %s", count, document_id)
    except Exception:  # noqa: BLE001
        logger.exception("Ingest failed for document %s", document_id)
        await db[COLLECTION].update_one(
            {"_id": ObjectId(document_id)},
            {"$set": {"status": "failed"}},
        )


@router.post("/documents", status_code=201)
async def create_documents(
    body: list[DocumentCreate],
    db: Annotated[AsyncDatabase, Depends(get_db)],
) -> None:
    docs = [
        {
            "file_name": d.file_name,
            "status": "not_started",
            "knowledge_group_id": d.knowledge_group_id,
            "cdp_upload_id": d.cdp_upload_id,
            "s3_key": d.s3_key,
            "created_at": datetime.now(UTC),
        }
        for d in body
    ]
    if docs:
        result = await db[COLLECTION].insert_many(docs)
        logger.info("Inserted %d documents", len(docs))

        tasks = []
        for i, doc in enumerate(docs):
            document_id = str(result.inserted_ids[i])
            task = asyncio.create_task(
                _run_ingest_for_document(
                    db=db,
                    document_id=document_id,
                    s3_key=doc["s3_key"],
                    knowledge_group_id=doc["knowledge_group_id"],
                    cdp_upload_id=doc["cdp_upload_id"],
                )
            )
            tasks.append(task)


@router.get("/upload-status/{upload_id}")
async def get_upload_status(
    upload_id: str,
    db: Annotated[AsyncDatabase, Depends(get_db)],
) -> list[Document]:
    cursor = db[COLLECTION].find({"cdp_upload_id": upload_id})
    result = []
    async for doc in cursor:
        result.append(
            Document(
                id=str(doc["_id"]),
                file_name=doc["file_name"],
                status=doc["status"],
                knowledge_group_id=doc["knowledge_group_id"],
                cdp_upload_id=doc["cdp_upload_id"],
                s3_key=doc["s3_key"],
                created_at=doc.get("created_at"),
                chunk_count=doc.get("chunk_count"),
            )
        )
    return result


@router.get(
    "/documents",
    responses={404: {"description": "Knowledge group not found or access denied"}},
)
async def list_documents_by_knowledge_group(
    knowledge_group_id: Annotated[str, Query(..., min_length=1)],
    user_id: Annotated[str, Header(..., alias="user-id")],
    db: Annotated[AsyncDatabase, Depends(get_db)],
) -> list[Document]:
    try:
        group = await db[KNOWLEDGE_GROUPS_COLLECTION].find_one(
            {"_id": ObjectId(knowledge_group_id)}
        )
    except Exception:
        group = None
    if not group:
        raise HTTPException(status_code=404, detail="Knowledge group not found")
    if group.get("created_by") != user_id:
        raise HTTPException(status_code=404, detail="Knowledge group not found")

    cursor = db[COLLECTION].find({"knowledge_group_id": knowledge_group_id})
    result = []
    async for doc in cursor:
        result.append(
            Document(
                id=str(doc["_id"]),
                file_name=doc["file_name"],
                status=doc["status"],
                knowledge_group_id=doc["knowledge_group_id"],
                cdp_upload_id=doc["cdp_upload_id"],
                s3_key=doc["s3_key"],
                created_at=doc.get("created_at"),
                chunk_count=doc.get("chunk_count"),
            )
        )
    return result
