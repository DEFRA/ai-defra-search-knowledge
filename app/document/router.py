import asyncio
from datetime import UTC, datetime
from logging import getLogger
from typing import Annotated

from fastapi import APIRouter, Depends
from pymongo.asynchronous.database import AsyncDatabase

from app.common.mongo import get_db
from app.config import config
from app.document.models import Document, DocumentCreate
from app.ingest import ingest_document

router = APIRouter()
logger = getLogger(__name__)

COLLECTION = "documents"


async def _run_ingest_for_document(
    *,
    document_id: str,
    s3_key: str,
    knowledge_group_id: str,
    cdp_upload_id: str,
) -> None:
    bucket = config.upload_bucket_name
    snapshot_id = f"{cdp_upload_id}:{s3_key}"
    try:
        count = await ingest_document(
            bucket=bucket,
            s3_key=s3_key,
            document_id=document_id,
            knowledge_group_id=knowledge_group_id,
            snapshot_id=snapshot_id,
        )
        logger.info("Ingested %d chunks for document %s", count, document_id)
    except Exception:  # noqa: BLE001
        logger.exception("Ingest failed for document %s", document_id)


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

        for i, doc in enumerate(docs):
            document_id = str(result.inserted_ids[i])
            asyncio.create_task(
                _run_ingest_for_document(
                    document_id=document_id,
                    s3_key=doc["s3_key"],
                    knowledge_group_id=doc["knowledge_group_id"],
                    cdp_upload_id=doc["cdp_upload_id"],
                )
            )


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
            )
        )
    return result
