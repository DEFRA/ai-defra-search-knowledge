from datetime import UTC, datetime
from logging import getLogger
from typing import Annotated

from fastapi import APIRouter, Depends
from pymongo.asynchronous.database import AsyncDatabase

from app.common.mongo import get_db
from app.document.models import Document, DocumentCreate, UploadCallbackBody

router = APIRouter()
logger = getLogger(__name__)

COLLECTION = "documents"


@router.post("/documents", status_code=201)
async def create_documents(
    body: list[DocumentCreate],
    db: Annotated[AsyncDatabase, Depends(get_db)],
) -> None:
    docs = [
        {
            "file_name": d.file_name,
            "status": d.status,
            "knowledge_group_id": d.knowledge_group_id,
            "cdp_upload_id": d.cdp_upload_id,
            "created_at": datetime.now(UTC),
        }
        for d in body
    ]
    if docs:
        await db[COLLECTION].insert_many(docs)
    logger.info("Inserted %d documents", len(docs))


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
                s3_path=doc.get("s3_path"),
                created_at=doc.get("created_at"),
            )
        )
    return result


@router.post("/upload-callback/{upload_id}")
async def upload_callback(
    upload_id: str,
    body: UploadCallbackBody,
    db: Annotated[AsyncDatabase, Depends(get_db)],
) -> None:
    if body.upload_status != "ready":
        return

    update: dict = {"status": "ready"}
    if body.s3_path is not None:
        update["s3_path"] = body.s3_path

    result = await db[COLLECTION].update_many(
        {"cdp_upload_id": upload_id},
        {"$set": update},
    )

    logger.info(
        "Ingest started for upload %s: updated %d documents",
        upload_id,
        result.modified_count,
    )
