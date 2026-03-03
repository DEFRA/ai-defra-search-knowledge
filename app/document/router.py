from datetime import UTC, datetime
from logging import getLogger
from typing import Annotated

from fastapi import APIRouter, Depends
from pymongo.asynchronous.database import AsyncDatabase

from app.common.mongo import get_db
from app.config import config
from app.document.models import (
    CallbackFileInfo,
    Document,
    DocumentCreate,
    UploadCallbackBody,
)
from app.ingest import ingest_document

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


def _extract_files_from_form(form: dict) -> list[CallbackFileInfo]:
    files: list[CallbackFileInfo] = []
    if form.get("file"):
        raw = form["file"]
        files.append(CallbackFileInfo.model_validate(raw))
    elif form.get("files"):
        for raw in form["files"]:
            files.append(CallbackFileInfo.model_validate(raw))
    return files


@router.post("/upload-callback/{upload_id}")
async def upload_callback(
    upload_id: str,
    body: UploadCallbackBody,
    db: Annotated[AsyncDatabase, Depends(get_db)],
) -> None:
    if body.upload_status != "ready":
        return

    files = _extract_files_from_form(body.form)
    updated = 0

    bucket = config.upload_bucket_name

    for file_info in files:
        if file_info.file_status != "complete":
            continue
        s3_path = file_info.file_id
        doc_filter = {"cdp_upload_id": upload_id, "file_name": file_info.filename}
        doc = await db[COLLECTION].find_one(doc_filter)
        if not doc:
            logger.warning(
                "No document matched for file %s in upload %s",
                file_info.filename,
                upload_id,
            )
            continue

        await db[COLLECTION].update_one(
            doc_filter,
            {"$set": {"status": "ready", "s3_path": s3_path}},
        )
        updated += 1

        document_id = str(doc["_id"])
        knowledge_group_id = doc["knowledge_group_id"]
        snapshot_id = f"{upload_id}:{s3_path}"

        try:
            count = await ingest_document(
                bucket=bucket,
                s3_key=s3_path,
                document_id=document_id,
                knowledge_group_id=knowledge_group_id,
                snapshot_id=snapshot_id,
            )
            logger.info(
                "Ingested %d chunks for document %s (file %s)",
                count,
                document_id,
                file_info.filename,
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "Ingest failed for document %s (s3://%s/%s)",
                document_id,
                bucket,
                s3_path,
            )

    if body.number_of_rejected_files > 0:
        logger.info(
            "Upload %s had %d rejected files",
            upload_id,
            body.number_of_rejected_files,
        )

    logger.info(
        "Ingest started for upload %s: updated %d documents",
        upload_id,
        updated,
    )
