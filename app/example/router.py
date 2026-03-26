from logging import getLogger
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pymongo.asynchronous.database import AsyncDatabase

from app.common.http_client import create_async_client
from app.common.mongo import get_db
from app.config import config

router = APIRouter(prefix="/example")
logger = getLogger(__name__)


# basic endpoint example
@router.get("/test")
async def root():
    logger.info("TEST ENDPOINT")
    return {"ok": True}


# database endpoint example
@router.get("/db")
async def db_query(db: Annotated[AsyncDatabase, Depends(get_db)]):
    await db.example.insert_one({"foo": "bar"})
    data = await db.example.find_one({}, {"_id": 0})
    return {"ok": data}


# http client endpoint example
@router.get("/http")
async def http_query(
    client: Annotated[httpx.AsyncClient, Depends(create_async_client)],
):
    endpoint = config.localstack_s3_endpoint_url
    if not endpoint:
        raise HTTPException(
            status_code=503,
            detail="LOCALSTACK_S3_ENDPOINT_URL must be set for this example endpoint",
        )
    base = endpoint.rstrip("/")
    resp = await client.get(f"{base}/health")
    return {"ok": resp.status_code}
