from logging import getLogger
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import DuplicateKeyError

from app.common.mongo import get_db
from app.knowledge_group.models import KnowledgeGroup, KnowledgeGroupCreate

router = APIRouter()
logger = getLogger(__name__)

COLLECTION = "knowledgeGroups"


@router.post(
    "/knowledge-group",
    responses={409: {"description": "Knowledge group with name already exists"}},
)
async def create_knowledge_group(
    body: KnowledgeGroupCreate,
    user_id: Annotated[str, Header(..., alias="user-id")],
    db: Annotated[AsyncDatabase, Depends(get_db)],
) -> KnowledgeGroup:
    doc = {
        "name": body.name,
        "description": body.description,
        "information_asset_owner": body.information_asset_owner,
        "created_by": user_id,
    }
    try:
        result = await db[COLLECTION].insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(
            status_code=409,
            detail=f"Knowledge group with name '{body.name}' already exists",
        ) from None
    return KnowledgeGroup(
        id=str(result.inserted_id),
        name=body.name,
        description=body.description,
        information_asset_owner=body.information_asset_owner,
        created_by=user_id,
    )


@router.get("/knowledge-groups")
async def list_knowledge_groups(
    user_id: Annotated[str, Header(..., alias="user-id")],
    db: Annotated[AsyncDatabase, Depends(get_db)],
) -> list[KnowledgeGroup]:
    cursor = db[COLLECTION].find({"created_by": user_id})
    groups = []
    async for doc in cursor:
        groups.append(
            KnowledgeGroup(
                id=str(doc["_id"]),
                name=doc["name"],
                description=doc.get("description"),
                information_asset_owner=doc.get("information_asset_owner"),
                created_by=doc["created_by"],
            )
        )
    return groups
