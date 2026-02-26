from logging import getLogger

from fastapi import APIRouter, Depends, Header
from pymongo.asynchronous.database import AsyncDatabase

from app.common.mongo import get_db
from app.knowledge_group.models import KnowledgeGroup, KnowledgeGroupCreate

router = APIRouter()
logger = getLogger(__name__)

COLLECTION = "knowledgeGroups"


@router.post("/knowledge-group", response_model=KnowledgeGroup)
async def create_knowledge_group(
    body: KnowledgeGroupCreate,
    user_id: str = Header(..., alias="user-id"),
    db: AsyncDatabase = Depends(get_db),
) -> KnowledgeGroup:
    doc = {
        "name": body.name,
        "description": body.description,
        "created_by": user_id,
    }
    result = await db[COLLECTION].insert_one(doc)
    return KnowledgeGroup(
        id=str(result.inserted_id),
        name=body.name,
        description=body.description,
        created_by=user_id,
    )


@router.get("/knowledge-groups", response_model=list[KnowledgeGroup])
async def list_knowledge_groups(
    user_id: str = Header(..., alias="user-id"),
    db: AsyncDatabase = Depends(get_db),
) -> list[KnowledgeGroup]:
    cursor = db[COLLECTION].find({"created_by": user_id})
    groups = []
    async for doc in cursor:
        groups.append(
            KnowledgeGroup(
                id=str(doc["_id"]),
                name=doc["name"],
                description=doc.get("description"),
                created_by=doc["created_by"],
            )
        )
    return groups
