from pydantic import BaseModel, Field


class KnowledgeGroupCreate(BaseModel):
    name: str = Field(..., min_length=1)
    description: str | None = None


class KnowledgeGroup(BaseModel):
    id: str
    name: str
    description: str | None = None
    created_by: str
