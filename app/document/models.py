from datetime import datetime

from pydantic import BaseModel, Field


class DocumentCreate(BaseModel):
    file_name: str = Field(..., min_length=1)
    knowledge_group_id: str = Field(..., min_length=1)
    cdp_upload_id: str = Field(..., min_length=1)
    s3_key: str = Field(..., min_length=1)


class Document(BaseModel):
    id: str
    file_name: str
    status: str
    knowledge_group_id: str
    cdp_upload_id: str
    s3_key: str
    created_at: datetime | None = None
    chunk_count: int | None = None
