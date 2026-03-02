from datetime import datetime

from pydantic import BaseModel, Field


class DocumentCreate(BaseModel):
    file_name: str = Field(..., min_length=1)
    status: str = Field(default="not_started")
    knowledge_group_id: str = Field(..., min_length=1)
    cdp_upload_id: str = Field(..., min_length=1)


class Document(BaseModel):
    id: str
    file_name: str
    status: str
    knowledge_group_id: str
    cdp_upload_id: str
    s3_path: str | None = None
    created_at: datetime | None = None


class UploadCallbackBody(BaseModel):
    model_config = {"populate_by_name": True}

    upload_status: str = Field(..., alias="uploadStatus")
    s3_path: str | None = Field(default=None, alias="s3Path")
