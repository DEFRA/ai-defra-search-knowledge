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


class CallbackFileInfo(BaseModel):
    model_config = {"populate_by_name": True}

    file_id: str = Field(..., alias="fileId")
    filename: str = Field(..., alias="filename")
    content_type: str | None = Field(default=None, alias="contentType")
    file_status: str | None = Field(default=None, alias="fileStatus")
    content_length: int | None = Field(default=None, alias="contentLength")


class CallbackMetadata(BaseModel):
    model_config = {"populate_by_name": True}

    reference: str | None = None
    customer_id: str | None = Field(default=None, alias="customerId")


class UploadCallbackBody(BaseModel):
    model_config = {"populate_by_name": True}

    upload_status: str = Field(..., alias="uploadStatus")
    metadata: CallbackMetadata | None = None
    form: dict = Field(default_factory=dict)
    number_of_rejected_files: int = Field(default=0, alias="numberOfRejectedFiles")
