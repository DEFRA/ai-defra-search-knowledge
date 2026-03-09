from pydantic import BaseModel, Field, field_validator


class RagSearchRequest(BaseModel):
    knowledge_group_ids: list[str] = Field(..., min_length=1)
    query: str
    max_results: int = Field(default=5, ge=1, le=100)

    @field_validator("query")
    @classmethod
    def query_must_not_be_whitespace(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("query must not be empty or whitespace-only")
        return v


class RagSearchResult(BaseModel):
    content: str
    similarity_score: float
    document_id: str
