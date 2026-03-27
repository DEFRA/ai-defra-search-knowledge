import fastapi
from fastapi import Security, status
from fastapi.security import APIKeyHeader

from app.config import config

_api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)


def verify_api_key(key: str | None = Security(_api_key_header)) -> None:
    if key is None:
        raise fastapi.HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    if key != config.api_key:
        raise fastapi.HTTPException(status_code=status.HTTP_403_FORBIDDEN)
