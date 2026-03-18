from logging import getLogger

import httpx

from app.common.tracing import ctx_trace_id
from app.config import config

logger = getLogger(__name__)


def _hook_request_tracing(request):
    trace_id = ctx_trace_id.get(None)
    if trace_id:
        request.headers[config.tracing_header] = trace_id


def _default_timeout() -> httpx.Timeout:
    return httpx.Timeout(
        connect=config.timeouts.http_connect_timeout,
        read=config.timeouts.http_read_timeout,
        write=config.timeouts.http_read_timeout,
        pool=config.timeouts.http_connect_timeout,
    )


def create_async_client() -> httpx.AsyncClient:
    """
    Create an async HTTP client using timeouts from config.

    Returns:
        Configured httpx.AsyncClient instance
    """
    return httpx.AsyncClient(
        timeout=_default_timeout(),
        event_hooks={"request": [_hook_request_tracing]},
    )


def create_client() -> httpx.Client:
    """
    Create a sync HTTP client using timeouts from config.

    Returns:
        Configured httpx.Client instance
    """
    return httpx.Client(
        timeout=_default_timeout(),
        event_hooks={"request": [_hook_request_tracing]},
    )
