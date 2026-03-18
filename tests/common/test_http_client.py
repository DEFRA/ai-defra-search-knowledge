import httpx

from app.common.http_client import (
    _default_timeout,
    _hook_request_tracing,
    create_async_client,
    create_client,
)
from app.common.tracing import ctx_trace_id
from app.config import config


def mock_handler(request):
    request_id = request.headers.get("x-cdp-request-id", "")
    return httpx.Response(200, text=request_id)


def test_trace_id_missing():
    ctx_trace_id.set("")
    client = httpx.Client(
        event_hooks={"request": [_hook_request_tracing]},
        transport=httpx.MockTransport(mock_handler),
    )
    resp = client.get("http://localhost:1234/test")
    assert resp.text == ""


def test_trace_id_set():
    ctx_trace_id.set("trace-id-value")
    client = httpx.Client(
        event_hooks={"request": [_hook_request_tracing]},
        transport=httpx.MockTransport(mock_handler),
    )
    resp = client.get("http://localhost:1234/test")
    assert resp.text == "trace-id-value"


def test_default_timeout_uses_config_values():
    timeout = _default_timeout()
    assert isinstance(timeout, httpx.Timeout)
    assert timeout.connect == config.timeouts.http_connect_timeout
    assert timeout.read == config.timeouts.http_read_timeout
    assert timeout.write == config.timeouts.http_read_timeout
    assert timeout.pool == config.timeouts.http_connect_timeout


def test_create_client_returns_sync_client_with_hook():
    client = create_client()
    assert isinstance(client, httpx.Client)
    assert _hook_request_tracing in client.event_hooks["request"]


def test_create_async_client_returns_async_client_with_hook():
    client = create_async_client()
    assert isinstance(client, httpx.AsyncClient)
    assert _hook_request_tracing in client.event_hooks["request"]
