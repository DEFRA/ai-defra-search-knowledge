import pytest

import app.common.postgres as postgres_module
from app.ingest.vector_store import close_pool, get_connection, insert_vectors


@pytest.fixture(autouse=True)
def reset_engine():
    postgres_module.engine = None
    yield
    postgres_module.engine = None


@pytest.mark.asyncio
async def test_get_connection(mocker):
    mock_conn = mocker.MagicMock()
    mock_conn.__aenter__ = mocker.AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = mocker.AsyncMock(return_value=None)
    mock_engine = mocker.MagicMock()
    mock_engine.connect.return_value.__aenter__ = mocker.AsyncMock(
        return_value=mock_conn
    )
    mock_engine.connect.return_value.__aexit__ = mocker.AsyncMock(return_value=None)
    mocker.patch(
        "app.ingest.vector_store.get_sql_engine",
        new=mocker.AsyncMock(return_value=mock_engine),
    )

    async with get_connection() as conn:
        assert conn is mock_conn


@pytest.mark.asyncio
async def test_close_pool(mocker):
    mock_close = mocker.AsyncMock()
    mocker.patch(
        "app.common.postgres.close_engine",
        new=mock_close,
    )

    await close_pool()

    mock_close.assert_awaited_once()


@pytest.mark.asyncio
async def test_insert_vectors_empty():
    await insert_vectors([])
    # No DB calls when empty


@pytest.mark.asyncio
async def test_insert_vectors(mocker):
    mock_conn = mocker.MagicMock()
    mock_conn.execute = mocker.AsyncMock()
    mock_conn.commit = mocker.AsyncMock()
    mock_conn.__aenter__ = mocker.AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = mocker.AsyncMock(return_value=None)

    mock_engine = mocker.MagicMock()
    mock_engine.connect.return_value.__aenter__ = mocker.AsyncMock(
        return_value=mock_conn
    )
    mock_engine.connect.return_value.__aexit__ = mocker.AsyncMock(return_value=None)
    mocker.patch(
        "app.ingest.vector_store.get_sql_engine",
        new=mocker.AsyncMock(return_value=mock_engine),
    )

    vectors = [
        ("text1", [0.1] * 1024, "snap-1", "doc-1", {"source": "s1"}),
        ("text2", [0.2] * 1024, "snap-1", "doc-1", None),
    ]
    await insert_vectors(vectors)

    mock_conn.execute.assert_called_once()
    params = mock_conn.execute.call_args[0][1]
    assert len(params) == 2
    assert params[0]["content"] == "text1"
    assert params[0]["metadata"] == '{"source": "s1"}'
    assert params[1]["content"] == "text2"
    assert params[1]["metadata"] is None
