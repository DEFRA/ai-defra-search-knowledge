import pytest

import app.ingest.vector_store as vector_store_module
from app.ingest.vector_store import close_pool, get_connection, get_pool, insert_vectors


@pytest.fixture(autouse=True)
def reset_pool():
    vector_store_module._pool = None
    yield
    vector_store_module._pool = None


@pytest.mark.asyncio
async def test_get_pool_creates_pool(mocker):
    mock_config = mocker.MagicMock()
    mock_config.postgres.host = "localhost"
    mock_config.postgres.port = 5432
    mock_config.postgres.database = "testdb"
    mock_config.postgres.user = "user"
    mock_config.postgres.password = "secret"  # noqa: S105
    mock_config.postgres.ssl_mode = "require"
    mocker.patch("app.ingest.vector_store.config", mock_config)

    mock_pool_class = mocker.MagicMock()
    mocker.patch(
        "app.ingest.vector_store.AsyncConnectionPool",
        mock_pool_class,
    )

    result = get_pool()

    assert result is mock_pool_class.return_value
    mock_pool_class.assert_called_once()
    call_kwargs = mock_pool_class.call_args[1]
    conninfo = call_kwargs["conninfo"]
    assert "password=secret" in conninfo
    assert "sslmode=require" in conninfo


@pytest.mark.asyncio
async def test_get_pool_without_password_or_ssl(mocker):
    mock_config = mocker.MagicMock()
    mock_config.postgres.host = "h"
    mock_config.postgres.port = 5432
    mock_config.postgres.database = "d"
    mock_config.postgres.user = "u"
    mock_config.postgres.password = None
    mock_config.postgres.ssl_mode = "disable"
    mocker.patch("app.ingest.vector_store.config", mock_config)

    mock_pool_class = mocker.MagicMock()
    mocker.patch(
        "app.ingest.vector_store.AsyncConnectionPool",
        mock_pool_class,
    )

    get_pool()

    conninfo = mock_pool_class.call_args[1]["conninfo"]
    assert "password" not in conninfo
    assert "sslmode" not in conninfo


def test_get_pool_returns_existing(mocker):
    mock_pool = mocker.MagicMock()
    vector_store_module._pool = mock_pool
    assert get_pool() is mock_pool


@pytest.mark.asyncio
async def test_close_pool(mocker):
    mock_pool = mocker.MagicMock()
    mock_pool.close = mocker.AsyncMock()
    vector_store_module._pool = mock_pool

    await close_pool()

    mock_pool.close.assert_awaited_once()
    assert vector_store_module._pool is None


@pytest.mark.asyncio
async def test_get_connection(mocker):
    mock_conn = mocker.MagicMock()
    mock_conn.__aenter__ = mocker.AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = mocker.AsyncMock(return_value=None)
    mock_pool = mocker.MagicMock()
    mock_pool.connection.return_value.__aenter__ = mocker.AsyncMock(
        return_value=mock_conn
    )
    mock_pool.connection.return_value.__aexit__ = mocker.AsyncMock(return_value=None)
    mocker.patch("app.ingest.vector_store.get_pool", return_value=mock_pool)

    async with get_connection() as conn:
        assert conn is mock_conn


@pytest.mark.asyncio
async def test_insert_vectors_empty():
    await insert_vectors([])
    # No DB calls when empty


@pytest.mark.asyncio
async def test_insert_vectors(mocker):
    mock_conn = mocker.MagicMock()
    mock_cur = mocker.MagicMock()
    mock_cur.executemany = mocker.AsyncMock()
    mock_conn.cursor.return_value.__aenter__ = mocker.AsyncMock(return_value=mock_cur)
    mock_conn.cursor.return_value.__aexit__ = mocker.AsyncMock(return_value=None)
    mock_conn.__aenter__ = mocker.AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = mocker.AsyncMock(return_value=None)

    mock_pool = mocker.MagicMock()
    mock_pool.connection.return_value.__aenter__ = mocker.AsyncMock(
        return_value=mock_conn
    )
    mock_pool.connection.return_value.__aexit__ = mocker.AsyncMock(return_value=None)

    mocker.patch("app.ingest.vector_store.get_pool", return_value=mock_pool)

    vectors = [
        ("text1", [0.1] * 1024, "snap-1", "doc-1", {"source": "s1"}),
        ("text2", [0.2] * 1024, "snap-1", "doc-1", None),
    ]
    await insert_vectors(vectors)

    mock_cur.executemany.assert_called_once()
    rows = mock_cur.executemany.call_args[0][1]
    assert len(rows) == 2
    assert rows[0][0] == "text1"
    assert rows[0][4] == '{"source": "s1"}'
    assert rows[1][0] == "text2"
    assert rows[1][4] is None
