import pytest

from app.ingest.vector_store import insert_vectors


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
