import pytest

from app.rag.vector_search import search_vectors

FAKE_EMBEDDING = [0.1] * 1024
GROUP_IDS = ["kg-abc", "kg-xyz"]


def _make_mock_conn(mocker, rows):
    """Build a mock psycopg async connection/cursor returning `rows`."""
    mock_cur = mocker.MagicMock()
    mock_cur.execute = mocker.AsyncMock()
    mock_cur.fetchall = mocker.AsyncMock(return_value=rows)
    mock_cur.__aenter__ = mocker.AsyncMock(return_value=mock_cur)
    mock_cur.__aexit__ = mocker.AsyncMock(return_value=None)

    mock_conn = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_conn.__aenter__ = mocker.AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = mocker.AsyncMock(return_value=None)

    return mock_conn, mock_cur


@pytest.mark.asyncio
async def test_search_vectors_executes_query_with_correct_limit(mocker):
    mock_conn, mock_cur = _make_mock_conn(mocker, [])
    mocker.patch(
        "app.rag.vector_search.get_connection",
        return_value=mock_conn,
    )

    await search_vectors(FAKE_EMBEDDING, GROUP_IDS, max_results=7)

    mock_cur.execute.assert_awaited_once()
    call_args = mock_cur.execute.call_args[0]
    # params tuple: (embedding, group_ids, embedding, max_results)
    params = call_args[1]
    assert params[3] == 7


@pytest.mark.asyncio
async def test_search_vectors_scopes_query_to_knowledge_group_ids(mocker):
    mock_conn, mock_cur = _make_mock_conn(mocker, [])
    mocker.patch(
        "app.rag.vector_search.get_connection",
        return_value=mock_conn,
    )

    await search_vectors(FAKE_EMBEDDING, GROUP_IDS, max_results=5)

    call_args = mock_cur.execute.call_args[0]
    params = call_args[1]
    assert params[1] == GROUP_IDS


@pytest.mark.asyncio
async def test_search_vectors_returns_content_similarity_score_and_document_id(mocker):
    rows = [
        ("chunk text", "doc-abc", 0.87),
        ("other text", "doc-xyz", 0.60),
    ]
    mock_conn, _mock_cur = _make_mock_conn(mocker, rows)
    mocker.patch(
        "app.rag.vector_search.get_connection",
        return_value=mock_conn,
    )

    results = await search_vectors(FAKE_EMBEDDING, GROUP_IDS, max_results=5)

    assert len(results) == 2
    assert results[0] == {
        "content": "chunk text",
        "document_id": "doc-abc",
        "similarity_score": 0.87,
    }
    assert results[1] == {
        "content": "other text",
        "document_id": "doc-xyz",
        "similarity_score": 0.60,
    }


@pytest.mark.asyncio
async def test_search_vectors_returns_empty_list_when_no_rows_match(mocker):
    mock_conn, _mock_cur = _make_mock_conn(mocker, [])
    mocker.patch(
        "app.rag.vector_search.get_connection",
        return_value=mock_conn,
    )

    results = await search_vectors(FAKE_EMBEDDING, GROUP_IDS, max_results=5)

    assert results == []


@pytest.mark.asyncio
async def test_search_vectors_propagates_postgres_exception(mocker):
    mock_conn, mock_cur = _make_mock_conn(mocker, [])
    mock_cur.execute = mocker.AsyncMock(side_effect=RuntimeError("Postgres down"))
    mocker.patch(
        "app.rag.vector_search.get_connection",
        return_value=mock_conn,
    )

    with pytest.raises(RuntimeError, match="Postgres down"):
        await search_vectors(FAKE_EMBEDDING, GROUP_IDS, max_results=5)
