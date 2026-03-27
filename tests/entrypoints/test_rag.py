import pytest
from bson import ObjectId
from fastapi.testclient import TestClient

from app.common.mongo import get_db
from app.main import app

# ---------------------------------------------------------------------------
# Test data constants
# ---------------------------------------------------------------------------

USER_ID = "user-1"
GROUP_ID_A = "507f1f77bcf86cd799439011"
GROUP_ID_B = "507f1f77bcf86cd799439012"

FAKE_EMBEDDING = [0.1] * 1024

RESULT_HIGH = {
    "content": "high content",
    "similarity_score": 0.95,
    "document_id": "doc-1",
}
RESULT_MID = {
    "content": "mid content",
    "similarity_score": 0.75,
    "document_id": "doc-2",
}

DOC_ID_AAA = "507f1f77bcf86cd799439021"
DOC_ID_BBB = "507f1f77bcf86cd799439022"

RESULT_WITH_DOC_AAA = {
    "content": "content aaa",
    "similarity_score": 0.90,
    "document_id": DOC_ID_AAA,
}
RESULT_WITH_DOC_BBB = {
    "content": "content bbb",
    "similarity_score": 0.80,
    "document_id": DOC_ID_BBB,
}

MONGO_DOC_AAA = {
    "_id": ObjectId(DOC_ID_AAA),
    "file_name": "report-aaa.pdf",
    "s3_key": "uploads/group-1/batch-1/report-aaa",
}
MONGO_DOC_BBB = {
    "_id": ObjectId(DOC_ID_BBB),
    "file_name": "report-bbb.pdf",
    "s3_key": "uploads/group-1/batch-1/report-bbb",
}


async def _empty_async_iter():
    return
    yield


def _make_async_iter(docs):
    async def _gen():
        for doc in docs:
            yield doc

    return _gen()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db(mocker):
    mock = mocker.MagicMock()

    knowledge_groups_collection = mocker.MagicMock()
    knowledge_groups_collection.find_one = mocker.AsyncMock(
        return_value={"_id": GROUP_ID_A, "created_by": USER_ID}
    )

    documents_collection = mocker.MagicMock()
    documents_collection.find = mocker.MagicMock(return_value=_empty_async_iter())

    def _getitem(_s, key):
        if key == "documents":
            return documents_collection
        if key == "knowledgeGroups":
            return knowledge_groups_collection
        msg = f"Unexpected collection access: {key!r}"
        raise KeyError(msg)

    mock.__getitem__ = _getitem
    return mock


@pytest.fixture(autouse=True)
def override_db(mock_db, mocker):
    mock_client = mocker.MagicMock()
    mock_client.close = mocker.AsyncMock()
    mocker.patch(
        "app.main.get_mongo_client", mocker.AsyncMock(return_value=mock_client)
    )

    async def _get_db():
        return mock_db

    app.dependency_overrides[get_db] = _get_db
    yield
    app.dependency_overrides.pop(get_db, None)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_search_returns_ranked_results(mocker):
    mocker.patch(
        "app.rag.router.BedrockEmbeddingService.generate_embeddings",
        return_value=FAKE_EMBEDDING,
    )
    mocker.patch(
        "app.rag.router.search_vectors",
        new=mocker.AsyncMock(return_value=[RESULT_HIGH, RESULT_MID]),
    )

    response = TestClient(app).post(
        "/rag/search",
        json={
            "knowledge_group_ids": [GROUP_ID_A, GROUP_ID_B],
            "query": "flood risk",
            "max_results": 3,
        },
        headers={"user-id": USER_ID, "X-API-KEY": "test-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["similarity_score"] >= data[1]["similarity_score"]
    assert set(data[0].keys()) == {
        "content",
        "similarity_score",
        "document_id",
        "file_name",
        "s3_key",
    }


def test_search_returns_empty_list_when_no_vectors_exist(mocker):
    mocker.patch(
        "app.rag.router.BedrockEmbeddingService.generate_embeddings",
        return_value=FAKE_EMBEDDING,
    )
    mocker.patch("app.rag.router.search_vectors", new=mocker.AsyncMock(return_value=[]))

    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A], "query": "air quality"},
        headers={"user-id": USER_ID, "X-API-KEY": "test-key"},
    )

    assert response.status_code == 200
    assert response.json() == []


# ---------------------------------------------------------------------------
# Invalid requests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("payload", "headers"),
    [
        (
            {"knowledge_group_ids": [GROUP_ID_A], "query": "flood"},
            {"X-API-KEY": "test-key"},
        ),  # missing user-id header
        (
            {"query": "flood"},
            {"user-id": USER_ID, "X-API-KEY": "test-key"},
        ),  # missing knowledge_group_ids
        (
            {"knowledge_group_ids": [GROUP_ID_A], "query": ""},
            {"user-id": USER_ID, "X-API-KEY": "test-key"},
        ),  # empty query
        (
            {"knowledge_group_ids": [GROUP_ID_A], "query": "flood", "max_results": 0},
            {"user-id": USER_ID, "X-API-KEY": "test-key"},
        ),  # invalid max_results
    ],
)
def test_search_rejects_invalid_request(payload, headers):
    response = TestClient(app).post("/rag/search", json=payload, headers=headers)
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Group not found
# ---------------------------------------------------------------------------


def test_search_group_not_found(mock_db, mocker):
    mock_db["knowledgeGroups"].find_one = mocker.AsyncMock(return_value=None)

    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A], "query": "flood risk"},
        headers={"user-id": USER_ID, "X-API-KEY": "test-key"},
    )

    assert response.status_code == 404
    assert f"Knowledge group '{GROUP_ID_A}' not found" in response.json()["detail"]


def test_search_group_belongs_to_different_user(mock_db, mocker):
    mock_db["knowledgeGroups"].find_one = mocker.AsyncMock(return_value=None)

    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A], "query": "flood risk"},
        headers={"user-id": "other-user", "X-API-KEY": "test-key"},
    )

    assert response.status_code == 404


# ---------------------------------------------------------------------------
# Upstream failures
# ---------------------------------------------------------------------------


def test_search_bedrock_failure(mocker):
    mocker.patch(
        "app.rag.router.BedrockEmbeddingService.generate_embeddings",
        side_effect=RuntimeError("Bedrock unavailable"),
    )
    mock_search = mocker.AsyncMock()
    mocker.patch("app.rag.router.search_vectors", new=mock_search)

    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A], "query": "flood risk"},
        headers={"user-id": USER_ID, "X-API-KEY": "test-key"},
    )

    assert response.status_code == 502
    assert response.json()["detail"] == "Embedding generation failed"
    mock_search.assert_not_called()


def test_search_postgres_failure(mocker):
    mocker.patch(
        "app.rag.router.BedrockEmbeddingService.generate_embeddings",
        return_value=FAKE_EMBEDDING,
    )
    mocker.patch(
        "app.rag.router.search_vectors",
        new=mocker.AsyncMock(side_effect=RuntimeError("Postgres down")),
    )

    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A], "query": "flood risk"},
        headers={"user-id": USER_ID, "X-API-KEY": "test-key"},
    )

    assert response.status_code == 502
    assert response.json()["detail"] == "Vector search failed"


# ---------------------------------------------------------------------------
# Document metadata enrichment
# ---------------------------------------------------------------------------


def test_search_returns_file_name_and_s3_key_when_documents_exist(mock_db, mocker):
    mock_db["documents"].find = mocker.MagicMock(
        return_value=_make_async_iter([MONGO_DOC_AAA, MONGO_DOC_BBB])
    )
    mocker.patch(
        "app.rag.router.BedrockEmbeddingService.generate_embeddings",
        return_value=FAKE_EMBEDDING,
    )
    mocker.patch(
        "app.rag.router.search_vectors",
        new=mocker.AsyncMock(return_value=[RESULT_WITH_DOC_AAA, RESULT_WITH_DOC_BBB]),
    )

    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A], "query": "flood risk"},
        headers={"user-id": USER_ID, "X-API-KEY": "test-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    result_aaa = next(r for r in data if r["document_id"] == DOC_ID_AAA)
    result_bbb = next(r for r in data if r["document_id"] == DOC_ID_BBB)
    assert result_aaa["file_name"] == MONGO_DOC_AAA["file_name"]
    assert result_aaa["s3_key"] == MONGO_DOC_AAA["s3_key"]
    assert result_bbb["file_name"] == MONGO_DOC_BBB["file_name"]
    assert result_bbb["s3_key"] == MONGO_DOC_BBB["s3_key"]


def test_search_returns_empty_strings_for_result_with_no_matching_document(
    mock_db, mocker
):
    mock_db["documents"].find = mocker.MagicMock(
        return_value=_make_async_iter([MONGO_DOC_AAA])
    )
    mocker.patch(
        "app.rag.router.BedrockEmbeddingService.generate_embeddings",
        return_value=FAKE_EMBEDDING,
    )
    result_missing = {
        "content": "missing content",
        "similarity_score": 0.60,
        "document_id": "507f1f77bcf86cd799439099",
    }
    mocker.patch(
        "app.rag.router.search_vectors",
        new=mocker.AsyncMock(return_value=[RESULT_WITH_DOC_AAA, result_missing]),
    )

    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A], "query": "flood risk"},
        headers={"user-id": USER_ID, "X-API-KEY": "test-key"},
    )

    assert response.status_code == 200
    data = response.json()
    result_aaa = next(r for r in data if r["document_id"] == DOC_ID_AAA)
    result_no_doc = next(
        r for r in data if r["document_id"] == result_missing["document_id"]
    )
    assert result_aaa["file_name"] == MONGO_DOC_AAA["file_name"]
    assert result_aaa["s3_key"] == MONGO_DOC_AAA["s3_key"]
    assert result_no_doc["file_name"] == ""
    assert result_no_doc["s3_key"] == ""


def test_search_returns_empty_strings_when_no_documents_found_in_mongo(mock_db, mocker):
    mock_db["documents"].find = mocker.MagicMock(return_value=_make_async_iter([]))
    mocker.patch(
        "app.rag.router.BedrockEmbeddingService.generate_embeddings",
        return_value=FAKE_EMBEDDING,
    )
    mocker.patch(
        "app.rag.router.search_vectors",
        new=mocker.AsyncMock(return_value=[RESULT_WITH_DOC_AAA]),
    )

    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A], "query": "flood risk"},
        headers={"user-id": USER_ID, "X-API-KEY": "test-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["file_name"] == ""
    assert data[0]["s3_key"] == ""


def test_search_skips_mongo_lookup_when_vector_search_returns_no_results(
    mock_db, mocker
):
    mocker.patch(
        "app.rag.router.BedrockEmbeddingService.generate_embeddings",
        return_value=FAKE_EMBEDDING,
    )
    mocker.patch(
        "app.rag.router.search_vectors",
        new=mocker.AsyncMock(return_value=[]),
    )

    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A], "query": "flood risk"},
        headers={"user-id": USER_ID, "X-API-KEY": "test-key"},
    )

    assert response.status_code == 200
    assert response.json() == []
    mock_db["documents"].find.assert_not_called()


def test_no_api_key_returns_401():
    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A], "query": "flood risk"},
        headers={"user-id": USER_ID},
    )
    assert response.status_code == 401


def test_wrong_api_key_returns_403():
    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A], "query": "flood risk"},
        headers={"user-id": USER_ID, "X-API-KEY": "wrong-key"},
    )
    assert response.status_code == 403
