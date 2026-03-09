import pytest
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

RESULT_HIGH = {"content": "high content", "similarity_score": 0.95, "document_id": "doc-1"}
RESULT_MID = {"content": "mid content", "similarity_score": 0.75, "document_id": "doc-2"}

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db(mocker):
    mock = mocker.MagicMock()
    mock_collection = mocker.MagicMock()
    mock_collection.find_one = mocker.AsyncMock(return_value={"_id": GROUP_ID_A, "created_by": USER_ID})
    mock.__getitem__ = lambda _s, _k: mock_collection
    return mock


@pytest.fixture(autouse=True)
def override_db(mock_db, mocker):
    mock_client = mocker.MagicMock()
    mock_client.close = mocker.AsyncMock()
    mocker.patch("app.main.get_mongo_client", mocker.AsyncMock(return_value=mock_client))

    async def _get_db():
        return mock_db

    app.dependency_overrides[get_db] = _get_db
    yield
    app.dependency_overrides.pop(get_db, None)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_search_returns_ranked_results(mocker):
    mocker.patch("app.rag.router.BedrockEmbeddingService.generate_embeddings", return_value=FAKE_EMBEDDING)
    mocker.patch("app.rag.router.search_vectors", new=mocker.AsyncMock(return_value=[RESULT_HIGH, RESULT_MID]))

    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A, GROUP_ID_B], "query": "flood risk", "max_results": 3},
        headers={"user-id": USER_ID},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["similarity_score"] >= data[1]["similarity_score"]
    assert set(data[0].keys()) == {"content", "similarity_score", "document_id"}


def test_search_returns_empty_list_when_no_vectors_exist(mocker):
    mocker.patch("app.rag.router.BedrockEmbeddingService.generate_embeddings", return_value=FAKE_EMBEDDING)
    mocker.patch("app.rag.router.search_vectors", new=mocker.AsyncMock(return_value=[]))

    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A], "query": "air quality"},
        headers={"user-id": USER_ID},
    )

    assert response.status_code == 200
    assert response.json() == []


# ---------------------------------------------------------------------------
# Invalid requests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("payload,headers", [
    ({"knowledge_group_ids": [GROUP_ID_A], "query": "flood"}, {}),                          # missing user-id header
    ({"query": "flood"}, {"user-id": USER_ID}),                                             # missing knowledge_group_ids
    ({"knowledge_group_ids": [GROUP_ID_A], "query": ""}, {"user-id": USER_ID}),             # empty query
    ({"knowledge_group_ids": [GROUP_ID_A], "query": "flood", "max_results": 0}, {"user-id": USER_ID}),  # invalid max_results
])
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
        headers={"user-id": USER_ID},
    )

    assert response.status_code == 404
    assert f"Knowledge group '{GROUP_ID_A}' not found" in response.json()["detail"]


def test_search_group_belongs_to_different_user(mock_db, mocker):
    mock_db["knowledgeGroups"].find_one = mocker.AsyncMock(return_value=None)

    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A], "query": "flood risk"},
        headers={"user-id": "other-user"},
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
        headers={"user-id": USER_ID},
    )

    assert response.status_code == 502
    assert response.json()["detail"] == "Embedding generation failed"
    mock_search.assert_not_called()


def test_search_postgres_failure(mocker):
    mocker.patch("app.rag.router.BedrockEmbeddingService.generate_embeddings", return_value=FAKE_EMBEDDING)
    mocker.patch("app.rag.router.search_vectors", new=mocker.AsyncMock(side_effect=RuntimeError("Postgres down")))

    response = TestClient(app).post(
        "/rag/search",
        json={"knowledge_group_ids": [GROUP_ID_A], "query": "flood risk"},
        headers={"user-id": USER_ID},
    )

    assert response.status_code == 502
    assert response.json()["detail"] == "Vector search failed"
