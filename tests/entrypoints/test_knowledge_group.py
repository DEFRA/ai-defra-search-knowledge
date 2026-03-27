import pytest
from fastapi.testclient import TestClient
from pymongo.errors import DuplicateKeyError

from app.common.mongo import get_db
from app.main import app


@pytest.fixture
def mock_db(mocker):
    mock = mocker.MagicMock()
    mock_collection = mocker.MagicMock()
    mock_collection.insert_one = mocker.AsyncMock(
        return_value=mocker.MagicMock(inserted_id="507f1f77bcf86cd799439011")
    )

    async def empty_cursor():
        return
        yield  # makes it async generator

    mock_collection.find = mocker.MagicMock(return_value=empty_cursor())
    mock.__getitem__ = lambda _s, _k: mock_collection
    return mock


@pytest.fixture(autouse=True)
def override_db(mock_db, mocker):
    # Mock mongo client so lifespan doesn't connect to real MongoDB
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


def test_create_knowledge_group():
    client = TestClient(app)
    response = client.post(
        "/knowledge-group",
        json={"name": "My Group"},
        headers={"user-id": "user-123", "X-API-KEY": "test-key"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "My Group"
    assert data["created_by"] == "user-123"
    assert "id" in data


def test_create_knowledge_group_with_description():
    client = TestClient(app)
    response = client.post(
        "/knowledge-group",
        json={"name": "My Group", "description": "Test desc"},
        headers={"user-id": "user-456", "X-API-KEY": "test-key"},
    )
    assert response.status_code == 200
    assert response.json()["name"] == "My Group"
    assert response.json()["description"] == "Test desc"
    assert response.json()["created_by"] == "user-456"


def test_create_knowledge_group_with_information_asset_owner():
    client = TestClient(app)
    response = client.post(
        "/knowledge-group",
        json={
            "name": "My Group",
            "description": "Test desc",
            "information_asset_owner": "Jane Smith",
        },
        headers={"user-id": "user-789", "X-API-KEY": "test-key"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "My Group"
    assert data["description"] == "Test desc"
    assert data["information_asset_owner"] == "Jane Smith"
    assert data["created_by"] == "user-789"


def test_create_knowledge_group_missing_user_id():
    client = TestClient(app)
    response = client.post(
        "/knowledge-group",
        json={"name": "Test"},
        headers={"X-API-KEY": "test-key"},
    )
    assert response.status_code == 422


def test_create_knowledge_group_duplicate_name(mock_db, mocker):
    mock_db["knowledgeGroups"].insert_one = mocker.AsyncMock(
        side_effect=DuplicateKeyError("duplicate")
    )
    client = TestClient(app)
    response = client.post(
        "/knowledge-group",
        json={"name": "My Group"},
        headers={"user-id": "user-123", "X-API-KEY": "test-key"},
    )
    assert response.status_code == 409
    assert "already exists" in response.json()["detail"]


def test_list_knowledge_groups(mock_db):
    client = TestClient(app)
    response = client.get(
        "/knowledge-groups", headers={"user-id": "user-123", "X-API-KEY": "test-key"}
    )
    assert response.status_code == 200
    assert response.json() == []
    mock_db["knowledgeGroups"].find.assert_called_once_with({"created_by": "user-123"})


def test_list_knowledge_groups_missing_user_id():
    client = TestClient(app)
    response = client.get("/knowledge-groups", headers={"X-API-KEY": "test-key"})
    assert response.status_code == 422


def test_no_api_key_returns_401():
    client = TestClient(app)
    response = client.post(
        "/knowledge-group",
        json={"name": "My Group"},
        headers={"user-id": "user-123"},
    )
    assert response.status_code == 401


def test_wrong_api_key_returns_403():
    client = TestClient(app)
    response = client.post(
        "/knowledge-group",
        json={"name": "My Group"},
        headers={"user-id": "user-123", "X-API-KEY": "wrong-key"},
    )
    assert response.status_code == 403
