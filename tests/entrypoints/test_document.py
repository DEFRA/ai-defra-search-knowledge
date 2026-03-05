import asyncio

import pytest
from bson import ObjectId
from fastapi.testclient import TestClient

from app.common.mongo import get_db
from app.main import app


@pytest.fixture
def mock_db(mocker):
    docs_collection = mocker.MagicMock()
    oid1 = ObjectId()
    oid2 = ObjectId()
    docs_collection.insert_many = mocker.AsyncMock(
        return_value=mocker.MagicMock(inserted_ids=[oid1, oid2])
    )
    docs_collection.update_one = mocker.AsyncMock()

    async def docs_cursor_empty():
        return
        yield

    docs_collection.find = mocker.MagicMock(return_value=docs_cursor_empty())

    mock = mocker.MagicMock()
    mock.__getitem__ = (
        lambda _s, key: docs_collection if key == "documents" else mocker.MagicMock()
    )
    mock._docs_collection = docs_collection
    return mock


@pytest.fixture(autouse=True)
def override_db(mock_db, mocker):
    mock_client = mocker.MagicMock()
    mock_client.close = mocker.AsyncMock()
    mocker.patch(
        "app.main.get_mongo_client", mocker.AsyncMock(return_value=mock_client)
    )
    mocker.patch(
        "app.document.router.ingest_document",
        mocker.AsyncMock(return_value=5),
    )

    async def _get_db():
        return mock_db

    app.dependency_overrides[get_db] = _get_db
    yield
    app.dependency_overrides.pop(get_db, None)


def test_create_documents_201(mock_db):
    client = TestClient(app)
    response = client.post(
        "/documents",
        json=[
            {
                "file_name": "doc1.pdf",
                "knowledge_group_id": "kg-1",
                "cdp_upload_id": "upload-1",
                "s3_key": "uploads/kg-1/doc1.pdf",
            },
            {
                "file_name": "doc2.pdf",
                "knowledge_group_id": "kg-1",
                "cdp_upload_id": "upload-1",
                "s3_key": "uploads/kg-1/doc2.pdf",
            },
        ],
    )
    assert response.status_code == 201
    mock_db._docs_collection.insert_many.assert_called_once()
    call_args = mock_db._docs_collection.insert_many.call_args[0][0]
    assert len(call_args) == 2
    assert call_args[0]["file_name"] == "doc1.pdf"
    assert call_args[0]["s3_key"] == "uploads/kg-1/doc1.pdf"
    assert call_args[0]["status"] == "not_started"
    assert call_args[1]["file_name"] == "doc2.pdf"
    assert call_args[1]["s3_key"] == "uploads/kg-1/doc2.pdf"
    assert call_args[1]["status"] == "not_started"


def test_create_documents_empty_list(mock_db):
    client = TestClient(app)
    response = client.post("/documents", json=[])
    assert response.status_code == 201
    mock_db._docs_collection.insert_many.assert_not_called()


@pytest.mark.usefixtures("mock_db")
def test_create_documents_triggers_async_ingest(mocker):
    """create_documents schedules ingest via create_task for each document."""
    create_task_calls = []

    def capture_create_task(coro):
        create_task_calls.append(coro)
        return mocker.MagicMock()

    mocker.patch(
        "app.document.router.asyncio.create_task",
        side_effect=capture_create_task,
    )
    client = TestClient(app)
    response = client.post(
        "/documents",
        json=[
            {
                "file_name": "doc1.pdf",
                "knowledge_group_id": "kg-1",
                "cdp_upload_id": "upload-1",
                "s3_key": "uploads/kg-1/doc1.pdf",
            },
        ],
    )
    assert response.status_code == 201
    assert len(create_task_calls) == 1
    # Run the captured coroutine to verify it calls ingest_document
    import app.document.router as router_module

    ingest_mock = mocker.patch.object(
        router_module, "ingest_document", mocker.AsyncMock(return_value=5)
    )
    asyncio.run(create_task_calls[0])
    ingest_mock.assert_called_once()
    call_kwargs = ingest_mock.call_args[1]
    assert call_kwargs["s3_key"] == "uploads/kg-1/doc1.pdf"
    assert call_kwargs["knowledge_group_id"] == "kg-1"


def test_create_documents_rejects_missing_s3_key(mock_db):
    client = TestClient(app)
    response = client.post(
        "/documents",
        json=[
            {
                "file_name": "doc1.pdf",
                "knowledge_group_id": "kg-1",
                "cdp_upload_id": "upload-1",
            },
        ],
    )
    assert response.status_code == 422
    mock_db._docs_collection.insert_many.assert_not_called()


def test_run_ingest_for_document_logs_on_exception(mocker):
    """When ingest_document raises, _run_ingest_for_document logs and does not propagate."""
    mock_db = mocker.MagicMock()
    mock_db.__getitem__ = (
        lambda _s, key: mock_db._coll if key == "documents" else mocker.MagicMock()
    )
    mock_db._coll = mocker.MagicMock()
    mock_db._coll.update_one = mocker.AsyncMock()

    mocker.patch(
        "app.document.router.ingest_document",
        mocker.AsyncMock(side_effect=RuntimeError("ingest failed")),
    )
    mocker.patch(
        "app.document.router.config",
        mocker.MagicMock(upload_bucket_name="test-bucket"),
    )
    from app.document.router import _run_ingest_for_document

    oid = ObjectId()
    asyncio.run(
        _run_ingest_for_document(
            db=mock_db,
            document_id=str(oid),
            s3_key="key",
            knowledge_group_id="kg-1",
            cdp_upload_id="upload-1",
        )
    )
    # Should not raise - exception is caught and logged


def test_get_upload_status_empty(mock_db):
    client = TestClient(app)
    response = client.get("/upload-status/upload-123")
    assert response.status_code == 200
    assert response.json() == []
    mock_db._docs_collection.find.assert_called_once_with(
        {"cdp_upload_id": "upload-123"}
    )


def test_list_documents_by_knowledge_group_empty(mock_db, mocker):
    """GET /documents?knowledge_group_id= returns 404 when group not found."""
    mock_db.__getitem__ = lambda _s, key: (
        mock_db._docs_collection
        if key == "documents"
        else mock_db._kg_collection
        if key == "knowledgeGroups"
        else mocker.MagicMock()
    )
    mock_db._kg_collection = mocker.MagicMock()
    mock_db._kg_collection.find_one = mocker.AsyncMock(return_value=None)

    client = TestClient(app)
    response = client.get(
        "/documents?knowledge_group_id=507f1f77bcf86cd799439011",
        headers={"user-id": "user-1"},
    )
    assert response.status_code == 404


def test_list_documents_by_knowledge_group_forbidden(mock_db, mocker):
    """GET /documents returns 404 when group belongs to different user."""
    mock_db.__getitem__ = lambda _s, key: (
        mock_db._docs_collection
        if key == "documents"
        else mock_db._kg_collection
        if key == "knowledgeGroups"
        else mocker.MagicMock()
    )
    mock_db._kg_collection = mocker.MagicMock()
    mock_db._kg_collection.find_one = mocker.AsyncMock(
        return_value={"created_by": "other-user"}
    )

    client = TestClient(app)
    response = client.get(
        "/documents?knowledge_group_id=507f1f77bcf86cd799439011",
        headers={"user-id": "user-1"},
    )
    assert response.status_code == 404


def test_list_documents_by_knowledge_group_success(mock_db, mocker):
    """GET /documents returns documents when group exists and user owns it."""

    async def cursor_with_doc():
        yield {
            "_id": "507f1f77bcf86cd799439012",
            "file_name": "guide.pdf",
            "status": "ready",
            "knowledge_group_id": "507f1f77bcf86cd799439011",
            "cdp_upload_id": "upload-1",
            "s3_key": "uploads/kg/doc.pdf",
            "created_at": None,
        }

    mock_db.__getitem__ = lambda _s, key: (
        mock_db._docs_collection
        if key == "documents"
        else mock_db._kg_collection
        if key == "knowledgeGroups"
        else mocker.MagicMock()
    )
    mock_db._kg_collection = mocker.MagicMock()
    mock_db._kg_collection.find_one = mocker.AsyncMock(
        return_value={"created_by": "user-1"}
    )
    mock_db._docs_collection.find = mocker.MagicMock(return_value=cursor_with_doc())

    client = TestClient(app)
    response = client.get(
        "/documents?knowledge_group_id=507f1f77bcf86cd799439011",
        headers={"user-id": "user-1"},
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["file_name"] == "guide.pdf"
    assert data[0]["status"] == "ready"


def test_get_upload_status_with_docs(mock_db, mocker):
    async def cursor_with_doc():
        yield {
            "_id": "507f1f77bcf86cd799439011",
            "file_name": "test.pdf",
            "status": "ready",
            "knowledge_group_id": "kg-123",
            "cdp_upload_id": "upload-456",
            "s3_key": "uploads/abc/file.pdf",
            "created_at": None,
        }

    mock_db._docs_collection.find = mocker.MagicMock(return_value=cursor_with_doc())

    client = TestClient(app)
    response = client.get("/upload-status/upload-456")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["file_name"] == "test.pdf"
    assert data[0]["status"] == "ready"
    assert data[0]["s3_key"] == "uploads/abc/file.pdf"


def test_run_ingest_updates_document_status(mock_db, mocker):
    """When ingest completes, document status is updated to ready with chunk_count."""
    oid = ObjectId()
    mock_db.__getitem__ = lambda _s, key: (
        mock_db._docs_collection if key == "documents" else mocker.MagicMock()
    )
    mock_db._docs_collection.insert_many = mocker.AsyncMock(
        return_value=mocker.MagicMock(inserted_ids=[oid])
    )
    mock_db._docs_collection.update_one = mocker.AsyncMock()

    mocker.patch(
        "app.document.router.ingest_document",
        mocker.AsyncMock(return_value=5),
    )
    create_task_calls = []

    def capture_create_task(coro):
        create_task_calls.append(coro)
        return mocker.MagicMock()

    mocker.patch(
        "app.document.router.asyncio.create_task",
        side_effect=capture_create_task,
    )

    client = TestClient(app)
    response = client.post(
        "/documents",
        json=[
            {
                "file_name": "doc1.pdf",
                "knowledge_group_id": "kg-1",
                "cdp_upload_id": "upload-1",
                "s3_key": "uploads/kg-1/doc1.pdf",
            },
        ],
    )
    assert response.status_code == 201
    assert len(create_task_calls) == 1
    asyncio.run(create_task_calls[0])

    assert mock_db._docs_collection.update_one.call_count == 2
    in_progress_call = mock_db._docs_collection.update_one.call_args_list[0]
    assert in_progress_call[0][1]["$set"]["status"] == "in_progress"
    ready_call = mock_db._docs_collection.update_one.call_args_list[1]
    assert ready_call[0][1]["$set"]["status"] == "ready"
    assert ready_call[0][1]["$set"]["chunk_count"] == 5


def test_run_ingest_sets_failed_on_exception(mock_db, mocker):
    """When ingest raises, document status is updated to failed."""
    oid = ObjectId()
    mock_db.__getitem__ = lambda _s, key: (
        mock_db._docs_collection if key == "documents" else mocker.MagicMock()
    )
    mock_db._docs_collection.insert_many = mocker.AsyncMock(
        return_value=mocker.MagicMock(inserted_ids=[oid])
    )
    mock_db._docs_collection.update_one = mocker.AsyncMock()

    mocker.patch(
        "app.document.router.ingest_document",
        mocker.AsyncMock(side_effect=RuntimeError("ingest failed")),
    )
    create_task_calls = []

    def capture_create_task(coro):
        create_task_calls.append(coro)
        return mocker.MagicMock()

    mocker.patch(
        "app.document.router.asyncio.create_task",
        side_effect=capture_create_task,
    )

    client = TestClient(app)
    response = client.post(
        "/documents",
        json=[
            {
                "file_name": "doc1.pdf",
                "knowledge_group_id": "kg-1",
                "cdp_upload_id": "upload-1",
                "s3_key": "uploads/kg-1/doc1.pdf",
            },
        ],
    )
    assert response.status_code == 201
    asyncio.run(create_task_calls[0])

    assert mock_db._docs_collection.update_one.call_count == 2
    failed_call = mock_db._docs_collection.update_one.call_args_list[1]
    assert failed_call[0][1]["$set"]["status"] == "failed"
