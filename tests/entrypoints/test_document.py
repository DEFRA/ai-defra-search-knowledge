import pytest
from fastapi.testclient import TestClient

from app.common.mongo import get_db
from app.main import app


@pytest.fixture
def mock_db(mocker):
    docs_collection = mocker.MagicMock()
    docs_collection.insert_many = mocker.AsyncMock(
        return_value=mocker.MagicMock(inserted_ids=["id1", "id2"])
    )
    docs_collection.update_many = mocker.AsyncMock(
        return_value=mocker.MagicMock(modified_count=2)
    )
    docs_collection.update_one = mocker.AsyncMock(
        return_value=mocker.MagicMock(modified_count=1, matched_count=1)
    )

    def _find_one_side_effect(*args, **kwargs):
        _filter = args[0] if args else kwargs.get("filter", {})
        fname = _filter.get("file_name", "test-file.pdf")
        return {
            "_id": "507f1f77bcf86cd799439011",
            "file_name": fname,
            "knowledge_group_id": "kg-1",
            "cdp_upload_id": _filter.get("cdp_upload_id", "upload-789"),
        }

    docs_collection.find_one = mocker.AsyncMock(side_effect=_find_one_side_effect)

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
                "status": "not_started",
                "knowledge_group_id": "kg-1",
                "cdp_upload_id": "upload-1",
            },
            {
                "file_name": "doc2.pdf",
                "knowledge_group_id": "kg-1",
                "cdp_upload_id": "upload-1",
            },
        ],
    )
    assert response.status_code == 201
    mock_db._docs_collection.insert_many.assert_called_once()
    call_args = mock_db._docs_collection.insert_many.call_args[0][0]
    assert len(call_args) == 2
    assert call_args[0]["file_name"] == "doc1.pdf"
    assert call_args[1]["file_name"] == "doc2.pdf"
    assert call_args[1]["status"] == "not_started"


def test_create_documents_empty_list(mock_db):
    client = TestClient(app)
    response = client.post("/documents", json=[])
    assert response.status_code == 201
    mock_db._docs_collection.insert_many.assert_not_called()


def test_get_upload_status_empty(mock_db):
    client = TestClient(app)
    response = client.get("/upload-status/upload-123")
    assert response.status_code == 200
    assert response.json() == []
    mock_db._docs_collection.find.assert_called_once_with(
        {"cdp_upload_id": "upload-123"}
    )


def test_get_upload_status_with_docs(mock_db, mocker):
    async def cursor_with_doc():
        yield {
            "_id": "507f1f77bcf86cd799439011",
            "file_name": "test.pdf",
            "status": "ready",
            "knowledge_group_id": "kg-123",
            "cdp_upload_id": "upload-456",
            "s3_path": "uploads/abc/file.pdf",
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
    assert data[0]["s3_path"] == "uploads/abc/file.pdf"


def test_upload_callback_200(mock_db):
    client = TestClient(app)
    response = client.post(
        "/upload-callback/upload-789",
        json={
            "uploadStatus": "ready",
            "metadata": {"reference": "ref-1", "customerId": "cust-1"},
            "form": {
                "file": {
                    "fileId": "c17543b8-e440-4156-8df4-af62f40a7ac8",
                    "filename": "test-file.pdf",
                    "contentType": "application/pdf",
                    "fileStatus": "complete",
                    "contentLength": 102400,
                }
            },
            "numberOfRejectedFiles": 0,
        },
    )
    assert response.status_code == 200
    mock_db._docs_collection.update_one.assert_called_once()
    call_args = mock_db._docs_collection.update_one.call_args
    assert call_args[0][0] == {
        "cdp_upload_id": "upload-789",
        "file_name": "test-file.pdf",
    }
    assert call_args[0][1]["$set"]["status"] == "ready"
    assert call_args[0][1]["$set"]["s3_path"] == "c17543b8-e440-4156-8df4-af62f40a7ac8"


def test_upload_callback_multiple_files(mock_db):
    client = TestClient(app)
    response = client.post(
        "/upload-callback/upload-789",
        json={
            "uploadStatus": "ready",
            "form": {
                "files": [
                    {
                        "fileId": "file-id-1",
                        "filename": "doc1.pdf",
                        "fileStatus": "complete",
                    },
                    {
                        "fileId": "file-id-2",
                        "filename": "doc2.pdf",
                        "fileStatus": "complete",
                    },
                ]
            },
            "numberOfRejectedFiles": 0,
        },
    )
    assert response.status_code == 200
    assert mock_db._docs_collection.update_one.call_count == 2
    calls = mock_db._docs_collection.update_one.call_args_list
    assert calls[0][0][0] == {"cdp_upload_id": "upload-789", "file_name": "doc1.pdf"}
    assert calls[0][0][1]["$set"]["s3_path"] == "file-id-1"
    assert calls[1][0][0] == {"cdp_upload_id": "upload-789", "file_name": "doc2.pdf"}
    assert calls[1][0][1]["$set"]["s3_path"] == "file-id-2"


def test_upload_callback_ignores_incomplete(mock_db):
    client = TestClient(app)
    response = client.post(
        "/upload-callback/upload-789",
        json={
            "uploadStatus": "ready",
            "form": {
                "file": {
                    "fileId": "file-id-1",
                    "filename": "doc.pdf",
                    "fileStatus": "scanning",
                }
            },
            "numberOfRejectedFiles": 0,
        },
    )
    assert response.status_code == 200
    mock_db._docs_collection.update_one.assert_not_called()


def test_upload_callback_ignores_non_ready(mock_db):
    client = TestClient(app)
    response = client.post(
        "/upload-callback/upload-789",
        json={"uploadStatus": "scanning"},
    )
    assert response.status_code == 200
    mock_db._docs_collection.update_one.assert_not_called()


def test_upload_callback_no_matching_doc(mock_db, mocker):
    """When find_one returns None, callback skips and does not update."""
    mock_db._docs_collection.find_one = mocker.AsyncMock(return_value=None)
    client = TestClient(app)
    response = client.post(
        "/upload-callback/upload-789",
        json={
            "uploadStatus": "ready",
            "form": {
                "file": {
                    "fileId": "file-id-1",
                    "filename": "unknown.pdf",
                    "fileStatus": "complete",
                }
            },
            "numberOfRejectedFiles": 0,
        },
    )
    assert response.status_code == 200
    mock_db._docs_collection.update_one.assert_not_called()


@pytest.mark.usefixtures("mock_db")
def test_upload_callback_rejected_files():
    """Callback logs when number_of_rejected_files > 0."""
    client = TestClient(app)
    response = client.post(
        "/upload-callback/upload-789",
        json={
            "uploadStatus": "ready",
            "form": {
                "file": {
                    "fileId": "file-id-1",
                    "filename": "test-file.pdf",
                    "fileStatus": "complete",
                }
            },
            "numberOfRejectedFiles": 2,
        },
    )
    assert response.status_code == 200


@pytest.mark.usefixtures("mock_db")
def test_upload_callback_ingest_exception(mocker):
    """When ingest_document raises, callback still returns 200."""
    mocker.patch(
        "app.document.router.ingest_document",
        mocker.AsyncMock(side_effect=RuntimeError("ingest failed")),
    )
    client = TestClient(app)
    response = client.post(
        "/upload-callback/upload-789",
        json={
            "uploadStatus": "ready",
            "form": {
                "file": {
                    "fileId": "file-id-1",
                    "filename": "test-file.pdf",
                    "fileStatus": "complete",
                }
            },
            "numberOfRejectedFiles": 0,
        },
    )
    assert response.status_code == 200
