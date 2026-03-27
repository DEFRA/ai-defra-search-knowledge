import pytest

from app.ingest.s3_client import (
    fetch_jsonl_from_s3,
    fetch_object_from_s3,
    get_s3_client,
    list_jsonl_keys,
)

# LocalStack default dummy credentials (same as compose defaults; not real secrets).
_LOCALSTACK_TEST_ACCESS_KEY = "ak"  # noqa: S105
_LOCALSTACK_TEST_SECRET_KEY = "sk"  # noqa: S105


def test_fetch_object_from_s3(mocker):
    mock_body = mocker.MagicMock()
    mock_body.read.return_value = b"raw bytes"
    mock_client = mocker.MagicMock()
    mock_client.get_object.return_value = {"Body": mock_body}
    mocker.patch("app.ingest.s3_client.get_s3_client", return_value=mock_client)

    result = fetch_object_from_s3("bucket", "path/to/object")
    assert result == b"raw bytes"
    mock_client.get_object.assert_called_once_with(
        Bucket="bucket", Key="path/to/object"
    )


def test_fetch_object_from_s3_nosuchkey_raises(mocker):
    from botocore.exceptions import ClientError

    mock_client = mocker.MagicMock()
    mock_client.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject"
    )
    mocker.patch("app.ingest.s3_client.get_s3_client", return_value=mock_client)

    with pytest.raises(FileNotFoundError, match="No object at"):
        fetch_object_from_s3("bucket", "missing")


def test_get_s3_client_creates_client(mocker):
    from botocore.config import Config

    mock_client = mocker.MagicMock()
    mock_boto3_client = mocker.patch(
        "app.ingest.s3_client.boto3.client", return_value=mock_client
    )
    mock_timeouts = mocker.MagicMock(aws_connect_timeout=5, aws_read_timeout=30)
    mocker.patch(
        "app.ingest.s3_client.config",
        aws_region="eu-west-2",
        localstack_s3_endpoint_url=None,
        timeouts=mock_timeouts,
    )

    # Reset module-level cache
    import app.ingest.s3_client as s3_module

    s3_module._s3_client = None

    result = get_s3_client()
    assert result is mock_client

    call_kwargs = mock_boto3_client.call_args[1]
    assert call_kwargs["region_name"] == "eu-west-2"
    assert isinstance(call_kwargs["config"], Config)
    assert call_kwargs["config"].connect_timeout == 5
    assert call_kwargs["config"].read_timeout == 30
    assert "endpoint_url" not in call_kwargs
    assert "aws_access_key_id" not in call_kwargs


def test_get_s3_client_with_endpoint_passes_localstack_keys(mocker):
    mock_client = mocker.MagicMock()
    mock_boto3_client = mocker.patch(
        "app.ingest.s3_client.boto3.client", return_value=mock_client
    )
    mock_timeouts = mocker.MagicMock(aws_connect_timeout=5, aws_read_timeout=30)
    mocker.patch(
        "app.ingest.s3_client.config",
        aws_region="eu-west-2",
        localstack_s3_endpoint_url="http://localstack:4566",
        localstack_access_key=_LOCALSTACK_TEST_ACCESS_KEY,
        localstack_secret_access_key=_LOCALSTACK_TEST_SECRET_KEY,
        timeouts=mock_timeouts,
    )

    import app.ingest.s3_client as s3_module

    s3_module._s3_client = None

    result = get_s3_client()
    assert result is mock_client

    call_kwargs = mock_boto3_client.call_args[1]
    assert call_kwargs["region_name"] == "eu-west-2"
    assert call_kwargs["endpoint_url"] == "http://localstack:4566"
    assert call_kwargs["aws_access_key_id"] == _LOCALSTACK_TEST_ACCESS_KEY
    assert call_kwargs["aws_secret_access_key"] == _LOCALSTACK_TEST_SECRET_KEY


def test_fetch_jsonl_from_s3_exact_key(mocker):
    mock_body = mocker.MagicMock()
    mock_body.read.return_value = b'{"text": "hello"}'
    mock_client = mocker.MagicMock()
    mock_client.get_object.return_value = {"Body": mock_body}
    mocker.patch("app.ingest.s3_client.get_s3_client", return_value=mock_client)

    result = fetch_jsonl_from_s3("bucket", "path/file.jsonl")
    assert result == b'{"text": "hello"}'
    mock_client.get_object.assert_called_once_with(
        Bucket="bucket", Key="path/file.jsonl"
    )


def test_fetch_jsonl_from_s3_prefix_finds_jsonl(mocker):
    mock_body = mocker.MagicMock()
    mock_body.read.return_value = b'{"text": "ok"}'
    mock_client = mocker.MagicMock()
    mock_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "path/other.txt"},
            {"Key": "path/chunks.jsonl"},
        ]
    }
    mock_client.get_object.return_value = {"Body": mock_body}
    mocker.patch("app.ingest.s3_client.get_s3_client", return_value=mock_client)

    result = fetch_jsonl_from_s3("bucket", "path/")
    assert result == b'{"text": "ok"}'
    mock_client.get_object.assert_called_once_with(
        Bucket="bucket", Key="path/chunks.jsonl"
    )


def test_fetch_jsonl_from_s3_prefix_no_jsonl_raises(mocker):
    mock_client = mocker.MagicMock()
    mock_client.list_objects_v2.return_value = {"Contents": [{"Key": "path/other.txt"}]}
    mocker.patch("app.ingest.s3_client.get_s3_client", return_value=mock_client)

    with pytest.raises(FileNotFoundError, match="No .jsonl files"):
        fetch_jsonl_from_s3("bucket", "path/")


def test_fetch_jsonl_from_s3_prefix_empty_contents_raises(mocker):
    mock_client = mocker.MagicMock()
    mock_client.list_objects_v2.return_value = {"Contents": []}
    mocker.patch("app.ingest.s3_client.get_s3_client", return_value=mock_client)

    with pytest.raises(FileNotFoundError, match="No .jsonl files"):
        fetch_jsonl_from_s3("bucket", "path/")


def test_fetch_jsonl_from_s3_nosuchkey_raises(mocker):
    from botocore.exceptions import ClientError

    mock_client = mocker.MagicMock()
    mock_client.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject"
    )
    mocker.patch("app.ingest.s3_client.get_s3_client", return_value=mock_client)

    with pytest.raises(FileNotFoundError, match="No object at"):
        fetch_jsonl_from_s3("bucket", "missing.jsonl")


def test_fetch_jsonl_from_s3_other_client_error_propagates(mocker):
    from botocore.exceptions import ClientError

    mock_client = mocker.MagicMock()
    mock_client.get_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Forbidden"}}, "GetObject"
    )
    mocker.patch("app.ingest.s3_client.get_s3_client", return_value=mock_client)

    with pytest.raises(ClientError):
        fetch_jsonl_from_s3("bucket", "file.jsonl")


def test_list_jsonl_keys(mocker):
    mock_client = mocker.MagicMock()
    mock_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "path/a.jsonl"},
            {"Key": "path/b.txt"},
            {"Key": "path/c.jsonl"},
        ]
    }
    mocker.patch("app.ingest.s3_client.get_s3_client", return_value=mock_client)

    result = list_jsonl_keys("bucket", "path/")
    assert result == ["path/a.jsonl", "path/c.jsonl"]
