from app.common.bedrock import BedrockEmbeddingService, get_bedrock_client


def test_get_bedrock_client(mocker):
    from botocore.config import Config

    mock_client = mocker.MagicMock()
    mock_boto3_client = mocker.patch(
        "app.common.bedrock.boto3.client", return_value=mock_client
    )
    mock_timeouts = mocker.MagicMock(aws_connect_timeout=5, aws_read_timeout=30)
    mocker.patch(
        "app.common.bedrock.config",
        aws_region="eu-west-2",
        bedrock_endpoint_url=None,
        timeouts=mock_timeouts,
    )

    import app.common.bedrock as bedrock_module

    bedrock_module._bedrock_client = None

    result = get_bedrock_client()
    assert result is mock_client

    call_kwargs = mock_boto3_client.call_args[1]
    assert call_kwargs["region_name"] == "eu-west-2"
    assert isinstance(call_kwargs["config"], Config)
    assert call_kwargs["config"].connect_timeout == 5
    assert call_kwargs["config"].read_timeout == 30


def test_bedrock_embedding_service_generate_embeddings(mocker):
    mock_body = mocker.MagicMock()
    mock_body.read.return_value = b'{"embedding": [0.1, 0.2, 0.3]}'
    mock_client = mocker.MagicMock()
    mock_client.invoke_model.return_value = {"body": mock_body}
    mocker.patch("app.common.bedrock.get_bedrock_client", return_value=mock_client)
    mocker.patch(
        "app.common.bedrock.config",
        bedrock_embedding=mocker.MagicMock(model_id="amazon.titan-embed-text-v2:0"),
    )

    service = BedrockEmbeddingService()
    result = service.generate_embeddings("hello world")

    assert result == [0.1, 0.2, 0.3]
    mock_client.invoke_model.assert_called_once()
    call_kwargs = mock_client.invoke_model.call_args[1]
    assert call_kwargs["modelId"] == "amazon.titan-embed-text-v2:0"
    assert "hello world" in call_kwargs["body"]
