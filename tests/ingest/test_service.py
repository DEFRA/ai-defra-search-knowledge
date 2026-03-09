import pytest

from app.ingest.extractors import JsonlChunkExtractor
from app.ingest.service import ingest_document


def test_jsonl_extractor_parses_chunks():
    extractor = JsonlChunkExtractor()
    data = b'{"source": "url1", "text": "chunk1"}\n{"source": "url2", "text": "chunk2"}'
    chunks = extractor.extract(data, "file.jsonl")
    assert len(chunks) == 2
    assert chunks[0] == {"source": "url1", "text": "chunk1"}
    assert chunks[1] == {"source": "url2", "text": "chunk2"}


def test_jsonl_extractor_empty():
    extractor = JsonlChunkExtractor()
    assert extractor.extract(b"", "") == []
    assert extractor.extract(b"\n\n", "") == []


def test_jsonl_extractor_uses_content_fallback():
    extractor = JsonlChunkExtractor()
    data = b'{"source": "url", "content": "fallback"}'
    chunks = extractor.extract(data, "")
    assert chunks[0]["content"] == "fallback"
    assert chunks[0].get("text") is None


@pytest.mark.asyncio
async def test_ingest_document_jsonl_fetches_embeds_inserts(mocker):
    jsonl_data = b'{"source": "s1", "text": "hello"}\n{"source": "s2", "text": "world"}'
    mocker.patch(
        "app.ingest.service.fetch_jsonl_from_s3",
        return_value=jsonl_data,
    )
    mocker.patch(
        "app.common.bedrock.get_bedrock_client", return_value=mocker.MagicMock()
    )
    mocker.patch(
        "app.common.bedrock.BedrockEmbeddingService.generate_embeddings",
        return_value=[0.1] * 1024,
    )
    insert_mock = mocker.AsyncMock()
    mocker.patch("app.ingest.service.insert_vectors", insert_mock)

    count = await ingest_document(
        bucket="bucket",
        s3_key="key",
        file_name="chunks.jsonl",
        document_id="doc-1",
        knowledge_group_id="kg-1",
        snapshot_id="snap-1",
    )

    assert count == 2
    insert_mock.assert_called_once()
    vectors = insert_mock.call_args[0][0]
    assert len(vectors) == 2
    assert vectors[0][0] == "hello"
    assert vectors[0][2] == "snap-1"
    assert vectors[0][3] == "doc-1"
    assert vectors[0][4] == {"source": "s1", "knowledge_group_id": "kg-1"}


@pytest.mark.asyncio
async def test_ingest_document_tries_jsonl_suffix_on_not_found(mocker):
    fetch_mock = mocker.patch(
        "app.ingest.service.fetch_jsonl_from_s3",
        side_effect=[FileNotFoundError, b'{"text": "ok"}'],
    )
    mocker.patch(
        "app.common.bedrock.get_bedrock_client", return_value=mocker.MagicMock()
    )
    mocker.patch(
        "app.common.bedrock.BedrockEmbeddingService.generate_embeddings",
        return_value=[0.1] * 1024,
    )
    insert_mock = mocker.AsyncMock()
    mocker.patch("app.ingest.service.insert_vectors", insert_mock)

    count = await ingest_document(
        bucket="b",
        s3_key="key",
        file_name="data.jsonl",
        document_id="d",
        knowledge_group_id="kg",
        snapshot_id="s",
    )

    assert count == 1
    assert fetch_mock.call_count == 2
    assert fetch_mock.call_args_list[1][0] == ("b", "key.jsonl")


@pytest.mark.asyncio
async def test_ingest_document_skips_empty_text(mocker):
    jsonl_data = b'{"source": "s1", "text": ""}\n{"source": "s2", "text": "ok"}'
    mocker.patch(
        "app.ingest.service.fetch_jsonl_from_s3",
        return_value=jsonl_data,
    )
    mocker.patch(
        "app.common.bedrock.get_bedrock_client", return_value=mocker.MagicMock()
    )
    mocker.patch(
        "app.common.bedrock.BedrockEmbeddingService.generate_embeddings",
        return_value=[0.1] * 1024,
    )
    insert_mock = mocker.AsyncMock()
    mocker.patch("app.ingest.service.insert_vectors", insert_mock)

    count = await ingest_document(
        bucket="b",
        s3_key="k",
        file_name="f.jsonl",
        document_id="d",
        knowledge_group_id="kg",
        snapshot_id="s",
    )

    assert count == 1
    vectors = insert_mock.call_args[0][0]
    assert len(vectors) == 1
    assert vectors[0][0] == "ok"


@pytest.mark.asyncio
async def test_ingest_document_jsonl_filenotfound_both_paths(mocker):
    mocker.patch(
        "app.ingest.service.fetch_jsonl_from_s3",
        side_effect=FileNotFoundError("not found"),
    )

    count = await ingest_document(
        bucket="b",
        s3_key="missing",
        file_name="data.jsonl",
        document_id="d",
        knowledge_group_id="kg",
        snapshot_id="s",
    )

    assert count == 0


@pytest.mark.asyncio
async def test_ingest_document_empty_jsonl(mocker):
    mocker.patch(
        "app.ingest.service.fetch_jsonl_from_s3",
        return_value=b"",
    )

    count = await ingest_document(
        bucket="b",
        s3_key="empty",
        file_name="empty.jsonl",
        document_id="d",
        knowledge_group_id="kg",
        snapshot_id="s",
    )

    assert count == 0


@pytest.mark.asyncio
async def test_ingest_document_pdf_fetches_embeds_inserts(mocker):
    pdf_bytes = b"%PDF-1.4 fake pdf content"
    mocker.patch(
        "app.ingest.service.fetch_object_from_s3",
        return_value=pdf_bytes,
    )
    mock_doc = mocker.MagicMock()
    mock_page = mocker.MagicMock()
    mock_page.get_text.return_value = "Page one content. More text here."
    mock_doc.__iter__ = lambda _: iter([mock_page])
    mocker.patch("app.ingest.extractors.pdf.pymupdf.open", return_value=mock_doc)
    mocker.patch(
        "app.common.bedrock.get_bedrock_client", return_value=mocker.MagicMock()
    )
    mocker.patch(
        "app.common.bedrock.BedrockEmbeddingService.generate_embeddings",
        return_value=[0.1] * 1024,
    )
    insert_mock = mocker.AsyncMock()
    mocker.patch("app.ingest.service.insert_vectors", insert_mock)

    count = await ingest_document(
        bucket="bucket",
        s3_key="uploads/kg/doc",
        file_name="doc.pdf",
        document_id="doc-1",
        knowledge_group_id="kg-1",
        snapshot_id="snap-1",
    )

    assert count >= 1
    insert_mock.assert_called_once()
    vectors = insert_mock.call_args[0][0]
    assert len(vectors) >= 1
    assert "Page one content" in vectors[0][0]
    assert vectors[0][4]["source"] == "doc.pdf"


@pytest.mark.asyncio
async def test_ingest_document_pdf_filenotfound(mocker):
    mocker.patch(
        "app.ingest.service.fetch_object_from_s3",
        side_effect=FileNotFoundError("not found"),
    )

    count = await ingest_document(
        bucket="b",
        s3_key="missing",
        file_name="doc.pdf",
        document_id="d",
        knowledge_group_id="kg",
        snapshot_id="s",
    )

    assert count == 0
