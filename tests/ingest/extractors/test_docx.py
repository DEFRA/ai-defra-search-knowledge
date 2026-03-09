from app.ingest.extractors import DocxChunkExtractor


def test_docx_extractor_extracts_and_chunks(mocker):
    mock_doc = mocker.MagicMock()
    mock_para1 = mocker.MagicMock()
    mock_para1.text = "First paragraph."
    mock_para2 = mocker.MagicMock()
    mock_para2.text = "Second paragraph with more text."
    mock_doc.paragraphs = [mock_para1, mock_para2]
    mocker.patch(
        "app.ingest.extractors.docx.Document",
        return_value=mock_doc,
    )

    extractor = DocxChunkExtractor(chunk_size=100, chunk_overlap=10)
    chunks = extractor.extract(b"PK\x03\x04", "report.docx")

    assert len(chunks) >= 1
    assert all("text" in c and "source" in c for c in chunks)
    assert all(c["source"] == "report.docx" for c in chunks)
    full_text = " ".join(c["text"] for c in chunks)
    assert "First paragraph" in full_text
    assert "Second paragraph" in full_text
