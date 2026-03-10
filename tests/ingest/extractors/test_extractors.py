import pytest

from app.ingest.extractors import (
    DocxChunkExtractor,
    JsonlChunkExtractor,
    PdfChunkExtractor,
    PptxChunkExtractor,
    get_extractor_for_file_name,
)
from app.ingest.extractors.chunking import chunk_text


def test_get_extractor_for_file_name_pdf():
    assert isinstance(get_extractor_for_file_name("doc.pdf"), PdfChunkExtractor)
    assert isinstance(get_extractor_for_file_name("DOC.PDF"), PdfChunkExtractor)


def test_get_extractor_for_file_name_jsonl():
    assert isinstance(get_extractor_for_file_name("chunks.jsonl"), JsonlChunkExtractor)
    assert isinstance(get_extractor_for_file_name("data.JSONL"), JsonlChunkExtractor)


def test_get_extractor_for_file_name_docx():
    assert isinstance(get_extractor_for_file_name("doc.docx"), DocxChunkExtractor)
    assert isinstance(get_extractor_for_file_name("report.DOCX"), DocxChunkExtractor)


def test_get_extractor_for_file_name_pptx():
    assert isinstance(get_extractor_for_file_name("deck.pptx"), PptxChunkExtractor)
    assert isinstance(get_extractor_for_file_name("DECK.PPTX"), PptxChunkExtractor)


def test_get_extractor_for_file_name_unknown_defaults_to_jsonl():
    assert isinstance(get_extractor_for_file_name("file.txt"), JsonlChunkExtractor)
    assert isinstance(get_extractor_for_file_name("noext"), JsonlChunkExtractor)


def test_pdf_extractor_extracts_and_chunks(mocker):
    mock_doc = mocker.MagicMock()
    mock_page = mocker.MagicMock()
    mock_page.get_text.return_value = (
        "First paragraph.\n\nSecond paragraph with more text."
    )
    mock_doc.__iter__ = lambda _: iter([mock_page])
    mocker.patch("app.ingest.extractors.pdf.pymupdf.open", return_value=mock_doc)

    extractor = PdfChunkExtractor(chunk_size=100, chunk_overlap=10)
    chunks = extractor.extract(b"%PDF-1.4", "doc.pdf")

    assert len(chunks) >= 1
    assert all("text" in c and "source" in c for c in chunks)
    assert all(c["source"] == "doc.pdf" for c in chunks)
    full_text = " ".join(c["text"] for c in chunks)
    assert "First paragraph" in full_text
    assert "Second paragraph" in full_text


def test_chunk_text_empty():
    assert chunk_text("", 100, 10) == []
    assert chunk_text("   ", 100, 10) == []


def test_chunk_text_short_fits_in_one():
    text = "Short text."
    assert chunk_text(text, 100, 10) == [text]


def test_chunk_text_splits_by_paragraph():
    text = "Para one.\n\nPara two.\n\nPara three."
    chunks = chunk_text(text, 15, 3)
    assert len(chunks) >= 2
    assert "Para one" in chunks[0]
    assert "Para two" in chunks[1] or "Para two" in "".join(chunks)


def test_chunk_text_hard_split_when_no_separator_works():
    long_word = "a" * 50
    text = long_word * 4
    chunks = chunk_text(text, 60, 10)
    assert len(chunks) >= 2
    assert all(len(c) <= 70 for c in chunks)


def test_chunk_text_splits_by_space():
    text = "word " * 30
    chunks = chunk_text(text.strip(), 50, 5)
    assert len(chunks) >= 2


def test_pdf_extractor_closes_doc_on_exception(mocker):
    mock_doc = mocker.MagicMock()
    mock_page = mocker.MagicMock()
    mock_page.get_text.side_effect = RuntimeError("pdf error")
    mock_doc.__iter__ = lambda _: iter([mock_page])
    mocker.patch("app.ingest.extractors.pdf.pymupdf.open", return_value=mock_doc)

    extractor = PdfChunkExtractor()
    with pytest.raises(RuntimeError, match="pdf error"):
        extractor.extract(b"%PDF", "x.pdf")
    mock_doc.close.assert_called()
