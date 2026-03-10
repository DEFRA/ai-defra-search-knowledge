from app.ingest.extractors import PptxChunkExtractor


def test_pptx_extractor_extracts_and_chunks(mocker):
    mock_shape1 = mocker.MagicMock()
    mock_shape1.has_text_frame = True
    mock_shape1.has_table = False
    mock_shape1.text = "Slide title."
    mock_shape2 = mocker.MagicMock()
    mock_shape2.has_text_frame = True
    mock_shape2.has_table = False
    mock_shape2.text = "Slide body with more text."
    mock_slide = mocker.MagicMock()
    mock_slide.shapes = [mock_shape1, mock_shape2]
    mock_slide.has_notes_slide = False

    mock_prs = mocker.MagicMock()
    mock_prs.slides = [mock_slide]

    mocker.patch(
        "app.ingest.extractors.pptx.Presentation",
        return_value=mock_prs,
    )

    extractor = PptxChunkExtractor(chunk_size=100, chunk_overlap=10)
    chunks = extractor.extract(b"PK\x03\x04", "deck.pptx")

    assert len(chunks) >= 1
    assert all("text" in c and "source" in c for c in chunks)
    assert all(c["source"] == "deck.pptx" for c in chunks)
    full_text = " ".join(c["text"] for c in chunks)
    assert "Slide title" in full_text
    assert "Slide body" in full_text


def test_pptx_extractor_extracts_tables(mocker):
    mock_cell1 = mocker.MagicMock()
    mock_cell1.text = "Cell A"
    mock_cell2 = mocker.MagicMock()
    mock_cell2.text = "Cell B"
    mock_row = mocker.MagicMock()
    mock_row.cells = [mock_cell1, mock_cell2]
    mock_table = mocker.MagicMock()
    mock_table.rows = [mock_row]

    mock_table_shape = mocker.MagicMock()
    mock_table_shape.has_text_frame = False
    mock_table_shape.has_table = True
    mock_table_shape.table = mock_table

    mock_slide = mocker.MagicMock()
    mock_slide.shapes = [mock_table_shape]
    mock_slide.has_notes_slide = False

    mock_prs = mocker.MagicMock()
    mock_prs.slides = [mock_slide]

    mocker.patch(
        "app.ingest.extractors.pptx.Presentation",
        return_value=mock_prs,
    )

    extractor = PptxChunkExtractor(chunk_size=100, chunk_overlap=10)
    chunks = extractor.extract(b"PK\x03\x04", "deck.pptx")

    assert len(chunks) >= 1
    full_text = " ".join(c["text"] for c in chunks)
    assert "Cell A" in full_text
    assert "Cell B" in full_text


def test_pptx_extractor_extracts_notes(mocker):
    mock_shape = mocker.MagicMock()
    mock_shape.has_text_frame = True
    mock_shape.has_table = False
    mock_shape.text = "Slide content."
    mock_slide = mocker.MagicMock()
    mock_slide.shapes = [mock_shape]
    mock_slide.has_notes_slide = True

    mock_notes_frame = mocker.MagicMock()
    mock_notes_frame.text = "Speaker notes here."
    mock_notes_slide = mocker.MagicMock()
    mock_notes_slide.notes_text_frame = mock_notes_frame
    mock_slide.notes_slide = mock_notes_slide

    mock_prs = mocker.MagicMock()
    mock_prs.slides = [mock_slide]

    mocker.patch(
        "app.ingest.extractors.pptx.Presentation",
        return_value=mock_prs,
    )

    extractor = PptxChunkExtractor(chunk_size=100, chunk_overlap=10)
    chunks = extractor.extract(b"PK\x03\x04", "deck.pptx")

    assert len(chunks) >= 1
    full_text = " ".join(c["text"] for c in chunks)
    assert "Slide content" in full_text
    assert "Speaker notes here" in full_text
