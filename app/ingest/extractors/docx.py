import io

from docx import Document

from app.ingest.extractors.chunking import (
    DEFAULT_CHUNK_OVERLAP,
    DEFAULT_CHUNK_SIZE,
    chunk_text,
)


class DocxChunkExtractor:
    """Extract text from DOCX bytes and chunk for embedding."""

    def __init__(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
    ) -> None:
        self._chunk_size = chunk_size
        self._chunk_overlap = chunk_overlap

    def extract(self, data: bytes, source: str) -> list[dict]:
        doc = Document(io.BytesIO(data))
        full_text = "\n".join(p.text for p in doc.paragraphs)
        chunks = chunk_text(
            full_text,
            chunk_size=self._chunk_size,
            chunk_overlap=self._chunk_overlap,
        )
        return [{"text": chunk, "source": source} for chunk in chunks]
