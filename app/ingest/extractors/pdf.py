import pymupdf

DEFAULT_CHUNK_SIZE = 800
DEFAULT_CHUNK_OVERLAP = 100


def _hard_split(block: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    """Split block by character count when no separator works."""
    result = []
    start = 0
    while start < len(block):
        end = min(start + chunk_size, len(block))
        result.append(block[start:end].strip())
        start = end - chunk_overlap if end < len(block) else len(block)
    return [c for c in result if c]


def _split_by_sep(
    block: str,
    sep_index: int,
    separators: list[str],
    chunk_size: int,
    chunk_overlap: int,
) -> list[str]:
    """Split block using separator at sep_index, recursing to next separator if needed."""
    sep = separators[sep_index]
    parts = block.split(sep)
    result: list[str] = []
    current = ""

    for part in parts:
        candidate = current + (sep if current else "") + part
        if len(candidate) <= chunk_size:
            current = candidate
        else:
            if current:
                result.extend(
                    _split_recursive(
                        current, sep_index + 1, separators, chunk_size, chunk_overlap
                    )
                )
            current = part
    if current:
        result.extend(
            _split_recursive(
                current, sep_index + 1, separators, chunk_size, chunk_overlap
            )
        )
    return result


def _split_recursive(
    block: str,
    sep_index: int,
    separators: list[str],
    chunk_size: int,
    chunk_overlap: int,
) -> list[str]:
    if len(block) <= chunk_size:
        return [block] if block.strip() else []
    if sep_index >= len(separators):
        return _hard_split(block, chunk_size, chunk_overlap)
    return _split_by_sep(block, sep_index, separators, chunk_size, chunk_overlap)


def _chunk_text(text: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    """Split text into chunks with overlap. Prefers paragraph and line boundaries."""
    text = text.strip()
    if not text:
        return []
    separators = ["\n\n", "\n", ". ", " "]
    return [
        c
        for c in _split_recursive(text, 0, separators, chunk_size, chunk_overlap)
        if c.strip()
    ]


class PdfChunkExtractor:
    """Extract text from PDF bytes and chunk for embedding."""

    def __init__(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
    ) -> None:
        self._chunk_size = chunk_size
        self._chunk_overlap = chunk_overlap

    def extract(self, data: bytes, source: str) -> list[dict]:
        doc = pymupdf.open(stream=data, filetype="pdf")
        try:
            full_text = ""
            for page in doc:
                full_text += page.get_text()
            doc.close()
        except Exception:
            doc.close()
            raise

        chunks = _chunk_text(
            full_text,
            chunk_size=self._chunk_size,
            chunk_overlap=self._chunk_overlap,
        )
        return [{"text": chunk, "source": source} for chunk in chunks]
