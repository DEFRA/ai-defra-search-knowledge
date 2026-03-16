from app.ingest.extractors.docx import DocxChunkExtractor
from app.ingest.extractors.jsonl import JsonlChunkExtractor
from app.ingest.extractors.pdf import PdfChunkExtractor
from app.ingest.extractors.pptx import PptxChunkExtractor

EXTRACTOR_REGISTRY: dict[str, type] = {
    "pdf": PdfChunkExtractor,
    "docx": DocxChunkExtractor,
    "pptx": PptxChunkExtractor,
    "jsonl": JsonlChunkExtractor,
}


def get_supported_extensions() -> list[str]:
    """Return supported file extensions (sorted, for API/display)."""
    return sorted(EXTRACTOR_REGISTRY.keys())


def get_extractor_for_file_name(
    file_name: str,
) -> DocxChunkExtractor | JsonlChunkExtractor | PdfChunkExtractor | PptxChunkExtractor:
    """Route by file_name extension; s3_key has no extension."""
    ext = file_name.lower().rsplit(".", 1)[-1] if "." in file_name else ""
    extractor_cls = EXTRACTOR_REGISTRY.get(ext, JsonlChunkExtractor)
    return extractor_cls()
