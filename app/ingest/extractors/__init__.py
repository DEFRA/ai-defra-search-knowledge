from app.ingest.extractors.docx import DocxChunkExtractor
from app.ingest.extractors.jsonl import JsonlChunkExtractor
from app.ingest.extractors.pdf import PdfChunkExtractor


def get_extractor_for_file_name(
    file_name: str,
) -> DocxChunkExtractor | JsonlChunkExtractor | PdfChunkExtractor:
    """Route by file_name extension; s3_key has no extension."""
    ext = file_name.lower().rsplit(".", 1)[-1] if "." in file_name else ""
    if ext == "pdf":
        return PdfChunkExtractor()
    if ext == "docx":
        return DocxChunkExtractor()
    return JsonlChunkExtractor()
