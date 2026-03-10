from app.ingest.extractors.docx import DocxChunkExtractor
from app.ingest.extractors.jsonl import JsonlChunkExtractor
from app.ingest.extractors.pdf import PdfChunkExtractor
from app.ingest.extractors.pptx import PptxChunkExtractor


def get_extractor_for_file_name(
    file_name: str,
) -> DocxChunkExtractor | JsonlChunkExtractor | PdfChunkExtractor | PptxChunkExtractor:
    """Route by file_name extension; s3_key has no extension."""
    ext = file_name.lower().rsplit(".", 1)[-1] if "." in file_name else ""
    if ext == "pdf":
        return PdfChunkExtractor()
    if ext == "docx":
        return DocxChunkExtractor()
    if ext == "pptx":
        return PptxChunkExtractor()
    return JsonlChunkExtractor()
