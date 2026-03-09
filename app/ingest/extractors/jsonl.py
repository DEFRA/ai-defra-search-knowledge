import json


class JsonlChunkExtractor:
    """Extract chunks from JSONL bytes. Each line is a JSON object with 'text' or 'content' and optional 'source'."""

    def extract(self, data: bytes, _: str) -> list[dict]:
        chunks: list[dict] = []
        for line in data.decode("utf-8").strip().split("\n"):
            if not line:
                continue
            chunks.append(json.loads(line))
        return chunks
