import pytest

from app.config import config


@pytest.fixture(autouse=True)
def set_test_env(monkeypatch):
    """Set required environment variables and config values for all tests."""
    monkeypatch.setenv("AI_DEFRA_SEARCH_KNOWLEDGE_API_KEY", "test-key")
    monkeypatch.setattr(config, "api_key", "test-key")
    monkeypatch.delenv("HTTP_PROXY", raising=False)
    monkeypatch.delenv("HTTPS_PROXY", raising=False)
