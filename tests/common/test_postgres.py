import pytest

import app.common.postgres as postgres_module
from app.common.postgres import (
    check_connection,
    close_engine,
    get_sql_engine,
    get_token,
)
from app.config import config


@pytest.fixture(autouse=True)
def reset_engine():
    postgres_module.engine = None
    yield
    postgres_module.engine = None


class TestGetToken:
    def test_development_uses_password(self, monkeypatch):
        expected = "dev-secret"
        monkeypatch.setattr(config, "python_env", "development")
        monkeypatch.setattr(config.postgres, "password", expected)
        cparams = {}

        get_token(None, None, None, cparams)

        assert cparams["password"] == expected

    def test_non_development_uses_rds_auth_token(self, monkeypatch, mocker):
        monkeypatch.setattr(config, "python_env", "production")
        monkeypatch.setattr(config, "aws_region", "eu-west-2")
        monkeypatch.setattr(config.postgres, "host", "db.example.com")
        monkeypatch.setattr(config.postgres, "port", 5432)
        monkeypatch.setattr(config.postgres, "user", "app_user")

        mock_client = mocker.MagicMock()
        expected_token = "rds-token-xyz"  # noqa: S105
        mock_client.generate_db_auth_token.return_value = expected_token
        mock_boto = mocker.patch("app.common.postgres.boto3.client")
        mock_boto.return_value = mock_client

        cparams = {}
        get_token(None, None, None, cparams)

        assert cparams["password"] == expected_token
        mock_client.generate_db_auth_token.assert_called_once_with(
            Region="eu-west-2",
            DBHostname="db.example.com",
            Port=5432,
            DBUsername="app_user",
        )


class TestGetSqlEngine:
    @pytest.mark.asyncio
    async def test_returns_cached_engine(self, mocker, monkeypatch):
        monkeypatch.setattr(config.postgres, "rds_truststore", None)
        mock_engine = mocker.MagicMock()
        postgres_module.engine = mock_engine

        result = await get_sql_engine()

        assert result is mock_engine

    @pytest.mark.asyncio
    async def test_creates_engine_without_cert(self, mocker, monkeypatch):
        monkeypatch.setattr(config.postgres, "rds_truststore", None)
        monkeypatch.setattr(config.postgres, "ssl_mode", "require")
        monkeypatch.setattr(config, "python_env", "development")

        mock_engine = mocker.MagicMock()
        mock_conn = mocker.MagicMock()
        mock_conn.execute = mocker.AsyncMock()
        mock_conn.__aenter__ = mocker.AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = mocker.AsyncMock(return_value=None)
        mock_engine.connect.return_value = mocker.MagicMock(
            __aenter__=mocker.AsyncMock(return_value=mock_conn),
            __aexit__=mocker.AsyncMock(return_value=None),
        )

        mock_create = mocker.patch(
            "app.common.postgres.sqlalchemy.ext.asyncio.create_async_engine",
            return_value=mock_engine,
        )
        mock_listen = mocker.patch("app.common.postgres.sqlalchemy.event.listen")

        result = await get_sql_engine()

        assert result is mock_engine
        mock_create.assert_called_once()
        call_kw = mock_create.call_args[1]
        assert call_kw["connect_args"] == {
            "sslmode": "require",
            "connect_timeout": config.timeouts.postgres_connect_timeout,
        }
        mock_listen.assert_called_once_with(
            mock_engine.sync_engine, "do_connect", get_token
        )

    @pytest.mark.asyncio
    async def test_creates_engine_with_cert(self, mocker, monkeypatch):
        from app.common import tls

        monkeypatch.setattr(config.postgres, "rds_truststore", "TRUSTSTORE_RDS_ROOT_CA")
        monkeypatch.setattr(config.postgres, "ssl_mode", "verify-full")
        monkeypatch.setattr(config, "python_env", "production")
        monkeypatch.setattr(
            tls, "custom_ca_certs", {"TRUSTSTORE_RDS_ROOT_CA": "/path/to/ca.pem"}
        )

        mock_engine = mocker.MagicMock()
        mock_conn = mocker.MagicMock()
        mock_conn.execute = mocker.AsyncMock()
        mock_conn.__aenter__ = mocker.AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = mocker.AsyncMock(return_value=None)
        mock_engine.connect.return_value = mocker.MagicMock(
            __aenter__=mocker.AsyncMock(return_value=mock_conn),
            __aexit__=mocker.AsyncMock(return_value=None),
        )

        mock_create = mocker.patch(
            "app.common.postgres.sqlalchemy.ext.asyncio.create_async_engine",
            return_value=mock_engine,
        )
        mocker.patch("app.common.postgres.sqlalchemy.event.listen")

        result = await get_sql_engine()

        assert result is mock_engine
        mock_create.assert_called_once()
        call_kw = mock_create.call_args[1]
        assert call_kw["connect_args"] == {
            "sslmode": "verify-full",
            "sslrootcert": "/path/to/ca.pem",
            "connect_timeout": config.timeouts.postgres_connect_timeout,
        }


class TestCheckConnection:
    @pytest.mark.asyncio
    async def test_executes_select(self, mocker):
        mock_conn = mocker.MagicMock()
        mock_conn.execute = mocker.AsyncMock()
        mock_conn.__aenter__ = mocker.AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = mocker.AsyncMock(return_value=None)

        mock_engine = mocker.MagicMock()
        mock_engine.connect.return_value = mocker.MagicMock(
            __aenter__=mocker.AsyncMock(return_value=mock_conn),
            __aexit__=mocker.AsyncMock(return_value=None),
        )

        result = await check_connection(mock_engine)

        assert result is True
        mock_conn.execute.assert_awaited_once()


class TestCloseEngine:
    @pytest.mark.asyncio
    async def test_disposes_and_clears_engine(self, mocker):
        mock_engine = mocker.MagicMock()
        mock_engine.dispose = mocker.AsyncMock()
        postgres_module.engine = mock_engine

        await close_engine()

        mock_engine.dispose.assert_awaited_once()
        assert postgres_module.engine is None

    @pytest.mark.asyncio
    async def test_noop_when_engine_none(self):
        postgres_module.engine = None
        await close_engine()
        assert postgres_module.engine is None
