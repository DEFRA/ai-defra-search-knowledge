import logging

import boto3
import sqlalchemy
import sqlalchemy.event
import sqlalchemy.ext.asyncio

from app.common import tls
from app.config import config

logger = logging.getLogger(__name__)

engine: sqlalchemy.ext.asyncio.AsyncEngine | None = None


def get_token(dialect, conn_rec, cargs, cparams):  # noqa: ARG001
    if config.python_env == "development":
        cparams["password"] = config.postgres.password
    else:
        logger.info("Generating RDS auth token for Postgres connection")
        client = boto3.client("rds")
        token = client.generate_db_auth_token(
            Region=config.aws_region,
            DBHostname=config.postgres.host,
            Port=config.postgres.port,
            DBUsername=config.postgres.user,
        )
        logger.info("Generated RDS auth token for Postgres connection")
        cparams["password"] = token


async def get_sql_engine() -> sqlalchemy.ext.asyncio.AsyncEngine:
    global engine

    if engine is not None:
        return engine

    url = sqlalchemy.URL.create(
        drivername="postgresql+psycopg",
        username=config.postgres.user,
        host=config.postgres.host,
        port=config.postgres.port,
        database=config.postgres.database,
    )

    cert = (
        tls.custom_ca_certs.get(config.postgres.rds_truststore)
        if config.postgres.rds_truststore
        else None
    )

    if cert:
        logger.info(
            "Creating Postgres SQLAlchemy engine with custom TLS cert %s",
            config.postgres.rds_truststore,
        )
        engine = sqlalchemy.ext.asyncio.create_async_engine(
            url,
            connect_args={
                "sslmode": config.postgres.ssl_mode,
                "sslrootcert": cert,
                "connect_timeout": config.timeouts.postgres_connect_timeout,
            },
            hide_parameters=config.python_env != "development",
        )
    else:
        logger.info("Creating Postgres SQLAlchemy engine without custom TLS cert")
        engine = sqlalchemy.ext.asyncio.create_async_engine(
            url,
            connect_args={
                "sslmode": config.postgres.ssl_mode,
                "connect_timeout": config.timeouts.postgres_connect_timeout,
            },
            hide_parameters=config.python_env != "development",
        )

    sqlalchemy.event.listen(engine.sync_engine, "do_connect", get_token)

    logger.info("Testing Postgres SQLAlchemy connection to %s", config.postgres.host)
    await check_connection(engine)

    return engine


async def check_connection(eng: sqlalchemy.ext.asyncio.AsyncEngine) -> bool:
    async with eng.connect() as connection:
        await connection.execute(sqlalchemy.text("SELECT 1"))
    return True


async def close_engine() -> None:
    global engine
    if engine is not None:
        await engine.dispose()
        engine = None
        logger.info("Postgres engine closed")
