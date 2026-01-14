from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from psycopg2 import pool
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class DatabaseConfig(BaseSettings):
    """Configuration for PostgreSQL database connections.

    Loads settings from environment variables with POSTGRES_ prefix.
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_pool_min_conn: int = 2
    postgres_pool_max_conn: int = 10


class DatabaseManager:
    """Manages PostgreSQL connection pooling and execution helpers."""

    def __init__(self, config: DatabaseConfig) -> None:
        """Initialise the connection pool.

        Args:
            config (DatabaseConfig): Database configuration settings.
        """
        self.config = config
        try:
            self.pool = pool.ThreadedConnectionPool(
                minconn=config.postgres_pool_min_conn,
                maxconn=config.postgres_pool_max_conn,
                host=config.postgres_host,
                port=config.postgres_port,
                database=config.postgres_db,
                user=config.postgres_user,
                password=config.postgres_password,
            )
            logger.info("PostgreSQL connection pool initialised.")
        except Exception as e:
            logger.error("Failed to initialise PostgreSQL connection pool: %s", e)
            raise

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """Context manager to acquire and release a database connection.

        Yields:
            psycopg2.extensions.connection: A connection from the pool.
        """
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)

    def execute_upsert(self, query: str, params: tuple[Any, ...] | dict[str, Any]) -> None:
        """Execute an idempotent INSERT ... ON CONFLICT DO NOTHING statement.

        Args:
            query (str): The SQL query to execute.
            params (tuple | dict): Parameters for the query.
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(query, params)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logger.error("Database upsert failed: %s", e)
                    raise

    def close(self) -> None:
        """Close all connections in the pool."""
        if hasattr(self, "pool"):
            self.pool.closeall()
            logger.info("PostgreSQL connection pool closed.")
