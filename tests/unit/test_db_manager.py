from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from consumer.db import DatabaseConfig, DatabaseManager


@pytest.fixture
def mock_config():
    config = MagicMock(spec=DatabaseConfig)
    config.postgres_host = "localhost"
    config.postgres_port = 5432
    config.postgres_user = "user"
    config.postgres_password = "password"
    config.postgres_db = "db"
    config.postgres_pool_min_conn = 1
    config.postgres_pool_max_conn = 2
    return config

@patch("psycopg2.pool.ThreadedConnectionPool")
def test_db_manager_init(mock_pool, mock_config):
    """Verifies DatabaseManager initialises connection pool."""
    DatabaseManager(mock_config)
    assert mock_pool.called

@patch("psycopg2.pool.ThreadedConnectionPool")
def test_db_manager_get_connection(mock_pool, mock_config):
    """Verifies get_connection context manager."""
    manager = DatabaseManager(mock_config)
    mock_conn = MagicMock()
    mock_pool.return_value.getconn.return_value = mock_conn

    with manager.get_connection() as conn:
        assert conn == mock_conn

    assert mock_pool.return_value.putconn.called

@patch("psycopg2.pool.ThreadedConnectionPool")
def test_db_manager_execute_upsert(mock_pool, mock_config):
    """Verifies execute_upsert executes and commits."""
    manager = DatabaseManager(mock_config)
    mock_conn = MagicMock()
    mock_pool.return_value.getconn.return_value = mock_conn
    mock_cur = mock_conn.cursor.return_value.__enter__.return_value

    manager.execute_upsert("INSERT ...", {"key": "val"})
    assert mock_cur.execute.called
    assert mock_conn.commit.called
