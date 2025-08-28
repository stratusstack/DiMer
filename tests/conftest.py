"""Test configuration and fixtures for DiffForge tests."""

import os
from typing import Any, Dict
from unittest.mock import Mock, patch

import pytest

from diffforge.core.factory import ConnectorFactory
from diffforge.core.manager import ConnectionManager
from diffforge.core.models import ConnectionConfig
from diffforge.metrics.collector import MetricsCollector


@pytest.fixture
def sample_connection_config():
    """Sample connection configuration for testing."""
    return ConnectionConfig(
        host="localhost",
        port=5432,
        username="testuser",
        password="testpass",
        database="testdb",
        schema_name="test_schema",
        extra_params={"test_param": "test_value"},
    )


@pytest.fixture
def snowflake_connection_config():
    """Snowflake-specific connection configuration for testing."""
    return ConnectionConfig(
        host="test-account.snowflakecomputing.com",
        username="testuser",
        password="testpass",
        database="TESTDB",
        schema_name="TEST_SCHEMA",
        extra_params={
            "warehouse": "TEST_WH",
            "role": "TEST_ROLE",
        },
    )


@pytest.fixture
def postgresql_connection_config():
    """PostgreSQL-specific connection configuration for testing."""
    return ConnectionConfig(
        host="localhost",
        port=5432,
        username="postgres",
        password="testpass",
        database="testdb",
        schema_name="public",
        extra_params={"ssl_mode": "prefer"},
    )


@pytest.fixture
def mock_connection():
    """Mock database connection object."""
    connection = Mock()
    connection.cursor.return_value = Mock()
    connection.cursor.return_value.execute = Mock()
    connection.cursor.return_value.fetchall.return_value = [("test_data",)]
    connection.cursor.return_value.description = [("column1",)]
    connection.close = Mock()
    return connection


@pytest.fixture
def connection_manager():
    """Connection manager instance for testing."""
    manager = ConnectionManager()
    yield manager
    manager.close_all()


@pytest.fixture
def metrics_collector():
    """Metrics collector instance for testing."""
    collector = MetricsCollector(max_records=100, cleanup_interval=3600)
    yield collector
    collector.stop()


@pytest.fixture(autouse=True)
def reset_factory():
    """Reset the connector factory before each test."""
    # Clear registered connectors
    ConnectorFactory._connectors.clear()
    yield
    # Re-register connectors after test
    try:
        from diffforge.core.factory import _auto_register_connectors

        _auto_register_connectors()
    except ImportError:
        pass  # Ignore if connectors are not available


@pytest.fixture
def mock_snowflake_connector():
    """Mock Snowflake connector for testing without actual dependencies."""
    with patch("snowflake.connector.connect") as mock_connect:
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ["8.0.0"]
        mock_cursor.description = [("CURRENT_VERSION()",)]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        yield mock_conn


@pytest.fixture
def mock_postgresql_connector():
    """Mock PostgreSQL connector for testing without actual dependencies."""
    with patch("psycopg2.pool.ThreadedConnectionPool") as mock_pool:
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ["PostgreSQL 14.0"]
        mock_cursor.description = [("version",)]
        mock_conn.cursor.return_value = mock_cursor

        mock_pool_instance = Mock()
        mock_pool_instance.getconn.return_value = mock_conn
        mock_pool_instance.putconn = Mock()
        mock_pool_instance.closeall = Mock()
        mock_pool.return_value = mock_pool_instance

        yield mock_pool_instance


@pytest.fixture
def sample_table_metadata():
    """Sample table metadata for testing."""
    from diffforge.core.models import ColumnMetadata, TableMetadata

    columns = [
        ColumnMetadata(
            name="id", data_type="int64", nullable=False, is_primary_key=True
        ),
        ColumnMetadata(name="name", data_type="string", nullable=True, max_length=255),
        ColumnMetadata(name="created_at", data_type="timestamp", nullable=False),
    ]

    return TableMetadata(
        columns=columns,
        row_count=1000,
        size_bytes=50000,
        statistics={"schema": "test", "table": "test_table"},
    )


@pytest.fixture
def sample_dataframe():
    """Sample pandas DataFrame for testing."""
    import pandas as pd

    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "city": ["New York", "London", "Tokyo", "Paris", "Berlin"],
        }
    )


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment variables and configuration."""
    # Set test environment variables
    os.environ["TESTING"] = "true"
    os.environ["LOG_LEVEL"] = "DEBUG"

    # Configure structlog for testing
    import structlog

    structlog.configure(
        processors=[
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(30),  # WARNING level
        logger_factory=structlog.testing.ReturnLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    yield

    # Clean up
    os.environ.pop("TESTING", None)
    os.environ.pop("LOG_LEVEL", None)


# Pytest markers for different test categories
pytest_plugins = []


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line(
        "markers", "requires_db: mark test as requiring database connection"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test location."""
    for item in items:
        # Add markers based on test file location
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        else:
            item.add_marker(pytest.mark.unit)

        # Add slow marker for tests that might be slow
        if any(
            keyword in item.name.lower() for keyword in ["connection", "query", "large"]
        ):
            item.add_marker(pytest.mark.slow)
