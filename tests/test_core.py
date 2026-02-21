"""Tests for core components."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest

from dimer.core.base import DataSourceConnector
from dimer.core.factory import ConnectorFactory
from dimer.core.manager import ConnectionManager
from dimer.core.models import (
    ColumnMetadata,
    ConnectionConfig,
    ConnectionMethod,
    TableMetadata,
)
from dimer.core.types import DataTypeMapper


class TestDataTypeMapper:
    """Tests for DataTypeMapper."""

    def test_map_snowflake_types(self):
        """Test mapping Snowflake types to common types."""
        assert DataTypeMapper.map_type("snowflake", "VARCHAR") == "string"
        assert DataTypeMapper.map_type("snowflake", "NUMBER") == "decimal"
        assert DataTypeMapper.map_type("snowflake", "TIMESTAMP") == "timestamp"
        assert DataTypeMapper.map_type("snowflake", "VARIANT") == "json"

    def test_map_postgresql_types(self):
        """Test mapping PostgreSQL types to common types."""
        assert DataTypeMapper.map_type("postgresql", "varchar") == "string"
        assert DataTypeMapper.map_type("postgresql", "integer") == "int32"
        assert DataTypeMapper.map_type("postgresql", "bigint") == "int64"
        assert DataTypeMapper.map_type("postgresql", "json") == "json"

    def test_case_insensitive_mapping(self):
        """Test case-insensitive type mapping."""
        assert DataTypeMapper.map_type("snowflake", "varchar") == "string"
        assert DataTypeMapper.map_type("snowflake", "VARCHAR") == "string"
        assert DataTypeMapper.map_type("postgresql", "VARCHAR") == "string"

    def test_parameterized_types(self):
        """Test mapping of parameterized types."""
        assert DataTypeMapper.map_type("snowflake", "VARCHAR(255)") == "string"
        assert DataTypeMapper.map_type("postgresql", "numeric(10,2)") == "decimal"

    def test_unknown_type_fallback(self):
        """Test fallback for unknown types."""
        assert DataTypeMapper.map_type("snowflake", "UNKNOWN_TYPE") == "string"
        assert DataTypeMapper.map_type("postgresql", "unknown_type") == "string"

    def test_unsupported_source(self):
        """Test error handling for unsupported sources."""
        with pytest.raises(ValueError, match="Unsupported source type"):
            DataTypeMapper.map_type("unsupported_db", "VARCHAR")

    def test_get_supported_sources(self):
        """Test getting supported source types."""
        sources = DataTypeMapper.get_supported_sources()
        assert "snowflake" in sources
        assert "postgresql" in sources
        assert "mysql" in sources

    def test_get_common_types(self):
        """Test getting common type names."""
        common_types = DataTypeMapper.get_common_types()
        assert "string" in common_types
        assert "int64" in common_types
        assert "decimal" in common_types

    def test_get_source_types(self):
        """Test getting source-specific types."""
        snowflake_types = DataTypeMapper.get_source_types("snowflake")
        assert "VARCHAR" in snowflake_types
        assert "NUMBER" in snowflake_types

        pg_types = DataTypeMapper.get_source_types("postgresql")
        assert "varchar" in pg_types
        assert "integer" in pg_types


class MockConnector(DataSourceConnector):
    """Mock connector for testing base functionality."""

    def get_required_params(self):
        return ["host", "username", "password"]

    def get_connection_methods(self):
        return [ConnectionMethod.NATIVE, ConnectionMethod.SQLALCHEMY]

    def _connect_native(self):
        mock_conn = Mock()
        mock_conn.test_method = Mock(return_value="connected")
        return mock_conn

    def _connect_sqlalchemy(self):
        raise ConnectionError("SQLAlchemy connection failed")

    def _execute_query_internal(self, query, params=None):
        import pandas as pd

        return pd.DataFrame({"result": ["test_data"]})

    def get_table_metadata(self, table_name, schema_name=None):
        columns = [
            ColumnMetadata(name="test_column", data_type="string", nullable=True)
        ]
        return TableMetadata(columns=columns)

    def get_sample_data(self, table_name, limit=10, schema_name=None):
        import pandas as pd

        return pd.DataFrame({"sample": ["data1", "data2"]})

    def _list_tables_internal(self, schema_name=None):
        return ["table1", "table2"]

    def _list_schemas_internal(self):
        return ["schema1", "schema2"]

    def _get_test_query(self):
        return "SELECT 1"


class TestDataSourceConnector:
    """Tests for DataSourceConnector base class."""

    def test_connector_initialization(self, sample_connection_config):
        """Test connector initialization."""
        connector = MockConnector(sample_connection_config)
        assert connector.connection_config == sample_connection_config
        assert connector.connection is None
        assert len(connector.connection_methods) == 2

    def test_successful_connection(self, sample_connection_config):
        """Test successful connection using fallback mechanism."""
        connector = MockConnector(sample_connection_config)
        connection = connector.connect()

        assert connection is not None
        assert connector.connection_method_used == ConnectionMethod.NATIVE
        assert connector.connection.test_method() == "connected"

    def test_connection_fallback(self, sample_connection_config):
        """Test connection fallback when first method fails."""
        connector = MockConnector(sample_connection_config)

        # Mock the first method to fail, second to succeed
        with patch.object(
            connector, "_connect_native", side_effect=ConnectionError("Native failed")
        ):
            with patch.object(connector, "_connect_sqlalchemy") as mock_sqlalchemy:
                mock_conn = Mock()
                mock_sqlalchemy.return_value = mock_conn

                connection = connector.connect()
                assert connection == mock_conn
                assert connector.connection_method_used == ConnectionMethod.SQLALCHEMY

    def test_all_connections_fail(self, sample_connection_config):
        """Test when all connection methods fail."""
        connector = MockConnector(sample_connection_config)

        with patch.object(
            connector, "_connect_native", side_effect=ConnectionError("Native failed")
        ):
            with pytest.raises(ConnectionError, match="All connection methods failed"):
                connector.connect()

    def test_missing_required_params(self):
        """Test validation of required parameters."""
        config = ConnectionConfig(host="localhost")  # Missing username and password

        with pytest.raises(ValueError, match="Missing required parameters"):
            MockConnector(config)

    def test_execute_query(self, sample_connection_config):
        """Test query execution."""
        connector = MockConnector(sample_connection_config)
        connector.connect()

        result = connector.execute_query("SELECT * FROM test")
        assert result.data is not None
        assert result.execution_time > 0
        assert result.query == "SELECT * FROM test"

    def test_get_table_metadata(self, sample_connection_config):
        """Test table metadata retrieval."""
        connector = MockConnector(sample_connection_config)
        connector.connect()

        metadata = connector.get_table_metadata("test_table")
        assert len(metadata.columns) == 1
        assert metadata.columns[0].name == "test_column"

    def test_get_sample_data(self, sample_connection_config):
        """Test sample data retrieval."""
        connector = MockConnector(sample_connection_config)
        connector.connect()

        sample = connector.get_sample_data("test_table", limit=5)
        assert not sample.empty
        assert "sample" in sample.columns

    def test_list_tables(self, sample_connection_config):
        """Test table listing."""
        connector = MockConnector(sample_connection_config)
        connector.connect()

        tables = connector.list_tables()
        assert "table1" in tables
        assert "table2" in tables

    def test_list_schemas(self, sample_connection_config):
        """Test schema listing."""
        connector = MockConnector(sample_connection_config)
        connector.connect()

        schemas = connector.list_schemas()
        assert "schema1" in schemas
        assert "schema2" in schemas

    def test_test_connection(self, sample_connection_config):
        """Test connection testing."""
        connector = MockConnector(sample_connection_config)

        assert connector.test_connection() is True
        assert connector.connection is not None

    def test_context_manager(self, sample_connection_config):
        """Test connector as context manager."""
        with MockConnector(sample_connection_config) as connector:
            assert connector.connection is not None

        # Connection should be closed after context
        assert connector.connection is None

    def test_connection_metrics(self, sample_connection_config):
        """Test connection metrics collection."""
        connector = MockConnector(sample_connection_config)
        connector.connect()

        metrics = connector.get_connection_metrics()
        assert len(metrics) > 0
        assert metrics[0]["method"] == "native"
        assert metrics[0]["success"] is True


class TestConnectorFactory:
    """Tests for ConnectorFactory."""

    def test_register_connector(self):
        """Test registering a connector."""
        ConnectorFactory.register_connector("test", MockConnector)
        assert "test" in ConnectorFactory.get_supported_sources()

    def test_register_invalid_connector(self):
        """Test registering invalid connector class."""

        class InvalidConnector:
            pass

        with pytest.raises(ValueError, match="must inherit from DataSourceConnector"):
            ConnectorFactory.register_connector("invalid", InvalidConnector)

    def test_create_connector(self, sample_connection_config):
        """Test creating a connector."""
        ConnectorFactory.register_connector("test", MockConnector)

        connector = ConnectorFactory.create_connector("test", sample_connection_config)
        assert isinstance(connector, MockConnector)

    def test_create_unsupported_connector(self, sample_connection_config):
        """Test creating unsupported connector."""
        with pytest.raises(ValueError, match="Unsupported source type"):
            ConnectorFactory.create_connector("unsupported", sample_connection_config)

    def test_is_source_supported(self):
        """Test checking if source is supported."""
        ConnectorFactory.register_connector("test", MockConnector)

        assert ConnectorFactory.is_source_supported("test") is True
        assert ConnectorFactory.is_source_supported("unsupported") is False


class TestConnectionManager:
    """Tests for ConnectionManager."""

    def test_manager_initialization(self):
        """Test connection manager initialization."""
        manager = ConnectionManager(default_pool_size=10)
        assert manager._default_pool_size == 10
        manager.close_all()

    def test_create_connection(self, sample_connection_config):
        """Test creating a connection."""
        ConnectorFactory.register_connector("test", MockConnector)
        manager = ConnectionManager()

        connector = manager.create_connection(
            connection_id="test_conn",
            source_type="test",
            connection_config=sample_connection_config,
        )

        assert isinstance(connector, MockConnector)
        assert connector.connection is not None

        manager.close_all()

    def test_get_connection(self, sample_connection_config):
        """Test retrieving a connection."""
        ConnectorFactory.register_connector("test", MockConnector)
        manager = ConnectionManager()

        # Create connection
        manager.create_connection(
            connection_id="test_conn",
            source_type="test",
            connection_config=sample_connection_config,
        )

        # Retrieve connection
        connector = manager.get_connection("test_conn")
        assert connector is not None
        assert isinstance(connector, MockConnector)

        manager.close_all()

    def test_remove_connection(self, sample_connection_config):
        """Test removing a connection."""
        ConnectorFactory.register_connector("test", MockConnector)
        manager = ConnectionManager()

        # Create connection
        manager.create_connection(
            connection_id="test_conn",
            source_type="test",
            connection_config=sample_connection_config,
        )

        # Remove connection
        result = manager.remove_connection("test_conn")
        assert result is True

        # Verify connection is gone
        connector = manager.get_connection("test_conn")
        assert connector is None

        manager.close_all()

    def test_list_connections(self, sample_connection_config):
        """Test listing connections."""
        ConnectorFactory.register_connector("test", MockConnector)
        manager = ConnectionManager()

        # Create connections
        manager.create_connection(
            connection_id="test_conn1",
            source_type="test",
            connection_config=sample_connection_config,
        )
        manager.create_connection(
            connection_id="test_conn2",
            source_type="test",
            connection_config=sample_connection_config,
        )

        # List connections
        connections = manager.list_connections()
        assert len(connections) == 2

        conn_ids = [conn["connection_id"] for conn in connections]
        assert "test_conn1" in conn_ids
        assert "test_conn2" in conn_ids

        manager.close_all()

    def test_test_connection(self, sample_connection_config):
        """Test testing a connection."""
        ConnectorFactory.register_connector("test", MockConnector)
        manager = ConnectionManager()

        # Create connection
        manager.create_connection(
            connection_id="test_conn",
            source_type="test",
            connection_config=sample_connection_config,
        )

        # Test connection
        result = manager.test_connection("test_conn")
        assert result is True

        manager.close_all()

    def test_reconnect(self, sample_connection_config):
        """Test reconnecting a connection."""
        ConnectorFactory.register_connector("test", MockConnector)
        manager = ConnectionManager()

        # Create connection
        manager.create_connection(
            connection_id="test_conn",
            source_type="test",
            connection_config=sample_connection_config,
        )

        # Reconnect
        result = manager.reconnect("test_conn")
        assert result is True

        manager.close_all()

    def test_duplicate_connection_id(self, sample_connection_config):
        """Test creating connection with duplicate ID."""
        ConnectorFactory.register_connector("test", MockConnector)
        manager = ConnectionManager()

        # Create first connection
        manager.create_connection(
            connection_id="test_conn",
            source_type="test",
            connection_config=sample_connection_config,
        )

        # Try to create duplicate
        with pytest.raises(
            ValueError, match="Connection ID 'test_conn' already exists"
        ):
            manager.create_connection(
                connection_id="test_conn",
                source_type="test",
                connection_config=sample_connection_config,
            )

        manager.close_all()

    def test_context_manager(self, sample_connection_config):
        """Test connection manager as context manager."""
        ConnectorFactory.register_connector("test", MockConnector)

        with ConnectionManager() as manager:
            manager.create_connection(
                connection_id="test_conn",
                source_type="test",
                connection_config=sample_connection_config,
            )

            connections = manager.list_connections()
            assert len(connections) == 1

        # Manager should be closed after context
