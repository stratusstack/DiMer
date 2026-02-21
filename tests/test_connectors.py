"""Tests for connector implementations."""

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pandas as pd
import pytest

from dimer.connectors.postgresql.connector import PostgreSQLConnector
from dimer.connectors.snowflake.connector import SnowflakeConnector
from dimer.core.models import ConnectionConfig, ConnectionMethod


class TestSnowflakeConnector:
    """Tests for SnowflakeConnector."""

    def test_required_params(self):
        """Test required parameters for Snowflake."""
        connector = SnowflakeConnector.__new__(SnowflakeConnector)
        required = connector.get_required_params()

        expected = ["host", "username", "password", "database"]
        assert all(param in required for param in expected)

    def test_connection_methods(self):
        """Test connection methods order."""
        connector = SnowflakeConnector.__new__(SnowflakeConnector)
        methods = connector.get_connection_methods()

        # Should start with optimized methods
        assert methods[0] == ConnectionMethod.ARROW
        assert methods[1] == ConnectionMethod.NATIVE
        assert ConnectionMethod.SQLALCHEMY in methods

    def test_extract_account_from_host(self, snowflake_connection_config):
        """Test extracting account from different host formats."""
        connector = SnowflakeConnector(snowflake_connection_config)

        # Test standard format
        assert connector._extract_account_from_host() == "test-account"

        # Test with protocol
        connector.connection_config.host = "https://test-account.snowflakecomputing.com"
        assert connector._extract_account_from_host() == "test-account"

        # Test with port
        connector.connection_config.host = "test-account.snowflakecomputing.com:443"
        assert connector._extract_account_from_host() == "test-account"

        # Test direct account
        connector.connection_config.host = "my-account"
        assert connector._extract_account_from_host() == "my-account"

    @patch("snowflake.connector.connect")
    def test_arrow_connection(self, mock_connect, snowflake_connection_config):
        """Test Arrow connection method."""
        # Setup mock
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ["8.0.0"]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        connector = SnowflakeConnector(snowflake_connection_config)
        connection = connector._connect_arrow()

        assert connection == mock_conn
        mock_connect.assert_called_once()

        # Verify session parameters
        call_args = mock_connect.call_args[1]
        assert (
            call_args["session_parameters"]["PYTHON_CONNECTOR_QUERY_RESULT_FORMAT"]
            == "arrow"
        )

    def test_snowpark_connection_unavailable(self, snowflake_connection_config):
        """Test that Snowpark connection is not included in methods."""
        connector = SnowflakeConnector(snowflake_connection_config)
        methods = connector.get_connection_methods()

        # Snowpark should not be in the methods since we removed it
        assert ConnectionMethod.SNOWPARK not in methods

    @patch("sqlalchemy.create_engine")
    def test_sqlalchemy_connection(
        self, mock_create_engine, snowflake_connection_config
    ):
        """Test SQLAlchemy connection method."""
        # Setup mock
        mock_engine = Mock()
        mock_connection = Mock()
        mock_connection.execute.return_value.fetchone.return_value = ["8.0.0"]
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        mock_create_engine.return_value = mock_engine

        connector = SnowflakeConnector(snowflake_connection_config)
        engine = connector._connect_sqlalchemy()

        assert engine == mock_engine
        mock_create_engine.assert_called_once()

        # Verify connection URL format
        call_args = mock_create_engine.call_args[0]
        connection_url = call_args[0]
        assert "snowflake://" in connection_url
        assert "test-account" in connection_url

    def test_execute_query_sqlalchemy(self, snowflake_connection_config):
        """Test query execution with SQLAlchemy connection."""
        connector = SnowflakeConnector(snowflake_connection_config)

        # Mock SQLAlchemy engine
        import pandas as pd

        mock_df = pd.DataFrame({"result": [1, 2, 3]})

        with patch("pandas.read_sql", return_value=mock_df) as mock_read_sql:
            mock_engine = Mock()
            connector.connection = mock_engine
            connector.connection_method_used = ConnectionMethod.SQLALCHEMY

            df = connector._execute_query_internal("SELECT 1")
            assert not df.empty
            assert "result" in df.columns
            mock_read_sql.assert_called_once()

    def test_execute_query_native(self, snowflake_connection_config):
        """Test query execution with native connection."""
        connector = SnowflakeConnector(snowflake_connection_config)

        # Mock native connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.description = [("result",)]
        mock_cursor.fetchall.return_value = [(1,), (2,), (3,)]
        mock_conn.cursor.return_value = mock_cursor

        connector.connection = mock_conn
        connector.connection_method_used = ConnectionMethod.NATIVE

        df = connector._execute_query_internal("SELECT 1")
        assert not df.empty
        assert len(df) == 3
        assert "result" in df.columns

    def test_get_table_metadata(self, snowflake_connection_config):
        """Test table metadata retrieval."""
        connector = SnowflakeConnector(snowflake_connection_config)

        # Mock query results
        columns_data = pd.DataFrame(
            {
                "COLUMN_NAME": ["ID", "NAME"],
                "DATA_TYPE": ["NUMBER", "VARCHAR"],
                "IS_NULLABLE": ["NO", "YES"],
                "COLUMN_DEFAULT": [None, None],
                "CHARACTER_MAXIMUM_LENGTH": [None, 255],
                "NUMERIC_PRECISION": [38, None],
                "NUMERIC_SCALE": [0, None],
                "COMMENT": [None, None],
            }
        )

        stats_data = pd.DataFrame(
            {
                "ROW_COUNT": [1000],
                "BYTES": [50000],
                "LAST_ALTERED": [pd.Timestamp("2024-01-01")],
            }
        )

        with patch.object(connector, "_execute_query_internal") as mock_execute:
            mock_execute.side_effect = [columns_data, stats_data]

            metadata = connector.get_table_metadata("TEST_TABLE")

            assert len(metadata.columns) == 2
            assert metadata.columns[0].name == "ID"
            assert metadata.columns[0].data_type == "decimal"  # NUMBER maps to decimal
            assert metadata.row_count == 1000

    def test_get_sample_data_small(self, snowflake_connection_config):
        """Test sample data retrieval for small limit."""
        connector = SnowflakeConnector(snowflake_connection_config)

        sample_df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})

        with patch.object(
            connector, "_execute_query_internal", return_value=sample_df
        ) as mock_execute:
            result = connector.get_sample_data("TEST_TABLE", limit=10)

            assert result.equals(sample_df)
            # Should use simple LIMIT for small samples
            mock_execute.assert_called_once()
            query = mock_execute.call_args[0][0]
            assert "LIMIT 10" in query
            assert "TABLESAMPLE" not in query

    def test_get_sample_data_large(self, snowflake_connection_config):
        """Test sample data retrieval for large limit."""
        connector = SnowflakeConnector(snowflake_connection_config)

        sample_df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})

        with patch.object(
            connector, "_execute_query_internal", return_value=sample_df
        ) as mock_execute:
            result = connector.get_sample_data("TEST_TABLE", limit=5000)

            assert result.equals(sample_df)
            # Should use TABLESAMPLE for large samples
            mock_execute.assert_called_once()
            query = mock_execute.call_args[0][0]
            assert "TABLESAMPLE" in query
            assert "LIMIT 5000" in query


class TestPostgreSQLConnector:
    """Tests for PostgreSQLConnector."""

    def test_required_params(self):
        """Test required parameters for PostgreSQL."""
        connector = PostgreSQLConnector.__new__(PostgreSQLConnector)
        required = connector.get_required_params()

        expected = ["host", "username", "password", "database"]
        assert all(param in required for param in expected)

    def test_connection_methods(self):
        """Test connection methods order."""
        connector = PostgreSQLConnector.__new__(PostgreSQLConnector)
        methods = connector.get_connection_methods()

        # Should start with AsyncPG for performance
        assert methods[0] == ConnectionMethod.ASYNCPG
        assert ConnectionMethod.PSYCOPG2 in methods
        assert ConnectionMethod.SQLALCHEMY in methods

    @patch("asyncpg.create_pool")
    def test_asyncpg_connection(self, mock_create_pool, postgresql_connection_config):
        """Test AsyncPG connection method."""
        # Setup async mocks
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = "PostgreSQL 14.0"

        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        async def create_pool_mock(**kwargs):
            return mock_pool

        mock_create_pool.side_effect = create_pool_mock

        connector = PostgreSQLConnector(postgresql_connection_config)
        pool = connector._connect_asyncpg()

        assert pool == mock_pool
        mock_create_pool.assert_called_once()

        # Verify connection parameters
        call_args = mock_create_pool.call_args[1]
        assert call_args["host"] == "localhost"
        assert call_args["port"] == 5432
        assert call_args["user"] == "postgres"

    @patch("psycopg2.pool.ThreadedConnectionPool")
    def test_psycopg2_connection(self, mock_pool_class, postgresql_connection_config):
        """Test Psycopg2 connection method."""
        # Setup mock
        mock_pool = Mock()
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ["PostgreSQL 14.0"]
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_conn
        mock_pool_class.return_value = mock_pool

        connector = PostgreSQLConnector(postgresql_connection_config)
        pool = connector._connect_psycopg2()

        assert pool == mock_pool
        mock_pool_class.assert_called_once()

        # Verify connection parameters
        call_args = mock_pool_class.call_args[1]
        assert call_args["host"] == "localhost"
        assert call_args["port"] == 5432

    @patch("sqlalchemy.create_engine")
    def test_sqlalchemy_connection(
        self, mock_create_engine, postgresql_connection_config
    ):
        """Test SQLAlchemy connection method."""
        # Setup mock
        mock_engine = Mock()
        mock_connection = Mock()
        mock_connection.execute.return_value.fetchone.return_value = ["PostgreSQL 14.0"]
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        mock_create_engine.return_value = mock_engine

        connector = PostgreSQLConnector(postgresql_connection_config)
        engine = connector._connect_sqlalchemy()

        assert engine == mock_engine
        mock_create_engine.assert_called_once()

        # Verify connection URL
        call_args = mock_create_engine.call_args[0]
        connection_url = call_args[0]
        assert "postgresql://" in connection_url
        assert "localhost:5432" in connection_url

    def test_execute_query_asyncpg(self, postgresql_connection_config):
        """Test query execution with AsyncPG connection."""
        connector = PostgreSQLConnector(postgresql_connection_config)

        # Mock AsyncPG pool and connection
        mock_pool = Mock()
        mock_conn = AsyncMock()

        # Mock record objects
        mock_record1 = Mock()
        mock_record1.keys.return_value = ["id", "name"]
        mock_record1.values.return_value = [1, "Alice"]

        mock_record2 = Mock()
        mock_record2.keys.return_value = ["id", "name"]
        mock_record2.values.return_value = [2, "Bob"]

        mock_conn.fetch.return_value = [mock_record1, mock_record2]
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        # Mock the event loop
        with patch("asyncio.new_event_loop") as mock_loop_class:
            mock_loop = Mock()
            mock_loop_class.return_value = mock_loop
            mock_loop.run_until_complete.return_value = pd.DataFrame(
                {"id": [1, 2], "name": ["Alice", "Bob"]}
            )

            connector.connection = mock_pool
            connector.connection_method_used = ConnectionMethod.ASYNCPG
            connector._asyncpg_loop = mock_loop

            df = connector._execute_query_internal("SELECT * FROM users")
            assert not df.empty
            assert len(df) == 2

    def test_execute_query_psycopg2(self, postgresql_connection_config):
        """Test query execution with Psycopg2 connection."""
        connector = PostgreSQLConnector(postgresql_connection_config)

        # Mock Psycopg2 pool and connection
        mock_pool = Mock()
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_conn

        connector.connection = mock_pool
        connector.connection_method_used = ConnectionMethod.PSYCOPG2

        df = connector._execute_query_internal("SELECT * FROM users")
        assert not df.empty
        assert len(df) == 2
        assert list(df.columns) == ["id", "name"]

    def test_get_table_metadata(self, postgresql_connection_config):
        """Test table metadata retrieval."""
        connector = PostgreSQLConnector(postgresql_connection_config)

        # Mock query results
        columns_data = pd.DataFrame(
            {
                "column_name": ["id", "name"],
                "data_type": ["integer", "character varying"],
                "is_nullable": ["NO", "YES"],
                "column_default": ["nextval(seq)", None],
                "character_maximum_length": [None, 255],
                "numeric_precision": [32, None],
                "numeric_scale": [0, None],
                "udt_name": ["int4", "varchar"],
            }
        )

        pk_data = pd.DataFrame({"column_name": ["id"]})

        stats_data = pd.DataFrame(
            {
                "schemaname": ["public"],
                "tablename": ["users"],
                "inserts": [100],
                "updates": [50],
                "deletes": [5],
                "live_tuples": [95],
                "dead_tuples": [5],
                "last_vacuum": [pd.Timestamp("2024-01-01")],
                "last_analyze": [pd.Timestamp("2024-01-01")],
            }
        )

        with patch.object(connector, "_execute_query_internal") as mock_execute:
            mock_execute.side_effect = [columns_data, pk_data, stats_data]

            metadata = connector.get_table_metadata("users")

            assert len(metadata.columns) == 2
            assert metadata.columns[0].name == "id"
            assert metadata.columns[0].is_primary_key is True
            assert metadata.columns[1].name == "name"
            assert metadata.row_count == 95

    def test_get_sample_data(self, postgresql_connection_config):
        """Test sample data retrieval."""
        connector = PostgreSQLConnector(postgresql_connection_config)

        sample_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        with patch.object(
            connector, "_execute_query_internal", return_value=sample_df
        ) as mock_execute:
            result = connector.get_sample_data("users", limit=10)

            assert result.equals(sample_df)
            mock_execute.assert_called_once()
            query = mock_execute.call_args[0][0]
            assert "LIMIT 10" in query

    def test_close_connections(self, postgresql_connection_config):
        """Test closing different connection types."""
        connector = PostgreSQLConnector(postgresql_connection_config)

        # Test AsyncPG pool closing
        mock_pool = AsyncMock()
        mock_loop = Mock()
        connector.connection = mock_pool
        connector.connection_method_used = ConnectionMethod.ASYNCPG
        connector._asyncpg_loop = mock_loop

        connector.close()

        mock_loop.run_until_complete.assert_called()
        mock_loop.close.assert_called()

        # Test Psycopg2 pool closing
        mock_pool = Mock()
        connector.connection = mock_pool
        connector.connection_method_used = ConnectionMethod.PSYCOPG2

        connector.close()
        mock_pool.closeall.assert_called()

        # Test SQLAlchemy engine closing
        mock_engine = Mock()
        connector.connection = mock_engine
        connector.connection_method_used = ConnectionMethod.SQLALCHEMY

        connector.close()
        mock_engine.dispose.assert_called()
