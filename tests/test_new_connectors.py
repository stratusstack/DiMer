"""Tests for MySQL, BigQuery, Databricks, and file-based connectors."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from dimer.connectors.bigquery.connector import BigQueryConnector
from dimer.connectors.databricks.connector import DatabricksConnector
from dimer.connectors.files.csv_connector import CSVConnector
from dimer.connectors.files.parquet_connector import ParquetConnector
from dimer.connectors.mysql.connector import MySQLConnector
from dimer.core.models import ConnectionConfig, ConnectionMethod


class TestMySQLConnector:
    """Tests for MySQLConnector."""

    @pytest.fixture
    def mysql_config(self):
        """MySQL connection configuration for testing."""
        return ConnectionConfig(
            host="localhost",
            port=3306,
            username="testuser",
            password="testpass",
            database="testdb",
            extra_params={"charset": "utf8mb4"},
        )

    def test_required_params(self):
        """Test required parameters for MySQL."""
        connector = MySQLConnector.__new__(MySQLConnector)
        required = connector.get_required_params()

        expected = ["host", "username", "password", "database"]
        assert all(param in required for param in expected)

    def test_connection_methods(self):
        """Test connection methods order."""
        connector = MySQLConnector.__new__(MySQLConnector)
        methods = connector.get_connection_methods()

        assert methods[0] == ConnectionMethod.MYSQL_CONNECTOR
        assert ConnectionMethod.PYMYSQL in methods
        assert ConnectionMethod.SQLALCHEMY in methods

    @patch("mysql.connector.connect")
    def test_mysql_connector_connection(self, mock_connect, mysql_config):
        """Test MySQL Connector connection method."""
        # Setup mock
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ["8.0.28"]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        connector = MySQLConnector(mysql_config)
        connection = connector._connect_mysql_connector()

        assert connection == mock_conn
        mock_connect.assert_called_once()

    @patch("pymysql.connect")
    def test_pymysql_connection(self, mock_connect, mysql_config):
        """Test PyMySQL connection method."""
        # Setup mock
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ["8.0.28"]
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn

        connector = MySQLConnector(mysql_config)
        connection = connector._connect_pymysql()

        assert connection == mock_conn
        mock_connect.assert_called_once()

    def test_execute_query_mysql_connector(self, mysql_config):
        """Test query execution with MySQL Connector."""
        connector = MySQLConnector(mysql_config)

        # Mock connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.description = [("result",)]
        mock_cursor.fetchall.return_value = [(1,), (2,), (3,)]
        mock_conn.cursor.return_value = mock_cursor

        connector.connection = mock_conn
        connector.connection_method_used = ConnectionMethod.MYSQL_CONNECTOR

        df = connector._execute_query_internal("SELECT 1")
        assert not df.empty
        assert len(df) == 3
        assert "result" in df.columns


class TestBigQueryConnector:
    """Tests for BigQueryConnector."""

    @pytest.fixture
    def bigquery_config(self):
        """BigQuery connection configuration for testing."""
        return ConnectionConfig(
            database="test-project",  # project_id
            schema_name="test_dataset",
            extra_params={
                "location": "US",
                "credentials_path": "/path/to/credentials.json",
            },
        )

    def test_required_params(self):
        """Test required parameters for BigQuery."""
        connector = BigQueryConnector.__new__(BigQueryConnector)
        required = connector.get_required_params()

        assert "database" in required  # Used as project_id

    def test_connection_methods(self):
        """Test connection methods order."""
        connector = BigQueryConnector.__new__(BigQueryConnector)
        methods = connector.get_connection_methods()

        assert methods[0] == ConnectionMethod.BIGQUERY_STORAGE
        assert ConnectionMethod.NATIVE in methods
        assert ConnectionMethod.SQLALCHEMY in methods

    @patch("google.cloud.bigquery.Client")
    @patch("google.cloud.bigquery_storage.BigQueryReadClient")
    def test_storage_api_connection(
        self, mock_storage_client, mock_client, bigquery_config
    ):
        """Test BigQuery Storage API connection method."""
        # Setup mocks
        mock_bq_client = Mock()
        mock_job = Mock()
        mock_job.result.return_value = []
        mock_bq_client.query.return_value = mock_job
        mock_client.return_value = mock_bq_client

        mock_storage = Mock()
        mock_storage_client.return_value = mock_storage

        with patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "/fake/path"}):
            connector = BigQueryConnector(bigquery_config)
            connection = connector._connect_bigquery_storage()

        assert "client" in connection
        assert "storage_client" in connection
        assert connection["project_id"] == "test-project"

    @patch("google.cloud.bigquery.Client")
    def test_native_connection(self, mock_client, bigquery_config):
        """Test native BigQuery connection method."""
        # Setup mock
        mock_bq_client = Mock()
        mock_job = Mock()
        mock_job.result.return_value = []
        mock_bq_client.query.return_value = mock_job
        mock_client.return_value = mock_bq_client

        with patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "/fake/path"}):
            connector = BigQueryConnector(bigquery_config)
            client = connector._connect_native()

        assert client == mock_bq_client

    def test_execute_query_native(self, bigquery_config):
        """Test query execution with native BigQuery client."""
        connector = BigQueryConnector(bigquery_config)

        # Mock client and job
        mock_client = Mock()
        mock_job = Mock()
        mock_job.to_dataframe.return_value = pd.DataFrame({"result": [1, 2, 3]})
        mock_client.query.return_value = mock_job

        connector.connection = mock_client
        connector.connection_method_used = ConnectionMethod.NATIVE

        df = connector._execute_query_internal("SELECT 1")
        assert not df.empty
        assert "result" in df.columns


class TestDatabricksConnector:
    """Tests for DatabricksConnector."""

    @pytest.fixture
    def databricks_config(self):
        """Databricks connection configuration for testing."""
        return ConnectionConfig(
            host="test-workspace.cloud.databricks.com",
            password="test-token",
            extra_params={
                "warehouse_id": "test-warehouse-id",
                "catalog": "main",
                "schema": "default",
            },
        )

    def test_required_params(self):
        """Test required parameters for Databricks."""
        connector = DatabricksConnector.__new__(DatabricksConnector)
        required = connector.get_required_params()

        assert "host" in required

    def test_connection_methods(self):
        """Test connection methods order."""
        connector = DatabricksConnector.__new__(DatabricksConnector)
        methods = connector.get_connection_methods()

        assert methods[0] == ConnectionMethod.DATABRICKS_CONNECT
        assert ConnectionMethod.NATIVE in methods
        assert ConnectionMethod.SQLALCHEMY in methods

    @patch("databricks.sql.connect")
    def test_native_connection(self, mock_connect, databricks_config):
        """Test native Databricks SQL connection method."""
        # Setup mock
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ["Databricks Runtime 10.4"]
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn

        connector = DatabricksConnector(databricks_config)
        connection = connector._connect_native()

        assert connection == mock_conn
        mock_connect.assert_called_once()

    def test_get_auth_token(self, databricks_config):
        """Test authentication token extraction."""
        connector = DatabricksConnector(databricks_config)

        # Test token from password field
        token = connector._get_auth_token()
        assert token == "test-token"

        # Test token from extra_params
        databricks_config.extra_params["access_token"] = "explicit-token"
        connector = DatabricksConnector(databricks_config)
        token = connector._get_auth_token()
        assert token == "explicit-token"

    def test_execute_query_native(self, databricks_config):
        """Test query execution with native connection."""
        connector = DatabricksConnector(databricks_config)

        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.description = [("result",)]
        mock_cursor.fetchall.return_value = [(1,), (2,), (3,)]
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        connector.connection = mock_conn
        connector.connection_method_used = ConnectionMethod.NATIVE

        df = connector._execute_query_internal("SELECT 1")
        assert not df.empty
        assert len(df) == 3
        assert "result" in df.columns


class TestParquetConnector:
    """Tests for ParquetConnector."""

    @pytest.fixture
    def temp_parquet_files(self):
        """Create temporary parquet files for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            df1 = pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "age": [25, 30, 35],
                }
            )
            df2 = pd.DataFrame(
                {
                    "id": [4, 5, 6],
                    "name": ["David", "Eve", "Frank"],
                    "age": [40, 45, 50],
                }
            )

            # Save as parquet files
            file1 = Path(temp_dir) / "test1.parquet"
            file2 = Path(temp_dir) / "test2.parquet"

            df1.to_parquet(file1, engine="pyarrow")
            df2.to_parquet(file2, engine="pyarrow")

            yield temp_dir, [str(file1), str(file2)]

    def test_required_params(self):
        """Test required parameters for Parquet."""
        connector = ParquetConnector.__new__(ParquetConnector)
        required = connector.get_required_params()

        assert "host" in required  # Used as base path

    def test_connection_methods(self):
        """Test connection methods order."""
        connector = ParquetConnector.__new__(ParquetConnector)
        methods = connector.get_connection_methods()

        assert ConnectionMethod.PYARROW_DIRECT in methods
        assert ConnectionMethod.PANDAS_DIRECT in methods

    def test_pandas_direct_connection(self, temp_parquet_files):
        """Test pandas direct connection method."""
        temp_dir, files = temp_parquet_files

        config = ConnectionConfig(host=temp_dir)
        connector = ParquetConnector(config)

        connection = connector._connect_pandas_direct()

        assert connection["type"] == "pandas"
        assert connection["base_path"] == temp_dir
        assert len(connection["files"]) == 2
        assert connection["engine"] in ["pyarrow", "fastparquet", "auto"]

    def test_execute_query(self, temp_parquet_files):
        """Test query execution on parquet files."""
        temp_dir, files = temp_parquet_files

        config = ConnectionConfig(host=temp_dir)
        connector = ParquetConnector(config)
        connector.connect()

        # Test basic query
        df = connector._execute_query_internal("SELECT * FROM test1 LIMIT 2")
        assert not df.empty
        assert len(df) <= 2
        assert "id" in df.columns
        assert "name" in df.columns

    def test_get_table_metadata(self, temp_parquet_files):
        """Test table metadata extraction."""
        temp_dir, files = temp_parquet_files

        config = ConnectionConfig(host=temp_dir)
        connector = ParquetConnector(config)

        metadata = connector.get_table_metadata("test1")

        assert len(metadata.columns) == 3
        assert metadata.columns[0].name == "id"
        assert metadata.statistics["file_format"] == "parquet"

    def test_list_tables(self, temp_parquet_files):
        """Test listing parquet files as tables."""
        temp_dir, files = temp_parquet_files

        config = ConnectionConfig(host=temp_dir)
        connector = ParquetConnector(config)

        tables = connector.list_tables()

        assert "test1" in tables
        assert "test2" in tables


class TestCSVConnector:
    """Tests for CSVConnector."""

    @pytest.fixture
    def temp_csv_files(self):
        """Create temporary CSV files for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            df1 = pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "age": [25, 30, 35],
                }
            )
            df2 = pd.DataFrame(
                {
                    "id": [4, 5, 6],
                    "name": ["David", "Eve", "Frank"],
                    "age": [40, 45, 50],
                }
            )

            # Save as CSV files
            file1 = Path(temp_dir) / "test1.csv"
            file2 = Path(temp_dir) / "test2.csv"

            df1.to_csv(file1, index=False)
            df2.to_csv(file2, index=False)

            yield temp_dir, [str(file1), str(file2)]

    def test_required_params(self):
        """Test required parameters for CSV."""
        connector = CSVConnector.__new__(CSVConnector)
        required = connector.get_required_params()

        assert "host" in required  # Used as base path

    def test_connection_methods(self):
        """Test connection methods order."""
        connector = CSVConnector.__new__(CSVConnector)
        methods = connector.get_connection_methods()

        assert ConnectionMethod.PANDAS_DIRECT in methods

    def test_pandas_direct_connection(self, temp_csv_files):
        """Test pandas direct connection method."""
        temp_dir, files = temp_csv_files

        config = ConnectionConfig(host=temp_dir)
        connector = CSVConnector(config)

        connection = connector._connect_pandas_direct()

        assert connection["type"] == "pandas"
        assert connection["base_path"] == temp_dir
        assert len(connection["files"]) == 2
        assert "csv_params" in connection

    def test_detect_csv_params(self, temp_csv_files):
        """Test CSV parameter detection."""
        temp_dir, files = temp_csv_files

        config = ConnectionConfig(host=temp_dir)
        connector = CSVConnector(config)

        params = connector._detect_csv_params(files[0])

        assert params["sep"] == ","
        assert params["encoding"] == "utf-8"
        assert params["header"] == 0

    def test_execute_query(self, temp_csv_files):
        """Test query execution on CSV files."""
        temp_dir, files = temp_csv_files

        config = ConnectionConfig(host=temp_dir)
        connector = CSVConnector(config)
        connector.connect()

        # Test basic query
        df = connector._execute_query_internal("SELECT * FROM test1 LIMIT 2")
        assert not df.empty
        assert len(df) <= 2
        assert "id" in df.columns
        assert "name" in df.columns

    def test_get_table_metadata(self, temp_csv_files):
        """Test table metadata extraction."""
        temp_dir, files = temp_csv_files

        config = ConnectionConfig(host=temp_dir)
        connector = CSVConnector(config)

        metadata = connector.get_table_metadata("test1")

        assert len(metadata.columns) == 3
        assert metadata.columns[0].name == "id"
        assert metadata.statistics["file_format"] == "csv"
        assert metadata.statistics["separator"] == ","

    def test_list_tables(self, temp_csv_files):
        """Test listing CSV files as tables."""
        temp_dir, files = temp_csv_files

        config = ConnectionConfig(host=temp_dir)
        connector = CSVConnector(config)

        tables = connector.list_tables()

        assert "test1" in tables
        assert "test2" in tables

    def test_custom_separator(self):
        """Test CSV with custom separator."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create TSV file
            df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
            tsv_file = Path(temp_dir) / "test.tsv"
            df.to_csv(tsv_file, index=False, sep="\t")

            config = ConnectionConfig(host=temp_dir, extra_params={"separator": "\t"})
            connector = CSVConnector(config)

            params = connector._detect_csv_params(str(tsv_file))
            assert params["sep"] == "\t"


class TestConnectorIntegration:
    """Integration tests for all new connectors."""

    def test_factory_registration(self):
        """Test that all new connectors are registered with factory."""
        from dimer.core.factory import ConnectorFactory

        supported = ConnectorFactory.get_supported_sources()

        # Check all new connectors are registered
        assert "mysql" in supported
        assert "bigquery" in supported or "bq" in supported
        assert "databricks" in supported
        assert "parquet" in supported
        assert "csv" in supported

    def test_connector_creation(self):
        """Test creating connectors through factory."""
        from dimer.core.factory import ConnectorFactory

        # Test MySQL
        mysql_config = ConnectionConfig(
            host="localhost", username="test", password="test", database="test"
        )
        mysql_connector = ConnectorFactory.create_connector("mysql", mysql_config)
        assert isinstance(mysql_connector, MySQLConnector)

        # Test BigQuery
        bq_config = ConnectionConfig(database="test-project")
        bq_connector = ConnectorFactory.create_connector("bigquery", bq_config)
        assert isinstance(bq_connector, BigQueryConnector)

        # Test Databricks
        databricks_config = ConnectionConfig(host="test-workspace.cloud.databricks.com")
        databricks_connector = ConnectorFactory.create_connector(
            "databricks", databricks_config
        )
        assert isinstance(databricks_connector, DatabricksConnector)

        # Test Parquet
        parquet_config = ConnectionConfig(host="/tmp")
        parquet_connector = ConnectorFactory.create_connector("parquet", parquet_config)
        assert isinstance(parquet_connector, ParquetConnector)

        # Test CSV
        csv_config = ConnectionConfig(host="/tmp")
        csv_connector = ConnectorFactory.create_connector("csv", csv_config)
        assert isinstance(csv_connector, CSVConnector)
