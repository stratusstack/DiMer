"""Integration tests for Databricks connector with real workspace connection."""

import os

import pandas as pd
import pytest
from dotenv import load_dotenv

from dimer.connectors.databricks.connector import DatabricksConnector
from dimer.core.factory import ConnectorFactory
from dimer.core.models import ConnectionConfig, ConnectionMethod

# Load environment variables
load_dotenv()

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def real_databricks_config():
    """Real Databricks configuration from environment variables."""
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")

    # Skip tests if credentials are not provided
    if not all([host, token, http_path]):
        pytest.skip(
            "Databricks credentials not provided in environment variables "
            "(DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH required)"
        )

    config = ConnectionConfig(
        host=host,
        # password carries the token so _get_auth_token() picks it up
        password=token,
        database=os.getenv("DATABRICKS_CATALOG", "main"),
        schema_name=os.getenv("DATABRICKS_SCHEMA", "default"),
        extra_params={
            "http_path": http_path,
            "catalog": os.getenv("DATABRICKS_CATALOG", "main"),
            # cluster_id or warehouse_id for DATABRICKS_CONNECT method
            "warehouse_id": os.getenv("DATABRICKS_WAREHOUSE_ID", ""),
            "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID", ""),
        },
    )

    return config


# Target table used for table-level tests; can be overridden via env
TARGET_TABLE = os.getenv("DATABRICKS_TABLE", "")


@pytest.mark.requires_db
class TestDatabricksIntegration:
    """Integration tests for real Databricks connections."""

    def test_connection_methods_priority(self, real_databricks_config):
        """Test that connection methods are returned in the expected order."""
        connector = DatabricksConnector(real_databricks_config)
        methods = connector.get_connection_methods()

        assert methods[0] == ConnectionMethod.DATABRICKS_CONNECT
        assert methods[1] == ConnectionMethod.NATIVE
        assert ConnectionMethod.SQLALCHEMY in methods

    def test_real_connection_establishment(self, real_databricks_config):
        """Test establishing a real connection to Databricks via native SQL connector."""
        connector = DatabricksConnector(real_databricks_config)

        try:
            # connect() tries methods in order; DATABRICKS_CONNECT will likely
            # fail without a live cluster, so it falls back to NATIVE
            connection = connector.connect()
            assert connection is not None
            assert connector.connection_method_used is not None

            print(
                f"Successfully connected using: {connector.connection_method_used.value}"
            )

            assert connector.test_connection() is True

        finally:
            connector.close()

    def test_factory_creates_databricks_connector(self, real_databricks_config):
        """Test that the factory creates a Databricks connector correctly."""
        from dimer.core.factory import _auto_register_connectors

        _auto_register_connectors()

        connector = ConnectorFactory.create_connector(
            "databricks", real_databricks_config
        )

        assert isinstance(connector, DatabricksConnector)
        assert connector.connection_config == real_databricks_config

    def test_list_schemas(self, real_databricks_config):
        """Test listing schemas in the catalog."""
        connector = DatabricksConnector(real_databricks_config)

        try:
            connector.connect()
            schemas = connector.list_schemas()

            assert isinstance(schemas, list)
            assert len(schemas) > 0

            expected_schema = real_databricks_config.schema_name or "default"
            print(f"Found schemas: {schemas}")

            if expected_schema in schemas:
                print(f"Expected schema '{expected_schema}' found")
            else:
                print(
                    f"Expected schema '{expected_schema}' not found. "
                    f"Available: {schemas}"
                )

        finally:
            connector.close()

    def test_list_tables_in_schema(self, real_databricks_config):
        """Test listing tables in the target schema."""
        connector = DatabricksConnector(real_databricks_config)

        try:
            connector.connect()
            schema = real_databricks_config.schema_name or "default"
            tables = connector.list_tables(schema_name=schema)

            assert isinstance(tables, list)
            print(f"Tables in schema '{schema}': {tables}")

            if TARGET_TABLE and TARGET_TABLE in tables:
                print(f"Target table '{TARGET_TABLE}' found")
            elif TARGET_TABLE:
                print(
                    f"Target table '{TARGET_TABLE}' not found. "
                    f"Available: {tables}"
                )

        finally:
            connector.close()

    def test_target_table_metadata(self, real_databricks_config):
        """Test retrieving metadata for the target table."""
        if not TARGET_TABLE:
            pytest.skip(
                "DATABRICKS_TABLE env var not set; skipping table metadata test"
            )

        connector = DatabricksConnector(real_databricks_config)

        try:
            connector.connect()
            schema = real_databricks_config.schema_name or "default"
            tables = connector.list_tables(schema_name=schema)

            if TARGET_TABLE not in tables:
                pytest.skip(
                    f"Table '{TARGET_TABLE}' not found in schema '{schema}'"
                )

            metadata = connector.get_table_metadata(TARGET_TABLE, schema_name=schema)

            print(f"Table '{TARGET_TABLE}' metadata retrieved successfully")
            print(f"Columns: {len(metadata.columns)}")
            print(f"Row count: {metadata.row_count}")

            for col in metadata.columns:
                print(
                    f"  - {col.name}: {col.data_type} "
                    f"(nullable: {col.nullable})"
                )

            assert len(metadata.columns) > 0

        finally:
            connector.close()

    def test_sample_data_from_target_table(self, real_databricks_config):
        """Test retrieving sample data from the target table."""
        if not TARGET_TABLE:
            pytest.skip(
                "DATABRICKS_TABLE env var not set; skipping sample data test"
            )

        connector = DatabricksConnector(real_databricks_config)

        try:
            connector.connect()
            schema = real_databricks_config.schema_name or "default"
            tables = connector.list_tables(schema_name=schema)

            if TARGET_TABLE not in tables:
                pytest.skip(
                    f"Table '{TARGET_TABLE}' not found in schema '{schema}'"
                )

            sample_df = connector.get_sample_data(
                TARGET_TABLE, limit=5, schema_name=schema
            )

            print(f"Retrieved {len(sample_df)} rows from '{TARGET_TABLE}'")
            print(f"Columns: {list(sample_df.columns)}")
            print(sample_df.head())

            assert isinstance(sample_df, pd.DataFrame)
            if not sample_df.empty:
                assert len(sample_df) <= 5

        finally:
            connector.close()

    def test_query_execution(self, real_databricks_config):
        """Test executing a basic query on the Databricks SQL warehouse."""
        connector = DatabricksConnector(real_databricks_config)

        try:
            connector.connect()

            result = connector.execute_query(
                "SELECT current_version() AS dbr_version, "
                "current_user() AS current_user, "
                "current_catalog() AS current_catalog, "
                "current_schema() AS current_schema"
            )

            print(f"Query executed successfully")
            print(f"Execution time: {result.execution_time:.3f}s")
            print(f"Connection method: {result.metadata.get('connection_method')}")

            df = result.data
            assert isinstance(df, pd.DataFrame)
            assert not df.empty

            print(f"DBR version: {df.iloc[0]['dbr_version']}")
            print(f"Current catalog: {df.iloc[0]['current_catalog']}")

        finally:
            connector.close()

    def test_query_execution_on_target_table(self, real_databricks_config):
        """Test executing a COUNT query on the target table."""
        if not TARGET_TABLE:
            pytest.skip(
                "DATABRICKS_TABLE env var not set; skipping table query test"
            )

        connector = DatabricksConnector(real_databricks_config)

        try:
            connector.connect()
            schema = real_databricks_config.schema_name or "default"
            catalog = real_databricks_config.extra_params.get("catalog", "main")
            tables = connector.list_tables(schema_name=schema)

            if TARGET_TABLE not in tables:
                pytest.skip(
                    f"Table '{TARGET_TABLE}' not found in schema '{schema}'"
                )

            result = connector.execute_query(
                f"SELECT COUNT(*) AS total_rows "
                f"FROM `{catalog}`.`{schema}`.`{TARGET_TABLE}`"
            )

            df = result.data
            assert isinstance(df, pd.DataFrame)
            assert not df.empty

            total_rows = df.iloc[0]["total_rows"]
            print(f"Total rows in '{TARGET_TABLE}': {total_rows}")
            assert total_rows >= 0

        finally:
            connector.close()

    def test_connection_metrics(self, real_databricks_config):
        """Test that connection metrics are collected properly."""
        connector = DatabricksConnector(real_databricks_config)

        try:
            connector.connect()
            connector.execute_query("SELECT 1 AS probe")

            metrics = connector.get_connection_metrics()

            print(f"Connection metrics collected: {len(metrics)} entries")
            assert len(metrics) > 0

            successful_connection = next(
                (m for m in metrics if m.get("success") is True), None
            )

            assert successful_connection is not None
            assert "method" in successful_connection
            assert "duration" in successful_connection
            assert successful_connection["duration"] > 0

            print(f"Connection method: {successful_connection['method']}")
            print(f"Connection duration: {successful_connection['duration']:.3f}s")

        finally:
            connector.close()

    def test_context_manager_usage(self, real_databricks_config):
        """Test using the connector as a context manager."""
        with DatabricksConnector(real_databricks_config) as connector:
            assert connector.connection is not None

            result = connector.execute_query("SELECT 1 AS test_value")
            df = result.data

            assert not df.empty
            assert df.iloc[0]["test_value"] == 1

        # Connection should be closed after exiting context
        assert connector.connection is None

    @pytest.mark.slow
    def test_native_connection_method(self, real_databricks_config):
        """Test the native Databricks SQL connector method directly."""
        print("\n--- Testing native (Databricks SQL Connector) connection method ---")

        try:
            test_connector = DatabricksConnector(real_databricks_config)
            connection = test_connector._connect_native()

            test_connector.connection = connection
            test_connector.connection_method_used = ConnectionMethod.NATIVE

            result = test_connector.execute_query(
                "SELECT current_version() AS dbr_version"
            )
            df = result.data

            assert not df.empty
            print(f"Native connection successful")
            print(f"DBR version: {df.iloc[0, 0]}")

            test_connector.close()

        except Exception as e:
            pytest.fail(f"Native connection method failed: {e}")

    @pytest.mark.slow
    def test_auth_token_resolution(self, real_databricks_config):
        """Test that the token is resolved correctly from config."""
        connector = DatabricksConnector(real_databricks_config)

        # Should resolve from password field (set in fixture)
        token = connector._get_auth_token()
        assert token is not None
        assert len(token) > 0
        print(f"Token resolved successfully (length: {len(token)})")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "--tb=short"])
