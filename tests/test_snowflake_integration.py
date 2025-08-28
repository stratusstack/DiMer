"""Integration tests for Snowflake connector with real database connection."""

import os

import pandas as pd
import pytest
from dotenv import load_dotenv

from diffforge.connectors.snowflake.connector import SnowflakeConnector
from diffforge.core.factory import ConnectorFactory
from diffforge.core.models import ConnectionConfig, ConnectionMethod

# Load environment variables
load_dotenv()

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def real_snowflake_config():
    """Real Snowflake configuration from environment variables."""
    config = ConnectionConfig(
        host=os.getenv("SNOWFLAKE_ACCOUNT"),
        username=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE", "TESTDB"),
        schema_name=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        extra_params={
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            "authenticator": os.getenv("SNOWFLAKE_AUTHENTICATOR", "snowflake"),
        },
    )

    # Skip tests if credentials are not provided
    if not all([config.host, config.username, config.password]):
        pytest.skip("Snowflake credentials not provided in environment variables")

    return config


@pytest.mark.requires_db
class TestSnowflakeIntegration:
    """Integration tests for real Snowflake connections."""

    def test_connection_methods_priority(self, real_snowflake_config):
        """Test that connection methods are tried in the right order."""
        connector = SnowflakeConnector(real_snowflake_config)
        methods = connector.get_connection_methods()

        # Verify expected order (Snowpark removed since not available)
        assert methods[0] == ConnectionMethod.ARROW
        assert methods[1] == ConnectionMethod.NATIVE
        assert ConnectionMethod.SQLALCHEMY in methods

    def test_real_connection_establishment(self, real_snowflake_config):
        """Test establishing a real connection to Snowflake."""
        connector = SnowflakeConnector(real_snowflake_config)

        try:
            # This will try all connection methods until one succeeds
            connection = connector.connect()
            assert connection is not None
            assert connector.connection_method_used is not None

            print(
                f"Successfully connected using: {connector.connection_method_used.value}"
            )

            # Test the connection works
            assert connector.test_connection() is True

        finally:
            connector.close()

    def test_factory_creates_snowflake_connector(self, real_snowflake_config):
        """Test that the factory creates a Snowflake connector correctly."""
        # Make sure Snowflake connector is registered
        from diffforge.core.factory import _auto_register_connectors

        _auto_register_connectors()

        connector = ConnectorFactory.create_connector(
            "snowflake", real_snowflake_config
        )

        assert isinstance(connector, SnowflakeConnector)
        assert connector.connection_config == real_snowflake_config

    def test_list_schemas(self, real_snowflake_config):
        """Test listing schemas in the database."""
        connector = SnowflakeConnector(real_snowflake_config)

        try:
            connector.connect()
            schemas = connector.list_schemas()

            assert isinstance(schemas, list)
            assert len(schemas) > 0
            assert "PUBLIC" in schemas
            print(f"Found schemas: {schemas}")

        finally:
            connector.close()

    def test_list_tables_in_public_schema(self, real_snowflake_config):
        """Test listing tables in the PUBLIC schema."""
        connector = SnowflakeConnector(real_snowflake_config)

        try:
            connector.connect()
            tables = connector.list_tables(schema_name="PUBLIC")

            assert isinstance(tables, list)
            print(f"Tables in PUBLIC schema: {tables}")

            # Check if our target table exists
            if "TBL" in tables:
                print("✅ Target table 'TBL' found in PUBLIC schema")
            else:
                print(f"⚠️  Target table 'TBL' not found. Available tables: {tables}")

        finally:
            connector.close()

    def test_tbl_table_exists(self, real_snowflake_config):
        """Test that the 'TBL' table exists and we can access it."""
        connector = SnowflakeConnector(real_snowflake_config)

        try:
            connector.connect()

            # Try to get metadata for the TBL table
            try:
                metadata = connector.get_table_metadata("TBL", schema_name="PUBLIC")

                print(f"✅ Table TBL metadata retrieved successfully")
                print(f"Columns: {len(metadata.columns)}")
                print(f"Row count: {metadata.row_count}")

                for col in metadata.columns:
                    print(f"  - {col.name}: {col.data_type} (nullable: {col.nullable})")

                assert len(metadata.columns) > 0

            except Exception as e:
                print(f"❌ Could not get metadata for table TBL: {e}")
                raise

        finally:
            connector.close()

    def test_sample_data_from_tbl(self, real_snowflake_config):
        """Test retrieving sample data from the TBL table."""
        connector = SnowflakeConnector(real_snowflake_config)

        try:
            connector.connect()

            # Get sample data from TBL table
            try:
                sample_df = connector.get_sample_data(
                    "TBL", limit=5, schema_name="PUBLIC"
                )

                print(f"✅ Retrieved {len(sample_df)} rows from TBL table")
                print(f"Columns: {list(sample_df.columns)}")
                print("Sample data:")
                print(sample_df.head())

                assert isinstance(sample_df, pd.DataFrame)

                # The table might be empty, which is valid
                if not sample_df.empty:
                    assert len(sample_df) <= 5
                    print("✅ Sample data contains rows")
                else:
                    print("ℹ️  Table is empty - no sample data available")

            except Exception as e:
                print(f"❌ Could not retrieve sample data from TBL: {e}")
                raise

        finally:
            connector.close()

    def test_query_execution_on_tbl(self, real_snowflake_config):
        """Test executing a query on the TBL table."""
        connector = SnowflakeConnector(real_snowflake_config)

        try:
            connector.connect()

            # Execute a simple count query
            try:
                result = connector.execute_query(
                    'SELECT COUNT(*) as total_rows FROM "PUBLIC"."TBL"'
                )

                print(f"✅ Query executed successfully")
                print(f"Query execution time: {result.execution_time:.3f} seconds")
                print(
                    f"Connection method used: {result.metadata.get('connection_method')}"
                )

                df = result.data
                assert isinstance(df, pd.DataFrame)
                assert not df.empty
                assert "TOTAL_ROWS" in df.columns

                total_rows = df.iloc[0]["TOTAL_ROWS"]
                print(f"Total rows in TBL table: {total_rows}")

                assert total_rows >= 0

            except Exception as e:
                print(f"❌ Could not execute query on TBL table: {e}")
                raise

        finally:
            connector.close()

    def test_connection_metrics(self, real_snowflake_config):
        """Test that connection metrics are collected properly."""
        connector = SnowflakeConnector(real_snowflake_config)

        try:
            connector.connect()

            # Execute a simple query to generate metrics
            connector.execute_query("SELECT CURRENT_VERSION()")

            metrics = connector.get_connection_metrics()

            print(f"✅ Connection metrics collected: {len(metrics)} entries")

            assert len(metrics) > 0

            # Check the successful connection metric
            successful_connection = next(
                (m for m in metrics if m.get("success") is True), None
            )

            assert successful_connection is not None
            assert "method" in successful_connection
            assert "duration" in successful_connection
            assert successful_connection["duration"] > 0

            print(f"Successful connection method: {successful_connection['method']}")
            print(
                f"Connection duration: {successful_connection['duration']:.3f} seconds"
            )

        finally:
            connector.close()

    def test_context_manager_usage(self, real_snowflake_config):
        """Test using the connector as a context manager."""
        with SnowflakeConnector(real_snowflake_config) as connector:
            # Connection should be automatically established
            assert connector.connection is not None

            # Test a simple query
            result = connector.execute_query("SELECT 1 as test_value")
            df = result.data

            assert not df.empty
            assert df.iloc[0]["TEST_VALUE"] == 1

        # Connection should be automatically closed after exiting context
        assert connector.connection is None

    @pytest.mark.slow
    def test_different_connection_methods(self, real_snowflake_config):
        """Test different connection methods individually."""
        connector = SnowflakeConnector(real_snowflake_config)

        methods_to_test = [
            ConnectionMethod.ARROW,
            ConnectionMethod.NATIVE,
        ]

        successful_methods = []

        for method in methods_to_test:
            print(f"\n--- Testing {method.value} connection method ---")
            try:
                # Create a fresh connector instance
                test_connector = SnowflakeConnector(real_snowflake_config)

                # Try to connect using the specific method
                if method == ConnectionMethod.ARROW:
                    connection = test_connector._connect_arrow()
                elif method == ConnectionMethod.NATIVE:
                    connection = test_connector._connect_native()
                else:
                    continue

                test_connector.connection = connection
                test_connector.connection_method_used = method

                # Test the connection works
                result = test_connector.execute_query(
                    "SELECT CURRENT_VERSION() as version"
                )
                df = result.data

                assert not df.empty
                print(f"✅ {method.value} connection successful")
                print(f"Snowflake version: {df.iloc[0]['VERSION']}")

                successful_methods.append(method.value)

                test_connector.close()

            except Exception as e:
                print(f"❌ {method.value} connection failed: {e}")
                # Don't fail the test - just record that this method didn't work
                continue

        print(f"\nSuccessful connection methods: {successful_methods}")

        # At least one method should work
        assert len(successful_methods) > 0, "No connection methods were successful"


if __name__ == "__main__":
    # Run integration tests
    pytest.main([__file__, "-v", "-s", "--tb=short"])
