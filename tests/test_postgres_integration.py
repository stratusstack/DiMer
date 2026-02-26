"""Integration tests for PostgreSQL connector with real database connection."""

import os

import pandas as pd
import pytest
from dotenv import load_dotenv

from dimer.connectors.postgresql.connector import PostgreSQLConnector
from dimer.core.factory import ConnectorFactory
from dimer.core.models import ConnectionConfig, ConnectionMethod

# Load environment variables
load_dotenv()

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def real_postgres_config():
    """Real PostgreSQL configuration from environment variables."""
    config = ConnectionConfig(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DATABASE", "postgres"),
        schema_name=os.getenv("POSTGRES_SCHEMA", "public"),
        extra_params={
            "ssl_mode": os.getenv("POSTGRES_SSL_MODE", "prefer"),
        },
    )

    # Skip tests if credentials are not provided
    if not all([config.username, config.password]):
        pytest.skip("PostgreSQL credentials not provided in environment variables")

    return config


# Target table used for table-level tests; can be overridden via env
TARGET_TABLE = os.getenv("POSTGRES_TABLE", "")


@pytest.mark.requires_db
class TestPostgreSQLIntegration:
    """Integration tests for real PostgreSQL connections."""

    def test_connection_methods_priority(self, real_postgres_config):
        """Test that connection methods are tried in the right order."""
        connector = PostgreSQLConnector(real_postgres_config)
        methods = connector.get_connection_methods()

        assert methods[0] == ConnectionMethod.ASYNCPG
        assert methods[1] == ConnectionMethod.PSYCOPG2
        assert ConnectionMethod.SQLALCHEMY in methods

    def test_real_connection_establishment(self, real_postgres_config):
        """Test establishing a real connection to PostgreSQL."""
        connector = PostgreSQLConnector(real_postgres_config)

        try:
            connection = connector.connect()
            assert connection is not None
            assert connector.connection_method_used is not None

            print(
                f"Successfully connected using: {connector.connection_method_used.value}"
            )

            assert connector.test_connection() is True

        finally:
            connector.close()

    def test_factory_creates_postgresql_connector(self, real_postgres_config):
        """Test that the factory creates a PostgreSQL connector correctly."""
        from dimer.core.factory import _auto_register_connectors

        _auto_register_connectors()

        connector = ConnectorFactory.create_connector(
            "postgresql", real_postgres_config
        )

        assert isinstance(connector, PostgreSQLConnector)
        assert connector.connection_config == real_postgres_config

    def test_list_schemas(self, real_postgres_config):
        """Test listing schemas in the database."""
        connector = PostgreSQLConnector(real_postgres_config)

        try:
            connector.connect()
            schemas = connector.list_schemas()

            assert isinstance(schemas, list)
            assert len(schemas) > 0
            assert "public" in schemas
            print(f"Found schemas: {schemas}")

        finally:
            connector.close()

    def test_list_tables_in_public_schema(self, real_postgres_config):
        """Test listing tables in the public schema."""
        connector = PostgreSQLConnector(real_postgres_config)

        try:
            connector.connect()
            tables = connector.list_tables(schema_name="public")

            assert isinstance(tables, list)
            print(f"Tables in public schema: {tables}")

            if TARGET_TABLE and TARGET_TABLE in tables:
                print(f"Target table '{TARGET_TABLE}' found in public schema")
            elif TARGET_TABLE:
                print(
                    f"Target table '{TARGET_TABLE}' not found. "
                    f"Available tables: {tables}"
                )

        finally:
            connector.close()

    def test_target_table_metadata(self, real_postgres_config):
        """Test retrieving metadata for the target table."""
        if not TARGET_TABLE:
            pytest.skip("POSTGRES_TABLE env var not set; skipping table metadata test")

        connector = PostgreSQLConnector(real_postgres_config)

        try:
            connector.connect()

            schema = real_postgres_config.schema_name or "public"
            tables = connector.list_tables(schema_name=schema)

            if TARGET_TABLE not in tables:
                pytest.skip(
                    f"Table '{TARGET_TABLE}' not found in schema '{schema}'"
                )

            metadata = connector.get_table_metadata(
                TARGET_TABLE, schema_name=schema
            )

            print(f"Table '{TARGET_TABLE}' metadata retrieved successfully")
            print(f"Columns: {len(metadata.columns)}")
            print(f"Row count: {metadata.row_count}")

            for col in metadata.columns:
                print(
                    f"  - {col.name}: {col.data_type} "
                    f"(nullable: {col.nullable}, pk: {col.is_primary_key})"
                )

            assert len(metadata.columns) > 0

        finally:
            connector.close()

    def test_sample_data_from_target_table(self, real_postgres_config):
        """Test retrieving sample data from the target table."""
        if not TARGET_TABLE:
            pytest.skip("POSTGRES_TABLE env var not set; skipping sample data test")

        connector = PostgreSQLConnector(real_postgres_config)

        try:
            connector.connect()

            schema = real_postgres_config.schema_name or "public"
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

    def test_query_execution(self, real_postgres_config):
        """Test executing a basic query on the PostgreSQL server."""
        connector = PostgreSQLConnector(real_postgres_config)

        try:
            connector.connect()

            result = connector.execute_query(
                "SELECT version() AS pg_version, current_user AS current_user, "
                "current_database() AS current_db"
            )

            print(f"Query executed successfully")
            print(f"Execution time: {result.execution_time:.3f}s")
            print(f"Connection method: {result.metadata.get('connection_method')}")

            df = result.data
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            assert "pg_version" in df.columns or "version" in str(df.columns).lower()

            print(f"Server version: {df.iloc[0, 0]}")

        finally:
            connector.close()

    def test_query_execution_on_target_table(self, real_postgres_config):
        """Test executing a COUNT query on the target table."""
        if not TARGET_TABLE:
            pytest.skip("POSTGRES_TABLE env var not set; skipping table query test")

        connector = PostgreSQLConnector(real_postgres_config)

        try:
            connector.connect()

            schema = real_postgres_config.schema_name or "public"
            tables = connector.list_tables(schema_name=schema)

            if TARGET_TABLE not in tables:
                pytest.skip(
                    f"Table '{TARGET_TABLE}' not found in schema '{schema}'"
                )

            result = connector.execute_query(
                f'SELECT COUNT(*) AS total_rows FROM "{schema}"."{TARGET_TABLE}"'
            )

            df = result.data
            assert isinstance(df, pd.DataFrame)
            assert not df.empty

            total_rows = df.iloc[0]["total_rows"]
            print(f"Total rows in '{TARGET_TABLE}': {total_rows}")
            assert total_rows >= 0

        finally:
            connector.close()

    def test_connection_metrics(self, real_postgres_config):
        """Test that connection metrics are collected properly."""
        connector = PostgreSQLConnector(real_postgres_config)

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

    def test_context_manager_usage(self, real_postgres_config):
        """Test using the connector as a context manager."""
        with PostgreSQLConnector(real_postgres_config) as connector:
            assert connector.connection is not None

            result = connector.execute_query("SELECT 1 AS test_value")
            df = result.data

            assert not df.empty
            assert df.iloc[0]["test_value"] == 1

        # Connection should be closed after exiting context
        assert connector.connection is None

    @pytest.mark.slow
    def test_different_connection_methods(self, real_postgres_config):
        """Test each connection method individually."""
        methods_to_test = [
            (ConnectionMethod.PSYCOPG2, "_connect_psycopg2"),
            (ConnectionMethod.SQLALCHEMY, "_connect_sqlalchemy"),
        ]

        successful_methods = []

        for method, connect_fn in methods_to_test:
            print(f"\n--- Testing {method.value} connection method ---")
            try:
                test_connector = PostgreSQLConnector(real_postgres_config)
                connection = getattr(test_connector, connect_fn)()

                test_connector.connection = connection
                test_connector.connection_method_used = method

                result = test_connector.execute_query(
                    "SELECT version() AS pg_version"
                )
                df = result.data

                assert not df.empty
                print(f"{method.value} connection successful")
                print(f"Version: {df.iloc[0, 0]}")

                successful_methods.append(method.value)
                test_connector.close()

            except Exception as e:
                print(f"{method.value} connection failed: {e}")
                continue

        print(f"\nSuccessful connection methods: {successful_methods}")
        assert len(successful_methods) > 0, "No connection methods were successful"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "--tb=short"])
