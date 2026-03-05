"""Databricks connector with multiple connection strategies."""

from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from dimer.core.base import DataSourceConnector
from dimer.core.models import (
    ColumnMetadata,
    ConnectionConfig,
    ConnectionMethod,
    TableMetadata,
)
from dimer.core.types import DataTypeMapper

logger = structlog.get_logger(__name__)


class DatabricksConnector(DataSourceConnector):
    """Databricks-specific implementation with multiple connection strategies."""

    # Databricks-specific configuration
    DEFAULT_CATALOG = "main"
    DEFAULT_SCHEMA = "default"
    MAX_SAMPLE_ROWS = 1000000
    DIALECTS = {
        "hash": "MD5(CAST({COL} AS STRING))",
        "concatenation": "||",
        "cast_to_text": "CAST({COL} AS STRING)",
    }

    def get_required_params(self) -> List[str]:
        """Return list of required connection parameters for Databricks."""
        return ["host"]  # host is the Databricks workspace URL

    def get_connection_methods(self) -> List[ConnectionMethod]:
        """
        Return methods in order of preference (fastest/most efficient first).

        Returns:
            Ordered list of connection methods to try
        """
        return [
            ConnectionMethod.DATABRICKS_CONNECT,  # Fastest for DataFrame operations
            ConnectionMethod.NATIVE,  # Databricks SQL Connector
            ConnectionMethod.SQLALCHEMY,  # ORM approach
        ]

    def _connect_databricks_connect(self) -> Any:
        """
        Connect using Databricks Connect for high-performance DataFrame operations.

        Returns:
            Databricks Connect session

        Raises:
            ImportError: If databricks-connect is not installed
            Exception: If connection fails
        """
        try:
            from databricks.connect import DatabricksSession
            from pyspark.sql import SparkSession
        except ImportError:
            raise ImportError(
                "databricks-connect is required for Databricks Connect connection"
            )

        # Extract connection parameters
        host = self.connection_config.host
        if not host.startswith(("http://", "https://")):
            host = f"https://{host}"

        # Get authentication token
        token = self._get_auth_token()

        # Get cluster/warehouse ID
        cluster_id = self.connection_config.extra_params.get("cluster_id")
        warehouse_id = self.connection_config.extra_params.get("warehouse_id")

        if not cluster_id and not warehouse_id:
            raise ValueError(
                "Either cluster_id or warehouse_id must be specified in extra_params"
            )

        logger.info("Attempting Databricks Connect connection")

        # Create Databricks session
        builder = DatabricksSession.builder

        if cluster_id:
            builder = builder.remote(host=host, token=token, cluster_id=cluster_id)
        else:
            builder = builder.serverless(
                host=host, token=token, warehouse_id=warehouse_id
            )

        # Set catalog and schema if provided
        catalog = self.connection_config.extra_params.get(
            "catalog", self.DEFAULT_CATALOG
        )
        schema = self.connection_config.schema_name or self.DEFAULT_SCHEMA

        session = builder.getOrCreate()

        # Set default catalog and schema
        session.sql(f"USE CATALOG {catalog}")
        session.sql(f"USE SCHEMA {schema}")

        # Test the connection
        result = session.sql("SELECT current_version()").collect()
        version = result[0][0] if result else "Unknown"

        logger.info("Databricks Connect connection established", version=version)
        return session

    def _connect_native(self) -> Any:
        """
        Connect using Databricks SQL Connector.

        Returns:
            Databricks SQL connection

        Raises:
            ImportError: If databricks-sql-connector is not installed
            Exception: If connection fails
        """
        try:
            from databricks import sql
        except ImportError:
            raise ImportError(
                "databricks-sql-connector is required for native Databricks connection"
            )

        # Extract connection parameters
        host = self.connection_config.host
        if host.startswith(("http://", "https://")):
            host = host.replace("https://", "").replace("http://", "")

        # Get authentication token
        token = self._get_auth_token()

        # Get HTTP path (warehouse or cluster endpoint)
        http_path = self.connection_config.extra_params.get("http_path")
        if not http_path:
            warehouse_id = self.connection_config.extra_params.get("warehouse_id")
            if warehouse_id:
                http_path = f"/sql/1.0/warehouses/{warehouse_id}"
            else:
                raise ValueError(
                    "Either http_path or warehouse_id must be specified in extra_params"
                )

        logger.info("Attempting native Databricks SQL connection")

        # Create connection
        connection = sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token,
            catalog=self.connection_config.extra_params.get(
                "catalog", self.DEFAULT_CATALOG
            ),
            schema=self.connection_config.schema_name or self.DEFAULT_SCHEMA,
        )

        # Test the connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_version()")
            version = cursor.fetchone()[0]

        logger.info("Native Databricks SQL connection established", version=version)
        return connection

    def _connect_sqlalchemy(self) -> Any:
        """
        Connect using SQLAlchemy with Databricks dialect.

        Returns:
            SQLAlchemy engine

        Raises:
            ImportError: If SQLAlchemy or Databricks dialect is not installed
            Exception: If connection fails
        """
        try:
            from sqlalchemy import create_engine
        except ImportError:
            raise ImportError("sqlalchemy is required for SQLAlchemy connection")

        # Extract connection parameters
        host = self.connection_config.host
        if host.startswith(("http://", "https://")):
            host = host.replace("https://", "").replace("http://", "")

        # Get authentication token
        token = self._get_auth_token()

        # Get HTTP path
        http_path = self.connection_config.extra_params.get("http_path")
        if not http_path:
            warehouse_id = self.connection_config.extra_params.get("warehouse_id")
            if warehouse_id:
                http_path = f"/sql/1.0/warehouses/{warehouse_id}"
            else:
                raise ValueError(
                    "Either http_path or warehouse_id must be specified in extra_params"
                )

        # Build connection URL
        catalog = self.connection_config.extra_params.get(
            "catalog", self.DEFAULT_CATALOG
        )
        schema = self.connection_config.schema_name or self.DEFAULT_SCHEMA

        url = (
            f"databricks+pyhive://token:{token}@{host}:443/"
            f"{catalog}.{schema}?http_path={http_path}"
        )

        logger.info("Attempting SQLAlchemy Databricks connection")

        engine = create_engine(
            url,
            echo=self.connection_config.extra_params.get("echo", False),
        )

        # Test the connection
        with engine.connect() as conn:
            result = conn.execute("SELECT current_version()").fetchone()
            version = result[0]

        logger.info("SQLAlchemy Databricks connection established", version=version)
        return engine

    def _get_auth_token(self) -> str:
        """Get authentication token from various sources."""
        # Priority order for authentication:
        # 1. Explicit token in extra_params
        # 2. Token from connection config password field
        # 3. DATABRICKS_TOKEN environment variable
        # 4. Azure AD token (for Azure Databricks)

        import os

        token = self.connection_config.extra_params.get("access_token")
        if token:
            return token

        if self.connection_config.password:
            return self.connection_config.password

        token = os.getenv("DATABRICKS_TOKEN")
        if token:
            return token

        # Try Azure AD authentication if on Azure
        azure_tenant_id = self.connection_config.extra_params.get("azure_tenant_id")
        azure_client_id = self.connection_config.extra_params.get("azure_client_id")
        azure_client_secret = self.connection_config.extra_params.get(
            "azure_client_secret"
        )

        if azure_tenant_id and azure_client_id and azure_client_secret:
            try:
                token = self._get_azure_ad_token(
                    azure_tenant_id, azure_client_id, azure_client_secret
                )
                return token
            except Exception as e:
                logger.warning("Failed to get Azure AD token", error=str(e))

        raise ValueError(
            "Authentication token required. Provide access_token in extra_params, "
            "set password field, or set DATABRICKS_TOKEN environment variable"
        )

    def _get_azure_ad_token(
        self, tenant_id: str, client_id: str, client_secret: str
    ) -> str:
        """Get Azure AD token for Azure Databricks."""
        try:
            from azure.identity import ClientSecretCredential
        except ImportError:
            raise ImportError("azure-identity is required for Azure AD authentication")

        credential = ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
        )

        # Get token for Databricks scope
        token = credential.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default")
        return token.token

    def _execute_query_internal(
        self, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        Internal method to execute query using the current connection.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            DataFrame containing query results
        """
        if not self.connection:
            raise RuntimeError("No active connection. Call connect() first.")

        # Handle different connection types
        if self.connection_method_used == ConnectionMethod.DATABRICKS_CONNECT:
            # Databricks Connect session (Spark)
            if params:
                # For parameterized queries, we'd need to format them properly
                # This is a simplified approach
                for key, value in params.items():
                    if isinstance(value, str):
                        query = query.replace(f":{key}", f"'{value}'")
                    else:
                        query = query.replace(f":{key}", str(value))

            spark_df = self.connection.sql(query)
            return spark_df.toPandas()

        elif self.connection_method_used == ConnectionMethod.SQLALCHEMY:
            # SQLAlchemy engine
            return pd.read_sql(query, self.connection, params=params)

        else:
            # Native Databricks SQL connection
            with self.connection.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Fetch results
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    data = cursor.fetchall()
                    return pd.DataFrame(data, columns=columns)
                else:
                    return pd.DataFrame()

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """
        Databricks-specific metadata extraction using information_schema.

        Args:
            table_name: Name of the table
            schema_name: Optional schema name

        Returns:
            TableMetadata object with comprehensive table information
        """
        catalog = self.connection_config.extra_params.get(
            "catalog", self.DEFAULT_CATALOG
        )
        schema = (
            schema_name or self.connection_config.schema_name or self.DEFAULT_SCHEMA
        )

        # Query for column metadata
        columns_query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            comment
        FROM {catalog}.information_schema.columns 
        WHERE table_catalog = '{catalog}' 
        AND table_schema = '{schema}' 
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """

        columns_df = self._execute_query_internal(columns_query)

        # Build column metadata
        columns = []
        for _, row in columns_df.iterrows():
            # Map Databricks types to common types
            common_type = DataTypeMapper.map_type("databricks", row["data_type"])

            column = ColumnMetadata(
                name=row["column_name"],
                data_type=common_type,
                nullable=row["is_nullable"] == "YES",
                default_value=row["column_default"],
                description=row["comment"],
            )
            columns.append(column)

        # Query for table metadata
        table_query = f"""
        SELECT 
            table_catalog,
            table_schema,
            table_name,
            table_type,
            comment as table_comment
        FROM {catalog}.information_schema.tables 
        WHERE table_catalog = '{catalog}' 
        AND table_schema = '{schema}' 
        AND table_name = '{table_name}'
        """

        table_df = self._execute_query_internal(table_query)

        # Extract table information
        statistics = {"catalog": catalog, "schema": schema, "table": table_name}

        if not table_df.empty:
            row = table_df.iloc[0]
            statistics.update(
                {
                    "table_type": row["table_type"],
                    "table_comment": row["table_comment"],
                }
            )

        # Try to get row count (may not be available for all table types)
        try:
            count_query = (
                f"SELECT COUNT(*) as row_count FROM {catalog}.{schema}.{table_name}"
            )
            count_df = self._execute_query_internal(count_query)
            row_count = count_df.iloc[0]["row_count"] if not count_df.empty else None
        except Exception as e:
            logger.warning("Failed to get row count", error=str(e))
            row_count = None

        return TableMetadata(
            columns=columns, row_count=row_count, statistics=statistics
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Efficiently sample data using Databricks TABLESAMPLE or LIMIT.

        Args:
            table_name: Name of the table
            limit: Maximum number of rows to return
            schema_name: Optional schema name

        Returns:
            DataFrame containing sample data
        """
        catalog = self.connection_config.extra_params.get(
            "catalog", self.DEFAULT_CATALOG
        )
        schema = (
            schema_name or self.connection_config.schema_name or self.DEFAULT_SCHEMA
        )
        full_table_name = f"{catalog}.{schema}.{table_name}"

        # Use TABLESAMPLE for larger samples, LIMIT for smaller ones
        if limit > 1000:
            # Use TABLESAMPLE with percentage for random sampling
            sample_percent = min(10, max(0.1, (limit / self.MAX_SAMPLE_ROWS) * 100))
            query = f"""
            SELECT * FROM {full_table_name} 
            TABLESAMPLE ({sample_percent} PERCENT)
            LIMIT {limit}
            """
        else:
            # Use simple LIMIT for small samples
            query = f"SELECT * FROM {full_table_name} LIMIT {limit}"

        return self._execute_query_internal(query)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        """
        List all tables in the schema.

        Args:
            schema_name: Optional schema name

        Returns:
            List of table names
        """
        catalog = self.connection_config.extra_params.get(
            "catalog", self.DEFAULT_CATALOG
        )
        schema = (
            schema_name or self.connection_config.schema_name or self.DEFAULT_SCHEMA
        )

        query = f"""
        SELECT table_name 
        FROM {catalog}.information_schema.tables
        WHERE table_catalog = '{catalog}' 
        AND table_schema = '{schema}'
        AND table_type IN ('MANAGED', 'EXTERNAL', 'VIEW')
        ORDER BY table_name
        """

        df = self._execute_query_internal(query)
        return df["table_name"].tolist()

    def _list_schemas_internal(self) -> List[str]:
        """
        List all schemas in the catalog.

        Returns:
            List of schema names
        """
        catalog = self.connection_config.extra_params.get(
            "catalog", self.DEFAULT_CATALOG
        )

        query = f"""
        SELECT schema_name 
        FROM {catalog}.information_schema.schemata
        WHERE catalog_name = '{catalog}'
        ORDER BY schema_name
        """

        df = self._execute_query_internal(query)
        return df["schema_name"].tolist()

    def _get_test_query(self) -> str:
        """
        Return a simple query to test the connection.

        Returns:
            A simple SQL query string
        """
        return "SELECT current_version(), current_user(), current_catalog(), current_schema()"

    def close(self) -> None:
        """Clean up Databricks-specific connection resources."""
        try:
            if self.connection:
                if self.connection_method_used == ConnectionMethod.DATABRICKS_CONNECT:
                    # Stop Databricks Connect session
                    if hasattr(self.connection, "stop"):
                        self.connection.stop()
                elif self.connection_method_used == ConnectionMethod.SQLALCHEMY:
                    # Close SQLAlchemy engine
                    self.connection.dispose()
                else:
                    # Close native Databricks SQL connection
                    if hasattr(self.connection, "close"):
                        self.connection.close()

                self.connection = None
                self.connection_method_used = None

            logger.info("Databricks connection closed successfully")

        except Exception as e:
            logger.error("Error closing Databricks connection", error=str(e))
