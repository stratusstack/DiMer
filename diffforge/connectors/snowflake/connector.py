"""Snowflake connector with multiple connection strategies."""

import time
from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from diffforge.core.base import DataSourceConnector
from diffforge.core.models import (
    ColumnMetadata,
    ConnectionConfig,
    ConnectionMethod,
    TableMetadata,
)
from diffforge.core.types import DataTypeMapper

logger = structlog.get_logger(__name__)


class SnowflakeConnector(DataSourceConnector):
    """Snowflake-specific implementation with multiple connection strategies."""

    # Snowflake-specific configuration
    DEFAULT_WAREHOUSE = "COMPUTE_WH"
    DEFAULT_ROLE = "PUBLIC"
    MAX_SAMPLE_ROWS = 100000

    def get_required_params(self) -> List[str]:
        """Return list of required connection parameters for Snowflake."""
        return ["host", "username", "password", "database"]

    def get_connection_methods(self) -> List[ConnectionMethod]:
        """
        Return methods in order of preference (fastest/most efficient first).

        Returns:
            Ordered list of connection methods to try
        """
        return [
            ConnectionMethod.ARROW,  # Fastest for large datasets
            ConnectionMethod.SNOWPARK,  # DataFrame operations
            ConnectionMethod.NATIVE,  # Standard snowflake-connector
            ConnectionMethod.SQLALCHEMY,  # ORM approach
        ]

    def _connect_arrow(self) -> Any:
        """
        Connect using Arrow optimization for best performance.

        Returns:
            Snowflake connection with Arrow support

        Raises:
            ImportError: If required packages are not installed
            Exception: If connection fails
        """
        try:
            import snowflake.connector
        except ImportError:
            raise ImportError(
                "snowflake-connector-python is required for Arrow connection"
            )

        connection_params = {
            "account": self._extract_account_from_host(),
            "user": self.connection_config.username,
            "password": self.connection_config.password,
            "database": self.connection_config.database,
            "warehouse": self.connection_config.extra_params.get(
                "warehouse", self.DEFAULT_WAREHOUSE
            ),
            "role": self.connection_config.extra_params.get("role", self.DEFAULT_ROLE),
            "schema": self.connection_config.schema_name or "PUBLIC",
            "session_parameters": {
                "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "arrow",
                "QUERY_TAG": "diffforge-arrow-connector",
            },
        }

        # Add optional parameters
        if self.connection_config.extra_params.get("region"):
            connection_params["region"] = self.connection_config.extra_params["region"]

        if self.connection_config.extra_params.get("authenticator"):
            connection_params["authenticator"] = self.connection_config.extra_params[
                "authenticator"
            ]

        logger.info("Attempting Arrow connection to Snowflake")
        connection = snowflake.connector.connect(**connection_params)

        # Test the connection
        cursor = connection.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        cursor.close()

        logger.info("Arrow connection established", snowflake_version=version)
        return connection

    def _connect_snowpark(self) -> Any:
        """
        Connect using Snowpark for DataFrame operations.

        Returns:
            Snowpark Session object

        Raises:
            ImportError: If Snowpark is not installed
            Exception: If connection fails
        """
        try:
            from snowflake.snowpark import Session
        except ImportError:
            raise ImportError("snowpark-python is required for Snowpark connection")

        connection_params = {
            "account": self._extract_account_from_host(),
            "user": self.connection_config.username,
            "password": self.connection_config.password,
            "database": self.connection_config.database,
            "warehouse": self.connection_config.extra_params.get(
                "warehouse", self.DEFAULT_WAREHOUSE
            ),
            "role": self.connection_config.extra_params.get("role", self.DEFAULT_ROLE),
            "schema": self.connection_config.schema_name or "PUBLIC",
        }

        # Add optional parameters
        if self.connection_config.extra_params.get("region"):
            connection_params["region"] = self.connection_config.extra_params["region"]

        if self.connection_config.extra_params.get("authenticator"):
            connection_params["authenticator"] = self.connection_config.extra_params[
                "authenticator"
            ]

        logger.info("Attempting Snowpark connection to Snowflake")
        session = Session.builder.configs(connection_params).create()

        # Test the session
        result = session.sql("SELECT CURRENT_VERSION()").collect()
        version = result[0][0]

        logger.info("Snowpark connection established", snowflake_version=version)
        return session

    def _connect_native(self) -> Any:
        """
        Connect using native snowflake-connector-python.

        Returns:
            Snowflake connection object

        Raises:
            ImportError: If connector is not installed
            Exception: If connection fails
        """
        try:
            import snowflake.connector
        except ImportError:
            raise ImportError(
                "snowflake-connector-python is required for native connection"
            )

        connection_params = {
            "account": self._extract_account_from_host(),
            "user": self.connection_config.username,
            "password": self.connection_config.password,
            "database": self.connection_config.database,
            "warehouse": self.connection_config.extra_params.get(
                "warehouse", self.DEFAULT_WAREHOUSE
            ),
            "role": self.connection_config.extra_params.get("role", self.DEFAULT_ROLE),
            "schema": self.connection_config.schema_name or "PUBLIC",
            "session_parameters": {
                "QUERY_TAG": "diffforge-native-connector",
            },
        }

        # Add optional parameters
        if self.connection_config.extra_params.get("region"):
            connection_params["region"] = self.connection_config.extra_params["region"]

        if self.connection_config.extra_params.get("authenticator"):
            connection_params["authenticator"] = self.connection_config.extra_params[
                "authenticator"
            ]

        logger.info("Attempting native connection to Snowflake")
        connection = snowflake.connector.connect(**connection_params)

        # Test the connection
        cursor = connection.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        cursor.close()

        logger.info("Native connection established", snowflake_version=version)
        return connection

    def _connect_sqlalchemy(self) -> Any:
        """
        Connect using SQLAlchemy ORM approach.

        Returns:
            SQLAlchemy engine

        Raises:
            ImportError: If SQLAlchemy or Snowflake dialect is not installed
            Exception: If connection fails
        """
        try:
            from sqlalchemy import create_engine
        except ImportError:
            raise ImportError("sqlalchemy is required for SQLAlchemy connection")

        # Build connection URL
        account = self._extract_account_from_host()
        warehouse = self.connection_config.extra_params.get(
            "warehouse", self.DEFAULT_WAREHOUSE
        )
        role = self.connection_config.extra_params.get("role", self.DEFAULT_ROLE)
        schema = self.connection_config.schema_name or "PUBLIC"

        url = (
            f"snowflake://{self.connection_config.username}:{self.connection_config.password}@"
            f"{account}/{self.connection_config.database}/{schema}?"
            f"warehouse={warehouse}&role={role}"
        )

        # Add optional parameters
        if self.connection_config.extra_params.get("region"):
            url += f"&region={self.connection_config.extra_params['region']}"

        if self.connection_config.extra_params.get("authenticator"):
            url += (
                f"&authenticator={self.connection_config.extra_params['authenticator']}"
            )

        logger.info("Attempting SQLAlchemy connection to Snowflake")
        engine = create_engine(
            url,
            pool_size=self.connection_config.pool_size,
            max_overflow=self.connection_config.max_overflow,
            pool_timeout=self.connection_config.pool_timeout,
            pool_recycle=self.connection_config.pool_recycle,
        )

        # Test the connection
        with engine.connect() as conn:
            result = conn.execute("SELECT CURRENT_VERSION()").fetchone()
            version = result[0]

        logger.info("SQLAlchemy connection established", snowflake_version=version)
        return engine

    def _connect_jdbc(self) -> Any:
        """
        Connect using JDBC driver (requires JVM).

        Returns:
            JDBC connection

        Raises:
            ImportError: If JDBC libraries are not available
            Exception: If connection fails
        """
        try:
            import jaydebeapi
        except ImportError:
            raise ImportError("jaydebeapi is required for JDBC connection")

        # JDBC URL for Snowflake
        account = self._extract_account_from_host()
        warehouse = self.connection_config.extra_params.get(
            "warehouse", self.DEFAULT_WAREHOUSE
        )
        role = self.connection_config.extra_params.get("role", self.DEFAULT_ROLE)
        schema = self.connection_config.schema_name or "PUBLIC"

        jdbc_url = (
            f"jdbc:snowflake://{account}.snowflakecomputing.com/?"
            f"db={self.connection_config.database}&schema={schema}&"
            f"warehouse={warehouse}&role={role}"
        )

        # Connection properties
        props = {
            "user": self.connection_config.username,
            "password": self.connection_config.password,
        }

        logger.info("Attempting JDBC connection to Snowflake")

        # Note: This would require the Snowflake JDBC driver JAR file
        # In a real implementation, you'd need to specify the path to the driver
        driver_path = self.connection_config.extra_params.get("jdbc_driver_path")
        if not driver_path:
            raise ValueError(
                "JDBC driver path must be specified in extra_params['jdbc_driver_path']"
            )

        connection = jaydebeapi.connect(
            "net.snowflake.client.jdbc.SnowflakeDriver", jdbc_url, props, driver_path
        )

        logger.info("JDBC connection established")
        return connection

    def _connect_odbc(self) -> Any:
        """
        Connect using ODBC driver fallback.

        Returns:
            ODBC connection

        Raises:
            ImportError: If ODBC libraries are not available
            Exception: If connection fails
        """
        try:
            import pyodbc
        except ImportError:
            raise ImportError("pyodbc is required for ODBC connection")

        account = self._extract_account_from_host()
        warehouse = self.connection_config.extra_params.get(
            "warehouse", self.DEFAULT_WAREHOUSE
        )
        role = self.connection_config.extra_params.get("role", self.DEFAULT_ROLE)
        schema = self.connection_config.schema_name or "PUBLIC"

        connection_string = (
            f"DRIVER={{SnowflakeDSIIDriver}};"
            f"SERVER={account}.snowflakecomputing.com;"
            f"UID={self.connection_config.username};"
            f"PWD={self.connection_config.password};"
            f"DATABASE={self.connection_config.database};"
            f"SCHEMA={schema};"
            f"WAREHOUSE={warehouse};"
            f"ROLE={role};"
        )

        logger.info("Attempting ODBC connection to Snowflake")
        connection = pyodbc.connect(connection_string)

        logger.info("ODBC connection established")
        return connection

    def _extract_account_from_host(self) -> str:
        """
        Extract Snowflake account identifier from host.

        Returns:
            Account identifier
            
        Raises:
            ValueError: If host is invalid or account cannot be extracted
        """
        if not self.connection_config.host:
            raise ValueError("Host is required for Snowflake connection")

        host = self.connection_config.host.strip()
        
        if not host:
            raise ValueError("Host cannot be empty for Snowflake connection")

        # Remove protocol if present
        if "://" in host:
            host = host.split("://", 1)[1]

        # Remove port if present
        if ":" in host:
            host = host.split(":", 1)[0]

        # Extract account from snowflakecomputing.com domain
        if ".snowflakecomputing.com" in host:
            account = host.replace(".snowflakecomputing.com", "")
            if not account:
                raise ValueError("Invalid Snowflake account: host contains only domain")
            return account

        # Validate if host looks like a valid account identifier
        if not host or "." in host or " " in host:
            raise ValueError(f"Invalid Snowflake account identifier: {host}")

        # Assume host is the account identifier
        return host

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
        if self.connection_method_used == ConnectionMethod.SNOWPARK:
            # Snowpark session
            result = self.connection.sql(query)
            return result.to_pandas()

        elif self.connection_method_used == ConnectionMethod.SQLALCHEMY:
            # SQLAlchemy engine
            return pd.read_sql(query, self.connection, params=params)

        else:
            # Native, Arrow, JDBC, ODBC connections
            cursor = self.connection.cursor()
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Fetch results
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()

                return pd.DataFrame(data, columns=columns)
            finally:
                cursor.close()

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """
        Snowflake-specific metadata extraction using INFORMATION_SCHEMA.

        Args:
            table_name: Name of the table
            schema_name: Optional schema name

        Returns:
            TableMetadata object with comprehensive table information
        """
        schema = schema_name or self.connection_config.schema_name or "PUBLIC"

        # Query for column metadata
        columns_query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """

        columns_df = self._execute_query_internal(
            columns_query, [schema.upper(), table_name.upper()]
        )

        # Build column metadata
        columns = []
        for _, row in columns_df.iterrows():
            # Map Snowflake types to common types
            common_type = DataTypeMapper.map_type("snowflake", row["DATA_TYPE"])

            column = ColumnMetadata(
                name=row["COLUMN_NAME"],
                data_type=common_type,
                nullable=row["IS_NULLABLE"] == "YES",
                max_length=row["CHARACTER_MAXIMUM_LENGTH"],
                precision=row["NUMERIC_PRECISION"],
                scale=row["NUMERIC_SCALE"],
                default_value=row["COLUMN_DEFAULT"],
                description=row["COMMENT"],
            )
            columns.append(column)

        # Query for table statistics
        stats_query = """
        SELECT 
            ROW_COUNT,
            BYTES,
            LAST_ALTERED
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """

        stats_df = self._execute_query_internal(
            stats_query, [schema.upper(), table_name.upper()]
        )

        # Extract statistics
        row_count = None
        size_bytes = None
        last_modified = None

        if not stats_df.empty:
            row_count = stats_df.iloc[0]["ROW_COUNT"]
            size_bytes = stats_df.iloc[0]["BYTES"]
            last_modified = stats_df.iloc[0]["LAST_ALTERED"]

        return TableMetadata(
            name=table_name,
            schema=schema,
            columns=columns,
            row_count=row_count,
            size_bytes=size_bytes,
            last_modified=last_modified,
            statistics={
                "schema": schema,
                "table": table_name,
            },
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Efficiently sample data using Snowflake's TABLESAMPLE or LIMIT.

        Args:
            table_name: Name of the table
            limit: Maximum number of rows to return
            schema_name: Optional schema name

        Returns:
            DataFrame containing sample data
        """
        schema = schema_name or self.connection_config.schema_name or "PUBLIC"
        full_table_name = f'"{schema}"."{table_name}"'

        # Use TABLESAMPLE for larger samples, LIMIT for smaller ones
        if limit > 1000:
            # Use TABLESAMPLE for better performance on large tables
            sample_percent = min(10, max(0.1, (limit / self.MAX_SAMPLE_ROWS) * 100))
            query = f"""
            SELECT * FROM {full_table_name} 
            TABLESAMPLE ({sample_percent}) 
            LIMIT {limit}
            """
        else:
            # Use simple LIMIT for small samples
            query = f"SELECT * FROM {full_table_name} LIMIT {limit}"

        return self._execute_query_internal(query)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        """
        List all tables in the database or schema.

        Args:
            schema_name: Optional schema name to filter tables

        Returns:
            List of table names
        """
        if schema_name:
            query = """
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """
            df = self._execute_query_internal(query, [schema_name.upper()])
        else:
            query = """
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """
            df = self._execute_query_internal(query)

        return df["TABLE_NAME"].tolist()

    def _list_schemas_internal(self) -> List[str]:
        """
        List all schemas in the database.

        Returns:
            List of schema names
        """
        query = """
        SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA 
        ORDER BY SCHEMA_NAME
        """
        df = self._execute_query_internal(query)
        return df["SCHEMA_NAME"].tolist()

    def _get_test_query(self) -> str:
        """
        Return a simple query to test the connection.

        Returns:
            A simple SQL query string
        """
        return "SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_DATABASE()"
