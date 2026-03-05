"""MySQL connector with multiple connection strategies."""

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


class MySQLConnector(DataSourceConnector):
    """MySQL-specific implementation with multiple connection strategies."""

    # MySQL-specific configuration
    DEFAULT_PORT = 3306
    DEFAULT_CHARSET = "utf8mb4"
    MAX_SAMPLE_ROWS = 100000
    DIALECTS = {
        "hash": "MD5(CONCAT({COL}))",
        "concatenation": ", ",
        "cast_to_text": "CAST({COL} AS CHAR)",
    }

    def get_required_params(self) -> List[str]:
        """Return list of required connection parameters for MySQL."""
        return ["host", "username", "password", "database"]

    def get_connection_methods(self) -> List[ConnectionMethod]:
        """
        Return methods in order of preference (fastest/most efficient first).

        Returns:
            Ordered list of connection methods to try
        """
        return [
            ConnectionMethod.MYSQL_CONNECTOR,  # Official MySQL connector
            ConnectionMethod.PYMYSQL,  # Pure Python MySQL client
            ConnectionMethod.SQLALCHEMY,  # ORM approach with pooling
        ]

    def _connect_mysql_connector(self) -> Any:
        """
        Connect using official MySQL Connector/Python.

        Returns:
            MySQL connection using official connector

        Raises:
            ImportError: If mysql-connector-python is not installed
            Exception: If connection fails
        """
        try:
            import mysql.connector
            from mysql.connector import pooling
        except ImportError:
            raise ImportError(
                "mysql-connector-python is required for MySQL Connector connection"
            )

        # Build connection parameters
        connection_params = {
            "host": self.connection_config.host,
            "port": self.connection_config.port or self.DEFAULT_PORT,
            "user": self.connection_config.username,
            "password": self.connection_config.password,
            "database": self.connection_config.database,
            "charset": self.connection_config.extra_params.get(
                "charset", self.DEFAULT_CHARSET
            ),
            "autocommit": True,
            "connect_timeout": self.connection_config.connect_timeout,
            "use_unicode": True,
        }

        # Add SSL parameters if specified
        ssl_config = self.connection_config.extra_params.get("ssl_config")
        if ssl_config:
            connection_params["ssl_config"] = ssl_config
        elif self.connection_config.extra_params.get("ssl_disabled"):
            connection_params["ssl_disabled"] = True

        # Add other MySQL-specific parameters
        if self.connection_config.extra_params.get("sql_mode"):
            connection_params["sql_mode"] = self.connection_config.extra_params[
                "sql_mode"
            ]

        if self.connection_config.extra_params.get("time_zone"):
            connection_params["time_zone"] = self.connection_config.extra_params[
                "time_zone"
            ]

        logger.info("Attempting MySQL Connector connection")

        # Create connection pool for better performance
        if self.connection_config.pool_size > 1:
            pool_config = {
                "pool_name": f"dimer_pool_{id(self)}",
                "pool_size": self.connection_config.pool_size,
                "pool_reset_session": True,
                **connection_params,
            }

            connection_pool = pooling.MySQLConnectionPool(**pool_config)
            connection = connection_pool.get_connection()
        else:
            connection = mysql.connector.connect(**connection_params)

        # Test the connection
        cursor = connection.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        cursor.close()

        logger.info("MySQL Connector connection established", mysql_version=version)
        return connection

    def _connect_pymysql(self) -> Any:
        """
        Connect using PyMySQL pure Python client.

        Returns:
            PyMySQL connection

        Raises:
            ImportError: If PyMySQL is not installed
            Exception: If connection fails
        """
        try:
            import pymysql
            from pymysql import cursors
        except ImportError:
            raise ImportError("PyMySQL is required for PyMySQL connection")

        # Build connection parameters
        connection_params = {
            "host": self.connection_config.host,
            "port": self.connection_config.port or self.DEFAULT_PORT,
            "user": self.connection_config.username,
            "password": self.connection_config.password,
            "database": self.connection_config.database,
            "charset": self.connection_config.extra_params.get(
                "charset", self.DEFAULT_CHARSET
            ),
            "autocommit": True,
            "connect_timeout": self.connection_config.connect_timeout,
            "cursorclass": cursors.Cursor,
        }

        # Add SSL parameters if specified
        ssl_config = self.connection_config.extra_params.get("ssl_config")
        if ssl_config:
            connection_params.update(ssl_config)

        # Add other PyMySQL-specific parameters
        if self.connection_config.extra_params.get("read_timeout"):
            connection_params["read_timeout"] = self.connection_config.extra_params[
                "read_timeout"
            ]

        if self.connection_config.extra_params.get("write_timeout"):
            connection_params["write_timeout"] = self.connection_config.extra_params[
                "write_timeout"
            ]

        logger.info("Attempting PyMySQL connection")
        connection = pymysql.connect(**connection_params)

        # Test the connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]

        logger.info("PyMySQL connection established", mysql_version=version)
        return connection

    def _connect_sqlalchemy(self) -> Any:
        """
        Connect using SQLAlchemy ORM approach.

        Returns:
            SQLAlchemy engine

        Raises:
            ImportError: If SQLAlchemy is not installed
            Exception: If connection fails
        """
        try:
            from sqlalchemy import create_engine
        except ImportError:
            raise ImportError("sqlalchemy is required for SQLAlchemy connection")

        # Build connection URL
        port = self.connection_config.port or self.DEFAULT_PORT
        charset = self.connection_config.extra_params.get(
            "charset", self.DEFAULT_CHARSET
        )

        url = (
            f"mysql+pymysql://{self.connection_config.username}:{self.connection_config.password}@"
            f"{self.connection_config.host}:{port}/{self.connection_config.database}?"
            f"charset={charset}"
        )

        # Add SSL parameters
        if self.connection_config.extra_params.get("ssl_disabled"):
            url += "&ssl_disabled=true"
        elif self.connection_config.extra_params.get("ssl_ca"):
            url += f"&ssl_ca={self.connection_config.extra_params['ssl_ca']}"

        # Add other parameters
        if self.connection_config.extra_params.get("sql_mode"):
            url += f"&sql_mode={self.connection_config.extra_params['sql_mode']}"

        logger.info("Attempting SQLAlchemy connection to MySQL")

        engine = create_engine(
            url,
            pool_size=self.connection_config.pool_size,
            max_overflow=self.connection_config.max_overflow,
            pool_timeout=self.connection_config.pool_timeout,
            pool_recycle=self.connection_config.pool_recycle,
            echo=self.connection_config.extra_params.get("echo", False),
        )

        # Test the connection
        with engine.connect() as conn:
            result = conn.execute("SELECT VERSION()").fetchone()
            if result is None or len(result) == 0:
                raise ConnectionError("Unable to retrieve MySQL version")
            version = result[0]

        logger.info("SQLAlchemy connection established", mysql_version=version)
        return engine

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
        if self.connection_method_used == ConnectionMethod.SQLALCHEMY:
            # SQLAlchemy engine
            return pd.read_sql(query, self.connection, params=params)

        else:
            # MySQL Connector or PyMySQL connections
            cursor = self.connection.cursor()
            try:
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
            finally:
                cursor.close()

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """
        MySQL-specific metadata extraction using information_schema.

        Args:
            table_name: Name of the table
            schema_name: Optional schema name (database name in MySQL)

        Returns:
            TableMetadata object with comprehensive table information
        """
        schema = schema_name or self.connection_config.database

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
            COLUMN_COMMENT,
            COLUMN_KEY,
            EXTRA
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table_name)s
        ORDER BY ORDINAL_POSITION
        """

        columns_df = self._execute_query_internal(columns_query, {'schema': schema, 'table_name': table_name})

        # Build column metadata
        columns = []
        for _, row in columns_df.iterrows():
            # Map MySQL types to common types
            common_type = DataTypeMapper.map_type("mysql", row["DATA_TYPE"])

            column = ColumnMetadata(
                name=row["COLUMN_NAME"],
                data_type=common_type,
                nullable=row["IS_NULLABLE"] == "YES",
                is_primary_key=row["COLUMN_KEY"] == "PRI",
                max_length=row["CHARACTER_MAXIMUM_LENGTH"],
                precision=row["NUMERIC_PRECISION"],
                scale=row["NUMERIC_SCALE"],
                default_value=row["COLUMN_DEFAULT"],
                description=row["COLUMN_COMMENT"],
                constraints=[row["EXTRA"]] if row["EXTRA"] else [],
            )
            columns.append(column)

        # Query for table statistics
        stats_query = """
        SELECT 
            TABLE_ROWS,
            DATA_LENGTH,
            INDEX_LENGTH,
            UPDATE_TIME,
            CREATE_TIME,
            TABLE_COMMENT
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table_name)s
        """

        stats_df = self._execute_query_internal(stats_query, {'schema': schema, 'table_name': table_name})

        # Extract statistics
        row_count = None
        size_bytes = None
        last_modified = None
        statistics = {"schema": schema, "table": table_name}

        if not stats_df.empty:
            row_count = stats_df.iloc[0]["TABLE_ROWS"]
            data_length = stats_df.iloc[0]["DATA_LENGTH"] or 0
            index_length = stats_df.iloc[0]["INDEX_LENGTH"] or 0
            size_bytes = data_length + index_length
            last_modified = stats_df.iloc[0]["UPDATE_TIME"]

            statistics.update(
                {
                    "data_length": data_length,
                    "index_length": index_length,
                    "create_time": stats_df.iloc[0]["CREATE_TIME"],
                    "table_comment": stats_df.iloc[0]["TABLE_COMMENT"],
                }
            )

        # Query for indexes
        indexes_query = """
        SELECT 
            INDEX_NAME,
            COLUMN_NAME,
            NON_UNIQUE,
            SEQ_IN_INDEX,
            INDEX_TYPE
        FROM information_schema.STATISTICS 
        WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table_name)s
        ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """

        indexes_df = self._execute_query_internal(indexes_query, {'schema': schema, 'table_name': table_name})

        # Group indexes
        indexes = []
        if not indexes_df.empty:
            current_index = None
            for _, row in indexes_df.iterrows():
                if current_index is None or current_index["name"] != row["INDEX_NAME"]:
                    if current_index:
                        indexes.append(current_index)
                    current_index = {
                        "name": row["INDEX_NAME"],
                        "columns": [row["COLUMN_NAME"]],
                        "unique": row["NON_UNIQUE"] == 0,
                        "type": row["INDEX_TYPE"],
                    }
                else:
                    current_index["columns"].append(row["COLUMN_NAME"])

            if current_index:
                indexes.append(current_index)

        return TableMetadata(
            name=table_name,
            schema=schema,
            columns=columns,
            row_count=row_count,
            size_bytes=size_bytes,
            last_modified=last_modified,
            statistics=statistics,
            indexes=indexes,
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Efficiently sample data using MySQL's LIMIT.

        Args:
            table_name: Name of the table
            limit: Maximum number of rows to return
            schema_name: Optional schema name (database name in MySQL)

        Returns:
            DataFrame containing sample data
        """
        schema = schema_name or self.connection_config.database
        full_table_name = f"`{schema}`.`{table_name}`"

        # MySQL doesn't have TABLESAMPLE, so use LIMIT with ORDER BY RAND() for larger samples
        if limit > 1000:
            # Use ORDER BY RAND() for random sampling, but limit to reasonable size for performance
            sample_limit = min(limit, 10000)
            query = f"""
            SELECT * FROM {full_table_name} 
            ORDER BY RAND() 
            LIMIT {sample_limit}
            """
        else:
            # Use simple LIMIT for small samples
            query = f"SELECT * FROM {full_table_name} LIMIT {limit}"

        return self._execute_query_internal(query)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        """
        List all tables in the database or schema.

        Args:
            schema_name: Optional schema name (database name in MySQL)

        Returns:
            List of table names
        """
        if schema_name:
            query = """
            SELECT TABLE_NAME FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = %(schema_name)s AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """
            df = self._execute_query_internal(query, {'schema_name': schema_name})
        else:
            query = """
            SELECT TABLE_NAME FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = %(database)s AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """
            df = self._execute_query_internal(query, {'database': self.connection_config.database})

        return df["TABLE_NAME"].tolist()

    def _list_schemas_internal(self) -> List[str]:
        """
        List all schemas (databases) in MySQL.

        Returns:
            List of schema names
        """
        query = """
        SELECT SCHEMA_NAME FROM information_schema.SCHEMATA 
        WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
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
        return "SELECT VERSION(), USER(), DATABASE()"

    def close(self) -> None:
        """Clean up MySQL-specific connection resources."""
        try:
            if self.connection:
                if self.connection_method_used == ConnectionMethod.SQLALCHEMY:
                    # Close SQLAlchemy engine
                    self.connection.dispose()
                else:
                    # Close MySQL Connector or PyMySQL connection
                    if hasattr(self.connection, "close"):
                        self.connection.close()

                self.connection = None
                self.connection_method_used = None

            logger.info("MySQL connection closed successfully")

        except Exception as e:
            logger.error("Error closing MySQL connection", error=str(e))
