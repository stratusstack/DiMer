"""PostgreSQL connector with multiple connection strategies."""

import asyncio
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


class PostgreSQLConnector(DataSourceConnector):
    """PostgreSQL-specific implementation with multiple connection strategies."""

    # PostgreSQL-specific configuration
    DEFAULT_PORT = 5432
    DEFAULT_SCHEMA = "public"
    MAX_SAMPLE_ROWS = 100000
    DIALECTS = {
        "hash": "MD5({COL}::text)",
        "concatenation": "||",
        "cast_to_text": "CAST({COL} AS TEXT)",
        "aggregate_hash": "BIT_XOR(CONV(SUBSTRING(MD5({COL}), 1, 16), 16, 10))",
    }

    def get_required_params(self) -> List[str]:
        """Return list of required connection parameters for PostgreSQL."""
        return ["host", "username", "password", "database"]

    def get_connection_methods(self) -> List[ConnectionMethod]:
        """
        Return methods in order of preference (fastest/most efficient first).

        Returns:
            Ordered list of connection methods to try
        """
        return [
            ConnectionMethod.ASYNCPG,  # Fastest async PostgreSQL driver
            ConnectionMethod.PSYCOPG2,  # Most popular PostgreSQL adapter
            ConnectionMethod.SQLALCHEMY,  # ORM approach with connection pooling
        ]

    def _connect_asyncpg(self) -> Any:
        """
        Connect using asyncpg for high performance.

        Returns:
            Asyncpg connection pool

        Raises:
            ImportError: If asyncpg is not installed
            Exception: If connection fails
        """
        try:
            import asyncpg
        except ImportError:
            raise ImportError("asyncpg is required for AsyncPG connection")

        # Build connection parameters
        connection_params = {
            "host": self.connection_config.host,
            "port": self.connection_config.port or self.DEFAULT_PORT,
            "user": self.connection_config.username,
            "password": self.connection_config.password,
            "database": self.connection_config.database,
        }

        # Add SSL parameters if specified
        if self.connection_config.extra_params.get("ssl_mode"):
            connection_params["ssl"] = self.connection_config.extra_params["ssl_mode"]

        logger.info("Attempting AsyncPG connection to PostgreSQL")

        # Create connection pool
        async def create_pool():
            return await asyncpg.create_pool(
                min_size=1,
                max_size=self.connection_config.pool_size,
                command_timeout=self.connection_config.query_timeout,
                **connection_params,
            )

        # Run async pool creation
        loop = asyncio.new_event_loop()

        try:
            pool = loop.run_until_complete(create_pool())

            # Test the connection
            async def test_connection():
                async with pool.acquire() as conn:
                    result = await conn.fetchval("SELECT version()")
                    return result

            version = loop.run_until_complete(test_connection())
            logger.info(
                "AsyncPG connection established", postgresql_version=version[:50]
            )

            # Store the loop for later use
            self._asyncpg_loop = loop
            return pool

        except Exception as e:
            loop.close()
            raise e

    def _connect_psycopg2(self) -> Any:
        """
        Connect using psycopg2 adapter.

        Returns:
            Psycopg2 connection

        Raises:
            ImportError: If psycopg2 is not installed
            Exception: If connection fails
        """
        try:
            import psycopg2
            from psycopg2.pool import ThreadedConnectionPool
        except ImportError:
            raise ImportError("psycopg2-binary is required for Psycopg2 connection")

        # Build connection parameters
        connection_params = {
            "host": self.connection_config.host,
            "port": self.connection_config.port or self.DEFAULT_PORT,
            "user": self.connection_config.username,
            "password": self.connection_config.password,
            "database": self.connection_config.database,
            "connect_timeout": self.connection_config.connect_timeout,
        }

        # Add SSL parameters if specified
        ssl_mode = self.connection_config.extra_params.get("ssl_mode")
        if ssl_mode:
            connection_params["sslmode"] = ssl_mode

        logger.info("Attempting Psycopg2 connection to PostgreSQL")

        # Create connection pool
        pool = ThreadedConnectionPool(
            minconn=1, maxconn=self.connection_config.pool_size, **connection_params
        )

        # Test the connection
        conn = pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            cursor.close()
            logger.info(
                "Psycopg2 connection established", postgresql_version=version[:50]
            )
        finally:
            pool.putconn(conn)

        return pool

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
        url = (
            f"postgresql://{self.connection_config.username}:{self.connection_config.password}@"
            f"{self.connection_config.host}:{port}/{self.connection_config.database}"
        )

        # Add SSL parameters if specified
        ssl_mode = self.connection_config.extra_params.get("ssl_mode")
        if ssl_mode:
            url += f"?sslmode={ssl_mode}"

        logger.info("Attempting SQLAlchemy connection to PostgreSQL")

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
            result = conn.execute("SELECT version()").fetchone()
            version = result[0]

        logger.info(
            "SQLAlchemy connection established", postgresql_version=version[:50]
        )
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
        if self.connection_method_used == ConnectionMethod.ASYNCPG:
            # AsyncPG pool
            async def execute_async():
                async with self.connection.acquire() as conn:
                    if params:
                        # asyncpg uses $1, $2, ... positional placeholders.
                        # Convert %(name)s style placeholders and extract values in order.
                        import re
                        param_order: list = []
                        counter = [0]

                        def _replace(m):
                            param_order.append(m.group(1))
                            counter[0] += 1
                            return f"${counter[0]}"

                        converted_query = re.sub(r"%\((\w+)\)s", _replace, query)
                        param_values = [params[name] for name in param_order]
                        result = await conn.fetch(converted_query, *param_values)
                    else:
                        result = await conn.fetch(query)

                    if result:
                        # Convert asyncpg records to DataFrame
                        columns = list(result[0].keys())
                        data = [list(record.values()) for record in result]
                        return pd.DataFrame(data, columns=columns)
                    else:
                        return pd.DataFrame()

            return self._asyncpg_loop.run_until_complete(execute_async())

        elif self.connection_method_used == ConnectionMethod.PSYCOPG2:
            # Psycopg2 pool
            conn = self.connection.getconn()
            try:
                cursor = conn.cursor()
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
                self.connection.putconn(conn)

        elif self.connection_method_used == ConnectionMethod.SQLALCHEMY:
            # SQLAlchemy engine
            return pd.read_sql(query, self.connection, params=params)

        else:
            raise ValueError(
                f"Unsupported connection method: {self.connection_method_used}"
            )

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """
        PostgreSQL-specific metadata extraction using information_schema.

        Args:
            table_name: Name of the table
            schema_name: Optional schema name

        Returns:
            TableMetadata object with comprehensive table information
        """
        schema = (
            schema_name or self.connection_config.schema_name or self.DEFAULT_SCHEMA
        )

        # Query for column metadata
        columns_query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            udt_name
        FROM information_schema.columns 
        WHERE table_schema = %(schema)s AND table_name = %(table_name)s
        ORDER BY ordinal_position
        """

        logger.debug(
            "get_table_metadata: fetching columns",
            schema=schema,
            table_name=table_name,
        )
        columns_df = self._execute_query_internal(
            columns_query, {"schema": schema, "table_name": table_name}
        )
        logger.debug(
            "get_table_metadata: columns_df result",
            row_count=len(columns_df),
            columns=list(columns_df.columns) if not columns_df.empty else [],
            first_rows=columns_df.head(3).to_dict("records") if not columns_df.empty else [],
        )

        # Build column metadata
        columns = []
        for _, row in columns_df.iterrows():
            # Map PostgreSQL types to common types
            common_type = DataTypeMapper.map_type("postgresql", row["data_type"])

            column = ColumnMetadata(
                name=row["column_name"],
                data_type=common_type,
                nullable=row["is_nullable"] == "YES",
                max_length=row["character_maximum_length"],
                precision=row["numeric_precision"],
                scale=row["numeric_scale"],
                default_value=row["column_default"],
            )
            columns.append(column)

        # Query for primary key constraints
        pk_query = """
        SELECT column_name
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name
        WHERE tc.table_schema = %(schema)s AND tc.table_name = %(table_name)s 
        AND tc.constraint_type = 'PRIMARY KEY'
        """

        pk_df = self._execute_query_internal(
            pk_query, {"schema": schema, "table_name": table_name}
        )

        # Mark primary key columns
        pk_columns = set(pk_df["column_name"].tolist() if not pk_df.empty else [])
        for column in columns:
            if column.name in pk_columns:
                column.is_primary_key = True

        # Query for table statistics
        stats_query = """
        SELECT
            schemaname,
            relname as tablename,
            n_tup_ins as inserts,
            n_tup_upd as updates,
            n_tup_del as deletes,
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM pg_stat_user_tables
        WHERE schemaname = %(schema)s AND relname = %(table_name)s
        """

        stats_df = self._execute_query_internal(
            stats_query, {"schema": schema, "table_name": table_name}
        )

        # Extract statistics
        row_count = None
        statistics = {"schema": schema, "table": table_name}

        if not stats_df.empty:
            row_count = stats_df.iloc[0]["live_tuples"]
            statistics.update(
                {
                    "inserts": stats_df.iloc[0]["inserts"],
                    "updates": stats_df.iloc[0]["updates"],
                    "deletes": stats_df.iloc[0]["deletes"],
                    "dead_tuples": stats_df.iloc[0]["dead_tuples"],
                    "last_vacuum": stats_df.iloc[0]["last_vacuum"],
                    "last_analyze": stats_df.iloc[0]["last_analyze"],
                }
            )

        return TableMetadata(
            columns=columns, row_count=row_count, statistics=statistics
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Efficiently sample data using PostgreSQL's TABLESAMPLE or LIMIT.

        Args:
            table_name: Name of the table
            limit: Maximum number of rows to return
            schema_name: Optional schema name

        Returns:
            DataFrame containing sample data
        """
        schema = (
            schema_name or self.connection_config.schema_name or self.DEFAULT_SCHEMA
        )
        full_table_name = f'"{schema}"."{table_name}"'

        # Use TABLESAMPLE for larger samples, LIMIT for smaller ones
        if limit > 1000:
            # Use TABLESAMPLE BERNOULLI for random sampling
            sample_percent = min(10, max(0.1, (limit / self.MAX_SAMPLE_ROWS) * 100))
            query = f"""
            SELECT * FROM {full_table_name} 
            TABLESAMPLE BERNOULLI ({sample_percent})
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
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = %(schema)s AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
            df = self._execute_query_internal(query, {"schema": schema_name})
        else:
            query = """
            SELECT table_name FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_name
            """
            df = self._execute_query_internal(query)

        return df["table_name"].tolist()

    def _list_schemas_internal(self) -> List[str]:
        """
        List all schemas in the database.

        Returns:
            List of schema names
        """
        query = """
        SELECT schema_name FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
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
        return "SELECT version(), current_user, current_database()"

    def close(self) -> None:
        """Clean up PostgreSQL-specific connection resources."""
        try:
            if self.connection:
                if self.connection_method_used == ConnectionMethod.ASYNCPG:
                    # Close AsyncPG pool
                    async def close_pool():
                        await self.connection.close()

                    if hasattr(self, "_asyncpg_loop") and self._asyncpg_loop:
                        self._asyncpg_loop.run_until_complete(close_pool())
                        self._asyncpg_loop.close()

                elif self.connection_method_used == ConnectionMethod.PSYCOPG2:
                    # Close Psycopg2 pool
                    self.connection.closeall()

                elif self.connection_method_used == ConnectionMethod.SQLALCHEMY:
                    # Close SQLAlchemy engine
                    self.connection.dispose()

                self.connection = None
                self.connection_method_used = None

            logger.info("PostgreSQL connection closed successfully")

        except Exception as e:
            logger.error("Error closing PostgreSQL connection", error=str(e))
