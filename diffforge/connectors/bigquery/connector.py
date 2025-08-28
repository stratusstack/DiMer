"""BigQuery connector with multiple connection strategies."""

import os
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


class BigQueryConnector(DataSourceConnector):
    """BigQuery-specific implementation with multiple connection strategies."""

    # BigQuery-specific configuration
    DEFAULT_LOCATION = "US"
    MAX_SAMPLE_ROWS = 1000000

    def get_required_params(self) -> List[str]:
        """Return list of required connection parameters for BigQuery."""
        # BigQuery requires project_id, credentials can come from various sources
        return ["database"]  # database field used as project_id

    def get_connection_methods(self) -> List[ConnectionMethod]:
        """
        Return methods in order of preference (fastest/most efficient first).

        Returns:
            Ordered list of connection methods to try
        """
        return [
            ConnectionMethod.BIGQUERY_STORAGE,  # Fastest for large datasets
            ConnectionMethod.NATIVE,  # Standard BigQuery client
            ConnectionMethod.SQLALCHEMY,  # ORM approach
        ]

    def _connect_bigquery_storage(self) -> Any:
        """
        Connect using BigQuery Storage API for high performance reads.

        Returns:
            BigQuery client with Storage API support

        Raises:
            ImportError: If required packages are not installed
            Exception: If connection fails
        """
        try:
            from google.cloud import bigquery, bigquery_storage
        except ImportError:
            raise ImportError(
                "google-cloud-bigquery and google-cloud-bigquery-storage are required for Storage API connection"
            )

        # Set up credentials
        self._setup_credentials()

        project_id = (
            self.connection_config.database
        )  # Using database field as project_id
        location = self.connection_config.extra_params.get(
            "location", self.DEFAULT_LOCATION
        )

        logger.info("Attempting BigQuery Storage API connection")

        # Create BigQuery client
        client = bigquery.Client(project=project_id, location=location)

        # Create Storage client for high-performance reads
        storage_client = bigquery_storage.BigQueryReadClient()

        # Test the connection
        query = "SELECT 1 as test_column"
        job = client.query(query)
        result = job.result()
        list(result)  # Consume the result

        # Store both clients
        connection_wrapper = {
            "client": client,
            "storage_client": storage_client,
            "project_id": project_id,
            "location": location,
        }

        logger.info("BigQuery Storage API connection established", project=project_id)
        return connection_wrapper

    def _connect_native(self) -> Any:
        """
        Connect using standard BigQuery client.

        Returns:
            BigQuery client

        Raises:
            ImportError: If BigQuery client is not installed
            Exception: If connection fails
        """
        try:
            from google.cloud import bigquery
        except ImportError:
            raise ImportError(
                "google-cloud-bigquery is required for native BigQuery connection"
            )

        # Set up credentials
        self._setup_credentials()

        project_id = (
            self.connection_config.database
        )  # Using database field as project_id
        location = self.connection_config.extra_params.get(
            "location", self.DEFAULT_LOCATION
        )

        logger.info("Attempting native BigQuery connection")

        # Create client
        client = bigquery.Client(project=project_id, location=location)

        # Test the connection
        query = "SELECT 1 as test_column"
        job = client.query(query)
        result = job.result()
        list(result)  # Consume the result

        logger.info("Native BigQuery connection established", project=project_id)
        return client

    def _connect_sqlalchemy(self) -> Any:
        """
        Connect using SQLAlchemy with BigQuery dialect.

        Returns:
            SQLAlchemy engine

        Raises:
            ImportError: If SQLAlchemy or BigQuery dialect is not installed
            Exception: If connection fails
        """
        try:
            from sqlalchemy import create_engine
        except ImportError:
            raise ImportError("sqlalchemy is required for SQLAlchemy connection")

        try:
            import sqlalchemy_bigquery
        except ImportError:
            raise ImportError(
                "sqlalchemy-bigquery is required for SQLAlchemy BigQuery connection"
            )

        # Set up credentials
        self._setup_credentials()

        project_id = (
            self.connection_config.database
        )  # Using database field as project_id
        dataset_id = self.connection_config.schema_name or "default"
        location = self.connection_config.extra_params.get(
            "location", self.DEFAULT_LOCATION
        )

        # Build connection URL
        url = f"bigquery://{project_id}/{dataset_id}?location={location}"

        logger.info("Attempting SQLAlchemy BigQuery connection")

        engine = create_engine(
            url,
            echo=self.connection_config.extra_params.get("echo", False),
        )

        # Test the connection
        with engine.connect() as conn:
            result = conn.execute("SELECT 1 as test_column").fetchone()

        logger.info("SQLAlchemy BigQuery connection established", project=project_id)
        return engine

    def _setup_credentials(self) -> None:
        """Set up Google Cloud credentials from various sources."""
        # Priority order for credentials:
        # 1. Explicit credentials file path in extra_params
        # 2. Service account key in extra_params
        # 3. GOOGLE_APPLICATION_CREDENTIALS environment variable
        # 4. Default credentials (gcloud, metadata service, etc.)

        credentials_path = self.connection_config.extra_params.get("credentials_path")
        service_account_info = self.connection_config.extra_params.get(
            "service_account_info"
        )

        if credentials_path and os.path.exists(credentials_path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
            logger.info("Using credentials from file", path=credentials_path)
        elif service_account_info:
            # Write service account info to temporary file
            import json
            import tempfile

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            ) as f:
                json.dump(service_account_info, f)
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name
            logger.info("Using service account info from config")
        elif "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
            logger.info("Using GOOGLE_APPLICATION_CREDENTIALS environment variable")
        else:
            logger.info("Using default credentials (gcloud, metadata service, etc.)")

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

        elif self.connection_method_used == ConnectionMethod.BIGQUERY_STORAGE:
            # BigQuery client with Storage API
            client = self.connection["client"]

            # For large queries, try to use Storage API if possible
            if self._should_use_storage_api(query):
                try:
                    return self._execute_with_storage_api(query)
                except Exception as e:
                    logger.warning(
                        "Storage API failed, falling back to standard query",
                        error=str(e),
                    )

            # Fall back to standard query
            job_config = None
            if params:
                # Convert parameters for BigQuery
                job_config = bigquery.QueryJobConfig(
                    query_parameters=self._convert_params_to_bigquery(params)
                )

            job = client.query(query, job_config=job_config)
            return job.to_dataframe()

        else:
            # Native BigQuery client
            job_config = None
            if params:
                job_config = bigquery.QueryJobConfig(
                    query_parameters=self._convert_params_to_bigquery(params)
                )

            job = self.connection.query(query, job_config=job_config)
            return job.to_dataframe()

    def _should_use_storage_api(self, query: str) -> bool:
        """Determine if query should use Storage API for better performance."""
        # Use Storage API for simple SELECT queries without complex operations
        query_lower = query.lower().strip()

        # Don't use for DDL, DML, or complex queries
        if any(
            keyword in query_lower
            for keyword in ["create", "insert", "update", "delete", "alter"]
        ):
            return False

        # Don't use for queries with functions that might not be supported
        if any(
            keyword in query_lower
            for keyword in ["current_timestamp", "rand()", "uuid()"]
        ):
            return False

        # Use for simple SELECT queries
        return query_lower.startswith("select")

    def _execute_with_storage_api(self, query: str) -> pd.DataFrame:
        """Execute query using BigQuery Storage API for better performance."""
        # This is a simplified implementation - in practice, you'd need to:
        # 1. Parse the query to extract the table reference
        # 2. Create a Storage API read session for the table
        # 3. Apply any WHERE clauses as filters
        # For now, fall back to standard query
        raise NotImplementedError("Storage API optimization not yet implemented")

    def _convert_params_to_bigquery(self, params: Dict) -> List:
        """Convert parameter dictionary to BigQuery query parameters."""
        from google.cloud import bigquery

        bq_params = []
        for key, value in params.items():
            param_type = bigquery.ScalarQueryParameter.infer_type(value)
            bq_params.append(bigquery.ScalarQueryParameter(key, param_type, value))

        return bq_params

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """
        BigQuery-specific metadata extraction using INFORMATION_SCHEMA.

        Args:
            table_name: Name of the table
            schema_name: Optional dataset name

        Returns:
            TableMetadata object with comprehensive table information
        """
        dataset = schema_name or self.connection_config.schema_name or "default"
        project_id = self.connection_config.database

        # Query for column metadata
        columns_query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            is_generated,
            generation_expression,
            is_stored,
            is_hidden,
            is_updatable,
            clustering_ordinal_position
        FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """

        columns_df = self._execute_query_internal(columns_query)

        # Build column metadata
        columns = []
        for _, row in columns_df.iterrows():
            # Map BigQuery types to common types
            common_type = DataTypeMapper.map_type("bigquery", row["data_type"])

            column = ColumnMetadata(
                name=row["column_name"],
                data_type=common_type,
                nullable=row["is_nullable"] == "YES",
                description=row.get("generation_expression", ""),
                constraints=[
                    constraint
                    for constraint in [
                        "GENERATED" if row.get("is_generated") == "ALWAYS" else None,
                        "STORED" if row.get("is_stored") == "YES" else None,
                        "HIDDEN" if row.get("is_hidden") == "YES" else None,
                    ]
                    if constraint
                ],
            )
            columns.append(column)

        # Query for table metadata
        table_query = f"""
        SELECT 
            creation_time,
            last_modified_time,
            row_count,
            size_bytes,
            type,
            description
        FROM `{project_id}.{dataset}.__TABLES__`
        WHERE table_id = '{table_name}'
        """

        try:
            table_df = self._execute_query_internal(table_query)
        except Exception as e:
            logger.warning("Failed to get table statistics", error=str(e))
            table_df = pd.DataFrame()

        # Extract statistics
        row_count = None
        size_bytes = None
        last_modified = None
        statistics = {"project": project_id, "dataset": dataset, "table": table_name}

        if not table_df.empty:
            row = table_df.iloc[0]
            row_count = row.get("row_count")
            size_bytes = row.get("size_bytes")
            last_modified = row.get("last_modified_time")

            statistics.update(
                {
                    "creation_time": row.get("creation_time"),
                    "table_type": row.get("type"),
                    "table_description": row.get("description"),
                }
            )

        return TableMetadata(
            columns=columns,
            row_count=row_count,
            size_bytes=size_bytes,
            last_modified=last_modified,
            statistics=statistics,
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Efficiently sample data using BigQuery's TABLESAMPLE or LIMIT.

        Args:
            table_name: Name of the table
            limit: Maximum number of rows to return
            schema_name: Optional dataset name

        Returns:
            DataFrame containing sample data
        """
        dataset = schema_name or self.connection_config.schema_name or "default"
        project_id = self.connection_config.database
        full_table_name = f"`{project_id}.{dataset}.{table_name}`"

        # Use TABLESAMPLE for larger samples, LIMIT for smaller ones
        if limit > 1000:
            # Use TABLESAMPLE SYSTEM for random sampling
            sample_percent = min(10, max(0.1, (limit / self.MAX_SAMPLE_ROWS) * 100))
            query = f"""
            SELECT * FROM {full_table_name} 
            TABLESAMPLE SYSTEM ({sample_percent} PERCENT)
            LIMIT {limit}
            """
        else:
            # Use simple LIMIT for small samples
            query = f"SELECT * FROM {full_table_name} LIMIT {limit}"

        return self._execute_query_internal(query)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        """
        List all tables in the dataset.

        Args:
            schema_name: Optional dataset name

        Returns:
            List of table names
        """
        dataset = schema_name or self.connection_config.schema_name or "default"
        project_id = self.connection_config.database

        query = f"""
        SELECT table_name 
        FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.TABLES`
        WHERE table_type = 'BASE TABLE'
        ORDER BY table_name
        """

        df = self._execute_query_internal(query)
        return df["table_name"].tolist()

    def _list_schemas_internal(self) -> List[str]:
        """
        List all datasets in the project.

        Returns:
            List of dataset names
        """
        project_id = self.connection_config.database

        query = f"""
        SELECT schema_name as dataset_name
        FROM `{project_id}.INFORMATION_SCHEMA.SCHEMATA`
        ORDER BY schema_name
        """

        df = self._execute_query_internal(query)
        return df["dataset_name"].tolist()

    def _get_test_query(self) -> str:
        """
        Return a simple query to test the connection.

        Returns:
            A simple SQL query string
        """
        return "SELECT CURRENT_TIMESTAMP() as current_time, @@project_id as project"

    def close(self) -> None:
        """Clean up BigQuery-specific connection resources."""
        try:
            if self.connection:
                if self.connection_method_used == ConnectionMethod.SQLALCHEMY:
                    # Close SQLAlchemy engine
                    self.connection.dispose()
                elif self.connection_method_used == ConnectionMethod.BIGQUERY_STORAGE:
                    # Close BigQuery clients
                    if isinstance(self.connection, dict):
                        client = self.connection.get("client")
                        storage_client = self.connection.get("storage_client")
                        if client:
                            client.close()
                        if storage_client:
                            storage_client.transport._channel.close()
                else:
                    # Close native BigQuery client
                    if hasattr(self.connection, "close"):
                        self.connection.close()

                self.connection = None
                self.connection_method_used = None

            logger.info("BigQuery connection closed successfully")

        except Exception as e:
            logger.error("Error closing BigQuery connection", error=str(e))
