"""Abstract base class for all data source connectors."""

import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import pandas as pd
import structlog
from dimer.core.models import (
    ColumnMetadata,
    ConnectionConfig,
    ConnectionMethod,
    QueryResult,
    TableMetadata,
)
from dimer.metrics.collector import get_metrics_collector

logger = structlog.get_logger(__name__)


class DataSourceConnector(ABC):
    """Abstract base class for all data source connectors."""

    # Default configuration that can be overridden by subclasses
    DEFAULT_TIMEOUT = 300
    MAX_RETRIES = 3
    CONNECTION_POOL_SIZE = 5
    RETRY_BACKOFF_FACTOR = 2.0

    def __init__(
        self,
        connection_config: ConnectionConfig,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the connector.

        Args:
            connection_config: Configuration for database connection
            metadata: Additional metadata for the connector
        """
        self.connection_config = connection_config
        self.metadata = metadata or {}
        self.connection = None
        self.connection_method_used: Optional[ConnectionMethod] = None
        self.connection_methods = self.get_connection_methods()
        self._connection_pool: List[Any] = []
        self._metrics: List[Dict[str, Any]] = []

        # Validate required parameters
        self._validate_params()

        logger.info(
            "Connector initialized",
            connector_type=self.__class__.__name__,
            connection_methods=[m.value for m in self.connection_methods],
        )

    def _validate_params(self) -> None:
        """
        Validate required connection parameters.

        Raises:
            ValueError: If required parameters are missing or invalid
        """
        required_params = self.get_required_params()
        missing = []
        invalid = []

        for param in required_params:
            value = getattr(self.connection_config, param, None)
            if value is None:
                missing.append(param)
            elif isinstance(value, str) and value.strip() == "":
                missing.append(f"{param} (empty string)")

        # Basic format validation for common parameters
        if hasattr(self.connection_config, 'port') and self.connection_config.port is not None:
            if not isinstance(self.connection_config.port, int) or not (1 <= self.connection_config.port <= 65535):
                invalid.append("port (must be integer between 1-65535)")

        if hasattr(self.connection_config, 'host') and self.connection_config.host:
            host = self.connection_config.host.strip()
            if ' ' in host:
                invalid.append("host (contains spaces)")

        # Collect all validation errors
        errors = []
        if missing:
            errors.append(f"Missing required parameters: {missing}")
        if invalid:
            errors.append(f"Invalid parameters: {invalid}")

        if errors:
            raise ValueError("; ".join(errors))

    @abstractmethod
    def get_required_params(self) -> List[str]:
        """
        Return list of required connection parameters.

        Returns:
            List of parameter names that must be provided
        """
        pass

    @abstractmethod
    def get_connection_methods(self) -> List[ConnectionMethod]:
        """
        Return ordered list of connection methods to try.

        Returns:
            List of ConnectionMethod enums in order of preference
        """
        pass

    def connect(self) -> Any:
        """
        Try each connection method in order until one succeeds.

        Returns:
            The active connection object

        Raises:
            ConnectionError: If all connection methods fail
        """
        errors = {}

        for method in self.connection_methods:
            start_time = time.time()
            try:
                connection_func = getattr(self, f"_connect_{method.value}", None)
                if connection_func is None:
                    errors[method.value] = "Connection method not implemented"
                    continue

                self.connection = connection_func()
                self.connection_method_used = method
                connect_time = time.time() - start_time

                # Record successful connection metrics
                self._record_connection_attempt(method.value, True, connect_time)

                logger.info(
                    "Successfully connected",
                    method=method.value,
                    connect_time=connect_time,
                )
                return self.connection

            except Exception as e:
                connect_time = time.time() - start_time
                error_msg = str(e)
                errors[method.value] = error_msg

                # Record failed connection metrics
                self._record_connection_attempt(
                    method.value, False, connect_time, error_msg
                )

                logger.warning(
                    "Failed to connect",
                    method=method.value,
                    error=error_msg,
                    connect_time=connect_time,
                )
                continue

        raise ConnectionError(f"All connection methods failed: {errors}")

    def _record_connection_attempt(
        self,
        method: str,
        success: bool,
        duration: float,
        error_message: Optional[str] = None,
    ) -> None:
        """Record connection attempt metrics."""
        self._metrics.append(
            {
                "method": method,
                "success": success,
                "duration": duration,
                "error_message": error_message,
                "timestamp": time.time(),
            }
        )

        # Also feed into the global MetricsCollector
        try:
            collector = get_metrics_collector()
            collector.record_connection_attempt(
                source_type=self.__class__.__name__.replace("Connector", "").lower(),
                method=method,
                success=success,
                duration=duration,
                error_message=error_message,
            )
        except Exception:
            pass  # Don't let metrics collection failures affect connectivity

    def execute_query(self, query: str, params: Optional[Dict] = None) -> QueryResult:
        """
        Execute arbitrary query and return results with retry logic.

        Uses max_retries, retry_delay, and backoff_factor from ConnectionConfig.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            QueryResult containing data and metadata
        """
        if not self.connection:
            self.connect()

        max_retries = self.connection_config.max_retries
        retry_delay = self.connection_config.retry_delay
        backoff_factor = self.connection_config.backoff_factor
        last_exception = None

        for attempt in range(1, max_retries + 1):
            start_time = time.time()
            try:
                result_data = self._execute_query_internal(query, params)
                execution_time = time.time() - start_time

                # Determine rows affected
                rows_affected = 0
                if hasattr(result_data, "__len__"):
                    rows_affected = len(result_data)
                elif hasattr(result_data, "shape"):
                    rows_affected = result_data.shape[0]

                return QueryResult(
                    data=result_data,
                    execution_time=execution_time,
                    rows_affected=rows_affected,
                    query=query,
                    metadata={
                        "connection_method": (
                            self.connection_method_used.value
                            if self.connection_method_used
                            else None
                        ),
                        "params": params,
                    },
                )

            except Exception as e:
                execution_time = time.time() - start_time
                last_exception = e
                logger.error(
                    "Query execution failed",
                    query=query[:100] + "..." if len(query) > 100 else query,
                    error=str(e),
                    execution_time=execution_time,
                    attempt=attempt,
                    max_retries=max_retries,
                )
                if attempt < max_retries:
                    wait_time = retry_delay * (backoff_factor ** (attempt - 1))
                    time.sleep(wait_time)

        raise last_exception  # type: ignore[misc]

    @abstractmethod
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
        pass

    @abstractmethod
    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """
        Extract metadata for a specific table.

        Args:
            table_name: Name of the table
            schema_name: Optional schema name

        Returns:
            TableMetadata object containing table information
        """
        pass

    @abstractmethod
    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Retrieve sample data from the table.

        Args:
            table_name: Name of the table
            limit: Maximum number of rows to return
            schema_name: Optional schema name

        Returns:
            DataFrame containing sample data
        """
        pass

    def list_tables(self, schema_name: Optional[str] = None) -> List[str]:
        """
        List all tables in the database or schema.

        Args:
            schema_name: Optional schema name to filter tables

        Returns:
            List of table names
        """
        try:
            return self._list_tables_internal(schema_name)
        except Exception as e:
            logger.error("Failed to list tables", error=str(e), schema=schema_name)
            raise

    @abstractmethod
    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        """
        Internal method to list tables.

        Args:
            schema_name: Optional schema name

        Returns:
            List of table names
        """
        pass

    def list_schemas(self) -> List[str]:
        """
        List all schemas in the database.

        Returns:
            List of schema names
        """
        try:
            return self._list_schemas_internal()
        except Exception as e:
            logger.error("Failed to list schemas", error=str(e))
            raise

    @abstractmethod
    def _list_schemas_internal(self) -> List[str]:
        """
        Internal method to list schemas.

        Returns:
            List of schema names
        """
        pass

    def test_connection(self) -> bool:
        """
        Test if the connection is working.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if not self.connection:
                self.connect()

            # Execute a simple query to test the connection
            test_query = self._get_test_query()
            self._execute_query_internal(test_query)
            return True

        except Exception as e:
            logger.warning("Connection test failed", error=str(e))
            return False

    @abstractmethod
    def _get_test_query(self) -> str:
        """
        Return a simple query to test the connection.

        Returns:
            A simple SQL query string
        """
        pass

    def close(self) -> None:
        """Clean up connection resources."""
        try:
            if self.connection:
                if hasattr(self.connection, "close"):
                    self.connection.close()
                elif hasattr(self.connection, "disconnect"):
                    self.connection.disconnect()

                self.connection = None
                self.connection_method_used = None

            # Close any pooled connections
            for conn in self._connection_pool:
                if hasattr(conn, "close"):
                    conn.close()
            self._connection_pool.clear()

            logger.info("Connection closed successfully")

        except Exception as e:
            logger.error("Error closing connection", error=str(e))

    def get_connection_metrics(self) -> List[Dict[str, Any]]:
        """
        Return connection performance metrics.

        Returns:
            List of metrics dictionaries
        """
        return self._metrics.copy()

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def __repr__(self) -> str:
        """String representation of the connector."""
        return (
            f"{self.__class__.__name__}("
            f"connection_method={self.connection_method_used.value if self.connection_method_used else None}, "
            f"connected={self.connection is not None})"
        )
