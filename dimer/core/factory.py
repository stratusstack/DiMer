"""Factory class for creating data source connectors."""

from typing import Any, Dict, Type

import structlog

from dimer.core.base import DataSourceConnector
from dimer.core.models import ConnectionConfig

logger = structlog.get_logger(__name__)


class ConnectorFactory:
    """Factory class to create appropriate connector instances."""

    _connectors: Dict[str, Type[DataSourceConnector]] = {}

    @classmethod
    def register_connector(
        cls, source_type: str, connector_class: Type[DataSourceConnector]
    ) -> None:
        """
        Register a connector class for a specific source type.

        Args:
            source_type: The data source type identifier
            connector_class: The connector class to register
        """
        if not issubclass(connector_class, DataSourceConnector):
            raise ValueError(f"Connector class must inherit from DataSourceConnector")

        cls._connectors[source_type.lower()] = connector_class
        logger.info(
            "Connector registered",
            source_type=source_type,
            connector_class=connector_class.__name__,
        )

    @classmethod
    def create_connector(
        cls,
        source_type: str,
        connection_config: ConnectionConfig,
        metadata: Dict[str, Any] = None,
    ) -> DataSourceConnector:
        """
        Create and return appropriate connector instance.

        Args:
            source_type: The data source type
            connection_config: Connection configuration
            metadata: Optional metadata

        Returns:
            Configured connector instance

        Raises:
            ValueError: If source_type is not supported
        """
        source_type_lower = source_type.lower()
        connector_class = cls._connectors.get(source_type_lower)

        if not connector_class:
            available_types = list(cls._connectors.keys())
            raise ValueError(
                f"Unsupported source type: {source_type}. "
                f"Available types: {available_types}"
            )

        logger.info(
            "Creating connector",
            source_type=source_type,
            connector_class=connector_class.__name__,
        )

        return connector_class(connection_config, metadata)

    @classmethod
    def get_supported_sources(cls) -> list[str]:
        """
        Get list of supported data source types.

        Returns:
            List of supported source type names
        """
        return list(cls._connectors.keys())

    @classmethod
    def is_source_supported(cls, source_type: str) -> bool:
        """
        Check if a source type is supported.

        Args:
            source_type: The data source type to check

        Returns:
            True if supported, False otherwise
        """
        return source_type.lower() in cls._connectors


# Auto-register connectors when they are imported
def _auto_register_connectors() -> None:
    """Automatically register available connectors."""
    try:
        # Import and register Snowflake connector
        from dimer.connectors.snowflake.connector import SnowflakeConnector

        ConnectorFactory.register_connector("snowflake", SnowflakeConnector)
    except ImportError:
        logger.debug("Snowflake connector not available")

    try:
        # Import and register PostgreSQL connector
        from dimer.connectors.postgresql.connector import PostgreSQLConnector

        ConnectorFactory.register_connector("postgresql", PostgreSQLConnector)
        ConnectorFactory.register_connector("postgres", PostgreSQLConnector)
    except ImportError:
        logger.debug("PostgreSQL connector not available")

    try:
        # Import and register MySQL connector
        from dimer.connectors.mysql.connector import MySQLConnector

        ConnectorFactory.register_connector("mysql", MySQLConnector)
    except ImportError:
        logger.debug("MySQL connector not available")

    try:
        # Import and register BigQuery connector
        from dimer.connectors.bigquery.connector import BigQueryConnector

        ConnectorFactory.register_connector("bigquery", BigQueryConnector)
        ConnectorFactory.register_connector("bq", BigQueryConnector)
    except ImportError:
        logger.debug("BigQuery connector not available")

    try:
        # Import and register Databricks connector
        from dimer.connectors.databricks.connector import DatabricksConnector

        ConnectorFactory.register_connector("databricks", DatabricksConnector)
    except ImportError:
        logger.debug("Databricks connector not available")

    try:
        # Import and register file-based connectors
        from dimer.connectors.files.csv_connector import CSVConnector
        from dimer.connectors.files.parquet_connector import ParquetConnector

        ConnectorFactory.register_connector("parquet", ParquetConnector)
        ConnectorFactory.register_connector("csv", CSVConnector)
    except ImportError:
        logger.debug("File connectors not available")


# Auto-register connectors on module import
_auto_register_connectors()
