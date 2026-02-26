"""Connection manager for handling connection lifecycle and pooling."""

import threading
import time
import weakref
from typing import Any, Dict, List, Optional

import structlog

from dimer.core.base import DataSourceConnector
from dimer.core.factory import ConnectorFactory
from dimer.core.models import ConnectionConfig

logger = structlog.get_logger(__name__)


class ConnectionManager:
    """Manage connection lifecycle and pooling across multiple data sources."""

    def __init__(self, default_pool_size: int = 5, connection_timeout: int = 300):
        """
        Initialize connection manager.

        Args:
            default_pool_size: Default size for connection pools
            connection_timeout: Default connection timeout in seconds
        """
        self._connections: Dict[str, DataSourceConnector] = {}
        self._connection_pools: Dict[str, List[DataSourceConnector]] = {}
        self._connection_configs: Dict[str, ConnectionConfig] = {}
        self._connection_metadata: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._default_pool_size = default_pool_size
        self._connection_timeout = connection_timeout
        self._last_used: Dict[str, float] = {}

        # Weak references to track connector instances
        self._connector_refs: weakref.WeakSet = weakref.WeakSet()

        # Start background cleanup thread
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_expired_connections, daemon=True
        )
        self._cleanup_running = True
        self._cleanup_thread.start()

        logger.info(
            "Connection manager initialized", default_pool_size=default_pool_size
        )

    def create_connection(
        self,
        connection_id: str,
        source_type: str,
        connection_config: ConnectionConfig,
        metadata: Optional[Dict[str, Any]] = None,
        connect_immediately: bool = True,
    ) -> DataSourceConnector:
        """
        Create a new connection and store it in the manager.

        Args:
            connection_id: Unique identifier for the connection
            source_type: Type of data source (e.g., 'snowflake', 'postgresql')
            connection_config: Connection configuration
            metadata: Optional connection metadata
            connect_immediately: Whether to establish connection immediately

        Returns:
            Configured connector instance

        Raises:
            ValueError: If connection_id already exists or source_type is unsupported
        """
        with self._lock:
            if connection_id in self._connections:
                raise ValueError(f"Connection ID '{connection_id}' already exists")

            # Create connector using factory
            connector = ConnectorFactory.create_connector(
                source_type=source_type,
                connection_config=connection_config,
                metadata=metadata,
            )

            # Store connection details
            self._connections[connection_id] = connector
            self._connection_configs[connection_id] = connection_config
            self._connection_metadata[connection_id] = metadata or {}
            self._last_used[connection_id] = time.time()

            # Track connector with weak reference
            self._connector_refs.add(connector)

            # Establish connection if requested
            if connect_immediately:
                try:
                    connector.connect()
                except Exception as e:
                    # Clean up on connection failure
                    del self._connections[connection_id]
                    del self._connection_configs[connection_id]
                    del self._connection_metadata[connection_id]
                    del self._last_used[connection_id]
                    raise

            logger.info(
                "Connection created",
                connection_id=connection_id,
                source_type=source_type,
                connected=connector.connection is not None,
            )

            return connector

    def get_connection(self, connection_id: str) -> Optional[DataSourceConnector]:
        """
        Get an existing connection by ID.

        Args:
            connection_id: The connection identifier

        Returns:
            Connector instance or None if not found
        """
        with self._lock:
            connector = self._connections.get(connection_id)
            if connector:
                self._last_used[connection_id] = time.time()
            return connector

    def remove_connection(self, connection_id: str) -> bool:
        """
        Remove and close a connection.

        Args:
            connection_id: The connection identifier

        Returns:
            True if connection was found and removed, False otherwise
        """
        with self._lock:
            connector = self._connections.get(connection_id)
            if not connector:
                return False

            try:
                connector.close()
            except Exception as e:
                logger.warning(
                    "Error closing connection during removal",
                    connection_id=connection_id,
                    error=str(e),
                )

            # Clean up all references
            del self._connections[connection_id]
            del self._connection_configs[connection_id]
            del self._connection_metadata[connection_id]
            del self._last_used[connection_id]

            # Remove from any pools
            for pool in self._connection_pools.values():
                if connector in pool:
                    pool.remove(connector)

            logger.info("Connection removed", connection_id=connection_id)
            return True

    def list_connections(self) -> List[Dict[str, Any]]:
        """
        List all managed connections with their status.

        Returns:
            List of connection information dictionaries
        """
        with self._lock:
            connections = []
            for connection_id, connector in self._connections.items():
                config = self._connection_configs[connection_id]
                metadata = self._connection_metadata[connection_id]
                last_used = self._last_used[connection_id]

                connections.append(
                    {
                        "connection_id": connection_id,
                        "source_type": connector.__class__.__name__.replace(
                            "Connector", ""
                        ).lower(),
                        "connected": connector.connection is not None,
                        "connection_method": (
                            connector.connection_method_used.value
                            if connector.connection_method_used
                            else None
                        ),
                        "last_used": last_used,
                        "host": getattr(config, "host", None),
                        "database": getattr(config, "database", None),
                        "metadata": metadata,
                    }
                )

            return connections

    def test_connection(self, connection_id: str) -> bool:
        """
        Test if a connection is working.

        Args:
            connection_id: The connection identifier

        Returns:
            True if connection test succeeds, False otherwise
        """
        connector = self.get_connection(connection_id)
        if not connector:
            return False

        try:
            return connector.test_connection()
        except Exception as e:
            logger.warning(
                "Connection test failed", connection_id=connection_id, error=str(e)
            )
            return False

    def reconnect(self, connection_id: str) -> bool:
        """
        Reconnect an existing connection.

        Args:
            connection_id: The connection identifier

        Returns:
            True if reconnection succeeds, False otherwise
        """
        connector = self.get_connection(connection_id)
        if not connector:
            return False

        try:
            # Close existing connection
            connector.close()

            # Establish new connection
            connector.connect()

            self._last_used[connection_id] = time.time()
            logger.info("Connection reconnected", connection_id=connection_id)
            return True

        except Exception as e:
            logger.error(
                "Failed to reconnect", connection_id=connection_id, error=str(e)
            )
            return False

    def get_connection_metrics(
        self, connection_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get performance metrics for connections.

        Args:
            connection_id: Optional specific connection ID, or None for all connections

        Returns:
            Dictionary containing metrics data
        """
        with self._lock:
            if connection_id:
                connector = self._connections.get(connection_id)
                if connector:
                    return {
                        "connection_id": connection_id,
                        "metrics": connector.get_connection_metrics(),
                    }
                return {}

            # Return metrics for all connections
            all_metrics = {}
            for conn_id, connector in self._connections.items():
                all_metrics[conn_id] = connector.get_connection_metrics()

            return all_metrics

    def close_all(self) -> None:
        """Clean up all connections and pools."""
        logger.info("Closing all connections")

        # Stop cleanup thread
        self._cleanup_running = False
        if self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=1.0)

        with self._lock:
            # Close all active connections
            for connection_id, connector in self._connections.items():
                try:
                    connector.close()
                except Exception as e:
                    logger.warning(
                        "Error closing connection",
                        connection_id=connection_id,
                        error=str(e),
                    )

            # Close all pooled connections
            for pool in self._connection_pools.values():
                for connector in pool:
                    try:
                        connector.close()
                    except Exception as e:
                        logger.warning("Error closing pooled connection", error=str(e))

            # Clear all data structures
            self._connections.clear()
            self._connection_pools.clear()
            self._connection_configs.clear()
            self._connection_metadata.clear()
            self._last_used.clear()

        logger.info("All connections closed")

    def _cleanup_expired_connections(self) -> None:
        """Background thread to clean up expired connections."""
        while self._cleanup_running:
            try:
                current_time = time.time()

                with self._lock:
                    expired_connections = [
                        connection_id
                        for connection_id, last_used in self._last_used.items()
                        if current_time - last_used > self._connection_timeout
                    ]

                    # Remove expired connections while still holding the lock
                    for connection_id in expired_connections:
                        logger.info(
                            "Removing expired connection", connection_id=connection_id
                        )
                        connector = self._connections.get(connection_id)
                        if connector:
                            try:
                                connector.close()
                            except Exception as e:
                                logger.warning(
                                    "Error closing expired connection",
                                    connection_id=connection_id,
                                    error=str(e),
                                )
                            del self._connections[connection_id]
                            del self._connection_configs[connection_id]
                            del self._connection_metadata[connection_id]
                            del self._last_used[connection_id]
                            for pool in self._connection_pools.values():
                                if connector in pool:
                                    pool.remove(connector)

                # Sleep for 60 seconds before next cleanup
                time.sleep(60)

            except Exception as e:
                logger.error("Error in connection cleanup thread", error=str(e))
                time.sleep(60)  # Wait before retrying

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_all()

    def __del__(self):
        """Destructor to ensure cleanup."""
        try:
            self.close_all()
        except Exception:
            pass  # Ignore errors during cleanup


# Global connection manager instance
_global_manager: Optional[ConnectionManager] = None
_manager_lock = threading.Lock()


def get_connection_manager() -> ConnectionManager:
    """
    Get the global connection manager instance.

    Returns:
        Global ConnectionManager instance
    """
    global _global_manager

    if _global_manager is None:
        with _manager_lock:
            if _global_manager is None:
                _global_manager = ConnectionManager()

    return _global_manager


def set_connection_manager(manager: ConnectionManager) -> None:
    """
    Set a custom connection manager as the global instance.

    Args:
        manager: ConnectionManager instance to use globally
    """
    global _global_manager

    with _manager_lock:
        if _global_manager is not None:
            _global_manager.close_all()
        _global_manager = manager
