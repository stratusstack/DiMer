"""
DiffForge: Universal Data Source Connector Module

A comprehensive integration and connectivity module for data diffing applications
that supports multiple data sources with intelligent fallback mechanisms.
"""

__version__ = "0.1.0"
__author__ = "DiffForge Team"

from diffforge.core.base import DataSourceConnector
from diffforge.core.factory import ConnectorFactory
from diffforge.core.manager import ConnectionManager
from diffforge.core.models import ColumnMetadata, ConnectionMethod, TableMetadata
from diffforge.core.types import DataTypeMapper

__all__ = [
    "DataSourceConnector",
    "ConnectorFactory",
    "ConnectionManager",
    "ColumnMetadata",
    "TableMetadata",
    "ConnectionMethod",
    "DataTypeMapper",
]
