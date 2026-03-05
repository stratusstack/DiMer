"""
DiMer: Universal Data Source Connector Module

A comprehensive integration and connectivity module for data diffing applications
that supports multiple data sources with intelligent fallback mechanisms.
"""

__version__ = "0.1.0"
__author__ = "DiMer Team"

from dimer.core.base import DataSourceConnector
from dimer.core.compare import Diffcheck
from dimer.core.factory import ConnectorFactory
from dimer.core.manager import ConnectionManager
from dimer.core.models import (
    ColumnMetadata,
    ComparisonConfig,
    DiffRun,
    DiffResult,
    DiffRow,
    DiffJob,
    Project,
    ProjectSource,
    RowStatus,
    User,
    ConnectionConfig,
    ConnectionMethod,
    ConnectionMetrics,
    QueryResult,
    TableMetadata,
)
from dimer.core.types import DataTypeMapper

__all__ = [
    "DataSourceConnector",
    "ConnectorFactory",
    "ConnectionManager",
    "ConnectionConfig",
    "ColumnMetadata",
    "TableMetadata",
    "ConnectionMethod",
    "ConnectionMetrics",
    "QueryResult",
    "ComparisonConfig",
    "DiffRun",
    "DiffResult",
    "DiffRow",
    "DiffJob",
    "Project",
    "ProjectSource",
    "RowStatus",
    "User",
    "Diffcheck",
    "DataTypeMapper",
]
