"""Data models and enums for the connector system."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, TypedDict


class ConnectionMethod(Enum):
    """Enum for different connection strategies."""

    # High-performance methods
    ARROW = "arrow"
    NATIVE = "native"

    # Cloud-specific optimized methods
    SNOWPARK = "snowpark"
    BIGQUERY_STORAGE = "bigquery_storage"
    DATABRICKS_CONNECT = "databricks_connect"

    # Standard database methods
    SQLALCHEMY = "sqlalchemy"
    ASYNCPG = "asyncpg"
    PSYCOPG2 = "psycopg2"
    PYMYSQL = "pymysql"
    MYSQL_CONNECTOR = "mysql_connector"

    # Legacy/fallback methods
    JDBC = "jdbc"
    ODBC = "odbc"

    # File-based methods
    PANDAS_DIRECT = "pandas_direct"
    PYARROW_DIRECT = "pyarrow_direct"


@dataclass
class ColumnMetadata:
    """Metadata for a database column."""

    name: str
    data_type: str
    nullable: bool
    is_primary_key: bool = False
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    default_value: Optional[str] = None
    description: Optional[str] = None
    constraints: List[str] = field(default_factory=list)


@dataclass
class TableMetadata:
    """Metadata for a database table."""

    columns: List[ColumnMetadata]
    name: Optional[str] = None
    schema: Optional[str] = None
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    last_modified: Optional[datetime] = None
    partitions: Optional[List[str]] = None
    statistics: Dict[str, Any] = field(default_factory=dict)
    indexes: List[Dict[str, Any]] = field(default_factory=list)
    foreign_keys: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ConnectionConfig:
    """Configuration for a database connection."""

    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    schema_name: Optional[str] = None

    # Connection pool settings
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600

    # Retry settings
    max_retries: int = 3
    retry_delay: float = 1.0
    backoff_factor: float = 2.0

    # Timeout settings
    connect_timeout: int = 30
    query_timeout: int = 300

    # Additional parameters
    extra_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryResult:
    """Result of a database query operation."""

    data: Any  # Usually pandas.DataFrame
    execution_time: float
    rows_affected: int
    query: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConnectionMetrics:
    """Metrics for connection performance tracking."""

    connection_method: str
    source_type: str
    connect_time: float
    success: bool
    error_message: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    query_count: int = 0
    total_query_time: float = 0.0
    bytes_transferred: int = 0


class ComparisonConfig(TypedDict):
    """Configuration for one side of a table comparison."""

    fq_table_name: str
    keys: List[str]


@dataclass
class ComparisonResult:
    """Result of a table comparison operation."""

    match: bool
    row_count: int = 0
    schema_differences: Optional[Dict[str, Any]] = None
    common_columns: Optional[List[str]] = None
    algorithm: Optional[str] = None
    error: Optional[str] = None
