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


class DiffAlgorithm(str, Enum):
    """Algorithm used to compare two tables.

    Inherits from ``str`` so enum members compare equal to their string values
    and can be passed directly wherever a string is expected (f-strings, DB
    inserts, JSON serialisation) without calling ``.value``.
    """

    JOIN_DIFF = "JOIN_DIFF"         # SQL JOIN-based; same-database tables only
    CROSS_DB_DIFF = "CROSS_DB_DIFF" # full fetch + Python hash; cross-database (legacy)
    HASH_DIFF = "HASH_DIFF"         # two-phase: narrow key+hash fetch, then targeted row fetch
    BISECTION = "BISECTION"          # NTILE segment hashing; explicit opt-in


class RowStatus(Enum):
    """Status of a single row in a diff result."""

    ADDED = "added"        # exists in target but not source
    DELETED = "deleted"    # exists in source but not target
    MODIFIED = "modified"  # exists in both but column values differ


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


class BisectionConfig(ComparisonConfig, total=False):
    """Extends ComparisonConfig with optional bisection parameters."""

    bisection_key: str        # sortable column for NTILE bucketing; defaults to keys[0]
    bisection_threshold: int  # rows/segment at which to fall back to row-level diff (default: 1000)
    use_bisection: bool       # explicit opt-in


# ---------------------------------------------------------------------------
# Diff result models  (DiffRow / DiffResult / DiffRun)
# ---------------------------------------------------------------------------


@dataclass
class DiffRow:
    """Diff result for a single row that differs between source and target.

    ``key_values`` is the combination of key column names and their values for
    that row, e.g. ``{"col_a": "val1", "col_b": "val2"}``.  This uniquely
    identifies the row across runs and sources regardless of which side it came
    from.

    ``mismatched_columns`` lists columns with different values between source A
    and B for this row (MODIFIED rows only).

    ``source_values`` / ``target_values`` store the full row from each source
    for MODIFIED rows.  They are only populated when ``save_original_values``
    is enabled on the job, and only for up to ``MAX_DETAIL_ROWS`` rows.
    """

    # Combination of key column names → values; uniquely identifies the row
    # across runs and both sources.  E.g. {"col_a": "val1", "col_b": "val2"}.
    key_values: Dict[str, Any]
    status: RowStatus
    # Columns with differing values between source A and B (MODIFIED rows only)
    mismatched_columns: List[str] = field(default_factory=list)
    # Full row from source A (populated for MODIFIED rows when save_original_values=True)
    source_values: Optional[Dict[str, Any]] = None
    # Full row from source B (populated for MODIFIED rows when save_original_values=True)
    target_values: Optional[Dict[str, Any]] = None


@dataclass
class DiffResult:
    """Aggregate row counts for a diff run."""

    source_row_count: int = 0
    target_row_count: int = 0
    added_count: int = 0      # rows in target not in source
    deleted_count: int = 0    # rows in source not in target
    modified_count: int = 0   # rows in both with value differences
    matched_count: int = 0    # identical rows

    @property
    def total_differences(self) -> int:
        return self.added_count + self.deleted_count + self.modified_count


@dataclass
class DiffRun:
    """
    Complete result of a single diff run.

    Multiple DiffRun records can exist for the same DiffJob (one per
    execution).  Serves as an in-memory store for all comparison output and
    maps to the persistence schema:
      - DiffRun    → diff_run  (metadata) + diff_run_detail (asset stats)
      - DiffResult → diff_result (aggregate counts)
      - DiffRow    → diff_row  (per-row differences)
    """

    match: bool
    summary: Optional[DiffResult] = None
    # Row-level diffs; ADDED/DELETED always fully populated; MODIFIED capped at MAX_DETAIL_ROWS
    row_diffs: List[DiffRow] = field(default_factory=list)
    # Columns only in source, only in target, or with type mismatches
    schema_differences: Optional[Dict[str, Any]] = None
    # Columns present in both assets that were included in the comparison
    common_columns: Optional[List[str]] = None
    algorithm: Optional[DiffAlgorithm] = None
    error: Optional[str] = None
    execution_time_seconds: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None  # algorithm-specific stats (bisection: segment_count, depth_reached, etc.)

    def added_rows(self) -> List[DiffRow]:
        return [r for r in self.row_diffs if r.status == RowStatus.ADDED]

    def deleted_rows(self) -> List[DiffRow]:
        return [r for r in self.row_diffs if r.status == RowStatus.DELETED]

    def modified_rows(self) -> List[DiffRow]:
        return [r for r in self.row_diffs if r.status == RowStatus.MODIFIED]


# ---------------------------------------------------------------------------
# Persistence domain models
# ---------------------------------------------------------------------------


@dataclass
class Project:
    """A DiMer project grouping related diff jobs."""

    name: str
    description: Optional[str] = None
    project_id: Optional[str] = None  # UUID str; set after DB insert


@dataclass
class User:
    """A DiMer user."""

    name: str
    email: Optional[str] = None
    local_cli: bool = False
    user_id: Optional[str] = None  # UUID str; set after DB insert


@dataclass
class ProjectSource:
    """A data source registered within a project.

    Only non-sensitive connection fields are stored here (host, port,
    db_name).  Credentials (username, password, tokens) are never
    persisted — they stay in environment variables / ``.env``.
    """

    project_id: str
    source_type: str       # 'postgresql', 'snowflake', etc.
    source_name: str       # human label unique within the project
    host: Optional[str] = None
    port: Optional[int] = None
    db_name: Optional[str] = None
    user_id: Optional[str] = None
    source_id: Optional[str] = None  # UUID str; set after DB insert


@dataclass
class DiffJob:
    """Configuration for a recurring diff between two assets.

    A job is uniquely identified by the combination of its two sources,
    asset names, and key columns.  The same job record is reused across
    multiple runs so that history accumulates under one job_id.
    """

    project_id: str
    source_a_id: str
    source_a_asset: str    # fully-qualified table name on source A, e.g. 'public.orders'
    source_b_id: str
    source_b_asset: str    # fully-qualified table name on source B, e.g. 'PUBLIC.ORDERS'
    # Columns used as join keys to match rows between the two assets, e.g. ["col_a", "col_b"]
    key_columns: List[str]
    # Maximum number of historical runs to retain for this job before pruning
    snapshot_retention_count: int = 10
    # When True, source_values and target_values are stored in diff_row for MODIFIED rows
    save_original_values: bool = False
    job_id: Optional[str] = None  # UUID str; set after DB insert
