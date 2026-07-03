"""Base class and shared utilities for all DiMer comparison algorithms."""

import hashlib
import re
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Tuple

import structlog

from dimer.core.models import (
    ComparisonConfig,
    DiffRow,
    DiffRun,
    RowStatus,
    TableMetadata,
)

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Pattern for valid SQL identifiers (alphanumeric + underscores, optionally dot-separated and quoted)
_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$')

# Max rows for which we fetch full column values on modified rows (avoids unbounded fetches)
MAX_DETAIL_ROWS = 100

# Max rows fetched per table in cross-database comparison before we warn
CROSS_DB_ROW_LIMIT = 100_000

# Bisection algorithm defaults
BISECTION_DEFAULT_SEGMENTS = 16
BISECTION_DEFAULT_THRESHOLD = 1000

# Max keys per WHERE ... OR ... clause chunk (avoids excessively long SQL)
_WHERE_CHUNK_SIZE = 500

# Sampling algorithm defaults
SAMPLED_DEFAULT_SIZE = 10_000
SAMPLED_DEFAULT_CONFIDENCE = 0.95

# Bloom prefilter defaults
BLOOM_DEFAULT_FPR = 0.01

# Embedding similarity defaults
EMBEDDING_DEFAULT_METRIC = "cosine"
EMBEDDING_DEFAULT_THRESHOLD = 1e-3

# Profile diff defaults
PROFILE_DEFAULT_NUMERIC_TOLERANCE = 1e-6

# Sketch diff defaults — looser than PROFILE_DIFF since values are genuinely
# approximate (HyperLogLog-family error is a few percent by design)
SKETCH_DEFAULT_RELATIVE_TOLERANCE = 0.05


def _supports_sql(connector) -> bool:
    """True when the connector executes SQL (default for all connectors).

    Document-store connectors (e.g. MongoDB) set ``SUPPORTS_SQL = False`` and
    instead expose data-access primitives (``count_rows``, ``fetch_all_rows``,
    ``fetch_rows_by_keys``, ``sample_rows``, ``fetch_key_hashes``) that the
    algorithms call in place of generated SQL.
    """
    return getattr(connector, "SUPPORTS_SQL", True)


def _raw_table(safe_table: str) -> str:
    """Recover the raw dotted table name from a quoted identifier."""
    return safe_table.replace('"', '')


# ---------------------------------------------------------------------------
# SQL identifier helpers
# ---------------------------------------------------------------------------

def _validate_identifier(name: str, case: str = "preserve") -> str:
    """Validate and quote a SQL identifier to prevent injection.

    Args:
        name: The identifier to validate and quote (may be dot-separated).
        case: How to transform each part before quoting:
              'upper' — uppercase (Snowflake convention)
              'lower' — lowercase (PostgreSQL/MySQL convention)
              'preserve' — leave as-is (default)
    """
    stripped = name.replace('"', '')
    if not _IDENTIFIER_RE.match(stripped):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    parts = stripped.split('.')
    if case == "upper":
        parts = [p.upper() for p in parts]
    elif case == "lower":
        parts = [p.lower() for p in parts]
    return '.'.join(f'"{part}"' for part in parts)


def _format_sql_value(val: Any) -> str:
    """Format a Python value for safe embedding in a SQL literal.

    Values come from our own query results (not user input), so this is safe.
    """
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float)):
        return str(val)
    escaped = str(val).replace("'", "''")
    return f"'{escaped}'"


def _get_col_value(row: Dict[str, Any], col: str) -> Any:
    """Case-insensitive column value lookup in a row dict."""
    if col in row:
        return row[col]
    col_lower = col.lower()
    for k, v in row.items():
        if k.lower() == col_lower:
            return v
    return None


# ---------------------------------------------------------------------------
# Hash expression builders
# ---------------------------------------------------------------------------

def _build_hash_expr(connector, col_exprs: List[str]) -> str:
    """Build a row-level SQL hash expression from a list of column expressions.

    Each column expression is first cast to text using the connector's dialect,
    then concatenated, then wrapped in the connector's hash function.
    """
    cast_tmpl = connector.DIALECTS.get("cast_to_text", "CAST({COL} AS VARCHAR)")
    sep = connector.DIALECTS["concatenation"]
    hash_tmpl = connector.DIALECTS["hash"]
    cast_cols = [cast_tmpl.replace("{COL}", col) for col in col_exprs]
    inner = sep.join(cast_cols)
    return hash_tmpl.replace("{COL}", inner)


def _python_row_hash(row: Dict[str, Any], columns: List[str]) -> str:
    """Compute an MD5 hash of a row's values for the given columns.

    Used for cross-database comparison where SQL hashing is not available.
    NULL values are represented as the empty string (known limitation: NULL == '').
    """
    parts = [
        str(_get_col_value(row, col)) if _get_col_value(row, col) is not None else ''
        for col in columns
    ]
    raw = '|'.join(parts)
    return hashlib.md5(raw.encode('utf-8')).hexdigest()


# ---------------------------------------------------------------------------
# BaseAlgorithm
# ---------------------------------------------------------------------------

class BaseAlgorithm(ABC):
    """Abstract base class for all comparison algorithms.

    Subclasses implement ``run()`` with the algorithm-specific logic.
    Shared infrastructure helpers (metadata fetch, row querying, row
    classification) are provided here so concrete classes stay focused.
    """

    _left_connector: Any
    _right_connector: Any
    _left_config: ComparisonConfig
    _right_config: ComparisonConfig

    def __init__(
        self,
        left_connector: Any,
        right_connector: Any,
        left_config: ComparisonConfig,
        right_config: ComparisonConfig,
    ) -> None:
        for key in ('fq_table_name', 'keys'):
            if key not in left_config:
                raise ValueError(f"left_config missing required key: {key!r}")
            if key not in right_config:
                raise ValueError(f"right_config missing required key: {key!r}")
        self._left_connector = left_connector
        self._right_connector = right_connector
        self._left_config = left_config
        self._right_config = right_config

    @abstractmethod
    def run(self) -> DiffRun:
        """Execute the algorithm and return the diff result."""
        ...

    # ------------------------------------------------------------------
    # Schema helpers
    # ------------------------------------------------------------------

    def get_schema_metadata(self, conn, table_name: str) -> Optional[TableMetadata]:
        """Get comprehensive table metadata including columns, types, and constraints."""
        try:
            if '.' in table_name:
                schema, table = table_name.split('.', 1)
                schema = schema.strip('"')
                table = table.strip('"')
            else:
                schema = conn.connection_config.schema_name
                table = table_name.strip('"')

            return conn.get_table_metadata(table, schema)

        except Exception as e:
            logger.error(f"Failed to get schema metadata for {table_name}: {e}", exc_info=True)
            return None

    def compare_schemas(self, metadata_a: TableMetadata, metadata_b: TableMetadata) -> Dict[str, Any]:
        """Compare table schemas and return detailed differences."""
        differences: Dict[str, Any] = {
            'columns_only_in_a': [],
            'columns_only_in_b': [],
            'column_type_differences': [],
            'row_count_difference': None,
            'size_difference': None
        }

        cols_a = {col.name.lower(): col for col in metadata_a.columns}
        cols_b = {col.name.lower(): col for col in metadata_b.columns}

        only_in_a_keys = set(cols_a.keys()) - set(cols_b.keys())
        only_in_b_keys = set(cols_b.keys()) - set(cols_a.keys())
        differences['columns_only_in_a'] = [cols_a[k].name for k in only_in_a_keys]
        differences['columns_only_in_b'] = [cols_b[k].name for k in only_in_b_keys]

        common_columns = set(cols_a.keys()) & set(cols_b.keys())
        for col_name in common_columns:
            col_a = cols_a[col_name]
            col_b = cols_b[col_name]
            if col_a.data_type != col_b.data_type or col_a.nullable != col_b.nullable:
                differences['column_type_differences'].append({
                    'column': col_name,
                    'table_a': {'type': col_a.data_type, 'nullable': col_a.nullable},
                    'table_b': {'type': col_b.data_type, 'nullable': col_b.nullable}
                })

        if metadata_a.row_count is not None and metadata_b.row_count is not None:
            differences['row_count_difference'] = metadata_a.row_count - metadata_b.row_count

        if metadata_a.size_bytes is not None and metadata_b.size_bytes is not None:
            differences['size_difference'] = metadata_a.size_bytes - metadata_b.size_bytes

        return differences

    # ------------------------------------------------------------------
    # Internal SQL execution helpers
    # ------------------------------------------------------------------

    def _count_rows(self, connector, safe_table: str) -> int:
        """Execute COUNT(*) on a table and return the integer result."""
        if not _supports_sql(connector):
            return int(connector.count_rows(_raw_table(safe_table)))
        sql = f"SELECT COUNT(*) AS row_count FROM {safe_table}"
        result = connector.execute_query(sql)
        df = result.data
        if df is None or len(df) == 0:
            return 0
        return int(df.iloc[0, 0])

    def _query_rows(self, connector, sql: str) -> List[Dict[str, Any]]:
        """Execute a query and return results as a list of row dicts."""
        result = connector.execute_query(sql)
        df = result.data
        if df is None or len(df) == 0:
            return []
        return df.to_dict(orient='records')

    def _build_key_where(
        self,
        key_rows: List[Dict[str, Any]],
        key_cols: List[str],
        case: str,
    ) -> str:
        """Build a WHERE clause matching specific key value combinations.

        key_rows are dicts from a prior query; key_cols are the column names
        to look up (may differ in casing from the dict keys).
        """
        conditions = []
        for row in key_rows:
            parts = [
                f'{_validate_identifier(k, case)} = {_format_sql_value(_get_col_value(row, k))}'
                for k in key_cols
            ]
            conditions.append(f'({" AND ".join(parts)})')
        return " OR ".join(conditions)

    def _classify_rows(
        self,
        lookup_a: Dict[tuple, Dict],
        lookup_b: Dict[tuple, Dict],
        keys_a: List[str],
        non_key_cols: List[str],
        common_columns: List[str],
    ) -> List[DiffRow]:
        """Classify rows as ADDED / DELETED / MODIFIED given two key→row lookups.

        Both lookups must use A-side canonical column names.  ``non_key_cols``
        drives hash comparison; ``common_columns`` is stored on modified rows.
        """
        keys_only_in_a = set(lookup_a.keys()) - set(lookup_b.keys())
        keys_only_in_b = set(lookup_b.keys()) - set(lookup_a.keys())
        keys_in_both = set(lookup_a.keys()) & set(lookup_b.keys())

        row_diffs: List[DiffRow] = []

        for key_tuple in keys_only_in_a:
            key_vals = {k: v for k, v in zip(keys_a, key_tuple)}
            row_diffs.append(DiffRow(key_values=key_vals, status=RowStatus.DELETED))

        for key_tuple in keys_only_in_b:
            key_vals = {k: v for k, v in zip(keys_a, key_tuple)}
            row_diffs.append(DiffRow(key_values=key_vals, status=RowStatus.ADDED))

        for key_tuple in keys_in_both:
            row_a = lookup_a[key_tuple]
            row_b = lookup_b[key_tuple]
            if _python_row_hash(row_a, non_key_cols) != _python_row_hash(row_b, non_key_cols):
                key_vals = {k: v for k, v in zip(keys_a, key_tuple)}
                mismatched = [
                    col for col in non_key_cols
                    if str(_get_col_value(row_a, col)) != str(_get_col_value(row_b, col))
                ]
                row_diffs.append(DiffRow(
                    key_values=key_vals,
                    status=RowStatus.MODIFIED,
                    mismatched_columns=mismatched,
                    source_values={c: _get_col_value(row_a, c) for c in common_columns},
                    target_values={c: _get_col_value(row_b, c) for c in common_columns},
                ))

        return row_diffs

    def _fetch_rows_by_keys(
        self,
        connector,
        safe_table: str,
        col_select: str,
        key_dicts: List[Dict[str, Any]],
        key_cols: List[str],
        case: str,
    ) -> List[Dict[str, Any]]:
        """Fetch rows matching a list of key value combinations, chunked to
        avoid generating excessively long OR clauses.

        Executes one query per chunk of ``_WHERE_CHUNK_SIZE`` keys and
        concatenates the results.
        """
        if not _supports_sql(connector):
            columns = [c.strip().strip('"') for c in col_select.split(',')]
            return connector.fetch_rows_by_keys(
                _raw_table(safe_table), columns, key_dicts, key_cols
            )
        all_rows: List[Dict[str, Any]] = []
        for i in range(0, len(key_dicts), _WHERE_CHUNK_SIZE):
            chunk = key_dicts[i:i + _WHERE_CHUNK_SIZE]
            where = self._build_key_where(chunk, key_cols, case)
            all_rows.extend(
                self._query_rows(connector, f"SELECT {col_select} FROM {safe_table} WHERE {where}")
            )
        return all_rows

    def _resolve_common_columns(
        self,
        metadata_a: TableMetadata,
        metadata_b: TableMetadata,
    ) -> Tuple[Optional[Dict[str, Any]], List[str]]:
        """Return (schema_diff, common_columns_list) using metadata."""
        schema_diff = self.compare_schemas(metadata_a, metadata_b)
        if schema_diff['columns_only_in_a']:
            logger.warning(f"Columns only in source: {schema_diff['columns_only_in_a']}")
        if schema_diff['columns_only_in_b']:
            logger.warning(f"Columns only in target: {schema_diff['columns_only_in_b']}")
        if schema_diff['column_type_differences']:
            logger.warning(f"Column type differences: {schema_diff['column_type_differences']}")

        cols_b_lower = {c.name.lower() for c in metadata_b.columns}
        common_columns = [col.name for col in metadata_a.columns if col.name.lower() in cols_b_lower]
        return schema_diff, common_columns

    def _diff_stat_dicts(
        self,
        columns_a: List[str],
        columns_b: List[str],
        stats_a: Dict[str, Dict[str, Any]],
        stats_b: Dict[str, Dict[str, Any]],
        stats_equal: Callable[[str, Any, Any], bool],
    ) -> Tuple[List[DiffRow], int]:
        """Turn two {column_lower: {stat: value}} dicts into per-column DiffRows.

        Shared by PROFILE_DIFF and SKETCH_DIFF: for each column, only stats
        present on **both** sides are compared (a stat missing on one side —
        e.g. a type that doesn't support AVG, or an engine with no native
        median function — is simply not diffable, not an error). A column
        with any differing stat becomes one MODIFIED ``DiffRow`` with
        ``mismatched_columns`` listing the differing *stat names* and
        ``source_values`` / ``target_values`` holding each side's full stat
        dict (including non-compared context, e.g. `*_method` labels).

        Returns (row_diffs, modified_count).
        """
        row_diffs: List[DiffRow] = []
        modified = 0
        for name_a, name_b in zip(columns_a, columns_b):
            sa = stats_a.get(name_a.lower(), {})
            sb = stats_b.get(name_b.lower(), {})
            mismatched = [
                stat for stat in sa.keys() & sb.keys()
                if not stats_equal(stat, sa[stat], sb[stat])
            ]
            if mismatched:
                modified += 1
                row_diffs.append(DiffRow(
                    key_values={"column": name_a},
                    status=RowStatus.MODIFIED,
                    mismatched_columns=sorted(mismatched),
                    source_values=sa,
                    target_values=sb,
                ))
        return row_diffs, modified
