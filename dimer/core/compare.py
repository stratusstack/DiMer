import hashlib
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import structlog

from dimer.core.models import (
    ComparisonConfig,
    DiffAlgorithm,
    DiffRun,
    DiffResult,
    DiffRow,
    RowStatus,
    TableMetadata,
)

logger = structlog.get_logger(__name__)

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


def _build_aggregate_hash_expr(connector, col_exprs: List[str]) -> str:
    """Build a segment-level aggregate hash expression from column expressions.

    Each column is cast to text and concatenated (same as _build_hash_expr),
    then wrapped in the connector's aggregate hash function (e.g. BIT_XOR(MD5(...))).
    Raises NotImplementedError if the connector does not declare 'aggregate_hash'.
    """
    if "aggregate_hash" not in connector.DIALECTS:
        raise NotImplementedError(
            f"Connector {type(connector).__name__} does not support bisection "
            "(DIALECTS missing 'aggregate_hash' key)"
        )
    cast_tmpl = connector.DIALECTS.get("cast_to_text", "CAST({COL} AS VARCHAR)")
    sep = connector.DIALECTS["concatenation"]
    agg_hash_tmpl = connector.DIALECTS["aggregate_hash"]
    cast_cols = [cast_tmpl.replace("{COL}", col) for col in col_exprs]
    inner = sep.join(cast_cols)
    return agg_hash_tmpl.replace("{COL}", inner)


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
# Diffcheck
# ---------------------------------------------------------------------------

class Diffcheck:
    _left_connector: Any
    _right_connector: Any
    _left_config: ComparisonConfig
    _right_config: ComparisonConfig

    def __init__(self, connection1, connection2, db1: ComparisonConfig, db2: ComparisonConfig):
        super().__init__()
        self._left_connector = connection1
        self._right_connector = connection2

        for key in ('fq_table_name', 'keys'):
            if key not in db1:
                raise ValueError(f"db1 missing required key: {key!r}")
            if key not in db2:
                raise ValueError(f"db2 missing required key: {key!r}")

        self._left_config = db1
        self._right_config = db2

    # ------------------------------------------------------------------
    # Schema helpers (unchanged)
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

    def _build_on_clause(
        self,
        keys_a: List[str], keys_b: List[str],
        alias_a: str, alias_b: str,
        case: str,
    ) -> str:
        """Build the JOIN ON clause for key columns."""
        conditions = [
            f'{alias_a}.{_validate_identifier(ka, case)} = {alias_b}.{_validate_identifier(kb, case)}'
            for ka, kb in zip(keys_a, keys_b)
        ]
        return " AND ".join(conditions)

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

    @staticmethod
    def _classify_rows(
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

    def _query_segment_hashes(
        self,
        connector,
        safe_table: str,
        common_columns: List[str],
        bisection_key: str,
        segment_count: int,
        case: str,
    ) -> Dict[int, Dict]:
        """Query COUNT(*) and aggregate hash per NTILE segment.

        Returns ``{bucket_num: {"cnt": int, "seg_hash": value}}``.
        """
        safe_key = _validate_identifier(bisection_key, case)
        col_exprs = [_validate_identifier(c, case) for c in common_columns]
        agg_hash_expr = _build_aggregate_hash_expr(connector, col_exprs)

        sql = (
            f"SELECT bucket, COUNT(*) AS row_count, {agg_hash_expr} AS seg_hash "
            f"FROM ("
            f"SELECT *, NTILE({segment_count}) OVER (ORDER BY {safe_key}) AS bucket "
            f"FROM {safe_table}"
            f") _bisect_inner "
            f"GROUP BY bucket "
            f"ORDER BY bucket"
        )
        rows = self._query_rows(connector, sql)
        return {
            int(_get_col_value(row, 'bucket')): {
                'cnt': int(_get_col_value(row, 'row_count')),
                'seg_hash': _get_col_value(row, 'seg_hash'),
            }
            for row in rows
        }

    def _fetch_bucket_rows(
        self,
        connector,
        safe_table: str,
        bisection_key: str,
        bucket_num: int,
        total_buckets: int,
        col_names: List[str],
        case: str,
    ) -> List[Dict[str, Any]]:
        """Fetch all rows belonging to a specific NTILE bucket."""
        safe_key = _validate_identifier(bisection_key, case)
        col_select = ", ".join(_validate_identifier(c, case) for c in col_names)

        sql = (
            f"SELECT {col_select} "
            f"FROM ("
            f"SELECT *, NTILE({total_buckets}) OVER (ORDER BY {safe_key}) AS _bisect_bucket "
            f"FROM {safe_table}"
            f") _bisect_inner "
            f"WHERE _bisect_bucket = {bucket_num}"
        )
        return self._query_rows(connector, sql)

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

    # ------------------------------------------------------------------
    # Within-database comparison (same host + database)
    # ------------------------------------------------------------------

    def compare_within_database(self) -> DiffRun:
        """Compare two tables on the same database instance using SQL JOINs.

        Uses the left connector for all queries since both tables are accessible
        from the same connection.
        """
        start = time.time()
        conn = self._left_connector
        case = getattr(conn, "IDENTIFIER_CASE", "preserve")

        table_a = self._left_config['fq_table_name']
        table_b = self._right_config['fq_table_name']
        keys_a = self._left_config['keys']
        keys_b = self._right_config['keys']

        if len(keys_a) != len(keys_b):
            return DiffRun(
                match=False,
                error="Key column lists must have equal length",
                algorithm=DiffAlgorithm.JOIN_DIFF,
            )

        # 1. Schema metadata
        logger.info("Fetching schema metadata for both tables")
        metadata_a = self.get_schema_metadata(conn, table_a)
        metadata_b = self.get_schema_metadata(conn, table_b)

        schema_diff: Optional[Dict[str, Any]] = None
        common_columns: List[str] = []

        if metadata_a is not None and metadata_b is not None:
            schema_diff, common_columns = self._resolve_common_columns(metadata_a, metadata_b)
            if not common_columns:
                return DiffRun(
                    match=False,
                    schema_differences=schema_diff,
                    error="No common columns found between tables",
                    algorithm=DiffAlgorithm.JOIN_DIFF,
                )
        else:
            logger.warning("Could not retrieve metadata; schema diff will be skipped")

        safe_a = _validate_identifier(table_a, case)
        safe_b = _validate_identifier(table_b, case)

        # 2. Row counts
        logger.info("Counting rows in both tables")
        count_a = self._count_rows(conn, safe_a)
        count_b = self._count_rows(conn, safe_b)
        logger.info(f"Row counts — source: {count_a}, target: {count_b}")

        on_clause = self._build_on_clause(keys_a, keys_b, "a", "b", case)
        first_key_a = f'a.{_validate_identifier(keys_a[0], case)}'
        first_key_b = f'b.{_validate_identifier(keys_b[0], case)}'
        key_select_a = ", ".join(f'a.{_validate_identifier(k, case)}' for k in keys_a)
        key_select_b = ", ".join(f'b.{_validate_identifier(k, case)}' for k in keys_b)

        # 3. Deleted rows: in source (A) but not in target (B)
        logger.info("Finding deleted rows (in source, not in target)")
        deleted_sql = f"""
            SELECT {key_select_a}
            FROM {safe_a} a
            LEFT JOIN {safe_b} b ON {on_clause}
            WHERE {first_key_b} IS NULL
        """.strip()
        deleted_key_rows = self._query_rows(conn, deleted_sql)
        logger.info(f"Deleted rows: {len(deleted_key_rows)}")

        # 4. Added rows: in target (B) but not in source (A)
        logger.info("Finding added rows (in target, not in source)")
        added_sql = f"""
            SELECT {key_select_b}
            FROM {safe_b} b
            LEFT JOIN {safe_a} a ON {on_clause}
            WHERE {first_key_a} IS NULL
        """.strip()
        added_key_rows = self._query_rows(conn, added_sql)
        logger.info(f"Added rows: {len(added_key_rows)}")

        # 5. Modified rows: in both tables but non-key columns differ
        modified_key_rows: List[Dict[str, Any]] = []
        if common_columns:
            key_set = {k.lower() for k in keys_a}
            non_key_cols = [c for c in common_columns if c.lower() not in key_set]

            if non_key_cols:
                logger.info(f"Finding modified rows (hashing {len(non_key_cols)} non-key columns)")
                col_exprs_a = [f'a.{_validate_identifier(c, case)}' for c in non_key_cols]
                col_exprs_b = [f'b.{_validate_identifier(c, case)}' for c in non_key_cols]
                hash_a = _build_hash_expr(conn, col_exprs_a)
                hash_b = _build_hash_expr(conn, col_exprs_b)

                modified_sql = f"""
                    SELECT {key_select_a}
                    FROM {safe_a} a
                    INNER JOIN {safe_b} b ON {on_clause}
                    WHERE {hash_a} != {hash_b}
                """.strip()
                modified_key_rows = self._query_rows(conn, modified_sql)
                logger.info(f"Modified rows: {len(modified_key_rows)}")
            else:
                logger.info("No non-key columns to compare for modifications")

        # 6. Build DiffRow objects
        row_diffs: List[DiffRow] = []

        for row in deleted_key_rows:
            key_vals = {k: _get_col_value(row, k) for k in keys_a}
            row_diffs.append(DiffRow(key_values=key_vals, status=RowStatus.DELETED))

        for row in added_key_rows:
            key_vals = {k: _get_col_value(row, k) for k in keys_b}
            row_diffs.append(DiffRow(key_values=key_vals, status=RowStatus.ADDED))

        # For modified rows: fetch actual column values (limited to MAX_DETAIL_ROWS)
        detail_rows = modified_key_rows[:MAX_DETAIL_ROWS]
        if detail_rows and common_columns:
            col_select = ", ".join(_validate_identifier(c, case) for c in common_columns)

            where_a = self._build_key_where(detail_rows, keys_a, case)
            fetch_a_sql = f"SELECT {col_select} FROM {safe_a} WHERE {where_a}"
            rows_a = self._query_rows(conn, fetch_a_sql)
            lookup_a = {
                tuple(_get_col_value(r, k) for k in keys_a): r
                for r in rows_a
            }

            where_b = self._build_key_where(
                [{kb: _get_col_value(r, ka) for kb, ka in zip(keys_b, keys_a)} for r in detail_rows],
                keys_b,
                case,
            )
            fetch_b_sql = f"SELECT {col_select} FROM {safe_b} WHERE {where_b}"
            rows_b = self._query_rows(conn, fetch_b_sql)
            lookup_b = {
                tuple(_get_col_value(r, k) for k in keys_b): r
                for r in rows_b
            }

            for key_row in detail_rows:
                key_tuple_a = tuple(_get_col_value(key_row, k) for k in keys_a)
                key_tuple_b = tuple(_get_col_value(key_row, k) for k in keys_a)
                row_a = lookup_a.get(key_tuple_a)
                row_b = lookup_b.get(key_tuple_b)

                key_vals = {k: _get_col_value(key_row, k) for k in keys_a}
                mismatched: List[str] = []
                if row_a and row_b:
                    key_set = {k.lower() for k in keys_a}
                    for col in common_columns:
                        if col.lower() in key_set:
                            continue
                        val_a = _get_col_value(row_a, col)
                        val_b = _get_col_value(row_b, col)
                        if str(val_a) != str(val_b):
                            mismatched.append(col)

                row_diffs.append(DiffRow(
                    key_values=key_vals,
                    status=RowStatus.MODIFIED,
                    mismatched_columns=mismatched,
                    source_values=row_a,
                    target_values=row_b,
                ))

        # Remaining modified rows without column detail
        for key_row in modified_key_rows[MAX_DETAIL_ROWS:]:
            key_vals = {k: _get_col_value(key_row, k) for k in keys_a}
            row_diffs.append(DiffRow(key_values=key_vals, status=RowStatus.MODIFIED))

        matched = max(0, count_a - len(deleted_key_rows) - len(modified_key_rows))
        summary = DiffResult(
            source_row_count=count_a,
            target_row_count=count_b,
            added_count=len(added_key_rows),
            deleted_count=len(deleted_key_rows),
            modified_count=len(modified_key_rows),
            matched_count=matched,
        )

        return DiffRun(
            match=summary.total_differences == 0,
            summary=summary,
            row_diffs=row_diffs,
            schema_differences=schema_diff,
            common_columns=common_columns,
            algorithm=DiffAlgorithm.JOIN_DIFF,
            execution_time_seconds=time.time() - start,
        )

    # ------------------------------------------------------------------
    # Cross-database comparison (different hosts or databases)
    # ------------------------------------------------------------------

    def compare_cross_database(self) -> DiffRun:
        """Compare tables from different database instances.

        Fetches all rows from both tables into memory, computes per-row MD5
        hashes in Python, then identifies added/deleted/modified rows.
        """
        start = time.time()
        case_a = getattr(self._left_connector, "IDENTIFIER_CASE", "preserve")
        case_b = getattr(self._right_connector, "IDENTIFIER_CASE", "preserve")

        table_a = self._left_config['fq_table_name']
        table_b = self._right_config['fq_table_name']
        keys_a = self._left_config['keys']
        keys_b = self._right_config['keys']

        if len(keys_a) != len(keys_b):
            return DiffRun(
                match=False,
                error="Key column lists must have equal length",
                algorithm=DiffAlgorithm.CROSS_DB_DIFF,
            )

        # 1. Schema metadata
        logger.info("Fetching schema metadata for both tables")
        metadata_a = self.get_schema_metadata(self._left_connector, table_a)
        metadata_b = self.get_schema_metadata(self._right_connector, table_b)

        schema_diff: Optional[Dict[str, Any]] = None
        common_columns: List[str] = []
        common_columns_b: List[str] = []  # matching B-side column names

        if metadata_a is not None and metadata_b is not None:
            schema_diff, common_columns = self._resolve_common_columns(metadata_a, metadata_b)
            # Build matching B-side column name list (preserving B casing)
            cols_b_map = {c.name.lower(): c.name for c in metadata_b.columns}
            common_columns_b = [cols_b_map[c.lower()] for c in common_columns]
        else:
            logger.warning("Could not retrieve metadata; proceeding without schema diff")

        if not common_columns:
            return DiffRun(
                match=False,
                schema_differences=schema_diff,
                error="No common columns found between tables",
                algorithm=DiffAlgorithm.CROSS_DB_DIFF,
            )

        safe_a = _validate_identifier(table_a, case_a)
        safe_b = _validate_identifier(table_b, case_b)
        safe_keys_a = ", ".join(_validate_identifier(k, case_a) for k in keys_a)
        safe_keys_b = ", ".join(_validate_identifier(k, case_b) for k in keys_b)
        cols_select_a = ", ".join(_validate_identifier(c, case_a) for c in common_columns)
        cols_select_b = ", ".join(_validate_identifier(c, case_b) for c in common_columns_b)

        # 2. Fetch all rows from both tables (with row limit warning)
        query_a = f"SELECT {cols_select_a} FROM {safe_a} ORDER BY {safe_keys_a}"
        query_b = f"SELECT {cols_select_b} FROM {safe_b} ORDER BY {safe_keys_b}"

        logger.info("Fetching all rows from source table")
        rows_a = self._query_rows(self._left_connector, query_a)
        logger.info("Fetching all rows from target table")
        rows_b = self._query_rows(self._right_connector, query_b)

        count_a = len(rows_a)
        count_b = len(rows_b)
        logger.info(f"Fetched — source: {count_a} rows, target: {count_b} rows")

        if count_a > CROSS_DB_ROW_LIMIT or count_b > CROSS_DB_ROW_LIMIT:
            logger.warning(
                f"Table exceeds {CROSS_DB_ROW_LIMIT:,} rows. "
                "Consider using bisection algorithm for large tables (see TODO_FOR_LATER.md)."
            )

        # Non-key columns for hashing (uses A-side names as canonical)
        key_set_lower = {k.lower() for k in keys_a}
        non_key_cols = [c for c in common_columns if c.lower() not in key_set_lower]

        # 3. Build key → {hash, row} lookup for both sides
        # Key tuple uses A-side column names for both (values are the data)
        def _make_key(row: Dict, key_cols: List[str]) -> tuple:
            return tuple(_get_col_value(row, k) for k in key_cols)

        lookup_a: Dict[tuple, Dict] = {}
        for row in rows_a:
            k = _make_key(row, keys_a)
            lookup_a[k] = row

        # For B rows, remap column names to A-side canonical names
        lookup_b: Dict[tuple, Dict] = {}
        for row in rows_b:
            k = _make_key(row, keys_b)
            # Remap B column names → A column names for uniform comparison
            remapped = {
                col_a: _get_col_value(row, col_b)
                for col_a, col_b in zip(common_columns, common_columns_b)
            }
            lookup_b[k] = remapped

        keys_only_in_a = set(lookup_a.keys()) - set(lookup_b.keys())
        keys_only_in_b = set(lookup_b.keys()) - set(lookup_a.keys())
        keys_in_both = set(lookup_a.keys()) & set(lookup_b.keys())

        # 4. Classify rows
        row_diffs: List[DiffRow] = []

        # Deleted: in A not in B
        for key_tuple in keys_only_in_a:
            key_vals = {k: v for k, v in zip(keys_a, key_tuple)}
            row_diffs.append(DiffRow(key_values=key_vals, status=RowStatus.DELETED))

        # Added: in B not in A
        for key_tuple in keys_only_in_b:
            key_vals = {k: v for k, v in zip(keys_b, key_tuple)}
            row_diffs.append(DiffRow(key_values=key_vals, status=RowStatus.ADDED))

        # Modified: in both, compare hashes on non-key columns
        modified_keys: List[tuple] = []
        for key_tuple in keys_in_both:
            row_a = lookup_a[key_tuple]
            row_b = lookup_b[key_tuple]
            hash_a = _python_row_hash(row_a, non_key_cols)
            hash_b = _python_row_hash(row_b, non_key_cols)
            if hash_a != hash_b:
                modified_keys.append(key_tuple)

        # Column-level detail for modified rows (limited to MAX_DETAIL_ROWS)
        for key_tuple in modified_keys[:MAX_DETAIL_ROWS]:
            row_a = lookup_a[key_tuple]
            row_b = lookup_b[key_tuple]
            key_vals = {k: v for k, v in zip(keys_a, key_tuple)}

            mismatched: List[str] = []
            for col in non_key_cols:
                val_a = _get_col_value(row_a, col)
                val_b = _get_col_value(row_b, col)
                if str(val_a) != str(val_b):
                    mismatched.append(col)

            row_diffs.append(DiffRow(
                key_values=key_vals,
                status=RowStatus.MODIFIED,
                mismatched_columns=mismatched,
                source_values={c: _get_col_value(row_a, c) for c in common_columns},
                target_values={c: _get_col_value(row_b, c) for c in common_columns},
            ))

        # Remaining modified rows without column detail
        for key_tuple in modified_keys[MAX_DETAIL_ROWS:]:
            key_vals = {k: v for k, v in zip(keys_a, key_tuple)}
            row_diffs.append(DiffRow(key_values=key_vals, status=RowStatus.MODIFIED))

        matched = len(keys_in_both) - len(modified_keys)
        summary = DiffResult(
            source_row_count=count_a,
            target_row_count=count_b,
            added_count=len(keys_only_in_b),
            deleted_count=len(keys_only_in_a),
            modified_count=len(modified_keys),
            matched_count=max(0, matched),
        )

        return DiffRun(
            match=summary.total_differences == 0,
            summary=summary,
            row_diffs=row_diffs,
            schema_differences=schema_diff,
            common_columns=common_columns,
            algorithm=DiffAlgorithm.CROSS_DB_DIFF,
            execution_time_seconds=time.time() - start,
        )

    # ------------------------------------------------------------------
    # Hash-diff comparison (default for cross-database)
    # ------------------------------------------------------------------

    def compare_hash_diff(self) -> DiffRun:
        """Two-phase cross-database diff using per-row hashes.

        Phase 1 — narrow fetch: queries ``SELECT <keys>, <hash(non_key_cols)>``
        from each DB.  This transfers exactly two logical columns per row
        regardless of table width, and immediately identifies ADDED and
        DELETED rows via set operations.

        Phase 2 — targeted fetch: retrieves non-key column values only for
        rows that require further inspection:

        * **Same DB type** (e.g. PostgreSQL ↔ PostgreSQL): hashes are
          produced by the same function, so rows with matching hashes are
          provably identical and skipped.  Only hash-differing rows are
          fetched for column-level detail.
        * **Different DB types** (e.g. PostgreSQL ↔ Snowflake): hash
          functions differ, so direct comparison is not valid.  Non-key
          columns are fetched for all common-key rows, but ADDED/DELETED
          rows are never re-fetched.

        Fetches are chunked into ``_WHERE_CHUNK_SIZE``-key batches to keep
        individual SQL statements within safe length limits.
        """
        start = time.time()
        case_a = getattr(self._left_connector, "IDENTIFIER_CASE", "preserve")
        case_b = getattr(self._right_connector, "IDENTIFIER_CASE", "preserve")

        table_a = self._left_config['fq_table_name']
        table_b = self._right_config['fq_table_name']
        keys_a = self._left_config['keys']
        keys_b = self._right_config['keys']

        if len(keys_a) != len(keys_b):
            return DiffRun(
                match=False,
                error="Key column lists must have equal length",
                algorithm=DiffAlgorithm.HASH_DIFF,
            )

        # Schema metadata and common columns
        logger.info("Fetching schema metadata for both tables")
        metadata_a = self.get_schema_metadata(self._left_connector, table_a)
        metadata_b = self.get_schema_metadata(self._right_connector, table_b)

        schema_diff: Optional[Dict[str, Any]] = None
        common_columns: List[str] = []
        common_columns_b: List[str] = []

        if metadata_a is not None and metadata_b is not None:
            schema_diff, common_columns = self._resolve_common_columns(metadata_a, metadata_b)
            cols_b_map = {c.name.lower(): c.name for c in metadata_b.columns}
            common_columns_b = [cols_b_map[c.lower()] for c in common_columns]
        else:
            logger.warning("Could not retrieve metadata; proceeding without schema diff")

        if not common_columns:
            return DiffRun(
                match=False,
                schema_differences=schema_diff,
                error="No common columns found between tables",
                algorithm=DiffAlgorithm.HASH_DIFF,
            )

        safe_a = _validate_identifier(table_a, case_a)
        safe_b = _validate_identifier(table_b, case_b)

        key_set_lower = {k.lower() for k in keys_a}
        non_key_cols = [c for c in common_columns if c.lower() not in key_set_lower]
        non_key_cols_b = [
            c for c in common_columns_b
            if c.lower() not in {k.lower() for k in keys_b}
        ]

        # ----------------------------------------------------------------
        # Phase 1: narrow fetch — key columns + one hash per row
        # ----------------------------------------------------------------
        key_select_a = ", ".join(_validate_identifier(k, case_a) for k in keys_a)
        key_select_b = ", ".join(_validate_identifier(k, case_b) for k in keys_b)

        if non_key_cols:
            hash_expr_a = _build_hash_expr(
                self._left_connector,
                [_validate_identifier(c, case_a) for c in non_key_cols],
            )
            hash_expr_b = _build_hash_expr(
                self._right_connector,
                [_validate_identifier(c, case_b) for c in non_key_cols_b],
            )
            phase1_sql_a = (
                f"SELECT {key_select_a}, {hash_expr_a} AS _dimer_row_hash FROM {safe_a}"
            )
            phase1_sql_b = (
                f"SELECT {key_select_b}, {hash_expr_b} AS _dimer_row_hash FROM {safe_b}"
            )
        else:
            # No non-key columns — keys are the whole row; just fetch keys
            phase1_sql_a = f"SELECT {key_select_a} FROM {safe_a}"
            phase1_sql_b = f"SELECT {key_select_b} FROM {safe_b}"

        logger.info("Phase 1: fetching key + row hash from both sources")
        rows_p1_a = self._query_rows(self._left_connector, phase1_sql_a)
        rows_p1_b = self._query_rows(self._right_connector, phase1_sql_b)

        count_a = len(rows_p1_a)
        count_b = len(rows_p1_b)
        logger.info(f"Phase 1 complete — source: {count_a} rows, target: {count_b} rows")

        if count_a > CROSS_DB_ROW_LIMIT or count_b > CROSS_DB_ROW_LIMIT:
            logger.warning(
                f"Table exceeds {CROSS_DB_ROW_LIMIT:,} rows. "
                "Consider using BISECTION algorithm for large tables."
            )

        # Build key → hash lookups
        hash_lookup_a: Dict[tuple, Any] = {
            tuple(_get_col_value(r, k) for k in keys_a): _get_col_value(r, '_dimer_row_hash')
            for r in rows_p1_a
        }
        hash_lookup_b: Dict[tuple, Any] = {
            tuple(_get_col_value(r, k) for k in keys_b): _get_col_value(r, '_dimer_row_hash')
            for r in rows_p1_b
        }

        # ----------------------------------------------------------------
        # Set operations — ADDED and DELETED need no further fetching
        # ----------------------------------------------------------------
        keys_only_in_a = set(hash_lookup_a.keys()) - set(hash_lookup_b.keys())
        keys_only_in_b = set(hash_lookup_b.keys()) - set(hash_lookup_a.keys())
        keys_in_both = set(hash_lookup_a.keys()) & set(hash_lookup_b.keys())

        row_diffs: List[DiffRow] = []

        for key_tuple in keys_only_in_a:
            key_vals = {k: v for k, v in zip(keys_a, key_tuple)}
            row_diffs.append(DiffRow(key_values=key_vals, status=RowStatus.DELETED))

        for key_tuple in keys_only_in_b:
            key_vals = {k: v for k, v in zip(keys_b, key_tuple)}
            row_diffs.append(DiffRow(key_values=key_vals, status=RowStatus.ADDED))

        # Short-circuit when there are no common rows or no non-key columns
        if not keys_in_both or not non_key_cols:
            matched_count = len(keys_in_both)
            summary = DiffResult(
                source_row_count=count_a,
                target_row_count=count_b,
                added_count=len(keys_only_in_b),
                deleted_count=len(keys_only_in_a),
                modified_count=0,
                matched_count=matched_count,
            )
            return DiffRun(
                match=summary.total_differences == 0,
                summary=summary,
                row_diffs=row_diffs,
                schema_differences=schema_diff,
                common_columns=common_columns,
                algorithm=DiffAlgorithm.HASH_DIFF,
                execution_time_seconds=time.time() - start,
            )

        # ----------------------------------------------------------------
        # Determine modification candidates
        # ----------------------------------------------------------------
        same_db_type = type(self._left_connector) is type(self._right_connector)

        if same_db_type:
            # Hashes are produced by the same function — directly comparable
            candidates = [
                k for k in keys_in_both
                if str(hash_lookup_a[k]) != str(hash_lookup_b[k])
            ]
            matched_count = len(keys_in_both) - len(candidates)
            logger.info(
                f"Phase 1 hash comparison (same DB type): "
                f"{matched_count} identical, {len(candidates)} modified candidates"
            )
        else:
            # Different hash functions — cannot compare across DB types
            candidates = list(keys_in_both)
            matched_count = 0  # recalculated after Phase 2
            logger.info(
                f"Phase 2 required (different DB types): "
                f"fetching non-key columns for {len(candidates)} common rows"
            )

        # Short-circuit when all common rows are confirmed identical
        if not candidates:
            summary = DiffResult(
                source_row_count=count_a,
                target_row_count=count_b,
                added_count=len(keys_only_in_b),
                deleted_count=len(keys_only_in_a),
                modified_count=0,
                matched_count=matched_count,
            )
            return DiffRun(
                match=summary.total_differences == 0,
                summary=summary,
                row_diffs=row_diffs,
                schema_differences=schema_diff,
                common_columns=common_columns,
                algorithm=DiffAlgorithm.HASH_DIFF,
                execution_time_seconds=time.time() - start,
            )

        # ----------------------------------------------------------------
        # Phase 2: targeted fetch of non-key columns for candidates only
        # ----------------------------------------------------------------
        col_select_a = ", ".join(_validate_identifier(c, case_a) for c in common_columns)
        col_select_b = ", ".join(_validate_identifier(c, case_b) for c in common_columns_b)

        candidate_dicts_a = [{k: v for k, v in zip(keys_a, kt)} for kt in candidates]
        candidate_dicts_b = [{k: v for k, v in zip(keys_b, kt)} for kt in candidates]

        logger.info(
            f"Phase 2: fetching full columns for {len(candidates)} candidates "
            f"(chunked into {(len(candidates) - 1) // _WHERE_CHUNK_SIZE + 1} queries per side)"
        )
        fetched_a = self._fetch_rows_by_keys(
            self._left_connector, safe_a, col_select_a, candidate_dicts_a, keys_a, case_a
        )
        fetched_b = self._fetch_rows_by_keys(
            self._right_connector, safe_b, col_select_b, candidate_dicts_b, keys_b, case_b
        )

        # Remap B rows to A-side canonical column names
        rows_b_remapped = [
            {col_a: _get_col_value(row, col_b)
             for col_a, col_b in zip(common_columns, common_columns_b)}
            for row in fetched_b
        ]

        lookup_a = {tuple(_get_col_value(r, k) for k in keys_a): r for r in fetched_a}
        lookup_b = {tuple(_get_col_value(r, k) for k in keys_a): r for r in rows_b_remapped}

        if same_db_type:
            # All candidates are confirmed modified — classify for detail only
            for key_tuple in candidates[:MAX_DETAIL_ROWS]:
                row_a = lookup_a.get(key_tuple)
                row_b = lookup_b.get(key_tuple)
                key_vals = {k: v for k, v in zip(keys_a, key_tuple)}
                mismatched: List[str] = []
                if row_a and row_b:
                    mismatched = [
                        col for col in non_key_cols
                        if str(_get_col_value(row_a, col)) != str(_get_col_value(row_b, col))
                    ]
                row_diffs.append(DiffRow(
                    key_values=key_vals,
                    status=RowStatus.MODIFIED,
                    mismatched_columns=mismatched,
                    source_values=row_a,
                    target_values=row_b,
                ))
            for key_tuple in candidates[MAX_DETAIL_ROWS:]:
                key_vals = {k: v for k, v in zip(keys_a, key_tuple)}
                row_diffs.append(DiffRow(key_values=key_vals, status=RowStatus.MODIFIED))
            modified_count = len(candidates)
        else:
            # Use Python hashing to determine which common rows actually differ
            diffs = self._classify_rows(lookup_a, lookup_b, keys_a, non_key_cols, common_columns)
            row_diffs.extend(diffs)
            modified_count = sum(1 for d in diffs if d.status == RowStatus.MODIFIED)
            matched_count = len(candidates) - modified_count

        added_count = sum(1 for r in row_diffs if r.status == RowStatus.ADDED)
        deleted_count = sum(1 for r in row_diffs if r.status == RowStatus.DELETED)

        summary = DiffResult(
            source_row_count=count_a,
            target_row_count=count_b,
            added_count=added_count,
            deleted_count=deleted_count,
            modified_count=modified_count,
            matched_count=max(0, matched_count),
        )

        return DiffRun(
            match=summary.total_differences == 0,
            summary=summary,
            row_diffs=row_diffs,
            schema_differences=schema_diff,
            common_columns=common_columns,
            algorithm=DiffAlgorithm.HASH_DIFF,
            execution_time_seconds=time.time() - start,
        )

    # ------------------------------------------------------------------
    # Bisection comparison (explicit opt-in via use_bisection=True)
    # ------------------------------------------------------------------

    def compare_bisection(self) -> DiffRun:
        """Compare tables using the NTILE bisection algorithm.

        Divides each table into ``BISECTION_DEFAULT_SEGMENTS`` buckets ordered
        by ``bisection_key``, computes an aggregate hash per bucket, and only
        fetches rows for buckets where hashes differ.  For each differing bucket
        the rows are fetched from both sides and classified in-memory.
        """
        start = time.time()

        bisection_key = (
            self._left_config.get('bisection_key')  # type: ignore[attr-defined]
            or self._left_config['keys'][0]
        )
        threshold = self._left_config.get('bisection_threshold', BISECTION_DEFAULT_THRESHOLD)  # type: ignore[attr-defined]
        segment_count = BISECTION_DEFAULT_SEGMENTS

        case_a = getattr(self._left_connector, "IDENTIFIER_CASE", "preserve")
        case_b = getattr(self._right_connector, "IDENTIFIER_CASE", "preserve")

        table_a = self._left_config['fq_table_name']
        table_b = self._right_config['fq_table_name']
        keys_a = self._left_config['keys']
        keys_b = self._right_config['keys']

        if len(keys_a) != len(keys_b):
            return DiffRun(
                match=False,
                error="Key column lists must have equal length",
                algorithm=DiffAlgorithm.BISECTION,
            )

        # Warn if bisection_key is not a join key (NTILE ties → non-deterministic)
        if bisection_key.lower() not in {k.lower() for k in keys_a}:
            logger.warning(
                f"bisection_key '{bisection_key}' is not a key column; "
                "NTILE ties may produce non-deterministic bucket assignments"
            )

        # Schema metadata and common columns
        logger.info("Fetching schema metadata for both tables")
        metadata_a = self.get_schema_metadata(self._left_connector, table_a)
        metadata_b = self.get_schema_metadata(self._right_connector, table_b)

        schema_diff: Optional[Dict[str, Any]] = None
        common_columns: List[str] = []
        common_columns_b: List[str] = []

        if metadata_a is not None and metadata_b is not None:
            schema_diff, common_columns = self._resolve_common_columns(metadata_a, metadata_b)
            cols_b_map = {c.name.lower(): c.name for c in metadata_b.columns}
            common_columns_b = [cols_b_map[c.lower()] for c in common_columns]
        else:
            logger.warning("Could not retrieve metadata; proceeding without schema diff")

        if not common_columns:
            return DiffRun(
                match=False,
                schema_differences=schema_diff,
                error="No common columns found between tables",
                algorithm=DiffAlgorithm.BISECTION,
            )

        safe_a = _validate_identifier(table_a, case_a)
        safe_b = _validate_identifier(table_b, case_b)

        # Row counts
        count_a = self._count_rows(self._left_connector, safe_a)
        count_b = self._count_rows(self._right_connector, safe_b)
        logger.info(f"Row counts — source: {count_a}, target: {count_b}")

        # Segment hashes for both sides
        logger.info(f"Querying segment hashes ({segment_count} buckets, bisection_key={bisection_key!r})")
        segs_a = self._query_segment_hashes(
            self._left_connector, safe_a, common_columns, bisection_key, segment_count, case_a
        )
        segs_b = self._query_segment_hashes(
            self._right_connector, safe_b, common_columns, bisection_key, segment_count, case_b
        )

        all_buckets = set(segs_a.keys()) | set(segs_b.keys())
        differing_buckets = [
            b for b in all_buckets
            if b not in segs_a
            or b not in segs_b
            or str(segs_a[b]['seg_hash']) != str(segs_b[b]['seg_hash'])
        ]

        logger.info(
            f"Segments — total: {len(all_buckets)}, differing: {len(differing_buckets)}"
        )

        if not differing_buckets:
            return DiffRun(
                match=True,
                summary=DiffResult(
                    source_row_count=count_a,
                    target_row_count=count_b,
                    matched_count=count_a,
                ),
                common_columns=common_columns,
                schema_differences=schema_diff,
                algorithm=DiffAlgorithm.BISECTION,
                metadata={
                    "segment_count": segment_count,
                    "depth_reached": 0,
                    "segments_compared": len(all_buckets),
                    "segments_differing": 0,
                },
                execution_time_seconds=time.time() - start,
            )

        # Non-key columns for hash comparison
        key_set_lower = {k.lower() for k in keys_a}
        non_key_cols = [c for c in common_columns if c.lower() not in key_set_lower]

        # Determine the B-side bisection key name (same index in keys_b if it's a key col)
        bisection_key_b = bisection_key
        for ka, kb in zip(keys_a, keys_b):
            if ka.lower() == bisection_key.lower():
                bisection_key_b = kb
                break

        # Process each differing bucket
        row_diffs: List[DiffRow] = []
        depth_reached = 1

        for bucket_num in sorted(differing_buckets):
            bucket_cnt_a = segs_a.get(bucket_num, {}).get('cnt', 0)
            bucket_cnt_b = segs_b.get(bucket_num, {}).get('cnt', 0)
            max_cnt = max(bucket_cnt_a, bucket_cnt_b)

            if max_cnt > threshold:
                logger.warning(
                    f"Bucket {bucket_num} has {max_cnt} rows (> threshold {threshold}); "
                    "fetching all rows for in-memory comparison"
                )

            logger.debug(f"Fetching rows for differing bucket {bucket_num} (≤{max_cnt} rows per side)")

            rows_a_raw = self._fetch_bucket_rows(
                self._left_connector, safe_a, bisection_key, bucket_num, segment_count, common_columns, case_a
            )
            rows_b_raw = self._fetch_bucket_rows(
                self._right_connector, safe_b, bisection_key_b, bucket_num, segment_count, common_columns_b, case_b
            )

            # Remap B rows to A-side canonical column names
            rows_b_remapped = [
                {col_a: _get_col_value(row, col_b) for col_a, col_b in zip(common_columns, common_columns_b)}
                for row in rows_b_raw
            ]

            lookup_a = {tuple(_get_col_value(r, k) for k in keys_a): r for r in rows_a_raw}
            lookup_b = {tuple(_get_col_value(r, k) for k in keys_a): r for r in rows_b_remapped}

            bucket_diffs = self._classify_rows(lookup_a, lookup_b, keys_a, non_key_cols, common_columns)
            row_diffs.extend(bucket_diffs)

        added_count = sum(1 for r in row_diffs if r.status == RowStatus.ADDED)
        deleted_count = sum(1 for r in row_diffs if r.status == RowStatus.DELETED)
        modified_count = sum(1 for r in row_diffs if r.status == RowStatus.MODIFIED)
        matched_count = max(0, count_a - deleted_count - modified_count)

        summary = DiffResult(
            source_row_count=count_a,
            target_row_count=count_b,
            added_count=added_count,
            deleted_count=deleted_count,
            modified_count=modified_count,
            matched_count=matched_count,
        )

        return DiffRun(
            match=summary.total_differences == 0,
            summary=summary,
            row_diffs=row_diffs,
            schema_differences=schema_diff,
            common_columns=common_columns,
            algorithm=DiffAlgorithm.BISECTION,
            metadata={
                "segment_count": segment_count,
                "depth_reached": depth_reached,
                "segments_compared": len(all_buckets),
                "segments_differing": len(differing_buckets),
            },
            execution_time_seconds=time.time() - start,
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def compare(self) -> DiffRun:
        """Choose the appropriate comparison strategy and run the diff."""
        logger.info("Starting table comparison")

        # Bisection is an explicit opt-in (checked before instance routing)
        if self._left_config.get('use_bisection') or self._right_config.get('use_bisection'):  # type: ignore[attr-defined]
            logger.info("Bisection algorithm selected — using NTILE segment comparison")
            return self.compare_bisection()

        same_instance = (
            self._left_connector.connection_config.host == self._right_connector.connection_config.host
            and self._left_connector.connection_config.database == self._right_connector.connection_config.database
        )

        if same_instance:
            logger.info("Same database instance — using JOIN-based comparison")
            return self.compare_within_database()
        else:
            logger.info("Different database instances — using hash-diff comparison")
            return self.compare_hash_diff()

    # ------------------------------------------------------------------
    # Standalone schema check (unchanged)
    # ------------------------------------------------------------------

    def check_schema(self, table_a: str, table_b: str) -> bool:
        """Detailed schema comparison between two tables. Returns True if schemas match."""
        logger.info("Starting detailed schema comparison")

        metadata_a = self.get_schema_metadata(self._left_connector, table_a)
        metadata_b = self.get_schema_metadata(self._right_connector, table_b)

        if metadata_a is None or metadata_b is None:
            logger.error("Could not retrieve metadata for schema comparison")
            return False

        differences = self.compare_schemas(metadata_a, metadata_b)
        logger.info(f"Table A ({table_a}): {len(metadata_a.columns)} columns, {metadata_a.row_count} rows")
        logger.info(f"Table B ({table_b}): {len(metadata_b.columns)} columns, {metadata_b.row_count} rows")

        if differences['columns_only_in_a']:
            logger.info(f"Columns only in A: {differences['columns_only_in_a']}")
        if differences['columns_only_in_b']:
            logger.info(f"Columns only in B: {differences['columns_only_in_b']}")
        if differences['column_type_differences']:
            logger.info(f"Type differences: {differences['column_type_differences']}")

        return (
            len(differences['columns_only_in_a']) == 0
            and len(differences['columns_only_in_b']) == 0
        )
