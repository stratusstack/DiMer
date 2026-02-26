import re
from typing import Any, Dict, List, Optional

import structlog

from dimer.core.models import (
    ColumnMetadata,
    ComparisonConfig,
    ComparisonResult,
    TableMetadata,
)

logger = structlog.get_logger(__name__)

# Pattern for valid SQL identifiers (alphanumeric + underscores, optionally dot-separated and quoted)
_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$')


def _validate_identifier(name: str) -> str:
    """Validate and quote a SQL identifier to prevent injection."""
    # Strip existing quotes for validation
    stripped = name.replace('"', '')
    if not _IDENTIFIER_RE.match(stripped):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    # Quote each part of a dot-separated identifier
    parts = stripped.split('.')
    return '.'.join(f'"{part}"' for part in parts)


class Diffcheck():
    _left_connector: Any
    _right_connector: Any
    _left_config: ComparisonConfig
    _right_config: ComparisonConfig

    def __init__(self, connection1, connection2, db1: ComparisonConfig, db2: ComparisonConfig):
        super().__init__()
        self._left_connector = connection1
        self._right_connector = connection2

        # Validate required keys in configs
        for key in ('fq_table_name', 'keys'):
            if key not in db1:
                raise ValueError(f"db1 missing required key: {key!r}")
            if key not in db2:
                raise ValueError(f"db2 missing required key: {key!r}")

        self._left_config = db1
        self._right_config = db2

    def get_column_list(self, conn, table_name: str) -> List[str]:
        """
        Get column list using fall back to query method if metadata fails.
        """
        safe_table = _validate_identifier(table_name)
        query = f"SELECT * FROM {safe_table} LIMIT 0"
        cur = conn.connection.cursor()
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        return columns

    def check_cols(self, column_list_a: List[str], column_list_b: List[str]) -> bool:
        """Check if column lists match exactly."""
        return column_list_a == column_list_b

    def get_schema_metadata(self, conn, table_name: str) -> Optional[TableMetadata]:
        """
        Get comprehensive table metadata including columns, types, and constraints.
        """
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
        """
        Compare table schemas and return detailed differences.
        """
        differences: Dict[str, Any] = {
            'columns_only_in_a': [],
            'columns_only_in_b': [],
            'column_type_differences': [],
            'row_count_difference': None,
            'size_difference': None
        }

        # Create column dictionaries for comparison
        cols_a = {col.name: col for col in metadata_a.columns}
        cols_b = {col.name: col for col in metadata_b.columns}

        # Find columns only in each table
        differences['columns_only_in_a'] = list(set(cols_a.keys()) - set(cols_b.keys()))
        differences['columns_only_in_b'] = list(set(cols_b.keys()) - set(cols_a.keys()))

        # Check data type differences for common columns
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

        # Compare row counts and sizes
        if metadata_a.row_count is not None and metadata_b.row_count is not None:
            differences['row_count_difference'] = metadata_a.row_count - metadata_b.row_count

        if metadata_a.size_bytes is not None and metadata_b.size_bytes is not None:
            differences['size_difference'] = metadata_a.size_bytes - metadata_b.size_bytes

        return differences

    def compare_within_database(self, algorithm: str) -> ComparisonResult:

        keys_a = self._left_config['keys']
        keys_b = self._right_config['keys']
        table_a = self._left_config['fq_table_name']
        table_b = self._right_config['fq_table_name']

        # Enhanced schema checking using metadata
        logger.info("Starting schema comparison...")
        metadata_a = self.get_schema_metadata(self._left_connector, table_a)
        metadata_b = self.get_schema_metadata(self._right_connector, table_b)

        schema_diff = None
        common_columns: Optional[List[str]] = None

        if metadata_a is None or metadata_b is None:
            logger.warning("Could not retrieve metadata, falling back to simple column check")
            # Fallback to original column check
            column_list_a = self.get_column_list(self._left_connector, table_a)
            column_list_b = self.get_column_list(self._right_connector, table_b)

            if not self.check_cols(column_list_a, column_list_b):
                raise ValueError("Column list mismatches")
        else:
            # Use enhanced schema comparison
            schema_diff = self.compare_schemas(metadata_a, metadata_b)

            # Log schema differences
            if schema_diff['columns_only_in_a']:
                logger.warning(f"Columns only in table A: {schema_diff['columns_only_in_a']}")
            if schema_diff['columns_only_in_b']:
                logger.warning(f"Columns only in table B: {schema_diff['columns_only_in_b']}")
            if schema_diff['column_type_differences']:
                logger.warning(f"Column type differences: {schema_diff['column_type_differences']}")

            # For data comparison, use only common columns (set-based lookup)
            cols_b_names = {c.name for c in metadata_b.columns}
            common_columns = [col.name for col in metadata_a.columns
                            if col.name in cols_b_names]

            if not common_columns:
                raise ValueError("No common columns found between tables")

            logger.info(f"Using {len(common_columns)} common columns for comparison")
            column_list_a = column_list_b = common_columns

        if algorithm == "JOIN_DIFF":
            join_q = self.construct_join(keys_a, keys_b, table_a, table_b, column_list_a, column_list_b, "LEFT")
            cur = self._left_connector.connection.cursor()
            cur.execute(join_q)
            row_count = cur.rowcount
            match = row_count == 0
            if match:
                logger.info(f"Segment equal - ROW_COUNT: {row_count}")
            else:
                logger.info(f"Segment not equal - ROW_COUNT:{row_count}")

            return ComparisonResult(
                match=match,
                row_count=row_count,
                schema_differences=schema_diff,
                common_columns=common_columns,
                algorithm=algorithm,
            )

        return ComparisonResult(
            match=False,
            error=f"Unsupported algorithm: {algorithm}",
            algorithm=algorithm,
        )

    def check_schema(self, table_a: str, table_b: str) -> bool:
        """
        Detailed schema comparison between two tables.
        """
        logger.info("Starting detailed schema comparison")

        metadata_a = self.get_schema_metadata(self._left_connector, table_a)
        metadata_b = self.get_schema_metadata(self._right_connector, table_b)

        if metadata_a is None or metadata_b is None:
            logger.error("Could not retrieve metadata for schema comparison")
            return False

        differences = self.compare_schemas(metadata_a, metadata_b)

        # Print detailed comparison results
        logger.info(f"Schema comparison results:")
        logger.info(f"Table A ({table_a}): {len(metadata_a.columns)} columns, {metadata_a.row_count} rows")
        logger.info(f"Table B ({table_b}): {len(metadata_b.columns)} columns, {metadata_b.row_count} rows")

        if differences['columns_only_in_a']:
            logger.info(f"Columns only in A: {differences['columns_only_in_a']}")
        if differences['columns_only_in_b']:
            logger.info(f"Columns only in B: {differences['columns_only_in_b']}")
        if differences['column_type_differences']:
            logger.info(f"Type differences: {differences['column_type_differences']}")

        # Return True if schemas are compatible (same columns, possibly different types)
        return len(differences['columns_only_in_a']) == 0 and len(differences['columns_only_in_b']) == 0

    def compare_cross_database(self, algorithm: str = "JOIN_DIFF") -> ComparisonResult:
        """
        Compare tables from different databases/instances.
        This method handles cross-database comparisons where direct JOIN is not possible.
        """
        logger.info("Starting cross-database table comparison")

        keys_a = self._left_config['keys']
        keys_b = self._right_config['keys']
        table_a = self._left_config['fq_table_name']
        table_b = self._right_config['fq_table_name']

        # Get metadata for both tables
        metadata_a = self.get_schema_metadata(self._left_connector, table_a)
        metadata_b = self.get_schema_metadata(self._right_connector, table_b)

        if metadata_a is None or metadata_b is None:
            logger.error("Could not retrieve metadata for cross-database comparison")
            return ComparisonResult(
                match=False,
                error="Could not retrieve metadata for cross-database comparison",
            )

        # Compare schemas first
        schema_diff = self.compare_schemas(metadata_a, metadata_b)
        logger.info(f"Schema differences: {schema_diff}")

        # Get common columns for comparison (set-based lookup)
        cols_b_names = {c.name for c in metadata_b.columns}
        common_columns = [col.name for col in metadata_a.columns
                        if col.name in cols_b_names]

        if not common_columns:
            logger.error("No common columns found for cross-database comparison")
            return ComparisonResult(
                match=False,
                schema_differences=schema_diff,
                error="No common columns found",
            )

        logger.info(f"Comparing {len(common_columns)} common columns")

        try:
            # Get sample data from both tables to compare
            sample_size = 1000

            # Build queries with quoted identifiers
            safe_cols = ', '.join(_validate_identifier(c) for c in common_columns)
            safe_keys_a = ', '.join(_validate_identifier(k) for k in keys_a)
            safe_keys_b = ', '.join(_validate_identifier(k) for k in keys_b)
            safe_table_a = _validate_identifier(table_a)
            safe_table_b = _validate_identifier(table_b)

            query_a = f"SELECT {safe_cols} FROM {safe_table_a} ORDER BY {safe_keys_a} LIMIT {sample_size}"
            query_b = f"SELECT {safe_cols} FROM {safe_table_b} ORDER BY {safe_keys_b} LIMIT {sample_size}"

            logger.info("Executing sample queries for comparison")

            result_a = self._left_connector.execute_query(query_a)
            result_b = self._right_connector.execute_query(query_b)

            # Compare the data
            data_a = result_a.data
            data_b = result_b.data

            rows_a = len(data_a)
            rows_b = len(data_b)

            logger.info(f"Retrieved {rows_a} rows from table A and {rows_b} rows from table B")

            match = rows_a == rows_b
            if not match:
                logger.info(f"Row count difference in sample: {rows_a} vs {rows_b}")

            logger.info("Cross-database comparison completed")

            return ComparisonResult(
                match=match,
                row_count=max(rows_a, rows_b),
                schema_differences=schema_diff,
                common_columns=common_columns,
                algorithm="cross_database",
            )

        except Exception as e:
            logger.error(f"Error during cross-database comparison: {e}", exc_info=True)
            return ComparisonResult(
                match=False,
                schema_differences=schema_diff,
                common_columns=common_columns,
                error=str(e),
            )

    def compare(self) -> ComparisonResult:
        """
        General comparison method that chooses appropriate strategy.
        """
        logger.info("Starting table comparison")

        # Check if both connections are to the same database instance
        same_instance = (
            self._left_connector.connection_config.host == self._right_connector.connection_config.host and
            self._left_connector.connection_config.database == self._right_connector.connection_config.database
        )

        if same_instance:
            logger.info("Same database instance detected, using JOIN-based comparison")
            return self.compare_within_database("JOIN_DIFF")
        else:
            logger.info("Different database instances detected, using cross-database comparison")
            return self.compare_cross_database()

    def construct_join(self, keys_a, keys_b, table_a, table_b, column_list_a, column_list_b, join_type="INNER"):
        """
        Build a SQL JOIN statement.
        join_type: INNER, LEFT, RIGHT, FULL
        """

        concatenation_dialect_a = self._left_connector.DIALECTS["concatenation"]
        concatenation_dialect_b = self._right_connector.DIALECTS["concatenation"]
        alias_a = "tbl1"
        alias_b = "tbl2"

        if len(keys_a) != len(keys_b):
            raise ValueError("Join keys must have the same length")

        # Validate and quote all identifiers
        safe_table_a = _validate_identifier(table_a)
        safe_table_b = _validate_identifier(table_b)

        # Build ON clause with quoted identifiers
        on_conditions = [
            f"{alias_a}.{_validate_identifier(ka)} = {alias_b}.{_validate_identifier(kb)}"
            for ka, kb in zip(keys_a, keys_b)
        ]
        on_clause = " AND ".join(on_conditions)

        # Build concatenated column expressions for hashing
        safe_cols_a = [f"{alias_a}.{_validate_identifier(col)}" for col in column_list_a]
        safe_cols_b = [f"{alias_b}.{_validate_identifier(col)}" for col in column_list_b]

        col_expr_a = f"{concatenation_dialect_a}".join(safe_cols_a)
        col_expr_b = f"{concatenation_dialect_b}".join(safe_cols_b)

        hash_query_a = self._left_connector.DIALECTS["hash"].replace("{COL}", col_expr_a)
        hash_query_b = self._right_connector.DIALECTS["hash"].replace("{COL}", col_expr_b)

        # Validate join_type
        valid_join_types = {"INNER", "LEFT", "RIGHT", "FULL"}
        if join_type.upper() not in valid_join_types:
            raise ValueError(f"Invalid join type: {join_type!r}")

        sql = f"""
        SELECT 1
        FROM {safe_table_a} {alias_a}
        {join_type} JOIN {safe_table_b} {alias_b} ON {on_clause}
        where {hash_query_a}!={hash_query_b}
        """

        logger.info(f"Query constructed: {sql.strip()}")
        return sql.strip()
