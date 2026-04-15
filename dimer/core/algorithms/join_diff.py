"""JOIN_DIFF algorithm — same-instance SQL JOIN-based comparison."""

import time
from typing import Any, Dict, List, Optional

import structlog

from dimer.core.algorithms.base import (
    BaseAlgorithm,
    MAX_DETAIL_ROWS,
    _build_hash_expr,
    _get_col_value,
    _validate_identifier,
)
from dimer.core.models import DiffAlgorithm, DiffResult, DiffRow, DiffRun, RowStatus

logger = structlog.get_logger(__name__)


class JoinDiffAlgorithm(BaseAlgorithm):
    """Compare two tables on the same database instance using SQL JOINs.

    Uses the left connector for all queries since both tables are accessible
    from the same connection.
    """

    def run(self) -> DiffRun:
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
