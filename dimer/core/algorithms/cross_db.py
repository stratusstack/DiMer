"""CROSS_DB_DIFF algorithm — full in-memory cross-database comparison."""

import time
from typing import Any, Dict, List, Optional

import structlog

from dimer.core.algorithms.base import (
    BaseAlgorithm,
    CROSS_DB_ROW_LIMIT,
    MAX_DETAIL_ROWS,
    _get_col_value,
    _python_row_hash,
    _validate_identifier,
)
from dimer.core.models import DiffAlgorithm, DiffResult, DiffRow, DiffRun, RowStatus

logger = structlog.get_logger(__name__)


class CrossDbDiffAlgorithm(BaseAlgorithm):
    """Compare tables from different database instances.

    Fetches all rows from both tables into memory, computes per-row MD5
    hashes in Python, then identifies added/deleted/modified rows.
    """

    def run(self) -> DiffRun:
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
