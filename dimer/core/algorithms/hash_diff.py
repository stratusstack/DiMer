"""HASH_DIFF algorithm — two-phase cross-database diff using per-row SQL hashes."""

import time
from typing import Any, Dict, List, Optional

import structlog

from dimer.core.algorithms.base import (
    BaseAlgorithm,
    CROSS_DB_ROW_LIMIT,
    MAX_DETAIL_ROWS,
    _WHERE_CHUNK_SIZE,
    _build_hash_expr,
    _get_col_value,
    _mismatched_columns,
    _python_row_hash,
    _supports_sql,
    _validate_identifier,
)
from dimer.core.models import (
    Assurance,
    ComparisonPurpose,
    Coverage,
    DiffAlgorithm,
    DiffResult,
    DiffRow,
    DiffRun,
    RowStatus,
)

logger = structlog.get_logger(__name__)


class HashDiffAlgorithm(BaseAlgorithm):
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

    ALGORITHM = DiffAlgorithm.HASH_DIFF
    PURPOSE = ComparisonPurpose.ROW_EQUALITY
    ASSURANCE = Assurance.PROBABILISTIC
    COVERAGE = Coverage.FULL
    REQUIRES_KEYS = True
    CAN_VERIFY_EXACT = True

    def _phase1_key_hashes(
        self,
        connector,
        table: str,
        safe_table: str,
        keys: List[str],
        non_key_cols: List[str],
        case: str,
    ) -> List[Dict[str, Any]]:
        """Phase-1 narrow fetch: key columns + one ``_dimer_row_hash`` per row.

        SQL connectors push the hash down via their DIALECTS; non-SQL
        connectors (``SUPPORTS_SQL = False``) compute the same Python MD5
        row hash client-side in ``fetch_key_hashes()``.
        """
        if not _supports_sql(connector):
            return connector.fetch_key_hashes(table, keys, non_key_cols)

        key_select = ", ".join(_validate_identifier(k, case) for k in keys)
        if non_key_cols:
            hash_expr = _build_hash_expr(
                connector, [_validate_identifier(c, case) for c in non_key_cols]
            )
            sql = f"SELECT {key_select}, {hash_expr} AS _dimer_row_hash FROM {safe_table}"
        else:
            # No non-key columns — keys are the whole row; just fetch keys
            sql = f"SELECT {key_select} FROM {safe_table}"
        return self._query_rows(connector, sql)

    def _run(self) -> DiffRun:
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
        logger.info("Phase 1: fetching key + row hash from both sources")
        rows_p1_a = self._phase1_key_hashes(
            self._left_connector, table_a, safe_a, keys_a, non_key_cols, case_a
        )
        rows_p1_b = self._phase1_key_hashes(
            self._right_connector, table_b, safe_b, keys_b, non_key_cols_b, case_b
        )

        count_a = len(rows_p1_a)
        count_b = len(rows_p1_b)
        logger.info(f"Phase 1 complete — source: {count_a} rows, target: {count_b} rows")

        if count_a > CROSS_DB_ROW_LIMIT or count_b > CROSS_DB_ROW_LIMIT:
            logger.warning(
                f"Table exceeds {CROSS_DB_ROW_LIMIT:,} rows. "
                "Consider using BISECTION algorithm for large tables."
            )

        # Build key → hash lookups; rejects NULL / duplicate keys (ADR-0001 §D4)
        def _row_hash(r: Dict[str, Any]) -> Any:
            return _get_col_value(r, '_dimer_row_hash')

        hash_lookup_a: Dict[tuple, Any] = self._index_rows(rows_p1_a, keys_a, table_a, remap=_row_hash)
        hash_lookup_b: Dict[tuple, Any] = self._index_rows(rows_p1_b, keys_b, table_b, remap=_row_hash)

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
            # Decided by key membership alone — no hash was trusted
            self._assurance = Assurance.EXACT
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
        # Hashes are only comparable when both sides use the same recipe; with
        # require_exact, hash-equal rows are value-verified like cross-engine rows.
        same_db_type = type(self._left_connector) is type(self._right_connector)
        trust_hashes = same_db_type and not self._require_exact
        self._assurance = Assurance.PROBABILISTIC if trust_hashes else Assurance.EXACT

        if trust_hashes:
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
            # Different hash functions (or require_exact) — value-compare every common row
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

        if trust_hashes:
            # All candidates are confirmed modified — classify for detail only
            for key_tuple in candidates[:MAX_DETAIL_ROWS]:
                row_a = lookup_a.get(key_tuple)
                row_b = lookup_b.get(key_tuple)
                key_vals = {k: v for k, v in zip(keys_a, key_tuple)}
                mismatched: List[str] = []
                if row_a and row_b:
                    mismatched = _mismatched_columns(row_a, row_b, non_key_cols)
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
