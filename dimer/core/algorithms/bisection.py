"""BISECTION algorithm — NTILE segment-based divide-and-conquer comparison."""

import time
from typing import Any, Dict, List, Optional

import structlog

from dimer.core.algorithms.base import (
    BaseAlgorithm,
    BISECTION_DEFAULT_SEGMENTS,
    BISECTION_DEFAULT_THRESHOLD,
    _get_col_value,
    _validate_identifier,
)
from dimer.core.models import DiffAlgorithm, DiffResult, DiffRun, RowStatus

logger = structlog.get_logger(__name__)


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


class BisectionAlgorithm(BaseAlgorithm):
    """Compare tables using the NTILE bisection algorithm.

    Divides each table into ``BISECTION_DEFAULT_SEGMENTS`` buckets ordered
    by ``bisection_key``, computes an aggregate hash per bucket, and only
    fetches rows for buckets where hashes differ.  For each differing bucket
    the rows are fetched from both sides and classified in-memory.
    """

    def run(self) -> DiffRun:
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
        row_diffs = []
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
