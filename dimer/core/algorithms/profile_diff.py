"""PROFILE_DIFF algorithm — per-column aggregate/profile compare (pushdown)."""

import time
from typing import Any, Dict, List, Optional, Tuple

import structlog

from dimer.core.algorithms.base import (
    BaseAlgorithm,
    PROFILE_DEFAULT_NUMERIC_TOLERANCE,
    _get_col_value,
    _validate_identifier,
)
from dimer.core.models import ColumnMetadata, DiffAlgorithm, DiffResult, DiffRow, DiffRun, RowStatus

logger = structlog.get_logger(__name__)

# Common types (post DataTypeMapper normalisation) that support AVG/SUM
_NUMERIC_TYPES = {
    "int8", "int16", "int32", "int64",
    "uint8", "uint16", "uint32", "uint64",
    "float32", "float64", "decimal",
}
# Types that support MIN/MAX (numeric + orderable date/time/text types)
_ORDERABLE_TYPES = _NUMERIC_TYPES | {"date", "datetime", "timestamp", "time", "string", "text"}
# Types that support COUNT(DISTINCT ...) reliably across engines.
# Excluded: json/array/object (no equality operator on some engines, e.g.
# Postgres 'json' vs 'jsonb'), binary (driver-dependent equality semantics).
_DISTINCT_TYPES = _ORDERABLE_TYPES | {"boolean", "uuid"}

# Integer-valued stats compared exactly; float-valued stats compared with tolerance
_EXACT_STATS = ("count", "null_count", "distinct_count")
_NUMERIC_STATS = ("min", "max", "avg", "sum")


class ProfileDiffAlgorithm(BaseAlgorithm):
    """UC3 — compare per-column aggregate profiles instead of row data.

    One aggregation query per side computes, for every profiled column:

    * ``count``         — non-null value count
    * ``null_count``     — ``row_count - count``
    * ``distinct_count`` — ``COUNT(DISTINCT col)`` (skipped for json/array/
      object/binary columns — see ``_DISTINCT_TYPES``)
    * ``min`` / ``max``   — orderable types only (numeric, date/time, text)
    * ``avg`` / ``sum``   — numeric types only

    Which stats are computed for a column is decided independently per side
    from that side's own catalog metadata (post ``DataTypeMapper``
    normalisation), so a type mismatch between sides simply means fewer
    stats are comparable for that column rather than a failed query. Only
    stats present on **both** sides are compared.

    This is a triage signal, not a row-level diff: equal profiles do not
    prove equal rows (two tables can have identical counts/min/max/avg with
    completely different row contents), but differing profiles prove the
    tables differ. Use HASH_DIFF / JOIN_DIFF / BISECTION for the exact
    row-level answer.

    Exact stats (``count``, ``null_count``, ``distinct_count``) are compared
    for equality; numeric stats (``min``, ``max``, ``avg``, ``sum``) use a
    relative tolerance (``profile_numeric_tolerance``, default 1e-6) to
    absorb cross-engine floating-point / aggregation-order noise.

    Each differing column becomes one ``DiffRow`` (mirrors ``SCHEMA_DIFF``):
    ``mismatched_columns`` lists the differing *stat names*, and
    ``source_values`` / ``target_values`` hold each side's full stat dict.
    ``DiffRun.summary`` counts are over **profiled columns**, not rows.

    ``DiffRun.metadata`` keys: numeric_tolerance, columns_profiled,
    columns_common, table_row_count_source, table_row_count_target.
    """

    def run(self) -> DiffRun:
        start = time.time()
        tolerance: float = self._left_config.get(  # type: ignore[attr-defined]
            'profile_numeric_tolerance', PROFILE_DEFAULT_NUMERIC_TOLERANCE
        )
        column_filter: Optional[List[str]] = self._left_config.get('profile_columns')  # type: ignore[attr-defined]

        case_a = getattr(self._left_connector, "IDENTIFIER_CASE", "preserve")
        case_b = getattr(self._right_connector, "IDENTIFIER_CASE", "preserve")

        table_a = self._left_config['fq_table_name']
        table_b = self._right_config['fq_table_name']

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
                algorithm=DiffAlgorithm.PROFILE_DIFF,
            )

        if column_filter:
            wanted = {c.lower() for c in column_filter}
            keep = [i for i, c in enumerate(common_columns) if c.lower() in wanted]
            common_columns = [common_columns[i] for i in keep]
            common_columns_b = [common_columns_b[i] for i in keep]
            if not common_columns:
                return DiffRun(
                    match=False,
                    schema_differences=schema_diff,
                    error="profile_columns did not match any common column",
                    algorithm=DiffAlgorithm.PROFILE_DIFF,
                )

        meta_map_a = {c.name.lower(): c for c in metadata_a.columns}
        meta_map_b = {c.name.lower(): c for c in metadata_b.columns}

        safe_a = _validate_identifier(table_a, case_a)
        safe_b = _validate_identifier(table_b, case_b)

        logger.info(f"Profiling {len(common_columns)} common columns (one aggregation scan per side)")
        stats_a, row_count_a = self._fetch_profile(
            self._left_connector, safe_a, common_columns, meta_map_a, case_a
        )
        stats_b, row_count_b = self._fetch_profile(
            self._right_connector, safe_b, common_columns_b, meta_map_b, case_b
        )

        row_diffs: List[DiffRow] = []
        modified = 0
        for name_a, name_b in zip(common_columns, common_columns_b):
            col_stats_a = stats_a.get(name_a.lower(), {})
            col_stats_b = stats_b.get(name_b.lower(), {})
            mismatched = [
                stat for stat in col_stats_a.keys() & col_stats_b.keys()
                if not self._stats_equal(stat, col_stats_a[stat], col_stats_b[stat], tolerance)
            ]
            if mismatched:
                modified += 1
                row_diffs.append(DiffRow(
                    key_values={"column": name_a},
                    status=RowStatus.MODIFIED,
                    mismatched_columns=sorted(mismatched),
                    source_values=col_stats_a,
                    target_values=col_stats_b,
                ))

        summary = DiffResult(
            source_row_count=len(common_columns),  # counts are over columns
            target_row_count=len(common_columns_b),
            modified_count=modified,
            matched_count=len(common_columns) - modified,
        )

        return DiffRun(
            match=summary.total_differences == 0,
            summary=summary,
            row_diffs=row_diffs,
            schema_differences=schema_diff,
            common_columns=common_columns,
            algorithm=DiffAlgorithm.PROFILE_DIFF,
            metadata={
                "numeric_tolerance": tolerance,
                "columns_profiled": len(common_columns),
                "columns_common": len(common_columns),
                "table_row_count_source": row_count_a,
                "table_row_count_target": row_count_b,
            },
            execution_time_seconds=time.time() - start,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _column_category(col: Optional[ColumnMetadata]) -> Tuple[bool, bool, bool]:
        """Return (supports_distinct, supports_minmax, supports_numeric) for a column."""
        if col is None:
            return False, False, False
        dtype = col.data_type
        return dtype in _DISTINCT_TYPES, dtype in _ORDERABLE_TYPES, dtype in _NUMERIC_TYPES

    def _fetch_profile(
        self,
        connector,
        safe_table: str,
        columns: List[str],
        meta_map: Dict[str, ColumnMetadata],
        case: str,
    ) -> Tuple[Dict[str, Dict[str, Any]], Optional[int]]:
        """Run one aggregation query and return {column_lower: {stat: value}}, row_count.

        Aliases are positional (``c0__count``, ``c1__min`` ...) rather than
        derived from the column name, so no alias quoting/identifier-folding
        concerns arise across engines; lookups use case-insensitive matching
        (``_get_col_value``) to tolerate each engine's own casing of
        unquoted result-column names.
        """
        exprs = ["COUNT(*) AS _dimer_row_count"]
        plan: List[Tuple[int, str, bool, bool, bool]] = []  # (idx, col_name, distinct, minmax, numeric)

        for i, col in enumerate(columns):
            meta = meta_map.get(col.lower())
            supports_distinct, supports_minmax, supports_numeric = self._column_category(meta)
            plan.append((i, col, supports_distinct, supports_minmax, supports_numeric))

            safe_col = _validate_identifier(col, case)
            exprs.append(f"COUNT({safe_col}) AS c{i}__count")
            if supports_distinct:
                exprs.append(f"COUNT(DISTINCT {safe_col}) AS c{i}__distinct")
            if supports_minmax:
                exprs.append(f"MIN({safe_col}) AS c{i}__min")
                exprs.append(f"MAX({safe_col}) AS c{i}__max")
            if supports_numeric:
                exprs.append(f"AVG({safe_col}) AS c{i}__avg")
                exprs.append(f"SUM({safe_col}) AS c{i}__sum")

        sql = f"SELECT {', '.join(exprs)} FROM {safe_table}"
        rows = self._query_rows(connector, sql)
        if not rows:
            return {}, 0

        row = rows[0]
        row_count = _get_col_value(row, '_dimer_row_count')
        row_count = int(row_count) if row_count is not None else 0

        result: Dict[str, Dict[str, Any]] = {}
        for i, col, supports_distinct, supports_minmax, supports_numeric in plan:
            count_val = _get_col_value(row, f"c{i}__count")
            count_val = int(count_val) if count_val is not None else 0
            stats: Dict[str, Any] = {
                "count": count_val,
                "null_count": row_count - count_val,
            }
            if supports_distinct:
                dv = _get_col_value(row, f"c{i}__distinct")
                stats["distinct_count"] = int(dv) if dv is not None else 0
            if supports_minmax:
                stats["min"] = _get_col_value(row, f"c{i}__min")
                stats["max"] = _get_col_value(row, f"c{i}__max")
            if supports_numeric:
                avg_v = _get_col_value(row, f"c{i}__avg")
                sum_v = _get_col_value(row, f"c{i}__sum")
                stats["avg"] = float(avg_v) if avg_v is not None else None
                stats["sum"] = float(sum_v) if sum_v is not None else None
            result[col.lower()] = stats

        return result, row_count

    @staticmethod
    def _stats_equal(stat: str, val_a: Any, val_b: Any, tolerance: float) -> bool:
        if val_a is None and val_b is None:
            return True
        if val_a is None or val_b is None:
            return False
        if stat in _EXACT_STATS:
            return val_a == val_b
        if stat in _NUMERIC_STATS:
            try:
                fa, fb = float(val_a), float(val_b)
            except (TypeError, ValueError):
                # min/max on non-numeric orderable types (date/time/text)
                return val_a == val_b
            # Relative tolerance absorbs cross-engine float noise; exact
            # integer/date-numeric values naturally pass with zero delta.
            scale = max(abs(fa), abs(fb), 1.0)
            return abs(fa - fb) <= tolerance * scale
        return val_a == val_b
