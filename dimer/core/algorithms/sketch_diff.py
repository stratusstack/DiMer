"""SKETCH_DIFF algorithm — approximate cardinality + approximate median compare.

Per-engine algorithm choices below were verified against each vendor's own
documentation (see ALGO.md §SKETCH_DIFF for the source-linked research):

    Snowflake    APPROX_COUNT_DISTINCT -> HyperLogLog (bias-corrected, Flajolet et al.)
                 APPROX_PERCENTILE     -> improved t-Digest
    BigQuery     APPROX_COUNT_DISTINCT -> HyperLogLog++
                 APPROX_QUANTILES      -> internal quantile-summary sketch (undocumented)
    Databricks   approx_count_distinct -> HyperLogLog++ (dense)
                 approx_percentile     -> Greenwald-Khanna quantile summary
    DuckDB       approx_count_distinct -> HyperLogLog
                 approx_quantile       -> t-Digest
    TiDB         APPROX_COUNT_DISTINCT -> BJKST algorithm
                 APPROX_PERCENTILE     -> undocumented; integer 0-100 percentage param
    PostgreSQL   no native sketch (postgresql-hll / tdigest are extensions,
                 not installed by default) -> exact COUNT(DISTINCT) / PERCENTILE_CONT
    CockroachDB  CREATE EXTENSION is a documented no-op -> same exact fallback as PostgreSQL
    YugabyteDB   the `hll` extension CAN be installed, but is not guaranteed present
                 on a given cluster -> same exact fallback as PostgreSQL for now (see TODO)
    MySQL        no native sketch AND no percentile_cont/percentile_disc at all
                 -> exact COUNT(DISTINCT) fallback for cardinality; median is
                 entirely omitted (not diffable) for MySQL-family columns
"""

import time
from typing import Any, Dict, List, Optional, Tuple

import structlog

from dimer.core.algorithms.base import (
    BaseAlgorithm,
    SKETCH_DEFAULT_RELATIVE_TOLERANCE,
    _get_col_value,
    _validate_identifier,
)
# Reuses PROFILE_DIFF's type-eligibility sets so a column is judged eligible
# for a stat the same way in both algorithms: _DISTINCT_TYPES (sketch
# functions aren't guaranteed well-defined for json/array/object/binary
# across every engine, mirroring the exact-COUNT-DISTINCT caveat) and
# _MEDIAN_ELIGIBLE_TYPES (numeric + date/time — sketches are built for
# numeric distributions, not lexicographic text ordering).
from dimer.core.algorithms.profile_diff import _DISTINCT_TYPES, _MEDIAN_ELIGIBLE_TYPES
from dimer.core.models import ColumnMetadata, DiffAlgorithm, DiffResult, DiffRun

logger = structlog.get_logger(__name__)

_METHOD_SUFFIX = "_method"


class SketchDiffAlgorithm(BaseAlgorithm):
    """UC3 — compare *approximate* per-column cardinality and median.

    Where PROFILE_DIFF computes exact aggregates, SKETCH_DIFF asks each
    connector for its native probabilistic sketch function and falls back to
    the exact equivalent when an engine has none. This is deliberately
    per-connector, not per-dialect-template like the hash functions: sketch
    availability varies even *within* the wire-compatible NSQL families
    (TiDB has a native `APPROX_COUNT_DISTINCT`; CockroachDB, despite
    accepting `CREATE EXTENSION`, treats it as a no-op and has none) — see
    the module docstring for the full per-engine matrix and its sourcing.

    Each connector optionally declares a ``SKETCH_FUNCS`` class dict:

    * ``distinct``            — SQL template (``{COL}`` placeholder) for an
      approximate-cardinality expression. Absent -> falls back to
      ``COUNT(DISTINCT {COL})`` (exact).
    * ``distinct_algorithm``  — human-readable label stored alongside the
      estimate (e.g. ``"HyperLogLog++"``); defaults to ``"exact"`` when
      ``distinct`` is absent.
    * ``median``              — SQL template for an (approximate or exact)
      median expression. Absent -> the median stat is omitted entirely for
      that connector (not attempted, not compared) — this is the MySQL case.
    * ``median_algorithm``    — human-readable label; defaults to ``"exact"``.

    Because estimates are approximate by construction — and because one side
    may use a genuinely different algorithm than the other, or an exact
    fallback against a sketch — comparisons use a looser relative tolerance
    (``sketch_relative_tolerance``, default 5%) than PROFILE_DIFF's 1e-6.
    ``*_method`` labels are carried in ``source_values``/``target_values``
    for context but never affect the match verdict.

    Reuses ``BaseAlgorithm._diff_stat_dicts`` (shared with PROFILE_DIFF) to
    turn per-column stat dicts into ``DiffRow``s; ``DiffRun.summary`` counts
    are over profiled columns, not rows.

    ``DiffRun.metadata`` keys: relative_tolerance, columns_profiled,
    columns_common, table_row_count_source, table_row_count_target,
    distinct_algorithm_source, distinct_algorithm_target,
    median_algorithm_source, median_algorithm_target (algorithm labels are
    reported once per side; per-column detail lives in each DiffRow's
    ``*_method`` entries when they differ from the side-level default).
    """

    def run(self) -> DiffRun:
        start = time.time()
        tolerance: float = self._left_config.get(  # type: ignore[attr-defined]
            'sketch_relative_tolerance', SKETCH_DEFAULT_RELATIVE_TOLERANCE
        )
        column_filter: Optional[List[str]] = self._left_config.get('sketch_columns')  # type: ignore[attr-defined]

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
                algorithm=DiffAlgorithm.SKETCH_DIFF,
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
                    error="sketch_columns did not match any common column",
                    algorithm=DiffAlgorithm.SKETCH_DIFF,
                )

        meta_map_a = {c.name.lower(): c for c in metadata_a.columns}
        meta_map_b = {c.name.lower(): c for c in metadata_b.columns}

        safe_a = _validate_identifier(table_a, case_a)
        safe_b = _validate_identifier(table_b, case_b)

        sketch_a = getattr(self._left_connector, "SKETCH_FUNCS", {})
        sketch_b = getattr(self._right_connector, "SKETCH_FUNCS", {})

        logger.info(
            f"Sketching {len(common_columns)} common columns (one aggregation scan per side) — "
            f"distinct: {sketch_a.get('distinct_algorithm', 'exact')} vs "
            f"{sketch_b.get('distinct_algorithm', 'exact')}, "
            f"median: {sketch_a.get('median_algorithm', 'exact') if sketch_a.get('median') else 'unsupported'} vs "
            f"{sketch_b.get('median_algorithm', 'exact') if sketch_b.get('median') else 'unsupported'}"
        )
        stats_a, row_count_a = self._fetch_sketch(
            self._left_connector, safe_a, common_columns, meta_map_a, case_a, sketch_a
        )
        stats_b, row_count_b = self._fetch_sketch(
            self._right_connector, safe_b, common_columns_b, meta_map_b, case_b, sketch_b
        )

        row_diffs, modified = self._diff_stat_dicts(
            common_columns, common_columns_b, stats_a, stats_b,
            lambda stat, va, vb: self._stats_equal(stat, va, vb, tolerance),
        )

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
            algorithm=DiffAlgorithm.SKETCH_DIFF,
            metadata={
                "relative_tolerance": tolerance,
                "columns_profiled": len(common_columns),
                "columns_common": len(common_columns),
                "table_row_count_source": row_count_a,
                "table_row_count_target": row_count_b,
                "distinct_algorithm_source": sketch_a.get('distinct_algorithm', 'exact'),
                "distinct_algorithm_target": sketch_b.get('distinct_algorithm', 'exact'),
                "median_algorithm_source": sketch_a.get('median_algorithm', 'exact') if sketch_a.get('median') else "unsupported",
                "median_algorithm_target": sketch_b.get('median_algorithm', 'exact') if sketch_b.get('median') else "unsupported",
            },
            execution_time_seconds=time.time() - start,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fetch_sketch(
        self,
        connector,
        safe_table: str,
        columns: List[str],
        meta_map: Dict[str, ColumnMetadata],
        case: str,
        sketch_funcs: Dict[str, str],
    ) -> Tuple[Dict[str, Dict[str, Any]], Optional[int]]:
        """Run one aggregation query and return {column_lower: {stat: value}}, row_count.

        Positional aliases (``c0__distinct``, ``c1__median`` ...), same
        rationale as PROFILE_DIFF: sidesteps alias quoting/casing differences
        across engines. ``median`` is only emitted when the connector
        declares a template for it — engines without one (MySQL-family)
        simply never get a ``c{i}__median`` expression, so that stat is
        absent from the result and skipped by the shared comparator.
        """
        distinct_tmpl = sketch_funcs.get('distinct')
        distinct_algo = sketch_funcs.get('distinct_algorithm', 'exact')
        median_tmpl = sketch_funcs.get('median')
        median_algo = sketch_funcs.get('median_algorithm', 'exact')

        exprs = ["COUNT(*) AS _dimer_row_count"]
        plan: List[Tuple[int, str, bool, bool]] = []  # (idx, col, want_distinct, want_median)

        for i, col in enumerate(columns):
            meta = meta_map.get(col.lower())
            dtype = meta.data_type if meta else None
            want_distinct = dtype in _DISTINCT_TYPES
            want_median = dtype in _MEDIAN_ELIGIBLE_TYPES and median_tmpl is not None
            plan.append((i, col, want_distinct, want_median))

            safe_col = _validate_identifier(col, case)
            if want_distinct:
                expr = (
                    distinct_tmpl.replace("{COL}", safe_col) if distinct_tmpl
                    else f"COUNT(DISTINCT {safe_col})"
                )
                exprs.append(f"{expr} AS c{i}__distinct")
            if want_median:
                exprs.append(f"{median_tmpl.replace('{COL}', safe_col)} AS c{i}__median")

        sql = f"SELECT {', '.join(exprs)} FROM {safe_table}"
        rows = self._query_rows(connector, sql)
        if not rows:
            return {}, 0

        row = rows[0]
        row_count = _get_col_value(row, '_dimer_row_count')
        row_count = int(row_count) if row_count is not None else 0

        result: Dict[str, Dict[str, Any]] = {}
        for i, col, want_distinct, want_median in plan:
            stats: Dict[str, Any] = {}
            if want_distinct:
                dv = _get_col_value(row, f"c{i}__distinct")
                stats["distinct_estimate"] = float(dv) if dv is not None else None
                stats[f"distinct{_METHOD_SUFFIX}"] = distinct_algo
            if want_median:
                mv = _get_col_value(row, f"c{i}__median")
                stats["median_estimate"] = float(mv) if mv is not None else None
                stats[f"median{_METHOD_SUFFIX}"] = median_algo
            result[col.lower()] = stats

        return result, row_count

    @staticmethod
    def _stats_equal(stat: str, val_a: Any, val_b: Any, tolerance: float) -> bool:
        # Method labels are context, not a diffable value — a genuinely
        # different algorithm on each side is expected and not a mismatch.
        if stat.endswith(_METHOD_SUFFIX):
            return True
        if val_a is None and val_b is None:
            return True
        if val_a is None or val_b is None:
            return False
        try:
            fa, fb = float(val_a), float(val_b)
        except (TypeError, ValueError):
            return val_a == val_b
        scale = max(abs(fa), abs(fb), 1.0)
        return abs(fa - fb) <= tolerance * scale
