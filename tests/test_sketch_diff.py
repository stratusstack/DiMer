"""Unit tests for the SKETCH_DIFF algorithm (UC3 — approximate cardinality/median compare).

Uses a tiny in-Python SQL aggregate emulator (``FakeSketchConnector``) that
parses the ``SELECT`` list the algorithm actually generates — including
engine-specific sketch templates with internal commas/brackets (TiDB's
``APPROX_PERCENTILE(col, 50)``, BigQuery's ``APPROX_QUANTILES(col, 2)
[OFFSET(1)]``) — and computes the aggregates from an in-memory row list, so
the real SQL-building logic in ``sketch_diff.py`` is exercised end to end
rather than bypassed.
"""

import re
import statistics
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest

from dimer.core.algorithms.sketch_diff import SketchDiffAlgorithm
from dimer.core.compare import Diffcheck
from dimer.core.models import (
    ColumnMetadata,
    ConnectionConfig,
    DiffAlgorithm,
    QueryResult,
    TableMetadata,
)

pytestmark = pytest.mark.unit

_COL_RE = re.compile(r'"([^"]+)"')
_ALIAS_RE = re.compile(r'\bAS\s+(\w+)$')


def _split_top_level(select_list: str) -> List[str]:
    """Split a SELECT list on top-level commas only (ignores commas inside () / [])."""
    parts: List[str] = []
    depth = 0
    current = ""
    for ch in select_list:
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(current.strip())
            current = ""
        else:
            current += ch
    if current.strip():
        parts.append(current.strip())
    return parts


class FakeSketchConnector:
    """Emulates just enough SQL to satisfy SketchDiffAlgorithm's queries."""

    def __init__(
        self,
        rows: List[Dict[str, Any]],
        metadata: TableMetadata,
        sketch_funcs: Optional[Dict[str, str]] = None,
        host: str = "host",
    ) -> None:
        self.rows = rows
        self._metadata = metadata
        self.SKETCH_FUNCS = sketch_funcs or {}
        self.connection_config = ConnectionConfig(host=host, database="db")

    def get_table_metadata(self, table_name, schema_name=None) -> TableMetadata:
        return self._metadata

    def execute_query(self, query: str, params=None) -> QueryResult:
        select_list = query[len("SELECT "):query.index(" FROM ")]
        out: Dict[str, Any] = {}
        for expr in _split_top_level(select_list):
            if expr == "COUNT(*) AS _dimer_row_count":
                out["_dimer_row_count"] = len(self.rows)
                continue
            alias = _ALIAS_RE.search(expr).group(1)
            col_match = _COL_RE.search(expr)
            col = col_match.group(1)
            values = [r.get(col) for r in self.rows]
            non_null = [v for v in values if v is not None]

            if alias.endswith("__distinct"):
                out[alias] = len(set(non_null))
            elif alias.endswith("__median"):
                out[alias] = statistics.median(non_null) if non_null else None
            else:
                raise AssertionError(f"Unexpected alias: {alias!r} in {expr!r}")

        return QueryResult(data=pd.DataFrame([out]), execution_time=0.0, rows_affected=1, query=query)


def _col(name, data_type, nullable=True, pk=False):
    return ColumnMetadata(name=name, data_type=data_type, nullable=nullable, is_primary_key=pk)


def _meta(*columns):
    return TableMetadata(columns=list(columns))


CONFIG = {"fq_table_name": "public.orders", "keys": []}

# Real per-engine SKETCH_FUNCS as declared on the connectors (kept in sync
# with production values so tests catch drift if a connector changes).
SNOWFLAKE_SKETCH = {
    "distinct": "APPROX_COUNT_DISTINCT({COL})",
    "distinct_algorithm": "HyperLogLog",
    "median": "APPROX_PERCENTILE({COL}, 0.5)",
    "median_algorithm": "t-Digest",
}
BIGQUERY_SKETCH = {
    "distinct": "APPROX_COUNT_DISTINCT({COL})",
    "distinct_algorithm": "HyperLogLog++",
    "median": "APPROX_QUANTILES({COL}, 2)[OFFSET(1)]",
    "median_algorithm": "quantile summary (undocumented internal algorithm)",
}
TIDB_SKETCH = {
    "distinct": "APPROX_COUNT_DISTINCT({COL})",
    "distinct_algorithm": "BJKST",
    "median": "APPROX_PERCENTILE({COL}, 50)",
    "median_algorithm": "TiDB APPROX_PERCENTILE (undocumented algorithm)",
}
POSTGRES_SKETCH = {
    "median": "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {COL})",
    "median_algorithm": "exact (percentile_cont, no native sketch)",
}
MYSQL_SKETCH: Dict[str, str] = {}


def _run(rows_a, meta_a, sketch_a, rows_b, meta_b, sketch_b, **overrides):
    return SketchDiffAlgorithm(
        FakeSketchConnector(rows_a, meta_a, sketch_a),
        FakeSketchConnector(rows_b, meta_b, sketch_b),
        {**CONFIG, **overrides},
        dict(CONFIG),
    ).run()


class TestSketchDiff:
    def test_identical_sketches_match(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"), _col("name", "string"))
        rows = [
            {"id": 1, "amount": 10.0, "name": "a"},
            {"id": 2, "amount": 20.0, "name": "b"},
            {"id": 3, "amount": 30.0, "name": "c"},
        ]
        result = _run(rows, meta, SNOWFLAKE_SKETCH, rows, meta, SNOWFLAKE_SKETCH)
        assert result.algorithm == DiffAlgorithm.SKETCH_DIFF
        assert result.match is True
        assert result.metadata["distinct_algorithm_source"] == "HyperLogLog"
        assert result.metadata["median_algorithm_source"] == "t-Digest"

    def test_snowflake_native_sketch_end_to_end(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"))
        rows_a = [{"id": i, "amount": float(i)} for i in range(1, 11)]
        rows_b = [{"id": i, "amount": float(i)} for i in range(1, 11)]
        result = _run(rows_a, meta, SNOWFLAKE_SKETCH, rows_b, meta, SNOWFLAKE_SKETCH)
        assert result.match is True

    def test_bigquery_array_offset_median_end_to_end(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"))
        rows = [{"id": i, "amount": float(i)} for i in range(1, 11)]
        result = _run(rows, meta, BIGQUERY_SKETCH, rows, meta, BIGQUERY_SKETCH)
        assert result.match is True

    def test_tidb_percentage_param_end_to_end(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"))
        rows = [{"id": i, "amount": float(i)} for i in range(1, 11)]
        result = _run(rows, meta, TIDB_SKETCH, rows, meta, TIDB_SKETCH)
        assert result.match is True

    def test_distinct_estimate_drift_detected(self):
        meta = _meta(_col("id", "int64", pk=True), _col("category", "string"))
        rows_a = [{"id": 1, "category": "x"}, {"id": 2, "category": "y"}]
        rows_b = [{"id": 1, "category": "x"}, {"id": 2, "category": "x"}]
        result = _run(rows_a, meta, SNOWFLAKE_SKETCH, rows_b, meta, SNOWFLAKE_SKETCH)
        assert result.match is False
        modified = result.modified_rows()[0]
        assert "distinct_estimate" in modified.mismatched_columns
        # method labels are context only, never a mismatch trigger
        assert "distinct_method" not in modified.mismatched_columns

    def test_median_drift_detected(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"))
        rows_a = [{"id": 1, "amount": 10.0}, {"id": 2, "amount": 20.0}, {"id": 3, "amount": 30.0}]
        rows_b = [{"id": 1, "amount": 10.0}, {"id": 2, "amount": 900.0}, {"id": 3, "amount": 30.0}]
        result = _run(rows_a, meta, SNOWFLAKE_SKETCH, rows_b, meta, SNOWFLAKE_SKETCH)
        assert result.match is False
        assert "median_estimate" in result.modified_rows()[0].mismatched_columns

    def test_relative_tolerance_absorbs_estimate_noise(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"))
        rows_a = [{"id": i, "amount": float(i)} for i in range(1, 101)]
        rows_b = [{"id": i, "amount": float(i)} for i in range(1, 101)]
        result = _run(rows_a, meta, SNOWFLAKE_SKETCH, rows_b, meta, SNOWFLAKE_SKETCH)
        assert result.match is True

        # Tight tolerance on a genuine (small) drift catches it
        rows_c = [{"id": i, "amount": float(i)} for i in range(1, 96)]  # 5 fewer distinct ids
        tight = _run(rows_a, meta, SNOWFLAKE_SKETCH, rows_c, meta, SNOWFLAKE_SKETCH, sketch_relative_tolerance=0.001)
        assert tight.match is False

    def test_median_omitted_for_string_column(self):
        meta = _meta(_col("id", "int64", pk=True), _col("name", "string"))
        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        result = _run(rows, meta, SNOWFLAKE_SKETCH, rows, meta, SNOWFLAKE_SKETCH)
        assert result.match is True
        # No column got a median stat since 'string' is not median-eligible;
        # only distinct_estimate/distinct_method are ever compared here.

    def test_mysql_no_sketch_funcs_falls_back_to_exact_distinct_and_skips_median(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"))
        rows_a = [{"id": 1, "amount": 10.0}, {"id": 2, "amount": 900.0}]  # very different median
        rows_b = [{"id": 1, "amount": 10.0}, {"id": 2, "amount": 20.0}]
        result = _run(rows_a, meta, MYSQL_SKETCH, rows_b, meta, MYSQL_SKETCH)
        # distinct_estimate equal (2 vs 2) on both sides; median never computed for MySQL
        assert result.match is True
        assert result.metadata["median_algorithm_source"] == "unsupported"
        assert result.metadata["median_algorithm_target"] == "unsupported"
        assert result.metadata["distinct_algorithm_source"] == "exact"

    def test_postgres_exact_median_fallback_still_detects_drift(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"))
        rows_a = [{"id": 1, "amount": 10.0}, {"id": 2, "amount": 20.0}, {"id": 3, "amount": 30.0}]
        rows_b = [{"id": 1, "amount": 10.0}, {"id": 2, "amount": 900.0}, {"id": 3, "amount": 30.0}]
        result = _run(rows_a, meta, POSTGRES_SKETCH, rows_b, meta, POSTGRES_SKETCH)
        assert result.match is False
        assert result.metadata["median_algorithm_source"] == "exact (percentile_cont, no native sketch)"

    def test_cross_engine_snowflake_vs_bigquery_still_comparable(self):
        # Different sketch algorithms on each side — should still compare
        # the resulting estimates against each other via tolerance.
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"))
        rows = [{"id": i, "amount": float(i)} for i in range(1, 21)]
        result = _run(rows, meta, SNOWFLAKE_SKETCH, rows, meta, BIGQUERY_SKETCH)
        assert result.match is True
        assert result.metadata["distinct_algorithm_source"] == "HyperLogLog"
        assert result.metadata["distinct_algorithm_target"] == "HyperLogLog++"

    def test_sketch_columns_filter(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"), _col("name", "string"))
        rows_a = [{"id": 1, "amount": 10.0, "name": "a"}]
        rows_b = [{"id": 1, "amount": 99.0, "name": "a"}]
        result = _run(rows_a, meta, SNOWFLAKE_SKETCH, rows_b, meta, SNOWFLAKE_SKETCH, sketch_columns=["name"])
        assert result.metadata["columns_profiled"] == 1
        assert result.match is True  # amount drift ignored — not in filter

    def test_no_common_columns_errors(self):
        meta_a = _meta(_col("id", "int64", pk=True))
        meta_b = _meta(_col("other", "int64", pk=True))
        result = _run([], meta_a, SNOWFLAKE_SKETCH, [], meta_b, SNOWFLAKE_SKETCH)
        assert result.match is False
        assert result.error is not None


class TestSketchDiffRouting:
    def test_use_sketch_diff_routes_even_same_instance(self):
        meta = _meta(_col("id", "int64", pk=True))
        rows = [{"id": 1}]
        left = FakeSketchConnector(rows, meta, SNOWFLAKE_SKETCH, host="same")
        right = FakeSketchConnector(rows, meta, SNOWFLAKE_SKETCH, host="same")
        result = Diffcheck(left, right, {**CONFIG, "use_sketch_diff": True}, dict(CONFIG)).compare()
        assert result.algorithm == DiffAlgorithm.SKETCH_DIFF

    def test_compare_sketch_only_direct(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"))
        rows_a = [{"id": 1, "amount": 1.0}, {"id": 2, "amount": 2.0}]
        rows_b = [{"id": 1, "amount": 1.0}, {"id": 2, "amount": 200.0}]
        result = Diffcheck(
            FakeSketchConnector(rows_a, meta, SNOWFLAKE_SKETCH),
            FakeSketchConnector(rows_b, meta, SNOWFLAKE_SKETCH),
            dict(CONFIG), dict(CONFIG),
        ).compare_sketch_only()
        assert result.algorithm == DiffAlgorithm.SKETCH_DIFF
        assert result.match is False
