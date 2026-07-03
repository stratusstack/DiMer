"""Unit tests for the PROFILE_DIFF algorithm (UC3 — aggregate/profile compare).

Uses a tiny in-Python SQL aggregate emulator (``FakeSqlConnector``) that
parses the ``SELECT`` list the algorithm actually generates and computes the
aggregates from an in-memory row list. This exercises the real SQL-building
logic in ``profile_diff.py`` (positional aliases, per-column category
selection) rather than bypassing it.
"""

import re
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest

from dimer.core.algorithms.profile_diff import ProfileDiffAlgorithm
from dimer.core.compare import Diffcheck
from dimer.core.models import (
    ColumnMetadata,
    ConnectionConfig,
    DiffAlgorithm,
    QueryResult,
    TableMetadata,
)

pytestmark = pytest.mark.unit

_EXPR_RE = re.compile(
    r'^(COUNT|MIN|MAX|AVG|SUM)\((DISTINCT\s+)?(\*|"[^"]+")\)\s+AS\s+(\w+)$'
)


class FakeSqlConnector:
    """Emulates just enough SQL to satisfy ProfileDiffAlgorithm's queries."""

    def __init__(self, rows: List[Dict[str, Any]], metadata: TableMetadata, host: str = "host") -> None:
        self.rows = rows
        self._metadata = metadata
        self.connection_config = ConnectionConfig(host=host, database="db")

    def get_table_metadata(self, table_name, schema_name=None) -> TableMetadata:
        return self._metadata

    def execute_query(self, query: str, params=None) -> QueryResult:
        select_list = query[len("SELECT "):query.index(" FROM ")]
        exprs = select_list.split(", ")
        out: Dict[str, Any] = {}
        for expr in exprs:
            if expr == "COUNT(*) AS _dimer_row_count":
                out["_dimer_row_count"] = len(self.rows)
                continue
            m = _EXPR_RE.match(expr)
            assert m, f"Unparseable expression from algorithm: {expr!r}"
            func, distinct, target, alias = m.groups()
            col = target.strip('"')
            values = [r.get(col) for r in self.rows]
            non_null = [v for v in values if v is not None]

            if func == "COUNT":
                if distinct:
                    out[alias] = len(set(non_null))
                else:
                    out[alias] = len(non_null)
            elif func == "MIN":
                out[alias] = min(non_null) if non_null else None
            elif func == "MAX":
                out[alias] = max(non_null) if non_null else None
            elif func == "AVG":
                out[alias] = (sum(non_null) / len(non_null)) if non_null else None
            elif func == "SUM":
                out[alias] = sum(non_null) if non_null else None

        return QueryResult(data=pd.DataFrame([out]), execution_time=0.0, rows_affected=1, query=query)


def _col(name, data_type, nullable=True, pk=False):
    return ColumnMetadata(name=name, data_type=data_type, nullable=nullable, is_primary_key=pk)


def _meta(*columns):
    return TableMetadata(columns=list(columns))

CONFIG = {"fq_table_name": "public.orders", "keys": []}


def _run(rows_a, meta_a, rows_b, meta_b, **overrides):
    return ProfileDiffAlgorithm(
        FakeSqlConnector(rows_a, meta_a),
        FakeSqlConnector(rows_b, meta_b),
        {**CONFIG, **overrides},
        dict(CONFIG),
    ).run()


class TestProfileDiff:
    def test_identical_profiles_match(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"), _col("name", "string"))
        rows = [
            {"id": 1, "amount": 10.0, "name": "a"},
            {"id": 2, "amount": 20.0, "name": "b"},
            {"id": 3, "amount": None, "name": "c"},
        ]
        result = _run(rows, meta, rows, meta)
        assert result.algorithm == DiffAlgorithm.PROFILE_DIFF
        assert result.match is True
        assert result.summary.matched_count == 3
        assert result.metadata["columns_profiled"] == 3
        assert result.metadata["table_row_count_source"] == 3

    def test_count_and_null_count_drift(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"))
        rows_a = [{"id": 1, "amount": 10.0}, {"id": 2, "amount": None}]
        rows_b = [{"id": 1, "amount": 10.0}, {"id": 2, "amount": 20.0}]
        result = _run(rows_a, meta, rows_b, meta)
        assert result.match is False
        modified = result.modified_rows()
        assert len(modified) == 1
        row = modified[0]
        assert row.key_values == {"column": "amount"}
        assert "count" in row.mismatched_columns
        assert "null_count" in row.mismatched_columns
        assert row.source_values["null_count"] == 1
        assert row.target_values["null_count"] == 0

    def test_distinct_count_drift(self):
        meta = _meta(_col("id", "int64", pk=True), _col("category", "string"))
        rows_a = [{"id": 1, "category": "x"}, {"id": 2, "category": "y"}]
        rows_b = [{"id": 1, "category": "x"}, {"id": 2, "category": "x"}]
        result = _run(rows_a, meta, rows_b, meta)
        modified = result.modified_rows()
        assert any("distinct_count" in r.mismatched_columns for r in modified)

    def test_min_max_drift(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"))
        rows_a = [{"id": 1, "amount": 5.0}, {"id": 2, "amount": 100.0}]
        rows_b = [{"id": 1, "amount": 5.0}, {"id": 2, "amount": 200.0}]
        result = _run(rows_a, meta, rows_b, meta)
        modified = result.modified_rows()[0]
        assert "max" in modified.mismatched_columns
        assert "min" not in modified.mismatched_columns

    def test_numeric_tolerance_absorbs_float_noise(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"))
        rows_a = [{"id": 1, "amount": 100.0}, {"id": 2, "amount": 200.0}]
        rows_b = [{"id": 1, "amount": 100.0000001}, {"id": 2, "amount": 200.0}]
        # Default tolerance (1e-6) absorbs this
        result = _run(rows_a, meta, rows_b, meta)
        assert result.match is True

        # A tight tolerance catches it
        strict = _run(rows_a, meta, rows_b, meta, profile_numeric_tolerance=1e-12)
        assert strict.match is False

    def test_json_column_skips_distinct(self):
        meta = _meta(_col("id", "int64", pk=True), _col("payload", "json"))
        rows_a = [{"id": 1, "payload": '{"a":1}'}, {"id": 2, "payload": '{"a":2}'}]
        rows_b = [{"id": 1, "payload": '{"a":1}'}, {"id": 2, "payload": '{"a":3}'}]
        result = _run(rows_a, meta, rows_b, meta)
        # count/null_count identical on both sides; no distinct/min/max/avg/sum for json
        assert result.match is True

    def test_type_mismatch_only_compares_shared_stats(self):
        # source has amount as numeric, target has it as string — avg/sum
        # only exist on the source side and are skipped in comparison
        meta_a = _meta(_col("id", "int64", pk=True), _col("amount", "float64"))
        meta_b = _meta(_col("id", "int64", pk=True), _col("amount", "string"))
        rows_a = [{"id": 1, "amount": 10.0}]
        rows_b = [{"id": 1, "amount": "10.0"}]
        result = _run(rows_a, meta_a, rows_b, meta_b)
        # count/null_count/distinct_count/min/max are shared and equal here
        assert result.match is True

    def test_profile_columns_filter(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"), _col("name", "string"))
        rows_a = [{"id": 1, "amount": 10.0, "name": "a"}]
        rows_b = [{"id": 1, "amount": 99.0, "name": "a"}]
        result = _run(rows_a, meta, rows_b, meta, profile_columns=["name"])
        assert result.metadata["columns_profiled"] == 1
        assert result.match is True  # amount drift ignored — not in filter

    def test_no_common_columns_errors(self):
        meta_a = _meta(_col("id", "int64", pk=True))
        meta_b = _meta(_col("other", "int64", pk=True))
        result = _run([], meta_a, [], meta_b)
        assert result.match is False
        assert result.error is not None


class TestProfileDiffRouting:
    def test_use_profile_diff_routes_even_same_instance(self):
        meta = _meta(_col("id", "int64", pk=True))
        rows = [{"id": 1}]
        left = FakeSqlConnector(rows, meta, host="same")
        right = FakeSqlConnector(rows, meta, host="same")
        result = Diffcheck(left, right, {**CONFIG, "use_profile_diff": True}, dict(CONFIG)).compare()
        assert result.algorithm == DiffAlgorithm.PROFILE_DIFF

    def test_compare_profile_only_direct(self):
        meta = _meta(_col("id", "int64", pk=True), _col("amount", "float64"))
        rows_a = [{"id": 1, "amount": 1.0}]
        rows_b = [{"id": 1, "amount": 2.0}]
        result = Diffcheck(
            FakeSqlConnector(rows_a, meta), FakeSqlConnector(rows_b, meta),
            dict(CONFIG), dict(CONFIG),
        ).compare_profile_only()
        assert result.algorithm == DiffAlgorithm.PROFILE_DIFF
        assert result.match is False
