"""Regression tests for ADR-0001 (docs/adr/0001-comparison-contract.md).

Covers the comparison contract: purpose / assurance / coverage labels,
canonicalization v1 (NULL handling, field framing, numeric equality) and key
rules (non-empty, unique, non-NULL). SQL paths run against in-memory DuckDB;
non-SQL paths use the in-memory fake document connectors.
"""

from decimal import Decimal

import pytest

pytest.importorskip("duckdb", reason="duckdb package not installed")

from dimer.connectors.duckdb.connector import DuckDBConnector
from dimer.core.algorithms import (
    BisectionAlgorithm,
    CrossDbDiffAlgorithm,
    HashDiffAlgorithm,
    JoinDiffAlgorithm,
    SketchDiffAlgorithm,
)
from dimer.core.canonicalization import CANONICALIZATION_VERSION, canonicalize, row_fingerprint
from dimer.core.compare import Diffcheck
from dimer.core.models import (
    Assurance,
    ComparisonPurpose,
    ConnectionConfig,
    Coverage,
    DiffAlgorithm,
    RowStatus,
)
from tests.test_new_algorithms import FakeDocConnector, OtherFakeDocConnector

pytestmark = pytest.mark.unit


CFG_A = {"fq_table_name": "t_a", "keys": ["id"]}
CFG_B = {"fq_table_name": "t_b", "keys": ["id"]}


@pytest.fixture
def duck():
    """One in-memory DuckDB holding both t_a and t_b (same instance → JOIN_DIFF)."""
    conn = DuckDBConnector(ConnectionConfig(host=":memory:", schema_name="main"))
    conn.connect()
    yield conn
    conn.close()


def _load(conn, table, rows, ddl="id INTEGER, c1 VARCHAR, c2 VARCHAR"):
    conn.connection.execute(f"CREATE TABLE {table} ({ddl})")
    if rows:
        placeholders = ", ".join("?" for _ in rows[0])
        conn.connection.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)


def _modified_ids(result):
    return sorted(r.key_values["id"] for r in result.row_diffs if r.status == RowStatus.MODIFIED)


# ---------------------------------------------------------------------------
# D3 — NULL handling in SQL row comparison
# ---------------------------------------------------------------------------


class TestSqlNullHandling:
    def test_join_diff_detects_value_to_null(self, duck):
        _load(duck, "t_a", [(1, "x", "same"), (2, "y", "same")])
        _load(duck, "t_b", [(1, None, "same"), (2, "y", "same")])
        result = Diffcheck(duck, duck, dict(CFG_A), dict(CFG_B)).compare()
        assert result.algorithm == DiffAlgorithm.JOIN_DIFF
        assert _modified_ids(result) == [1]
        assert result.match is False

    def test_join_diff_detects_change_beside_a_null(self, duck):
        _load(duck, "t_a", [(1, None, "old")])
        _load(duck, "t_b", [(1, None, "new")])
        result = Diffcheck(duck, duck, dict(CFG_A), dict(CFG_B)).compare()
        assert _modified_ids(result) == [1]

    def test_join_diff_null_equals_null(self, duck):
        _load(duck, "t_a", [(1, None, "v")])
        _load(duck, "t_b", [(1, None, "v")])
        result = Diffcheck(duck, duck, dict(CFG_A), dict(CFG_B)).compare()
        assert result.match is True

    def test_join_diff_reports_null_column_as_mismatched(self, duck):
        _load(duck, "t_a", [(1, "x", "same")])
        _load(duck, "t_b", [(1, None, "same")])
        result = Diffcheck(duck, duck, dict(CFG_A), dict(CFG_B)).compare()
        (row,) = result.row_diffs
        assert row.mismatched_columns == ["c1"]

    def test_hash_diff_same_engine_detects_change_beside_a_null(self, duck):
        _load(duck, "t_a", [(1, None, "old")])
        _load(duck, "t_b", [(1, None, "new")])
        result = HashDiffAlgorithm(duck, duck, dict(CFG_A), dict(CFG_B)).run()
        assert _modified_ids(result) == [1]

    def test_sql_fingerprint_null_differs_from_empty_string(self, duck):
        _load(duck, "t_a", [(1, None, "v")])
        _load(duck, "t_b", [(1, "", "v")])
        result = HashDiffAlgorithm(duck, duck, dict(CFG_A), dict(CFG_B)).run()
        assert _modified_ids(result) == [1]

    def test_sql_fingerprint_preserves_field_boundaries(self, duck):
        _load(duck, "t_a", [(1, "ab", "c")])
        _load(duck, "t_b", [(1, "a", "bc")])
        result = HashDiffAlgorithm(duck, duck, dict(CFG_A), dict(CFG_B)).run()
        assert _modified_ids(result) == [1]

    def test_bisection_detects_change_beside_a_null(self, duck):
        _load(duck, "t_a", [(i, None if i == 3 else "x", "old" if i == 3 else "v") for i in range(1, 6)])
        _load(duck, "t_b", [(i, None if i == 3 else "x", "new" if i == 3 else "v") for i in range(1, 6)])
        cfg_a = {**CFG_A, "bisection_threshold": 1}
        result = BisectionAlgorithm(duck, duck, cfg_a, dict(CFG_B)).run()
        assert _modified_ids(result) == [3]


# ---------------------------------------------------------------------------
# D3 — Python comparator (non-SQL path, cross-engine phase 2, FULL_FETCH)
# ---------------------------------------------------------------------------


class TestPythonComparator:
    def test_null_differs_from_empty_string_same_class(self):
        a = [{"id": 1, "c1": None, "c2": "v"}]
        b = [{"id": 1, "c1": "", "c2": "v"}]
        result = Diffcheck(FakeDocConnector(a), FakeDocConnector(b), dict(CFG_A), dict(CFG_B)).compare()
        assert _modified_ids(result) == [1]

    def test_null_differs_from_empty_string_cross_class(self):
        a = [{"id": 1, "c1": None, "c2": "v"}]
        b = [{"id": 1, "c1": "", "c2": "v"}]
        result = Diffcheck(FakeDocConnector(a), OtherFakeDocConnector(b), dict(CFG_A), dict(CFG_B)).compare()
        assert _modified_ids(result) == [1]

    def test_delimiter_collision_detected(self):
        a = [{"id": 1, "c1": "x|y", "c2": ""}]
        b = [{"id": 1, "c1": "x", "c2": "y|"}]
        result = Diffcheck(FakeDocConnector(a), FakeDocConnector(b), dict(CFG_A), dict(CFG_B)).compare()
        assert _modified_ids(result) == [1]

    def test_decimal_scale_is_not_a_difference_cross_engine(self):
        a = [{"id": 1, "amount": Decimal("125.00")}]
        b = [{"id": 1, "amount": Decimal("125.0000")}]
        result = Diffcheck(FakeDocConnector(a), OtherFakeDocConnector(b), dict(CFG_A), dict(CFG_B)).compare()
        assert result.match is True

    def test_full_fetch_decimal_scale_and_null(self):
        a = [{"id": 1, "amount": Decimal("125.00"), "c": None}, {"id": 2, "amount": 1, "c": None}]
        b = [{"id": 1, "amount": Decimal("125.0"), "c": None}, {"id": 2, "amount": 1, "c": ""}]
        result = CrossDbDiffAlgorithm(
            FakeDocConnector(a), OtherFakeDocConnector(b), dict(CFG_A), dict(CFG_B)
        ).run()
        assert _modified_ids(result) == [2]
        assert result.row_diffs[0].mismatched_columns == ["c"]

    def test_int_and_float_from_pandas_are_equal(self):
        a = [{"id": 1, "n": 3}]
        b = [{"id": 1, "n": 3.0}]
        result = Diffcheck(FakeDocConnector(a), OtherFakeDocConnector(b), dict(CFG_A), dict(CFG_B)).compare()
        assert result.match is True


class TestCanonicalize:
    def test_null_is_not_empty(self):
        assert canonicalize(None) != canonicalize("")

    def test_decimal_scale_insensitive(self):
        assert canonicalize(Decimal("125.00")) == canonicalize(Decimal("125.0000"))
        assert canonicalize(Decimal("-0")) == canonicalize(0)

    def test_float_vs_decimal_shortest_repr(self):
        assert canonicalize(0.1) == canonicalize(Decimal("0.1"))

    def test_nan_is_null(self):
        assert canonicalize(float("nan")) == canonicalize(None)

    def test_string_case_and_whitespace_preserved(self):
        assert canonicalize("John") != canonicalize("John ")
        assert canonicalize("john") != canonicalize("John")

    def test_bool_equals_numeric_one_zero(self):
        assert canonicalize(True) == canonicalize(1)
        assert canonicalize(False) == canonicalize(0)
        assert canonicalize(True) != canonicalize("true")

    def test_tz_aware_compared_in_utc(self):
        from datetime import datetime, timedelta, timezone

        utc = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        ist = datetime(2026, 1, 1, 17, 30, tzinfo=timezone(timedelta(hours=5, minutes=30)))
        assert canonicalize(utc) == canonicalize(ist)
        assert canonicalize(utc) != canonicalize(utc.replace(tzinfo=None))

    def test_date_never_equals_timestamp(self):
        from datetime import date, datetime

        assert canonicalize(date(2026, 1, 1)) != canonicalize(datetime(2026, 1, 1))

    def test_json_key_order_ignored_array_order_kept(self):
        assert canonicalize({"a": 1, "b": 2}) == canonicalize({"b": 2, "a": 1})
        assert canonicalize([1, 2]) != canonicalize([2, 1])

    def test_fingerprint_framing(self):
        assert row_fingerprint({"a": "ab", "b": "c"}, ["a", "b"]) != row_fingerprint(
            {"a": "a", "b": "bc"}, ["a", "b"]
        )
        assert row_fingerprint({"a": None}, ["a"]) != row_fingerprint({"a": ""}, ["a"])


# ---------------------------------------------------------------------------
# D3 — type-incompatible columns are excluded, not str()-compared
# ---------------------------------------------------------------------------


class TestTypeCompatibility:
    def test_incompatible_column_excluded_and_listed(self, duck):
        _load(duck, "t_a", [(1, "1")], ddl="id INTEGER, c VARCHAR")
        _load(duck, "t_b", [(1, 2)], ddl="id INTEGER, c INTEGER")
        result = HashDiffAlgorithm(duck, duck, dict(CFG_A), dict(CFG_B)).run()
        assert result.error is None
        assert result.metadata["excluded_columns"] == ["c"]
        assert "c" not in result.common_columns


# ---------------------------------------------------------------------------
# D4 — key rules
# ---------------------------------------------------------------------------


class TestKeyRules:
    def test_duplicate_key_fails_join_diff(self, duck):
        _load(duck, "t_a", [(1, "x", "v"), (1, "y", "v")])
        _load(duck, "t_b", [(1, "x", "v")])
        result = Diffcheck(duck, duck, dict(CFG_A), dict(CFG_B)).compare()
        assert result.error and "duplicate" in result.error.lower()
        assert result.match is False

    def test_duplicate_key_fails_hash_diff_non_sql(self):
        a = [{"id": 1, "c1": "x"}, {"id": 1, "c1": "y"}]
        b = [{"id": 1, "c1": "x"}]
        result = Diffcheck(FakeDocConnector(a), FakeDocConnector(b), dict(CFG_A), dict(CFG_B)).compare()
        assert result.error and "duplicate" in result.error.lower()

    def test_duplicate_key_fails_full_fetch(self):
        a = [{"id": 1, "c1": "x"}]
        b = [{"id": 1, "c1": "x"}, {"id": 1, "c1": "z"}]
        result = CrossDbDiffAlgorithm(FakeDocConnector(a), FakeDocConnector(b), dict(CFG_A), dict(CFG_B)).run()
        assert result.error and "duplicate" in result.error.lower()

    def test_null_key_fails_join_diff(self, duck):
        _load(duck, "t_a", [(None, "x", "v"), (2, "y", "v")])
        _load(duck, "t_b", [(None, "x", "v"), (2, "y", "v")])
        result = Diffcheck(duck, duck, dict(CFG_A), dict(CFG_B)).compare()
        assert result.error and "null" in result.error.lower()

    def test_null_key_fails_hash_diff_non_sql(self):
        a = [{"id": None, "c1": "x"}]
        b = [{"id": None, "c1": "x"}]
        result = Diffcheck(FakeDocConnector(a), FakeDocConnector(b), dict(CFG_A), dict(CFG_B)).compare()
        assert result.error and "null" in result.error.lower()

    def test_empty_keys_fail_cleanly(self, duck):
        _load(duck, "t_a", [(1, "x", "v")])
        _load(duck, "t_b", [(1, "x", "v")])
        cfg_a = {"fq_table_name": "t_a", "keys": []}
        cfg_b = {"fq_table_name": "t_b", "keys": []}
        for algo in (JoinDiffAlgorithm, HashDiffAlgorithm, CrossDbDiffAlgorithm):
            result = algo(duck, duck, dict(cfg_a), dict(cfg_b)).run()
            assert result.error and "key" in result.error.lower(), algo.__name__


# ---------------------------------------------------------------------------
# D1 — one purpose per request
# ---------------------------------------------------------------------------


class TestRequestResolution:
    def test_conflicting_flags_rejected(self):
        rows = [{"id": 1, "c1": "x"}]
        cfg = {**CFG_A, "use_bloom": True, "use_bisection": True}
        with pytest.raises(ValueError, match="use_bisection.*use_bloom|use_bloom.*use_bisection"):
            Diffcheck(FakeDocConnector(rows), FakeDocConnector(rows), cfg, dict(CFG_B)).compare()

    def test_conflicting_flags_across_sides_rejected(self):
        rows = [{"id": 1, "c1": "x"}]
        with pytest.raises(ValueError):
            Diffcheck(
                FakeDocConnector(rows), FakeDocConnector(rows),
                {**CFG_A, "use_schema_diff": True}, {**CFG_B, "use_profile_diff": True},
            ).compare()

    def test_sampling_on_same_instance_rejected(self, duck):
        _load(duck, "t_a", [(1, "x", "v")])
        _load(duck, "t_b", [(1, "x", "v")])
        with pytest.raises(ValueError, match="[Ss]ampl"):
            Diffcheck(duck, duck, {**CFG_A, "use_sampling": True}, dict(CFG_B)).compare()


# ---------------------------------------------------------------------------
# D2 — purpose / assurance / coverage labels
# ---------------------------------------------------------------------------


class TestResultLabels:
    def test_join_diff_is_exact_full_row_equality(self, duck):
        _load(duck, "t_a", [(1, "x", "v")])
        _load(duck, "t_b", [(1, "x", "v")])
        result = Diffcheck(duck, duck, dict(CFG_A), dict(CFG_B)).compare()
        assert result.purpose == ComparisonPurpose.ROW_EQUALITY
        assert result.assurance == Assurance.EXACT
        assert result.coverage == Coverage.FULL
        assert result.metadata["canonicalization_version"] == CANONICALIZATION_VERSION
        assert result.metadata["purpose"] == "ROW_EQUALITY"
        assert result.metadata["assurance"] == "EXACT"
        assert result.metadata["coverage"] == "FULL"

    def test_hash_diff_same_engine_is_probabilistic(self, duck):
        _load(duck, "t_a", [(1, "x", "v")])
        _load(duck, "t_b", [(1, "x", "v")])
        result = HashDiffAlgorithm(duck, duck, dict(CFG_A), dict(CFG_B)).run()
        assert result.assurance == Assurance.PROBABILISTIC

    def test_hash_diff_cross_engine_is_exact(self):
        rows = [{"id": 1, "c1": "x"}]
        result = Diffcheck(FakeDocConnector(rows), OtherFakeDocConnector(rows), dict(CFG_A), dict(CFG_B)).compare()
        assert result.algorithm == DiffAlgorithm.HASH_DIFF
        assert result.assurance == Assurance.EXACT

    def test_sampled_is_approximate_sampled(self):
        rows = [{"id": 1, "c1": "x"}]
        result = Diffcheck(
            FakeDocConnector(rows), FakeDocConnector(rows),
            {**CFG_A, "use_sampling": True, "sample_size": 10}, dict(CFG_B),
        ).compare()
        assert result.purpose == ComparisonPurpose.SAMPLED_VALIDATION
        assert result.assurance == Assurance.APPROXIMATE
        assert result.coverage == Coverage.SAMPLED

    def test_bloom_is_prefilter(self):
        rows = [{"id": 1, "c1": "x"}]
        result = Diffcheck(
            FakeDocConnector(rows), FakeDocConnector(rows), {**CFG_A, "use_bloom": True}, dict(CFG_B)
        ).compare()
        assert result.purpose == ComparisonPurpose.PREFILTER
        assert result.assurance == Assurance.PROBABILISTIC

    def test_schema_diff_purpose(self, duck):
        _load(duck, "t_a", [(1, "x", "v")])
        _load(duck, "t_b", [(1, "x", "v")])
        result = Diffcheck(duck, duck, {**CFG_A, "use_schema_diff": True}, dict(CFG_B)).compare()
        assert result.purpose == ComparisonPurpose.SCHEMA
        assert result.assurance == Assurance.EXACT

    def test_failed_run_still_labelled(self, duck):
        cfg_a = {"fq_table_name": "t_a", "keys": []}
        result = JoinDiffAlgorithm(duck, duck, cfg_a, {"fq_table_name": "t_b", "keys": []}).run()
        assert result.error
        assert result.purpose == ComparisonPurpose.ROW_EQUALITY


# ---------------------------------------------------------------------------
# D2 — require_exact
# ---------------------------------------------------------------------------


class TestRequireExact:
    def test_bisection_rejected_when_exact_required(self, duck):
        _load(duck, "t_a", [(1, "x", "v")])
        _load(duck, "t_b", [(1, "x", "v")])
        result = BisectionAlgorithm(duck, duck, {**CFG_A, "require_exact": True}, dict(CFG_B)).run()
        assert result.error and "require_exact" in result.error

    def test_sketch_rejected_when_exact_required(self, duck):
        _load(duck, "t_a", [(1, "x", "v")])
        _load(duck, "t_b", [(1, "x", "v")])
        result = SketchDiffAlgorithm(duck, duck, {**CFG_A, "require_exact": True}, dict(CFG_B)).run()
        assert result.error and "require_exact" in result.error

    def test_hash_diff_same_engine_verifies_when_exact_required(self, duck):
        _load(duck, "t_a", [(1, "x", "v"), (2, "y", "v")])
        _load(duck, "t_b", [(1, "x", "v"), (2, "y", "changed")])
        result = HashDiffAlgorithm(duck, duck, {**CFG_A, "require_exact": True}, dict(CFG_B)).run()
        assert result.error is None
        assert result.assurance == Assurance.EXACT
        assert _modified_ids(result) == [2]
        assert result.summary.matched_count == 1
