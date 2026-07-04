"""End-to-end tests for UC6 — diffing tabular data files (DELIM = CSV,
COLF = Parquet) through the non-SQL execution path.

These use real temporary files (pandas and pyarrow are core dependencies),
exercising the full stack: connector connect() → TabularFileDiffMixin
primitives → Diffcheck algorithm layer. No mocks.
"""

import pandas as pd
import pytest

from dimer.core.compare import Diffcheck
from dimer.core.models import ConnectionConfig, DiffAlgorithm

pytestmark = pytest.mark.unit


ROWS_A = pd.DataFrame(
    {"id": [1, 2, 3], "name": ["alice", "bob", "carol"], "amount": [10.0, 20.0, 30.0]}
)
# vs A: id=2 modified, id=3 deleted, id=4 added
ROWS_B = pd.DataFrame(
    {"id": [1, 2, 4], "name": ["alice", "bob", "dave"], "amount": [10.0, 99.0, 40.0]}
)


def _csv_connector(tmp_path, name, df):
    from dimer.connectors.files.csv_connector import CSVConnector

    tmp_path.mkdir(parents=True, exist_ok=True)
    file_path = tmp_path / f"{name}.csv"
    df.to_csv(file_path, index=False)
    conn = CSVConnector(ConnectionConfig(host=str(file_path)))
    conn.connect()
    return conn


def _parquet_connector(tmp_path, name, df):
    from dimer.connectors.files.parquet_connector import ParquetConnector

    tmp_path.mkdir(parents=True, exist_ok=True)
    file_path = tmp_path / f"{name}.parquet"
    df.to_parquet(file_path, index=False)
    conn = ParquetConnector(ConnectionConfig(host=str(file_path)))
    conn.connect()
    return conn


CFG = {"fq_table_name": "orders", "keys": ["id"]}


class TestTabularFileContract:
    @pytest.mark.parametrize("factory", [_csv_connector, _parquet_connector])
    def test_declares_non_sql_with_primitives(self, tmp_path, factory):
        conn = factory(tmp_path, "orders", ROWS_A)
        assert conn.SUPPORTS_SQL is False
        assert conn.DIALECTS == {}
        for primitive in ("count_rows", "fetch_all_rows", "fetch_rows_by_keys",
                          "sample_rows", "fetch_key_hashes"):
            assert callable(getattr(conn, primitive))

    @pytest.mark.parametrize("factory", [_csv_connector, _parquet_connector])
    def test_primitives_return_normalized_rows(self, tmp_path, factory):
        conn = factory(tmp_path, "orders", ROWS_A)

        assert conn.count_rows("orders") == 3

        rows = conn.fetch_all_rows("orders", ["id", "name", "amount"])
        assert {r["name"] for r in rows} == {"alice", "bob", "carol"}
        # numpy scalars must come back as native Python types (hashable/comparable)
        assert all(type(r["id"]) is int for r in rows)

        by_key = conn.fetch_rows_by_keys("orders", ["id", "name"], [{"id": 2}], ["id"])
        assert by_key == [{"id": 2, "name": "bob"}]

        sample = conn.sample_rows("orders", ["id"], 2)
        assert len(sample) == 2

        hashes = conn.fetch_key_hashes("orders", ["id"], ["name", "amount"])
        assert len(hashes) == 3
        assert all("_dimer_row_hash" in h for h in hashes)

    def test_nan_normalizes_to_none(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2], "name": ["alice", None]})
        conn = _csv_connector(tmp_path, "orders", df)
        rows = conn.fetch_all_rows("orders", ["id", "name"])
        assert rows[1]["name"] is None


class TestTabularFileFullFetchDiff:
    """UC6: FULL_FETCH_DIFF over DELIM (CSV) and COLF (Parquet) sources."""

    @pytest.mark.parametrize("factory", [_csv_connector, _parquet_connector])
    def test_full_fetch_diff_detects_all_change_kinds(self, tmp_path, factory):
        a = factory(tmp_path / "a", "orders", ROWS_A)
        b = factory(tmp_path / "b", "orders", ROWS_B)

        result = Diffcheck(a, b, dict(CFG), dict(CFG)).compare_cross_database()

        assert result.algorithm == DiffAlgorithm.FULL_FETCH_DIFF
        s = result.summary
        assert (s.added_count, s.deleted_count, s.modified_count) == (1, 1, 1)

    @pytest.mark.parametrize("factory", [_csv_connector, _parquet_connector])
    def test_full_fetch_diff_identical_files_match(self, tmp_path, factory):
        a = factory(tmp_path / "a", "orders", ROWS_A)
        b = factory(tmp_path / "b", "orders", ROWS_A)

        result = Diffcheck(a, b, dict(CFG), dict(CFG)).compare_cross_database()

        assert result.match is True

    def test_full_fetch_diff_csv_vs_parquet(self, tmp_path):
        """Cross-format: DELIM source vs COLF target."""
        a = _csv_connector(tmp_path / "a", "orders", ROWS_A)
        b = _parquet_connector(tmp_path / "b", "orders", ROWS_B)

        result = Diffcheck(a, b, dict(CFG), dict(CFG)).compare_cross_database()

        assert result.algorithm == DiffAlgorithm.FULL_FETCH_DIFF
        s = result.summary
        assert (s.added_count, s.deleted_count, s.modified_count) == (1, 1, 1)


class TestTabularFileRouting:
    def test_auto_routing_picks_hash_diff_not_join_diff(self, tmp_path):
        """Two file connectors must never route to JOIN_DIFF (no SQL joins),
        even when both point at the same base path."""
        a = _csv_connector(tmp_path, "orders", ROWS_A)
        b = _csv_connector(tmp_path, "orders", ROWS_A)

        result = Diffcheck(a, b, dict(CFG), dict(CFG)).compare()

        assert result.algorithm == DiffAlgorithm.HASH_DIFF
        assert result.match is True

    def test_schema_diff_over_files(self, tmp_path):
        """UC2 works for files too: column drift is detected from metadata."""
        df_b = ROWS_B.rename(columns={"amount": "total"})
        a = _csv_connector(tmp_path / "a", "orders", ROWS_A)
        b = _csv_connector(tmp_path / "b", "orders", df_b)

        result = Diffcheck(
            a, b,
            {"fq_table_name": "orders", "keys": []},
            {"fq_table_name": "orders", "keys": []},
        ).compare_schema_only()

        assert result.algorithm == DiffAlgorithm.SCHEMA_DIFF
        assert result.match is False
        statuses = {(r.key_values["column"], r.status.value) for r in result.row_diffs}
        assert ("amount", "deleted") in statuses
        assert ("total", "added") in statuses
