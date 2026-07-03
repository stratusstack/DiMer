"""Unit tests for the SCHEMA_DIFF algorithm (UC2 — structure compare)."""

from typing import Optional

import pytest

from dimer.core.algorithms.schema_diff import SchemaDiffAlgorithm
from dimer.core.compare import Diffcheck
from dimer.core.models import (
    ColumnMetadata,
    ConnectionConfig,
    DiffAlgorithm,
    RowStatus,
    TableMetadata,
)

pytestmark = pytest.mark.unit


class FakeMetadataConnector:
    """Connector stand-in that serves a fixed TableMetadata (or None)."""

    def __init__(self, metadata: Optional[TableMetadata], host: str = "host-a") -> None:
        self._metadata = metadata
        self.connection_config = ConnectionConfig(host=host, database="db")

    def get_table_metadata(self, table_name, schema_name=None):
        if self._metadata is None:
            raise RuntimeError("metadata unavailable")
        return self._metadata


def _col(name, data_type="integer", nullable=False, pk=False, **kwargs):
    return ColumnMetadata(
        name=name, data_type=data_type, nullable=nullable, is_primary_key=pk, **kwargs
    )


def _meta(*columns, row_count=None):
    return TableMetadata(columns=list(columns), row_count=row_count)


CONFIG = {"fq_table_name": "public.orders", "keys": []}


def _run(meta_a, meta_b, **config_overrides):
    return SchemaDiffAlgorithm(
        FakeMetadataConnector(meta_a),
        FakeMetadataConnector(meta_b),
        {**CONFIG, **config_overrides},
        dict(CONFIG),
    ).run()


class TestSchemaDiff:
    def test_identical_schemas_match(self):
        meta = _meta(_col("id", pk=True), _col("name", "text", nullable=True))
        result = _run(meta, meta)
        assert result.algorithm == DiffAlgorithm.SCHEMA_DIFF
        assert result.match is True
        assert result.summary.matched_count == 2
        assert result.metadata["primary_key_match"] is True

    def test_case_insensitive_column_matching(self):
        meta_a = _meta(_col("ID", pk=True), _col("Name", "text"))
        meta_b = _meta(_col("id", pk=True), _col("name", "text"))
        result = _run(meta_a, meta_b)
        assert result.match is True

    def test_added_and_removed_columns(self):
        meta_a = _meta(_col("id", pk=True), _col("legacy_flag", "boolean"))
        meta_b = _meta(_col("id", pk=True), _col("created_at", "timestamp"))
        result = _run(meta_a, meta_b)
        assert result.match is False
        assert result.summary.added_count == 1
        assert result.summary.deleted_count == 1
        by_status = {r.status: r for r in result.row_diffs}
        assert by_status[RowStatus.DELETED].key_values == {"column": "legacy_flag"}
        assert by_status[RowStatus.ADDED].key_values == {"column": "created_at"}

    def test_type_drift_is_modified(self):
        meta_a = _meta(_col("id", pk=True), _col("amount", "integer"))
        meta_b = _meta(_col("id", pk=True), _col("amount", "text"))
        result = _run(meta_a, meta_b)
        assert result.match is False
        modified = result.modified_rows()[0]
        assert modified.key_values == {"column": "amount"}
        assert modified.mismatched_columns == ["data_type"]
        assert modified.source_values["data_type"] == "integer"
        assert modified.target_values["data_type"] == "text"

    def test_nullability_drift_is_modified(self):
        meta_a = _meta(_col("id", pk=True), _col("email", "text", nullable=False))
        meta_b = _meta(_col("id", pk=True), _col("email", "text", nullable=True))
        result = _run(meta_a, meta_b)
        assert result.summary.modified_count == 1
        assert result.modified_rows()[0].mismatched_columns == ["nullable"]

    def test_primary_key_drift(self):
        meta_a = _meta(_col("id", pk=True), _col("code", "text", pk=False))
        meta_b = _meta(_col("id", pk=False), _col("code", "text", pk=True))
        result = _run(meta_a, meta_b)
        assert result.match is False
        assert result.metadata["primary_key_match"] is False
        assert result.metadata["primary_key_source"] == ["id"]
        assert result.metadata["primary_key_target"] == ["code"]

    def test_precision_ignored_unless_strict(self):
        meta_a = _meta(_col("id", pk=True), _col("price", "decimal", precision=10, scale=2))
        meta_b = _meta(_col("id", pk=True), _col("price", "decimal", precision=38, scale=2))
        assert _run(meta_a, meta_b).match is True

        strict = _run(meta_a, meta_b, schema_strict=True)
        assert strict.match is False
        assert strict.modified_rows()[0].mismatched_columns == ["precision"]
        assert strict.metadata["strict"] is True

    def test_metadata_failure_returns_error(self):
        meta = _meta(_col("id", pk=True))
        result = _run(None, meta)
        assert result.match is False
        assert result.error is not None
        assert "public.orders" in result.error

    def test_row_counts_surface_in_metadata(self):
        meta_a = _meta(_col("id", pk=True), row_count=100)
        meta_b = _meta(_col("id", pk=True), row_count=90)
        result = _run(meta_a, meta_b)
        assert result.metadata["table_row_count_source"] == 100
        assert result.metadata["table_row_count_target"] == 90
        # Row-count drift is not structural — schemas still match
        assert result.match is True


class TestSchemaDiffRouting:
    def test_use_schema_diff_routes_even_same_instance(self):
        meta = _meta(_col("id", pk=True))
        # Same host + database would normally route to JOIN_DIFF
        left = FakeMetadataConnector(meta, host="same")
        right = FakeMetadataConnector(meta, host="same")
        result = Diffcheck(left, right, {**CONFIG, "use_schema_diff": True}, dict(CONFIG)).compare()
        assert result.algorithm == DiffAlgorithm.SCHEMA_DIFF
        assert result.match is True

    def test_compare_schema_only_direct(self):
        meta_a = _meta(_col("id", pk=True), _col("extra", "text"))
        meta_b = _meta(_col("id", pk=True))
        result = Diffcheck(
            FakeMetadataConnector(meta_a), FakeMetadataConnector(meta_b),
            dict(CONFIG), dict(CONFIG),
        ).compare_schema_only()
        assert result.algorithm == DiffAlgorithm.SCHEMA_DIFF
        assert result.summary.deleted_count == 1

    def test_check_schema_presence_only(self):
        # Type drift on a common column does not fail check_schema (legacy behavior)
        meta_a = _meta(_col("id", pk=True), _col("amount", "integer"))
        meta_b = _meta(_col("id", pk=True), _col("amount", "text"))
        diff = Diffcheck(
            FakeMetadataConnector(meta_a), FakeMetadataConnector(meta_b),
            dict(CONFIG), dict(CONFIG),
        )
        assert diff.check_schema("public.orders", "public.orders") is True

        # A missing column does fail it
        meta_c = _meta(_col("id", pk=True))
        diff2 = Diffcheck(
            FakeMetadataConnector(meta_a), FakeMetadataConnector(meta_c),
            dict(CONFIG), dict(CONFIG),
        )
        assert diff2.check_schema("public.orders", "public.orders") is False
