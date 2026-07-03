"""SCHEMA_DIFF algorithm — catalog/metadata structure compare; no data read."""

import time
from typing import Any, Dict, List, Optional

import structlog

from dimer.core.algorithms.base import BaseAlgorithm
from dimer.core.models import (
    ColumnMetadata,
    DiffAlgorithm,
    DiffResult,
    DiffRow,
    DiffRun,
    RowStatus,
)

logger = structlog.get_logger(__name__)

# Attributes always compared (drive the match verdict)
_CORE_ATTRS = ("data_type", "nullable", "is_primary_key")
# Attributes compared only when schema_strict=True
_STRICT_ATTRS = ("max_length", "precision", "scale")


class SchemaDiffAlgorithm(BaseAlgorithm):
    """Compare table *structure* from catalog metadata — no data is read.

    Fetches ``TableMetadata`` for both sides through each connector's
    existing ``get_table_metadata()`` (information_schema on
    PostgreSQL/MySQL and their NSQL subclasses, native catalogs on
    Snowflake/BigQuery/Databricks/DuckDB, sampled inference on MongoDB)
    and compares column sets and per-column attributes.

    Data types are compared after ``DataTypeMapper`` normalisation, which the
    connectors already apply when building metadata — so a PostgreSQL
    ``character varying`` and a Snowflake ``VARCHAR`` compare equal.  Column
    names are matched case-insensitively (identifier-case differences across
    engines are not structural drift).

    Each differing column becomes one ``DiffRow`` so persistence and CLI
    display work unchanged:

    * column only in source  → ``DELETED`` (key ``{"column": name}``)
    * column only in target  → ``ADDED``
    * attribute drift        → ``MODIFIED`` with ``mismatched_columns``
      listing the differing *attributes* and ``source_values`` /
      ``target_values`` holding each side's attribute dict.

    ``schema_strict=True`` additionally compares max_length / precision /
    scale.  These are noisy across engines (e.g. Snowflake defaults NUMBER
    to precision 38), hence off by default.

    ``DiffRun.summary`` counts are over **columns**, not rows.
    ``DiffRun.metadata`` keys: strict, columns_source, columns_target,
    columns_common, primary_key_source, primary_key_target,
    primary_key_match, table_row_count_source, table_row_count_target.
    """

    def run(self) -> DiffRun:
        start = time.time()
        strict: bool = self._left_config.get('schema_strict', False)  # type: ignore[attr-defined]

        table_a = self._left_config['fq_table_name']
        table_b = self._right_config['fq_table_name']

        logger.info("Fetching catalog metadata for both tables (no data read)")
        metadata_a = self.get_schema_metadata(self._left_connector, table_a)
        metadata_b = self.get_schema_metadata(self._right_connector, table_b)

        if metadata_a is None or metadata_b is None:
            missing = table_a if metadata_a is None else table_b
            return DiffRun(
                match=False,
                error=f"Could not retrieve catalog metadata for {missing}",
                algorithm=DiffAlgorithm.SCHEMA_DIFF,
            )

        cols_a = {c.name.lower(): c for c in metadata_a.columns}
        cols_b = {c.name.lower(): c for c in metadata_b.columns}
        only_in_a = [c for c in metadata_a.columns if c.name.lower() not in cols_b]
        only_in_b = [c for c in metadata_b.columns if c.name.lower() not in cols_a]
        common = [c.name.lower() for c in metadata_a.columns if c.name.lower() in cols_b]

        attrs = _CORE_ATTRS + _STRICT_ATTRS if strict else _CORE_ATTRS

        row_diffs: List[DiffRow] = []
        for col in only_in_a:
            row_diffs.append(DiffRow(
                key_values={"column": col.name},
                status=RowStatus.DELETED,
                source_values=self._attr_dict(col, attrs),
            ))
        for col in only_in_b:
            row_diffs.append(DiffRow(
                key_values={"column": col.name},
                status=RowStatus.ADDED,
                target_values=self._attr_dict(col, attrs),
            ))

        modified = 0
        for name in common:
            col_a, col_b = cols_a[name], cols_b[name]
            mismatched = [
                attr for attr in attrs
                if getattr(col_a, attr) != getattr(col_b, attr)
            ]
            if mismatched:
                modified += 1
                row_diffs.append(DiffRow(
                    key_values={"column": col_a.name},
                    status=RowStatus.MODIFIED,
                    mismatched_columns=mismatched,
                    source_values=self._attr_dict(col_a, attrs),
                    target_values=self._attr_dict(col_b, attrs),
                ))

        pk_a = sorted(c.name.lower() for c in metadata_a.columns if c.is_primary_key)
        pk_b = sorted(c.name.lower() for c in metadata_b.columns if c.is_primary_key)

        # Reuse the shared schema-difference dict for display/persistence parity
        schema_diff: Optional[Dict[str, Any]] = self.compare_schemas(metadata_a, metadata_b)

        summary = DiffResult(
            source_row_count=len(metadata_a.columns),   # counts are over columns
            target_row_count=len(metadata_b.columns),
            added_count=len(only_in_b),
            deleted_count=len(only_in_a),
            modified_count=modified,
            matched_count=len(common) - modified,
        )

        return DiffRun(
            match=summary.total_differences == 0,
            summary=summary,
            row_diffs=row_diffs,
            schema_differences=schema_diff,
            common_columns=[cols_a[n].name for n in common],
            algorithm=DiffAlgorithm.SCHEMA_DIFF,
            metadata={
                "strict": strict,
                "columns_source": len(metadata_a.columns),
                "columns_target": len(metadata_b.columns),
                "columns_common": len(common),
                "primary_key_source": pk_a,
                "primary_key_target": pk_b,
                "primary_key_match": pk_a == pk_b,
                "table_row_count_source": metadata_a.row_count,
                "table_row_count_target": metadata_b.row_count,
            },
            execution_time_seconds=time.time() - start,
        )

    @staticmethod
    def _attr_dict(col: ColumnMetadata, attrs) -> Dict[str, Any]:
        return {attr: getattr(col, attr) for attr in attrs}
