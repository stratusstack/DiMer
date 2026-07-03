"""Thin facade: Diffcheck class and re-exported constants.

External callers import ``Diffcheck`` and the module-level constants from here.
The actual algorithm implementations live in ``dimer.core.algorithms``.
"""

from typing import Any

import structlog

from dimer.core.algorithms import (
    BisectionAlgorithm,
    BloomPrefilterAlgorithm,
    CrossDbDiffAlgorithm,
    EmbeddingSimilarityAlgorithm,
    HashDiffAlgorithm,
    JoinDiffAlgorithm,
    SampledAlgorithm,
    SchemaDiffAlgorithm,
)
from dimer.core.algorithms.base import (
    BISECTION_DEFAULT_SEGMENTS,
    BISECTION_DEFAULT_THRESHOLD,
    BLOOM_DEFAULT_FPR,
    CROSS_DB_ROW_LIMIT,
    EMBEDDING_DEFAULT_METRIC,
    EMBEDDING_DEFAULT_THRESHOLD,
    MAX_DETAIL_ROWS,
    SAMPLED_DEFAULT_CONFIDENCE,
    SAMPLED_DEFAULT_SIZE,
    BaseAlgorithm,
    _supports_sql,
)
from dimer.core.models import ComparisonConfig, DiffRun

logger = structlog.get_logger(__name__)

__all__ = [
    "Diffcheck",
    "MAX_DETAIL_ROWS",
    "CROSS_DB_ROW_LIMIT",
    "BISECTION_DEFAULT_SEGMENTS",
    "BISECTION_DEFAULT_THRESHOLD",
    "SAMPLED_DEFAULT_SIZE",
    "SAMPLED_DEFAULT_CONFIDENCE",
    "BLOOM_DEFAULT_FPR",
    "EMBEDDING_DEFAULT_METRIC",
    "EMBEDDING_DEFAULT_THRESHOLD",
]


class Diffcheck:
    """Public façade: selects and runs the appropriate comparison algorithm.

    Usage::

        result = Diffcheck(connector1, connector2, db1_config, db2_config).compare()

    The algorithm is chosen automatically based on config flags and connector
    topology. ``compare_cross_database()`` is available for direct invocation
    (see ALGO.md).
    """

    _left_connector: Any
    _right_connector: Any
    _left_config: ComparisonConfig
    _right_config: ComparisonConfig

    def __init__(
        self,
        connection1: Any,
        connection2: Any,
        db1: ComparisonConfig,
        db2: ComparisonConfig,
    ) -> None:
        super().__init__()
        self._left_connector = connection1
        self._right_connector = connection2

        for key in ('fq_table_name', 'keys'):
            if key not in db1:
                raise ValueError(f"db1 missing required key: {key!r}")
            if key not in db2:
                raise ValueError(f"db2 missing required key: {key!r}")

        self._left_config = db1
        self._right_config = db2

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_same_instance(self) -> bool:
        """True when both connectors point to the same host and database.

        JOIN_DIFF requires running SQL joins on one connection, so non-SQL
        connectors (document stores) are never treated as same-instance.
        """
        if not (_supports_sql(self._left_connector) and _supports_sql(self._right_connector)):
            return False
        return (
            self._left_connector.connection_config.host
            == self._right_connector.connection_config.host
            and self._left_connector.connection_config.database
            == self._right_connector.connection_config.database
        )

    def _make_algorithm(self, cls) -> BaseAlgorithm:
        """Instantiate any algorithm class with the stored connectors and configs."""
        return cls(
            self._left_connector,
            self._right_connector,
            self._left_config,
            self._right_config,
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def compare(self) -> DiffRun:
        """Choose the appropriate comparison strategy and run the diff."""
        logger.info("Starting table comparison")

        # Schema-only diff is an explicit opt-in (no data read; UC2)
        if self._left_config.get('use_schema_diff') or self._right_config.get('use_schema_diff'):
            logger.info("Schema-diff algorithm selected — catalog metadata compare only")
            return self._make_algorithm(SchemaDiffAlgorithm).run()

        # Embedding similarity is an explicit opt-in (vector sources)
        if self._left_config.get('use_embedding') or self._right_config.get('use_embedding'):
            logger.info("Embedding-similarity algorithm selected — per-id vector distance")
            return self._make_algorithm(EmbeddingSimilarityAlgorithm).run()

        # Bloom prefilter is an explicit opt-in (cheap "definitely differs" signal)
        if self._left_config.get('use_bloom') or self._right_config.get('use_bloom'):
            logger.info("Bloom prefilter selected — probabilistic membership check")
            return self._make_algorithm(BloomPrefilterAlgorithm).run()

        # Bisection is an explicit opt-in (checked before instance routing)
        if self._left_config.get('use_bisection') or self._right_config.get('use_bisection'):
            logger.info("Bisection algorithm selected — using NTILE segment comparison")
            return self._make_algorithm(BisectionAlgorithm).run()

        # Sampling is an explicit opt-in; only valid for cross-database comparisons
        if self._left_config.get('use_sampling'):
            if self._is_same_instance():
                logger.warning(
                    "Sampling is only supported for cross-database comparisons; "
                    "falling back to JOIN_DIFF"
                )
                return self._make_algorithm(JoinDiffAlgorithm).run()
            sample_size = self._left_config.get('sample_size', SAMPLED_DEFAULT_SIZE)
            confidence = self._left_config.get('confidence', SAMPLED_DEFAULT_CONFIDENCE)
            logger.info(
                f"Sampled algorithm selected — source-perspective sampling "
                f"(n={sample_size}, confidence={confidence})"
            )
            return self._make_algorithm(SampledAlgorithm).run()

        # Automatic routing based on instance topology
        if self._is_same_instance():
            logger.info("Same database instance — using JOIN-based comparison")
            return self._make_algorithm(JoinDiffAlgorithm).run()
        else:
            logger.info("Different database instances — using hash-diff comparison")
            return self._make_algorithm(HashDiffAlgorithm).run()

    def compare_schema_only(self) -> DiffRun:
        """Run the SCHEMA_DIFF algorithm directly (UC2 — structure compare).

        Compares catalog metadata only; no data rows are read.  Also selected
        automatically by ``compare()`` when ``use_schema_diff=True``.
        """
        return self._make_algorithm(SchemaDiffAlgorithm).run()

    def compare_cross_database(self) -> DiffRun:
        """Compare tables using the full in-memory FULL_FETCH_DIFF algorithm.

        Not selected automatically — available for direct invocation when
        debugging or verifying HASH_DIFF results (see ALGO.md).
        """
        return self._make_algorithm(CrossDbDiffAlgorithm).run()

    # ------------------------------------------------------------------
    # Standalone schema check
    # ------------------------------------------------------------------

    def check_schema(self, table_a: str, table_b: str) -> bool:
        """Detailed schema comparison between two tables. Returns True if schemas match.

        Thin wrapper over the SCHEMA_DIFF algorithm, kept for backward
        compatibility.  Note: like the original implementation, only column
        *presence* determines the boolean result; attribute drift on common
        columns is logged but does not fail the check.  Use
        ``compare_schema_only()`` for the full structural verdict.
        """
        logger.info("Starting detailed schema comparison")

        result = SchemaDiffAlgorithm(
            self._left_connector,
            self._right_connector,
            {"fq_table_name": table_a, "keys": []},
            {"fq_table_name": table_b, "keys": []},
        ).run()

        if result.error:
            logger.error(f"Could not retrieve metadata for schema comparison: {result.error}")
            return False

        differences = result.schema_differences or {}
        if differences.get('columns_only_in_a'):
            logger.info(f"Columns only in A: {differences['columns_only_in_a']}")
        if differences.get('columns_only_in_b'):
            logger.info(f"Columns only in B: {differences['columns_only_in_b']}")
        if differences.get('column_type_differences'):
            logger.info(f"Type differences: {differences['column_type_differences']}")

        s = result.summary
        return s is not None and s.added_count == 0 and s.deleted_count == 0
