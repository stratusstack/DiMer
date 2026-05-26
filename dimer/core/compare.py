"""Thin facade: Diffcheck class and re-exported constants.

External callers import ``Diffcheck`` and the module-level constants from here.
The actual algorithm implementations live in ``dimer.core.algorithms``.
"""

from typing import Any

import structlog

from dimer.core.algorithms import (
    BisectionAlgorithm,
    CrossDbDiffAlgorithm,
    HashDiffAlgorithm,
    JoinDiffAlgorithm,
    SampledAlgorithm,
)
from dimer.core.algorithms.base import (
    BISECTION_DEFAULT_SEGMENTS,
    BISECTION_DEFAULT_THRESHOLD,
    CROSS_DB_ROW_LIMIT,
    MAX_DETAIL_ROWS,
    SAMPLED_DEFAULT_CONFIDENCE,
    SAMPLED_DEFAULT_SIZE,
    BaseAlgorithm,
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
        """True when both connectors point to the same host and database."""
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
        """Detailed schema comparison between two tables. Returns True if schemas match."""
        logger.info("Starting detailed schema comparison")

        algo = self._make_algorithm(HashDiffAlgorithm)
        metadata_a = algo.get_schema_metadata(self._left_connector, table_a)
        metadata_b = algo.get_schema_metadata(self._right_connector, table_b)

        if metadata_a is None or metadata_b is None:
            logger.error("Could not retrieve metadata for schema comparison")
            return False

        differences = algo.compare_schemas(metadata_a, metadata_b)
        logger.info(f"Table A ({table_a}): {len(metadata_a.columns)} columns, {metadata_a.row_count} rows")
        logger.info(f"Table B ({table_b}): {len(metadata_b.columns)} columns, {metadata_b.row_count} rows")

        if differences['columns_only_in_a']:
            logger.info(f"Columns only in A: {differences['columns_only_in_a']}")
        if differences['columns_only_in_b']:
            logger.info(f"Columns only in B: {differences['columns_only_in_b']}")
        if differences['column_type_differences']:
            logger.info(f"Type differences: {differences['column_type_differences']}")

        return (
            len(differences['columns_only_in_a']) == 0
            and len(differences['columns_only_in_b']) == 0
        )
