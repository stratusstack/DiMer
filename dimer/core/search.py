"""Thin facade: ValueSearch class for UC10 value search.

External callers import ``ValueSearch`` from here, mirroring how ``Diffcheck``
fronts the diff algorithms in ``dimer.core.compare``.  The implementation
lives in ``dimer.core.algorithms.value_search``.
"""

import structlog

from dimer.core.algorithms.value_search import (
    VALUE_SEARCH_DEFAULT_MAX_VALUES,
    ValueSearchAlgorithm,
)
from dimer.core.models import (
    SearchMode,
    SearchRun,
    ValueSearchSourceConfig,
    ValueSearchTargetConfig,
)

logger = structlog.get_logger(__name__)

__all__ = ["ValueSearch", "VALUE_SEARCH_DEFAULT_MAX_VALUES"]


class ValueSearch:
    """Public façade for UC10 value search.

    Usage::

        run = ValueSearch(
            source_connector, target_connector,
            {"fq_table_name": "public.customers", "source_column": "customer_id"},
            {"fq_table_name": "public.orders"},
            mode=SearchMode.EXACT,
        ).search()

    Unlike ``Diffcheck``, the two sides are asymmetric: the *source* provides
    the values, the *target* is searched.  No join keys are involved.
    """

    def __init__(
        self,
        source_connector,
        target_connector,
        source_config: ValueSearchSourceConfig,
        target_config: ValueSearchTargetConfig,
        mode: SearchMode = SearchMode.EXACT,
    ) -> None:
        self._algorithm = ValueSearchAlgorithm(
            source_connector, target_connector, source_config, target_config, mode
        )

    def search(self) -> SearchRun:
        """Run the value search and return the SearchRun result."""
        logger.info("Starting value search")
        return self._algorithm.run()
