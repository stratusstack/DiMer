"""EMBEDDING_SIMILARITY algorithm — per-id vector distance diff for vector sources."""

import math
import time
from typing import Any, Dict, List, Optional

import structlog

from dimer.core.algorithms.base import (
    BaseAlgorithm,
    EMBEDDING_DEFAULT_METRIC,
    EMBEDDING_DEFAULT_THRESHOLD,
    MAX_DETAIL_ROWS,
    _get_col_value,
    _supports_sql,
    _validate_identifier,
)
from dimer.core.models import DiffAlgorithm, DiffResult, DiffRow, DiffRun, RowStatus

logger = structlog.get_logger(__name__)


def _parse_vector(value: Any) -> Optional[List[float]]:
    """Coerce a stored embedding into a list of floats.

    Accepts Python sequences, numpy arrays, and the textual forms returned by
    pgvector / SQL drivers: ``'[0.1, 0.2]'`` or ``'{0.1, 0.2}'``.
    Returns None when the value cannot be interpreted as a vector.
    """
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [float(v) for v in value]
    if hasattr(value, 'tolist'):  # numpy array / pandas types
        return [float(v) for v in value.tolist()]
    if isinstance(value, str):
        stripped = value.strip().lstrip('[{(').rstrip(')}]')
        if not stripped:
            return []
        try:
            return [float(p) for p in stripped.split(',')]
        except ValueError:
            return None
    return None


def _cosine_distance(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0.0 or norm_b == 0.0:
        # Zero vector: identical only to another zero vector
        return 0.0 if norm_a == norm_b else 1.0
    return 1.0 - dot / (norm_a * norm_b)


def _l2_distance(a: List[float], b: List[float]) -> float:
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


class EmbeddingSimilarityAlgorithm(BaseAlgorithm):
    """Vector diff: same id, embedding distance beyond tolerance = MODIFIED.

    Row-hash equality is meaningless for float embeddings produced by
    different index builds, so this algorithm defines "modified" as
    ``distance(vec_a, vec_b) > distance_threshold`` per common id.

    Steps:
      1. Fetch ``(keys, vector_column)`` from both sides.
      2. ADDED / DELETED via key-set operations (same as other algorithms).
      3. For common ids, parse both vectors and compute cosine (default) or
         L2 distance; ids beyond the threshold — or with unparseable /
         dimension-mismatched vectors — are MODIFIED.

    Works today against pgvector through the PostgreSQL connector (the vector
    column arrives as ``'[…]'`` text) and against any connector that can
    return the embedding column; dedicated vector-DB connectors (Pinecone,
    Milvus, Qdrant) plug in via the same non-SQL primitives as MongoDB.

    ``DiffRun.metadata`` keys: vector_column, distance_metric,
    distance_threshold, compared_pairs, max_distance, mean_distance,
    over_threshold, dimension_mismatches, parse_failures.
    """

    def run(self) -> DiffRun:
        start = time.time()

        vector_column: Optional[str] = self._left_config.get('vector_column')  # type: ignore[attr-defined]
        metric: str = self._left_config.get('distance_metric', EMBEDDING_DEFAULT_METRIC)  # type: ignore[attr-defined]
        threshold: float = self._left_config.get('distance_threshold', EMBEDDING_DEFAULT_THRESHOLD)  # type: ignore[attr-defined]

        if not vector_column:
            return DiffRun(
                match=False,
                error="EMBEDDING_SIMILARITY requires 'vector_column' in the comparison config",
                algorithm=DiffAlgorithm.EMBEDDING_SIMILARITY,
            )
        if metric not in ('cosine', 'l2'):
            return DiffRun(
                match=False,
                error=f"Unsupported distance_metric {metric!r} (use 'cosine' or 'l2')",
                algorithm=DiffAlgorithm.EMBEDDING_SIMILARITY,
            )
        distance_fn = _cosine_distance if metric == 'cosine' else _l2_distance

        case_a = getattr(self._left_connector, "IDENTIFIER_CASE", "preserve")
        case_b = getattr(self._right_connector, "IDENTIFIER_CASE", "preserve")

        table_a = self._left_config['fq_table_name']
        table_b = self._right_config['fq_table_name']
        keys_a = self._left_config['keys']
        keys_b = self._right_config['keys']

        if len(keys_a) != len(keys_b):
            return DiffRun(
                match=False,
                error="Key column lists must have equal length",
                algorithm=DiffAlgorithm.EMBEDDING_SIMILARITY,
            )

        # Fetch (keys, vector) from both sides
        logger.info(f"Fetching keys + vector column {vector_column!r} from both sources")
        rows_a = self._fetch_vectors(self._left_connector, table_a, keys_a, vector_column, case_a)
        rows_b = self._fetch_vectors(self._right_connector, table_b, keys_b, vector_column, case_b)
        count_a, count_b = len(rows_a), len(rows_b)
        logger.info(f"Fetched — source: {count_a} rows, target: {count_b} rows")

        lookup_a: Dict[tuple, Any] = {
            tuple(_get_col_value(r, k) for k in keys_a): _get_col_value(r, vector_column)
            for r in rows_a
        }
        lookup_b: Dict[tuple, Any] = {
            tuple(_get_col_value(r, k) for k in keys_b): _get_col_value(r, vector_column)
            for r in rows_b
        }

        keys_only_in_a = set(lookup_a.keys()) - set(lookup_b.keys())
        keys_only_in_b = set(lookup_b.keys()) - set(lookup_a.keys())
        keys_in_both = set(lookup_a.keys()) & set(lookup_b.keys())

        row_diffs: List[DiffRow] = []
        for key_tuple in keys_only_in_a:
            key_vals = {k: v for k, v in zip(keys_a, key_tuple)}
            row_diffs.append(DiffRow(key_values=key_vals, status=RowStatus.DELETED))
        for key_tuple in keys_only_in_b:
            key_vals = {k: v for k, v in zip(keys_a, key_tuple)}
            row_diffs.append(DiffRow(key_values=key_vals, status=RowStatus.ADDED))

        # Distance comparison for common ids
        modified = 0
        over_threshold = 0
        dimension_mismatches = 0
        parse_failures = 0
        max_distance = 0.0
        distance_sum = 0.0
        compared = 0

        for key_tuple in keys_in_both:
            vec_a = _parse_vector(lookup_a[key_tuple])
            vec_b = _parse_vector(lookup_b[key_tuple])
            key_vals = {k: v for k, v in zip(keys_a, key_tuple)}

            if vec_a is None or vec_b is None:
                parse_failures += 1
                is_modified, distance = True, None
            elif len(vec_a) != len(vec_b):
                dimension_mismatches += 1
                is_modified, distance = True, None
            else:
                distance = distance_fn(vec_a, vec_b)
                compared += 1
                distance_sum += distance
                max_distance = max(max_distance, distance)
                is_modified = distance > threshold
                if is_modified:
                    over_threshold += 1

            if is_modified:
                modified += 1
                if len(row_diffs) < MAX_DETAIL_ROWS:
                    row_diffs.append(DiffRow(
                        key_values=key_vals,
                        status=RowStatus.MODIFIED,
                        mismatched_columns=[vector_column],
                        source_values={vector_column: lookup_a[key_tuple],
                                       "_distance": distance},
                        target_values={vector_column: lookup_b[key_tuple],
                                       "_distance": distance},
                    ))
                else:
                    row_diffs.append(DiffRow(key_values=key_vals, status=RowStatus.MODIFIED))

        summary = DiffResult(
            source_row_count=count_a,
            target_row_count=count_b,
            added_count=len(keys_only_in_b),
            deleted_count=len(keys_only_in_a),
            modified_count=modified,
            matched_count=len(keys_in_both) - modified,
        )

        return DiffRun(
            match=summary.total_differences == 0,
            summary=summary,
            row_diffs=row_diffs,
            common_columns=list(dict.fromkeys(keys_a + [vector_column])),
            algorithm=DiffAlgorithm.EMBEDDING_SIMILARITY,
            metadata={
                "vector_column": vector_column,
                "distance_metric": metric,
                "distance_threshold": threshold,
                "compared_pairs": compared,
                "max_distance": round(max_distance, 8),
                "mean_distance": round(distance_sum / compared, 8) if compared else None,
                "over_threshold": over_threshold,
                "dimension_mismatches": dimension_mismatches,
                "parse_failures": parse_failures,
            },
            execution_time_seconds=time.time() - start,
        )

    def _fetch_vectors(
        self,
        connector,
        table: str,
        keys: List[str],
        vector_column: str,
        case: str,
    ) -> List[Dict[str, Any]]:
        """Fetch key columns + the vector column from one side."""
        columns = list(keys) + [vector_column]
        if not _supports_sql(connector):
            return connector.fetch_all_rows(table, columns)
        safe_table = _validate_identifier(table, case)
        # Cast the vector column to text so drivers without a vector type
        # adapter (e.g. pgvector without the client extension) still work.
        cast_tmpl = connector.DIALECTS.get("cast_to_text", "CAST({COL} AS VARCHAR)")
        vec_expr = (
            cast_tmpl.replace("{COL}", _validate_identifier(vector_column, case))
            + f" AS {_validate_identifier(vector_column, case)}"
        )
        select = ", ".join(
            [_validate_identifier(k, case) for k in keys] + [vec_expr]
        )
        return self._query_rows(connector, f"SELECT {select} FROM {safe_table}")
