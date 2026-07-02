"""SAMPLED algorithm — statistical sampling with Wilson confidence intervals."""

import math
import time
from typing import Any, Dict, List, Optional, Tuple

import structlog

from dimer.core.algorithms.base import (
    BaseAlgorithm,
    SAMPLED_DEFAULT_CONFIDENCE,
    SAMPLED_DEFAULT_SIZE,
    _get_col_value,
    _supports_sql,
    _validate_identifier,
)
from dimer.core.models import DiffAlgorithm, DiffResult, DiffRun, RowStatus

logger = structlog.get_logger(__name__)


def _z_score(confidence: float) -> float:
    """Return the z-score for a two-tailed confidence level.

    Pre-computed for common levels (0.90, 0.95, 0.99); falls back to a
    rational approximation of the inverse normal CDF for other values
    (Abramowitz & Stegun 26.2.17, max error < 4.5e-4).
    """
    _COMMON = {0.90: 1.6449, 0.95: 1.9600, 0.99: 2.5758}
    if confidence in _COMMON:
        return _COMMON[confidence]
    p = (1.0 + confidence) / 2.0
    t = math.sqrt(-2.0 * math.log(1.0 - p))
    num = 2.515517 + 0.802853 * t + 0.010328 * t * t
    den = 1.0 + 1.432788 * t + 0.189269 * t * t + 0.001308 * t * t * t
    return t - num / den


def _wilson_ci(k: int, n: int, confidence: float = 0.95) -> Tuple[float, float]:
    """Wilson score confidence interval for a proportion k/n.

    Returns (lower, upper) as fractions in [0, 1].
    Handles the edge case n=0 by returning (0.0, 1.0).
    """
    if n == 0:
        return 0.0, 1.0
    z = _z_score(confidence)
    z2 = z * z
    p_hat = k / n
    center = (p_hat + z2 / (2 * n)) / (1 + z2 / n)
    spread = z * math.sqrt(p_hat * (1 - p_hat) / n + z2 / (4 * n * n)) / (1 + z2 / n)
    return max(0.0, center - spread), min(1.0, center + spread)


class SampledAlgorithm(BaseAlgorithm):
    """Cross-database diff on a random sample of source rows (source-perspective).

    Steps:
      1. Fetch schema metadata and resolve common columns.
      2. Sample ``sample_size`` rows from the source using ORDER BY RANDOM()/RAND().
      3. Extract key values from the sample and fetch matching rows from target
         via WHERE key IN (...).
      4. Classify rows as DELETED or MODIFIED (source-perspective only).
      5. Compute a Wilson score confidence interval for the observed diff rate.
      6. Extrapolate counts to the full source table via COUNT(*).

    Limitation — ADDED rows not detected:
      Because rows are sampled from the source, any rows that exist only in
      the target (ADDED) are not part of the sample and will never be seen.
      The diff rate and CI reflect only source-perspective differences
      (DELETED + MODIFIED). This is an inherent property of Option B1 sampling.

    ``DiffRun.metadata`` keys set by this method:
      sample_size, source_row_count_full, sampled_diff_count,
      observed_diff_rate, estimated_diff_pct, ci_lower, ci_upper,
      margin_of_error, confidence_level, estimated_total_diffs
    """

    def run(self) -> DiffRun:
        sample_size: int = self._left_config.get('sample_size', SAMPLED_DEFAULT_SIZE)  # type: ignore[attr-defined]
        confidence: float = self._left_config.get('confidence', SAMPLED_DEFAULT_CONFIDENCE)  # type: ignore[attr-defined]

        start = time.time()
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
                algorithm=DiffAlgorithm.SAMPLED,
            )

        # Schema metadata
        logger.info("Fetching schema metadata for both tables")
        metadata_a = self.get_schema_metadata(self._left_connector, table_a)
        metadata_b = self.get_schema_metadata(self._right_connector, table_b)

        schema_diff: Optional[Dict[str, Any]] = None
        common_columns: List[str] = []
        common_columns_b: List[str] = []

        if metadata_a is not None and metadata_b is not None:
            schema_diff, common_columns = self._resolve_common_columns(metadata_a, metadata_b)
            cols_b_map = {c.name.lower(): c.name for c in metadata_b.columns}
            common_columns_b = [cols_b_map[c.lower()] for c in common_columns]
        else:
            logger.warning("Could not retrieve metadata; proceeding without schema diff")

        if not common_columns:
            return DiffRun(
                match=False,
                schema_differences=schema_diff,
                error="No common columns found between tables",
                algorithm=DiffAlgorithm.SAMPLED,
            )

        safe_a = _validate_identifier(table_a, case_a)
        safe_b = _validate_identifier(table_b, case_b)

        key_set_lower = {k.lower() for k in keys_a}
        non_key_cols = [c for c in common_columns if c.lower() not in key_set_lower]

        # Full source row count for extrapolation
        logger.info("Counting full source table for extrapolation")
        full_count_a = self._count_rows(self._left_connector, safe_a)
        logger.info(f"Full source row count: {full_count_a:,}")

        # Sample rows from source using ORDER BY RANDOM()/RAND()
        logger.info(f"Sampling {sample_size:,} rows from source")
        if _supports_sql(self._left_connector):
            random_fn = self._left_connector.DIALECTS.get('random_func', 'RAND()')
            cols_select_a = ", ".join(_validate_identifier(c, case_a) for c in common_columns)
            sample_sql = (
                f"SELECT {cols_select_a} FROM {safe_a} "
                f"ORDER BY {random_fn} LIMIT {sample_size}"
            )
            sample_rows_a = self._query_rows(self._left_connector, sample_sql)
        else:
            sample_rows_a = self._left_connector.sample_rows(
                table_a, common_columns, sample_size
            )
        actual_sample = len(sample_rows_a)
        logger.info(f"Sampled {actual_sample:,} rows from source")

        if actual_sample == 0:
            return DiffRun(
                match=True,
                summary=DiffResult(source_row_count=0, target_row_count=0),
                common_columns=common_columns,
                schema_differences=schema_diff,
                algorithm=DiffAlgorithm.SAMPLED,
                metadata={"sample_size": 0, "source_row_count_full": full_count_a},
                execution_time_seconds=time.time() - start,
            )

        # Fetch matching rows from target using the sampled source keys
        # Build target-side key dicts (B column names, A values)
        cols_select_b = ", ".join(_validate_identifier(c, case_b) for c in common_columns_b)
        key_dicts_for_target = [
            {kb: _get_col_value(row, ka) for ka, kb in zip(keys_a, keys_b)}
            for row in sample_rows_a
        ]
        logger.info(f"Fetching matching rows from target for {actual_sample:,} sampled keys")
        target_rows_b = self._fetch_rows_by_keys(
            self._right_connector, safe_b, cols_select_b,
            key_dicts_for_target, keys_b, case_b,
        )
        logger.info(f"Fetched {len(target_rows_b):,} matching rows from target")

        # Build key → row lookups (both keyed by A-side key values)
        lookup_a: Dict[tuple, Dict] = {
            tuple(_get_col_value(r, k) for k in keys_a): r
            for r in sample_rows_a
        }
        lookup_b: Dict[tuple, Dict] = {
            tuple(_get_col_value(r, k) for k in keys_b): {
                col_a: _get_col_value(r, col_b)
                for col_a, col_b in zip(common_columns, common_columns_b)
            }
            for r in target_rows_b
        }

        # Classify: DELETED rows (sampled from source, missing in target) and
        # MODIFIED rows (present in both but values differ).
        # ADDED rows (in target but not in source sample) are not detected.
        row_diffs = self._classify_rows(lookup_a, lookup_b, keys_a, non_key_cols, common_columns)

        deleted_count = sum(1 for r in row_diffs if r.status == RowStatus.DELETED)
        modified_count = sum(1 for r in row_diffs if r.status == RowStatus.MODIFIED)
        added_count = sum(1 for r in row_diffs if r.status == RowStatus.ADDED)
        differing = deleted_count + modified_count + added_count
        matched = max(0, actual_sample - differing)

        # Wilson score CI on the observed source-perspective diff rate
        p_hat = differing / actual_sample
        ci_lower, ci_upper = _wilson_ci(differing, actual_sample, confidence)
        margin_of_error = (ci_upper - ci_lower) / 2

        summary = DiffResult(
            source_row_count=actual_sample,
            target_row_count=len(target_rows_b),
            added_count=added_count,
            deleted_count=deleted_count,
            modified_count=modified_count,
            matched_count=matched,
        )

        return DiffRun(
            match=differing == 0,
            summary=summary,
            row_diffs=row_diffs,
            schema_differences=schema_diff,
            common_columns=common_columns,
            algorithm=DiffAlgorithm.SAMPLED,
            metadata={
                "sample_size": actual_sample,
                "source_row_count_full": full_count_a,
                "sampled_diff_count": differing,
                "observed_diff_rate": p_hat,
                "estimated_diff_pct": round(p_hat * 100, 4),
                "ci_lower": round(ci_lower * 100, 4),
                "ci_upper": round(ci_upper * 100, 4),
                "margin_of_error": round(margin_of_error * 100, 4),
                "confidence_level": confidence,
                "estimated_total_diffs": int(p_hat * full_count_a),
            },
            execution_time_seconds=time.time() - start,
        )
