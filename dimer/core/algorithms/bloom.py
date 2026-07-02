"""BLOOM algorithm — Bloom-filter prefilter giving a cheap "definitely differs" signal."""

import hashlib
import math
import time
from typing import Any, Dict, List, Optional, Tuple

import structlog

from dimer.core.algorithms.base import (
    BLOOM_DEFAULT_FPR,
    BaseAlgorithm,
    MAX_DETAIL_ROWS,
    _build_hash_expr,
    _get_col_value,
    _supports_sql,
    _validate_identifier,
)
from dimer.core.models import DiffAlgorithm, DiffResult, DiffRow, DiffRun, RowStatus

logger = structlog.get_logger(__name__)


class BloomFilter:
    """Minimal pure-Python Bloom filter (no external dependencies).

    Uses the standard Kirsch–Mitzenmacher double-hashing scheme: two base
    hashes derived from one MD5 digest generate ``k`` index functions.
    No false negatives — if ``contains()`` returns False the item was
    definitely never added.
    """

    def __init__(self, capacity: int, fpr: float = BLOOM_DEFAULT_FPR) -> None:
        capacity = max(1, capacity)
        # Optimal bit count m = -n·ln(p) / (ln2)² and hash count k = (m/n)·ln2
        self.bit_count = max(8, int(-capacity * math.log(fpr) / (math.log(2) ** 2)))
        self.hash_count = max(1, round((self.bit_count / capacity) * math.log(2)))
        self.capacity = capacity
        self.fpr = fpr
        self._bits = bytearray((self.bit_count + 7) // 8)

    def _indexes(self, item: str):
        digest = hashlib.md5(item.encode('utf-8')).digest()
        h1 = int.from_bytes(digest[:8], 'big')
        h2 = int.from_bytes(digest[8:], 'big') | 1  # odd so strides cover the table
        for i in range(self.hash_count):
            yield (h1 + i * h2) % self.bit_count

    def add(self, item: str) -> None:
        for idx in self._indexes(item):
            self._bits[idx >> 3] |= 1 << (idx & 7)

    def contains(self, item: str) -> bool:
        return all(self._bits[idx >> 3] & (1 << (idx & 7)) for idx in self._indexes(item))

    @property
    def size_bytes(self) -> int:
        return len(self._bits)


class BloomPrefilterAlgorithm(BaseAlgorithm):
    """Bloom-filter prefilter — cheap "definitely differs" signal (opt-in).

    Fetches only ``(key columns, row hash)`` from each side (like HASH_DIFF
    Phase 1), inserts them into Bloom filters, and streams the opposite side
    through the filters.  There is **no Phase-2 row fetch** — this algorithm
    never retrieves column values, which is what makes it a prefilter.

    Semantics (asymmetric to a real diff):

    * A key/hash that *misses* the opposite filter is **definitely** a
      difference (Bloom filters have no false negatives).
    * A hit may be a false positive, so up to ``bloom_fpr`` (default 1%) of
      truly differing rows can be missed.  ``match=True`` therefore means
      "no differences detected"; run HASH_DIFF/BISECTION to prove parity.

    Hash comparability follows the HASH_DIFF rules: when both connectors are
    the same type their row hashes are comparable and MODIFIED rows can be
    flagged.  Across different connector types only key membership is tested
    (ADDED/DELETED signal only) and ``metadata["hash_comparable"]`` is False.

    Non-SQL connectors (``SUPPORTS_SQL = False``) participate via their
    ``fetch_key_hashes()`` primitive, which computes the same Python MD5 row
    hash client-side; two such connectors are hash-comparable.

    ``DiffRun.metadata`` keys: bloom_fpr, bloom_bits_per_side,
    bloom_hash_count, hash_comparable, definite_added, definite_deleted,
    definite_modified, prefilter (always True).
    """

    def run(self) -> DiffRun:
        start = time.time()
        fpr: float = self._left_config.get('bloom_fpr', BLOOM_DEFAULT_FPR)  # type: ignore[attr-defined]

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
                algorithm=DiffAlgorithm.BLOOM,
            )

        # Schema metadata and common columns
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
                algorithm=DiffAlgorithm.BLOOM,
            )

        key_set_lower = {k.lower() for k in keys_a}
        non_key_cols = [c for c in common_columns if c.lower() not in key_set_lower]
        non_key_cols_b = [
            c for c in common_columns_b
            if c.lower() not in {k.lower() for k in keys_b}
        ]

        # Hash comparability follows the HASH_DIFF rule
        hash_comparable = type(self._left_connector) is type(self._right_connector)

        logger.info("Fetching key + row hash from both sources (narrow fetch)")
        rows_a = self._fetch_key_hashes(
            self._left_connector, table_a, keys_a, non_key_cols, case_a
        )
        rows_b = self._fetch_key_hashes(
            self._right_connector, table_b, keys_b, non_key_cols_b, case_b
        )
        count_a, count_b = len(rows_a), len(rows_b)
        logger.info(f"Narrow fetch complete — source: {count_a} rows, target: {count_b} rows")

        # Build (key string, key|hash string) pairs per side
        pairs_a = self._make_pairs(rows_a, keys_a)
        pairs_b = self._make_pairs(rows_b, keys_b)

        # Build Bloom filters over each side
        bloom_keys_a = BloomFilter(count_a, fpr)
        bloom_keyhash_a = BloomFilter(count_a, fpr)
        for key_s, keyhash_s in pairs_a:
            bloom_keys_a.add(key_s)
            bloom_keyhash_a.add(keyhash_s)

        bloom_keys_b = BloomFilter(count_b, fpr)
        bloom_keyhash_b = BloomFilter(count_b, fpr)
        for key_s, keyhash_s in pairs_b:
            bloom_keys_b.add(key_s)
            bloom_keyhash_b.add(keyhash_s)

        # Stream each side through the opposite filters.
        # A miss is a *definite* difference; hits may be false positives.
        row_diffs: List[DiffRow] = []
        definite_deleted = 0
        definite_modified = 0
        definite_added = 0

        for (key_s, keyhash_s), row in zip(pairs_a, rows_a):
            if not bloom_keys_b.contains(key_s):
                definite_deleted += 1
                self._append_diff(row_diffs, row, keys_a, keys_a, RowStatus.DELETED)
            elif hash_comparable and non_key_cols and not bloom_keyhash_b.contains(keyhash_s):
                definite_modified += 1
                self._append_diff(row_diffs, row, keys_a, keys_a, RowStatus.MODIFIED)

        for (key_s, _), row in zip(pairs_b, rows_b):
            if not bloom_keys_a.contains(key_s):
                definite_added += 1
                self._append_diff(row_diffs, row, keys_b, keys_a, RowStatus.ADDED)

        summary = DiffResult(
            source_row_count=count_a,
            target_row_count=count_b,
            added_count=definite_added,
            deleted_count=definite_deleted,
            modified_count=definite_modified,
            matched_count=max(0, count_a - definite_deleted - definite_modified),
        )

        if not hash_comparable:
            logger.info(
                "Different connector types — row hashes not comparable; "
                "BLOOM tested key membership only (ADDED/DELETED signal, no MODIFIED)"
            )

        return DiffRun(
            match=summary.total_differences == 0 and count_a == count_b,
            summary=summary,
            row_diffs=row_diffs,
            schema_differences=schema_diff,
            common_columns=common_columns,
            algorithm=DiffAlgorithm.BLOOM,
            metadata={
                "prefilter": True,
                "bloom_fpr": fpr,
                "bloom_bits_per_side": bloom_keyhash_a.bit_count,
                "bloom_hash_count": bloom_keyhash_a.hash_count,
                "hash_comparable": hash_comparable,
                "definite_added": definite_added,
                "definite_deleted": definite_deleted,
                "definite_modified": definite_modified,
            },
            execution_time_seconds=time.time() - start,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fetch_key_hashes(
        self,
        connector,
        table: str,
        keys: List[str],
        non_key_cols: List[str],
        case: str,
    ) -> List[Dict[str, Any]]:
        """Fetch key columns + one row hash per row (``_dimer_row_hash``)."""
        if not _supports_sql(connector):
            return connector.fetch_key_hashes(table, keys, non_key_cols)

        safe_table = _validate_identifier(table, case)
        key_select = ", ".join(_validate_identifier(k, case) for k in keys)
        if non_key_cols:
            hash_expr = _build_hash_expr(
                connector, [_validate_identifier(c, case) for c in non_key_cols]
            )
            sql = f"SELECT {key_select}, {hash_expr} AS _dimer_row_hash FROM {safe_table}"
        else:
            sql = f"SELECT {key_select} FROM {safe_table}"
        return self._query_rows(connector, sql)

    @staticmethod
    def _make_pairs(
        rows: List[Dict[str, Any]], keys: List[str]
    ) -> List[Tuple[str, str]]:
        """Return (key string, key|hash string) for every row."""
        pairs: List[Tuple[str, str]] = []
        for row in rows:
            key_s = '|'.join(str(_get_col_value(row, k)) for k in keys)
            hash_v = _get_col_value(row, '_dimer_row_hash')
            pairs.append((key_s, f"{key_s}#{hash_v}"))
        return pairs

    @staticmethod
    def _append_diff(
        row_diffs: List[DiffRow],
        row: Dict[str, Any],
        keys_read: List[str],
        keys_report: List[str],
        status: RowStatus,
    ) -> None:
        """Record a key-only DiffRow, capped at MAX_DETAIL_ROWS per run."""
        if len(row_diffs) >= MAX_DETAIL_ROWS:
            return
        key_vals = {
            kr: _get_col_value(row, k) for k, kr in zip(keys_read, keys_report)
        }
        row_diffs.append(DiffRow(key_values=key_vals, status=status))
