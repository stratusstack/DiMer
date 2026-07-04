"""Shared non-SQL diff primitives for tabular file connectors (UC6).

File-based sources (DELIM — CSV/TSV; COLF — Parquet) have no SQL engine
behind them: their ``_execute_query_internal`` is a minimal
``SELECT … FROM … LIMIT n`` parser with no ``WHERE``, ``ORDER BY``, or hash
functions, so the SQL-generating algorithms (JOIN_DIFF, HASH_DIFF pushdown,
BISECTION) can never run against them. Declaring ``SUPPORTS_SQL = False``
and implementing the same client-side primitives as the MongoDB/Redis/etc.
connectors routes them down the non-SQL execution path instead, which gives
tabular files FULL_FETCH_DIFF (UC6), HASH_DIFF (client-side hashes),
SAMPLED, BLOOM, and SCHEMA_DIFF with no algorithm-layer changes.

Connectors mix this in and implement one method: ``_read_table_df`` — load
the named table (file) into a pandas DataFrame. Everything else is derived
from that. Files are read fully into memory per primitive call, which is the
same cost profile FULL_FETCH_DIFF has anyway — these are files, not servers;
there is no cheaper remote path to preserve.
"""

from typing import Any, Dict, List

import pandas as pd

from dimer.core.algorithms.base import _python_row_hash


class TabularFileDiffMixin:
    """Client-side diff primitives on top of ``_read_table_df``."""

    SUPPORTS_SQL = False
    DIALECTS: Dict[str, str] = {}  # no SQL dialect — everything is client-side

    def _read_table_df(self, table_name: str) -> pd.DataFrame:
        raise NotImplementedError("connector must implement _read_table_df")

    # ------------------------------------------------------------------
    # Row helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        """Make pandas/NumPy values comparable across sources (NaN → None, numpy scalars → Python)."""
        try:
            if value is None or pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass  # pd.isna chokes on lists/arrays — fall through to str()
        item = getattr(value, "item", None)
        if callable(item):
            try:
                value = value.item()  # numpy scalar → native Python scalar
            except (ValueError, AttributeError):
                pass
        if isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    def _rows_from_df(self, df: pd.DataFrame, columns: List[str]) -> List[Dict[str, Any]]:
        records = df.to_dict("records")
        return [
            {c: self._normalize_value(rec.get(c)) for c in columns}
            for rec in records
        ]

    # ------------------------------------------------------------------
    # Diff primitives (called by the algorithm layer when SUPPORTS_SQL=False)
    # ------------------------------------------------------------------

    def count_rows(self, table_name: str) -> int:
        return len(self._read_table_df(table_name))

    def fetch_all_rows(self, table_name: str, columns: List[str]) -> List[Dict[str, Any]]:
        return self._rows_from_df(self._read_table_df(table_name), columns)

    def fetch_rows_by_keys(
        self,
        table_name: str,
        columns: List[str],
        key_dicts: List[Dict[str, Any]],
        key_cols: List[str],
    ) -> List[Dict[str, Any]]:
        wanted = {
            tuple(self._normalize_value(d.get(k)) for k in key_cols)
            for d in key_dicts
        }
        rows = self._rows_from_df(self._read_table_df(table_name), columns)
        return [r for r in rows if tuple(r.get(k) for k in key_cols) in wanted]

    def sample_rows(self, table_name: str, columns: List[str], n: int) -> List[Dict[str, Any]]:
        df = self._read_table_df(table_name)
        if len(df) > n:
            df = df.sample(n=n)
        return self._rows_from_df(df, columns)

    def fetch_key_hashes(
        self, table_name: str, keys: List[str], non_key_cols: List[str]
    ) -> List[Dict[str, Any]]:
        """Return one dict per row: key fields + ``_dimer_row_hash``.

        The hash is the Python MD5 row hash over the non-key columns —
        identical to the recipe used by the cross-database Python hashing
        path, so two file sides are directly comparable (CSV↔CSV,
        Parquet↔Parquet, and even CSV↔Parquet when values stringify equally).
        """
        columns = list(keys) + list(non_key_cols)
        rows: List[Dict[str, Any]] = []
        for normalized in self.fetch_all_rows(table_name, columns):
            row = {k: normalized.get(k) for k in keys}
            if non_key_cols:
                row["_dimer_row_hash"] = _python_row_hash(normalized, non_key_cols)
            rows.append(row)
        return rows
