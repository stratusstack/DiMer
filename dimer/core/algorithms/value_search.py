"""VALUE_SEARCH algorithm — column-value membership search (UC10, pushdown).

Takes the distinct values of one column in a **source** table and determines
whether/where they appear across a **target** table's columns, with occurrence
counts and bounded evidence rows.  Not a diff: no join keys, no
added/deleted/modified semantics — the output is a ``SearchRun``.

Matching modes (both pure pushdown SQL — validated for REL and DWH engines;
NSQL inherits through the wire-compatible connectors):

* ``EXACT``   — per target column, one ``WHERE col IN (...) GROUP BY`` scan
  per value chunk returns the occurrence count of every matched value.
  Index-friendly on the target side.
* ``PATTERN`` — per target column, one aggregation scan per value chunk
  computes ``SUM(CASE WHEN col LIKE '%value%' THEN 1 ELSE 0 END)`` per value
  (substring containment).  Always a full column scan.

Both sides of every comparison are text-cast using each connector's
``cast_to_text`` dialect template, so values match across type boundaries
(e.g. an integer id column against a varchar reference column).  Caveats:
the text rendering of non-string types is engine-specific, so cross-engine
searches on dates/floats may miss (documented in ALGO.md §VALUE_SEARCH);
``%``/``_`` characters inside source values act as LIKE wildcards in PATTERN
mode (an ``ESCAPE`` clause is not portable across all supported engines).

Fuzzy / phonetic / semantic modes are backlog (need UDFs, extensions, or
embedding infrastructure — see TODO_FOR_LATER.md §Value search & membership).
"""

import time
from typing import Any, Dict, List, Optional, Tuple

import structlog

from dimer.core.algorithms.base import (
    _format_sql_value,
    _get_col_value,
    _supports_sql,
    _validate_identifier,
)
from dimer.core.models import (
    SearchColumnStat,
    SearchMatch,
    SearchMode,
    SearchRun,
    ValueSearchSourceConfig,
    ValueSearchTargetConfig,
)

logger = structlog.get_logger(__name__)

# Cap on distinct source values fetched (overridable via max_values)
VALUE_SEARCH_DEFAULT_MAX_VALUES = 1000

# Values per IN (...) list in EXACT mode (mirrors _WHERE_CHUNK_SIZE)
_EXACT_CHUNK_SIZE = 500

# Values per CASE-sum aggregation query in PATTERN mode (each value adds a
# LIKE predicate to the SELECT list, so chunks are much smaller)
_PATTERN_CHUNK_SIZE = 50

# Evidence rows: fetched for the top matches only, a few rows each
_EVIDENCE_MATCH_LIMIT = 10
_EVIDENCE_ROWS_PER_MATCH = 3

# Cap on values_not_found reported back (the full count is in metadata)
_NOT_FOUND_REPORT_LIMIT = 50

# Normalised (DataTypeMapper) types with no meaningful text-equality semantics
_UNSEARCHABLE_TYPES = {"binary", "json", "array", "object", "variant", "vector"}


class ValueSearchAlgorithm:
    """UC10 — search one column's values across a target table's columns.

    Deliberately *not* a ``BaseAlgorithm`` subclass: it returns a
    ``SearchRun`` rather than a ``DiffRun`` and has source/target roles
    instead of two symmetric sides.
    """

    def __init__(
        self,
        source_connector: Any,
        target_connector: Any,
        source_config: ValueSearchSourceConfig,
        target_config: ValueSearchTargetConfig,
        mode: SearchMode = SearchMode.EXACT,
    ) -> None:
        for cfg, key, name in (
            (source_config, "fq_table_name", "source_config"),
            (source_config, "source_column", "source_config"),
            (target_config, "fq_table_name", "target_config"),
        ):
            if not cfg.get(key):
                raise ValueError(f"{name} missing required key: {key!r}")
        self._source_connector = source_connector
        self._target_connector = target_connector
        self._source_config = source_config
        self._target_config = target_config
        self._mode = mode

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> SearchRun:
        start = time.time()
        source_table = self._source_config["fq_table_name"]
        source_column = self._source_config["source_column"]
        target_table = self._target_config["fq_table_name"]

        run = SearchRun(
            source_table=source_table,
            source_column=source_column,
            target_table=target_table,
            mode=self._mode,
        )

        if not (_supports_sql(self._source_connector) and _supports_sql(self._target_connector)):
            run.error = (
                "VALUE_SEARCH requires SQL connectors on both sides "
                "(non-SQL sources are not supported yet)"
            )
            return run

        case_src = getattr(self._source_connector, "IDENTIFIER_CASE", "preserve")
        case_tgt = getattr(self._target_connector, "IDENTIFIER_CASE", "preserve")
        max_values = self._source_config.get("max_values", VALUE_SEARCH_DEFAULT_MAX_VALUES)

        try:
            values, values_truncated = self._fetch_source_values(
                source_table, source_column, max_values, case_src
            )
        except Exception as e:
            run.error = f"Failed to fetch source values: {e}"
            run.execution_time_seconds = time.time() - start
            return run

        if not values:
            run.error = f"No non-null values found in {source_table}.{source_column}"
            run.execution_time_seconds = time.time() - start
            return run

        columns, skipped_columns = self._resolve_target_columns(target_table)
        if not columns:
            run.error = "No searchable target columns (after type filtering / target_columns)"
            run.execution_time_seconds = time.time() - start
            return run

        safe_target = _validate_identifier(target_table, case_tgt)
        cast_tmpl = self._target_connector.DIALECTS.get("cast_to_text", "CAST({COL} AS VARCHAR)")

        logger.info(
            f"Searching {len(values)} distinct values across "
            f"{len(columns)} target columns ({self._mode} mode)"
        )

        matches: List[SearchMatch] = []
        column_stats: List[SearchColumnStat] = []
        found_values: set = set()

        for col in columns:
            cast_col = cast_tmpl.replace("{COL}", _validate_identifier(col, case_tgt))
            if self._mode == SearchMode.EXACT:
                counts = self._search_column_exact(safe_target, cast_col, values)
            else:
                counts = self._search_column_pattern(safe_target, cast_col, values)

            col_matches = [
                SearchMatch(value=v, column=col, occurrence_count=c, match_mode=self._mode)
                for v, c in counts.items() if c > 0
            ]
            matches.extend(col_matches)
            found_values.update(m.value for m in col_matches)
            column_stats.append(SearchColumnStat(
                column=col,
                values_matched=len(col_matches),
                total_occurrences=sum(m.occurrence_count for m in col_matches),
                hit_rate=len(col_matches) / len(values),
            ))

        # Highest-signal output first: most occurrences, then value/column
        matches.sort(key=lambda m: (-m.occurrence_count, m.value, m.column))
        column_stats.sort(key=lambda s: (-s.values_matched, -s.total_occurrences, s.column))

        self._attach_evidence(safe_target, cast_tmpl, case_tgt, matches)

        not_found = sorted(v for v in values if v not in found_values)

        run.values_searched = len(values)
        run.values_found = len(found_values)
        run.columns_searched = columns
        run.column_stats = column_stats
        run.matches = matches
        run.values_not_found = not_found[:_NOT_FOUND_REPORT_LIMIT]
        run.metadata = {
            "max_values": max_values,
            "source_values_truncated": values_truncated,
            "columns_skipped": skipped_columns,
            "values_not_found_count": len(not_found),
            "values_not_found_truncated": len(not_found) > _NOT_FOUND_REPORT_LIMIT,
            "evidence_match_limit": _EVIDENCE_MATCH_LIMIT,
            "evidence_rows_per_match": _EVIDENCE_ROWS_PER_MATCH,
        }
        run.execution_time_seconds = time.time() - start
        return run

    # ------------------------------------------------------------------
    # Source values
    # ------------------------------------------------------------------

    def _fetch_source_values(
        self, table: str, column: str, max_values: int, case: str
    ) -> Tuple[List[str], bool]:
        """Fetch distinct non-null text-cast values, capped at max_values.

        Fetches one extra row to detect truncation.  Values are text-cast on
        the source engine so their string form matches how that engine
        renders the column, then compared against text-cast target columns.
        """
        safe_table = _validate_identifier(table, case)
        safe_col = _validate_identifier(column, case)
        cast_tmpl = self._source_connector.DIALECTS.get("cast_to_text", "CAST({COL} AS VARCHAR)")
        cast_col = cast_tmpl.replace("{COL}", safe_col)
        sql = (
            f"SELECT DISTINCT {cast_col} AS _dimer_v FROM {safe_table} "
            f"WHERE {safe_col} IS NOT NULL LIMIT {max_values + 1}"
        )
        rows = self._query_rows(self._source_connector, sql)
        values = [str(_get_col_value(r, "_dimer_v")) for r in rows]
        truncated = len(values) > max_values
        return values[:max_values], truncated

    # ------------------------------------------------------------------
    # Target columns
    # ------------------------------------------------------------------

    def _resolve_target_columns(self, target_table: str) -> Tuple[List[str], List[str]]:
        """Return (searchable_columns, skipped_columns) from target metadata."""
        schema: Optional[str] = None
        table = target_table
        if "." in target_table:
            schema, table = target_table.rsplit(".", 1)
            schema = schema.split(".")[-1].strip('"')
            table = table.strip('"')
        metadata = self._target_connector.get_table_metadata(table, schema_name=schema)

        column_filter = self._target_config.get("target_columns")
        wanted = {c.lower() for c in column_filter} if column_filter else None

        searchable: List[str] = []
        skipped: List[str] = []
        for col in metadata.columns:
            if wanted is not None and col.name.lower() not in wanted:
                continue
            if col.data_type in _UNSEARCHABLE_TYPES:
                skipped.append(col.name)
            else:
                searchable.append(col.name)
        return searchable, skipped

    # ------------------------------------------------------------------
    # Per-column search
    # ------------------------------------------------------------------

    def _search_column_exact(
        self, safe_table: str, cast_col: str, values: List[str]
    ) -> Dict[str, int]:
        """EXACT mode: IN-list + GROUP BY per chunk; returns {value: count}."""
        counts: Dict[str, int] = {}
        for i in range(0, len(values), _EXACT_CHUNK_SIZE):
            chunk = values[i:i + _EXACT_CHUNK_SIZE]
            in_list = ", ".join(_format_sql_value(v) for v in chunk)
            sql = (
                f"SELECT {cast_col} AS _dimer_v, COUNT(*) AS _dimer_c "
                f"FROM {safe_table} WHERE {cast_col} IN ({in_list}) GROUP BY 1"
            )
            for row in self._query_rows(self._target_connector, sql):
                value = str(_get_col_value(row, "_dimer_v"))
                counts[value] = int(_get_col_value(row, "_dimer_c") or 0)
        return counts

    def _search_column_pattern(
        self, safe_table: str, cast_col: str, values: List[str]
    ) -> Dict[str, int]:
        """PATTERN mode: one CASE-sum aggregation scan per chunk."""
        counts: Dict[str, int] = {}
        for i in range(0, len(values), _PATTERN_CHUNK_SIZE):
            chunk = values[i:i + _PATTERN_CHUNK_SIZE]
            exprs = [
                f"SUM(CASE WHEN {cast_col} LIKE {_format_sql_value(f'%{v}%')} "
                f"THEN 1 ELSE 0 END) AS m{j}"
                for j, v in enumerate(chunk)
            ]
            sql = f"SELECT {', '.join(exprs)} FROM {safe_table}"
            rows = self._query_rows(self._target_connector, sql)
            if not rows:
                continue
            row = rows[0]
            for j, v in enumerate(chunk):
                c = _get_col_value(row, f"m{j}")
                counts[v] = int(c) if c is not None else 0
        return counts

    # ------------------------------------------------------------------
    # Evidence rows
    # ------------------------------------------------------------------

    def _attach_evidence(
        self,
        safe_table: str,
        cast_tmpl: str,
        case: str,
        matches: List[SearchMatch],
    ) -> None:
        """Fetch a few full target rows for the top matches (best-effort)."""
        for match in matches[:_EVIDENCE_MATCH_LIMIT]:
            cast_col = cast_tmpl.replace("{COL}", _validate_identifier(match.column, case))
            if self._mode == SearchMode.EXACT:
                predicate = f"{cast_col} = {_format_sql_value(match.value)}"
            else:
                predicate = f"{cast_col} LIKE {_format_sql_value(f'%{match.value}%')}"
            sql = (
                f"SELECT * FROM {safe_table} WHERE {predicate} "
                f"LIMIT {_EVIDENCE_ROWS_PER_MATCH}"
            )
            try:
                match.evidence_rows = self._query_rows(self._target_connector, sql)
            except Exception as e:
                logger.warning(f"Evidence fetch failed for {match.column}={match.value!r}: {e}")

    # ------------------------------------------------------------------
    # Shared query helper (mirror of BaseAlgorithm._query_rows)
    # ------------------------------------------------------------------

    @staticmethod
    def _query_rows(connector, sql: str) -> List[Dict[str, Any]]:
        result = connector.execute_query(sql)
        df = result.data
        if df is None or len(df) == 0:
            return []
        return df.to_dict(orient="records")
