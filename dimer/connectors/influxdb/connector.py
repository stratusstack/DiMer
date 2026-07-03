"""InfluxDB connector — time-series (TS) source with client-side diff primitives."""

from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from dimer.core.algorithms.base import _WHERE_CHUNK_SIZE, _python_row_hash
from dimer.core.base import DataSourceConnector
from dimer.core.models import ColumnMetadata, ConnectionMethod, TableMetadata

logger = structlog.get_logger(__name__)


class InfluxDBConnector(DataSourceConnector):
    """InfluxDB implementation for time-series diffing (UC1/UC2, TS family).

    Targets InfluxDB 1.x / InfluxQL (also served by InfluxDB Cloud/OSS 2.x
    via its 1.x compatibility API), since InfluxQL's ``database`` concept
    maps directly onto ``ConnectionConfig.database`` and its ``SELECT`` shape
    is the closest of the six new families to real SQL. It is still not
    relational SQL — no joins, no server-side row-hash function — so this
    connector follows the same non-SQL contract as MongoDB:
    ``SUPPORTS_SQL = False``.

    ``table_name`` is the measurement name. A row is one point; InfluxDB has
    no per-point primary key, so the point ``time`` is the synthetic
    identifying column (use ``keys=["time"]`` — this assumes distinct
    timestamps per point, which holds for most single-series measurements
    but not for measurements with multiple concurrent tag series at the same
    timestamp; include the differentiating tag columns in ``keys`` too if so).

    ``fetch_key_hashes`` computes the row hash client-side (``_python_row_hash``),
    so two InfluxDB sides are hash-comparable in HASH_DIFF/BLOOM. JOIN_DIFF
    and BISECTION are not supported (no joins; no aggregate-hash pushdown).

    Schema (UC2) is read from the real catalog (``SHOW FIELD KEYS`` /
    ``SHOW TAG KEYS``), not sampled — InfluxDB tracks field types and tag
    keys internally per measurement.
    """

    SUPPORTS_SQL = False
    DEFAULT_PORT = 8086
    DIALECTS: Dict[str, str] = {}  # no SQL dialect — InfluxQL primitives are hand-rolled

    def get_required_params(self) -> List[str]:
        return ["host", "database"]

    def get_connection_methods(self) -> List[ConnectionMethod]:
        return [ConnectionMethod.NATIVE]

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _connect_native(self) -> Any:
        try:
            from influxdb import InfluxDBClient
        except ImportError:
            raise ImportError("influxdb is required for the InfluxDB connector")

        cfg = self.connection_config
        client = InfluxDBClient(
            host=cfg.host,
            port=cfg.port or self.DEFAULT_PORT,
            username=cfg.username,
            password=cfg.password,
            database=cfg.database,
            timeout=cfg.connect_timeout,
            ssl=cfg.extra_params.get("ssl", False),
        )
        client.ping()  # force a round-trip so connection fallback logic sees real failures
        logger.info("InfluxDB connection established", database=cfg.database)
        return client

    def _client(self):
        if not self.connection:
            self.connect()
        return self.connection

    @staticmethod
    def _quote(name: str) -> str:
        return f'"{name}"'

    def _select_columns(self, columns: List[str]) -> str:
        non_time = [c for c in columns if c != "time"]
        return ", ".join(self._quote(c) for c in non_time) if non_time else "*"

    # ------------------------------------------------------------------
    # Diff primitives (called by the algorithm layer when SUPPORTS_SQL=False)
    # ------------------------------------------------------------------

    def count_rows(self, table_name: str) -> int:
        result = self._client().query(f'SELECT COUNT(*) FROM "{table_name}"')
        points = list(result.get_points())
        if not points:
            return 0
        counts = [v for k, v in points[0].items() if k != "time" and isinstance(v, (int, float))]
        return int(max(counts)) if counts else 0

    def fetch_all_rows(self, table_name: str, columns: List[str]) -> List[Dict[str, Any]]:
        select = self._select_columns(columns)
        result = self._client().query(f'SELECT {select} FROM "{table_name}"')
        return [self._normalize_point(p, columns) for p in result.get_points()]

    def fetch_rows_by_keys(
        self,
        table_name: str,
        columns: List[str],
        key_dicts: List[Dict[str, Any]],
        key_cols: List[str],
    ) -> List[Dict[str, Any]]:
        select = self._select_columns(columns)
        client = self._client()
        rows: List[Dict[str, Any]] = []
        for i in range(0, len(key_dicts), _WHERE_CHUNK_SIZE):
            chunk = key_dicts[i:i + _WHERE_CHUNK_SIZE]
            clauses = []
            for d in chunk:
                parts = [f"{self._quote(k)} = '{d.get(k)}'" for k in key_cols]
                clauses.append("(" + " AND ".join(parts) + ")")
            where = " OR ".join(clauses)
            result = client.query(f'SELECT {select} FROM "{table_name}" WHERE {where}')
            rows.extend(self._normalize_point(p, columns) for p in result.get_points())
        return rows

    def sample_rows(self, table_name: str, columns: List[str], n: int) -> List[Dict[str, Any]]:
        select = self._select_columns(columns)
        result = self._client().query(f'SELECT {select} FROM "{table_name}" LIMIT {n}')
        return [self._normalize_point(p, columns) for p in result.get_points()]

    def fetch_key_hashes(
        self, table_name: str, keys: List[str], non_key_cols: List[str]
    ) -> List[Dict[str, Any]]:
        """Return one dict per point: key fields + ``_dimer_row_hash``.

        The hash is the Python MD5 row hash over the non-key fields/tags —
        identical to the recipe used by the cross-database Python hashing
        path, so two InfluxDB sides are directly comparable.
        """
        columns = list(keys) + list(non_key_cols)
        rows: List[Dict[str, Any]] = []
        for normalized in self.fetch_all_rows(table_name, columns):
            row = {k: normalized.get(k) for k in keys}
            if non_key_cols:
                row["_dimer_row_hash"] = _python_row_hash(normalized, non_key_cols)
            rows.append(row)
        return rows

    # ------------------------------------------------------------------
    # Point helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    def _normalize_point(self, point: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
        return {c: self._normalize_value(point.get(c)) for c in columns}

    # ------------------------------------------------------------------
    # DataSourceConnector interface
    # ------------------------------------------------------------------

    def _execute_query_internal(
        self, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "InfluxDB has no relational SQL surface; the algorithm layer "
            "uses the SUPPORTS_SQL=False primitives instead"
        )

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """Read the real schema from ``SHOW FIELD KEYS`` / ``SHOW TAG KEYS``."""
        client = self._client()
        field_result = client.query(f'SHOW FIELD KEYS FROM "{table_name}"')
        tag_result = client.query(f'SHOW TAG KEYS FROM "{table_name}"')

        columns = [
            ColumnMetadata(name="time", data_type="timestamp", nullable=False, is_primary_key=True)
        ]
        columns += [
            ColumnMetadata(name=p["tagKey"], data_type="tag/string", nullable=True)
            for p in tag_result.get_points()
        ]
        columns += [
            ColumnMetadata(name=p["fieldKey"], data_type=p["fieldType"], nullable=True)
            for p in field_result.get_points()
        ]

        row_count = self.count_rows(table_name)
        return TableMetadata(columns=columns, name=table_name, schema=schema_name, row_count=row_count)

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        result = self._client().query(f'SELECT * FROM "{table_name}" LIMIT {limit}')
        return pd.DataFrame(list(result.get_points()))

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        result = self._client().query("SHOW MEASUREMENTS")
        return sorted(p["name"] for p in result.get_points())

    def _list_schemas_internal(self) -> List[str]:
        result = self._client().query("SHOW DATABASES")
        return sorted(p["name"] for p in result.get_points())

    def _get_test_query(self) -> str:
        return "SHOW DATABASES"

    def test_connection(self) -> bool:
        try:
            if not self.connection:
                self.connect()
            return bool(self.connection.ping())
        except Exception as e:
            logger.warning("InfluxDB connection test failed", error=str(e))
            return False

    def close(self) -> None:
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                self.connection_method_used = None
            logger.info("InfluxDB connection closed successfully")
        except Exception as e:
            logger.error("Error closing InfluxDB connection", error=str(e))
