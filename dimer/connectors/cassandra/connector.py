"""Cassandra connector — wide-column (WIDE) source with client-side diff primitives."""

from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from dimer.core.algorithms.base import _python_row_hash
from dimer.core.base import DataSourceConnector
from dimer.core.models import ColumnMetadata, ConnectionMethod, TableMetadata

logger = structlog.get_logger(__name__)


class CassandraConnector(DataSourceConnector):
    """Cassandra implementation for wide-column store diffing (UC1/UC2, WIDE family).

    CQL is SQL-shaped but deliberately not full SQL — no joins, no ``OR`` in
    ``WHERE``, no server-side hash/aggregate-hash functions — so this
    connector follows the same non-SQL contract as MongoDB/Redis rather than
    the ``DIALECTS``-based SQL connectors: ``SUPPORTS_SQL = False`` and the
    five data-access primitives are implemented directly against CQL.

    ``table_name`` is ``keyspace.table`` (or a bare table name resolved
    against the configured keyspace, i.e. ``ConnectionConfig.database``).

    Because CQL has no row-hash pushdown, ``fetch_key_hashes`` computes the
    MD5 row hash client-side (``_python_row_hash``), so two Cassandra sides
    are hash-comparable in HASH_DIFF/BLOOM. JOIN_DIFF and BISECTION are not
    supported (no joins; no ``NTILE``/aggregate-hash pushdown).

    Schema (UC2) is read from the real catalog (``system_schema.columns``),
    not sampled — Cassandra tables have a genuine, enforced schema per
    partition/clustering/regular column, unlike MongoDB/Redis.
    """

    SUPPORTS_SQL = False
    DEFAULT_PORT = 9042
    DIALECTS: Dict[str, str] = {}  # no SQL dialect — CQL primitives are hand-rolled

    def get_required_params(self) -> List[str]:
        return ["host", "database"]

    def get_connection_methods(self) -> List[ConnectionMethod]:
        return [ConnectionMethod.NATIVE]

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _connect_native(self) -> Any:
        try:
            from cassandra.auth import PlainTextAuthProvider
            from cassandra.cluster import Cluster
        except ImportError:
            raise ImportError("cassandra-driver is required for the Cassandra connector")

        cfg = self.connection_config
        auth_provider = None
        if cfg.username:
            auth_provider = PlainTextAuthProvider(username=cfg.username, password=cfg.password)

        cluster = Cluster(
            [cfg.host],
            port=cfg.port or self.DEFAULT_PORT,
            auth_provider=auth_provider,
            connect_timeout=cfg.connect_timeout,
        )
        session = cluster.connect(cfg.database)
        session.execute("SELECT release_version FROM system.local")
        logger.info("Cassandra connection established", keyspace=cfg.database)
        return session

    def _session(self):
        if not self.connection:
            self.connect()
        return self.connection

    def _resolve(self, table_name: str) -> "tuple[str, str]":
        """Return (keyspace, table); ``keyspace.table`` overrides the configured keyspace."""
        name = table_name.replace('"', "")
        if "." in name:
            ks, tbl = name.split(".", 1)
            return ks, tbl
        return self.connection_config.database, name

    @staticmethod
    def _quote_col(col: str) -> str:
        return f'"{col}"'

    # ------------------------------------------------------------------
    # Diff primitives (called by the algorithm layer when SUPPORTS_SQL=False)
    # ------------------------------------------------------------------

    def count_rows(self, table_name: str) -> int:
        ks, tbl = self._resolve(table_name)
        row = self._session().execute(f"SELECT COUNT(*) FROM {ks}.{tbl}").one()
        return int(row[0]) if row else 0

    def fetch_all_rows(self, table_name: str, columns: List[str]) -> List[Dict[str, Any]]:
        ks, tbl = self._resolve(table_name)
        cols_sql = ", ".join(self._quote_col(c) for c in columns)
        result = self._session().execute(f"SELECT {cols_sql} FROM {ks}.{tbl}")
        return [self._normalize_row(dict(r._asdict()), columns) for r in result]

    def fetch_rows_by_keys(
        self,
        table_name: str,
        columns: List[str],
        key_dicts: List[Dict[str, Any]],
        key_cols: List[str],
    ) -> List[Dict[str, Any]]:
        ks, tbl = self._resolve(table_name)
        cols_sql = ", ".join(self._quote_col(c) for c in columns)
        session = self._session()
        rows: List[Dict[str, Any]] = []
        for d in key_dicts:
            where = " AND ".join(f"{self._quote_col(k)} = %s" for k in key_cols)
            values = [d.get(k) for k in key_cols]
            query = f"SELECT {cols_sql} FROM {ks}.{tbl} WHERE {where} ALLOW FILTERING"
            for r in session.execute(query, values):
                rows.append(self._normalize_row(dict(r._asdict()), columns))
        return rows

    def sample_rows(self, table_name: str, columns: List[str], n: int) -> List[Dict[str, Any]]:
        ks, tbl = self._resolve(table_name)
        cols_sql = ", ".join(self._quote_col(c) for c in columns)
        result = self._session().execute(f"SELECT {cols_sql} FROM {ks}.{tbl} LIMIT {n}")
        return [self._normalize_row(dict(r._asdict()), columns) for r in result]

    def fetch_key_hashes(
        self, table_name: str, keys: List[str], non_key_cols: List[str]
    ) -> List[Dict[str, Any]]:
        """Return one dict per row: key fields + ``_dimer_row_hash``.

        The hash is the Python MD5 row hash over the non-key columns —
        identical to the recipe used by the cross-database Python hashing
        path, so two Cassandra sides are directly comparable.
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
    # Row helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    def _normalize_row(self, row: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
        return {c: self._normalize_value(row.get(c)) for c in columns}

    # ------------------------------------------------------------------
    # DataSourceConnector interface
    # ------------------------------------------------------------------

    def _execute_query_internal(
        self, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "Cassandra has no cross-table SQL surface; the algorithm layer "
            "uses the SUPPORTS_SQL=False primitives instead"
        )

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """Read the real schema from ``system_schema.columns`` (a true catalog, not sampled)."""
        ks, tbl = self._resolve(f"{schema_name}.{table_name}" if schema_name else table_name)
        session = self._session()
        result = session.execute(
            "SELECT column_name, type, kind FROM system_schema.columns "
            "WHERE keyspace_name = %s AND table_name = %s",
            [ks, tbl],
        )
        columns = [
            ColumnMetadata(
                name=r.column_name,
                data_type=r.type,
                nullable=(r.kind == "regular"),  # partition/clustering keys are non-null
                is_primary_key=(r.kind in ("partition_key", "clustering")),
            )
            for r in result
        ]

        row_count = None
        try:
            count_row = session.execute(f"SELECT COUNT(*) FROM {ks}.{tbl}").one()
            row_count = int(count_row[0]) if count_row else None
        except Exception as e:
            logger.warning("Cassandra COUNT(*) failed", error=str(e))

        return TableMetadata(
            columns=columns,
            name=tbl,
            schema=ks,
            row_count=row_count,
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        ks, tbl = self._resolve(f"{schema_name}.{table_name}" if schema_name else table_name)
        result = self._session().execute(f"SELECT * FROM {ks}.{tbl} LIMIT {limit}")
        return pd.DataFrame([dict(r._asdict()) for r in result])

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        ks = schema_name or self.connection_config.database
        result = self._session().execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s", [ks]
        )
        return sorted(r.table_name for r in result)

    def _list_schemas_internal(self) -> List[str]:
        result = self._session().execute("SELECT keyspace_name FROM system_schema.keyspaces")
        return sorted(r.keyspace_name for r in result)

    def _get_test_query(self) -> str:
        return "SELECT release_version FROM system.local"

    def test_connection(self) -> bool:
        try:
            if not self.connection:
                self.connect()
            self.connection.execute(self._get_test_query())
            return True
        except Exception as e:
            logger.warning("Cassandra connection test failed", error=str(e))
            return False

    def close(self) -> None:
        try:
            if self.connection:
                cluster = self.connection.cluster
                self.connection.shutdown()
                cluster.shutdown()
                self.connection = None
                self.connection_method_used = None
            logger.info("Cassandra connection closed successfully")
        except Exception as e:
            logger.error("Error closing Cassandra connection", error=str(e))
