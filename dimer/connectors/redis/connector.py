"""Redis connector — key-value (KV) source with client-side diff primitives."""

import random
from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from dimer.core.algorithms.base import _WHERE_CHUNK_SIZE, _python_row_hash
from dimer.core.base import DataSourceConnector
from dimer.core.models import ColumnMetadata, ConnectionMethod, TableMetadata

logger = structlog.get_logger(__name__)

# Number of keys sampled to infer a "table's" (key-namespace's) schema
SCHEMA_SAMPLE_SIZE = 100

# SCAN batch size per round-trip
SCAN_COUNT = 1000


class RedisConnector(DataSourceConnector):
    """Redis implementation for key-value store diffing (UC1/UC2, KV family).

    Redis has no tables, no SQL, and no server-side hashing, so a "table" is
    modeled as a key namespace: ``table_name`` is a SCAN ``MATCH`` pattern
    (e.g. ``user:*``); a bare name without ``*`` is treated as a prefix and
    expanded to ``<name>:*``. Each matching key is a "row" — only Hash-type
    keys (``HSET``) are diffable, since they are the only Redis structure
    with named fields comparable to columns; other types are skipped with a
    warning. The redis key itself becomes the synthetic identifying column
    ``_key`` (use ``keys=["_key"]`` in the comparison config).

    Sets ``SUPPORTS_SQL = False`` and implements the same primitives as the
    MongoDB connector:

    * ``count_rows``, ``fetch_all_rows``, ``fetch_rows_by_keys``,
      ``sample_rows``, ``fetch_key_hashes``

    Because there is no server-side hash function, ``fetch_key_hashes`` uses
    the client-side MD5 recipe (``_python_row_hash``) — two Redis sides are
    therefore hash-comparable in HASH_DIFF/BLOOM. JOIN_DIFF and BISECTION are
    not supported (no joins; no aggregate hash pushdown).

    Schema (UC2) is inferred by sampling ``SCHEMA_SAMPLE_SIZE`` matching keys
    and unioning their hash field names/types, mirroring MongoDB's sampled
    inference for a schemaless store.
    """

    SUPPORTS_SQL = False
    DEFAULT_PORT = 6379
    DIALECTS: Dict[str, str] = {}  # no SQL dialect — everything is client-side

    def get_required_params(self) -> List[str]:
        return ["host"]

    def get_connection_methods(self) -> List[ConnectionMethod]:
        return [ConnectionMethod.NATIVE]

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _connect_native(self) -> Any:
        try:
            import redis
        except ImportError:
            raise ImportError("redis is required for the Redis connector")

        cfg = self.connection_config
        client = redis.Redis(
            host=cfg.host,
            port=cfg.port or self.DEFAULT_PORT,
            password=cfg.password or None,
            db=int(cfg.extra_params.get("db", 0)),
            socket_connect_timeout=cfg.connect_timeout,
            decode_responses=True,
        )
        # Force a round-trip so connection fallback logic sees real failures
        client.ping()
        logger.info("Redis connection established", db=cfg.extra_params.get("db", 0))
        return client

    def _client(self):
        if not self.connection:
            self.connect()
        return self.connection

    @staticmethod
    def _pattern(table_name: str) -> str:
        return table_name if "*" in table_name else f"{table_name}:*"

    def _scan_keys(self, table_name: str) -> List[str]:
        client = self._client()
        pattern = self._pattern(table_name)
        found: List[str] = []
        cursor = 0
        while True:
            cursor, batch = client.scan(cursor=cursor, match=pattern, count=SCAN_COUNT)
            found.extend(k for k in batch if client.type(k) == "hash")
            if cursor == 0:
                break
        return found

    def _row_for_key(self, key: str, columns: List[str]) -> Dict[str, Any]:
        data = self._client().hgetall(key)
        row: Dict[str, Any] = {c: data.get(c) for c in columns if c != "_key"}
        row["_key"] = key
        return row

    # ------------------------------------------------------------------
    # Diff primitives (called by the algorithm layer when SUPPORTS_SQL=False)
    # ------------------------------------------------------------------

    def count_rows(self, table_name: str) -> int:
        return len(self._scan_keys(table_name))

    def fetch_all_rows(self, table_name: str, columns: List[str]) -> List[Dict[str, Any]]:
        return [self._row_for_key(k, columns) for k in self._scan_keys(table_name)]

    def fetch_rows_by_keys(
        self,
        table_name: str,
        columns: List[str],
        key_dicts: List[Dict[str, Any]],
        key_cols: List[str],
    ) -> List[Dict[str, Any]]:
        client = self._client()
        rows: List[Dict[str, Any]] = []
        redis_keys = [d.get("_key") for d in key_dicts if d.get("_key")]
        for i in range(0, len(redis_keys), _WHERE_CHUNK_SIZE):
            for rk in redis_keys[i:i + _WHERE_CHUNK_SIZE]:
                if client.type(rk) == "hash":
                    rows.append(self._row_for_key(rk, columns))
        return rows

    def sample_rows(self, table_name: str, columns: List[str], n: int) -> List[Dict[str, Any]]:
        keys = self._scan_keys(table_name)
        sampled = random.sample(keys, min(n, len(keys)))
        return [self._row_for_key(k, columns) for k in sampled]

    def fetch_key_hashes(
        self, table_name: str, keys: List[str], non_key_cols: List[str]
    ) -> List[Dict[str, Any]]:
        """Return one dict per key: key fields + ``_dimer_row_hash``.

        The hash is the Python MD5 row hash over the non-key (hash field)
        columns — identical to the recipe used by the cross-database Python
        hashing path, so two Redis sides are directly comparable.
        """
        columns = list(keys) + list(non_key_cols)
        rows: List[Dict[str, Any]] = []
        for k in self._scan_keys(table_name):
            normalized = self._row_for_key(k, columns)
            row = {kk: normalized.get(kk) for kk in keys}
            if non_key_cols:
                row["_dimer_row_hash"] = _python_row_hash(normalized, non_key_cols)
            rows.append(row)
        return rows

    # ------------------------------------------------------------------
    # DataSourceConnector interface
    # ------------------------------------------------------------------

    def _execute_query_internal(
        self, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "Redis has no SQL surface; the algorithm layer uses the "
            "SUPPORTS_SQL=False primitives instead"
        )

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """Infer a schema by sampling hash keys matching the namespace pattern."""
        keys = self._scan_keys(table_name)
        sample = keys[:SCHEMA_SAMPLE_SIZE]

        field_types: Dict[str, set] = {"_key": {"str"}}
        presence: Dict[str, int] = {"_key": len(sample)}
        client = self._client()
        for k in sample:
            data = client.hgetall(k)
            for field, value in data.items():
                field_types.setdefault(field, set())
                presence[field] = presence.get(field, 0) + 1
                field_types[field].add(type(value).__name__)

        columns = [
            ColumnMetadata(
                name=field,
                data_type="/".join(sorted(types)) if types else "unknown",
                nullable=presence.get(field, 0) < len(sample),
                is_primary_key=(field == "_key"),
            )
            for field, types in field_types.items()
        ]

        return TableMetadata(
            columns=columns,
            name=table_name,
            schema=schema_name,
            row_count=len(keys),
            statistics={"schema_inferred_from_keys": len(sample)},
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        keys = self._scan_keys(table_name)[:limit]
        client = self._client()
        rows = []
        for k in keys:
            data = client.hgetall(k)
            data["_key"] = k
            rows.append(data)
        return pd.DataFrame(rows)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        """Best-effort: derive namespace prefixes from key names (``prefix:id``)."""
        client = self._client()
        prefixes = set()
        cursor = 0
        while True:
            cursor, batch = client.scan(cursor=cursor, count=SCAN_COUNT)
            for k in batch:
                if ":" in k:
                    prefixes.add(k.split(":", 1)[0])
            if cursor == 0:
                break
        return sorted(prefixes)

    def _list_schemas_internal(self) -> List[str]:
        return [str(self.connection_config.extra_params.get("db", 0))]

    def _get_test_query(self) -> str:
        return "PING"

    def test_connection(self) -> bool:
        try:
            if not self.connection:
                self.connect()
            return bool(self.connection.ping())
        except Exception as e:
            logger.warning("Redis connection test failed", error=str(e))
            return False

    def close(self) -> None:
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                self.connection_method_used = None
            logger.info("Redis connection closed successfully")
        except Exception as e:
            logger.error("Error closing Redis connection", error=str(e))
