"""Weaviate connector — vector-store (VEC) source with client-side diff primitives."""

from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from dimer.core.algorithms.base import _WHERE_CHUNK_SIZE, _python_row_hash
from dimer.core.base import DataSourceConnector
from dimer.core.models import ColumnMetadata, ConnectionMethod, TableMetadata

logger = structlog.get_logger(__name__)

# Number of objects fetched to infer a collection's property schema, when
# the config-derived property list needs a data sample as a fallback.
SCHEMA_SAMPLE_SIZE = 100


class WeaviateConnector(DataSourceConnector):
    """Weaviate implementation for vector-store diffing (UC1/UC2, VEC family).

    Weaviate has no SQL surface and no server-side row-hash function, so this
    connector follows the same non-SQL contract as Qdrant/Pinecone/Milvus:
    ``SUPPORTS_SQL = False``, with the five data-access primitives
    implemented against the Weaviate v4 client's collection API
    (``collection.iterator()`` / ``query.fetch_objects()`` /
    ``aggregate.over_all()``).

    ``table_name`` is the collection name. Objects are identified by their
    Weaviate ``uuid`` (exposed as the synthetic column ``_id`` — use
    ``keys=["_id"]``). Properties are exposed as-is; the vector itself is
    exposed as the column ``_vector`` (stringified for hashing/comparison —
    for true float-tolerant vector comparison use EMBEDDING_SIMILARITY with
    ``vector_column="_vector"`` instead of an exact diff). Collections
    configured with multiple named vectors only expose the ``"default"``
    named vector (or the first one, if unnamed) — diff a specific named
    vector directly via a future ``vector_name`` override if that's needed.

    ``fetch_key_hashes`` computes the row hash client-side
    (``_python_row_hash``), so two Weaviate sides are hash-comparable in
    HASH_DIFF/BLOOM. JOIN_DIFF and BISECTION are not supported (no joins;
    no aggregate-hash pushdown).

    Schema (UC2) is read from the real collection config
    (``collection.config.get()``) — Weaviate collections have a declared
    property schema, unlike Qdrant/Pinecone payloads, so no sampling is
    needed for property names/types.
    """

    SUPPORTS_SQL = False
    DEFAULT_PORT = 8080
    DEFAULT_GRPC_PORT = 50051
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
            import weaviate
            from weaviate.auth import AuthApiKey
        except ImportError:
            raise ImportError("weaviate-client is required for the Weaviate connector")

        cfg = self.connection_config
        auth = AuthApiKey(cfg.password) if cfg.password else None
        client = weaviate.connect_to_custom(
            http_host=cfg.host,
            http_port=cfg.port or self.DEFAULT_PORT,
            http_secure=cfg.extra_params.get("http_secure", False),
            grpc_host=cfg.extra_params.get("grpc_host", cfg.host),
            grpc_port=cfg.extra_params.get("grpc_port", self.DEFAULT_GRPC_PORT),
            grpc_secure=cfg.extra_params.get("grpc_secure", False),
            auth_credentials=auth,
        )
        client.is_ready()  # force a round-trip
        logger.info("Weaviate connection established", host=cfg.host)
        return client

    def _client(self):
        if not self.connection:
            self.connect()
        return self.connection

    def _collection(self, table_name: str):
        return self._client().collections.get(table_name)

    # ------------------------------------------------------------------
    # Diff primitives (called by the algorithm layer when SUPPORTS_SQL=False)
    # ------------------------------------------------------------------

    def count_rows(self, table_name: str) -> int:
        result = self._collection(table_name).aggregate.over_all(total_count=True)
        return int(result.total_count or 0)

    def fetch_all_rows(self, table_name: str, columns: List[str]) -> List[Dict[str, Any]]:
        collection = self._collection(table_name)
        include_vector = "_vector" in columns
        return [
            self._normalize_object(obj, columns)
            for obj in collection.iterator(include_vector=include_vector)
        ]

    def fetch_rows_by_keys(
        self,
        table_name: str,
        columns: List[str],
        key_dicts: List[Dict[str, Any]],
        key_cols: List[str],
    ) -> List[Dict[str, Any]]:
        from weaviate.classes.query import Filter

        collection = self._collection(table_name)
        include_vector = "_vector" in columns
        ids = [d.get("_id") for d in key_dicts if d.get("_id") is not None]
        rows: List[Dict[str, Any]] = []
        for i in range(0, len(ids), _WHERE_CHUNK_SIZE):
            chunk = ids[i:i + _WHERE_CHUNK_SIZE]
            result = collection.query.fetch_objects(
                filters=Filter.by_id().contains_any(chunk),
                limit=len(chunk),
                include_vector=include_vector,
            )
            rows.extend(self._normalize_object(obj, columns) for obj in result.objects)
        return rows

    def sample_rows(self, table_name: str, columns: List[str], n: int) -> List[Dict[str, Any]]:
        collection = self._collection(table_name)
        result = collection.query.fetch_objects(limit=n, include_vector="_vector" in columns)
        return [self._normalize_object(obj, columns) for obj in result.objects]

    def fetch_key_hashes(
        self, table_name: str, keys: List[str], non_key_cols: List[str]
    ) -> List[Dict[str, Any]]:
        """Return one dict per object: key fields + ``_dimer_row_hash``.

        The hash is the Python MD5 row hash over the non-key properties
        (and ``_vector`` if included) — identical to the recipe used by the
        cross-database Python hashing path, so two Weaviate sides are
        directly comparable.
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
    # Object helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    @staticmethod
    def _flatten_vector(vector: Any) -> Optional[List[float]]:
        """Collapse Weaviate's (possibly named/multi-vector) ``.vector`` into one list."""
        if vector is None:
            return None
        if isinstance(vector, dict):
            if not vector:
                return None
            return vector.get("default") or next(iter(vector.values()))
        return vector

    def _normalize_object(self, obj: Any, columns: List[str]) -> Dict[str, Any]:
        props = obj.properties or {}
        row = {
            c: self._normalize_value(props.get(c))
            for c in columns
            if c not in ("_id", "_vector")
        }
        row["_id"] = str(obj.uuid)
        if "_vector" in columns:
            vec = self._flatten_vector(getattr(obj, "vector", None))
            row["_vector"] = str(list(vec)) if vec is not None else None
        return row

    # ------------------------------------------------------------------
    # DataSourceConnector interface
    # ------------------------------------------------------------------

    def _execute_query_internal(
        self, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "Weaviate has no SQL surface; the algorithm layer uses the "
            "SUPPORTS_SQL=False primitives instead"
        )

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """Read the real property schema from ``collection.config.get()`` (a true catalog)."""
        collection = self._collection(table_name)
        config = collection.config.get()

        columns = [
            ColumnMetadata(name="_id", data_type="uuid", nullable=False, is_primary_key=True),
            ColumnMetadata(name="_vector", data_type="vector", nullable=True),
        ]
        for prop in getattr(config, "properties", None) or []:
            columns.append(
                ColumnMetadata(
                    name=prop.name,
                    data_type=str(prop.data_type),
                    nullable=True,
                )
            )

        return TableMetadata(
            columns=columns,
            name=table_name,
            schema=schema_name,
            row_count=self.count_rows(table_name),
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        collection = self._collection(table_name)
        result = collection.query.fetch_objects(limit=limit)
        rows = []
        for obj in result.objects:
            row = dict(obj.properties or {})
            row["_id"] = str(obj.uuid)
            rows.append(row)
        return pd.DataFrame(rows)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        collections = self._client().collections.list_all(simple=True)
        return sorted(collections.keys())

    def _list_schemas_internal(self) -> List[str]:
        # Weaviate has no schema/database concept above the collection level.
        return []

    def _get_test_query(self) -> str:
        return "is_ready"

    def test_connection(self) -> bool:
        try:
            if not self.connection:
                self.connect()
            return bool(self.connection.is_ready())
        except Exception as e:
            logger.warning("Weaviate connection test failed", error=str(e))
            return False

    def close(self) -> None:
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                self.connection_method_used = None
            logger.info("Weaviate connection closed successfully")
        except Exception as e:
            logger.error("Error closing Weaviate connection", error=str(e))
