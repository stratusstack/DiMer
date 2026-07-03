"""Qdrant connector — vector-store (VEC) source with client-side diff primitives."""

from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from dimer.core.algorithms.base import _WHERE_CHUNK_SIZE, _python_row_hash
from dimer.core.base import DataSourceConnector
from dimer.core.models import ColumnMetadata, ConnectionMethod, TableMetadata

logger = structlog.get_logger(__name__)

# Number of points scrolled to infer a collection's payload schema
SCHEMA_SAMPLE_SIZE = 100
SCROLL_PAGE_SIZE = 500


class QdrantConnector(DataSourceConnector):
    """Qdrant implementation for vector-store diffing (UC1/UC2, VEC family).

    Qdrant has no SQL surface and no server-side row-hash function, so this
    connector follows the same non-SQL contract as MongoDB:
    ``SUPPORTS_SQL = False``, with the five data-access primitives
    implemented against the Qdrant client's scroll/retrieve/count API.

    ``table_name`` is the collection name. Points are identified by their
    Qdrant point ``id`` (exposed as the synthetic column ``_id`` — use
    ``keys=["_id"]``). Payload fields are exposed as-is; the vector itself is
    exposed as the column ``_vector`` (stringified for hashing/comparison —
    for true float-tolerant vector comparison use EMBEDDING_SIMILARITY with
    ``vector_column="_vector"`` instead of an exact diff).

    ``fetch_key_hashes`` computes the row hash client-side (``_python_row_hash``),
    so two Qdrant sides are hash-comparable in HASH_DIFF/BLOOM. JOIN_DIFF and
    BISECTION are not supported (no joins; no aggregate-hash pushdown).

    Schema (UC2) combines the real vector configuration (size + distance
    metric, read from ``get_collection``) with a sampled payload-field
    inference (``SCHEMA_SAMPLE_SIZE`` points) — Qdrant payloads are schemaless
    JSON, so field presence/type can only be inferred, not read from a catalog.
    """

    SUPPORTS_SQL = False
    DEFAULT_PORT = 6333
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
            from qdrant_client import QdrantClient
        except ImportError:
            raise ImportError("qdrant-client is required for the Qdrant connector")

        cfg = self.connection_config
        client = QdrantClient(
            host=cfg.host,
            port=cfg.port or self.DEFAULT_PORT,
            api_key=cfg.extra_params.get("api_key") or cfg.password or None,
            https=cfg.extra_params.get("https", False),
            timeout=cfg.connect_timeout,
        )
        client.get_collections()  # force a round-trip
        logger.info("Qdrant connection established", host=cfg.host)
        return client

    def _client(self):
        if not self.connection:
            self.connect()
        return self.connection

    # ------------------------------------------------------------------
    # Diff primitives (called by the algorithm layer when SUPPORTS_SQL=False)
    # ------------------------------------------------------------------

    def count_rows(self, table_name: str) -> int:
        return int(self._client().count(collection_name=table_name, exact=True).count)

    def fetch_all_rows(self, table_name: str, columns: List[str]) -> List[Dict[str, Any]]:
        client = self._client()
        rows: List[Dict[str, Any]] = []
        offset = None
        while True:
            points, offset = client.scroll(
                collection_name=table_name,
                limit=SCROLL_PAGE_SIZE,
                offset=offset,
                with_payload=True,
                with_vectors="_vector" in columns,
            )
            rows.extend(self._normalize_point(p, columns) for p in points)
            if offset is None:
                break
        return rows

    def fetch_rows_by_keys(
        self,
        table_name: str,
        columns: List[str],
        key_dicts: List[Dict[str, Any]],
        key_cols: List[str],
    ) -> List[Dict[str, Any]]:
        client = self._client()
        ids = [d.get("_id") for d in key_dicts if d.get("_id") is not None]
        rows: List[Dict[str, Any]] = []
        for i in range(0, len(ids), _WHERE_CHUNK_SIZE):
            chunk = ids[i:i + _WHERE_CHUNK_SIZE]
            points = client.retrieve(
                collection_name=table_name,
                ids=chunk,
                with_payload=True,
                with_vectors="_vector" in columns,
            )
            rows.extend(self._normalize_point(p, columns) for p in points)
        return rows

    def sample_rows(self, table_name: str, columns: List[str], n: int) -> List[Dict[str, Any]]:
        points, _ = self._client().scroll(
            collection_name=table_name,
            limit=n,
            with_payload=True,
            with_vectors="_vector" in columns,
        )
        return [self._normalize_point(p, columns) for p in points]

    def fetch_key_hashes(
        self, table_name: str, keys: List[str], non_key_cols: List[str]
    ) -> List[Dict[str, Any]]:
        """Return one dict per point: key fields + ``_dimer_row_hash``.

        The hash is the Python MD5 row hash over the non-key payload fields
        (and ``_vector`` if included) — identical to the recipe used by the
        cross-database Python hashing path, so two Qdrant sides are directly
        comparable.
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

    def _normalize_point(self, point: Any, columns: List[str]) -> Dict[str, Any]:
        payload = point.payload or {}
        row = {
            c: self._normalize_value(payload.get(c))
            for c in columns
            if c not in ("_id", "_vector")
        }
        row["_id"] = point.id
        if "_vector" in columns:
            row["_vector"] = str(point.vector) if point.vector is not None else None
        return row

    # ------------------------------------------------------------------
    # DataSourceConnector interface
    # ------------------------------------------------------------------

    def _execute_query_internal(
        self, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "Qdrant has no SQL surface; the algorithm layer uses the "
            "SUPPORTS_SQL=False primitives instead"
        )

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """Combine the real vector config with sampled payload-field inference."""
        client = self._client()
        info = client.get_collection(collection_name=table_name)
        vectors_config = info.config.params.vectors

        columns = [
            ColumnMetadata(name="_id", data_type="point_id", nullable=False, is_primary_key=True)
        ]
        if hasattr(vectors_config, "size"):
            size = vectors_config.size
            distance = str(vectors_config.distance)
        else:
            # Named/multi-vector collections: describe the default/first vector
            first = next(iter(vectors_config.values())) if vectors_config else None
            size = getattr(first, "size", None)
            distance = str(getattr(first, "distance", "unknown"))
        columns.append(
            ColumnMetadata(
                name="_vector",
                data_type=f"vector(size={size}, distance={distance})",
                nullable=False,
            )
        )

        points, _ = client.scroll(
            collection_name=table_name, limit=SCHEMA_SAMPLE_SIZE, with_payload=True
        )
        field_types: Dict[str, set] = {}
        presence: Dict[str, int] = {}
        for p in points:
            for field, value in (p.payload or {}).items():
                field_types.setdefault(field, set())
                presence[field] = presence.get(field, 0) + 1
                if value is not None:
                    field_types[field].add(type(value).__name__)
        columns += [
            ColumnMetadata(
                name=field,
                data_type="/".join(sorted(types)) if types else "unknown",
                nullable=presence.get(field, 0) < len(points),
            )
            for field, types in field_types.items()
        ]

        return TableMetadata(
            columns=columns,
            name=table_name,
            schema=schema_name,
            row_count=info.points_count,
            statistics={"schema_inferred_from_points": len(points)},
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        points, _ = self._client().scroll(collection_name=table_name, limit=limit, with_payload=True)
        rows = []
        for p in points:
            row = dict(p.payload or {})
            row["_id"] = p.id
            rows.append(row)
        return pd.DataFrame(rows)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        collections = self._client().get_collections().collections
        return sorted(c.name for c in collections)

    def _list_schemas_internal(self) -> List[str]:
        # Qdrant has no schema/database concept above the collection level.
        return []

    def _get_test_query(self) -> str:
        return "get_collections"

    def test_connection(self) -> bool:
        try:
            if not self.connection:
                self.connect()
            self.connection.get_collections()
            return True
        except Exception as e:
            logger.warning("Qdrant connection test failed", error=str(e))
            return False

    def close(self) -> None:
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                self.connection_method_used = None
            logger.info("Qdrant connection closed successfully")
        except Exception as e:
            logger.error("Error closing Qdrant connection", error=str(e))
