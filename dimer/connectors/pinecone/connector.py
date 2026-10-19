"""Pinecone connector — vector-store (VEC) source with client-side diff primitives."""

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import structlog

from dimer.core.algorithms.base import _WHERE_CHUNK_SIZE, _python_row_hash
from dimer.core.base import DataSourceConnector
from dimer.core.models import ColumnMetadata, ConnectionMethod, TableMetadata

logger = structlog.get_logger(__name__)

# Number of vectors fetched to infer an index's metadata-field schema
SCHEMA_SAMPLE_SIZE = 100
LIST_PAGE_SIZE = 100


def _get(obj: Any, key: str, default: Any = None) -> Any:
    """Read ``key`` from ``obj``, whether it's a plain dict or an
    attribute-style Pinecone SDK response object.

    Dict access is tried first: a dict's own method names (``values``,
    ``items``, ``keys``, ...) shadow same-named fields under ``getattr``,
    so preferring ``.get()`` on dicts avoids silently reading the bound
    method instead of the field.
    """
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


class PineconeConnector(DataSourceConnector):
    """Pinecone implementation for vector-store diffing (UC1/UC2, VEC family).

    Pinecone has no SQL surface and no server-side row-hash function, so this
    connector follows the same non-SQL contract as Qdrant/MongoDB:
    ``SUPPORTS_SQL = False``, with the five data-access primitives
    implemented against the Pinecone client's ``list``/``fetch``/
    ``describe_index_stats`` API.

    ``table_name`` is the index name, optionally with a namespace suffix
    (``"index"`` or ``"index.namespace"`` — namespace defaults to the
    connection's ``extra_params["namespace"]``, or Pinecone's default `""`
    namespace when unset). Vectors are identified by their Pinecone vector
    ``id`` (a string, exposed as the synthetic column ``_id`` — use
    ``keys=["_id"]``). Metadata fields are exposed as-is; the embedding
    itself is exposed as the column ``_vector`` (stringified for
    hashing/comparison — for true float-tolerant vector comparison use
    EMBEDDING_SIMILARITY with ``vector_column="_vector"`` instead of an
    exact diff).

    ``fetch_key_hashes`` computes the row hash client-side
    (``_python_row_hash``), so two Pinecone sides are hash-comparable in
    HASH_DIFF/BLOOM. JOIN_DIFF and BISECTION are not supported (no joins;
    no aggregate-hash pushdown).

    Schema (UC2) combines the real vector configuration (dimension + metric,
    read from ``describe_index``) with a sampled metadata-field inference
    (``SCHEMA_SAMPLE_SIZE`` vectors) — Pinecone metadata is schemaless JSON,
    so field presence/type can only be inferred, not read from a catalog.
    """

    SUPPORTS_SQL = False
    DIALECTS: Dict[str, str] = {}  # no SQL dialect — everything is client-side

    def get_required_params(self) -> List[str]:
        # The Pinecone API key doubles as the "password" — there is no
        # host/port to dial; the client resolves index hosts via the API.
        return ["password"]

    def get_connection_methods(self) -> List[ConnectionMethod]:
        return [ConnectionMethod.NATIVE]

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _connect_native(self) -> Any:
        try:
            from pinecone import Pinecone
        except ImportError:
            raise ImportError("pinecone-client is required for the Pinecone connector")

        cfg = self.connection_config
        client = Pinecone(api_key=cfg.password or cfg.extra_params.get("api_key"))
        client.list_indexes()  # force a round-trip
        logger.info("Pinecone connection established")
        return client

    def _client(self):
        if not self.connection:
            self.connect()
        return self.connection

    def _resolve(self, table_name: str) -> Tuple[str, str]:
        """Split ``table_name`` into (index_name, namespace)."""
        if "." in table_name:
            index_name, namespace = table_name.split(".", 1)
            return index_name, namespace
        return table_name, self.connection_config.extra_params.get("namespace", "")

    def _index(self, table_name: str):
        index_name, _ = self._resolve(table_name)
        return self._client().Index(index_name)

    # ------------------------------------------------------------------
    # Diff primitives (called by the algorithm layer when SUPPORTS_SQL=False)
    # ------------------------------------------------------------------

    def count_rows(self, table_name: str) -> int:
        _, namespace = self._resolve(table_name)
        stats = self._index(table_name).describe_index_stats()
        if namespace:
            namespaces = _get(stats, "namespaces", {}) or {}
            ns_stats = _get(namespaces, namespace)
            return int(_get(ns_stats, "vector_count", 0) or 0) if ns_stats else 0
        return int(_get(stats, "total_vector_count", 0) or 0)

    def _iter_ids(self, table_name: str):
        _, namespace = self._resolve(table_name)
        index = self._index(table_name)
        for page in index.list(namespace=namespace, limit=LIST_PAGE_SIZE):
            yield list(page)

    def fetch_all_rows(self, table_name: str, columns: List[str]) -> List[Dict[str, Any]]:
        _, namespace = self._resolve(table_name)
        index = self._index(table_name)
        rows: List[Dict[str, Any]] = []
        for id_batch in self._iter_ids(table_name):
            for i in range(0, len(id_batch), _WHERE_CHUNK_SIZE):
                chunk = id_batch[i:i + _WHERE_CHUNK_SIZE]
                response = index.fetch(ids=chunk, namespace=namespace)
                rows.extend(self._normalize_vectors(response, columns))
        return rows

    def fetch_rows_by_keys(
        self,
        table_name: str,
        columns: List[str],
        key_dicts: List[Dict[str, Any]],
        key_cols: List[str],
    ) -> List[Dict[str, Any]]:
        _, namespace = self._resolve(table_name)
        index = self._index(table_name)
        ids = [d.get("_id") for d in key_dicts if d.get("_id") is not None]
        rows: List[Dict[str, Any]] = []
        for i in range(0, len(ids), _WHERE_CHUNK_SIZE):
            chunk = ids[i:i + _WHERE_CHUNK_SIZE]
            response = index.fetch(ids=chunk, namespace=namespace)
            rows.extend(self._normalize_vectors(response, columns))
        return rows

    def sample_rows(self, table_name: str, columns: List[str], n: int) -> List[Dict[str, Any]]:
        _, namespace = self._resolve(table_name)
        index = self._index(table_name)
        ids: List[str] = []
        for id_batch in self._iter_ids(table_name):
            ids.extend(id_batch)
            if len(ids) >= n:
                break
        response = index.fetch(ids=ids[:n], namespace=namespace)
        return self._normalize_vectors(response, columns)

    def fetch_key_hashes(
        self, table_name: str, keys: List[str], non_key_cols: List[str]
    ) -> List[Dict[str, Any]]:
        """Return one dict per vector: key fields + ``_dimer_row_hash``.

        The hash is the Python MD5 row hash over the non-key metadata fields
        (and ``_vector`` if included) — identical to the recipe used by the
        cross-database Python hashing path, so two Pinecone sides are
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
    # Vector helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    def _normalize_vectors(self, response: Any, columns: List[str]) -> List[Dict[str, Any]]:
        vectors = _get(response, "vectors", {}) or {}
        rows = []
        for vector_id, vector in vectors.items():
            metadata = _get(vector, "metadata", {}) or {}
            row = {
                c: self._normalize_value(metadata.get(c))
                for c in columns
                if c not in ("_id", "_vector")
            }
            row["_id"] = vector_id
            if "_vector" in columns:
                values = _get(vector, "values")
                row["_vector"] = str(list(values)) if values is not None else None
            rows.append(row)
        return rows

    # ------------------------------------------------------------------
    # DataSourceConnector interface
    # ------------------------------------------------------------------

    def _execute_query_internal(
        self, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "Pinecone has no SQL surface; the algorithm layer uses the "
            "SUPPORTS_SQL=False primitives instead"
        )

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """Combine the real vector config with sampled metadata-field inference."""
        index_name, namespace = self._resolve(table_name)
        client = self._client()
        info = client.describe_index(index_name)
        dimension = _get(info, "dimension")
        metric = _get(info, "metric")

        columns = [
            ColumnMetadata(name="_id", data_type="vector_id", nullable=False, is_primary_key=True),
            ColumnMetadata(
                name="_vector",
                data_type=f"vector(dimension={dimension}, metric={metric})",
                nullable=False,
            ),
        ]

        sample = self.sample_rows(table_name, ["_id"], SCHEMA_SAMPLE_SIZE)
        index = self._index(table_name)
        response = index.fetch(ids=[r["_id"] for r in sample], namespace=namespace)
        field_types: Dict[str, set] = {}
        presence: Dict[str, int] = {}
        vectors = _get(response, "vectors", {}) or {}
        for vector in vectors.values():
            metadata = _get(vector, "metadata", {}) or {}
            for field, value in metadata.items():
                field_types.setdefault(field, set())
                presence[field] = presence.get(field, 0) + 1
                if value is not None:
                    field_types[field].add(type(value).__name__)
        columns += [
            ColumnMetadata(
                name=field,
                data_type="/".join(sorted(types)) if types else "unknown",
                nullable=presence.get(field, 0) < len(vectors),
            )
            for field, types in field_types.items()
        ]

        return TableMetadata(
            columns=columns,
            name=table_name,
            schema=schema_name,
            row_count=self.count_rows(table_name),
            statistics={"schema_inferred_from_vectors": len(vectors)},
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        rows = self.sample_rows(table_name, ["_id", "_vector"], limit)
        return pd.DataFrame(rows)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        indexes = self._client().list_indexes()
        names = getattr(indexes, "names", None)
        if callable(names):
            return sorted(names())
        return sorted(idx["name"] if isinstance(idx, dict) else idx.name for idx in indexes)

    def _list_schemas_internal(self) -> List[str]:
        # Pinecone namespaces live within an index, not above it — there is
        # no database/schema concept to enumerate independently.
        return []

    def _get_test_query(self) -> str:
        return "list_indexes"

    def test_connection(self) -> bool:
        try:
            if not self.connection:
                self.connect()
            self.connection.list_indexes()
            return True
        except Exception as e:
            logger.warning("Pinecone connection test failed", error=str(e))
            return False

    def close(self) -> None:
        try:
            self.connection = None
            self.connection_method_used = None
            logger.info("Pinecone connection closed successfully")
        except Exception as e:
            logger.error("Error closing Pinecone connection", error=str(e))
