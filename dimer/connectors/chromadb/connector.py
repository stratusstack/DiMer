"""ChromaDB connector — vector-store (VEC) source with client-side diff primitives."""

from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from dimer.core.algorithms.base import _WHERE_CHUNK_SIZE, _python_row_hash
from dimer.core.base import DataSourceConnector
from dimer.core.models import ColumnMetadata, ConnectionMethod, TableMetadata

logger = structlog.get_logger(__name__)

# Number of records fetched to infer a collection's metadata-field schema
SCHEMA_SAMPLE_SIZE = 100
GET_PAGE_SIZE = 500


class ChromaDBConnector(DataSourceConnector):
    """ChromaDB implementation for vector-store diffing (UC1/UC2, VEC family).

    ChromaDB has no SQL surface and no server-side row-hash function, so
    this connector follows the same non-SQL contract as
    Qdrant/Pinecone/Milvus/Weaviate: ``SUPPORTS_SQL = False``, with the five
    data-access primitives implemented against the Chroma client's
    ``collection.get()`` / ``collection.count()`` API.

    ``table_name`` is the collection name. Records are identified by their
    Chroma ``id`` (a string, exposed as the synthetic column ``_id`` — use
    ``keys=["_id"]``). Metadata fields are exposed as-is; the embedding
    itself is exposed as the column ``_vector`` (stringified for
    hashing/comparison — for true float-tolerant vector comparison use
    EMBEDDING_SIMILARITY with ``vector_column="_vector"`` instead of an
    exact diff). Chroma's raw text ``document`` — the string the embedding
    was computed from, distinct from metadata — is exposed as the column
    ``_document`` when requested, since it's frequently the field worth
    diffing for a Chroma collection.

    ``fetch_key_hashes`` computes the row hash client-side
    (``_python_row_hash``), so two Chroma sides are hash-comparable in
    HASH_DIFF/BLOOM. JOIN_DIFF and BISECTION are not supported (no joins;
    no aggregate-hash pushdown).

    Schema (UC2) combines the fixed ``_id``/``_vector``/``_document``
    columns with a sampled metadata-field inference (``SCHEMA_SAMPLE_SIZE``
    records) — Chroma metadata is schemaless JSON, so field presence/type
    can only be inferred, not read from a catalog.
    """

    SUPPORTS_SQL = False
    DEFAULT_PORT = 8000
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
            import chromadb
        except ImportError:
            raise ImportError("chromadb is required for the ChromaDB connector")

        cfg = self.connection_config
        headers = {"Authorization": f"Bearer {cfg.password}"} if cfg.password else None
        client = chromadb.HttpClient(
            host=cfg.host,
            port=cfg.port or self.DEFAULT_PORT,
            ssl=cfg.extra_params.get("ssl", False),
            headers=headers,
        )
        client.heartbeat()  # force a round-trip
        logger.info("ChromaDB connection established", host=cfg.host)
        return client

    def _client(self):
        if not self.connection:
            self.connect()
        return self.connection

    def _collection(self, table_name: str):
        return self._client().get_collection(name=table_name)

    @staticmethod
    def _include_for(columns: List[str]) -> List[str]:
        include = []
        if "_vector" in columns:
            include.append("embeddings")
        if "_document" in columns:
            include.append("documents")
        if any(c not in ("_id", "_vector", "_document") for c in columns):
            include.append("metadatas")
        return include

    # ------------------------------------------------------------------
    # Diff primitives (called by the algorithm layer when SUPPORTS_SQL=False)
    # ------------------------------------------------------------------

    def count_rows(self, table_name: str) -> int:
        return int(self._collection(table_name).count())

    def fetch_all_rows(self, table_name: str, columns: List[str]) -> List[Dict[str, Any]]:
        collection = self._collection(table_name)
        include = self._include_for(columns)
        rows: List[Dict[str, Any]] = []
        offset = 0
        while True:
            result = collection.get(include=include, limit=GET_PAGE_SIZE, offset=offset)
            ids = result.get("ids") or []
            if not ids:
                break
            rows.extend(self._normalize_batch(result, columns))
            if len(ids) < GET_PAGE_SIZE:
                break
            offset += GET_PAGE_SIZE
        return rows

    def fetch_rows_by_keys(
        self,
        table_name: str,
        columns: List[str],
        key_dicts: List[Dict[str, Any]],
        key_cols: List[str],
    ) -> List[Dict[str, Any]]:
        collection = self._collection(table_name)
        include = self._include_for(columns)
        ids = [d.get("_id") for d in key_dicts if d.get("_id") is not None]
        rows: List[Dict[str, Any]] = []
        for i in range(0, len(ids), _WHERE_CHUNK_SIZE):
            chunk = ids[i:i + _WHERE_CHUNK_SIZE]
            result = collection.get(ids=chunk, include=include)
            rows.extend(self._normalize_batch(result, columns))
        return rows

    def sample_rows(self, table_name: str, columns: List[str], n: int) -> List[Dict[str, Any]]:
        collection = self._collection(table_name)
        include = self._include_for(columns)
        result = collection.get(include=include, limit=n)
        return self._normalize_batch(result, columns)

    def fetch_key_hashes(
        self, table_name: str, keys: List[str], non_key_cols: List[str]
    ) -> List[Dict[str, Any]]:
        """Return one dict per record: key fields + ``_dimer_row_hash``.

        The hash is the Python MD5 row hash over the non-key metadata
        fields (and ``_vector``/``_document`` if included) — identical to
        the recipe used by the cross-database Python hashing path, so two
        Chroma sides are directly comparable.
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
    # Record helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    @staticmethod
    def _column_at(values: Optional[List[Any]], i: int) -> Any:
        if values is None or i >= len(values):
            return None
        return values[i]

    def _normalize_batch(self, result: Dict[str, Any], columns: List[str]) -> List[Dict[str, Any]]:
        ids = result.get("ids") or []
        metadatas = result.get("metadatas")
        embeddings = result.get("embeddings")
        documents = result.get("documents")

        rows = []
        for i, record_id in enumerate(ids):
            metadata = self._column_at(metadatas, i) or {}
            row = {
                c: self._normalize_value(metadata.get(c))
                for c in columns
                if c not in ("_id", "_vector", "_document")
            }
            row["_id"] = record_id
            if "_vector" in columns:
                vec = self._column_at(embeddings, i)
                row["_vector"] = str(list(vec)) if vec is not None else None
            if "_document" in columns:
                row["_document"] = self._column_at(documents, i)
            rows.append(row)
        return rows

    # ------------------------------------------------------------------
    # DataSourceConnector interface
    # ------------------------------------------------------------------

    def _execute_query_internal(
        self, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "ChromaDB has no SQL surface; the algorithm layer uses the "
            "SUPPORTS_SQL=False primitives instead"
        )

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """Combine the fixed id/vector/document columns with sampled metadata-field inference."""
        columns = [
            ColumnMetadata(name="_id", data_type="chroma_id", nullable=False, is_primary_key=True),
            ColumnMetadata(name="_vector", data_type="vector", nullable=True),
            ColumnMetadata(name="_document", data_type="text", nullable=True),
        ]

        collection = self._collection(table_name)
        sample = collection.get(include=["metadatas"], limit=SCHEMA_SAMPLE_SIZE)
        metadatas = sample.get("metadatas") or []
        field_types: Dict[str, set] = {}
        presence: Dict[str, int] = {}
        for metadata in metadatas:
            for field, value in (metadata or {}).items():
                field_types.setdefault(field, set())
                presence[field] = presence.get(field, 0) + 1
                if value is not None:
                    field_types[field].add(type(value).__name__)
        columns += [
            ColumnMetadata(
                name=field,
                data_type="/".join(sorted(types)) if types else "unknown",
                nullable=presence.get(field, 0) < len(metadatas),
            )
            for field, types in field_types.items()
        ]

        return TableMetadata(
            columns=columns,
            name=table_name,
            schema=schema_name,
            row_count=self.count_rows(table_name),
            statistics={"schema_inferred_from_records": len(metadatas)},
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        collection = self._collection(table_name)
        result = collection.get(include=["metadatas", "documents"], limit=limit)
        ids = result.get("ids") or []
        metadatas = result.get("metadatas")
        documents = result.get("documents")
        rows = []
        for i, record_id in enumerate(ids):
            row = dict(self._column_at(metadatas, i) or {})
            row["_id"] = record_id
            row["_document"] = self._column_at(documents, i)
            rows.append(row)
        return pd.DataFrame(rows)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        collections = self._client().list_collections()
        return sorted(c if isinstance(c, str) else c.name for c in collections)

    def _list_schemas_internal(self) -> List[str]:
        # Chroma's tenant/database multi-tenancy sits above collections, but
        # is not exposed as a general-purpose listable "schema" here.
        return []

    def _get_test_query(self) -> str:
        return "heartbeat"

    def test_connection(self) -> bool:
        try:
            if not self.connection:
                self.connect()
            self.connection.heartbeat()
            return True
        except Exception as e:
            logger.warning("ChromaDB connection test failed", error=str(e))
            return False

    def close(self) -> None:
        try:
            self.connection = None
            self.connection_method_used = None
            logger.info("ChromaDB connection closed successfully")
        except Exception as e:
            logger.error("Error closing ChromaDB connection", error=str(e))
