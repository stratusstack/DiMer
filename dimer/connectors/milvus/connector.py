"""Milvus connector — vector-store (VEC) source with client-side diff primitives."""

from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from dimer.core.algorithms.base import _WHERE_CHUNK_SIZE, _python_row_hash
from dimer.core.base import DataSourceConnector
from dimer.core.models import ColumnMetadata, ConnectionMethod, TableMetadata

logger = structlog.get_logger(__name__)

QUERY_PAGE_SIZE = 500


class MilvusConnector(DataSourceConnector):
    """Milvus implementation for vector-store diffing (UC1/UC2, VEC family).

    Milvus has no SQL surface and no server-side row-hash function, so this
    connector follows the same non-SQL contract as Qdrant/Pinecone:
    ``SUPPORTS_SQL = False``, with the five data-access primitives
    implemented against the simplified ``MilvusClient`` (pymilvus >= 2.4)
    ``query``/``get``/``get_collection_stats`` API.

    ``table_name`` is the collection name. Entities are identified by
    whatever field the collection declares as its primary key (looked up
    once per call via ``describe_collection`` and exposed as the synthetic
    column ``_id`` — use ``keys=["_id"]``). Scalar fields are exposed as-is;
    the collection's (first) vector field is exposed as the column
    ``_vector`` (stringified for hashing/comparison — for true
    float-tolerant vector comparison use EMBEDDING_SIMILARITY with
    ``vector_column="_vector"`` instead of an exact diff). Collections with
    more than one vector field only expose the first — diff a specific
    field directly via a future ``vector_field`` override if that's needed.

    ``fetch_key_hashes`` computes the row hash client-side
    (``_python_row_hash``), so two Milvus sides are hash-comparable in
    HASH_DIFF/BLOOM. JOIN_DIFF and BISECTION are not supported (no joins;
    no aggregate-hash pushdown).

    Schema (UC2) is read from the real collection schema
    (``describe_collection``), not sampled — Milvus collections have a
    genuine, enforced schema per field, unlike MongoDB/Qdrant payloads.
    """

    SUPPORTS_SQL = False
    DEFAULT_PORT = 19530
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
            from pymilvus import MilvusClient
        except ImportError:
            raise ImportError("pymilvus is required for the Milvus connector")

        cfg = self.connection_config
        uri = cfg.extra_params.get("uri") or f"http://{cfg.host}:{cfg.port or self.DEFAULT_PORT}"
        client = MilvusClient(
            uri=uri,
            token=cfg.extra_params.get("token") or cfg.password,
            db_name=cfg.database or cfg.extra_params.get("db_name", "default"),
        )
        client.list_collections()  # force a round-trip
        logger.info("Milvus connection established", uri=uri)
        return client

    def _client(self):
        if not self.connection:
            self.connect()
        return self.connection

    def _describe(self, table_name: str) -> Dict[str, Any]:
        return self._client().describe_collection(collection_name=table_name)

    def _pk_field(self, table_name: str) -> str:
        for f in self._describe(table_name).get("fields", []):
            if f.get("is_primary"):
                return f["name"]
        return "id"

    def _vector_field(self, table_name: str) -> Optional[str]:
        for f in self._describe(table_name).get("fields", []):
            if "dim" in (f.get("params") or {}):
                return f["name"]
        return None

    def _output_fields(
        self, columns: List[str], pk_field: str, vector_field: Optional[str]
    ) -> List[str]:
        fields = [c for c in columns if c not in ("_id", "_vector")]
        if pk_field not in fields:
            fields = [pk_field] + fields
        if "_vector" in columns and vector_field and vector_field not in fields:
            fields = fields + [vector_field]
        return fields

    # ------------------------------------------------------------------
    # Diff primitives (called by the algorithm layer when SUPPORTS_SQL=False)
    # ------------------------------------------------------------------

    def count_rows(self, table_name: str) -> int:
        stats = self._client().get_collection_stats(collection_name=table_name)
        return int(stats.get("row_count", 0))

    def fetch_all_rows(self, table_name: str, columns: List[str]) -> List[Dict[str, Any]]:
        pk_field = self._pk_field(table_name)
        vector_field = self._vector_field(table_name)
        output_fields = self._output_fields(columns, pk_field, vector_field)
        client = self._client()
        rows: List[Dict[str, Any]] = []
        offset = 0
        while True:
            page = client.query(
                collection_name=table_name,
                filter="",
                output_fields=output_fields,
                limit=QUERY_PAGE_SIZE,
                offset=offset,
            )
            if not page:
                break
            rows.extend(self._normalize_entity(e, columns, pk_field, vector_field) for e in page)
            if len(page) < QUERY_PAGE_SIZE:
                break
            offset += QUERY_PAGE_SIZE
        return rows

    def fetch_rows_by_keys(
        self,
        table_name: str,
        columns: List[str],
        key_dicts: List[Dict[str, Any]],
        key_cols: List[str],
    ) -> List[Dict[str, Any]]:
        pk_field = self._pk_field(table_name)
        vector_field = self._vector_field(table_name)
        output_fields = self._output_fields(columns, pk_field, vector_field)
        client = self._client()
        ids = [d.get("_id") for d in key_dicts if d.get("_id") is not None]
        rows: List[Dict[str, Any]] = []
        for i in range(0, len(ids), _WHERE_CHUNK_SIZE):
            chunk = ids[i:i + _WHERE_CHUNK_SIZE]
            entities = client.get(collection_name=table_name, ids=chunk, output_fields=output_fields)
            rows.extend(self._normalize_entity(e, columns, pk_field, vector_field) for e in entities)
        return rows

    def sample_rows(self, table_name: str, columns: List[str], n: int) -> List[Dict[str, Any]]:
        pk_field = self._pk_field(table_name)
        vector_field = self._vector_field(table_name)
        output_fields = self._output_fields(columns, pk_field, vector_field)
        entities = self._client().query(
            collection_name=table_name, filter="", output_fields=output_fields, limit=n
        )
        return [self._normalize_entity(e, columns, pk_field, vector_field) for e in entities]

    def fetch_key_hashes(
        self, table_name: str, keys: List[str], non_key_cols: List[str]
    ) -> List[Dict[str, Any]]:
        """Return one dict per entity: key fields + ``_dimer_row_hash``.

        The hash is the Python MD5 row hash over the non-key fields (and
        ``_vector`` if included) — identical to the recipe used by the
        cross-database Python hashing path, so two Milvus sides are directly
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
    # Entity helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    def _normalize_entity(
        self,
        entity: Dict[str, Any],
        columns: List[str],
        pk_field: str,
        vector_field: Optional[str],
    ) -> Dict[str, Any]:
        row = {
            c: self._normalize_value(entity.get(c))
            for c in columns
            if c not in ("_id", "_vector")
        }
        row["_id"] = entity.get(pk_field)
        if "_vector" in columns:
            raw = entity.get(vector_field) if vector_field else None
            row["_vector"] = str(list(raw)) if raw is not None else None
        return row

    # ------------------------------------------------------------------
    # DataSourceConnector interface
    # ------------------------------------------------------------------

    def _execute_query_internal(
        self, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "Milvus has no SQL surface; the algorithm layer uses the "
            "SUPPORTS_SQL=False primitives instead"
        )

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """Read the real schema from ``describe_collection`` (a true catalog, not sampled)."""
        info = self._describe(table_name)
        pk_field = self._pk_field(table_name)
        vector_field = self._vector_field(table_name)

        columns = []
        for f in info.get("fields", []):
            name = f["name"]
            params = f.get("params") or {}
            if name == pk_field:
                columns.append(
                    ColumnMetadata(
                        name="_id",
                        data_type=str(f.get("type", "unknown")),
                        nullable=False,
                        is_primary_key=True,
                    )
                )
            elif name == vector_field:
                columns.append(
                    ColumnMetadata(
                        name="_vector",
                        data_type=f"vector(dim={params.get('dim')})",
                        nullable=False,
                    )
                )
            else:
                columns.append(
                    ColumnMetadata(
                        name=name,
                        data_type=str(f.get("type", "unknown")),
                        nullable=not f.get("is_primary", False),
                    )
                )

        stats = self._client().get_collection_stats(collection_name=table_name)
        return TableMetadata(
            columns=columns,
            name=table_name,
            schema=schema_name,
            row_count=int(stats.get("row_count", 0)),
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        rows = self.sample_rows(table_name, ["_id", "_vector"], limit)
        return pd.DataFrame(rows)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        return sorted(self._client().list_collections())

    def _list_schemas_internal(self) -> List[str]:
        # Milvus "databases" are the closest analogue to a schema namespace
        # (multi-tenant deployments only); standalone/single-tenant clusters
        # expose just "default".
        client = self._client()
        if not hasattr(client, "list_databases"):
            return []
        return sorted(client.list_databases())

    def _get_test_query(self) -> str:
        return "list_collections"

    def test_connection(self) -> bool:
        try:
            if not self.connection:
                self.connect()
            self.connection.list_collections()
            return True
        except Exception as e:
            logger.warning("Milvus connection test failed", error=str(e))
            return False

    def close(self) -> None:
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                self.connection_method_used = None
            logger.info("Milvus connection closed successfully")
        except Exception as e:
            logger.error("Error closing Milvus connection", error=str(e))
