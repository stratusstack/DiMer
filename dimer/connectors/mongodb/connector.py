"""MongoDB connector — document-store source with client-side diff primitives."""

from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from dimer.core.algorithms.base import _WHERE_CHUNK_SIZE, _python_row_hash
from dimer.core.base import DataSourceConnector
from dimer.core.models import ColumnMetadata, ConnectionMethod, TableMetadata

logger = structlog.get_logger(__name__)

# Number of documents sampled to infer a collection's schema
SCHEMA_SAMPLE_SIZE = 100


class MongoDBConnector(DataSourceConnector):
    """MongoDB implementation for document-store diffing.

    MongoDB has no SQL surface, so this connector sets ``SUPPORTS_SQL = False``
    and exposes the data-access primitives the algorithm layer calls in place
    of generated SQL:

    * ``count_rows``       — COUNT(*) equivalent
    * ``fetch_all_rows``   — full projection fetch (FULL_FETCH_DIFF)
    * ``fetch_rows_by_keys`` — ``$or`` key-filter fetch (HASH_DIFF Phase 2, SAMPLED target)
    * ``sample_rows``      — ``$sample`` aggregation (SAMPLED source)
    * ``fetch_key_hashes`` — keys + client-side MD5 row hash (HASH_DIFF Phase 1, BLOOM)

    Because MongoDB has no server-side hash/aggregate functions, the row hash
    is computed client-side with the same ``_python_row_hash`` recipe used for
    cross-database Python hashing — two MongoDB sides are therefore
    hash-comparable in HASH_DIFF/BLOOM.  JOIN_DIFF and BISECTION are not
    supported (no SQL joins; no server-side aggregate hash).

    Schemas are inferred by sampling ``SCHEMA_SAMPLE_SIZE`` documents; only
    top-level fields are compared.  ``_id`` values are stringified so
    ObjectIds behave as stable keys.
    """

    SUPPORTS_SQL = False
    DEFAULT_PORT = 27017
    DIALECTS: Dict[str, str] = {}  # no SQL dialect — everything is client-side

    def get_required_params(self) -> List[str]:
        return ["host", "database"]

    def get_connection_methods(self) -> List[ConnectionMethod]:
        return [ConnectionMethod.NATIVE]

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _connect_native(self) -> Any:
        try:
            from pymongo import MongoClient
        except ImportError:
            raise ImportError("pymongo is required for the MongoDB connector")

        cfg = self.connection_config
        uri = cfg.extra_params.get("uri")
        if uri:
            client = MongoClient(uri, connectTimeoutMS=cfg.connect_timeout * 1000)
        else:
            kwargs: Dict[str, Any] = {
                "host": cfg.host,
                "port": cfg.port or self.DEFAULT_PORT,
                "connectTimeoutMS": cfg.connect_timeout * 1000,
                "serverSelectionTimeoutMS": cfg.connect_timeout * 1000,
            }
            if cfg.username:
                kwargs["username"] = cfg.username
                kwargs["password"] = cfg.password
                kwargs["authSource"] = cfg.extra_params.get("auth_source", "admin")
            client = MongoClient(**kwargs)

        # Force a round-trip so connection fallback logic sees real failures
        client.admin.command("ping")
        logger.info("MongoDB connection established", database=cfg.database)
        return client

    def _db(self):
        if not self.connection:
            self.connect()
        return self.connection[self.connection_config.database]

    def _collection(self, table_name: str):
        """Resolve a (possibly dotted) asset name to a collection handle.

        ``db.collection`` uses the named database; a plain name uses the
        configured database.
        """
        name = table_name.replace('"', '')
        if '.' in name:
            db_name, coll = name.split('.', 1)
            if not self.connection:
                self.connect()
            return self.connection[db_name][coll]
        return self._db()[name]

    # ------------------------------------------------------------------
    # Diff primitives (called by the algorithm layer when SUPPORTS_SQL=False)
    # ------------------------------------------------------------------

    def count_rows(self, table_name: str) -> int:
        return self._collection(table_name).count_documents({})

    def fetch_all_rows(self, table_name: str, columns: List[str]) -> List[Dict[str, Any]]:
        projection = {c: 1 for c in columns}
        if '_id' not in projection:
            projection['_id'] = 0
        cursor = self._collection(table_name).find({}, projection)
        return [self._normalize_doc(doc, columns) for doc in cursor]

    def fetch_rows_by_keys(
        self,
        table_name: str,
        columns: List[str],
        key_dicts: List[Dict[str, Any]],
        key_cols: List[str],
    ) -> List[Dict[str, Any]]:
        projection = {c: 1 for c in columns}
        if '_id' not in projection:
            projection['_id'] = 0
        coll = self._collection(table_name)
        rows: List[Dict[str, Any]] = []
        for i in range(0, len(key_dicts), _WHERE_CHUNK_SIZE):
            chunk = key_dicts[i:i + _WHERE_CHUNK_SIZE]
            filters = [
                {k: self._coerce_key_value(k, d.get(k)) for k in key_cols}
                for d in chunk
            ]
            for doc in coll.find({"$or": filters}, projection):
                rows.append(self._normalize_doc(doc, columns))
        return rows

    def sample_rows(self, table_name: str, columns: List[str], n: int) -> List[Dict[str, Any]]:
        projection = {c: 1 for c in columns}
        if '_id' not in projection:
            projection['_id'] = 0
        pipeline = [{"$sample": {"size": n}}, {"$project": projection}]
        cursor = self._collection(table_name).aggregate(pipeline)
        return [self._normalize_doc(doc, columns) for doc in cursor]

    def fetch_key_hashes(
        self, table_name: str, keys: List[str], non_key_cols: List[str]
    ) -> List[Dict[str, Any]]:
        """Return one dict per document: key fields + ``_dimer_row_hash``.

        The hash is the Python MD5 row hash over the non-key columns —
        identical to the recipe used by the cross-database Python hashing
        path, so two MongoDB sides are directly comparable.
        """
        columns = list(keys) + list(non_key_cols)
        rows: List[Dict[str, Any]] = []
        projection = {c: 1 for c in columns}
        if '_id' not in projection:
            projection['_id'] = 0
        for doc in self._collection(table_name).find({}, projection):
            normalized = self._normalize_doc(doc, columns)
            row = {k: normalized.get(k) for k in keys}
            if non_key_cols:
                row['_dimer_row_hash'] = _python_row_hash(normalized, non_key_cols)
            rows.append(row)
        return rows

    # ------------------------------------------------------------------
    # Document helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        """Make BSON values comparable across sources (ObjectId → str, etc.)."""
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    def _normalize_doc(self, doc: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
        return {c: self._normalize_value(doc.get(c)) for c in columns}

    @staticmethod
    def _coerce_key_value(key: str, value: Any) -> Any:
        """Coerce stringified ObjectIds back for ``_id`` lookups."""
        if key == '_id' and isinstance(value, str):
            try:
                from bson import ObjectId
                if ObjectId.is_valid(value):
                    return ObjectId(value)
            except ImportError:
                pass
        return value

    # ------------------------------------------------------------------
    # DataSourceConnector interface
    # ------------------------------------------------------------------

    def _execute_query_internal(
        self, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "MongoDB has no SQL surface; the algorithm layer uses the "
            "SUPPORTS_SQL=False primitives instead"
        )

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """Infer a schema by sampling documents (document stores are schemaless)."""
        coll = self._collection(
            f"{schema_name}.{table_name}" if schema_name else table_name
        )

        field_types: Dict[str, set] = {}
        presence: Dict[str, int] = {}
        saw_null: Dict[str, bool] = {}
        sampled = 0
        for doc in coll.find({}, limit=SCHEMA_SAMPLE_SIZE):
            sampled += 1
            for field, value in doc.items():
                field_types.setdefault(field, set())
                presence[field] = presence.get(field, 0) + 1
                if value is None:
                    saw_null[field] = True
                else:
                    field_types[field].add(type(value).__name__)

        columns = [
            ColumnMetadata(
                name=field,
                data_type='/'.join(sorted(types)) if types else 'unknown',
                # Absent from some sampled docs, or explicitly null → nullable
                nullable=saw_null.get(field, False) or presence[field] < sampled,
                is_primary_key=(field == '_id'),
            )
            for field, types in field_types.items()
        ]

        row_count = coll.estimated_document_count()
        return TableMetadata(
            columns=columns,
            name=table_name,
            schema=schema_name,
            row_count=row_count,
            statistics={"schema_inferred_from_docs": sampled},
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        docs = list(self._collection(table_name).find({}, limit=limit))
        for doc in docs:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
        return pd.DataFrame(docs)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        if schema_name:
            if not self.connection:
                self.connect()
            return sorted(self.connection[schema_name].list_collection_names())
        return sorted(self._db().list_collection_names())

    def _list_schemas_internal(self) -> List[str]:
        if not self.connection:
            self.connect()
        return sorted(self.connection.list_database_names())

    def _get_test_query(self) -> str:
        return "ping"

    def test_connection(self) -> bool:
        try:
            if not self.connection:
                self.connect()
            self.connection.admin.command("ping")
            return True
        except Exception as e:
            logger.warning("MongoDB connection test failed", error=str(e))
            return False

    def close(self) -> None:
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                self.connection_method_used = None
            logger.info("MongoDB connection closed successfully")
        except Exception as e:
            logger.error("Error closing MongoDB connection", error=str(e))
