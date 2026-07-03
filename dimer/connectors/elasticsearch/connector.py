"""Elasticsearch connector — search-engine (SRCH) source with client-side diff primitives."""

from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from dimer.core.algorithms.base import _WHERE_CHUNK_SIZE, _python_row_hash
from dimer.core.base import DataSourceConnector
from dimer.core.models import ColumnMetadata, ConnectionMethod, TableMetadata

logger = structlog.get_logger(__name__)


class ElasticsearchConnector(DataSourceConnector):
    """Elasticsearch implementation for search-engine diffing (UC1/UC2, SRCH family).

    Elasticsearch has no SQL joins or server-side row-hash pushdown (the SQL
    plugin can run flat `SELECT`s but not the hash/aggregate-hash expressions
    the SQL connectors rely on), so this connector follows the same
    non-SQL contract as MongoDB: ``SUPPORTS_SQL = False``, with the five
    data-access primitives implemented directly against the REST API.

    ``table_name`` is the index name. The document ``_id`` is the synthetic
    identifying column (use ``keys=["_id"]``).

    ``fetch_key_hashes`` computes the row hash client-side (``_python_row_hash``),
    so two Elasticsearch sides are hash-comparable in HASH_DIFF/BLOOM.
    JOIN_DIFF and BISECTION are not supported (no joins; no aggregate-hash
    pushdown).

    Schema (UC2) is read from the real index mapping (``GET <index>/_mapping``),
    not sampled — Elasticsearch enforces a mapping per field once it's seen a
    value, unlike MongoDB's fully schemaless documents.
    """

    SUPPORTS_SQL = False
    DEFAULT_PORT = 9200
    DIALECTS: Dict[str, str] = {}  # no SQL dialect — REST API primitives are hand-rolled

    def get_required_params(self) -> List[str]:
        return ["host"]

    def get_connection_methods(self) -> List[ConnectionMethod]:
        return [ConnectionMethod.NATIVE]

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _connect_native(self) -> Any:
        try:
            from elasticsearch import Elasticsearch
        except ImportError:
            raise ImportError("elasticsearch is required for the Elasticsearch connector")

        cfg = self.connection_config
        scheme = cfg.extra_params.get("scheme", "https")
        hosts = [f"{scheme}://{cfg.host}:{cfg.port or self.DEFAULT_PORT}"]
        kwargs: Dict[str, Any] = {"request_timeout": cfg.connect_timeout}
        if cfg.username:
            kwargs["basic_auth"] = (cfg.username, cfg.password)
        api_key = cfg.extra_params.get("api_key")
        if api_key:
            kwargs["api_key"] = api_key
        if not cfg.extra_params.get("verify_certs", True):
            kwargs["verify_certs"] = False

        client = Elasticsearch(hosts, **kwargs)
        client.info()  # force a round-trip so connection fallback logic sees real failures
        logger.info("Elasticsearch connection established", hosts=hosts)
        return client

    def _client(self):
        if not self.connection:
            self.connect()
        return self.connection

    # ------------------------------------------------------------------
    # Diff primitives (called by the algorithm layer when SUPPORTS_SQL=False)
    # ------------------------------------------------------------------

    def count_rows(self, table_name: str) -> int:
        return int(self._client().count(index=table_name)["count"])

    def fetch_all_rows(self, table_name: str, columns: List[str]) -> List[Dict[str, Any]]:
        from elasticsearch.helpers import scan

        source_fields = [c for c in columns if c != "_id"]
        docs = scan(self._client(), index=table_name, query={"query": {"match_all": {}}},
                    _source=source_fields or True)
        return [self._normalize_doc(d, columns) for d in docs]

    def fetch_rows_by_keys(
        self,
        table_name: str,
        columns: List[str],
        key_dicts: List[Dict[str, Any]],
        key_cols: List[str],
    ) -> List[Dict[str, Any]]:
        client = self._client()
        ids = [d.get("_id") for d in key_dicts if d.get("_id") is not None]
        source_fields = [c for c in columns if c != "_id"]
        rows: List[Dict[str, Any]] = []
        for i in range(0, len(ids), _WHERE_CHUNK_SIZE):
            chunk = ids[i:i + _WHERE_CHUNK_SIZE]
            resp = client.mget(index=table_name, body={"ids": chunk},
                                _source=source_fields or True)
            for doc in resp.get("docs", []):
                if doc.get("found"):
                    rows.append(self._normalize_doc(doc, columns))
        return rows

    def sample_rows(self, table_name: str, columns: List[str], n: int) -> List[Dict[str, Any]]:
        source_fields = [c for c in columns if c != "_id"]
        resp = self._client().search(
            index=table_name,
            size=n,
            _source=source_fields or True,
            query={"function_score": {"query": {"match_all": {}}, "random_score": {}}},
        )
        return [self._normalize_doc(h, columns) for h in resp["hits"]["hits"]]

    def fetch_key_hashes(
        self, table_name: str, keys: List[str], non_key_cols: List[str]
    ) -> List[Dict[str, Any]]:
        """Return one dict per document: key fields + ``_dimer_row_hash``.

        The hash is the Python MD5 row hash over the non-key fields —
        identical to the recipe used by the cross-database Python hashing
        path, so two Elasticsearch sides are directly comparable.
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
    # Document helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    def _normalize_doc(self, doc: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
        source = doc.get("_source", {})
        row = {c: self._normalize_value(source.get(c)) for c in columns if c != "_id"}
        row["_id"] = doc.get("_id")
        return row

    # ------------------------------------------------------------------
    # DataSourceConnector interface
    # ------------------------------------------------------------------

    def _execute_query_internal(
        self, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "Elasticsearch has no relational SQL surface; the algorithm "
            "layer uses the SUPPORTS_SQL=False primitives instead"
        )

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """Read the real schema from the index mapping (a true catalog, not sampled)."""
        mapping = self._client().indices.get_mapping(index=table_name)
        index_mapping = next(iter(mapping.values()))
        properties = index_mapping.get("mappings", {}).get("properties", {})

        columns = [
            ColumnMetadata(name="_id", data_type="keyword", nullable=False, is_primary_key=True)
        ]
        for field, spec in properties.items():
            field_type = spec.get("type")
            if field_type is None and "properties" in spec:
                field_type = "object"
            columns.append(
                ColumnMetadata(
                    name=field,
                    data_type=field_type or "unknown",
                    nullable=True,  # Elasticsearch does not enforce non-null fields
                )
            )

        row_count = self.count_rows(table_name)
        return TableMetadata(columns=columns, name=table_name, schema=schema_name, row_count=row_count)

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        resp = self._client().search(index=table_name, size=limit, query={"match_all": {}})
        rows = []
        for h in resp["hits"]["hits"]:
            row = dict(h.get("_source", {}))
            row["_id"] = h.get("_id")
            rows.append(row)
        return pd.DataFrame(rows)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        indices = self._client().indices.get_alias(index="*")
        return sorted(k for k in indices.keys() if not k.startswith("."))

    def _list_schemas_internal(self) -> List[str]:
        # Elasticsearch has no schema/database concept above the index level.
        return []

    def _get_test_query(self) -> str:
        return "_cluster/health"

    def test_connection(self) -> bool:
        try:
            if not self.connection:
                self.connect()
            return bool(self.connection.ping())
        except Exception as e:
            logger.warning("Elasticsearch connection test failed", error=str(e))
            return False

    def close(self) -> None:
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                self.connection_method_used = None
            logger.info("Elasticsearch connection closed successfully")
        except Exception as e:
            logger.error("Error closing Elasticsearch connection", error=str(e))
