"""Neo4j connector — graph (GRPH) source with client-side diff primitives."""

from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from dimer.core.algorithms.base import _WHERE_CHUNK_SIZE, _python_row_hash
from dimer.core.base import DataSourceConnector
from dimer.core.models import ColumnMetadata, ConnectionMethod, TableMetadata

logger = structlog.get_logger(__name__)


class Neo4jConnector(DataSourceConnector):
    """Neo4j implementation for graph diffing (UC1/UC2, GRPH family).

    Cypher has no relational joins or server-side row-hash pushdown, so this
    connector follows the same non-SQL contract as MongoDB:
    ``SUPPORTS_SQL = False``, with the five data-access primitives
    implemented directly against Cypher via the official Bolt driver.

    A "table" is a node label: ``table_name`` selects ``MATCH (n:table_name)``.
    Nodes are identified by Neo4j's internal ``elementId(n)`` (exposed as the
    synthetic column ``_id`` — use ``keys=["_id"]``); relationships are not
    diffed by this connector.

    ``fetch_key_hashes`` computes the row hash client-side (``_python_row_hash``),
    so two Neo4j sides are hash-comparable in HASH_DIFF/BLOOM. JOIN_DIFF and
    BISECTION are not supported (no joins; no aggregate-hash pushdown).

    Schema (UC2) is read from the real catalog procedure
    ``db.schema.nodeTypeProperties()`` (Neo4j 4.3+), not sampled — Neo4j
    tracks observed property types per label internally.
    """

    SUPPORTS_SQL = False
    DEFAULT_PORT = 7687
    DIALECTS: Dict[str, str] = {}  # no SQL dialect — Cypher primitives are hand-rolled

    def get_required_params(self) -> List[str]:
        return ["host"]

    def get_connection_methods(self) -> List[ConnectionMethod]:
        return [ConnectionMethod.NATIVE]

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _connect_native(self) -> Any:
        try:
            from neo4j import GraphDatabase
        except ImportError:
            raise ImportError("neo4j is required for the Neo4j connector")

        cfg = self.connection_config
        scheme = cfg.extra_params.get("scheme", "bolt")
        uri = cfg.extra_params.get("uri") or f"{scheme}://{cfg.host}:{cfg.port or self.DEFAULT_PORT}"
        auth = (cfg.username, cfg.password) if cfg.username else None
        driver = GraphDatabase.driver(uri, auth=auth, connection_timeout=cfg.connect_timeout)
        driver.verify_connectivity()
        logger.info("Neo4j connection established", uri=uri)
        return driver

    def _driver(self):
        if not self.connection:
            self.connect()
        return self.connection

    def _database(self) -> Optional[str]:
        return self.connection_config.database or None

    def _run(self, query: str, **params: Any) -> List[Dict[str, Any]]:
        with self._driver().session(database=self._database()) as session:
            return [record.data() for record in session.run(query, **params)]

    @staticmethod
    def _quote_label(label: str) -> str:
        return label.replace("`", "")

    # ------------------------------------------------------------------
    # Diff primitives (called by the algorithm layer when SUPPORTS_SQL=False)
    # ------------------------------------------------------------------

    def count_rows(self, table_name: str) -> int:
        label = self._quote_label(table_name)
        rows = self._run(f"MATCH (n:`{label}`) RETURN count(n) AS c")
        return int(rows[0]["c"]) if rows else 0

    def fetch_all_rows(self, table_name: str, columns: List[str]) -> List[Dict[str, Any]]:
        label = self._quote_label(table_name)
        rows = self._run(f"MATCH (n:`{label}`) RETURN elementId(n) AS _id, properties(n) AS props")
        return [self._normalize_row(r, columns) for r in rows]

    def fetch_rows_by_keys(
        self,
        table_name: str,
        columns: List[str],
        key_dicts: List[Dict[str, Any]],
        key_cols: List[str],
    ) -> List[Dict[str, Any]]:
        label = self._quote_label(table_name)
        ids = [d.get("_id") for d in key_dicts if d.get("_id") is not None]
        rows: List[Dict[str, Any]] = []
        for i in range(0, len(ids), _WHERE_CHUNK_SIZE):
            chunk = ids[i:i + _WHERE_CHUNK_SIZE]
            result = self._run(
                f"MATCH (n:`{label}`) WHERE elementId(n) IN $ids "
                "RETURN elementId(n) AS _id, properties(n) AS props",
                ids=chunk,
            )
            rows.extend(self._normalize_row(r, columns) for r in result)
        return rows

    def sample_rows(self, table_name: str, columns: List[str], n: int) -> List[Dict[str, Any]]:
        label = self._quote_label(table_name)
        rows = self._run(
            f"MATCH (n:`{label}`) WITH n, rand() AS r ORDER BY r "
            "RETURN elementId(n) AS _id, properties(n) AS props LIMIT $n",
            n=n,
        )
        return [self._normalize_row(r, columns) for r in rows]

    def fetch_key_hashes(
        self, table_name: str, keys: List[str], non_key_cols: List[str]
    ) -> List[Dict[str, Any]]:
        """Return one dict per node: key fields + ``_dimer_row_hash``.

        The hash is the Python MD5 row hash over the non-key properties —
        identical to the recipe used by the cross-database Python hashing
        path, so two Neo4j sides are directly comparable.
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
    # Node helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    def _normalize_row(self, record: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
        props = record.get("props", {}) or {}
        row = {c: self._normalize_value(props.get(c)) for c in columns if c != "_id"}
        row["_id"] = record.get("_id")
        return row

    # ------------------------------------------------------------------
    # DataSourceConnector interface
    # ------------------------------------------------------------------

    def _execute_query_internal(
        self, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "Neo4j has no relational SQL surface; the algorithm layer uses "
            "the SUPPORTS_SQL=False primitives instead"
        )

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """Read the real per-label schema from ``db.schema.nodeTypeProperties()``."""
        label = self._quote_label(table_name)
        try:
            rows = self._run(
                "CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName, "
                "propertyTypes, mandatory "
                "WHERE $label IN nodeLabels AND propertyName IS NOT NULL "
                "RETURN propertyName, propertyTypes, mandatory",
                label=label,
            )
            columns = [
                ColumnMetadata(
                    name="_id", data_type="element_id", nullable=False, is_primary_key=True
                )
            ] + [
                ColumnMetadata(
                    name=r["propertyName"],
                    data_type="/".join(sorted(t.replace("`", "") for t in r["propertyTypes"] or [])) or "unknown",
                    nullable=not r.get("mandatory", False),
                )
                for r in rows
            ]
        except Exception as e:
            logger.warning(
                "db.schema.nodeTypeProperties() unavailable, falling back to sampling",
                error=str(e),
            )
            columns = self._sampled_schema(label)

        row_count = self.count_rows(table_name)
        return TableMetadata(columns=columns, name=table_name, schema=schema_name, row_count=row_count)

    def _sampled_schema(self, label: str, sample_size: int = 100) -> List[ColumnMetadata]:
        rows = self._run(
            f"MATCH (n:`{label}`) RETURN properties(n) AS props LIMIT {sample_size}"
        )
        field_types: Dict[str, set] = {}
        for r in rows:
            for field, value in (r.get("props") or {}).items():
                field_types.setdefault(field, set()).add(type(value).__name__)
        columns = [
            ColumnMetadata(name="_id", data_type="element_id", nullable=False, is_primary_key=True)
        ]
        columns += [
            ColumnMetadata(name=f, data_type="/".join(sorted(t)), nullable=True)
            for f, t in field_types.items()
        ]
        return columns

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        label = self._quote_label(table_name)
        rows = self._run(
            f"MATCH (n:`{label}`) RETURN elementId(n) AS _id, properties(n) AS props LIMIT {limit}"
        )
        data = []
        for r in rows:
            row = dict(r.get("props") or {})
            row["_id"] = r.get("_id")
            data.append(row)
        return pd.DataFrame(data)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        rows = self._run("CALL db.labels() YIELD label RETURN label")
        return sorted(r["label"] for r in rows)

    def _list_schemas_internal(self) -> List[str]:
        try:
            rows = self._run("SHOW DATABASES YIELD name RETURN name")
            return sorted(r["name"] for r in rows)
        except Exception:
            return []

    def _get_test_query(self) -> str:
        return "RETURN 1"

    def test_connection(self) -> bool:
        try:
            if not self.connection:
                self.connect()
            self.connection.verify_connectivity()
            return True
        except Exception as e:
            logger.warning("Neo4j connection test failed", error=str(e))
            return False

    def close(self) -> None:
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                self.connection_method_used = None
            logger.info("Neo4j connection closed successfully")
        except Exception as e:
            logger.error("Error closing Neo4j connection", error=str(e))
