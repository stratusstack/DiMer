"""Unit tests for the six new non-SQL store-family connectors added for
UC1 (FULL_FETCH_DIFF) and UC2 (SCHEMA_DIFF):

    KV    -> Redis          WIDE -> Cassandra
    SRCH  -> Elasticsearch  GRPH -> Neo4j
    VEC   -> Qdrant         TS   -> InfluxDB

None of the real driver packages are installed in this environment (they are
optional extras), so every test drives the connector by injecting a
``Mock()`` directly as ``connector.connection`` — this exercises exactly the
same code path ``_client()``/``_session()``/``_driver()`` would use after a
real ``connect()``, without needing network access or the driver installed.
"""

import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

from dimer.core.models import ConnectionConfig

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Redis (KV)
# ---------------------------------------------------------------------------


class TestRedisConnector:
    def _connector(self):
        from dimer.connectors.redis.connector import RedisConnector

        conn = RedisConnector(ConnectionConfig(host="localhost"))
        conn.connection = Mock()
        return conn

    def test_pattern_expands_bare_prefix(self):
        conn = self._connector()
        assert conn._pattern("user") == "user:*"
        assert conn._pattern("user:*") == "user:*"

    def test_fetch_all_rows_scans_hash_keys_only(self):
        conn = self._connector()
        conn.connection.scan.return_value = (0, ["user:1", "user:2", "user:counter"])
        conn.connection.type.side_effect = lambda k: {"user:1": "hash", "user:2": "hash", "user:counter": "string"}[k]
        conn.connection.hgetall.side_effect = lambda k: {"user:1": {"name": "Alice"}, "user:2": {"name": "Bob"}}[k]

        rows = conn.fetch_all_rows("user", ["_key", "name"])

        assert rows == [
            {"name": "Alice", "_key": "user:1"},
            {"name": "Bob", "_key": "user:2"},
        ]

    def test_get_table_metadata_infers_fields_from_sample(self):
        conn = self._connector()
        conn.connection.scan.return_value = (0, ["user:1", "user:2"])
        conn.connection.type.return_value = "hash"
        conn.connection.hgetall.side_effect = [
            {"name": "Alice", "age": "30"},
            {"name": "Bob"},
        ]

        meta = conn.get_table_metadata("user")

        names = {c.name for c in meta.columns}
        assert {"_key", "name", "age"} <= names
        key_col = next(c for c in meta.columns if c.name == "_key")
        assert key_col.is_primary_key is True
        age_col = next(c for c in meta.columns if c.name == "age")
        assert age_col.nullable is True  # only present in 1 of 2 sampled keys
        assert meta.row_count == 2


# ---------------------------------------------------------------------------
# Cassandra (WIDE)
# ---------------------------------------------------------------------------


class _FakeCQLRow:
    def __init__(self, **fields):
        self._fields = fields
        for k, v in fields.items():
            setattr(self, k, v)

    def _asdict(self):
        return dict(self._fields)


class TestCassandraConnector:
    def _connector(self):
        from dimer.connectors.cassandra.connector import CassandraConnector

        conn = CassandraConnector(ConnectionConfig(host="localhost", database="ks"))
        conn.connection = Mock()
        return conn

    def test_resolve_uses_dotted_name_or_configured_keyspace(self):
        conn = self._connector()
        assert conn._resolve("ks2.orders") == ("ks2", "orders")
        assert conn._resolve("orders") == ("ks", "orders")

    def test_fetch_all_rows_selects_requested_columns(self):
        conn = self._connector()
        conn.connection.execute.return_value = [
            _FakeCQLRow(id=1, amount=10.0),
            _FakeCQLRow(id=2, amount=20.0),
        ]

        rows = conn.fetch_all_rows("orders", ["id", "amount"])

        assert rows == [{"id": 1, "amount": 10.0}, {"id": 2, "amount": 20.0}]
        query = conn.connection.execute.call_args[0][0]
        assert "ks.orders" in query

    def test_get_table_metadata_reads_system_schema_columns(self):
        conn = self._connector()
        count_result = Mock()
        count_result.one.return_value = [42]
        conn.connection.execute.side_effect = [
            [
                _FakeCQLRow(column_name="id", type="int", kind="partition_key"),
                _FakeCQLRow(column_name="amount", type="double", kind="regular"),
            ],
            count_result,
        ]

        meta = conn.get_table_metadata("orders", schema_name="ks")

        by_name = {c.name: c for c in meta.columns}
        assert by_name["id"].is_primary_key is True
        assert by_name["amount"].nullable is True
        assert meta.row_count == 42


# ---------------------------------------------------------------------------
# Elasticsearch (SRCH)
# ---------------------------------------------------------------------------


class TestElasticsearchConnector:
    def _connector(self):
        from dimer.connectors.elasticsearch.connector import ElasticsearchConnector

        conn = ElasticsearchConnector(ConnectionConfig(host="localhost"))
        conn.connection = Mock()
        return conn

    def test_normalize_doc_pulls_source_fields_and_id(self):
        conn = self._connector()
        doc = {"_id": "abc", "_source": {"name": "Alice", "age": 30}}

        row = conn._normalize_doc(doc, ["_id", "name", "age"])

        assert row == {"name": "Alice", "age": 30, "_id": "abc"}

    def test_fetch_all_rows_uses_scan_helper(self, monkeypatch):
        conn = self._connector()

        fake_helpers = types.ModuleType("elasticsearch.helpers")
        fake_helpers.scan = Mock(return_value=iter([
            {"_id": "1", "_source": {"name": "Alice"}},
            {"_id": "2", "_source": {"name": "Bob"}},
        ]))
        monkeypatch.setitem(sys.modules, "elasticsearch.helpers", fake_helpers)

        rows = conn.fetch_all_rows("orders", ["_id", "name"])

        assert rows == [{"name": "Alice", "_id": "1"}, {"name": "Bob", "_id": "2"}]
        fake_helpers.scan.assert_called_once()

    def test_get_table_metadata_reads_index_mapping(self):
        conn = self._connector()
        conn.connection.indices.get_mapping.return_value = {
            "orders": {"mappings": {"properties": {"name": {"type": "text"}, "amount": {"type": "double"}}}}
        }
        conn.connection.count.return_value = {"count": 5}

        meta = conn.get_table_metadata("orders")

        by_name = {c.name: c for c in meta.columns}
        assert by_name["_id"].is_primary_key is True
        assert by_name["name"].data_type == "text"
        assert by_name["amount"].data_type == "double"
        assert meta.row_count == 5


# ---------------------------------------------------------------------------
# Neo4j (GRPH)
# ---------------------------------------------------------------------------


class _FakeRecord(dict):
    def data(self):
        return dict(self)


class TestNeo4jConnector:
    def _connector(self):
        from dimer.connectors.neo4j.connector import Neo4jConnector

        conn = Neo4jConnector(ConnectionConfig(host="localhost"))
        session_cm = MagicMock()
        session = MagicMock()
        session_cm.__enter__.return_value = session
        driver = Mock()
        driver.session.return_value = session_cm
        conn.connection = driver
        return conn, session

    def test_normalize_row_extracts_properties_and_id(self):
        conn, _ = self._connector()
        record = {"_id": "4:abc:1", "props": {"name": "Order1", "amount": 10}}

        row = conn._normalize_row(record, ["_id", "name", "amount"])

        assert row == {"name": "Order1", "amount": 10, "_id": "4:abc:1"}

    def test_fetch_all_rows_runs_cypher_match(self):
        conn, session = self._connector()
        session.run.return_value = [
            _FakeRecord(_id="1", props={"name": "A"}),
            _FakeRecord(_id="2", props={"name": "B"}),
        ]

        rows = conn.fetch_all_rows("Order", ["_id", "name"])

        assert rows == [{"name": "A", "_id": "1"}, {"name": "B", "_id": "2"}]
        cypher = session.run.call_args[0][0]
        assert "Order" in cypher
        assert "MATCH" in cypher

    def test_count_rows(self):
        conn, session = self._connector()
        session.run.return_value = [_FakeRecord(c=7)]

        assert conn.count_rows("Order") == 7


# ---------------------------------------------------------------------------
# Qdrant (VEC)
# ---------------------------------------------------------------------------


class _FakePoint:
    def __init__(self, id, payload, vector=None):
        self.id = id
        self.payload = payload
        self.vector = vector


class TestQdrantConnector:
    def _connector(self):
        from dimer.connectors.qdrant.connector import QdrantConnector

        conn = QdrantConnector(ConnectionConfig(host="localhost"))
        conn.connection = Mock()
        return conn

    def test_normalize_point_excludes_vector_unless_requested(self):
        conn = self._connector()
        point = _FakePoint(id="p1", payload={"category": "shoes"})

        row = conn._normalize_point(point, ["_id", "category"])
        assert row == {"category": "shoes", "_id": "p1"}

        point_with_vec = _FakePoint(id="p2", payload={"category": "hats"}, vector=[0.1, 0.2])
        row_vec = conn._normalize_point(point_with_vec, ["_id", "category", "_vector"])
        assert row_vec["_vector"] == "[0.1, 0.2]"

    def test_fetch_all_rows_paginates_via_scroll(self):
        conn = self._connector()
        page1 = [_FakePoint(id=1, payload={"category": "a"})]
        page2 = [_FakePoint(id=2, payload={"category": "b"})]
        conn.connection.scroll.side_effect = [(page1, "next-offset"), (page2, None)]

        rows = conn.fetch_all_rows("products", ["_id", "category"])

        assert rows == [{"category": "a", "_id": 1}, {"category": "b", "_id": 2}]
        assert conn.connection.scroll.call_count == 2

    def test_count_rows(self):
        conn = self._connector()
        conn.connection.count.return_value = SimpleNamespace(count=3)

        assert conn.count_rows("products") == 3


# ---------------------------------------------------------------------------
# InfluxDB (TS)
# ---------------------------------------------------------------------------


class TestInfluxDBConnector:
    def _connector(self):
        from dimer.connectors.influxdb.connector import InfluxDBConnector

        conn = InfluxDBConnector(ConnectionConfig(host="localhost", database="metrics"))
        conn.connection = Mock()
        return conn

    def test_select_columns_excludes_time(self):
        conn = self._connector()
        assert conn._select_columns(["time", "host", "cpu"]) == '"host", "cpu"'
        assert conn._select_columns(["time"]) == "*"

    def test_fetch_all_rows_queries_measurement(self):
        conn = self._connector()
        result = Mock()
        result.get_points.return_value = [
            {"time": "t1", "host": "a", "cpu": 1.0},
            {"time": "t2", "host": "b", "cpu": 2.0},
        ]
        conn.connection.query.return_value = result

        rows = conn.fetch_all_rows("cpu_usage", ["time", "host", "cpu"])

        assert rows == [
            {"time": "t1", "host": "a", "cpu": 1.0},
            {"time": "t2", "host": "b", "cpu": 2.0},
        ]
        query = conn.connection.query.call_args[0][0]
        assert "cpu_usage" in query

    def test_get_table_metadata_reads_field_and_tag_keys(self):
        conn = self._connector()
        field_result = Mock()
        field_result.get_points.return_value = [{"fieldKey": "cpu", "fieldType": "float"}]
        tag_result = Mock()
        tag_result.get_points.return_value = [{"tagKey": "host"}]
        count_result = Mock()
        count_result.get_points.return_value = [{"time": "t", "cpu": 5}]
        conn.connection.query.side_effect = [field_result, tag_result, count_result]

        meta = conn.get_table_metadata("cpu_usage")

        by_name = {c.name: c for c in meta.columns}
        assert by_name["time"].is_primary_key is True
        assert by_name["host"].data_type == "tag/string"
        assert by_name["cpu"].data_type == "float"
        assert meta.row_count == 5
