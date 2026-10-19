"""Unit tests for the Pinecone, Milvus, Weaviate, and ChromaDB vector-store
(VEC family) connectors, added alongside the existing Qdrant connector (see
``test_new_store_connectors.py``).

None of the driver packages (``pinecone-client``, ``pymilvus``,
``weaviate-client``, ``chromadb``) are installed in this environment (they
are optional extras), so every test drives the connector by injecting a
``Mock()`` directly as ``connector.connection`` — this exercises exactly the
same code path ``_client()`` would use after a real ``connect()``, without
needing network access or the driver installed.
"""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from dimer.core.models import ConnectionConfig

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Pinecone (VEC)
# ---------------------------------------------------------------------------


class TestPineconeConnector:
    def _connector(self):
        from dimer.connectors.pinecone.connector import PineconeConnector

        conn = PineconeConnector(ConnectionConfig(password="test-api-key"))
        conn.connection = Mock()
        return conn

    def test_resolve_splits_index_and_namespace(self):
        conn = self._connector()
        assert conn._resolve("orders") == ("orders", "")
        assert conn._resolve("orders.prod") == ("orders", "prod")

    def test_normalize_vectors_excludes_vector_unless_requested(self):
        conn = self._connector()
        response = {"vectors": {"p1": {"metadata": {"category": "shoes"}, "values": None}}}

        rows = conn._normalize_vectors(response, ["_id", "category"])
        assert rows == [{"category": "shoes", "_id": "p1"}]

        response_vec = {
            "vectors": {"p2": {"metadata": {"category": "hats"}, "values": [0.1, 0.2]}}
        }
        rows_vec = conn._normalize_vectors(response_vec, ["_id", "category", "_vector"])
        assert rows_vec == [{"category": "hats", "_id": "p2", "_vector": "[0.1, 0.2]"}]

    def test_fetch_all_rows_paginates_via_list_and_fetch(self):
        conn = self._connector()
        index = Mock()
        conn.connection.Index.return_value = index
        index.list.return_value = iter([["p1"], ["p2"]])
        index.fetch.side_effect = [
            {"vectors": {"p1": {"metadata": {"category": "a"}}}},
            {"vectors": {"p2": {"metadata": {"category": "b"}}}},
        ]

        rows = conn.fetch_all_rows("orders", ["_id", "category"])

        assert rows == [{"category": "a", "_id": "p1"}, {"category": "b", "_id": "p2"}]
        index.list.assert_called_once_with(namespace="", limit=100)
        assert index.fetch.call_count == 2

    def test_count_rows_uses_describe_index_stats(self):
        conn = self._connector()
        index = Mock()
        conn.connection.Index.return_value = index
        index.describe_index_stats.return_value = {"total_vector_count": 5, "namespaces": {}}

        assert conn.count_rows("orders") == 5

    def test_count_rows_scopes_to_namespace(self):
        conn = self._connector()
        index = Mock()
        conn.connection.Index.return_value = index
        index.describe_index_stats.return_value = {
            "total_vector_count": 5,
            "namespaces": {"prod": {"vector_count": 2}},
        }

        assert conn.count_rows("orders.prod") == 2


# ---------------------------------------------------------------------------
# Milvus (VEC)
# ---------------------------------------------------------------------------


class TestMilvusConnector:
    def _connector(self):
        from dimer.connectors.milvus.connector import MilvusConnector

        conn = MilvusConnector(ConnectionConfig(host="localhost"))
        conn.connection = Mock()
        return conn

    def test_normalize_entity_excludes_vector_unless_requested(self):
        conn = self._connector()
        entity = {"id": 1, "category": "shoes"}

        row = conn._normalize_entity(entity, ["_id", "category"], "id", "embedding")
        assert row == {"category": "shoes", "_id": 1}

        entity_vec = {"id": 2, "category": "hats", "embedding": [0.1, 0.2]}
        row_vec = conn._normalize_entity(
            entity_vec, ["_id", "category", "_vector"], "id", "embedding"
        )
        assert row_vec == {"category": "hats", "_id": 2, "_vector": "[0.1, 0.2]"}

    def test_fetch_all_rows_pages_via_query_offset(self):
        conn = self._connector()
        conn.connection.describe_collection.return_value = {
            "fields": [
                {"name": "id", "is_primary": True, "params": {}},
                {"name": "embedding", "is_primary": False, "params": {"dim": 128}},
                {"name": "category", "is_primary": False, "params": {}},
            ]
        }
        conn.connection.query.return_value = [{"id": 1, "category": "a"}]

        rows = conn.fetch_all_rows("products", ["_id", "category"])

        assert rows == [{"category": "a", "_id": 1}]
        conn.connection.query.assert_called_once_with(
            collection_name="products",
            filter="",
            output_fields=["id", "category"],
            limit=500,
            offset=0,
        )

    def test_count_rows(self):
        conn = self._connector()
        conn.connection.get_collection_stats.return_value = {"row_count": 3}

        assert conn.count_rows("products") == 3

    def test_pk_field_falls_back_to_id_when_no_primary_declared(self):
        conn = self._connector()
        conn.connection.describe_collection.return_value = {"fields": []}

        assert conn._pk_field("products") == "id"


# ---------------------------------------------------------------------------
# Weaviate (VEC)
# ---------------------------------------------------------------------------


class _FakeWeaviateObject:
    def __init__(self, uuid, properties, vector=None):
        self.uuid = uuid
        self.properties = properties
        self.vector = vector


class TestWeaviateConnector:
    def _connector(self):
        from dimer.connectors.weaviate.connector import WeaviateConnector

        conn = WeaviateConnector(ConnectionConfig(host="localhost"))
        conn.connection = Mock()
        return conn

    def test_flatten_vector_handles_named_and_plain_vectors(self):
        from dimer.connectors.weaviate.connector import WeaviateConnector

        assert WeaviateConnector._flatten_vector(None) is None
        assert WeaviateConnector._flatten_vector([0.1, 0.2]) == [0.1, 0.2]
        assert WeaviateConnector._flatten_vector({"default": [0.1, 0.2]}) == [0.1, 0.2]
        assert WeaviateConnector._flatten_vector({"other": [0.3]}) == [0.3]
        assert WeaviateConnector._flatten_vector({}) is None

    def test_normalize_object_excludes_vector_unless_requested(self):
        conn = self._connector()
        obj = _FakeWeaviateObject(uuid="u1", properties={"category": "shoes"})

        row = conn._normalize_object(obj, ["_id", "category"])
        assert row == {"category": "shoes", "_id": "u1"}

        obj_vec = _FakeWeaviateObject(
            uuid="u2", properties={"category": "hats"}, vector={"default": [0.1, 0.2]}
        )
        row_vec = conn._normalize_object(obj_vec, ["_id", "category", "_vector"])
        assert row_vec == {"category": "hats", "_id": "u2", "_vector": "[0.1, 0.2]"}

    def test_fetch_all_rows_uses_collection_iterator(self):
        conn = self._connector()
        collection = Mock()
        conn.connection.collections.get.return_value = collection
        collection.iterator.return_value = [
            _FakeWeaviateObject(uuid="u1", properties={"category": "a"}),
            _FakeWeaviateObject(uuid="u2", properties={"category": "b"}),
        ]

        rows = conn.fetch_all_rows("Order", ["_id", "category"])

        assert rows == [{"category": "a", "_id": "u1"}, {"category": "b", "_id": "u2"}]
        collection.iterator.assert_called_once_with(include_vector=False)

    def test_count_rows(self):
        conn = self._connector()
        collection = Mock()
        conn.connection.collections.get.return_value = collection
        collection.aggregate.over_all.return_value = SimpleNamespace(total_count=7)

        assert conn.count_rows("Order") == 7


# ---------------------------------------------------------------------------
# ChromaDB (VEC)
# ---------------------------------------------------------------------------


class TestChromaDBConnector:
    def _connector(self):
        from dimer.connectors.chromadb.connector import ChromaDBConnector

        conn = ChromaDBConnector(ConnectionConfig(host="localhost"))
        conn.connection = Mock()
        return conn

    def test_include_for_selects_requested_groups(self):
        conn = self._connector()
        assert conn._include_for(["_id"]) == []
        assert conn._include_for(["_id", "_vector"]) == ["embeddings"]
        assert conn._include_for(["_id", "_document"]) == ["documents"]
        assert conn._include_for(["_id", "category"]) == ["metadatas"]

    def test_normalize_batch_excludes_vector_and_document_unless_requested(self):
        conn = self._connector()
        result = {
            "ids": ["r1", "r2"],
            "metadatas": [{"category": "shoes"}, {"category": "hats"}],
            "embeddings": [[0.1, 0.2], None],
            "documents": ["shoe desc", "hat desc"],
        }

        rows = conn._normalize_batch(result, ["_id", "category"])
        assert rows == [
            {"category": "shoes", "_id": "r1"},
            {"category": "hats", "_id": "r2"},
        ]

        rows_full = conn._normalize_batch(result, ["_id", "category", "_vector", "_document"])
        assert rows_full[0]["_vector"] == "[0.1, 0.2]"
        assert rows_full[0]["_document"] == "shoe desc"
        assert rows_full[1]["_vector"] is None

    def test_fetch_all_rows_pages_via_get_offset(self):
        conn = self._connector()
        collection = Mock()
        conn.connection.get_collection.return_value = collection
        collection.get.return_value = {"ids": ["r1"], "metadatas": [{"category": "a"}]}

        rows = conn.fetch_all_rows("products", ["_id", "category"])

        assert rows == [{"category": "a", "_id": "r1"}]
        collection.get.assert_called_once_with(include=["metadatas"], limit=500, offset=0)

    def test_count_rows(self):
        conn = self._connector()
        collection = Mock()
        conn.connection.get_collection.return_value = collection
        collection.count.return_value = 3

        assert conn.count_rows("products") == 3
