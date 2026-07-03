"""Unit tests for the BLOOM prefilter, EMBEDDING_SIMILARITY algorithm, the
non-SQL (document-store) execution path, and the new connector registrations."""

from typing import Any, Dict, List, Optional

import pytest

from dimer.core.algorithms.base import _python_row_hash
from dimer.core.algorithms.bloom import BloomFilter, BloomPrefilterAlgorithm
from dimer.core.algorithms.embedding import (
    EmbeddingSimilarityAlgorithm,
    _cosine_distance,
    _l2_distance,
    _parse_vector,
)
from dimer.core.compare import Diffcheck
from dimer.core.factory import ConnectorFactory
from dimer.core.models import (
    ColumnMetadata,
    ConnectionConfig,
    DiffAlgorithm,
    RowStatus,
    TableMetadata,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Fake document-store connectors (SUPPORTS_SQL = False)
# ---------------------------------------------------------------------------


class FakeDocConnector:
    """In-memory stand-in for a document-store connector.

    Implements the SUPPORTS_SQL=False primitives the algorithm layer calls.
    """

    SUPPORTS_SQL = False

    def __init__(self, rows: List[Dict[str, Any]], host: str = "dochost") -> None:
        self.rows = rows
        self.connection_config = ConnectionConfig(host=host, database="testdb")

    # -- metadata ----------------------------------------------------------
    def get_table_metadata(self, table_name: str, schema_name: Optional[str] = None) -> TableMetadata:
        fields: List[str] = []
        for row in self.rows:
            for f in row:
                if f not in fields:
                    fields.append(f)
        columns = [
            ColumnMetadata(name=f, data_type="str", nullable=True, is_primary_key=(f == "id"))
            for f in fields
        ]
        return TableMetadata(columns=columns, row_count=len(self.rows))

    # -- primitives ----------------------------------------------------------
    def count_rows(self, table_name: str) -> int:
        return len(self.rows)

    def fetch_all_rows(self, table_name: str, columns: List[str]) -> List[Dict[str, Any]]:
        return [{c: row.get(c) for c in columns} for row in self.rows]

    def fetch_rows_by_keys(self, table_name, columns, key_dicts, key_cols):
        wanted = {tuple(d.get(k) for k in key_cols) for d in key_dicts}
        return [
            {c: row.get(c) for c in columns}
            for row in self.rows
            if tuple(row.get(k) for k in key_cols) in wanted
        ]

    def sample_rows(self, table_name, columns, n):
        return self.fetch_all_rows(table_name, columns)[:n]

    def fetch_key_hashes(self, table_name, keys, non_key_cols):
        out = []
        for row in self.rows:
            r = {k: row.get(k) for k in keys}
            if non_key_cols:
                r["_dimer_row_hash"] = _python_row_hash(row, non_key_cols)
            out.append(r)
        return out


class OtherFakeDocConnector(FakeDocConnector):
    """Different class → hashes treated as non-comparable by the algorithms."""


def _rows(*triples):
    return [{"id": i, "name": n, "amount": a} for i, n, a in triples]


CONFIG_A = {"fq_table_name": "orders", "keys": ["id"]}
CONFIG_B = {"fq_table_name": "orders", "keys": ["id"]}


# ---------------------------------------------------------------------------
# BloomFilter
# ---------------------------------------------------------------------------


class TestBloomFilter:
    def test_no_false_negatives(self):
        bf = BloomFilter(capacity=1000, fpr=0.01)
        items = [f"item-{i}" for i in range(1000)]
        for item in items:
            bf.add(item)
        assert all(bf.contains(item) for item in items)

    def test_absent_items_mostly_rejected(self):
        bf = BloomFilter(capacity=1000, fpr=0.01)
        for i in range(1000):
            bf.add(f"present-{i}")
        false_positives = sum(bf.contains(f"absent-{i}") for i in range(1000))
        # 1% target FPR — allow generous slack for a probabilistic test
        assert false_positives < 50

    def test_sizing(self):
        bf = BloomFilter(capacity=10_000, fpr=0.01)
        assert bf.bit_count > 10_000  # ~9.6 bits per element at 1% FPR
        assert 1 <= bf.hash_count <= 20


# ---------------------------------------------------------------------------
# BLOOM prefilter algorithm
# ---------------------------------------------------------------------------


class TestBloomPrefilter:
    def test_identical_tables_match(self):
        rows = _rows((1, "a", 10), (2, "b", 20))
        result = BloomPrefilterAlgorithm(
            FakeDocConnector(rows), FakeDocConnector(rows), dict(CONFIG_A), dict(CONFIG_B)
        ).run()
        assert result.algorithm == DiffAlgorithm.BLOOM
        assert result.match is True
        assert result.metadata["prefilter"] is True
        assert result.metadata["hash_comparable"] is True

    def test_detects_added_deleted_modified(self):
        rows_a = _rows((1, "a", 10), (2, "b", 20), (3, "c", 30))
        rows_b = _rows((1, "a", 10), (2, "b", 99), (4, "d", 40))
        result = BloomPrefilterAlgorithm(
            FakeDocConnector(rows_a), FakeDocConnector(rows_b), dict(CONFIG_A), dict(CONFIG_B)
        ).run()
        assert result.match is False
        m = result.metadata
        assert m["definite_deleted"] == 1   # id=3
        assert m["definite_added"] == 1     # id=4
        assert m["definite_modified"] == 1  # id=2
        statuses = {(tuple(r.key_values.values()), r.status) for r in result.row_diffs}
        assert ((3,), RowStatus.DELETED) in statuses
        assert ((4,), RowStatus.ADDED) in statuses
        assert ((2,), RowStatus.MODIFIED) in statuses

    def test_cross_type_key_membership_only(self):
        rows_a = _rows((1, "a", 10), (2, "b", 20))
        rows_b = _rows((1, "a", 10), (2, "b", 99))  # modified, but not detectable
        result = BloomPrefilterAlgorithm(
            FakeDocConnector(rows_a), OtherFakeDocConnector(rows_b),
            dict(CONFIG_A), dict(CONFIG_B),
        ).run()
        assert result.metadata["hash_comparable"] is False
        assert result.metadata["definite_modified"] == 0
        # Row counts equal and all keys present on both sides → no signal
        assert result.match is True

    def test_row_count_mismatch_prevents_match(self):
        rows_a = _rows((1, "a", 10))
        rows_b = _rows((1, "a", 10), (1, "a", 10))  # duplicate key inflates count
        result = BloomPrefilterAlgorithm(
            FakeDocConnector(rows_a), FakeDocConnector(rows_b),
            dict(CONFIG_A), dict(CONFIG_B),
        ).run()
        assert result.match is False


# ---------------------------------------------------------------------------
# Embedding similarity
# ---------------------------------------------------------------------------


class TestVectorParsing:
    def test_parses_pgvector_text(self):
        assert _parse_vector("[0.1, 0.2, 0.3]") == [0.1, 0.2, 0.3]
        assert _parse_vector("{1, 2}") == [1.0, 2.0]

    def test_parses_sequences(self):
        assert _parse_vector([1, 2]) == [1.0, 2.0]
        assert _parse_vector((1.5,)) == [1.5]

    def test_rejects_garbage(self):
        assert _parse_vector("not a vector") is None
        assert _parse_vector(None) is None

    def test_distances(self):
        assert _cosine_distance([1, 0], [1, 0]) == pytest.approx(0.0)
        assert _cosine_distance([1, 0], [0, 1]) == pytest.approx(1.0)
        assert _l2_distance([0, 0], [3, 4]) == pytest.approx(5.0)


class TestEmbeddingSimilarity:
    @staticmethod
    def _vec_rows(*pairs):
        return [{"id": i, "embedding": v} for i, v in pairs]

    def _run(self, rows_a, rows_b, **overrides):
        config_a = {
            "fq_table_name": "vectors", "keys": ["id"],
            "use_embedding": True, "vector_column": "embedding",
            **overrides,
        }
        config_b = {"fq_table_name": "vectors", "keys": ["id"]}
        return EmbeddingSimilarityAlgorithm(
            FakeDocConnector(rows_a), FakeDocConnector(rows_b), config_a, config_b
        ).run()

    def test_identical_vectors_match(self):
        rows = self._vec_rows((1, [0.1, 0.2]), (2, [0.3, 0.4]))
        result = self._run(rows, rows)
        assert result.algorithm == DiffAlgorithm.EMBEDDING_SIMILARITY
        assert result.match is True
        assert result.metadata["compared_pairs"] == 2

    def test_float_noise_within_tolerance_matches(self):
        rows_a = self._vec_rows((1, [0.10000, 0.20000]))
        rows_b = self._vec_rows((1, [0.10001, 0.20001]))
        result = self._run(rows_a, rows_b, distance_threshold=1e-3)
        assert result.match is True

    def test_divergent_vector_is_modified(self):
        rows_a = self._vec_rows((1, [1.0, 0.0]), (2, [0.5, 0.5]))
        rows_b = self._vec_rows((1, [0.0, 1.0]), (2, [0.5, 0.5]))
        result = self._run(rows_a, rows_b)
        assert result.match is False
        assert result.summary.modified_count == 1
        modified = result.modified_rows()[0]
        assert modified.key_values == {"id": 1}
        assert modified.mismatched_columns == ["embedding"]

    def test_added_and_deleted_ids(self):
        rows_a = self._vec_rows((1, [1.0]), (2, [2.0]))
        rows_b = self._vec_rows((2, [2.0]), (3, [3.0]))
        result = self._run(rows_a, rows_b)
        assert result.summary.deleted_count == 1
        assert result.summary.added_count == 1

    def test_dimension_mismatch_is_modified(self):
        rows_a = self._vec_rows((1, [1.0, 2.0]))
        rows_b = self._vec_rows((1, [1.0, 2.0, 3.0]))
        result = self._run(rows_a, rows_b)
        assert result.summary.modified_count == 1
        assert result.metadata["dimension_mismatches"] == 1

    def test_l2_metric(self):
        rows_a = self._vec_rows((1, [0.0, 0.0]))
        rows_b = self._vec_rows((1, [3.0, 4.0]))
        result = self._run(rows_a, rows_b, distance_metric="l2", distance_threshold=4.9)
        assert result.summary.modified_count == 1

    def test_missing_vector_column_errors(self):
        algo = EmbeddingSimilarityAlgorithm(
            FakeDocConnector([]), FakeDocConnector([]),
            {"fq_table_name": "t", "keys": ["id"], "use_embedding": True},
            {"fq_table_name": "t", "keys": ["id"]},
        )
        result = algo.run()
        assert result.error is not None
        assert "vector_column" in result.error


# ---------------------------------------------------------------------------
# Diffcheck routing + non-SQL execution path
# ---------------------------------------------------------------------------


class TestRoutingAndDocPath:
    def test_use_bloom_routes_to_bloom(self):
        rows = _rows((1, "a", 10))
        cfg = {**CONFIG_A, "use_bloom": True}
        result = Diffcheck(FakeDocConnector(rows), FakeDocConnector(rows), cfg, dict(CONFIG_B)).compare()
        assert result.algorithm == DiffAlgorithm.BLOOM

    def test_use_embedding_routes_to_embedding(self):
        rows = [{"id": 1, "embedding": [1.0]}]
        cfg = {"fq_table_name": "t", "keys": ["id"], "use_embedding": True, "vector_column": "embedding"}
        result = Diffcheck(FakeDocConnector(rows), FakeDocConnector(rows), cfg,
                           {"fq_table_name": "t", "keys": ["id"]}).compare()
        assert result.algorithm == DiffAlgorithm.EMBEDDING_SIMILARITY

    def test_non_sql_same_host_avoids_join_diff(self):
        # Same host + database, but SUPPORTS_SQL=False → must not pick JOIN_DIFF
        rows = _rows((1, "a", 10))
        result = Diffcheck(
            FakeDocConnector(rows, host="samehost"),
            FakeDocConnector(rows, host="samehost"),
            dict(CONFIG_A), dict(CONFIG_B),
        ).compare()
        assert result.algorithm == DiffAlgorithm.HASH_DIFF
        assert result.match is True

    def test_hash_diff_over_doc_connectors(self):
        rows_a = _rows((1, "a", 10), (2, "b", 20), (3, "c", 30))
        rows_b = _rows((1, "a", 10), (2, "b", 99), (4, "d", 40))
        result = Diffcheck(FakeDocConnector(rows_a), FakeDocConnector(rows_b),
                           dict(CONFIG_A), dict(CONFIG_B)).compare()
        assert result.algorithm == DiffAlgorithm.HASH_DIFF
        s = result.summary
        assert (s.added_count, s.deleted_count, s.modified_count) == (1, 1, 1)

    def test_full_fetch_over_doc_connectors(self):
        rows_a = _rows((1, "a", 10), (2, "b", 20))
        rows_b = _rows((1, "a", 10), (2, "b", 99))
        result = Diffcheck(FakeDocConnector(rows_a), FakeDocConnector(rows_b),
                           dict(CONFIG_A), dict(CONFIG_B)).compare_cross_database()
        assert result.algorithm == DiffAlgorithm.FULL_FETCH_DIFF
        assert result.summary.modified_count == 1

    def test_sampled_over_doc_connectors(self):
        rows_a = _rows((1, "a", 10), (2, "b", 20))
        rows_b = _rows((1, "a", 10), (2, "b", 99))
        cfg = {**CONFIG_A, "use_sampling": True, "sample_size": 10}
        result = Diffcheck(FakeDocConnector(rows_a), FakeDocConnector(rows_b),
                           cfg, dict(CONFIG_B)).compare()
        assert result.algorithm == DiffAlgorithm.SAMPLED
        assert result.summary.modified_count == 1


# ---------------------------------------------------------------------------
# Connector registrations (NSQL + DOC)
# ---------------------------------------------------------------------------


class TestNewConnectorRegistration:
    @pytest.mark.parametrize("source_type", [
        "cockroachdb", "cockroach", "crdb", "yugabyte", "yugabytedb", "tidb",
        "mongodb", "mongo",
        "redis", "cassandra", "elasticsearch", "elastic", "neo4j", "qdrant", "influxdb",
    ])
    def test_registered(self, source_type):
        # The autouse reset_factory fixture clears the registry pre-test
        from dimer.core.factory import _auto_register_connectors
        _auto_register_connectors()
        assert ConnectorFactory.is_source_supported(source_type)

    def test_cockroach_inherits_postgres_dialect_with_custom_agg_hash(self):
        from dimer.connectors.cockroachdb.connector import CockroachDBConnector
        from dimer.connectors.postgresql.connector import PostgreSQLConnector

        assert issubclass(CockroachDBConnector, PostgreSQLConnector)
        assert CockroachDBConnector.DIALECTS["hash"] == PostgreSQLConnector.DIALECTS["hash"]
        assert "xor_agg" in CockroachDBConnector.DIALECTS["aggregate_hash"]
        assert CockroachDBConnector.DEFAULT_PORT == 26257

    def test_yugabyte_inherits_postgres_dialect(self):
        from dimer.connectors.postgresql.connector import PostgreSQLConnector
        from dimer.connectors.yugabyte.connector import YugabyteConnector

        assert issubclass(YugabyteConnector, PostgreSQLConnector)
        assert YugabyteConnector.DIALECTS == PostgreSQLConnector.DIALECTS
        assert YugabyteConnector.DEFAULT_PORT == 5433

    def test_tidb_inherits_mysql_dialect(self):
        from dimer.connectors.mysql.connector import MySQLConnector
        from dimer.connectors.tidb.connector import TiDBConnector

        assert issubclass(TiDBConnector, MySQLConnector)
        assert TiDBConnector.DIALECTS == MySQLConnector.DIALECTS
        assert TiDBConnector.DEFAULT_PORT == 4000

    def test_mongodb_declares_non_sql(self):
        from dimer.connectors.mongodb.connector import MongoDBConnector

        assert MongoDBConnector.SUPPORTS_SQL is False
        # All primitives the algorithm layer relies on must exist
        for primitive in ("count_rows", "fetch_all_rows", "fetch_rows_by_keys",
                          "sample_rows", "fetch_key_hashes"):
            assert callable(getattr(MongoDBConnector, primitive))

    @pytest.mark.parametrize("module_path, class_name", [
        ("dimer.connectors.redis.connector", "RedisConnector"),
        ("dimer.connectors.cassandra.connector", "CassandraConnector"),
        ("dimer.connectors.elasticsearch.connector", "ElasticsearchConnector"),
        ("dimer.connectors.neo4j.connector", "Neo4jConnector"),
        ("dimer.connectors.qdrant.connector", "QdrantConnector"),
        ("dimer.connectors.influxdb.connector", "InfluxDBConnector"),
    ])
    def test_new_store_family_declares_non_sql(self, module_path, class_name):
        """KV/WIDE/SRCH/GRPH/VEC/TS connectors all follow the MongoDB contract:
        SUPPORTS_SQL=False, no DIALECTS, and every non-SQL primitive present —
        this is what makes UC1 (FULL_FETCH_DIFF) and UC2 (SCHEMA_DIFF) work for
        them with zero changes to the algorithm layer."""
        import importlib

        module = importlib.import_module(module_path)
        connector_class = getattr(module, class_name)

        assert connector_class.SUPPORTS_SQL is False
        assert connector_class.DIALECTS == {}
        for primitive in ("count_rows", "fetch_all_rows", "fetch_rows_by_keys",
                          "sample_rows", "fetch_key_hashes", "get_table_metadata"):
            assert callable(getattr(connector_class, primitive))
