"""Tests for DuckDBConnector.

Because DuckDB is an embedded in-process database there is no external server
or credentials required — everything runs against ``:memory:``.

Requires the ``duckdb`` package::

    pip install dimer[duckdb]

Run with::

    pytest tests/test_duckdb_integration.py -v -s
"""

from unittest.mock import Mock, patch

import pytest

pytest.importorskip("duckdb", reason="duckdb package not installed")

import numpy as np
import pandas as pd

from dimer.connectors.duckdb.connector import DuckDBConnector
from dimer.core.models import ConnectionConfig, ConnectionMethod

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE orders (
    order_id   INTEGER PRIMARY KEY,
    customer   VARCHAR,
    amount     DOUBLE,
    created_at DATE
)
"""

ROWS = [
    (1, "alice", 99.50,  "2024-01-01"),
    (2, "bob",   149.00, "2024-02-15"),
    (3, "carol", 49.99,  "2024-03-10"),
]


@pytest.fixture
def duckdb_config():
    return ConnectionConfig(host=":memory:", schema_name="main")


@pytest.fixture(scope="module")
def live_connector():
    """A DuckDBConnector connected to an in-memory database with sample data."""
    cfg = ConnectionConfig(host=":memory:", schema_name="main")
    conn = DuckDBConnector(cfg)
    conn.connect()
    conn.connection.execute(DDL)
    conn.connection.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", ROWS)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Connector class-level properties
# ---------------------------------------------------------------------------

class TestDuckDBConnectorProperties:
    """Static connector properties — no connection needed."""

    def test_required_params(self):
        c = DuckDBConnector.__new__(DuckDBConnector)
        assert c.get_required_params() == ["host"]

    def test_connection_methods_order(self):
        c = DuckDBConnector.__new__(DuckDBConnector)
        methods = c.get_connection_methods()
        assert methods[0] == ConnectionMethod.NATIVE
        assert ConnectionMethod.SQLALCHEMY in methods

    def test_identifier_case(self):
        assert DuckDBConnector.IDENTIFIER_CASE == "lower"

    def test_dialects_keys(self):
        expected = {"hash", "concatenation", "cast_to_text", "aggregate_hash", "random_func"}
        assert expected == set(DuckDBConnector.DIALECTS.keys())

    def test_dialects_col_placeholders(self):
        """Every template that wraps a column expression must contain {COL}."""
        for key, tmpl in DuckDBConnector.DIALECTS.items():
            if key not in ("concatenation", "random_func"):
                assert "{COL}" in tmpl, f"DIALECTS[{key!r}] missing {{COL}} placeholder"

    def test_test_query(self, duckdb_config):
        c = DuckDBConnector(duckdb_config)
        assert c._get_test_query() == "SELECT version()"


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

class TestDuckDBConnection:
    """Connection establishment and error handling."""

    def test_native_connection_succeeds(self, live_connector):
        assert live_connector.connection_method_used == ConnectionMethod.NATIVE
        assert live_connector.connection is not None

    def test_native_import_error(self, duckdb_config):
        """Raises ImportError with a clear message when duckdb is absent."""
        c = DuckDBConnector(duckdb_config)
        with patch.dict("sys.modules", {"duckdb": None}):
            with pytest.raises(ImportError, match="duckdb is required"):
                c._connect_native()

    @patch("sqlalchemy.create_engine")
    def test_sqlalchemy_import_error(self, _mock_engine, duckdb_config):
        """Raises ImportError when duckdb-engine is absent."""
        c = DuckDBConnector(duckdb_config)
        with patch.dict("sys.modules", {"duckdb_engine": None}):
            with pytest.raises(ImportError, match="duckdb-engine is required"):
                c._connect_sqlalchemy()

    @patch("sqlalchemy.create_engine")
    def test_sqlalchemy_url_format(self, mock_create_engine, duckdb_config):
        """SQLAlchemy URL uses the duckdb:/// scheme."""
        mock_engine = Mock()
        mock_ctx = Mock()
        mock_ctx.__enter__ = Mock(return_value=Mock())
        mock_ctx.__exit__ = Mock(return_value=False)
        mock_engine.connect.return_value = mock_ctx
        mock_create_engine.return_value = mock_engine

        c = DuckDBConnector(duckdb_config)
        c._connect_sqlalchemy()

        url = mock_create_engine.call_args[0][0]
        assert "duckdb:///" in url
        assert ":memory:" in url


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

class TestDuckDBQueryExecution:
    """_execute_query_internal — live and error-path tests."""

    def test_simple_select(self, live_connector):
        df = live_connector._execute_query_internal("SELECT 1 AS n")
        assert int(df.iloc[0]["n"]) == 1

    def test_named_params_converted(self, live_connector):
        """%(name)s placeholders are rewritten to positional ? for DuckDB native."""
        df = live_connector._execute_query_internal(
            "SELECT %(x)s + %(y)s AS total", params={"x": 3, "y": 4}
        )
        assert int(df.iloc[0]["total"]) == 7

    def test_select_from_table(self, live_connector):
        df = live_connector._execute_query_internal(
            'SELECT COUNT(*) AS cnt FROM "main"."orders"'
        )
        assert int(df.iloc[0]["cnt"]) == 3

    def test_no_connection_raises(self, duckdb_config):
        c = DuckDBConnector(duckdb_config)
        with pytest.raises(RuntimeError, match="No active connection"):
            c._execute_query_internal("SELECT 1")

    def test_unsupported_method_raises(self, duckdb_config):
        c = DuckDBConnector(duckdb_config)
        c.connection = Mock()
        c.connection_method_used = ConnectionMethod.ASYNCPG
        with pytest.raises(ValueError, match="Unsupported connection method"):
            c._execute_query_internal("SELECT 1")


# ---------------------------------------------------------------------------
# get_table_metadata
# ---------------------------------------------------------------------------

class TestDuckDBTableMetadata:
    """Column metadata, primary-key detection, row count."""

    def test_column_count(self, live_connector):
        meta = live_connector.get_table_metadata("orders", schema_name="main")
        assert len(meta.columns) == 4

    def test_column_names_in_order(self, live_connector):
        meta = live_connector.get_table_metadata("orders", schema_name="main")
        assert [c.name for c in meta.columns] == ["order_id", "customer", "amount", "created_at"]

    def test_primary_key_detected(self, live_connector):
        meta = live_connector.get_table_metadata("orders", schema_name="main")
        pk_cols = [c.name for c in meta.columns if c.is_primary_key]
        assert pk_cols == ["order_id"]

    def test_non_pk_columns_not_marked(self, live_connector):
        meta = live_connector.get_table_metadata("orders", schema_name="main")
        assert all(not c.is_primary_key for c in meta.columns if c.name != "order_id")

    def test_pk_nullable_is_false(self, live_connector):
        meta = live_connector.get_table_metadata("orders", schema_name="main")
        pk = next(c for c in meta.columns if c.name == "order_id")
        assert pk.nullable is False

    def test_type_mapping(self, live_connector):
        meta = live_connector.get_table_metadata("orders", schema_name="main")
        col_map = {c.name: c.data_type for c in meta.columns}
        assert col_map["order_id"] == "int32"
        assert col_map["customer"] == "string"
        assert col_map["amount"] == "float64"
        assert col_map["created_at"] == "date"

    def test_row_count(self, live_connector):
        meta = live_connector.get_table_metadata("orders", schema_name="main")
        assert meta.row_count == 3

    def test_default_schema_is_main(self, duckdb_config):
        """When schema_name is omitted, the connector falls back to 'main'."""
        cfg = ConnectionConfig(host=":memory:")  # no schema_name
        c = DuckDBConnector(cfg)
        c.connect()
        c.connection.execute(DDL)
        c.connection.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", ROWS)
        meta = c.get_table_metadata("orders")  # no schema_name argument
        assert len(meta.columns) == 4
        c.close()

    def test_pk_error_does_not_abort(self, duckdb_config):
        """If duckdb_constraints() fails, columns are still returned without PK flags."""
        columns_df = pd.DataFrame({
            "column_name": ["order_id", "customer"],
            "data_type": ["INTEGER", "VARCHAR"],
            "is_nullable": ["NO", "YES"],
            "column_default": [None, None],
            "character_maximum_length": [None, None],
            "numeric_precision": [None, None],
            "numeric_scale": [None, None],
        })
        count_df = pd.DataFrame({"cnt": [0]})

        c = DuckDBConnector(duckdb_config)
        mock_conn = Mock()
        c.connection = mock_conn
        c.connection_method_used = ConnectionMethod.NATIVE

        # First call (columns query) succeeds; second call (PK query) raises.
        mock_conn.execute.side_effect = [
            Mock(df=Mock(return_value=columns_df)),
            Exception("simulated duckdb_constraints failure"),
        ]
        with patch.object(c, "_execute_query_internal", return_value=count_df):
            meta = c.get_table_metadata("orders", schema_name="main")

        assert len(meta.columns) == 2
        assert all(not col.is_primary_key for col in meta.columns)

    def test_pk_column_names_as_json_string(self, duckdb_config):
        """PK detection works when constraint_column_names is a JSON string."""
        c = DuckDBConnector(duckdb_config)
        c.connect()
        c.connection.execute(DDL)

        columns_df = pd.DataFrame({
            "column_name": ["order_id", "customer"],
            "data_type": ["INTEGER", "VARCHAR"],
            "is_nullable": ["NO", "YES"],
            "column_default": [None, None],
            "character_maximum_length": [None, None],
            "numeric_precision": [None, None],
            "numeric_scale": [None, None],
        })
        pk_df = pd.DataFrame({"constraint_column_names": ['["order_id"]']})
        count_df = pd.DataFrame({"cnt": [0]})

        mock_conn = Mock()
        c.connection = mock_conn
        mock_conn.execute.side_effect = [
            Mock(df=Mock(return_value=columns_df)),
            Mock(df=Mock(return_value=pk_df)),
        ]
        with patch.object(c, "_execute_query_internal", return_value=count_df):
            meta = c.get_table_metadata("orders", schema_name="main")

        assert meta.columns[0].is_primary_key is True
        assert meta.columns[1].is_primary_key is False


# ---------------------------------------------------------------------------
# get_sample_data
# ---------------------------------------------------------------------------

class TestDuckDBSampleData:

    def test_returns_dataframe(self, live_connector):
        df = live_connector.get_sample_data("orders", limit=10, schema_name="main")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_limit_respected(self, live_connector):
        df = live_connector.get_sample_data("orders", limit=2, schema_name="main")
        assert len(df) == 2

    def test_expected_columns(self, live_connector):
        df = live_connector.get_sample_data("orders", limit=5, schema_name="main")
        for col in ("order_id", "customer", "amount", "created_at"):
            assert col in df.columns


# ---------------------------------------------------------------------------
# Table and schema listing
# ---------------------------------------------------------------------------

class TestDuckDBListing:

    def test_list_tables_in_schema(self, live_connector):
        tables = live_connector._list_tables_internal(schema_name="main")
        assert "orders" in tables

    def test_list_tables_all_schemas(self, live_connector):
        tables = live_connector._list_tables_internal()
        assert "orders" in tables

    def test_list_schemas(self, live_connector):
        schemas = live_connector._list_schemas_internal()
        assert "main" in schemas


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------

class TestDuckDBClose:

    def test_close_clears_state(self):
        cfg = ConnectionConfig(host=":memory:")
        c = DuckDBConnector(cfg)
        c.connect()
        assert c.connection is not None

        c.close()
        assert c.connection is None
        assert c.connection_method_used is None

    def test_close_native_calls_connection_close(self, duckdb_config):
        c = DuckDBConnector(duckdb_config)
        mock_conn = Mock()
        c.connection = mock_conn
        c.connection_method_used = ConnectionMethod.NATIVE
        c.close()
        mock_conn.close.assert_called_once()

    def test_close_sqlalchemy_calls_dispose(self, duckdb_config):
        c = DuckDBConnector(duckdb_config)
        mock_engine = Mock()
        c.connection = mock_engine
        c.connection_method_used = ConnectionMethod.SQLALCHEMY
        c.close()
        mock_engine.dispose.assert_called_once()

    def test_close_safe_when_no_connection(self, duckdb_config):
        c = DuckDBConnector(duckdb_config)
        c.connection = None
        c.connection_method_used = None
        c.close()  # must not raise


# ---------------------------------------------------------------------------
# Factory registration
# ---------------------------------------------------------------------------

class TestDuckDBFactory:

    def test_registered_under_duckdb(self):
        from dimer.core.factory import ConnectorFactory, _auto_register_connectors
        ConnectorFactory._connectors.clear()
        _auto_register_connectors()
        assert "duckdb" in ConnectorFactory.get_supported_sources()

    def test_factory_creates_correct_class(self, duckdb_config):
        from dimer.core.factory import ConnectorFactory, _auto_register_connectors
        ConnectorFactory._connectors.clear()
        _auto_register_connectors()
        connector = ConnectorFactory.create_connector("duckdb", duckdb_config)
        assert isinstance(connector, DuckDBConnector)
