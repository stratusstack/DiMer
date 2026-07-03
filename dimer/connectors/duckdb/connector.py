"""DuckDB connector — embedded in-process OLAP database."""

import json
from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from dimer.core.base import DataSourceConnector
from dimer.core.models import (
    ColumnMetadata,
    ConnectionConfig,
    ConnectionMethod,
    TableMetadata,
)
from dimer.core.types import DataTypeMapper

logger = structlog.get_logger(__name__)


class DuckDBConnector(DataSourceConnector):
    """DuckDB connector — file-based or in-memory embedded database.

    The ``host`` field of :class:`~dimer.core.models.ConnectionConfig` holds
    the DuckDB file path (e.g. ``/data/mydb.duckdb``) or ``:memory:`` for a
    transient in-process database.

    .. note::
        Two ``:memory:`` connectors each hold **independent** databases.
        ``Diffcheck._is_same_instance()`` returns ``True`` for both (same
        ``host == ":memory:"``) and will route to JOIN_DIFF, which will fail
        at runtime because the tables exist in separate processes.  Comparing
        two in-memory DuckDB databases is not supported.
    """

    DEFAULT_SCHEMA = "main"
    IDENTIFIER_CASE = "lower"

    DIALECTS = {
        "hash": "md5(cast({COL} as varchar))",
        "concatenation": "||",
        "cast_to_text": "cast({COL} as varchar)",
        # DuckDB's built-in HASH() is a fast 64-bit non-crypto hash; BIT_XOR
        # collapses per-row hashes into a single segment fingerprint.
        "aggregate_hash": "bit_xor(hash({COL}))",
        "random_func": "random()",
    }
    # SKETCH_DIFF (UC3): approx_count_distinct is HyperLogLog;
    # approx_quantile is t-Digest. See ALGO.md §SKETCH_DIFF for sources.
    SKETCH_FUNCS = {
        "distinct": "approx_count_distinct({COL})",
        "distinct_algorithm": "HyperLogLog",
        "median": "approx_quantile({COL}, 0.5)",
        "median_algorithm": "t-Digest",
    }

    def get_required_params(self) -> List[str]:
        """DuckDB only needs the file path / ':memory:' in ``host``."""
        return ["host"]

    def get_connection_methods(self) -> List[ConnectionMethod]:
        return [
            ConnectionMethod.NATIVE,      # duckdb Python package (preferred)
            ConnectionMethod.SQLALCHEMY,  # duckdb-engine fallback
        ]

    # ------------------------------------------------------------------
    # Connection methods
    # ------------------------------------------------------------------

    def _connect_native(self) -> Any:
        """Open a DuckDB connection using the native ``duckdb`` package."""
        try:
            import duckdb
        except ImportError:
            raise ImportError(
                "duckdb is required for the native DuckDB connection. "
                "Install it with: pip install dimer[duckdb]"
            )

        db_path = self.connection_config.host
        logger.info("Opening DuckDB connection", db_path=db_path)
        conn = duckdb.connect(db_path)
        # Smoke-test
        conn.execute("SELECT 42").fetchone()
        logger.info("DuckDB native connection established", db_path=db_path)
        return conn

    def _connect_sqlalchemy(self) -> Any:
        """Open a DuckDB connection via SQLAlchemy + duckdb-engine."""
        try:
            from sqlalchemy import create_engine, text
        except ImportError:
            raise ImportError("sqlalchemy is required for the SQLAlchemy connection")
        try:
            import duckdb_engine  # noqa: F401
        except ImportError:
            raise ImportError(
                "duckdb-engine is required for the SQLAlchemy DuckDB connection. "
                "Install it with: pip install dimer[duckdb]"
            )

        db_path = self.connection_config.host
        # duckdb+duckdb_engine:///path/to/file or duckdb+duckdb_engine:///:memory:
        url = f"duckdb:///{db_path}"
        logger.info("Opening DuckDB SQLAlchemy connection", url=url)
        engine = create_engine(url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 42"))
        logger.info("DuckDB SQLAlchemy connection established")
        return engine

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def _execute_query_internal(
        self, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        if not self.connection:
            raise RuntimeError("No active connection. Call connect() first.")

        if self.connection_method_used == ConnectionMethod.NATIVE:
            if params:
                # duckdb native supports positional ? placeholders; convert
                # %(name)s style if present.
                import re

                param_order: list = []

                def _replace(m: Any) -> str:
                    param_order.append(m.group(1))
                    return "?"

                converted = re.sub(r"%\((\w+)\)s", _replace, query)
                param_values = [params[k] for k in param_order]
                result = self.connection.execute(converted, param_values)
            else:
                result = self.connection.execute(query)
            df = result.df()
            return df

        elif self.connection_method_used == ConnectionMethod.SQLALCHEMY:
            return pd.read_sql(query, self.connection, params=params)

        raise ValueError(
            f"Unsupported connection method: {self.connection_method_used}"
        )

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """Return column metadata and primary-key info for *table_name*."""
        schema = schema_name or self.connection_config.schema_name or self.DEFAULT_SCHEMA

        columns_query = """
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """

        logger.debug(
            "get_table_metadata: fetching columns",
            schema=schema,
            table_name=table_name,
        )

        if self.connection_method_used == ConnectionMethod.NATIVE:
            columns_df = self.connection.execute(
                columns_query, [schema, table_name]
            ).df()
        else:
            columns_df = self._execute_query_internal(
                columns_query.replace("?", "%s"),
                {"schema": schema, "table_name": table_name},
            )

        columns: List[ColumnMetadata] = []
        for _, row in columns_df.iterrows():
            common_type = DataTypeMapper.map_type("duckdb", str(row["data_type"]))
            col = ColumnMetadata(
                name=row["column_name"],
                data_type=common_type,
                nullable=str(row["is_nullable"]).upper() == "YES",
                max_length=row.get("character_maximum_length"),
                precision=row.get("numeric_precision"),
                scale=row.get("numeric_scale"),
                default_value=row.get("column_default"),
            )
            columns.append(col)

        # Primary keys via duckdb_constraints()
        pk_cols: set = set()
        try:
            pk_query = """
            SELECT constraint_column_names
            FROM duckdb_constraints()
            WHERE schema_name = ? AND table_name = ?
              AND constraint_type = 'PRIMARY KEY'
            """
            if self.connection_method_used == ConnectionMethod.NATIVE:
                pk_result = self.connection.execute(pk_query, [schema, table_name])
                pk_df = pk_result.df()
            else:
                pk_df = self._execute_query_internal(
                    pk_query.replace("?", "%s"),
                    {"schema": schema, "table_name": table_name},
                )

            for _, row in pk_df.iterrows():
                raw = row["constraint_column_names"]
                # Native duckdb returns a numpy.ndarray; SQLAlchemy may return
                # a JSON-encoded string.  Plain strings must be parsed before
                # being iterated so that list("…") doesn't yield characters.
                if isinstance(raw, str):
                    try:
                        names = json.loads(raw)
                    except (TypeError, ValueError):
                        names = []
                else:
                    try:
                        names = list(raw)
                    except TypeError:
                        names = []
                pk_cols.update(names)
        except Exception as exc:
            logger.warning("Could not retrieve primary key info", error=str(exc))

        for col in columns:
            if col.name in pk_cols:
                col.is_primary_key = True

        # Row count via COUNT(*)
        row_count: Optional[int] = None
        try:
            safe_schema = schema.replace('"', '""')
            safe_table = table_name.replace('"', '""')
            count_result = self._execute_query_internal(
                f'SELECT COUNT(*) AS cnt FROM "{safe_schema}"."{safe_table}"'
            )
            if not count_result.empty:
                row_count = int(count_result.iloc[0]["cnt"])
        except Exception as exc:
            logger.warning("Could not retrieve row count", error=str(exc))

        return TableMetadata(
            columns=columns,
            row_count=row_count,
            statistics={"schema": schema, "table": table_name},
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        schema = schema_name or self.connection_config.schema_name or self.DEFAULT_SCHEMA
        safe_schema = schema.replace('"', '""')
        safe_table = table_name.replace('"', '""')
        query = f'SELECT * FROM "{safe_schema}"."{safe_table}" LIMIT {limit}'
        return self._execute_query_internal(query)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        if schema_name:
            query = """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = ? AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
            if self.connection_method_used == ConnectionMethod.NATIVE:
                df = self.connection.execute(query, [schema_name]).df()
            else:
                df = self._execute_query_internal(
                    query.replace("?", "%s"), {"schema_name": schema_name}
                )
        else:
            query = """
            SELECT table_name FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_name
            """
            df = self._execute_query_internal(query)
        return df["table_name"].tolist()

    def _list_schemas_internal(self) -> List[str]:
        query = """
        SELECT schema_name FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schema_name
        """
        df = self._execute_query_internal(query)
        return df["schema_name"].tolist()

    def _get_test_query(self) -> str:
        return "SELECT version()"

    def close(self) -> None:
        """Close the DuckDB connection."""
        try:
            if self.connection:
                if self.connection_method_used == ConnectionMethod.NATIVE:
                    self.connection.close()
                elif self.connection_method_used == ConnectionMethod.SQLALCHEMY:
                    self.connection.dispose()
                self.connection = None
                self.connection_method_used = None
            logger.info("DuckDB connection closed successfully")
        except Exception as exc:
            logger.error("Error closing DuckDB connection", error=str(exc))
