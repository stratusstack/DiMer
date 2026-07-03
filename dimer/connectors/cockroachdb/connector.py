"""CockroachDB connector — PostgreSQL wire-compatible NewSQL/Distributed SQL."""

import structlog

from dimer.connectors.postgresql.connector import PostgreSQLConnector

logger = structlog.get_logger(__name__)


class CockroachDBConnector(PostgreSQLConnector):
    """CockroachDB implementation reusing the PostgreSQL wire protocol.

    CockroachDB speaks the PostgreSQL protocol, so all connection methods
    (ASYNCPG → PSYCOPG2 → SQLALCHEMY), information_schema metadata queries,
    and the row-level hash dialect are inherited unchanged.

    Only the segment-level aggregate hash differs: CockroachDB has no CONV()
    but ships ``xor_agg`` over bytes and ``decode``, so BISECTION XORs the
    raw MD5 digests directly.

    SKETCH_FUNCS (UC3) is inherited unchanged from PostgreSQL: CockroachDB's
    ``CREATE EXTENSION`` is a documented no-op, so ``postgresql-hll`` cannot
    actually be installed — there is no native cardinality sketch here.
    PERCENTILE_CONT/PERCENTILE_DISC *are* natively supported (confirmed via
    CockroachDB's own aggregate-function docs), so the inherited exact
    median fallback is correct. See ALGO.md §SKETCH_DIFF.
    """

    DEFAULT_PORT = 26257
    DIALECTS = {
        **PostgreSQLConnector.DIALECTS,
        "aggregate_hash": "xor_agg(decode(md5({COL}), 'hex'))",
    }
