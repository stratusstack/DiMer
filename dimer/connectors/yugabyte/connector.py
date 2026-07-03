"""YugabyteDB connector — PostgreSQL-fork NewSQL/Distributed SQL (YSQL)."""

import structlog

from dimer.connectors.postgresql.connector import PostgreSQLConnector

logger = structlog.get_logger(__name__)


class YugabyteConnector(PostgreSQLConnector):
    """YugabyteDB YSQL implementation.

    YSQL is a fork of the actual PostgreSQL query layer, so every connection
    method, metadata query, and SQL dialect entry (including the aggregate
    hash used by BISECTION) is inherited from the PostgreSQL connector
    unchanged.  Only the default port differs.

    SKETCH_FUNCS (UC3) is also inherited unchanged: YugabyteDB's ``hll``
    extension *can* be installed (`CREATE EXTENSION hll`, unlike
    CockroachDB where the statement is a no-op), but nothing guarantees a
    given cluster has actually done so — DiMer cannot assume server-side
    admin actions it didn't perform. Detecting the extension at connect time
    and opportunistically using it is tracked as a follow-up in
    TODO_FOR_LATER.md; for now Yugabyte gets the same exact-fallback
    behavior as PostgreSQL (median via PERCENTILE_CONT, which YSQL supports
    natively; distinct-count via exact COUNT(DISTINCT)).
    """

    DEFAULT_PORT = 5433
