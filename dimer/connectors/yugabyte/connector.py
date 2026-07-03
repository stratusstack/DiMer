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
    """

    DEFAULT_PORT = 5433
