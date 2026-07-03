"""TiDB connector — MySQL wire-compatible NewSQL/Distributed SQL."""

import structlog

from dimer.connectors.mysql.connector import MySQLConnector

logger = structlog.get_logger(__name__)


class TiDBConnector(MySQLConnector):
    """TiDB implementation reusing the MySQL wire protocol.

    TiDB is MySQL-compatible: connection methods (MYSQL_CONNECTOR → PYMYSQL
    → SQLALCHEMY), information_schema metadata queries, and the full SQL
    dialect (MD5/CONCAT row hash, BIT_XOR/CONV aggregate hash, NTILE window
    functions since TiDB 3.0) are inherited unchanged.  Only the default
    port differs.
    """

    DEFAULT_PORT = 4000
