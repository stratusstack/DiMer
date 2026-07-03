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

    # SKETCH_DIFF (UC3): TiDB, unlike vanilla MySQL, has its own native
    # APPROX_COUNT_DISTINCT (BJKST algorithm) and APPROX_PERCENTILE. Note
    # APPROX_PERCENTILE takes an integer *percentage* (0-100), not a 0-1
    # fraction like most other engines' percentile functions — 50 is the
    # median. APPROX_PERCENTILE only supports numeric and date/time return
    # types (matches _MEDIAN_ELIGIBLE_TYPES already). See ALGO.md §SKETCH_DIFF.
    SKETCH_FUNCS = {
        "distinct": "APPROX_COUNT_DISTINCT({COL})",
        "distinct_algorithm": "BJKST",
        "median": "APPROX_PERCENTILE({COL}, 50)",
        "median_algorithm": "TiDB APPROX_PERCENTILE (undocumented algorithm)",
    }
