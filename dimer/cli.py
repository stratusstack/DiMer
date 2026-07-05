"""DiMer command-line data diff utility."""

import logging
import os
import sys
import traceback
from typing import Dict, List, Optional, Tuple

import structlog
from dotenv import load_dotenv

from dimer.core.compare import (
    Diffcheck,
    BISECTION_DEFAULT_THRESHOLD,
    BLOOM_DEFAULT_FPR,
    EMBEDDING_DEFAULT_METRIC,
    EMBEDDING_DEFAULT_THRESHOLD,
    PROFILE_DEFAULT_NUMERIC_TOLERANCE,
    SAMPLED_DEFAULT_SIZE,
    SAMPLED_DEFAULT_CONFIDENCE,
    SKETCH_DEFAULT_RELATIVE_TOLERANCE,
)
from dimer.core.factory import ConnectorFactory
from dimer.core.models import (
    ComparisonConfig,
    ConnectionConfig,
    DiffAlgorithm,
    DiffRun,
    SearchMode,
    SearchRun,
    TableMetadata,
)
from dimer.core.search import VALUE_SEARCH_DEFAULT_MAX_VALUES, ValueSearch

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------

_DEV_MODE: bool = False


def _strip_exc_info_in_normal_mode(_, __, event_dict):
    """Remove exc_info from log events unless running in dev mode."""
    if not _DEV_MODE:
        event_dict.pop("exc_info", None)
    return event_dict


# Third-party loggers that are excessively noisy at DEBUG level.
_NOISY_LOGGERS = [
    "snowflake.connector",
    "botocore",
    "boto3",
    "urllib3",
    "asyncio",
]


def configure_logging(debug: bool) -> None:
    """Configure structlog and stdlib logging level."""
    global _DEV_MODE
    _DEV_MODE = debug

    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=log_level)

    # Keep third-party libraries quiet even in dev mode.
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.processors.StackInfoRenderer(),
            _strip_exc_info_in_normal_mode,
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=False,
    )


# ---------------------------------------------------------------------------
# ANSI colour helpers
# ---------------------------------------------------------------------------


class _C:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"


def _green(s: str) -> str:
    return f"{_C.GREEN}{s}{_C.RESET}"


def _red(s: str) -> str:
    return f"{_C.RED}{s}{_C.RESET}"


def _yellow(s: str) -> str:
    return f"{_C.YELLOW}{s}{_C.RESET}"


def _cyan(s: str) -> str:
    return f"{_C.CYAN}{s}{_C.RESET}"


def _bold(s: str) -> str:
    return f"{_C.BOLD}{s}{_C.RESET}"


def _dim(s: str) -> str:
    return f"{_C.DIM}{s}{_C.RESET}"


# ---------------------------------------------------------------------------
# Source metadata
# ---------------------------------------------------------------------------

SUPPORTED_SOURCES: List[str] = [
    "snowflake",
    "postgresql",
    "mysql",
    "bigquery",
    "databricks",
    "duckdb",
    "cockroachdb",
    "yugabyte",
    "tidb",
    "mongodb",
    "redis",
    "cassandra",
    "elasticsearch",
    "neo4j",
    "qdrant",
    "influxdb",
]

REQUIRED_VARS: Dict[str, List[str]] = {
    "snowflake": [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_DATABASE",
    ],
    "postgresql": [
        "POSTGRES_HOST",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DATABASE",
    ],
    "mysql": [
        "MYSQL_HOST",
        "MYSQL_USER",
        "MYSQL_PASSWORD",
        "MYSQL_DATABASE",
    ],
    "bigquery": [
        "BIGQUERY_PROJECT_ID",
        "BIGQUERY_DATASET",
        "BIGQUERY_CREDENTIALS_PATH",
    ],
    "databricks": [
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_HTTP_PATH",
    ],
    "duckdb": [
        "DUCKDB_DATABASE",
    ],
    "cockroachdb": [
        "COCKROACH_HOST",
        "COCKROACH_USER",
        "COCKROACH_PASSWORD",
        "COCKROACH_DATABASE",
    ],
    "yugabyte": [
        "YUGABYTE_HOST",
        "YUGABYTE_USER",
        "YUGABYTE_PASSWORD",
        "YUGABYTE_DATABASE",
    ],
    "tidb": [
        "TIDB_HOST",
        "TIDB_USER",
        "TIDB_PASSWORD",
        "TIDB_DATABASE",
    ],
    "mongodb": [
        "MONGODB_HOST",
        "MONGODB_DATABASE",
    ],
    "redis": [
        "REDIS_HOST",
    ],
    "cassandra": [
        "CASSANDRA_HOST",
        "CASSANDRA_DATABASE",
    ],
    "elasticsearch": [
        "ELASTICSEARCH_HOST",
    ],
    "neo4j": [
        "NEO4J_HOST",
        "NEO4J_USER",
        "NEO4J_PASSWORD",
    ],
    "qdrant": [
        "QDRANT_HOST",
    ],
    "influxdb": [
        "INFLUXDB_HOST",
        "INFLUXDB_DATABASE",
    ],
}

# Shown to the user when they are asked to enter the FQ table name
FQ_HINTS: Dict[str, str] = {
    "snowflake": "SCHEMA.TABLE              e.g. PUBLIC.ORDERS",
    "postgresql": "schema.table              e.g. public.orders",
    "mysql": "database.table            e.g. mydb.customers",
    "bigquery": (
        "dataset.table             e.g. my_dataset.orders\n"
        "                          or   project.dataset.table"
    ),
    "databricks": (
        "schema.table              e.g. default.orders\n"
        "                          or   catalog.schema.table"
    ),
    "duckdb": "schema.table              e.g. main.orders",
    "cockroachdb": "schema.table              e.g. public.orders",
    "yugabyte": "schema.table              e.g. public.orders",
    "tidb": "database.table            e.g. mydb.customers",
    "mongodb": (
        "collection                e.g. orders\n"
        "                          or   database.collection"
    ),
    "redis": "key namespace pattern     e.g. user  (expands to user:*) or user:*",
    "cassandra": "keyspace.table            e.g. app.orders",
    "elasticsearch": "index                     e.g. orders",
    "neo4j": "node label                e.g. Order",
    "qdrant": "collection                e.g. orders",
    "influxdb": "measurement               e.g. orders",
}


# ---------------------------------------------------------------------------
# ConnectionConfig builders
# ---------------------------------------------------------------------------


def build_config(source_type: str) -> ConnectionConfig:
    """Build a ConnectionConfig from environment variables for the given source."""
    if source_type == "snowflake":
        return ConnectionConfig(
            host=os.getenv("SNOWFLAKE_ACCOUNT"),
            username=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema_name=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
            extra_params={
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
                "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
                "authenticator": os.getenv("SNOWFLAKE_AUTHENTICATOR", "snowflake"),
            },
        )
    if source_type == "postgresql":
        return ConnectionConfig(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            username=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DATABASE"),
            schema_name=os.getenv("POSTGRES_SCHEMA", "public"),
            extra_params={
                "ssl_mode": os.getenv("POSTGRES_SSL_MODE", "prefer"),
            },
        )
    if source_type == "mysql":
        return ConnectionConfig(
            host=os.getenv("MYSQL_HOST", "localhost"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            username=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
            extra_params={
                "charset": os.getenv("MYSQL_CHARSET", "utf8mb4"),
            },
        )
    if source_type == "bigquery":
        return ConnectionConfig(
            database=os.getenv("BIGQUERY_PROJECT_ID"),
            schema_name=os.getenv("BIGQUERY_DATASET"),
            extra_params={
                "credentials_path": os.getenv("BIGQUERY_CREDENTIALS_PATH"),
                "location": os.getenv("BIGQUERY_LOCATION", "US"),
            },
        )
    if source_type == "databricks":
        return ConnectionConfig(
            host=os.getenv("DATABRICKS_HOST"),
            extra_params={
                "token": os.getenv("DATABRICKS_TOKEN"),
                "http_path": os.getenv("DATABRICKS_HTTP_PATH"),
                "catalog": os.getenv("DATABRICKS_CATALOG", "main"),
                "schema": os.getenv("DATABRICKS_SCHEMA", "default"),
            },
        )
    if source_type == "duckdb":
        return ConnectionConfig(
            host=os.getenv("DUCKDB_DATABASE", ":memory:"),
            schema_name=os.getenv("DUCKDB_SCHEMA", "main"),
        )
    if source_type == "cockroachdb":
        return ConnectionConfig(
            host=os.getenv("COCKROACH_HOST", "localhost"),
            port=int(os.getenv("COCKROACH_PORT", "26257")),
            username=os.getenv("COCKROACH_USER"),
            password=os.getenv("COCKROACH_PASSWORD"),
            database=os.getenv("COCKROACH_DATABASE"),
            schema_name=os.getenv("COCKROACH_SCHEMA", "public"),
            extra_params={
                "ssl_mode": os.getenv("COCKROACH_SSL_MODE", "prefer"),
            },
        )
    if source_type == "yugabyte":
        return ConnectionConfig(
            host=os.getenv("YUGABYTE_HOST", "localhost"),
            port=int(os.getenv("YUGABYTE_PORT", "5433")),
            username=os.getenv("YUGABYTE_USER"),
            password=os.getenv("YUGABYTE_PASSWORD"),
            database=os.getenv("YUGABYTE_DATABASE"),
            schema_name=os.getenv("YUGABYTE_SCHEMA", "public"),
            extra_params={
                "ssl_mode": os.getenv("YUGABYTE_SSL_MODE", "prefer"),
            },
        )
    if source_type == "tidb":
        return ConnectionConfig(
            host=os.getenv("TIDB_HOST", "localhost"),
            port=int(os.getenv("TIDB_PORT", "4000")),
            username=os.getenv("TIDB_USER"),
            password=os.getenv("TIDB_PASSWORD"),
            database=os.getenv("TIDB_DATABASE"),
            extra_params={
                "charset": os.getenv("TIDB_CHARSET", "utf8mb4"),
            },
        )
    if source_type == "mongodb":
        return ConnectionConfig(
            host=os.getenv("MONGODB_HOST", "localhost"),
            port=int(os.getenv("MONGODB_PORT", "27017")),
            username=os.getenv("MONGODB_USER"),
            password=os.getenv("MONGODB_PASSWORD"),
            database=os.getenv("MONGODB_DATABASE"),
            extra_params={
                "uri": os.getenv("MONGODB_URI"),
                "auth_source": os.getenv("MONGODB_AUTH_SOURCE", "admin"),
            },
        )
    if source_type == "redis":
        return ConnectionConfig(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            password=os.getenv("REDIS_PASSWORD"),
            extra_params={
                "db": int(os.getenv("REDIS_DB", "0")),
            },
        )
    if source_type == "cassandra":
        return ConnectionConfig(
            host=os.getenv("CASSANDRA_HOST", "localhost"),
            port=int(os.getenv("CASSANDRA_PORT", "9042")),
            username=os.getenv("CASSANDRA_USER"),
            password=os.getenv("CASSANDRA_PASSWORD"),
            database=os.getenv("CASSANDRA_DATABASE"),
        )
    if source_type == "elasticsearch":
        return ConnectionConfig(
            host=os.getenv("ELASTICSEARCH_HOST", "localhost"),
            port=int(os.getenv("ELASTICSEARCH_PORT", "9200")),
            username=os.getenv("ELASTICSEARCH_USER"),
            password=os.getenv("ELASTICSEARCH_PASSWORD"),
            extra_params={
                "scheme": os.getenv("ELASTICSEARCH_SCHEME", "https"),
                "api_key": os.getenv("ELASTICSEARCH_API_KEY"),
            },
        )
    if source_type == "neo4j":
        return ConnectionConfig(
            host=os.getenv("NEO4J_HOST", "localhost"),
            port=int(os.getenv("NEO4J_PORT", "7687")),
            username=os.getenv("NEO4J_USER"),
            password=os.getenv("NEO4J_PASSWORD"),
            extra_params={
                "scheme": os.getenv("NEO4J_SCHEME", "bolt"),
            },
        )
    if source_type == "qdrant":
        return ConnectionConfig(
            host=os.getenv("QDRANT_HOST", "localhost"),
            port=int(os.getenv("QDRANT_PORT", "6333")),
            extra_params={
                "api_key": os.getenv("QDRANT_API_KEY"),
            },
        )
    if source_type == "influxdb":
        return ConnectionConfig(
            host=os.getenv("INFLUXDB_HOST", "localhost"),
            port=int(os.getenv("INFLUXDB_PORT", "8086")),
            username=os.getenv("INFLUXDB_USER"),
            password=os.getenv("INFLUXDB_PASSWORD"),
            database=os.getenv("INFLUXDB_DATABASE"),
        )
    raise ValueError(f"Unknown source type: {source_type!r}")


# ---------------------------------------------------------------------------
# .env verification
# ---------------------------------------------------------------------------


def check_env(source_type: str) -> Tuple[bool, List[str]]:
    """
    Reload .env and check required vars for source_type.
    Prints a ✓/✗ line for each variable.
    Returns (all_ok, missing_vars).
    """
    load_dotenv(override=True)
    missing: List[str] = []
    for var in REQUIRED_VARS[source_type]:
        if os.getenv(var):
            print(f"    {_green('✓')}  {var}")
        else:
            print(f"    {_red('✗')}  {var}  {_dim('(not set)')}")
            missing.append(var)
    return len(missing) == 0, missing


def verify_config_loop(source_type: str, label: str) -> None:
    """
    Verify .env config for source_type, looping until all required
    variables are present (user updates .env and presses Enter to retry).
    """
    while True:
        print(f"\n  Checking {_bold(label)} ({_cyan(source_type)}) configuration...")
        ok, missing = check_env(source_type)
        if ok:
            print(f"    {_green('All required variables are set.')}")
            return
        print(
            f"\n    {_yellow('⚠')}  Missing: {_bold(', '.join(missing))}\n"
            f"    Please set the above variables in your {_bold('.env')} file\n"
            f"    then press {_bold('Enter')} to re-check (or Ctrl+C to exit)."
        )
        input()


# ---------------------------------------------------------------------------
# Interactive prompts
# ---------------------------------------------------------------------------


def select_source(label: str) -> str:
    """Display a numbered menu of supported sources and return the chosen one."""
    print(f"\n  {_bold(label + ':')} ")
    for i, src in enumerate(SUPPORTED_SOURCES, 1):
        print(f"    {_cyan(str(i))}.  {src}")
    while True:
        raw = input(f"\n    Enter number (1–{len(SUPPORTED_SOURCES)}): ").strip()
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(SUPPORTED_SOURCES):
                chosen = SUPPORTED_SOURCES[idx]
                print(f"    → {_green(chosen)}")
                return chosen
        print(f"    {_red('Invalid choice.')}  Please enter a number between 1 and {len(SUPPORTED_SOURCES)}.")


def prompt_fq_table(source_type: str, label: str) -> str:
    """
    Prompt for a fully-qualified table name, showing a source-specific
    format hint.  Returns the raw string entered by the user.
    """
    hint = FQ_HINTS[source_type]
    print(f"\n  {_bold(label)} ({_cyan(source_type)}) — table name")
    for line in hint.splitlines():
        print(f"    {_dim(line)}")
    while True:
        raw = input("    > ").strip()
        if raw:
            return raw
        print(f"    {_red('Table name cannot be empty.')}")


def _parse_fq_table(fq_table: str) -> Tuple[Optional[str], str]:
    """
    Split a fully-qualified table identifier into (schema, table).

    Handles:
      - "table"              → (None, "table")
      - "schema.table"       → ("schema", "table")
      - "cat.schema.table"   → ("schema", "table")   ← last two parts used
    Strips surrounding quotes/backticks from each part.
    """
    parts = [p.strip().strip('"').strip("`") for p in fq_table.split(".")]
    if len(parts) == 1:
        return None, parts[0]
    return parts[-2], parts[-1]


def detect_or_prompt_keys(connector, fq_table: str, label: str) -> Tuple[List[str], TableMetadata]:
    """
    Try to detect primary key columns from table metadata.
    If found, ask the user to confirm or override.
    If not found (or metadata unavailable), prompt for manual entry.

    Raises if get_table_metadata() fails — metadata is mandatory.

    Returns:
        (keys, metadata)
    """
    schema, table = _parse_fq_table(fq_table)
    detected: List[str] = []

    print(f"\n  Detecting join keys for {_bold(label)} ({_cyan(fq_table)})...")
    try:
        metadata = connector.get_table_metadata(table, schema_name=schema)
        detected = [col.name for col in metadata.columns if col.is_primary_key]
    except Exception as e:
        print(f"    {_red('✗')}  Could not read table metadata for {fq_table}: {e}")
        raise

    if detected:
        print(f"    {_green('✓')}  Primary keys detected: {_bold(', '.join(detected))}")
        ans = input("    Use these as join keys? [Y/n]: ").strip().lower()
        if ans in ("", "y", "yes"):
            return detected, metadata

    # Manual entry
    print(f"    {_dim('Enter the column(s) to use as join keys (comma-separated).')}")
    while True:
        raw = input("    > ").strip()
        keys = [k.strip() for k in raw.split(",") if k.strip()]
        if keys:
            return keys, metadata
        print(f"    {_red('At least one key column is required.')}")


# ---------------------------------------------------------------------------
# Bisection prompt
# ---------------------------------------------------------------------------


def prompt_bisection(
    fq1: str,
    fq2: str,
    keys1: List[str],
    row_count1: Optional[int],
    row_count2: Optional[int],
) -> Tuple[bool, Optional[str], int]:
    """Prompt the user about whether to use the BISECTION algorithm.

    Uses pre-fetched row counts from table metadata; auto-suggests bisection
    when either table exceeds 1 million rows.

    Returns:
        (use_bisection, bisection_key_override, threshold)
    """
    ROW_SUGGEST_THRESHOLD = 1_000_000

    print(f"\n  {_bold('Algorithm selection')}")

    if row_count1 is not None:
        print(f"  Source row count from {fq1}: {row_count1:,}")
    else:
        print(f"  {_yellow('⚠')}  Row count unavailable for {fq1} — auto-suggest disabled.")
    if row_count2 is not None:
        print(f"  Source row count from {fq2}: {row_count2:,}")
    else:
        print(f"  {_yellow('⚠')}  Row count unavailable for {fq2} — auto-suggest disabled.")

    row_count_threshold_exceeded1 = (row_count1 is not None and row_count1 > ROW_SUGGEST_THRESHOLD) 
    row_count_threshold_exceeded2 = (row_count2 is not None and row_count2 > ROW_SUGGEST_THRESHOLD)
    auto_suggest = row_count_threshold_exceeded1 or row_count_threshold_exceeded2
    if auto_suggest:
        print(
            f"  {_yellow('⚠')}  Large table detected ({row_count1 if row_count_threshold_exceeded1 else row_count2:,} rows). "
            f"BISECTION algorithm recommended."
        )

    prompt = "  Use BISECTION algorithm? [Y/n]: " if auto_suggest else "  Use BISECTION algorithm? [y/N]: "
    ans = input(prompt).strip().lower()

    if auto_suggest:
        use_bisection = ans not in ("n", "no")
    else:
        use_bisection = ans in ("y", "yes")

    if not use_bisection:
        return False, None, BISECTION_DEFAULT_THRESHOLD

    # Optional bisection key override
    default_key = keys1[0] if keys1 else ""
    raw_key = input(
        f"  Bisection key column [{_dim(default_key)}]: "
    ).strip()
    bisection_key: Optional[str] = raw_key if raw_key else None

    # Optional threshold override
    raw_threshold = input(
        f"  Threshold rows/segment [{_dim(str(BISECTION_DEFAULT_THRESHOLD))}]: "
    ).strip()
    threshold = int(raw_threshold) if raw_threshold.isdigit() and int(raw_threshold) > 0 else BISECTION_DEFAULT_THRESHOLD

    return True, bisection_key, threshold


def prompt_schema_diff() -> "Tuple[bool, bool]":
    """Prompt the user about running a schema-only (structure) diff.

    Compares catalog metadata — column sets, types, nullability, primary
    keys — without reading any data rows.  No join keys are needed.

    Returns:
        (use_schema_diff, schema_strict)
    """
    print(f"\n  {_bold('Schema-only diff')} {_dim('(structure compare; no data read)')}")
    print(f"  {_dim('Compares column sets, types, nullability and primary keys')}")
    print(f"  {_dim('from catalog metadata. Useful as a CI/CD gate before a data diff.')}")

    ans = input("  Compare schemas only (no data)? [y/N]: ").strip().lower()
    if ans not in ("y", "yes"):
        return False, False

    strict = input(
        "  Strict mode — also compare length/precision/scale? "
        f"{_dim('(noisy across engines)')} [y/N]: "
    ).strip().lower() in ("y", "yes")

    return True, strict


def prompt_aggregate_diff() -> "Tuple[bool, bool, float]":
    """Prompt the user to choose between PROFILE_DIFF, SKETCH_DIFF, or neither.

    Both algorithms compare per-column aggregates instead of row data (no
    join keys needed) — the choice is exact-but-heavier vs. approximate-
    but-cheap-at-scale.

    Returns:
        (use_profile_diff, use_sketch_diff, tolerance) — tolerance is
        PROFILE_DEFAULT_NUMERIC_TOLERANCE / SKETCH_DEFAULT_RELATIVE_TOLERANCE
        for whichever mode was chosen (unused for the other).
    """
    print(f"\n  {_bold('Aggregate diff')} {_dim('(per-column stats instead of row data; cheap triage)')}")
    print(f"    {_cyan('1')}. {_bold('PROFILE_DIFF')}  {_dim('— exact count/nulls/distinct/min/max/avg/sum per column')}")
    print(f"    {_cyan('2')}. {_bold('SKETCH_DIFF')}   {_dim('— approximate cardinality (HLL-family) + approximate median; cheaper at huge scale')}")
    print(f"    {_cyan('3')}. {_dim('Skip — use a row-level diff instead')}")

    raw = input("  Enter number (1-3) [3]: ").strip()

    if raw == "1":
        print(f"  {_dim('Equal profiles do NOT prove equal rows — this is a triage signal.')}")
        raw_tol = input(
            f"  Numeric tolerance (relative) [{_dim(str(PROFILE_DEFAULT_NUMERIC_TOLERANCE))}]: "
        ).strip()
        try:
            tolerance = float(raw_tol) if raw_tol else PROFILE_DEFAULT_NUMERIC_TOLERANCE
            if tolerance < 0:
                tolerance = PROFILE_DEFAULT_NUMERIC_TOLERANCE
        except ValueError:
            tolerance = PROFILE_DEFAULT_NUMERIC_TOLERANCE
        return True, False, tolerance

    if raw == "2":
        print(f"  {_dim('Uses each engine native sketch function where available')}")
        print(f"  {_dim('(HyperLogLog-family / t-Digest / Greenwald-Khanna); falls back to')}")
        print(f"  {_dim('exact COUNT(DISTINCT)/PERCENTILE_CONT where an engine has none.')}")
        raw_tol = input(
            f"  Relative tolerance [{_dim(str(SKETCH_DEFAULT_RELATIVE_TOLERANCE))}]: "
        ).strip()
        try:
            tolerance = float(raw_tol) if raw_tol else SKETCH_DEFAULT_RELATIVE_TOLERANCE
            if tolerance < 0:
                tolerance = SKETCH_DEFAULT_RELATIVE_TOLERANCE
        except ValueError:
            tolerance = SKETCH_DEFAULT_RELATIVE_TOLERANCE
        return False, True, tolerance

    return False, False, PROFILE_DEFAULT_NUMERIC_TOLERANCE


def prompt_embedding() -> "Tuple[bool, Optional[str], str, float]":
    """Prompt the user about whether to use the EMBEDDING_SIMILARITY algorithm.

    Intended for vector sources (e.g. pgvector columns) where float noise
    between index builds makes row-hash equality meaningless.

    Returns:
        (use_embedding, vector_column, distance_metric, distance_threshold)
    """
    print(f"\n  {_bold('Embedding similarity')} {_dim('(vector columns, e.g. pgvector)')}")
    print(f"  {_dim('Rows are MODIFIED when the vector distance exceeds a tolerance,')}")
    print(f"  {_dim('instead of exact value equality.')}")

    ans = input("  Compare an embedding/vector column? [y/N]: ").strip().lower()
    if ans not in ("y", "yes"):
        return False, None, EMBEDDING_DEFAULT_METRIC, EMBEDDING_DEFAULT_THRESHOLD

    vector_column = ""
    while not vector_column:
        vector_column = input("  Vector column name: ").strip()
        if not vector_column:
            print(f"    {_red('Vector column is required for embedding similarity.')}")

    raw_metric = input(f"  Distance metric (cosine / l2) [{_dim(EMBEDDING_DEFAULT_METRIC)}]: ").strip().lower()
    metric = raw_metric if raw_metric in ("cosine", "l2") else EMBEDDING_DEFAULT_METRIC

    raw_thresh = input(f"  Distance threshold [{_dim(str(EMBEDDING_DEFAULT_THRESHOLD))}]: ").strip()
    try:
        threshold = float(raw_thresh) if raw_thresh else EMBEDDING_DEFAULT_THRESHOLD
    except ValueError:
        threshold = EMBEDDING_DEFAULT_THRESHOLD

    return True, vector_column, metric, threshold


def prompt_bloom() -> "Tuple[bool, float]":
    """Prompt the user about whether to run the BLOOM prefilter.

    Returns:
        (use_bloom, bloom_fpr)
    """
    print(f"\n  {_bold('Bloom prefilter')} {_dim('(cheap definitely-differs signal; not an exact diff)')}")
    print(f"  {_dim('Fetches only keys + row hashes; reported differences are certain,')}")
    print(f"  {_dim('but up to the false-positive rate of real differences may be missed.')}")

    ans = input("  Run BLOOM prefilter instead of a full diff? [y/N]: ").strip().lower()
    if ans not in ("y", "yes"):
        return False, BLOOM_DEFAULT_FPR

    raw_fpr = input(f"  Target false-positive rate [{_dim(str(BLOOM_DEFAULT_FPR))}]: ").strip()
    try:
        fpr = float(raw_fpr) if raw_fpr else BLOOM_DEFAULT_FPR
        if not (0.0 < fpr < 1.0):
            print(f"  {_yellow('⚠')}  FPR must be between 0 and 1; defaulting to {BLOOM_DEFAULT_FPR}.")
            fpr = BLOOM_DEFAULT_FPR
    except ValueError:
        fpr = BLOOM_DEFAULT_FPR

    return True, fpr


def prompt_sampling() -> "Tuple[bool, int, float]":
    """Prompt the user about whether to use the SAMPLED algorithm.

    Only offered for cross-database comparisons.

    Returns:
        (use_sampling, sample_size, confidence)
    """
    print(f"\n  {_bold('Sampling')} {_dim('(statistical alternative to full table fetch)')}")
    print(f"  {_dim('Samples source rows → fetches matching rows in target → estimates diff rate.')}")
    print(f"  {_yellow('⚠')}  {_dim('ADDED rows in target are not detected (source-perspective only).')}")

    ans = input("  Use SAMPLED algorithm? [y/N]: ").strip().lower()
    if ans not in ("y", "yes"):
        return False, SAMPLED_DEFAULT_SIZE, SAMPLED_DEFAULT_CONFIDENCE

    # Sample size
    print(f"  {_dim('Guidance — rows needed for target margin of error at 95% confidence:')}")
    print(f"  {_dim('  ±5% → 385   ±2% → 2,401   ±1% → 9,604   ±0.5% → 38,416')}")
    raw_n = input(f"  Sample size [{_dim(str(SAMPLED_DEFAULT_SIZE))}]: ").strip()
    sample_size = int(raw_n) if raw_n.isdigit() and int(raw_n) > 0 else SAMPLED_DEFAULT_SIZE

    # Confidence level
    raw_conf = input(f"  Confidence level (0.90 / 0.95 / 0.99) [{_dim('0.95')}]: ").strip()
    try:
        conf = float(raw_conf) if raw_conf else SAMPLED_DEFAULT_CONFIDENCE
        if conf not in (0.90, 0.95, 0.99):
            print(f"  {_yellow('⚠')}  Unrecognised confidence level; defaulting to 0.95.")
            conf = SAMPLED_DEFAULT_CONFIDENCE
    except ValueError:
        conf = SAMPLED_DEFAULT_CONFIDENCE

    return True, sample_size, conf


# ---------------------------------------------------------------------------
# Value search prompts (UC10)
# ---------------------------------------------------------------------------


def prompt_task() -> str:
    """Ask whether to run a diff or a value search. Returns 'diff' or 'search'."""
    print(f"\n  {_bold('Task:')}")
    print(f"    {_cyan('1')}. {_bold('Data diff')}     {_dim('— compare two assets (rows, schema, or aggregates)')}")
    print(f"    {_cyan('2')}. {_bold('Value search')}  {_dim('— find where one column values occur in another table')}")
    raw = input("  Enter number (1-2) [1]: ").strip()
    return "search" if raw == "2" else "diff"


def prompt_match_mode() -> SearchMode:
    """Ask which matching type to use for a value search.

    Shows a one-liner per mode so the user can weigh precision vs cost.
    """
    print(f"\n  {_bold('Matching type:')}")
    print(f"    {_cyan('1')}. {_bold('EXACT')}    {_dim('— value must equal a target cell exactly (pushdown IN semi-join; fast, index-friendly)')}")
    print(f"    {_cyan('2')}. {_bold('PATTERN')}  {_dim('— value matches anywhere inside a target cell (LIKE %value%; full column scan, slower)')}")
    raw = input("  Enter number (1-2) [1]: ").strip()
    return SearchMode.PATTERN if raw == "2" else SearchMode.EXACT


def run_value_search(connector1, src1: str, connector2, src2: str) -> None:
    """Interactive UC10 flow: prompt, run the search, and display the result.

    Target 1 is the *source of values*; Target 2 is the table searched.
    """
    if not (getattr(connector1, "SUPPORTS_SQL", True) and getattr(connector2, "SUPPORTS_SQL", True)):
        print(f"\n  {_red('✗')}  Value search requires SQL sources on both sides "
              f"(non-SQL connectors are not supported yet).")
        return

    print(f"\n  {_dim('Target 1 provides the values; Target 2 is searched for them.')}")
    fq_source = prompt_fq_table(src1, "Target 1 (source of values)")

    source_column = ""
    while not source_column:
        source_column = input("    Column whose values to search for: ").strip()
        if not source_column:
            print(f"    {_red('A source column is required.')}")

    fq_target = prompt_fq_table(src2, "Target 2 (searched table)")

    raw_cols = input(
        f"    Target columns to search {_dim('(comma-separated; empty = all searchable columns)')}: "
    ).strip()
    target_columns = [c.strip() for c in raw_cols.split(",") if c.strip()] or None

    mode = prompt_match_mode()

    raw_max = input(
        f"  Max distinct source values [{_dim(str(VALUE_SEARCH_DEFAULT_MAX_VALUES))}]: "
    ).strip()
    max_values = (
        int(raw_max) if raw_max.isdigit() and int(raw_max) > 0
        else VALUE_SEARCH_DEFAULT_MAX_VALUES
    )

    # Confirmation summary
    print()
    print("  " + "─" * 54)
    print(f"  Values  : {_cyan(src1):<20} {_bold(fq_source)}.{_bold(source_column)}")
    print(f"  Search  : {_cyan(src2):<20} {_bold(fq_target)}"
          f"  ({', '.join(target_columns) if target_columns else 'all searchable columns'})")
    print(f"  Mode    : {mode}  (max {max_values:,} values)")
    print("  " + "─" * 54)

    ans = input("\n  Run search? [Y/n]: ").strip().lower()
    if ans not in ("", "y", "yes"):
        print(_dim("  Skipped."))
        return

    print("\n  Running value search...", flush=True)
    try:
        run = ValueSearch(
            connector1,
            connector2,
            {"fq_table_name": fq_source, "source_column": source_column, "max_values": max_values},
            {"fq_table_name": fq_target, **({"target_columns": target_columns} if target_columns else {})},
            mode=mode,
        ).search()
        display_search_result(run)
        print(f"  {_dim('Search runs are not persisted yet (search_run tables are on the backlog).')}")
    except Exception as exc:
        print(f"\n  {_red('✗  Value search failed:')} {exc}")
        if _DEV_MODE:
            traceback.print_exc()


def display_search_result(run: SearchRun) -> None:
    """Print a human-readable summary of a SearchRun."""
    print()
    print("  " + "─" * 54)

    if run.error:
        print(f"  {_red('✗  ERROR')}  {run.error}")
        print("  " + "─" * 54)
        return

    if run.values_found == run.values_searched:
        headline = _green(f"✓  ALL {run.values_searched:,} values found")
    elif run.values_found:
        headline = _yellow(f"◐  {run.values_found:,} of {run.values_searched:,} values found")
    else:
        headline = _red(f"✗  none of {run.values_searched:,} values found")
    print(f"  {headline}  in {_bold(run.target_table)}")

    print(f"  Mode           : {run.mode}")
    if run.execution_time_seconds is not None:
        print(f"  Elapsed        : {run.execution_time_seconds:.2f}s")
    m = run.metadata or {}
    truncated_note = _dim(" (value cap hit — results are a lower bound)") if m.get("source_values_truncated") else ""
    print(f"  Source values  : {run.values_searched:,} distinct from "
          f"{run.source_table}.{run.source_column}{truncated_note}")
    skipped = m.get("columns_skipped") or []
    skipped_note = _dim(f" (skipped unsearchable: {', '.join(skipped)})") if skipped else ""
    print(f"  Columns        : {len(run.columns_searched)} searched{skipped_note}")

    hit_stats = [s for s in run.column_stats if s.values_matched]
    if hit_stats:
        print(f"\n  {_bold('Per-column hits')} {_dim('(highest hit-rate column likely corresponds to the source column)')}")
        print(f"  {'Column':<28} {'Values matched':>14} {'Occurrences':>12} {'Hit rate':>9}")
        print(f"  {'─'*28} {'─'*14} {'─'*12} {'─'*9}")
        for s in hit_stats[:10]:
            print(f"  {s.column:<28} {s.values_matched:>14,} {s.total_occurrences:>12,} {s.hit_rate:>8.0%}")

    if run.matches:
        show = run.matches[:10]
        print(f"\n  {_bold('Top matches')} (showing {len(show)} of {len(run.matches)}):")
        for match in show:
            print(f"    {_bold(match.value)} → {match.column}  ×{match.occurrence_count:,}")
        evidenced = [mt for mt in show if mt.evidence_rows]
        if evidenced:
            first = evidenced[0]
            row = first.evidence_rows[0]
            sample = ", ".join(f"{k}={v}" for k, v in list(row.items())[:6])
            print(f"  {_dim('Evidence (first row for ' + first.value + ' in ' + first.column + '): ' + sample)}")

    if run.values_not_found:
        total_missing = m.get("values_not_found_count", len(run.values_not_found))
        shown = ", ".join(run.values_not_found[:8])
        suffix = ", ..." if total_missing > 8 else ""
        print(f"\n  {_yellow('Not found')} ({total_missing:,} values): {shown}{suffix}")

    print()
    print("  " + "─" * 54)


# ---------------------------------------------------------------------------
# Result display
# ---------------------------------------------------------------------------


def display_result(result: DiffRun) -> None:
    """Print a human-readable summary of a DiffRunResult."""
    print()
    print("  " + "─" * 54)

    if result.error:
        print(f"  {_red('✗  ERROR')}  {result.error}")
    elif result.match:
        print(f"  {_green('✓  MATCH')}  — tables are identical")
    else:
        print(f"  {_red('✗  MISMATCH')}  — tables differ")

    if result.algorithm:
        print(f"  Algorithm      : {result.algorithm}")
    if result.execution_time_seconds is not None:
        print(f"  Elapsed        : {result.execution_time_seconds:.2f}s")
    if result.algorithm == DiffAlgorithm.BISECTION and result.metadata:
        m = result.metadata
        print(f"  Segments       : {m.get('segment_count', '?')} initial, {m.get('segments_differing', '?')} differing")
        print(f"  Depth          : {m.get('depth_reached', '?')}")

    if result.algorithm == DiffAlgorithm.SCHEMA_DIFF and result.metadata:
        m = result.metadata
        print(f"  Mode           : structure only, no data read"
              f"{' (strict: length/precision/scale compared)' if m.get('strict') else ''}")
        pk_a = m.get('primary_key_source') or []
        pk_b = m.get('primary_key_target') or []
        pk_flag = _green('✓') if m.get('primary_key_match') else _red('✗')
        print(f"  Primary keys   : {pk_flag}  source=({', '.join(pk_a) or '—'})  target=({', '.join(pk_b) or '—'})")
        rc_a = m.get('table_row_count_source')
        rc_b = m.get('table_row_count_target')
        if rc_a is not None or rc_b is not None:
            fmt = lambda v: f"{v:,}" if isinstance(v, int) else "?"
            print(f"  Table rows     : source≈{fmt(rc_a)}  target≈{fmt(rc_b)}  {_dim('(catalog estimate)')}")

    if result.algorithm == DiffAlgorithm.PROFILE_DIFF and result.metadata:
        m = result.metadata
        print(f"  Mode           : per-column aggregate profile, no row data read")
        print(f"  Numeric tol.   : ±{m.get('numeric_tolerance', '?')} (relative)")
        print(f"  Columns        : {m.get('columns_profiled', '?')} profiled")
        rc_a = m.get('table_row_count_source')
        rc_b = m.get('table_row_count_target')
        if rc_a is not None or rc_b is not None:
            fmt = lambda v: f"{v:,}" if isinstance(v, int) else "?"
            print(f"  Table rows     : source={fmt(rc_a)}  target={fmt(rc_b)}")
        print(f"  {_yellow('⚠')}  Triage signal only: equal profiles do NOT prove equal rows")

    if result.algorithm == DiffAlgorithm.SKETCH_DIFF and result.metadata:
        m = result.metadata
        print(f"  Mode           : approximate per-column cardinality/median, no row data read")
        print(f"  Relative tol.  : ±{m.get('relative_tolerance', '?')}")
        print(f"  Columns        : {m.get('columns_profiled', '?')} profiled")
        print(f"  Distinct algo  : source={m.get('distinct_algorithm_source', '?')}  "
              f"target={m.get('distinct_algorithm_target', '?')}")
        print(f"  Median algo    : source={m.get('median_algorithm_source', '?')}  "
              f"target={m.get('median_algorithm_target', '?')}")
        rc_a = m.get('table_row_count_source')
        rc_b = m.get('table_row_count_target')
        if rc_a is not None or rc_b is not None:
            fmt = lambda v: f"{v:,}" if isinstance(v, int) else "?"
            print(f"  Table rows     : source={fmt(rc_a)}  target={fmt(rc_b)}")
        print(f"  {_yellow('⚠')}  Triage signal only — estimates are approximate by design")

    if result.algorithm == DiffAlgorithm.BLOOM and result.metadata:
        m = result.metadata
        comparable = m.get('hash_comparable', False)
        print(f"  Prefilter      : FPR={m.get('bloom_fpr', '?')}, "
              f"{m.get('bloom_bits_per_side', '?'):,} bits/side, "
              f"{m.get('bloom_hash_count', '?')} hash fns")
        print(f"  Definite diffs : +{m.get('definite_added', 0):,} added, "
              f"-{m.get('definite_deleted', 0):,} deleted, "
              f"~{m.get('definite_modified', 0):,} modified")
        if not comparable:
            print(f"  {_yellow('⚠')}  Different source types — key membership only (MODIFIED not detectable)")
        print(f"  {_yellow('⚠')}  Prefilter result: differences shown are certain; a clean result")
        print(f"     may still hide up to ~{m.get('bloom_fpr', 0):.0%} of real diffs — verify with HASH_DIFF/BISECTION")

    if result.algorithm == DiffAlgorithm.EMBEDDING_SIMILARITY and result.metadata:
        m = result.metadata
        print(f"  Vector column  : {m.get('vector_column', '?')}")
        print(f"  Metric         : {m.get('distance_metric', '?')} (threshold {m.get('distance_threshold', '?')})")
        print(f"  Compared pairs : {m.get('compared_pairs', 0):,}")
        if m.get('mean_distance') is not None:
            print(f"  Distance       : mean {m.get('mean_distance')}, max {m.get('max_distance')}")
        if m.get('over_threshold'):
            print(f"  Over threshold : {m.get('over_threshold'):,}")
        if m.get('dimension_mismatches'):
            print(f"  {_yellow('Dim mismatches')} : {m.get('dimension_mismatches'):,}")
        if m.get('parse_failures'):
            print(f"  {_yellow('Parse failures')} : {m.get('parse_failures'):,}")

    if result.algorithm == DiffAlgorithm.SAMPLED and result.metadata:
        m = result.metadata
        n = m.get('sample_size', 0)
        full = m.get('source_row_count_full', 0)
        conf_pct = int(m.get('confidence_level', 0.95) * 100)
        n_str = f"{n:,}" if isinstance(n, int) else str(n)
        full_str = f"{full:,}" if isinstance(full, int) else str(full)
        print(f"  Sample size    : {n_str} of {full_str} source rows")
        print(f"  Observed diff  : {m.get('estimated_diff_pct', 0):.2f}% in sample")
        print(f"  {conf_pct}% CI          : [{m.get('ci_lower', 0):.2f}%, {m.get('ci_upper', 0):.2f}%]")
        print(f"  Margin of error: ±{m.get('margin_of_error', 0):.2f}%")
        est = m.get('estimated_total_diffs', 0)
        print(f"  Est. total diffs: ~{est:,} rows (extrapolated)")
        print(f"  {_yellow('⚠')}  ADDED rows in target are not detected (source-perspective only)")

    # Row count summary (SCHEMA_DIFF / PROFILE_DIFF / SKETCH_DIFF count columns, not rows)
    s = result.summary
    if s is not None:
        column_mode = result.algorithm in (
            DiffAlgorithm.SCHEMA_DIFF, DiffAlgorithm.PROFILE_DIFF, DiffAlgorithm.SKETCH_DIFF
        )
        unit_src = "Source columns" if column_mode else "Source rows   "
        unit_tgt = "Target columns" if column_mode else "Target rows   "
        if result.algorithm == DiffAlgorithm.PROFILE_DIFF:
            modified_hint = "(profile stats differ)"
        elif result.algorithm == DiffAlgorithm.SKETCH_DIFF:
            modified_hint = "(estimates differ)"
        elif column_mode:
            modified_hint = "(attributes differ)"
        else:
            modified_hint = "(values differ)"
        print(f"  {unit_src} : {s.source_row_count:,}")
        print(f"  {unit_tgt} : {s.target_row_count:,}")
        if not result.match:
            if s.added_count:
                print(f"  {_green('Added')}          : {s.added_count:,}  (in target, not in source)")
            if s.deleted_count:
                print(f"  {_red('Deleted')}        : {s.deleted_count:,}  (in source, not in target)")
            if s.modified_count:
                print(f"  {_yellow('Modified')}       : {s.modified_count:,}  {modified_hint}")
            print(f"  Matched        : {s.matched_count:,}")

    if result.common_columns:
        print(f"  Common columns : {len(result.common_columns)}")

    # Schema differences
    diff = result.schema_differences or {}
    only_a = diff.get("columns_only_in_a", [])
    only_b = diff.get("columns_only_in_b", [])
    type_diffs = diff.get("column_type_differences", [])
    rc_delta = diff.get("row_count_difference")

    if only_a:
        print(f"  {_yellow('Cols only in source:')} {', '.join(only_a)}")
    if only_b:
        print(f"  {_yellow('Cols only in target:')} {', '.join(only_b)}")
    if type_diffs:
        print(f"  {_yellow('Type differences:')}")
        for td in type_diffs:
            col = td.get("column", "?")
            ta = td.get("table_a", {})
            tb = td.get("table_b", {})
            print(f"    {col}: source={ta.get('type')}  target={tb.get('type')}")
    if rc_delta is not None:
        sign = "+" if rc_delta > 0 else ""
        print(f"  Row Δ (schema) : {sign}{rc_delta}")

    # Modified row detail (show up to 5 in the CLI)
    modified = result.modified_rows()
    detailed = [r for r in modified if r.source_values is not None]
    if detailed:
        show = detailed[:5]
        print(f"\n  {_yellow('Modified row details')} (showing {len(show)} of {len(modified)}):")
        for row_diff in show:
            key_str = ", ".join(f"{k}={v}" for k, v in row_diff.key_values.items())
            print(f"\n  {_bold('Key:')} {key_str}")
            print(f"  {'Column':<28} {'Source':<20} {'Target'}")
            print(f"  {'─'*28} {'─'*20} {'─'*20}")
            for col in (row_diff.mismatched_columns or []):
                src_val = str(row_diff.source_values.get(col, '')) if row_diff.source_values else ''
                tgt_val = str(row_diff.target_values.get(col, '')) if row_diff.target_values else ''
                flag = _red("←")
                print(f"  {col:<28} {src_val:<20} {tgt_val}  {flag}")

    print()
    print("  " + "─" * 54)


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------


def _connect_with_retry(source_type: str, label: str):
    """
    Build config, create connector, and connect — retrying on failure.
    Returns a connected connector, or None if the user declines to retry.
    """
    while True:
        print(f"\n  Connecting to {_cyan(source_type)} ({label})...", end=" ", flush=True)
        try:
            cfg = build_config(source_type)
            connector = ConnectorFactory.create_connector(source_type, cfg)
            connector.connect()
            method = (
                connector.connection_method_used.value
                if connector.connection_method_used
                else "unknown"
            )
            print(_green(f"✓  {source_type} (via {method})"))
            return connector
        except Exception as exc:
            print(_red(f"✗\n    {exc}"))
            if _DEV_MODE:
                traceback.print_exc()
            ans = input("    Retry? [Y/n]: ").strip().lower()
            if ans not in ("", "y", "yes"):
                return None


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------


def _save_run(
    result: DiffRun,
    src1_type: str,
    connector1,
    src2_type: str,
    connector2,
    fq1: str,
    keys1: List[str],
    fq2: str,
    keys2: List[str],
    save_original_values: bool,
) -> None:
    """Persist a completed DiffRun to the configured database."""
    from dimer.persistence.config import get_db_url
    from dimer.persistence.repository import (
        delete_old_runs,
        ensure_defaults,
        get_db,
        get_or_create_diff_job,
        get_or_create_project_source,
        save_diff_run,
    )

    db_url = get_db_url()
    try:
        with get_db(db_url) as db:
            project_id, user_id = ensure_defaults(db)

            cfg1 = connector1.connection_config
            src_a_id = get_or_create_project_source(
                db,
                project_id=project_id,
                source_type=src1_type,
                source_name=f"{src1_type}:{cfg1.host or ''}:{cfg1.database or ''}",
                host=cfg1.host,
                port=cfg1.port,
                db_name=cfg1.database,
                user_id=user_id,
            )

            cfg2 = connector2.connection_config
            src_b_id = get_or_create_project_source(
                db,
                project_id=project_id,
                source_type=src2_type,
                source_name=f"{src2_type}:{cfg2.host or ''}:{cfg2.database or ''}",
                host=cfg2.host,
                port=cfg2.port,
                db_name=cfg2.database,
                user_id=user_id,
            )

            job_id = get_or_create_diff_job(
                db, project_id, src_a_id, fq1, src_b_id, fq2, keys1,
            )

            run_id = save_diff_run(
                db, result, job_id, fq1, fq2, save_original_values,
            )

        print(f"  {_green('✓')}  Saved  run {_dim(run_id[:8] + '...')}  job {_dim(job_id[:8] + '...')}")
        print(f"  DB     : {_dim(db_url)}")

        # Retention prompt
        ans = input("  Clean up old runs for this job? [y/N]: ").strip().lower()
        if ans in ("y", "yes"):
            raw = input("  Keep how many recent runs? [10]: ").strip()
            keep = int(raw) if raw.isdigit() and int(raw) > 0 else 10
            with get_db(db_url) as db:
                deleted = delete_old_runs(db, job_id, keep)
            if deleted:
                print(f"  {_dim(f'Removed {deleted} old run(s).')}")
            else:
                print(f"  {_dim('Nothing to remove.')}")

    except Exception as exc:
        print(f"\n  {_yellow('⚠')}  Could not save results: {exc}")
        if _DEV_MODE:
            traceback.print_exc()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point for the DiMer interactive data diff CLI."""
    configure_logging(debug="-dev" in sys.argv)
    load_dotenv()

    # Header
    print()
    print(_bold(_cyan("  ╔══════════════════════════════╗")))
    print(_bold(_cyan("  ║    DiMer  —  Data Diff CLI   ║")))
    print(_bold(_cyan("  ╚══════════════════════════════╝")))
    print()
    print(_dim("  Compare tables across any two supported data sources."))
    print(_dim("  Press Ctrl+C at any time to exit.\n"))

    connector1 = None
    connector2 = None

    try:
        # ── Step 1: Select data sources ───────────────────────────────────────
        print(_bold("── Step 1: Select data sources ───────────────────────────"))
        src1 = select_source("Target 1 source")
        src2 = select_source("Target 2 source")

        # ── Step 2: Verify .env configuration ────────────────────────────────
        print(_bold("\n── Step 2: Verify .env configuration ─────────────────────"))
        verify_config_loop(src1, "Target 1")
        if src2 != src1:
            verify_config_loop(src2, "Target 2")
        else:
            print(
                f"\n  {_dim('Both targets use the same source type')} ({_cyan(src1)})"
                f"{_dim(' — configuration already verified.')}"
            )

        # ── Step 3: Establish connections ─────────────────────────────────────
        print(_bold("\n── Step 3: Establish connections ──────────────────────────"))
        connector1 = _connect_with_retry(src1, "Target 1")
        if connector1 is None:
            print(_dim("  Exiting."))
            return

        connector2 = _connect_with_retry(src2, "Target 2")
        if connector2 is None:
            print(_dim("  Exiting."))
            return

        # ── Step 4: Comparison loop ───────────────────────────────────────────
        while True:
            print(_bold("\n── Step 4: Asset comparison ───────────────────────────────"))

            if prompt_task() == "search":
                run_value_search(connector1, src1, connector2, src2)
                ans = input("\n  Run another comparison or search? [Y/n]: ").strip().lower()
                if ans not in ("", "y", "yes"):
                    break
                continue

            fq1 = prompt_fq_table(src1, "Target 1")
            fq2 = prompt_fq_table(src2, "Target 2")

            use_bisection = False
            use_sampling = False
            use_bloom = False
            use_embedding = False
            bloom_fpr = BLOOM_DEFAULT_FPR

            # Schema-only and aggregate-only diffs need no join keys — offered before key detection
            use_schema_diff, schema_strict = prompt_schema_diff()
            use_profile_diff, use_sketch_diff, aggregate_tolerance = (
                False, False, PROFILE_DEFAULT_NUMERIC_TOLERANCE
            )
            if not use_schema_diff:
                use_profile_diff, use_sketch_diff, aggregate_tolerance = prompt_aggregate_diff()

            if use_schema_diff or use_profile_diff or use_sketch_diff:
                keys1: List[str] = []
                keys2: List[str] = []
            else:
                keys1, meta1 = detect_or_prompt_keys(connector1, fq1, "Target 1")
                keys2, meta2 = detect_or_prompt_keys(connector2, fq2, "Target 2")

                # Embedding similarity applies to vector columns regardless of topology
                use_embedding, vector_column, embedding_metric, embedding_threshold = prompt_embedding()

            same_instance = connector1.connection_config.host == connector2.connection_config.host and connector1.connection_config.database == connector2.connection_config.database

            if not use_schema_diff and not use_profile_diff and not use_sketch_diff and not use_embedding and not same_instance:

                # Bloom prefilter: cheap signal before committing to a full diff
                use_bloom, bloom_fpr = prompt_bloom()

                if not use_bloom:
                    use_bisection, bisection_key, bisection_threshold = prompt_bisection(fq1, fq2, keys1, meta1.row_count, meta2.row_count)

                sample_size_val = SAMPLED_DEFAULT_SIZE
                sample_confidence = SAMPLED_DEFAULT_CONFIDENCE
                if not use_bloom and not use_bisection:
                    # Sampling is only meaningful for cross-database comparisons
                    use_sampling, sample_size_val, sample_confidence = prompt_sampling()

            # Confirmation summary
            print()
            print("  " + "─" * 54)
            print(f"  Source  : {_cyan(src1):<20} {_bold(fq1)}")
            print(f"  Target  : {_cyan(src2):<20} {_bold(fq2)}")
            if use_schema_diff or use_profile_diff or use_sketch_diff:
                print(f"  Keys    : {_dim('— (not needed for this algorithm)')}")
            else:
                print(f"  Keys    : {', '.join(keys1)}  ←→  {', '.join(keys2)}")
            if use_schema_diff:
                print(f"  Algorithm: SCHEMA_DIFF  (structure only{', strict' if schema_strict else ''}; no data read)")
            elif use_profile_diff:
                print(f"  Algorithm: PROFILE_DIFF  (numeric tolerance={aggregate_tolerance}; no row data read)")
            elif use_sketch_diff:
                print(f"  Algorithm: SKETCH_DIFF  (relative tolerance={aggregate_tolerance}; no row data read)")
            elif use_embedding:
                print(f"  Algorithm: EMBEDDING_SIMILARITY  (column={vector_column}, metric={embedding_metric}, threshold={embedding_threshold})")
            elif use_bloom:
                print(f"  Algorithm: BLOOM prefilter  (fpr={bloom_fpr})")
            elif use_bisection:
                bkey_display = bisection_key or keys1[0]
                print(f"  Algorithm: BISECTION  (key={bkey_display}, threshold={bisection_threshold})")
            elif use_sampling:
                print(f"  Algorithm: SAMPLED  (n={sample_size_val:,}, confidence={sample_confidence})")
            print("  " + "─" * 54)

            ans = input("\n  Run diff? [Y/n]: ").strip().lower()
            if ans not in ("", "y", "yes"):
                print(_dim("  Skipped."))
            else:
                print("\n  Running comparison...", flush=True)
                try:
                    db1: ComparisonConfig = {"fq_table_name": fq1, "keys": keys1}
                    db2: ComparisonConfig = {"fq_table_name": fq2, "keys": keys2}
                    if use_schema_diff:
                        db1["use_schema_diff"] = True
                        db1["schema_strict"] = schema_strict
                    elif use_profile_diff:
                        db1["use_profile_diff"] = True
                        db1["profile_numeric_tolerance"] = aggregate_tolerance
                    elif use_sketch_diff:
                        db1["use_sketch_diff"] = True
                        db1["sketch_relative_tolerance"] = aggregate_tolerance
                    elif use_embedding:
                        db1["use_embedding"] = True
                        db1["vector_column"] = vector_column
                        db1["distance_metric"] = embedding_metric
                        db1["distance_threshold"] = embedding_threshold
                    elif use_bloom:
                        db1["use_bloom"] = True
                        db1["bloom_fpr"] = bloom_fpr
                    elif use_bisection:
                        db1["use_bisection"] = True
                        db1["bisection_threshold"] = bisection_threshold
                        if bisection_key:
                            db1["bisection_key"] = bisection_key
                    elif use_sampling:
                        db1["use_sampling"] = True
                        db1["sample_size"] = sample_size_val
                        db1["confidence"] = sample_confidence
                    result: DiffRun = Diffcheck(connector1, connector2, db1, db2).compare()
                    display_result(result)

                    ans = input("  Save results? [Y/n]: ").strip().lower()
                    if ans in ("", "y", "yes"):
                        sov = input("  Save original values for modified rows? [y/N]: ").strip().lower()
                        _save_run(
                            result,
                            src1, connector1,
                            src2, connector2,
                            fq1, keys1, fq2, keys2,
                            save_original_values=sov in ("y", "yes"),
                        )
                except Exception as exc:
                    print(f"\n  {_red('✗  Comparison failed:')} {exc}")
                    if _DEV_MODE:
                        traceback.print_exc()

            # Continue?
            ans = input("\n  Compare another table? [Y/n]: ").strip().lower()
            if ans not in ("", "y", "yes"):
                break

    except KeyboardInterrupt:
        print(f"\n\n  {_dim('Goodbye!')}")
    finally:
        for label, conn in (("Target 1", connector1), ("Target 2", connector2)):
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
