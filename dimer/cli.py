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
    SAMPLED_DEFAULT_SIZE,
    SAMPLED_DEFAULT_CONFIDENCE,
)
from dimer.core.factory import ConnectorFactory
from dimer.core.models import ComparisonConfig, DiffAlgorithm, DiffRun, ConnectionConfig, TableMetadata

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

    # Row count summary
    s = result.summary
    if s is not None:
        print(f"  Source rows    : {s.source_row_count:,}")
        print(f"  Target rows    : {s.target_row_count:,}")
        if not result.match:
            if s.added_count:
                print(f"  {_green('Added')}          : {s.added_count:,}  (in target, not in source)")
            if s.deleted_count:
                print(f"  {_red('Deleted')}        : {s.deleted_count:,}  (in source, not in target)")
            if s.modified_count:
                print(f"  {_yellow('Modified')}       : {s.modified_count:,}  (values differ)")
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

            fq1 = prompt_fq_table(src1, "Target 1")
            fq2 = prompt_fq_table(src2, "Target 2")

            keys1, meta1 = detect_or_prompt_keys(connector1, fq1, "Target 1")
            keys2, meta2 = detect_or_prompt_keys(connector2, fq2, "Target 2")

            same_instance = connector1.connection_config.host == connector2.connection_config.host and connector1.connection_config.database == connector2.connection_config.database

            use_bisection = False
            use_sampling = False
            use_bloom = False
            bloom_fpr = BLOOM_DEFAULT_FPR

            # Embedding similarity applies to vector columns regardless of topology
            use_embedding, vector_column, embedding_metric, embedding_threshold = prompt_embedding()

            if not use_embedding and not same_instance:

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
            print(f"  Keys    : {', '.join(keys1)}  ←→  {', '.join(keys2)}")
            if use_embedding:
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
                    if use_embedding:
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
