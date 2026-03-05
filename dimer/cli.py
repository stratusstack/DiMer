"""DiMer command-line data diff utility."""

import logging
import os
import sys
import traceback
from typing import Dict, List, Optional, Tuple

import structlog
from dotenv import load_dotenv

from dimer.core.compare import Diffcheck
from dimer.core.factory import ConnectorFactory
from dimer.core.models import ComparisonConfig, ComparisonResult, ConnectionConfig

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


def detect_or_prompt_keys(connector, fq_table: str, label: str) -> List[str]:
    """
    Try to detect primary key columns from table metadata.
    If found, ask the user to confirm or override.
    If not found (or metadata unavailable), prompt for manual entry.
    """
    schema, table = _parse_fq_table(fq_table)
    detected: List[str] = []

    print(f"\n  Detecting join keys for {_bold(label)} ({_cyan(fq_table)})...")
    try:
        metadata = connector.get_table_metadata(table, schema_name=schema)
        detected = [col.name for col in metadata.columns if col.is_primary_key]
    except Exception as e:
        print(f"    {_yellow('⚠')}  Could not read metadata: {e}")

    if detected:
        print(f"    {_green('✓')}  Primary keys detected: {_bold(', '.join(detected))}")
        ans = input("    Use these as join keys? [Y/n]: ").strip().lower()
        if ans in ("", "y", "yes"):
            return detected

    # Manual entry
    print(f"    {_dim('Enter the column(s) to use as join keys (comma-separated).')}")
    while True:
        raw = input("    > ").strip()
        keys = [k.strip() for k in raw.split(",") if k.strip()]
        if keys:
            return keys
        print(f"    {_red('At least one key column is required.')}")


# ---------------------------------------------------------------------------
# Result display
# ---------------------------------------------------------------------------


def display_result(result: ComparisonResult) -> None:
    """Print a human-readable summary of a ComparisonResult."""
    print()
    print("  " + "─" * 54)

    if result.error:
        print(f"  {_red('✗  ERROR')}  {result.error}")
    elif result.match:
        print(f"  {_green('✓  MATCH')}  — tables are identical")
    else:
        print(f"  {_red('✗  MISMATCH')}  — tables differ")

    if result.algorithm:
        print(f"  Algorithm   : {result.algorithm}")

    if result.row_count is not None and result.row_count > 0:
        if result.match:
            print(f"  Rows checked: {result.row_count}")
        else:
            print(f"  Differing   : {result.row_count} rows")

    if result.common_columns:
        print(f"  Common cols : {len(result.common_columns)}")

    diff = result.schema_differences or {}
    only_a = diff.get("columns_only_in_a", [])
    only_b = diff.get("columns_only_in_b", [])
    type_diffs = diff.get("column_type_differences", [])
    rc_delta = diff.get("row_count_difference")

    if only_a:
        print(f"  {_yellow('Cols only in Source:')} {', '.join(only_a)}")
    if only_b:
        print(f"  {_yellow('Cols only in Target:')} {', '.join(only_b)}")
    if type_diffs:
        print(f"  {_yellow('Type differences:')}")
        for td in type_diffs:
            col = td.get("column", "?")
            ta = td.get("table_a", {})
            tb = td.get("table_b", {})
            print(f"    {col}: source={ta.get('type')}  target={tb.get('type')}")
    if rc_delta is not None:
        sign = "+" if rc_delta > 0 else ""
        print(f"  Row Δ       : {sign}{rc_delta}")

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

            keys1 = detect_or_prompt_keys(connector1, fq1, "Target 1")
            keys2 = detect_or_prompt_keys(connector2, fq2, "Target 2")

            # Confirmation summary
            print()
            print("  " + "─" * 54)
            print(f"  Source  : {_cyan(src1):<20} {_bold(fq1)}")
            print(f"  Target  : {_cyan(src2):<20} {_bold(fq2)}")
            print(f"  Keys    : {', '.join(keys1)}  ←→  {', '.join(keys2)}")
            print("  " + "─" * 54)

            ans = input("\n  Run diff? [Y/n]: ").strip().lower()
            if ans not in ("", "y", "yes"):
                print(_dim("  Skipped."))
            else:
                print("\n  Running comparison...", flush=True)
                try:
                    db1: ComparisonConfig = {"fq_table_name": fq1, "keys": keys1}
                    db2: ComparisonConfig = {"fq_table_name": fq2, "keys": keys2}
                    result = Diffcheck(connector1, connector2, db1, db2).compare()
                    display_result(result)
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
