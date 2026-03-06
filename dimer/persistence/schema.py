"""Schema setup for the DiMer persistence layer.

Supports SQLite (local/dev) and PostgreSQL (production).
All CREATE TABLE statements use IF NOT EXISTS so setup() is safe to call
on every application start.
"""

import sqlite3
from pathlib import Path

from dimer.persistence.config import get_sqlite_path, is_sqlite_url

# ---------------------------------------------------------------------------
# DDL — read from the companion .sql files
# ---------------------------------------------------------------------------

_SQL_DIR = Path(__file__).parent / "sql"


def _read_sql(filename: str) -> str:
    return (_SQL_DIR / filename).read_text(encoding="utf-8")


def get_sqlite_ddl() -> str:
    """Return the SQLite CREATE TABLE statements."""
    return _read_sql("sqlite_schema.sql")


def get_postgres_ddl() -> str:
    """Return the PostgreSQL CREATE TABLE statements."""
    return _read_sql("postgres_schema.sql")


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def setup(db_url: str) -> None:
    """Create all tables in the target database (idempotent).

    Args:
        db_url: A ``sqlite:///path`` or ``postgresql://...`` URL.
    """
    if is_sqlite_url(db_url):
        _setup_sqlite(db_url)
    else:
        _setup_postgres(db_url)


def _setup_sqlite(db_url: str) -> None:
    path = get_sqlite_path(db_url)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(get_sqlite_ddl())
        conn.commit()
        # Forward-compatible migration: add metadata column to diff_run if absent
        try:
            conn.execute("ALTER TABLE diff_run ADD COLUMN metadata TEXT")
            conn.commit()
        except Exception:
            pass  # column already exists
    finally:
        conn.close()


def _setup_postgres(db_url: str) -> None:
    try:
        import psycopg2
    except ImportError as exc:
        raise ImportError(
            "psycopg2 is required for PostgreSQL support. "
            "Install it with: pip install dimer[postgresql]"
        ) from exc

    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(get_postgres_ddl())
    finally:
        conn.close()
