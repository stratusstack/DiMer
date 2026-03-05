"""Database URL configuration for the DiMer persistence layer."""

import os
from pathlib import Path

# Default SQLite database in the user's home directory
DEFAULT_DB_PATH = Path.home() / ".dimer" / "dimer.db"
DEFAULT_DB_URL = f"sqlite:///{DEFAULT_DB_PATH}"


def get_db_url() -> str:
    """Return the configured database URL, falling back to the default SQLite path."""
    return os.getenv("DIMER_DB_URL", DEFAULT_DB_URL)


def is_sqlite_url(url: str) -> bool:
    return url.startswith("sqlite:///")


def get_sqlite_path(url: str) -> Path:
    """Extract the filesystem path from a sqlite:/// URL."""
    return Path(url[len("sqlite:///"):])
