"""Repository layer for the DiMer persistence layer.

All public functions accept a ``DimerDB`` instance (obtained from the
``get_db()`` context manager) and operate within its transaction.

Usage::

    from dimer.persistence.repository import get_db, ensure_defaults, save_diff_run

    with get_db() as db:
        project_id, user_id = ensure_defaults(db)
        run_id = save_diff_run(db, result, job_id, ...)
"""

import hashlib
import json
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

from dimer.core.compare import MAX_DETAIL_ROWS
from dimer.core.models import DiffRun, DiffRow, RowStatus
from dimer.persistence.config import get_db_url, get_sqlite_path, is_sqlite_url
from dimer.persistence.schema import setup

# ---------------------------------------------------------------------------
# Well-known IDs for the default CLI project / user
# ---------------------------------------------------------------------------

_DEFAULT_PROJECT_ID = "00000000-0000-0000-0000-000000000001"
_DEFAULT_PROJECT_NAME = "default"

_DEFAULT_USER_ID = "00000000-0000-0000-0000-000000000002"
_DEFAULT_USER_NAME = "cli"


# ---------------------------------------------------------------------------
# DimerDB — thin dialect-aware wrapper
# ---------------------------------------------------------------------------


@dataclass
class DimerDB:
    """Wraps a raw DB connection and normalises SQLite / PostgreSQL differences."""

    _conn: Any
    _is_sqlite: bool

    def execute(self, sql: str, params: tuple = ()) -> Any:
        if self._is_sqlite:
            return self._conn.execute(sql, params)
        cur = self._conn.cursor()
        cur.execute(sql.replace("?", "%s"), params)
        return cur

    def executemany(self, sql: str, params_list: List[tuple]) -> None:
        if self._is_sqlite:
            self._conn.executemany(sql, params_list)
        else:
            cur = self._conn.cursor()
            cur.executemany(sql.replace("?", "%s"), params_list)

    def fetchone(self, sql: str, params: tuple = ()) -> Optional[Any]:
        return self.execute(sql, params).fetchone()

    def fetchall(self, sql: str, params: tuple = ()) -> List[Any]:
        return self.execute(sql, params).fetchall()

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------


@contextmanager
def get_db(db_url: Optional[str] = None) -> Generator[DimerDB, None, None]:
    """Context manager that yields a connected ``DimerDB``, auto-creating the schema.

    Commits on clean exit; rolls back on exception.
    """
    url = db_url or get_db_url()
    setup(url)  # idempotent — CREATE TABLE IF NOT EXISTS

    is_sqlite = is_sqlite_url(url)
    if is_sqlite:
        path = get_sqlite_path(url)
        raw = sqlite3.connect(str(path))
        raw.execute("PRAGMA foreign_keys = ON")
        raw.row_factory = sqlite3.Row
    else:
        try:
            import psycopg2
        except ImportError as exc:
            raise ImportError(
                "psycopg2 is required for PostgreSQL support. "
                "Install it with: pip install dimer[postgresql]"
            ) from exc
        raw = psycopg2.connect(url)

    db = DimerDB(_conn=raw, _is_sqlite=is_sqlite)
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------


def _json(value: Any) -> Optional[str]:
    """Serialise a Python value to a JSON string for storage, or return None."""
    if value is None:
        return None
    return json.dumps(value, default=str)


def _key_hash(key_values: Dict[str, Any]) -> str:
    """Stable MD5 hex digest of a row's key values (sorted keys)."""
    serialised = json.dumps(key_values, sort_keys=True, default=str)
    return hashlib.md5(serialised.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Default project / user
# ---------------------------------------------------------------------------


def ensure_defaults(db: DimerDB) -> Tuple[str, str]:
    """Ensure the default CLI project and user exist.

    Returns:
        (project_id, user_id) of the default records.
    """
    if not db.fetchone(
        "SELECT project_id FROM project WHERE project_id = ?", (_DEFAULT_PROJECT_ID,)
    ):
        db.execute(
            "INSERT INTO project (project_id, name, description) VALUES (?, ?, ?)",
            (_DEFAULT_PROJECT_ID, _DEFAULT_PROJECT_NAME, "Auto-created by DiMer CLI"),
        )

    if not db.fetchone(
        'SELECT user_id FROM "user" WHERE user_id = ?', (_DEFAULT_USER_ID,)
    ):
        db.execute(
            'INSERT INTO "user" (user_id, name, email, local_cli) VALUES (?, ?, ?, ?)',
            (_DEFAULT_USER_ID, _DEFAULT_USER_NAME, None, 1),
        )

    return _DEFAULT_PROJECT_ID, _DEFAULT_USER_ID


# ---------------------------------------------------------------------------
# Project source
# ---------------------------------------------------------------------------


def get_or_create_project_source(
    db: DimerDB,
    project_id: str,
    source_type: str,
    source_name: str,
    host: Optional[str],
    port: Optional[int],
    db_name: Optional[str],
    user_id: str,
) -> str:
    """Return the ``source_id`` for a matching source, creating one if absent."""
    row = db.fetchone(
        "SELECT source_id FROM project_source "
        "WHERE project_id = ? AND source_type = ? AND source_name = ?",
        (project_id, source_type, source_name),
    )
    if row:
        return row[0]

    source_id = str(uuid.uuid4())
    db.execute(
        "INSERT INTO project_source "
        "(source_id, project_id, source_type, source_name, host, port, db_name, user_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (source_id, project_id, source_type, source_name, host, port, db_name, user_id),
    )
    return source_id


# ---------------------------------------------------------------------------
# Diff job
# ---------------------------------------------------------------------------


def get_or_create_diff_job(
    db: DimerDB,
    project_id: str,
    source_a_id: str,
    source_a_asset: str,
    source_b_id: str,
    source_b_asset: str,
    key_columns: List[str],
) -> str:
    """Return the ``job_id`` for a matching diff job, creating one if absent.

    Two jobs are considered the same when the source IDs, asset names, and
    sorted key columns are identical.
    """
    key_cols_json = json.dumps(sorted(key_columns))
    row = db.fetchone(
        "SELECT job_id FROM diff_job "
        "WHERE source_a_id = ? AND source_a_asset = ? "
        "AND source_b_id = ? AND source_b_asset = ? "
        "AND key_columns = ?",
        (source_a_id, source_a_asset, source_b_id, source_b_asset, key_cols_json),
    )
    if row:
        return row[0]

    job_id = str(uuid.uuid4())
    db.execute(
        "INSERT INTO diff_job "
        "(job_id, project_id, source_a_id, source_a_asset, "
        "source_b_id, source_b_asset, key_columns) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (job_id, project_id, source_a_id, source_a_asset,
         source_b_id, source_b_asset, key_cols_json),
    )
    return job_id


# ---------------------------------------------------------------------------
# Save a diff run
# ---------------------------------------------------------------------------


def save_diff_run(
    db: DimerDB,
    result: DiffRun,
    job_id: str,
    source_a_asset: str,
    source_b_asset: str,
    save_original_values: bool,
) -> str:
    """Persist a ``DiffRun`` across diff_run, diff_run_detail, diff_result, and diff_row.

    Row-level diffs are capped at ``MAX_DETAIL_ROWS``.  When
    ``save_original_values`` is False the ``source_values`` / ``target_values``
    columns in diff_row are stored as NULL even if the in-memory DiffRow has them.

    Returns:
        The newly created ``run_id`` (UUID string).
    """
    run_id = str(uuid.uuid4())
    now = datetime.now(tz=timezone.utc).isoformat()
    s = result.summary

    # 1. diff_run
    db.execute(
        "INSERT INTO diff_run "
        "(run_id, job_id, run_at, status, algorithm, execution_time_seconds, match, error) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            run_id,
            job_id,
            now,
            "failed" if result.error else "success",
            result.algorithm,
            result.execution_time_seconds,
            1 if result.match else 0,
            result.error,
        ),
    )

    # 2. diff_run_detail
    cols_not_matched = _extract_columns_not_matched(result.schema_differences)
    db.execute(
        "INSERT INTO diff_run_detail "
        "(run_id, source_a_asset, source_a_row_count, source_b_asset, source_b_row_count, "
        "common_columns, schema_differences, columns_not_matched) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            run_id,
            source_a_asset,
            s.source_row_count if s else None,
            source_b_asset,
            s.target_row_count if s else None,
            _json(result.common_columns),
            _json(result.schema_differences),
            _json(cols_not_matched),
        ),
    )

    # 3. diff_result
    if s:
        db.execute(
            "INSERT INTO diff_result "
            "(run_id, job_id, added_count, deleted_count, modified_count, matched_count, diffed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (run_id, job_id, s.added_count, s.deleted_count,
             s.modified_count, s.matched_count, now),
        )

    # 4. diff_row  (capped at MAX_DETAIL_ROWS)
    rows_to_save = result.row_diffs[:MAX_DETAIL_ROWS]
    if rows_to_save:
        batch = []
        for dr in rows_to_save:
            src_vals = _json(dr.source_values) if save_original_values else None
            tgt_vals = _json(dr.target_values) if save_original_values else None
            batch.append((
                run_id,
                _key_hash(dr.key_values),
                _json(dr.key_values),
                dr.status.value,
                _json(dr.mismatched_columns) if dr.mismatched_columns else None,
                src_vals,
                tgt_vals,
            ))
        db.executemany(
            "INSERT INTO diff_row "
            "(run_id, key_hash, key_values, status, mismatched_columns, "
            "source_values, target_values) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            batch,
        )

    return run_id


def _extract_columns_not_matched(schema_diff: Optional[Dict]) -> Optional[Dict]:
    if not schema_diff:
        return None
    result: Dict[str, Any] = {}
    if schema_diff.get("columns_only_in_a"):
        result["source_only"] = schema_diff["columns_only_in_a"]
    if schema_diff.get("columns_only_in_b"):
        result["target_only"] = schema_diff["columns_only_in_b"]
    return result or None


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def list_runs(db: DimerDB, job_id: str) -> List[Dict[str, Any]]:
    """Return all runs for a job ordered newest-first."""
    rows = db.fetchall(
        "SELECT run_id, run_at, status, algorithm, execution_time_seconds, match, error "
        "FROM diff_run WHERE job_id = ? ORDER BY run_at DESC",
        (job_id,),
    )
    return [dict(r) for r in rows]


def delete_old_runs(db: DimerDB, job_id: str, keep_count: int) -> int:
    """Delete runs beyond ``keep_count`` for a job (oldest first).

    Child rows in diff_run_detail, diff_result, and diff_row are removed first
    to satisfy foreign-key constraints.

    Returns:
        Number of runs deleted.
    """
    all_runs = db.fetchall(
        "SELECT run_id FROM diff_run WHERE job_id = ? ORDER BY run_at DESC",
        (job_id,),
    )
    if len(all_runs) <= keep_count:
        return 0

    to_delete = [row[0] for row in all_runs[keep_count:]]
    for run_id in to_delete:
        db.execute("DELETE FROM diff_row WHERE run_id = ?", (run_id,))
        db.execute("DELETE FROM diff_result WHERE run_id = ?", (run_id,))
        db.execute("DELETE FROM diff_run_detail WHERE run_id = ?", (run_id,))
        db.execute("DELETE FROM diff_run WHERE run_id = ?", (run_id,))

    return len(to_delete)
