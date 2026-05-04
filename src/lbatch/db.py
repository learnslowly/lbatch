from __future__ import annotations

import json
import random
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from .config import Paths


# SQLite "database is locked" recovery. Under heavy concurrency (daemon
# dispatching while user runs `lbatch status`/`cancel`/`submit`), the WAL
# busy_timeout can be exhausted before BEGIN IMMEDIATE acquires the write
# lock. Without retry the daemon used to crash and stop dispatching;
# every transaction now retries with exponential backoff before re-raising.
TRANSACTION_RETRIES = 8
TRANSACTION_BACKOFF_BASE = 0.05  # seconds; doubles each attempt
TRANSACTION_BACKOFF_MAX = 2.0


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def public_group_id(group_id: str) -> str:
    return group_id if group_id.startswith("lb:") else f"lb:{group_id}"


def internal_group_id(group_id: str) -> str:
    return group_id[3:] if group_id.startswith("lb:") else group_id


class Database:
    def __init__(self, paths: Paths | None = None):
        self.paths = paths or Paths.defaults()
        self.paths.ensure()
        self.conn = sqlite3.connect(self.paths.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode = WAL")
        # Bumped from 5s to 30s. Concurrent readers + heavy daemon writes
        # under panel-scale submissions need more headroom than the
        # original default; with WAL mode this just means the daemon
        # waits on a busy DB instead of immediately raising.
        self.conn.execute("PRAGMA busy_timeout = 30000")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.migrate()

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        attempt = 0
        backoff = TRANSACTION_BACKOFF_BASE
        while True:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                break
            except sqlite3.OperationalError as exc:
                msg = str(exc).lower()
                if "locked" not in msg and "busy" not in msg:
                    raise
                attempt += 1
                if attempt > TRANSACTION_RETRIES:
                    raise
                # Sleep with jitter, then retry. The connection itself is
                # not in a transaction state on this failure (BEGIN was
                # rejected before any work), so it is safe to retry.
                time.sleep(min(backoff, TRANSACTION_BACKOFF_MAX) *
                           (1.0 + random.random() * 0.25))
                backoff *= 2
        try:
            yield self.conn
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def migrate(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS groups (
                group_id TEXT PRIMARY KEY,
                label TEXT,
                original_argv_json TEXT NOT NULL,
                normalized_sbatch_options_json TEXT NOT NULL,
                external_dependency_json TEXT DEFAULT '[]',
                script_path TEXT NOT NULL,
                script_args_json TEXT DEFAULT '[]',
                workdir TEXT NOT NULL,
                array_spec TEXT,
                array_count INTEGER NOT NULL DEFAULT 1,
                array_min INTEGER,
                array_max INTEGER,
                array_step INTEGER,
                array_concurrency_limit INTEGER,
                priority INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS units (
                unit_id TEXT PRIMARY KEY,
                group_id TEXT NOT NULL REFERENCES groups(group_id) ON DELETE CASCADE,
                array_task_id INTEGER,
                array_order INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL,
                effective_sbatch_options_json TEXT NOT NULL,
                wrapper_path TEXT,
                slurm_job_id TEXT,
                submit_attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                exit_code INTEGER,
                release_event_path TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                submitted_at TEXT,
                released_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_units_state ON units(state);
            CREATE INDEX IF NOT EXISTS idx_units_group_state ON units(group_id, state);
            CREATE INDEX IF NOT EXISTS idx_units_slurm_job_id ON units(slurm_job_id);
            CREATE TABLE IF NOT EXISTS dependencies (
                dependency_id INTEGER PRIMARY KEY AUTOINCREMENT,
                dependent_group_id TEXT NOT NULL REFERENCES groups(group_id) ON DELETE CASCADE,
                upstream_group_id TEXT NOT NULL REFERENCES groups(group_id) ON DELETE CASCADE,
                dependency_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(dependent_group_id, upstream_group_id, dependency_type)
            );
            CREATE INDEX IF NOT EXISTS idx_dependencies_dependent ON dependencies(dependent_group_id);
            CREATE INDEX IF NOT EXISTS idx_dependencies_upstream ON dependencies(upstream_group_id);
            CREATE TABLE IF NOT EXISTS ingested_events (
                event_path TEXT PRIMARY KEY,
                unit_id TEXT NOT NULL REFERENCES units(unit_id),
                event_type TEXT NOT NULL,
                sha256 TEXT,
                ingested_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                level TEXT NOT NULL,
                action TEXT NOT NULL,
                group_id TEXT,
                unit_id TEXT,
                message TEXT NOT NULL,
                details_json TEXT DEFAULT '{}'
            );
            """
        )
        self.conn.commit()

    def next_group_id(self) -> str:
        row = self.conn.execute("SELECT group_id FROM groups ORDER BY rowid DESC LIMIT 1").fetchone()
        if not row:
            return "g000001"
        try:
            n = int(str(row["group_id"])[1:]) + 1
        except ValueError:
            n = self.conn.execute("SELECT COUNT(*) AS c FROM groups").fetchone()["c"] + 1
        return f"g{n:06d}"

    def next_unit_id(self, group_id: str, order: int) -> str:
        return f"u{group_id[1:]}_{order:06d}"

    def audit(self, level: str, action: str, message: str, group_id: str | None = None, unit_id: str | None = None, details: dict | None = None) -> None:
        self.conn.execute(
            "INSERT INTO audit_log(timestamp, level, action, group_id, unit_id, message, details_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (utcnow(), level, action, group_id, unit_id, message, json.dumps(details or {})),
        )
        self.conn.commit()

    def count_units_by_state(self) -> dict[str, int]:
        rows = self.conn.execute("SELECT state, COUNT(*) AS c FROM units GROUP BY state").fetchall()
        return {row["state"]: row["c"] for row in rows}

    def setting(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None

    def set_setting(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO settings(key, value, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, value, utcnow()),
        )
        self.conn.commit()
