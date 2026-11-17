"""SQLite persistence layer.

The database is the single source of truth: every state transition is committed
before any external side effect (notification delivery) is attempted, and every
side effect carries an idempotency key (the notification number) so that a
restart resumes exactly where it stopped.

All transactions are short and never contain network I/O, so a slow gateway
call cannot hold the database lock or delay an unrelated confirmation.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS alarms (
    alarm_no                  TEXT PRIMARY KEY,
    content_hash              TEXT NOT NULL,
    payload_json              TEXT NOT NULL,
    primary_contact           TEXT NOT NULL,
    backup_contact            TEXT NOT NULL,
    ack_deadline_seconds      INTEGER NOT NULL,
    created_at                TEXT NOT NULL,
    created_real              REAL NOT NULL,
    -- Set only once the gateway has confirmed that the primary notification
    -- was accepted. No confirmed acceptance -> no timer -> no escalation.
    primary_accepted_at       TEXT,
    primary_accepted_real     REAL,
    primary_ack_deadline_at   TEXT,
    primary_ack_deadline_real REAL,
    confirmed_at              TEXT,
    confirmed_real            REAL,
    confirmed_by              TEXT,
    escalated_at              TEXT,
    escalated_real            REAL
);

CREATE TABLE IF NOT EXISTS notifications (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    alarm_no           TEXT NOT NULL REFERENCES alarms(alarm_no),
    notification_no    TEXT NOT NULL UNIQUE,
    role               TEXT NOT NULL CHECK (role IN ('primary', 'backup')),
    contact_json       TEXT NOT NULL,
    content_hash       TEXT NOT NULL,
    -- Everything needed to (re)post this notification after a restart.
    payload_json       TEXT NOT NULL,
    created_at         TEXT NOT NULL,
    created_real       REAL NOT NULL,
    -- pending   : persisted, not yet handed to the gateway
    -- in_flight : POST is on the wire, outcome not yet known
    -- unknown   : timeout / transport error / ambiguous reply; needs lookup
    -- accepted  : gateway confirmed acceptance
    -- failed    : gateway definitively rejected (no blind retry)
    -- cancelled : confirmation arrived before this backup was dispatched
    state              TEXT NOT NULL CHECK (
                           state IN ('pending','in_flight','unknown','accepted','failed','cancelled')
                       ),
    gateway_status     TEXT,
    detail             TEXT,
    attempts           INTEGER NOT NULL DEFAULT 0,
    -- Consecutive gateway lookups that answered "no such notification".
    -- Distinguishes "the POST never arrived" (safe to re-post) from "the POST
    -- is still being processed by a slow gateway" (must keep waiting).
    lookup_misses      INTEGER NOT NULL DEFAULT 0,
    created_by         TEXT NOT NULL,  -- initial | escalation | recovery | retry
    history_json       TEXT NOT NULL DEFAULT '[]',
    accepted_at        TEXT,
    accepted_real      REAL,
    last_attempt_at    TEXT,
    lease_until        REAL,
    next_attempt_at    REAL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_notifications_alarm_role
    ON notifications(alarm_no, role);

CREATE INDEX IF NOT EXISTS ix_notifications_due
    ON notifications(state, next_attempt_at);

CREATE INDEX IF NOT EXISTS ix_alarms_deadline
    ON alarms(primary_ack_deadline_real);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    alarm_no    TEXT NOT NULL,
    at          TEXT NOT NULL,
    kind        TEXT NOT NULL,
    detail      TEXT
);
"""


class Database:
    """Synchronous SQLite wrapper with one serialised writer."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            if path != ":memory:":
                self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(SCHEMA)
            self._migrate()

    def _migrate(self) -> None:
        """Additive, idempotent migrations for databases created by older builds."""
        columns = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(notifications)").fetchall()
        }
        if "lookup_misses" not in columns:
            self._conn.execute(
                "ALTER TABLE notifications ADD COLUMN lookup_misses INTEGER NOT NULL DEFAULT 0"
            )

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(sql, params).fetchall())

    def query_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: tuple = ()) -> None:
        with self._lock:
            self._conn.execute(sql, params)

    def close(self) -> None:
        with self._lock:
            self._conn.close()


def utc_now() -> str:
    now = datetime.now(UTC)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def iso_from_real(value: float) -> str:
    dt = datetime.fromtimestamp(value, tz=UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def parse_ts(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC).timestamp()
    except ValueError:
        try:
            # Python 3.11 accepts a trailing "Z"; other ISO-8601 variants are
            # accepted too so gateway timestamps need no preprocessing.
            return datetime.fromisoformat(value).timestamp()
        except ValueError:
            return None


def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
