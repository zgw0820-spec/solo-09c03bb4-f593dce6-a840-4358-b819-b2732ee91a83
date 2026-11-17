"""Runtime configuration for the alarm API."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _str(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class Settings:
    database_path: str = field(default_factory=lambda: _str("DATABASE_PATH", "./data/alarms.db"))
    gateway_url: str = field(default_factory=lambda: _str("GATEWAY_URL", "http://127.0.0.1:9000"))
    # POST timeouts are deliberately short: a slow gateway must not block the
    # API, and an ambiguous outcome is resolved by looking the receipt up.
    post_timeout_seconds: float = field(default_factory=lambda: _float("GATEWAY_TIMEOUT_SECONDS", 2.0))
    lookup_timeout_seconds: float = field(default_factory=lambda: _float("GATEWAY_LOOKUP_TIMEOUT_SECONDS", 1.0))
    worker_interval_seconds: float = field(default_factory=lambda: _float("WORKER_INTERVAL_SECONDS", 0.1))
    delivery_lease_seconds: float = field(default_factory=lambda: _float("DELIVERY_LEASE_SECONDS", 10.0))
    max_backoff_seconds: float = field(default_factory=lambda: _float("DELIVERY_MAX_BACKOFF_SECONDS", 1.0))
    delivery_concurrency: int = field(default_factory=lambda: _int("DELIVERY_CONCURRENCY", 8))
    # Consecutive "not found" gateway lookups tolerated before re-posting an
    # unresolved notification. Guards against racing an original POST that a
    # slow gateway is still processing.
    not_found_misses_before_repost: int = field(
        default_factory=lambda: _int("NOT_FOUND_MISSES_BEFORE_REPOST", 3)
    )
    log_level: str = field(default_factory=lambda: _str("LOG_LEVEL", "info"))
    # When true, status reads resolve ambiguous receipts with the gateway before
    # rendering. Read-only: it never posts.
    reconcile_on_read: bool = field(default_factory=lambda: _bool("RECONCILE_ON_READ", True))
    # A single API instance owns the SQLite file, so on startup any in_flight
    # row was interrupted by the restart and may be reconciled immediately.
    # Set to false if running multiple replicas against one database.
    recover_inflight_on_startup: bool = field(
        default_factory=lambda: _bool("RECOVER_INFLIGHT_ON_STARTUP", True)
    )
