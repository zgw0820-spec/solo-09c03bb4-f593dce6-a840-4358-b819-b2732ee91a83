"""Notification dispatch worker.

Three independent loops run side by side so that latency anywhere never
serialises unrelated work:

* ``_dispatch_loop`` — claims notifications (a short DB transaction) and runs up
  to ``max_concurrency`` deliveries in parallel tasks. A slow gateway response
  for one alarm therefore never delays another alarm's page.
* ``_deadline_loop`` — scans for expired confirmation clocks and enqueues the
  single backup notification. It performs no network I/O, so it stays
  responsive regardless of gateway latency.
* ``_recovery_loop`` — periodically re-checks notifications whose receipt is
  unknown by looking them up, never by assuming delivery.

Every loop derives its work from the database on each tick; there is no
in-memory queue, so a restart loses nothing.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from .db import Database, parse_ts
from .gateway_client import GatewayClient, GatewayResult
from .service import AlarmService

log = logging.getLogger("alarm.worker")


class Dispatcher:
    def __init__(
        self,
        db: Database,
        service: AlarmService,
        gateway: GatewayClient,
        *,
        interval: float = 0.1,
        lease_seconds: float = 10.0,
        max_backoff_seconds: float = 1.0,
        max_concurrency: int = 8,
        not_found_misses_before_repost: int = 3,
    ):
        self.db = db
        self.service = service
        self.gateway = gateway
        self.interval = interval
        self.lease_seconds = lease_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.max_concurrency = max_concurrency
        # A "not found" lookup is ambiguous while the original POST might still
        # be running, so require several consecutive misses before re-posting.
        self.not_found_misses_before_repost = not_found_misses_before_repost
        self._tasks: list[asyncio.Task] = []
        self._delivery_tasks: set[asyncio.Task] = set()
        self._stop = asyncio.Event()
        # When paused, the deadline loop does not enqueue backups. Escalation can
        # then be driven explicitly, which is useful for tests and for draining
        # an outbox by hand. Delivery is unaffected.
        self.deadlines_paused = False
        # When paused, no notification is handed to the gateway. Lets tests (and
        # maintenance) hold the outbox still.
        self.delivery_paused = False
        # Observability for tests and operators.
        self.delivery_attempts = 0
        self.lookup_attempts = 0

    async def start(self) -> None:
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self._dispatch_loop(), name="alarm-dispatch"),
            asyncio.create_task(self._deadline_loop(), name="alarm-deadline"),
            asyncio.create_task(self._recovery_loop(), name="alarm-recovery"),
        ]

    def recover_interrupted(self) -> int:
        """Make notifications interrupted by a restart eligible immediately.

        An ``in_flight`` row means the previous process died with the POST
        unresolved. The outcome is genuinely unknown, so the row becomes
        ``unknown`` and will be reconciled by looking up the notification number
        — never by assuming delivery and never by blindly re-posting first.
        """
        with self.db.transaction() as conn:
            rows = conn.execute(
                "SELECT id, notification_no, history_json FROM notifications WHERE state = 'in_flight'"
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE notifications SET state = 'unknown', next_attempt_at = 0, lease_until = NULL, "
                    " detail = 'interrupted by restart; receipt to be confirmed via gateway lookup', "
                    " history_json = ? WHERE id = ?",
                    (
                        self.service._append_history(
                            row["history_json"],
                            "unknown",
                            "interrupted by restart; receipt to be confirmed via gateway lookup",
                        ),
                        row["id"],
                    ),
                )
                log.info("recovering interrupted notification %s", row["notification_no"])
            return len(rows)

    async def stop(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        if self._delivery_tasks:
            for task in list(self._delivery_tasks):
                task.cancel()
            await asyncio.gather(*self._delivery_tasks, return_exceptions=True)
            self._delivery_tasks.clear()

    # --------------------------------------------------------------- main loops

    async def _dispatch_loop(self) -> None:
        """Keep up to ``max_concurrency`` deliveries running in parallel.

        Each delivery is its own task, so one alarm's slow gateway POST never
        delays another alarm's notification. Claiming is a short database
        transaction, done before the task starts, which is what makes parallel
        dispatch safe.
        """
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                launched = await self.dispatch_available()
            except asyncio.CancelledError:
                raise
            except Exception:  # never let one bad row kill dispatch
                log.exception("dispatch loop iteration failed")
                launched = 0
            await self._sleep(max(0.0, self.interval - (time.monotonic() - started)), launched > 0)

    async def dispatch_available(self) -> int:
        """Claim and launch as many notifications as concurrency allows."""
        if self.delivery_paused:
            return 0
        self._delivery_tasks = {t for t in self._delivery_tasks if not t.done()}
        capacity = self.max_concurrency - len(self._delivery_tasks)
        if capacity <= 0:
            return 0
        launched = 0
        for row in self.service.notifications_needing_action(time.time())[:capacity]:
            if not self.service.claim_notification(row["id"], time.time(), self.lease_seconds):
                continue
            task = asyncio.create_task(self._run_delivery(row), name=f"deliver-{row['notification_no']}")
            self._delivery_tasks.add(task)
            launched += 1
        return launched

    async def _run_delivery(self, row: dict[str, Any]) -> None:
        try:
            if row["state"] in {"unknown", "in_flight"}:
                await self._resolve_ambiguous(row)
            else:
                await self._post(row)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("delivery of %s failed", row["notification_no"])

    async def _deadline_loop(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            if not self.deadlines_paused:
                try:
                    self.process_deadlines()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("deadline loop iteration failed")
            await self._sleep(max(0.0, self.interval - (time.monotonic() - started)), False)

    async def _recovery_loop(self) -> None:
        """Periodically reconcile receipts that are still unknown.

        This is what makes a lost gateway acknowledgment recoverable: the
        notification number is looked up instead of blindly re-posting, so the
        gateway's own dedupe record determines the truth.
        """
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                await self.reconcile_unknown(limit=5)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("recovery loop iteration failed")
            await self._sleep(max(0.0, 0.5 - (time.monotonic() - started)), False)

    async def _sleep(self, seconds: float, worked: bool) -> None:
        # When there was work, loop again immediately (bounded by interval=0);
        # otherwise wait for the interval or a stop signal.
        delay = min(seconds, self.interval) if worked else seconds
        if delay <= 0:
            delay = self.interval
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except TimeoutError:
            pass

    # ------------------------------------------------------------- unit pieces

    def process_deadlines(self, now_real: float | None = None) -> list[str]:
        """Enqueue backups for overdue alarms. Pure DB work, no network."""
        now_real = now_real if now_real is not None else parse_ts(self._now()) or time.time()
        escalated: list[str] = []
        for alarm_no in self.service.due_escalations(now_real):
            if self.service.escalate(alarm_no, now_real):
                log.info("escalating alarm %s to backup contact", alarm_no)
                escalated.append(alarm_no)
        return escalated

    async def dispatch_once(self) -> bool:
        """Claim and process at most one notification synchronously.

        Kept for tests and manual operation; the background loop uses
        ``dispatch_available`` so slow deliveries never serialise.
        """
        now_real = time.time()
        candidates = self.service.notifications_needing_action(now_real)
        if not candidates:
            return False

        row = candidates[0]
        if not self.service.claim_notification(row["id"], now_real, self.lease_seconds):
            return False  # another worker got it

        if row["state"] == "pending":
            await self._post(row)
        else:
            await self._resolve_ambiguous(row)
        return True

    async def _post(self, row: dict[str, Any]) -> None:
        self.delivery_attempts += 1
        self.service.note_post_attempt(row["id"])
        result = await self.gateway.post_notification(
            notification_no=row["notification_no"],
            alarm_no=row["alarm_no"],
            role=row["role"],
            contact=row["contact_json"],
            content_hash=row["content_hash"],
            payload=_load(row["payload_json"], {}),
        )
        await self._apply_result(row, result)

    async def _resolve_ambiguous(self, row: dict[str, Any]) -> None:
        """Ask the gateway whether the notification was accepted.

        A definitive answer fixes the state. "Not found" means the gateway has no
        record of this notification, so re-posting is harmless: the notification
        number is the gateway's dedupe key, and a still-running first attempt
        will win the insert. Anything else stays unknown and is retried on the
        next tick; critically, no timer is started and no escalation is
        triggered while the receipt is unknown.
        """
        self.lookup_attempts += 1
        result = await self.gateway.lookup(row["notification_no"])
        if result.outcome == "failed" and result.gateway_status == "not_found":
            # The gateway has no record *yet*. It may simply still be processing
            # the original POST, so re-post only after repeated misses, with a
            # backoff between lookups. The notification number is the gateway's
            # dedupe key, so even a re-post racing the original yields exactly
            # one delivery.
            misses = int(row.get("lookup_misses") or 0) + 1
            if misses >= self.not_found_misses_before_repost:
                log.info(
                    "notification %s absent from gateway after %d lookups; re-posting",
                    row["notification_no"],
                    misses,
                )
                await self._post(row)
                return
            self.service.mark_notification(
                row["id"],
                "unknown",
                "not_found",
                f"gateway has no record yet (lookup {misses}); will re-check",
                retry_at=time.time() + self.max_backoff_seconds,
                lookup_miss=True,
            )
            return
        if result.outcome == "unknown":
            self.service.mark_notification(
                row["id"],
                "unknown",
                result.gateway_status,
                result.detail,
                retry_at=time.time() + self.max_backoff_seconds,
            )
            return
        await self._apply_result(row, result)

    async def _apply_result(self, row: dict[str, Any], result: GatewayResult) -> None:
        if result.outcome in {"accepted", "duplicate"}:
            self.service.mark_notification(
                row["id"],
                "accepted",
                result.gateway_status or "accepted",
                result.detail,
                accepted_real=self._accepted_real(result),
            )
            return
        if result.outcome == "failed":
            self.service.mark_notification(
                row["id"], "failed", result.gateway_status, result.detail
            )
            return
        # unknown: retry posting later; never counts as delivery.
        self.service.mark_notification(
            row["id"],
            "unknown",
            result.gateway_status,
            result.detail,
            retry_at=time.time() + self.max_backoff_seconds,
        )

    @staticmethod
    def _accepted_real(result: GatewayResult) -> float | None:
        """Use the gateway's acceptance timestamp when it provides one.

        The confirmation clock measures how long the primary has had the page,
        so a receipt discovered later (after a lost acknowledgment or a restart)
        starts the clock at the real acceptance time rather than at discovery
        time. A future timestamp is clamped to now defensively.
        """
        if not isinstance(result.raw, dict):
            return None
        raw = result.raw.get("accepted_at")
        if not isinstance(raw, str):
            return None
        parsed = parse_ts(raw)
        if parsed is None:
            return None
        return min(parsed, time.time())

    async def reconcile_unknown(self, limit: int = 5, alarm_no: str | None = None) -> int:
        """Explicitly resolve notifications whose receipt is unknown.

        Used by the background recovery loop and by the read path, so a status
        query reflects the gateway's actual record without waiting for the next
        tick. Lookups run concurrently and are individually timeout-bounded, so
        the read path's worst case is one lookup timeout, not one per row.
        """
        if alarm_no is None:
            rows = self.db.query(
                "SELECT * FROM notifications WHERE state = 'unknown' ORDER BY created_real LIMIT ?",
                (limit,),
            )
        else:
            rows = self.db.query(
                "SELECT * FROM notifications WHERE state = 'unknown' AND alarm_no = ? "
                "ORDER BY created_real LIMIT ?",
                (alarm_no, limit),
            )
        if not rows:
            return 0

        results = await asyncio.gather(
            *(self.gateway.lookup(dict(row)["notification_no"]) for row in rows)
        )
        resolved = 0
        for raw, result in zip(rows, results):
            row = dict(raw)
            # Re-read: the dispatch loop may have resolved or re-posted it while
            # the lookup was in flight, and that newer state must win.
            current = self.db.query_one(
                "SELECT state FROM notifications WHERE id = ?", (row["id"],)
            )
            if current is None or current["state"] != "unknown":
                continue
            if result.outcome == "unknown":
                continue
            if result.outcome == "failed" and result.gateway_status == "not_found":
                continue  # the dispatch loop owns re-posting
            if result.outcome in {"accepted", "duplicate"}:
                self.service.mark_notification(
                    row["id"],
                    "accepted",
                    result.gateway_status,
                    "resolved via gateway lookup",
                    accepted_real=self._accepted_real(result),
                )
            else:
                self.service.mark_notification(
                    row["id"], "failed", result.gateway_status, "resolved via gateway lookup"
                )
            resolved += 1
        return resolved

    @staticmethod
    def _now() -> str:
        from .db import utc_now

        return utc_now()


def _load(raw: str, default: Any) -> Any:
    import json

    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return default
