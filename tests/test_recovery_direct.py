"""Direct tests for restart recovery of interrupted deliveries."""

from __future__ import annotations

import asyncio
import time

import httpx

from app.db import Database
from app.gateway_client import GatewayClient
from app.models import AlarmCreate
from app.service import AlarmService
from app.worker import Dispatcher
from tests.fake_gateway import FakeGateway, build_fake_gateway_app


def _payload(alarm_no: str) -> AlarmCreate:
    return AlarmCreate(
        alarm_no=alarm_no,
        content="供电柜 H08 温度 87°C",
        primary_contact={"name": "主值班员"},
        backup_contact={"name": "备班"},
        ack_deadline_seconds=5,
    )


def _build(tmp_path, fake: FakeGateway):
    db = Database(str(tmp_path / "recover.db"))
    service = AlarmService(db)
    gateway = GatewayClient("http://gw.test")
    gateway._client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=build_fake_gateway_app(fake)), base_url="http://gw.test"
    )
    dispatcher = Dispatcher(
        db, service, gateway, interval=0.01, lease_seconds=30, max_backoff_seconds=0.01
    )
    return db, service, dispatcher


async def _run(dispatcher: Dispatcher, seconds: float = 1.0) -> None:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        await dispatcher.dispatch_available()
        await asyncio.sleep(0.01)


async def test_interrupted_post_that_gateway_accepted_is_not_reposted(tmp_path):
    """Simulate a crash mid-POST: the row is in_flight and the gateway already
    has the record. Recovery looks it up and adopts acceptance."""
    fake = FakeGateway()
    db, service, dispatcher = _build(tmp_path, fake)
    _view, _deduplicated = service.submit_alarm(_payload("INT-1"))
    notification = db.query_one("SELECT * FROM notifications WHERE alarm_no = 'INT-1'")

    # The gateway accepted, but the process died before recording it.
    fake.deliver(
        {
            "notification_no": notification["notification_no"],
            "alarm_no": "INT-1",
            "role": "primary",
            "contact": notification["contact_json"],
            "content_hash": notification["content_hash"],
            "payload": {},
        }
    )
    db.execute("UPDATE notifications SET state = 'in_flight' WHERE id = ?", (notification["id"],))

    assert dispatcher.recover_interrupted() == 1
    row = db.query_one("SELECT * FROM notifications WHERE id = ?", (notification["id"],))
    assert row["state"] == "unknown"

    await _run(dispatcher)
    row = db.query_one("SELECT * FROM notifications WHERE id = ?", (notification["id"],))
    assert row["state"] == "accepted"
    assert len(fake.role_notifications("primary")) == 1, "recovery must not page twice"
    alarm = db.query_one("SELECT * FROM alarms WHERE alarm_no = 'INT-1'")
    assert alarm["primary_accepted_real"] is not None


async def test_interrupted_post_the_gateway_never_saw_is_reposted_once(tmp_path):
    """The reverse case: nothing reached the gateway, so after enough missed
    lookups the notification is posted, and the deterministic notification
    number keeps the gateway's dedupe intact."""
    fake = FakeGateway()
    db, service, dispatcher = _build(tmp_path, fake)
    service.submit_alarm(_payload("INT-2"))
    notification = db.query_one("SELECT * FROM notifications WHERE alarm_no = 'INT-2'")
    db.execute("UPDATE notifications SET state = 'in_flight' WHERE id = ?", (notification["id"],))
    dispatcher.recover_interrupted()

    await _run(dispatcher, seconds=1.5)
    row = db.query_one("SELECT * FROM notifications WHERE id = ?", (notification["id"],))
    assert row["state"] == "accepted"
    assert len(fake.role_notifications("primary")) == 1
    assert fake.post_calls >= 1


async def test_recovery_is_idempotent_and_keeps_history_bounded(tmp_path):
    fake = FakeGateway()
    fake.lookup_down = True
    db, service, dispatcher = _build(tmp_path, fake)
    service.submit_alarm(_payload("INT-3"))
    notification = db.query_one("SELECT * FROM notifications WHERE alarm_no = 'INT-3'")
    db.execute("UPDATE notifications SET state = 'in_flight' WHERE id = ?", (notification["id"],))
    dispatcher.recover_interrupted()
    # Second call after a clean state finds nothing to do.
    assert dispatcher.recover_interrupted() == 0

    await _run(dispatcher, seconds=1.0)
    row = db.query_one("SELECT * FROM notifications WHERE id = ?", (notification["id"],))
    assert row["state"] == "unknown"
    import json

    history = json.loads(row["history_json"])
    assert len(history) <= 4, f"unresolved retries must not grow history: {history}"
    alarm_events = db.query("SELECT kind FROM events WHERE alarm_no = 'INT-3'")
    assert len(alarm_events) <= 4, "unresolved retries must not grow the event log"
