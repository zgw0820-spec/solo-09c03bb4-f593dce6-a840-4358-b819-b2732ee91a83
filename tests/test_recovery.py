"""Restart and receipt-loss recovery.

These tests kill and rebuild the API process (fresh Application, same SQLite
file and same in-process gateway) to prove that no in-memory state is required
to finish in-flight work.
"""

from __future__ import annotations

import asyncio
import time

import httpx

from app.config import Settings
from app.main import create_app
from tests.fake_gateway import FakeGateway, build_fake_gateway_app


class RestartableApi:
    def __init__(self, db_path: str, fake: FakeGateway, gateway_url: str):
        self.db_path = db_path
        self.fake = fake
        self.gateway_url = gateway_url
        self.gateway_app = build_fake_gateway_app(fake)

    async def start(self) -> RestartableApi:
        settings = Settings(
            database_path=self.db_path,
            gateway_url=self.gateway_url,
            post_timeout_seconds=0.6,
            lookup_timeout_seconds=0.4,
            worker_interval_seconds=0.02,
            delivery_lease_seconds=1.0,
            max_backoff_seconds=0.05,
            reconcile_on_read=True,
        )
        self.app = create_app(settings)
        self.settings = settings
        self._lifespan = self.app.router.lifespan_context(self.app)
        await self._lifespan.__aenter__()
        self.app.state.gateway._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.gateway_app), base_url=self.gateway_url
        )
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://api.test"
        )
        return self

    async def stop(self) -> None:
        await self.client.aclose()
        await self._lifespan.__aexit__(None, None, None)

    async def wait_until(self, predicate, timeout: float = 8.0) -> bool:
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            if await predicate():
                return True
            await asyncio.sleep(0.02)
        return False


def payload(alarm_no: str, deadline: int = 3) -> dict:
    return {
        "alarm_no": alarm_no,
        "content": "供电柜 B12 温度 82°C",
        "primary_contact": {"name": "主值班员", "channel": "app"},
        "backup_contact": {"name": "备班", "channel": "sms", "address": "+8613800000001"},
        "ack_deadline_seconds": deadline,
    }


async def test_restart_resumes_pending_delivery_without_duplicate(tmp_path):
    """Stop the API before the primary POST happens; the new process must send
    exactly one page."""
    fake = FakeGateway()
    fake.slow_seconds = 5.0  # ensure nothing completes before the restart
    db_path = str(tmp_path / "alarms.db")
    first = await RestartableApi(db_path, fake, "http://gw.test").start()
    submit = await first.client.post("/alarms", json=payload("R1", deadline=10))
    assert submit.status_code == 201
    # Crash while the notification is still queued/in flight.
    await asyncio.sleep(0.15)
    await first.stop()

    fake.slow_seconds = 0.0
    second = await RestartableApi(db_path, fake, "http://gw.test").start()
    try:
        assert await second.wait_until(
            lambda: _accepted(second, "R1"), timeout=10
        ), (await second.client.get("/alarms/R1")).text
        assert len(fake.role_notifications("primary")) == 1
        view = (await second.client.get("/alarms/R1")).json()
        assert view["primary_accepted_at"] is not None
        assert view["deduplicated"] is False
    finally:
        await second.stop()


async def test_restart_after_gateway_accepted_but_receipt_lost_does_not_page_twice(tmp_path):
    """The gateway accepted the primary and then the receipt (and the API) were
    lost. Recovery must query the gateway, adopt the acceptance, and never
    re-post."""
    fake = FakeGateway()
    fake.slow_seconds = 0.6
    fake.drop_receipt = True
    fake.drop_hold_seconds = 2.0
    db_path = str(tmp_path / "alarms.db")
    first = await RestartableApi(db_path, fake, "http://gw.test").start()
    await first.client.post("/alarms", json=payload("R2", deadline=5))
    # Wait until the fake gateway has durably accepted the notification.
    deadline = time.monotonic() + 6
    while not _gateway_has(fake, "R2", "primary") and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    assert _gateway_has(fake, "R2", "primary")
    await first.stop()

    fake.slow_seconds = 0.0
    fake.drop_receipt = False
    second = await RestartableApi(db_path, fake, "http://gw.test").start()
    try:
        assert await second.wait_until(lambda: _accepted(second, "R2"), timeout=10)
        assert len(fake.role_notifications("primary")) == 1, "receipt loss caused a duplicate page"
        view = (await second.client.get("/alarms/R2")).json()
        assert view["primary_accepted_at"] is not None
        assert view["escalated"] is False
        assert not any(n["state"] == "unknown" for n in view["notifications"])
    finally:
        await second.stop()


async def test_restart_preserves_escalation_single_delivery(tmp_path):
    """An alarm that escalated before a restart must not enqueue a second backup
    after the restart."""
    fake = FakeGateway()
    db_path = str(tmp_path / "alarms.db")
    first = await RestartableApi(db_path, fake, "http://gw.test").start()
    await first.client.post("/alarms", json=payload("R3", deadline=2))
    assert await first.wait_until(
        lambda: _has_backup(first, "R3"), timeout=10
    ), (await first.client.get("/alarms/R3")).text
    await first.stop()

    second = await RestartableApi(db_path, fake, "http://gw.test").start()
    try:
        await asyncio.sleep(1.2)  # several worker ticks after restart
        view = (await second.client.get("/alarms/R3")).json()
        assert len([n for n in view["notifications"] if n["role"] == "backup"]) == 1
        assert len(fake.role_notifications("backup")) == 1
    finally:
        await second.stop()


async def test_confirmation_survives_restart(tmp_path):
    fake = FakeGateway()
    db_path = str(tmp_path / "alarms.db")
    first = await RestartableApi(db_path, fake, "http://gw.test").start()
    await first.client.post("/alarms", json=payload("R4", deadline=3))
    assert await first.wait_until(lambda: _accepted(first, "R4"), timeout=8)
    assert (await first.client.post("/alarms/R4/confirm", json={"confirmed_by": "x"})).status_code == 200
    await first.stop()

    second = await RestartableApi(db_path, fake, "http://gw.test").start()
    try:
        await asyncio.sleep(3.2)  # past the deadline
        view = (await second.client.get("/alarms/R4")).json()
        assert view["status"] == "confirmed"
        assert view["escalated"] is False
        assert fake.role_notifications("backup") == []
    finally:
        await second.stop()


async def _accepted(api: RestartableApi, alarm_no: str) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    if response.status_code != 200:
        return False
    return response.json()["primary_accepted_at"] is not None


async def _has_backup(api: RestartableApi, alarm_no: str) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    if response.status_code != 200:
        return False
    return any(n["role"] == "backup" for n in response.json()["notifications"])


def _gateway_has(fake: FakeGateway, alarm_no: str, role: str) -> bool:
    return any(r["alarm_no"] == alarm_no and r["role"] == role for r in fake.records.values())
