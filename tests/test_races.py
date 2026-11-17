"""Confirmation vs. deadline races, and gateway rejection handling."""

from __future__ import annotations

import asyncio
import time

from tests.conftest import ApiHarness
from tests.helpers import _status


async def test_deadline_and_confirmation_race_never_creates_a_backup_delivery(api: ApiHarness):
    """Race confirm against the deadline handler, repeatedly.

    The background dispatcher is stopped first, so this isolates the two domain
    operations: either the confirmation wins (no backup row at all) or escalation
    wins and the confirmation cancels the still-queued backup. Neither outcome
    may leave a dispatchable backup behind a confirmation.
    """
    service = api.app.state.service
    dispatcher = api.app.state.dispatcher

    for index in range(8):
        alarm_no = f"RACE-{index}"
        submit = await api.client.post("/alarms", json=api.alarm_payload(alarm_no, deadline=2))
        assert submit.status_code == 201
        assert await api.wait_until(
            lambda name=alarm_no: _status(api, name, "awaiting_confirmation")
        )

        # Freeze delivery, then race the two domain operations with an
        # already-expired deadline.
        dispatcher.delivery_paused = True
        loop = asyncio.get_running_loop()
        await asyncio.gather(
            loop.run_in_executor(
                None, lambda name=alarm_no: service.confirm(name, "张工", None)
            ),
            loop.run_in_executor(
                None, lambda: dispatcher.process_deadlines(time.time() + 3600)
            ),
        )
        dispatcher.delivery_paused = False

        view = (await api.client.get(f"/alarms/{alarm_no}")).json()
        assert view["confirmed"] is True
        backups = [n for n in view["notifications"] if n["role"] == "backup"]
        assert len(backups) <= 1
        for backup in backups:
            assert backup["state"] == "cancelled", (
                f"backup left dispatchable after confirmation: {backup}"
            )
        assert all(
            record["alarm_no"] != alarm_no
            for record in api.fake.role_notifications("backup")
        ), "a backup notification was delivered for a confirmed alarm"

    # Unpause: a cancelled backup must still never be delivered.
    dispatcher.delivery_paused = False
    await asyncio.sleep(0.5)
    assert api.fake.role_notifications("backup") == []


async def test_primary_rejection_is_reported_and_never_escalates(api: ApiHarness):
    api.fake.reject = True
    await api.client.post("/alarms", json=api.alarm_payload("REJ", deadline=2))

    assert await api.wait_until(lambda: _status(api, "REJ", "primary_rejected"), timeout=6)
    await asyncio.sleep(2.4)  # deadline window elapses
    view = (await api.client.get("/alarms/REJ")).json()
    assert view["primary_accepted_at"] is None
    assert view["confirmation_due_at"] is None
    assert view["escalated"] is False
    assert api.fake.role_notifications("backup") == []
    assert any("拒收" in w for w in view["warnings"])
    # A rejected notification is not retried blindly.
    assert api.fake.post_calls == 1


async def test_ack_deadline_bounds_are_enforced(api: ApiHarness):
    for bad in (1, 31):
        response = await api.client.post(
            "/alarms", json=api.alarm_payload(f"BAD-{bad}", deadline=bad)
        )
        assert response.status_code == 422, response.text


async def test_swagger_and_openapi_are_exposed(api: ApiHarness):
    docs = await api.client.get("/docs")
    assert docs.status_code == 200
    assert "swagger" in docs.text.lower()

    schema = (await api.client.get("/openapi.json")).json()
    paths = schema["paths"]
    assert "/alarms" in paths
    assert "/alarms/{alarm_no}/confirm" in paths
    assert "/alarms/{alarm_no}/notifications" in paths
    assert "/notifications/{notification_no}" in paths


async def test_notification_lookup_by_number(api: ApiHarness):
    submit = await api.client.post("/alarms", json=api.alarm_payload("N-BY-ID", deadline=5))
    notification_no = submit.json()["notifications"][0]["notification_no"]
    assert await api.wait_until(lambda: _status(api, "N-BY-ID", "awaiting_confirmation"))

    response = await api.client.get(f"/notifications/{notification_no}")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["notification_no"] == notification_no
    assert body["role"] == "primary"
    assert body["state"] == "accepted"
    assert body["history"]

    missing = await api.client.get("/notifications/N-NOPE")
    assert missing.status_code == 404
