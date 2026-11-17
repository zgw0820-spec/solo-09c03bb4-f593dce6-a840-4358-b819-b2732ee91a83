"""End-to-end behaviour of the alarm escalation API."""

from __future__ import annotations

import asyncio

from tests.conftest import ApiHarness
from tests.helpers import _get, _status


async def test_submit_creates_primary_and_starts_clock_only_after_acceptance(api: ApiHarness):
    submit = await api.client.post("/alarms", json=api.alarm_payload("A1", deadline=5))
    assert submit.status_code == 201, submit.text
    body = submit.json()
    assert body["status"] == "primary_queued"
    assert body["primary_accepted_at"] is None
    assert body["confirmation_due_at"] is None

    ok = await api.wait_until(
        lambda: _status(api, "A1", "awaiting_confirmation")
    )
    assert ok, await _get(api, "A1")

    view = (await api.client.get("/alarms/A1")).json()
    assert view["deduplicated"] is False
    assert view["primary_accepted_at"] is not None
    assert view["confirmation_due_at"] is not None
    assert view["notifications"][0]["role"] == "primary"
    assert view["notifications"][0]["display_status"] == "accepted"
    # Clock must be exactly accepted_at + deadline, not submission time.
    assert view["seconds_until_escalation"] is not None
    assert 0 < view["seconds_until_escalation"] <= 5.0


async def test_same_content_retry_is_idempotent_and_different_content_conflicts(api: ApiHarness):
    payload = api.alarm_payload("A2", deadline=5)
    first = await api.client.post("/alarms", json=payload)
    assert first.status_code == 201
    await api.wait_until(lambda: _status(api, "A2", "awaiting_confirmation"))

    retry = await api.client.post("/alarms", json=payload)
    assert retry.status_code == 200
    assert retry.json()["deduplicated"] is True
    assert retry.json()["alarm_no"] == "A2"
    # The retry must not generate another notification.
    primary_no = first.json()["notifications"][0]["notification_no"]
    gemini = retry.json()["notifications"][0]["notification_no"]
    assert primary_no == gemini
    assert len(api.fake.role_notifications("primary")) == 1

    changed = dict(payload, content="供电柜 A08 温度 81°C")
    conflict = await api.client.post("/alarms", json=changed)
    assert conflict.status_code == 409
    assert len(api.fake.role_notifications("primary")) == 1


async def test_confirmation_stops_escalation(api: ApiHarness):
    await api.client.post("/alarms", json=api.alarm_payload("A3", deadline=2))
    await api.wait_until(lambda: _status(api, "A3", "awaiting_confirmation"))

    confirm = await api.client.post("/alarms/A3/confirm", json={"confirmed_by": "张工"})
    assert confirm.status_code == 200, confirm.text
    assert confirm.json()["status"] == "confirmed"

    # Past the deadline: no backup may appear.
    await asyncio.sleep(2.5)
    view = (await api.client.get("/alarms/A3")).json()
    assert view["escalated"] is False
    assert [n["role"] for n in view["notifications"]] == ["primary"]
    assert api.fake.role_notifications("backup") == []


async def test_overdue_alarm_escalates_exactly_once(api: ApiHarness):
    await api.client.post("/alarms", json=api.alarm_payload("A4", deadline=2))
    assert await api.wait_until(lambda: _status(api, "A4", "escalated"), timeout=8)

    await asyncio.sleep(1.0)  # several worker ticks after escalation
    view = (await api.client.get("/alarms/A4")).json()
    backups = [n for n in view["notifications"] if n["role"] == "backup"]
    assert len(backups) == 1
    assert backups[0]["display_status"] == "accepted"
    assert len(api.fake.role_notifications("backup")) == 1
    assert sum(1 for no in api.fake.deliveries if no in {b["notification_no"] for b in backups}) == 1


async def test_unknown_receipt_does_not_start_clock_or_escalate(api: ApiHarness):
    """Gateway accepts but drops the receipt and its lookup is unreachable: the
    API must report an unconfirmed receipt and must not escalate, even after the
    nominal deadline has passed."""
    api.fake.drop_receipt = True
    api.fake.drop_hold_seconds = 0.3
    api.fake.lookup_down = True
    await api.client.post("/alarms", json=api.alarm_payload("A5", deadline=2))

    unknown = await api.wait_until(lambda: _status(api, "A5", "primary_unconfirmed"), timeout=6)
    assert unknown, await _get(api, "A5")
    view = (await api.client.get("/alarms/A5")).json()
    assert view["primary_accepted_at"] is None
    assert view["confirmation_due_at"] is None
    assert view["seconds_until_escalation"] is None
    assert any("回执" in w for w in view["warnings"])

    # Let the nominal deadline pass: the clock never started, so no escalation.
    await asyncio.sleep(2.5)
    view = (await api.client.get("/alarms/A5")).json()
    assert view["status"] == "primary_unconfirmed"
    assert view["escalated"] is False
    assert api.fake.role_notifications("backup") == []

    # Once the gateway becomes queryable, the true acceptance is discovered, the
    # clock starts, and no duplicate page is sent.
    api.fake.drop_receipt = False
    api.fake.lookup_down = False
    assert await api.wait_until(
        lambda: _api_true(api, lambda v: v["primary_accepted_at"] is not None), timeout=8
    ), await _get(api, "A5")
    view = (await api.client.get("/alarms/A5")).json()
    assert len(api.fake.role_notifications("primary")) == 1  # no duplicate page


async def _api_true(api, predicate) -> bool:
    response = await api.client.get("/alarms/A5")
    return response.status_code == 200 and predicate(response.json())


async def test_late_confirmation_creates_no_new_delivery_and_reports_inflight_truthfully(api: ApiHarness):
    """If the backup is already on the wire when confirmation arrives, the
    confirmation cannot recall it. It must not create a second delivery, and the
    final status must disclose that the in-flight notification was still
    accepted by the gateway after confirmation."""
    await api.client.post("/alarms", json=api.alarm_payload("A6", deadline=2))
    assert await api.wait_until(lambda: _status(api, "A6", "awaiting_confirmation"), timeout=6)

    # Make the backup POST slow so confirmation lands while it is in flight.
    api.fake.slow_seconds = 1.5
    assert await api.wait_until(
        lambda: _api_true_alarm(
            api,
            "A6",
            lambda v: any(
                n["role"] == "backup" and n["state"] == "in_flight" for n in v["notifications"]
            ),
        ),
        timeout=8,
    ), await _get(api, "A6")
    in_flight = (await api.client.get("/alarms/A6")).json()
    backup = next(
        n for n in in_flight["notifications"] if n["role"] == "backup"
    )
    assert backup["state"] == "in_flight", backup

    confirm = await api.client.post("/alarms/A6/confirm", json={"confirmed_by": "张工"})
    assert confirm.status_code == 200
    assert confirm.json()["status"] == "confirmed_after_escalation"
    assert confirm.json()["confirmed"] is True

    await asyncio.sleep(1.5)
    view = (await api.client.get("/alarms/A6")).json()
    backups = [n for n in view["notifications"] if n["role"] == "backup"]
    assert len(backups) == 1, "confirmation must not create a second backup delivery"
    assert len(api.fake.role_notifications("backup")) == 1
    # Whatever happened to the in-flight notification is disclosed, not hidden.
    assert backups[0]["state"] in {"accepted", "unknown", "cancelled"}
    if backups[0]["state"] == "accepted":
        assert any("在途" in w for w in view["warnings"])


async def _api_true_alarm(api, alarm_no: str, predicate) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    return response.status_code == 200 and predicate(response.json())


async def test_slow_primary_does_not_block_other_alarm_confirmation(api: ApiHarness):
    """A slow delivery for one alarm must not delay confirming another."""
    api.fake.slow_seconds = 1.0
    slow = await api.client.post("/alarms", json=api.alarm_payload("SLOW", deadline=5))
    assert slow.status_code == 201

    # Normal gateway for the second alarm: give it a head start so it is
    # accepted before we submit the slow one's confirmation.
    api.fake.slow_seconds = 0.0
    await api.client.post("/alarms", json=api.alarm_payload("FAST", deadline=5))
    assert await api.wait_until(lambda: _status(api, "FAST", "awaiting_confirmation"))

    api.fake.slow_seconds = 1.0
    started = asyncio.get_event_loop().time()
    confirm = await api.client.post("/alarms/FAST/confirm", json={"confirmed_by": "张工"})
    elapsed = asyncio.get_event_loop().time() - started
    assert confirm.status_code == 200
    assert elapsed < 0.5, f"confirmation blocked for {elapsed:.2f}s by a slow gateway"
