"""In-flight backup disclosure and miscellaneous API surface."""

from __future__ import annotations

from tests.conftest import ApiHarness
from tests.helpers import _get, _status


async def test_unresolved_inflight_backup_is_disclosed_after_confirmation(api: ApiHarness):
    """The backup is on the wire when the primary confirms, its receipt is lost,
    and the gateway only accepts it afterwards. The API must not create a new
    delivery, must resolve the receipt by lookup, and must disclose that the
    backup was delivered after the confirmation."""
    api.fake.slow_seconds = 0.0
    await api.client.post("/alarms", json=api.alarm_payload("INFLIGHT-UNK", deadline=2))
    assert await api.wait_until(lambda: _status(api, "INFLIGHT-UNK", "awaiting_confirmation"))

    # Slow acceptance so the backup is still in flight at confirmation time, and
    # lose its receipt once the gateway does accept it.
    api.fake.slow_seconds = 2.0
    api.fake.drop_receipt = True
    api.fake.drop_hold_seconds = 0.2
    assert await api.wait_until(
        lambda: _has_backup_state(api, "INFLIGHT-UNK", {"in_flight"}), timeout=8
    ), await _get(api, "INFLIGHT-UNK")

    confirm = await api.client.post(
        "/alarms/INFLIGHT-UNK/confirm", json={"confirmed_by": "张工"}
    )
    assert confirm.status_code == 200
    assert confirm.json()["status"] == "confirmed_after_escalation"

    # Let the gateway finish accepting and become queryable.
    api.fake.slow_seconds = 0.0
    api.fake.drop_receipt = False
    resolved = await api.wait_until(
        lambda: _backup_accepted(api, "INFLIGHT-UNK"), timeout=10
    )
    assert resolved, await _get(api, "INFLIGHT-UNK")

    view = (await api.client.get("/alarms/INFLIGHT-UNK")).json()
    backups = [n for n in view["notifications"] if n["role"] == "backup"]
    assert len(backups) == 1
    assert backups[0]["state"] == "accepted"
    assert backups[0]["delivered_after_confirmation"] is True
    assert any("在途" in w for w in view["warnings"])
    assert len(api.fake.role_notifications("backup")) == 1


async def test_notifications_list_endpoint(api: ApiHarness):
    first = await api.client.post("/alarms", json=api.alarm_payload("LIST-1", deadline=5))
    notification_no = first.json()["notifications"][0]["notification_no"]
    listed = await api.client.get("/alarms/LIST-1/notifications")
    assert listed.status_code == 200
    body = listed.json()
    assert body["total"] == 1
    assert body["items"][0]["notification_no"] == notification_no

    missing = await api.client.get("/alarms/NOPE/notifications")
    assert missing.status_code == 404


async def test_alarm_list_and_filter(api: ApiHarness):
    await api.client.post("/alarms", json=api.alarm_payload("L-1", deadline=5))
    await api.client.post("/alarms", json=api.alarm_payload("L-2", deadline=5))
    assert await api.wait_until(lambda: _status(api, "L-2", "awaiting_confirmation"))

    everything = (await api.client.get("/alarms")).json()
    assert {item["alarm_no"] for item in everything["items"]} >= {"L-1", "L-2"}

    filtered = (await api.client.get("/alarms", params={"status": "awaiting_confirmation"})).json()
    assert filtered["total"] >= 1
    assert all(item["status"] == "awaiting_confirmation" for item in filtered["items"])


async def test_confirm_is_idempotent(api: ApiHarness):
    await api.client.post("/alarms", json=api.alarm_payload("IDEM", deadline=5))
    assert await api.wait_until(lambda: _status(api, "IDEM", "awaiting_confirmation"))
    first = await api.client.post("/alarms/IDEM/confirm", json={"confirmed_by": "张工"})
    second = await api.client.post("/alarms/IDEM/confirm", json={"confirmed_by": "李工"})
    assert first.status_code == second.status_code == 200
    assert second.json()["confirmed_by"] == "张工", "repeat confirmation must not rewrite the record"
    events = [e["kind"] for e in second.json()["events"]]
    assert events.count("confirmed") == 1


async def test_unknown_alarm_returns_404(api: ApiHarness):
    assert (await api.client.get("/alarms/DOES-NOT-EXIST")).status_code == 404
    assert (
        await api.client.post("/alarms/DOES-NOT-EXIST/confirm", json={"confirmed_by": "x"})
    ).status_code == 404


async def _has_backup_state(api: ApiHarness, alarm_no: str, states: set[str]) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    if response.status_code != 200:
        return False
    return any(
        n["role"] == "backup" and n["state"] in states
        for n in response.json()["notifications"]
    )


async def _backup_accepted(api: ApiHarness, alarm_no: str) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    if response.status_code != 200:
        return False
    return any(
        n["role"] == "backup" and n["state"] == "accepted"
        for n in response.json()["notifications"]
    )
