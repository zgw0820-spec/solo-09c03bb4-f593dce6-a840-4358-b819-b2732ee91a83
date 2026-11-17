"""Notification number ownership conflicts.

Scenario behind the incident: the gateway already holds an accepted record
under the same notification number, but for a different alarm number, contact,
role or content. A re-post returns "duplicate"/409 and a lookup returns the
foreign record; the API must never mistake that foreign acceptance for its own
receipt, must not start the confirmation clock or escalate, must disclose the
clash, and must stop re-posting the disputed number.

Same number *and* same content still delivers exactly once.
"""

from __future__ import annotations

import asyncio

import httpx

from tests.conftest import ApiHarness
from tests.helpers import _get, _status


def _primary_view(view: dict) -> dict:
    return next(n for n in view["notifications"] if n["role"] == "primary")


async def _wait_state(api: ApiHarness, alarm_no: str, state: str, timeout: float = 6.0) -> bool:
    return await api.wait_until(
        lambda: _has_state(api, alarm_no, state), timeout=timeout
    )


async def _has_state(api: ApiHarness, alarm_no: str, state: str) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    if response.status_code != 200:
        return False
    return any(n["state"] == state for n in response.json()["notifications"])


async def test_lookup_foreign_record_is_conflict_not_acceptance(api: ApiHarness):
    """Receipt was lost; the gateway answers the lookup with a same-number
    record that belongs to a different alarm. It must not be adopted."""
    # Make the first POST's receipt ambiguous and block direct lookup long
    # enough for the API to land in the unknown state without resolving.
    api.fake.drop_receipt = True
    api.fake.drop_hold_seconds = 0.3
    api.fake.lookup_down = True
    submit = await api.client.post("/alarms", json=api.alarm_payload("CFL-1", deadline=2))
    assert submit.status_code == 201
    notification_no = submit.json()["notifications"][0]["notification_no"]

    assert await _wait_state(api, "CFL-1", "unknown"), await _get(api, "CFL-1")

    # Simulate the real-world clash: the number at the gateway belongs to
    # another alarm (different alarm number, contact and content hash).
    api.fake.records[notification_no] = {
        "notification_no": notification_no,
        "alarm_no": "SOME-OTHER-ALARM",
        "role": "primary",
        "contact": {"name": "无关联系人", "channel": "sms"},
        "content_hash": "deadbeef",
        "payload": {"content": "完全不同的正文"},
        "status": "accepted",
        "deliveries": 1,
        "duplicate_requests": 0,
        "conflict_requests": 0,
        "accepted_at": "2026-01-01T00:00:00.000Z",
    }
    api.fake.drop_receipt = False
    api.fake.lookup_down = False

    assert await _wait_state(api, "CFL-1", "conflicted"), await _get(api, "CFL-1")
    view = (await api.client.get("/alarms/CFL-1")).json()
    primary = _primary_view(view)
    assert primary["display_status"] == "identity_conflict"
    assert view["status"] == "primary_conflict"
    # The foreign acceptance must never start this alarm's clock.
    assert view["primary_accepted_at"] is None
    assert view["confirmation_due_at"] is None
    assert view["escalated"] is False
    assert any("归属冲突" in w for w in view["warnings"])
    # The gateway's original record is untouched.
    assert api.fake.records[notification_no]["alarm_no"] == "SOME-OTHER-ALARM"


async def test_conflicted_notification_never_reposts_or_escalates(api: ApiHarness):
    """Once the clash is known, the disputed number must not be hammered and no
    backup notification may ever be created, even well past the deadline."""
    api.fake.drop_receipt = True
    api.fake.drop_hold_seconds = 0.3
    api.fake.lookup_down = True
    submit = await api.client.post("/alarms", json=api.alarm_payload("CFL-2", deadline=2))
    notification_no = submit.json()["notifications"][0]["notification_no"]
    assert await _wait_state(api, "CFL-2", "unknown"), await _get(api, "CFL-2")

    # Turn the gateway record into a foreign one, then expose the clash.
    held = api.fake.records[notification_no]
    held["alarm_no"] = "FOREIGN-ALARM"
    held["content_hash"] = "0" * 64
    api.fake.drop_receipt = False
    api.fake.lookup_down = False

    assert await _wait_state(api, "CFL-2", "conflicted"), await _get(api, "CFL-2")
    post_calls_at_conflict = api.fake.post_calls
    await asyncio.sleep(2.5)  # past the nominal 2s deadline, many worker ticks

    view = (await api.client.get("/alarms/CFL-2")).json()
    assert view["escalated"] is False
    assert [n["role"] for n in view["notifications"]] == ["primary"]
    assert api.fake.role_notifications("backup") == []
    assert _primary_view(view)["state"] == "conflicted"
    assert api.fake.post_calls == post_calls_at_conflict, "disputed number was re-posted"
    assert view["primary_accepted_at"] is None


async def test_repost_same_number_different_content_gets_409_and_is_terminal(api: ApiHarness):
    """The gateway itself rejects a same-number/different-identity POST with
    409; the API records the terminal conflict and never retries the number."""
    api.fake.drop_receipt = True
    api.fake.drop_hold_seconds = 0.3
    api.fake.lookup_down = True
    submit = await api.client.post("/alarms", json=api.alarm_payload("CFL-3", deadline=2))
    notification_no = submit.json()["notifications"][0]["notification_no"]
    assert await _wait_state(api, "CFL-3", "unknown"), await _get(api, "CFL-3")

    # Make every POST to the gateway a conflict for this number, as a gateway
    # holding a foreign record would answer.
    held = api.fake.records[notification_no]
    held["alarm_no"] = "FOREIGN-ALARM"
    held["content_hash"] = "1" * 64
    api.fake.drop_receipt = False
    api.fake.lookup_down = False

    # The worker first tries a lookup (which reports the conflict); assert the
    # end state and that no further POSTs happen.
    assert await _wait_state(api, "CFL-3", "conflicted"), await _get(api, "CFL-3")
    posts_after = api.fake.post_calls
    await asyncio.sleep(1.0)
    assert api.fake.post_calls == posts_after
    view = (await api.client.get("/alarms/CFL-3")).json()
    assert _primary_view(view)["gateway_status"] == "identity_conflict"
    assert view["status"] == "primary_conflict"
    assert view["escalated"] is False


async def test_pending_post_that_hits_foreign_record_becomes_terminal_conflict(api: ApiHarness):
    """A notification still waiting to be POSTed finds the gateway already
    holding its number for another alarm. The gateway's 409 must end in the
    terminal conflicted state without starting any clock."""
    api.app.state.dispatcher.delivery_paused = True
    submit = await api.client.post("/alarms", json=api.alarm_payload("CFL-4", deadline=2))
    assert submit.status_code == 201
    notification_no = submit.json()["notifications"][0]["notification_no"]

    # Foreign record squats the number before our first POST goes out.
    api.fake.records[notification_no] = {
        "notification_no": notification_no,
        "alarm_no": "SQUATTER",
        "role": "primary",
        "contact": {"name": "抢占者", "channel": "phone"},
        "content_hash": "ab" * 32,
        "payload": {"content": "抢占正文"},
        "status": "accepted",
        "deliveries": 1,
        "duplicate_requests": 0,
        "conflict_requests": 0,
        "accepted_at": "2026-01-02T00:00:00.000Z",
    }
    api.app.state.dispatcher.delivery_paused = False

    assert await _wait_state(api, "CFL-4", "conflicted"), await _get(api, "CFL-4")
    assert api.fake.conflict_calls >= 1
    view = (await api.client.get("/alarms/CFL-4")).json()
    assert view["primary_accepted_at"] is None
    assert view["confirmation_due_at"] is None
    assert view["escalated"] is False
    assert _primary_view(view)["gateway_status"] == "identity_conflict"

    await asyncio.sleep(2.5)  # well past the nominal deadline
    view = (await api.client.get("/alarms/CFL-4")).json()
    assert view["escalated"] is False
    assert api.fake.role_notifications("backup") == []
    assert _primary_view(view)["state"] == "conflicted"


async def test_same_number_same_content_retry_still_delivers_once(api: ApiHarness):
    """The fix must not break the happy idempotent path: a lost receipt
    followed by a lookup matching the exact identity is adopted, and the page
    is never sent twice."""
    api.fake.drop_receipt = True
    api.fake.drop_hold_seconds = 0.3
    api.fake.lookup_down = True
    submit = await api.client.post("/alarms", json=api.alarm_payload("CFL-OK", deadline=5))
    notification_no = submit.json()["notifications"][0]["notification_no"]
    assert await _wait_state(api, "CFL-OK", "unknown"), await _get(api, "CFL-OK")
    # The gateway's record is genuinely ours: identity untouched.
    assert api.fake.records[notification_no]["alarm_no"] == "CFL-OK"

    api.fake.drop_receipt = False
    api.fake.lookup_down = False
    assert await api.wait_until(
        lambda: _status(api, "CFL-OK", "awaiting_confirmation"), timeout=8
    ), await _get(api, "CFL-OK")
    view = (await api.client.get("/alarms/CFL-OK")).json()
    assert view["primary_accepted_at"] is not None
    assert view["confirmation_due_at"] is not None
    assert len(api.fake.role_notifications("primary")) == 1
    assert _primary_view(view)["state"] == "accepted"


async def test_gateway_does_not_overwrite_foreign_record_on_conflict(api: ApiHarness):
    """A conflicting POST must leave the gateway's original record intact."""
    # First alarm owns the number at the gateway.
    first = await api.client.post("/alarms", json=api.alarm_payload("OWNER", deadline=5))
    notification_no = first.json()["notifications"][0]["notification_no"]
    assert await api.wait_until(lambda: _status(api, "OWNER", "awaiting_confirmation"))
    original = dict(api.fake.records[notification_no])

    # Drive a conflicting POST directly against the gateway.
    gateway = api.app.state.gateway
    result = await gateway.post_notification(
        notification_no=notification_no,
        alarm_no="INTRUDER",
        role="primary",
        contact={"name": "冒名联系人", "channel": "sms"},
        content_hash="f" * 64,
        payload={"content": "冒名正文"},
    )
    assert result.outcome == "conflict"
    assert result.gateway_status == "identity_conflict"
    held = api.fake.records[notification_no]
    assert held["alarm_no"] == "OWNER"
    assert held["content_hash"] == original["content_hash"]
    assert held["deliveries"] == 1, "conflict must not page again"
    assert held["conflict_requests"] == 1

    # Same identity re-post is still a benign duplicate, exactly once.
    ok = await gateway.post_notification(
        notification_no=notification_no,
        alarm_no=original["alarm_no"],
        role=original["role"],
        contact=original["contact"],
        content_hash=original["content_hash"],
        payload=original["payload"],
    )
    assert ok.outcome in {"accepted", "duplicate"}
    assert api.fake.records[notification_no]["deliveries"] == 1


async def test_backup_notification_conflict_is_disclosed_and_not_reposted(api: ApiHarness):
    """A foreign record squatting the backup notification number must end in a
    terminal conflict too; the conflict never retro-validates an escalation and
    the disputed backup number is not re-posted."""
    # Accept the primary normally so the clock runs and a backup gets enqueued.
    await api.client.post("/alarms", json=api.alarm_payload("CFL-BK", deadline=2))
    assert await api.wait_until(
        lambda: _status(api, "CFL-BK", "escalated"), timeout=8
    ), await _get(api, "CFL-BK")

    view = await api.client.get("/alarms/CFL-BK")
    backup_no = next(
        n["notification_no"] for n in view.json()["notifications"] if n["role"] == "backup"
    )
    # Wait until the backup actually reaches the gateway before mutating it.
    async def _backup_at_gateway() -> bool:
        return backup_no in api.fake.records

    assert await api.wait_until(_backup_at_gateway, timeout=6), "backup never reached the gateway"
    # Once accepted, replace the gateway's backup record with a foreign one and
    # force the API row back to unknown so it must reconcile again.
    held = api.fake.records[backup_no]
    held["alarm_no"] = "FOREIGN-BACKUP"
    held["content_hash"] = "77" * 32
    db = api.app.state.db
    db.execute(
        "UPDATE notifications SET state='unknown', next_attempt_at=0, lease_until=NULL "
        "WHERE notification_no = ?",
        (backup_no,),
    )

    assert await api.wait_until(
        lambda: _has_state(api, "CFL-BK", "conflicted"), timeout=6
    ), await _get(api, "CFL-BK")
    final = (await api.client.get("/alarms/CFL-BK")).json()
    backup = next(n for n in final["notifications"] if n["role"] == "backup")
    assert backup["state"] == "conflicted"
    assert backup["display_status"] == "identity_conflict"
    assert any("备班通知号归属冲突" in w for w in final["warnings"])
    posts = api.fake.post_calls
    await asyncio.sleep(0.8)
    assert api.fake.post_calls == posts, "conflicted backup number was re-posted"
    # The foreign gateway record remains intact.
    assert api.fake.records[backup_no]["alarm_no"] == "FOREIGN-BACKUP"


async def test_gateway_lookup_reports_ownership_verdict(api: ApiHarness):
    """The gateway lookup endpoint explains the conflict explicitly when asked
    with expected identity parameters."""
    first = await api.client.post("/alarms", json=api.alarm_payload("VERDICT", deadline=5))
    notification_no = first.json()["notifications"][0]["notification_no"]
    assert await api.wait_until(lambda: _status(api, "VERDICT", "awaiting_confirmation"))

    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.gateway_app), base_url=api.settings.gateway_url
    )
    try:
        import json as _json

        record = api.fake.records[notification_no]
        # Matching identity (all fields) -> match verdict.
        ok = await client.get(
            f"/notifications/{notification_no}",
            params={
                "alarm_no": "VERDICT",
                "role": "primary",
                "contact": _json.dumps(record["contact"], ensure_ascii=False),
                "content_hash": record["content_hash"],
                "payload": _json.dumps(record["payload"], ensure_ascii=False),
            },
        )
        assert ok.status_code == 200, ok.text
        assert ok.json()["ownership"] == "match"

        # Foreign identity -> conflict verdict with an explanation.
        bad = await client.get(
            f"/notifications/{notification_no}",
            params={
                "alarm_no": "INTRUDER",
                "role": "primary",
                "contact": _json.dumps({"name": "冒名", "channel": "sms"}),
                "content_hash": "9" * 64,
                "payload": _json.dumps({"content": "冒名正文"}),
            },
        )
        assert bad.status_code == 200
        body = bad.json()
        assert body["ownership"] == "conflict"
        assert body["mismatched_field"] in {"alarm_no", "content_hash"}
        assert "conflict" in body["ownership_detail"]
    finally:
        await client.aclose()
