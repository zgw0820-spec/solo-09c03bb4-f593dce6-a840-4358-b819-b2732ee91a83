"""Clock semantics when a receipt is discovered late."""

from __future__ import annotations

import asyncio
import time

from tests.conftest import ApiHarness
from tests.helpers import _get


async def test_late_receipt_discovery_uses_real_acceptance_time(api: ApiHarness):
    """The gateway accepted at T0 but the receipt was lost. The primary is
    entitled to its full deadline measured from *acceptance*, not from the
    moment the API happened to learn about it, so escalation must not be
    postponed. It must still page the backup exactly once."""
    api.fake.drop_receipt = True
    api.fake.drop_hold_seconds = 1.2
    api.fake.lookup_down = True
    await api.client.post("/alarms", json=api.alarm_payload("LATE-RCPT", deadline=2))

    # Wait until the gateway has durably accepted the primary.
    deadline = time.monotonic() + 5
    while not any(
        r["alarm_no"] == "LATE-RCPT" and r["role"] == "primary"
        for r in api.fake.records.values()
    ):
        assert time.monotonic() < deadline, "gateway never accepted the primary"
        await asyncio.sleep(0.02)
    accepted_at = time.time()

    # Keep the lookup down so the receipt stays unknown past the deadline.
    await asyncio.sleep(2.3)
    view = (await api.client.get("/alarms/LATE-RCPT")).json()
    assert view["status"] == "primary_unconfirmed"
    assert view["escalated"] is False, "no escalation while the receipt is unknown"

    # The gateway becomes queryable: the true acceptance (in the past) is
    # adopted, so the alarm is already overdue and escalates promptly.
    api.fake.drop_receipt = False
    api.fake.lookup_down = False
    escalated = await api.wait_until(
        lambda: _escalated(api, "LATE-RCPT"), timeout=8
    )
    assert escalated, await _get(api, "LATE-RCPT")

    view = (await api.client.get("/alarms/LATE-RCPT")).json()
    from datetime import datetime

    accepted_real = datetime.strptime(
        view["primary_accepted_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
    ).timestamp()
    assert abs(accepted_real - accepted_at) < 1.0, "clock starts at gateway acceptance"
    assert view["escalated"] is True
    backups = [n for n in view["notifications"] if n["role"] == "backup"]
    assert len(backups) == 1
    assert await api.wait_until(lambda: _backup_delivered(api), timeout=5), await _get(
        api, "LATE-RCPT"
    )
    assert len(api.fake.role_notifications("backup")) == 1


async def _escalated(api: ApiHarness, alarm_no: str) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    if response.status_code != 200:
        return False
    return response.json()["escalated"] is True


async def _backup_delivered(api: ApiHarness) -> bool:
    return len(api.fake.role_notifications("backup")) == 1
