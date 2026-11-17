"""Behaviour when the gateway is unreachable."""

from __future__ import annotations

import asyncio

import httpx

from tests.conftest import ApiHarness
from tests.helpers import _get


async def test_gateway_down_leaves_receipt_unknown_and_never_escalates(api: ApiHarness):
    """A connection failure is not an acceptance. The alarm must stay in an
    unconfirmed state, no timer may start, and no escalation may fire."""

    async def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    api.app.state.gateway._client = httpx.AsyncClient(
        transport=httpx.MockTransport(refuse), base_url=api.settings.gateway_url
    )

    submit = await api.client.post("/alarms", json=api.alarm_payload("DOWN", deadline=2))
    assert submit.status_code == 201

    await asyncio.sleep(2.6)  # past the nominal deadline
    view = (await api.client.get("/alarms/DOWN")).json()
    assert view["status"] == "primary_unconfirmed"
    assert view["primary_accepted_at"] is None
    assert view["confirmation_due_at"] is None
    assert view["escalated"] is False
    assert view["notifications"][0]["state"] == "unknown"
    assert view["notifications"][0]["display_status"] == "receipt_unconfirmed"
    assert view["notifications"][0]["attempts"] >= 1

    # Confirming an unaccepted alarm is still allowed and still blocks escalation.
    confirmed = await api.client.post("/alarms/DOWN/confirm", json={"confirmed_by": "张工"})
    assert confirmed.status_code == 200
    assert confirmed.json()["confirmed"] is True


async def test_gateway_recovering_from_outage_delivers_once(api: ApiHarness):
    """Once the gateway comes back, the queued notification is delivered exactly
    once and the timer starts from that acceptance."""
    gateway_client = api.app.state.gateway

    async def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    real_transport = gateway_client._client._transport
    gateway_client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(refuse), base_url=api.settings.gateway_url
    )

    await api.client.post("/alarms", json=api.alarm_payload("RECOVER", deadline=3))
    assert await api.wait_until(
        lambda: _state(api, "RECOVER", "unknown"), timeout=5
    ), await _get(api, "RECOVER")

    # Gateway comes back.
    gateway_client._client = httpx.AsyncClient(
        transport=real_transport, base_url=api.settings.gateway_url
    )
    assert await api.wait_until(
        lambda: _accepted(api, "RECOVER"), timeout=8
    ), await _get(api, "RECOVER")
    view = (await api.client.get("/alarms/RECOVER")).json()
    assert len(api.fake.role_notifications("primary")) == 1
    assert view["primary_accepted_at"] is not None


async def _state(api: ApiHarness, alarm_no: str, state: str) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    if response.status_code != 200:
        return False
    return response.json()["notifications"][0]["state"] == state


async def _accepted(api: ApiHarness, alarm_no: str) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    if response.status_code != 200:
        return False
    return response.json()["primary_accepted_at"] is not None
