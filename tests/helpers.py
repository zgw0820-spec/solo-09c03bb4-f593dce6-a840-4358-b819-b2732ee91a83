"""Test helper accessors."""

from __future__ import annotations

from typing import Any


async def _get(api, alarm_no: str) -> dict[str, Any]:
    response = await api.client.get(f"/alarms/{alarm_no}")
    return response.json()


async def _status(api, alarm_no: str, expected: str) -> bool:
    response = await api.client.get(f"/alarms/{alarm_no}")
    if response.status_code != 200:
        return False
    return response.json()["status"] == expected
