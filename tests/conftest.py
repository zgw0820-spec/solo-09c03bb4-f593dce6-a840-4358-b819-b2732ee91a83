"""Shared fixtures: an in-process fake gateway and a live API app."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Self

import httpx
import pytest
from fastapi import FastAPI

from app.config import Settings
from app.main import create_app
from tests.fake_gateway import FakeGateway, build_fake_gateway_app


@pytest.fixture
def fake_gateway() -> FakeGateway:
    return FakeGateway()


@pytest.fixture
def gateway_app(fake_gateway: FakeGateway) -> FastAPI:
    return build_fake_gateway_app(fake_gateway)


def make_settings(tmp_path, gateway_url: str = "http://gateway.test") -> Settings:
    return Settings(
        database_path=str(tmp_path / "alarms.db"),
        gateway_url=gateway_url,
        post_timeout_seconds=1.0,
        lookup_timeout_seconds=0.5,
        worker_interval_seconds=0.02,
        delivery_lease_seconds=2.0,
        max_backoff_seconds=0.05,
        reconcile_on_read=True,
    )


class ApiHarness:
    """Run the real API app against an in-process gateway transport."""

    def __init__(self, tmp_path, fake: FakeGateway):
        self.fake = fake
        self.gateway_app = build_fake_gateway_app(fake)
        self.settings = make_settings(tmp_path)
        self.app = create_app(self.settings)
        self._gateway_transport = httpx.ASGITransport(app=self.gateway_app)

    async def __aenter__(self) -> Self:
        self._lifespan = self.app.router.lifespan_context(self.app)
        await self._lifespan.__aenter__()
        # Replace the gateway client's HTTP client with one wired to the
        # in-process fake gateway.
        gateway = self.app.state.gateway
        gateway._client = httpx.AsyncClient(
            transport=self._gateway_transport, base_url=self.settings.gateway_url
        )
        gateway._owns_client = True
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://api.test"
        )
        return self

    async def __aexit__(self, *exc) -> None:
        await self.client.aclose()
        await self._lifespan.__aexit__(*exc)

    async def wait_until(self, predicate, timeout: float = 5.0) -> bool:
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            if await predicate():
                return True
            await asyncio.sleep(0.02)
        return False

    def alarm_payload(self, alarm_no: str = "ALM-1", deadline: int = 2, **overrides: Any) -> dict:
        payload = {
            "alarm_no": alarm_no,
            "content": "供电柜 A07 温度 78°C，超过阈值",
            "metadata": {"cabinet": "A07"},
            "primary_contact": {"name": "主值班员 张工", "channel": "app"},
            "backup_contact": {"name": "备班 李工", "channel": "sms", "address": "+8613800000000"},
            "ack_deadline_seconds": deadline,
        }
        payload.update(overrides)
        return payload


@pytest.fixture
async def api(tmp_path, fake_gateway):
    async with ApiHarness(tmp_path, fake_gateway) as harness:
        yield harness
