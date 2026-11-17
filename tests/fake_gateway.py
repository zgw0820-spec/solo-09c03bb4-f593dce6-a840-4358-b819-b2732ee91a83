"""In-process fake notification gateway with failure injection."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Response, status


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class FakeGateway:
    # Keyed by notification_no, the gateway's dedupe key.
    records: dict[str, dict[str, Any]] = field(default_factory=dict)
    deliveries: list[str] = field(default_factory=list)
    # Delay before acceptance, simulating a slow gateway.
    slow_seconds: float = 0.0
    # Accept, persist, but then make the caller wait past its timeout and drop
    # the reply, so the caller's receipt is lost.
    drop_receipt: bool = False
    drop_hold_seconds: float = 0.4
    reject: bool = False
    # When true, the lookup endpoint is unreachable, so an ambiguous receipt
    # cannot be resolved and must stay unknown without escalating.
    lookup_down: bool = False
    # Namespace used to inject errors per role in specific tests.
    fail_roles: set[str] = field(default_factory=set)
    post_calls: int = 0
    lookups: list[str] = field(default_factory=list)

    def deliver(self, payload: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        no = payload["notification_no"]
        existing = self.records.get(no)
        if existing is not None:
            existing["duplicate_requests"] += 1
            return False, existing
        record = {
            "notification_no": no,
            "alarm_no": payload["alarm_no"],
            "role": payload["role"],
            "contact": payload["contact"],
            "content_hash": payload.get("content_hash"),
            "payload": payload.get("payload", {}),
            "status": "accepted",
            "deliveries": 1,
            "duplicate_requests": 0,
            "accepted_at": _now_iso(),
        }
        self.records[no] = record
        self.deliveries.append(no)
        return True, record

    def role_notifications(self, role: str) -> list[dict[str, Any]]:
        return [r for r in self.records.values() if r["role"] == role]


def build_fake_gateway_app(fake: FakeGateway) -> FastAPI:
    app = FastAPI()

    @app.post("/notifications", status_code=status.HTTP_202_ACCEPTED)
    async def accept(payload: dict[str, Any], response: Response) -> dict[str, Any]:
        fake.post_calls += 1
        if fake.reject or payload.get("role") in fake.fail_roles:
            raise HTTPException(status_code=422, detail="rejected by fake gateway")
        if fake.slow_seconds:
            await asyncio.sleep(fake.slow_seconds)
        created, record = fake.deliver(payload)
        if fake.drop_receipt:
            # Acceptance already happened; the caller just never hears about it.
            await asyncio.sleep(fake.drop_hold_seconds)
            response.status_code = status.HTTP_504_GATEWAY_TIMEOUT
            return {"status": "receipt_lost"}
        if not created:
            response.status_code = status.HTTP_200_OK
            return {"status": "duplicate", "notification_no": record["notification_no"]}
        return {"status": "accepted", "notification_no": record["notification_no"]}

    @app.get("/notifications/{notification_no}")
    async def lookup(notification_no: str) -> dict[str, Any]:
        fake.lookups.append(notification_no)
        if fake.lookup_down:
            raise HTTPException(status_code=503, detail="lookup unavailable")
        record = fake.records.get(notification_no)
        if record is None:
            raise HTTPException(status_code=404, detail="unknown notification")
        return record

    @app.get("/notifications")
    def listing() -> dict[str, Any]:
        return {"items": list(fake.records.values())}

    return app
