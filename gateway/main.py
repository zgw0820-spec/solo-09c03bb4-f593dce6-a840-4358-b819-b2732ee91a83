"""Local notification gateway used to exercise alarm escalation end to end.

It behaves like a real paging gateway:

* ``POST /notifications`` accepts a notification idempotently on
  ``notification_no``. Re-posting the same number never pages twice.
* ``GET /notifications/{no}`` reports the authoritative receipt, which lets the
  API resolve a lost acknowledgment without resending.
* Failure modes are configurable at runtime, via env or ``PUT /config``:

  - ``slow_ms``: delay before the notification is accepted. If the caller's
    timeout is shorter, the caller sees an indeterminate outcome.
  - ``drop_receipt``: accept and persist, then never answer (or answer with an
    incomplete reply), so the caller's receipt is lost while the gateway still
    knows the truth.
  - ``reject``: refuse the notification outright.

Deliveries are recorded so tests can assert exactly-once behaviour.
"""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import UTC
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel, Field


class NotificationIn(BaseModel):
    notification_no: str = Field(min_length=1)
    alarm_no: str
    role: str
    contact: Any
    content_hash: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class GatewayConfig(BaseModel):
    slow_ms: int = Field(default=0, ge=0, le=60_000)
    drop_receipt: bool = False
    reject: bool = False
    # How long a dropped receipt waits before the socket is torn down. Long
    # enough that a client with a short timeout gives up first.
    drop_hold_ms: int = Field(default=3_000, ge=0, le=60_000)


class GatewayState:
    def __init__(self, path: str | None = None):
        self.path = path
        self.records: dict[str, dict[str, Any]] = {}
        self.config = GatewayConfig(
            slow_ms=int(os.getenv("GATEWAY_SLOW_MS", "0")),
            drop_receipt=os.getenv("GATEWAY_DROP_RECEIPT", "0").lower() in {"1", "true", "yes"},
            reject=os.getenv("GATEWAY_REJECT", "0").lower() in {"1", "true", "yes"},
        )
        self._load()

    def _load(self) -> None:
        if not self.path or not Path(self.path).exists():
            return
        try:
            data = json.loads(Path(self.path).read_text("utf-8"))
            self.records = data.get("records", {})
        except (OSError, ValueError):
            self.records = {}

    def _save(self) -> None:
        if not self.path:
            return
        target = Path(self.path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        tmp.write_text(json.dumps({"records": self.records}, ensure_ascii=False, indent=2), "utf-8")
        os.replace(tmp, target)

    def deliver(self, payload: NotificationIn) -> tuple[bool, dict[str, Any]]:
        """Return (created, record). Idempotent on notification number."""
        existing = self.records.get(payload.notification_no)
        if existing is not None:
            existing["duplicate_requests"] += 1
            self._save()
            return False, existing
        record = {
            "notification_no": payload.notification_no,
            "alarm_no": payload.alarm_no,
            "role": payload.role,
            "contact": payload.contact,
            "content_hash": payload.content_hash,
            "payload": payload.payload,
            "status": "accepted",
            "deliveries": 1,
            "duplicate_requests": 0,
            "accepted_at": _now(),
        }
        self.records[payload.notification_no] = record
        self._save()
        return True, record


_STATE: GatewayState | None = None


def _now() -> str:
    from datetime import datetime

    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def state() -> GatewayState:
    global _STATE
    if _STATE is None:
        _STATE = GatewayState(os.getenv("GATEWAY_STATE_PATH"))
    return _STATE


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _STATE
    _STATE = GatewayState(os.getenv("GATEWAY_STATE_PATH"))
    yield


app = FastAPI(
    title="本地通知网关（测试用）",
    version="1.0.0",
    description="可按通知号去重、可设置慢响应/丢回执的本地通知网关。",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "recorded": len(state().records), "config": state().config.model_dump()}


@app.get("/config", response_model=GatewayConfig)
def get_config() -> GatewayConfig:
    return state().config


@app.put("/config", response_model=GatewayConfig)
def set_config(config: GatewayConfig) -> GatewayConfig:
    state().config = config
    return config


@app.delete("/admin/reset")
def reset() -> dict[str, Any]:
    """Clear all records and restore defaults. Test convenience."""
    s = state()
    s.records.clear()
    s.config = GatewayConfig()
    s._save()
    return {"status": "reset"}


@app.post("/notifications", status_code=status.HTTP_202_ACCEPTED)
async def accept_notification(payload: NotificationIn, response: Response) -> dict[str, Any]:
    s = state()
    config = s.config

    if config.reject:
        raise HTTPException(status_code=422, detail="gateway configured to reject notifications")

    if config.slow_ms:
        # The delay happens *before* acceptance: a caller that times out cannot
        # know whether acceptance happened, which is exactly the ambiguous case
        # the API must handle without false escalation.
        await asyncio.sleep(config.slow_ms / 1000)

    created, record = s.deliver(payload)

    if config.drop_receipt:
        # The notification is now accepted and durable, but the caller never
        # learns that. Hold the connection past any reasonable client timeout.
        await asyncio.sleep(config.drop_hold_ms / 1000)
        response.status_code = status.HTTP_504_GATEWAY_TIMEOUT
        return {"status": "receipt_lost"}

    if not created:
        response.status_code = status.HTTP_200_OK
        return {
            "status": "duplicate",
            "notification_no": record["notification_no"],
            "accepted_at": record["accepted_at"],
            "deliveries": record["deliveries"],
        }
    return {
        "status": "accepted",
        "notification_no": record["notification_no"],
        "accepted_at": record["accepted_at"],
        "deliveries": record["deliveries"],
    }


@app.get("/notifications/{notification_no}")
async def lookup(notification_no: str) -> dict[str, Any]:
    record = state().records.get(notification_no)
    if record is None:
        raise HTTPException(status_code=404, detail="unknown notification")
    return record


@app.get("/notifications")
def list_notifications(role: str | None = None, alarm_no: str | None = None) -> dict[str, Any]:
    records = list(state().records.values())
    if role:
        records = [r for r in records if r["role"] == role]
    if alarm_no:
        records = [r for r in records if r["alarm_no"] == alarm_no]
    return {"items": records, "total": len(records)}
