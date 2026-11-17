"""Local notification gateway used to exercise alarm escalation end to end.

It behaves like a real paging gateway:

* ``POST /notifications`` accepts a notification idempotently on
  ``notification_no``, but only when the whole notification identity matches
  (alarm number, role, contact, content hash and payload). Re-posting the same
  number with the same content never pages twice; a same-number request whose
  identity differs is rejected with ``409 identity_conflict`` and the stored
  record is never overwritten or paged again.
* ``GET /notifications/{no}`` reports the authoritative receipt, which lets the
  API resolve a lost acknowledgment without resending. Supplying the expected
  identity adds an ``ownership`` verdict (``match``/``conflict``) so a foreign
  same-number record can never be mistaken for the caller's receipt.
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

from app.identity import (
    describe_mismatch,
    expected_identity,
    identity_mismatch,
    record_identity,
)


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

    def deliver(self, payload: NotificationIn) -> tuple[str, dict[str, Any]]:
        """Idempotent on notification number *with ownership verification*.

        Returns ``(outcome, record)`` where outcome is one of:

        * ``created``  — the number was unknown and has now been recorded;
        * ``duplicate`` — the number exists and belongs to the same notification
          (identical alarm number, role, contact, content hash and payload);
        * ``conflict``  — the number exists but belongs to a different
          notification. The stored record is never overwritten or paged again.
        """
        existing = self.records.get(payload.notification_no)
        if existing is not None:
            mismatch = identity_mismatch(
                expected_identity(
                    alarm_no=payload.alarm_no,
                    role=payload.role,
                    contact=payload.contact,
                    content_hash=payload.content_hash,
                    payload=payload.payload,
                ),
                record_identity(existing),
            )
            if mismatch is not None:
                existing["conflict_requests"] = existing.get("conflict_requests", 0) + 1
                existing.setdefault("conflicts", []).append(
                    {
                        "at": _now(),
                        "alarm_no": payload.alarm_no,
                        "role": payload.role,
                        "content_hash": payload.content_hash,
                        "mismatched_field": mismatch["field"],
                    }
                )
                self._save()
                return "conflict", existing
            existing["duplicate_requests"] += 1
            self._save()
            return "duplicate", existing
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
            "conflict_requests": 0,
            "accepted_at": _now(),
        }
        self.records[payload.notification_no] = record
        self._save()
        return "created", record


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
    description=(
        "可按通知号去重、可设置慢响应/丢回执的本地通知网关。\n\n"
        "同号且身份（告警号、角色、联系人、内容指纹、正文）完全一致才视为重复；"
        "同号异内容返回 409 identity_conflict 且不覆盖原记录。"
        "GET 查询携带期望身份时返回 ownership=match/conflict。"
    ),
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
        # A conflicting re-post never reaches here: conflicts are decided before
        # the receipt-drop simulation and answered immediately.
        await asyncio.sleep(config.drop_hold_ms / 1000)
        response.status_code = status.HTTP_504_GATEWAY_TIMEOUT
        return {"status": "receipt_lost"}

    if created == "conflict":
        # Same number, different notification. The original record is untouched
        # and must not be paged again; tell the caller precisely what differs.
        mismatch = identity_mismatch(
            expected_identity(
                alarm_no=payload.alarm_no,
                role=payload.role,
                contact=payload.contact,
                content_hash=payload.content_hash,
                payload=payload.payload,
            ),
            record_identity(record),
        )
        response.status_code = status.HTTP_409_CONFLICT
        return {
            "status": "identity_conflict",
            "notification_no": record["notification_no"],
            "detail": describe_mismatch(mismatch),
            "mismatched_field": mismatch["field"] if mismatch else None,
            "held": {
                "alarm_no": record.get("alarm_no"),
                "role": record.get("role"),
                "contact": record.get("contact"),
                "content_hash": record.get("content_hash"),
                "payload": record.get("payload"),
            },
            "received": {
                "alarm_no": payload.alarm_no,
                "role": payload.role,
                "contact": payload.contact,
                "content_hash": payload.content_hash,
                "payload": payload.payload,
            },
        }

    if created == "duplicate":
        response.status_code = status.HTTP_200_OK
        return {
            "status": "duplicate",
            "notification_no": record["notification_no"],
            "accepted_at": record["accepted_at"],
            "deliveries": record["deliveries"],
            "alarm_no": record["alarm_no"],
            "role": record["role"],
            "contact": record["contact"],
            "content_hash": record["content_hash"],
            "payload": record["payload"],
        }
    return {
        "status": "accepted",
        "notification_no": record["notification_no"],
        "accepted_at": record["accepted_at"],
        "deliveries": record["deliveries"],
    }


@app.get("/notifications/{notification_no}")
async def lookup(
    notification_no: str,
    alarm_no: str | None = None,
    role: str | None = None,
    contact: str | None = None,
    content_hash: str | None = None,
    payload: str | None = None,
) -> dict[str, Any]:
    """Report the authoritative record for a notification number.

    When the caller supplies its expected identity (the API always does), the
    response additionally carries an ``ownership`` verdict:

    * ``match``    — the stored record is this exact notification;
    * ``conflict`` — the same number is held by a different notification. The
      record is returned unchanged so the caller can disclose the clash, but it
      must not adopt its acceptance or re-post the number.

    Without identity parameters the endpoint behaves exactly as before.
    """
    record = state().records.get(notification_no)
    if record is None:
        raise HTTPException(status_code=404, detail="unknown notification")

    if alarm_no is None and role is None and content_hash is None and contact is None and payload is None:
        return record

    import json as _json

    try:
        expected_contact = _json.loads(contact) if contact is not None else None
        expected_payload = _json.loads(payload) if payload is not None else {}
    except ValueError:
        raise HTTPException(status_code=400, detail="contact/payload must be JSON-encoded") from None

    expected = expected_identity(
        alarm_no=alarm_no or "",
        role=role or "",
        contact=expected_contact,
        content_hash=content_hash,
        payload=expected_payload,
    )
    mismatch = identity_mismatch(expected, record_identity(record))
    if mismatch is None:
        return {**record, "ownership": "match"}
    return {
        **record,
        "ownership": "conflict",
        "ownership_detail": describe_mismatch(mismatch),
        "mismatched_field": mismatch["field"],
    }


@app.get("/notifications")
def list_notifications(role: str | None = None, alarm_no: str | None = None) -> dict[str, Any]:
    records = list(state().records.values())
    if role:
        records = [r for r in records if r["role"] == role]
    if alarm_no:
        records = [r for r in records if r["alarm_no"] == alarm_no]
    return {"items": records, "total": len(records)}
