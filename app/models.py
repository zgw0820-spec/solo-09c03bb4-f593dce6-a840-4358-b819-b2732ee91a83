"""Request/response models (also drive the OpenAPI/Swagger schema)."""

from __future__ import annotations

import hashlib
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .db import dumps

Channel = Literal["app", "sms", "phone", "email"]


class Contact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200, description="Display name of the on-duty person")
    channel: Channel = Field(default="app", description="How the gateway should page this contact")
    address: str | None = Field(default=None, max_length=200, description="Optional address (phone / email)")


class AlarmCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "alarm_no": "PWR-CAB-07-20261116-001",
                "content": "供电柜 A07 温度 78°C，超过 70°C 告警阈值",
                "metadata": {"cabinet": "A07", "temperature_c": 78},
                "primary_contact": {"name": "主值班员 张工", "channel": "app"},
                "backup_contact": {"name": "备班 李工", "channel": "sms", "address": "+8613800000000"},
                "ack_deadline_seconds": 10,
            }
        },
    )

    alarm_no: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
        description="Alarm number; it is the idempotency key for submissions",
    )
    content: str = Field(min_length=1, max_length=4000, description="Alarm description")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Optional structured alarm details")
    primary_contact: Contact
    backup_contact: Contact
    ack_deadline_seconds: int = Field(
        ge=2,
        le=30,
        description="Seconds the primary has to confirm, counted from confirmed gateway acceptance",
    )

    def content_hash(self) -> str:
        """Fingerprint of everything that defines this alarm submission.

        Retrying the same alarm number with an identical fingerprint reuses the
        original alarm; any difference is rejected with 409.
        """
        canonical = dumps(
            {
                "content": self.content,
                "metadata": self.metadata,
                "primary_contact": self.primary_contact.model_dump(mode="json"),
                "backup_contact": self.backup_contact.model_dump(mode="json"),
                "ack_deadline_seconds": self.ack_deadline_seconds,
            }
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class ConfirmRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"example": {"confirmed_by": "主值班员 张工", "note": "已到现场处理"}},
    )

    confirmed_by: str = Field(default="primary", min_length=1, max_length=200)
    note: str | None = Field(default=None, max_length=500)


class NotificationView(BaseModel):
    notification_no: str
    role: Literal["primary", "backup"]
    contact: Contact
    state: str = Field(description="pending | in_flight | unknown | accepted | failed | cancelled")
    display_status: str = Field(
        description="Operator-facing state: queued | sending | receipt_unconfirmed | accepted | rejected | cancelled_before_dispatch"
    )
    display_text: str = Field(description="Human-readable Chinese status label")
    gateway_status: str | None = None
    detail: str | None = None
    attempts: int
    created_at: str
    accepted_at: str | None = None
    last_attempt_at: str | None = None
    history: list[dict[str, Any]] = Field(default_factory=list)
    delivered_after_confirmation: bool = False


class AlarmView(BaseModel):
    alarm_no: str
    status: str = Field(
        description=(
            "primary_queued | primary_sending | primary_unconfirmed | primary_rejected | "
            "awaiting_confirmation | escalated | confirmed | confirmed_after_escalation"
        )
    )
    status_text: str = Field(description="Human-readable Chinese status label")
    content: str
    metadata: dict[str, Any]
    primary_contact: Contact
    backup_contact: Contact
    ack_deadline_seconds: int
    created_at: str
    # Timers are only meaningful once the gateway has confirmed acceptance.
    primary_accepted_at: str | None = None
    confirmation_due_at: str | None = Field(
        default=None, description="Accepted time + ack deadline; the escalation timer starts here"
    )
    seconds_until_escalation: float | None = Field(
        default=None, description="Remaining seconds before backup escalation, if the timer is running"
    )
    confirmed: bool
    confirmed_at: str | None = None
    confirmed_by: str | None = None
    escalated: bool
    escalated_at: str | None = None
    notifications: list[NotificationView] = Field(default_factory=list)
    events: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    deduplicated: bool = Field(default=False, description="True when a retry reused an existing alarm")


class NotificationList(BaseModel):
    items: list[NotificationView]
    total: int


class HealthView(BaseModel):
    status: str
    database: str
    gateway_url: str
