"""Notification ownership verification.

The notification number is the gateway's idempotency key, but a number alone is
not proof that a gateway record belongs to *this* notification. Numbers are
generated independently by each API instance/database, so after a rebuilt
database, a reused number, or a foreign gateway record, the gateway may hold a
record under the same number whose alarm number, role, contact, content
fingerprint or payload is different.

Both the gateway and the API use the same comparison here so that "same number,
different content" is detected identically on both sides:

* the gateway refuses to overwrite or re-acknowledge the foreign record;
* the API never adopts a foreign record as its own acceptance, never starts the
  confirmation clock on it, never escalates because of it, and stops re-posting
  the disputed number once the conflict is known.
"""

from __future__ import annotations

import json
from typing import Any

# Fields that define which logical notification a record belongs to. Every one
# of them must match for a same-number record to count as the same notification.
IDENTITY_FIELDS = ("alarm_no", "role", "contact", "content_hash", "payload")


def _canonical(value: Any) -> str:
    """Normalise a field that may arrive as a dict, a JSON string, or None."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def expected_identity(
    *,
    alarm_no: str,
    role: str,
    contact: Any,
    content_hash: str | None,
    payload: Any,
) -> dict[str, Any]:
    """Build the canonical identity an API-side notification expects."""
    return {
        "alarm_no": alarm_no,
        "role": role,
        "contact": _canonical(contact),
        "content_hash": content_hash,
        "payload": _canonical(payload),
    }


def record_identity(record: dict[str, Any]) -> dict[str, Any]:
    """Extract a comparable identity from a gateway record."""
    return {
        "alarm_no": record.get("alarm_no"),
        "role": record.get("role"),
        "contact": _canonical(record.get("contact")),
        "content_hash": record.get("content_hash"),
        "payload": _canonical(record.get("payload")),
        "payload_present": "payload" in record,
    }


def identity_mismatch(
    expected: dict[str, Any], actual: dict[str, Any]
) -> dict[str, str] | None:
    """Return the first mismatching field, or None when the records are the same
    notification.

    A missing field on the gateway side (an older gateway that did not persist
    payloads, for example) is treated as "cannot prove ownership" rather than a
    match: callers must keep the receipt unconfirmed instead of adopting a record
    they cannot verify.
    """
    for field in IDENTITY_FIELDS:
        if field not in actual or actual.get(field) is None:
            return {
                "field": field,
                "expected": str(expected.get(field)),
                "actual": "<missing on gateway record>",
            }
        if _canonical(actual.get(field)) != _canonical(expected.get(field)):
            return {
                "field": field,
                "expected": str(expected.get(field)),
                "actual": str(actual.get(field)),
            }
    return None


def describe_mismatch(mismatch: dict[str, str]) -> str:
    return (
        f"notification number ownership conflict on {mismatch['field']}: "
        f"expected {mismatch['expected']!r}, gateway holds {mismatch['actual']!r}"
    )
