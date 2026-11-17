"""HTTP client for the local notification gateway.

Two operations are needed:

* ``post_notification`` — ask the gateway to accept a notification. It is
  idempotent per notification number, so re-posting after a timeout is safe.
* ``lookup`` — ask whether a notification number was accepted. This is what
  resolves ambiguous outcomes without ever sending a duplicate page.

The client never raises on transport trouble; it returns an outcome object so
callers can persist an ``unknown`` state instead of guessing.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import httpx

from .identity import (
    describe_mismatch,
    expected_identity,
    identity_mismatch,
    record_identity,
)


def _exc_detail(exc: BaseException) -> str:
    message = str(exc).strip()
    return f"{type(exc).__name__}: {message}" if message else type(exc).__name__


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@dataclass
class GatewayResult:
    # accepted | duplicate | failed | unknown | conflict
    outcome: str
    gateway_status: str | None = None
    detail: str | None = None
    http_status: int | None = None
    raw: dict[str, Any] | None = None

    @property
    def confirmed_accepted(self) -> bool:
        # A conflict is never an acceptance of *this* notification.
        return self.outcome in {"accepted", "duplicate"}


class GatewayClient:
    def __init__(
        self,
        base_url: str,
        post_timeout: float = 2.0,
        lookup_timeout: float = 1.0,
        client: httpx.AsyncClient | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.post_timeout = post_timeout
        self.lookup_timeout = lookup_timeout
        self._client = client
        self._owns_client = client is None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(base_url=self.base_url, timeout=self.post_timeout)
        return self._client

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def post_notification(
        self,
        notification_no: str,
        alarm_no: str,
        role: str,
        contact: str,
        content_hash: str,
        payload: dict[str, Any],
    ) -> GatewayResult:
        body = {
            "notification_no": notification_no,
            "alarm_no": alarm_no,
            "role": role,
            "contact": contact,
            "content_hash": content_hash,
            "payload": payload,
        }
        expected = expected_identity(
            alarm_no=alarm_no,
            role=role,
            contact=contact,
            content_hash=content_hash,
            payload=payload,
        )
        try:
            response = await self._get_client().post(
                "/notifications", json=body, timeout=self.post_timeout
            )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            # Outcome genuinely unknown: the gateway may or may not have
            # accepted before the connection broke.
            return GatewayResult(outcome="unknown", detail=_exc_detail(exc))
        except Exception as exc:  # noqa: BLE001 - a client bug must not kill dispatch
            # Any other client-side failure is also an unknown outcome, never an
            # implicit "delivered". Reported as such rather than raised.
            return GatewayResult(outcome="unknown", detail=_exc_detail(exc))

        try:
            data = response.json()
        except ValueError:
            data = {}

        gateway_status = None
        if isinstance(data, dict):
            gateway_status = data.get("status")
        if response.status_code == 202 and gateway_status == "accepted":
            return GatewayResult("accepted", "accepted", None, response.status_code, data)
        if response.status_code == 409 or gateway_status == "identity_conflict":
            # Gateway already holds this number for a different notification.
            return GatewayResult(
                "conflict",
                "identity_conflict",
                data.get("detail") if isinstance(data, dict) else "gateway identity conflict",
                response.status_code,
                data,
            )
        if response.status_code == 200 and gateway_status == "duplicate":
            # A duplicate reply is only trustworthy if the record it describes is
            # really ours. An older gateway may omit identity fields; in that
            # case verify with a lookup rather than assuming ownership.
            if isinstance(data, dict) and self._has_identity_fields(data):
                mismatch = identity_mismatch(expected, record_identity(data))
                if mismatch is not None:
                    return self._conflict_result(mismatch, response.status_code, data)
            else:
                lookup_result = await self.lookup(
                    notification_no,
                    alarm_no=alarm_no,
                    role=role,
                    contact=contact,
                    content_hash=content_hash,
                    payload=payload,
                )
                if lookup_result.outcome == "conflict":
                    return lookup_result
                if lookup_result.outcome == "accepted":
                    # Verified same notification: treat the duplicate as accepted.
                    return GatewayResult(
                        "duplicate",
                        "duplicate",
                        "verified via gateway lookup",
                        response.status_code,
                        lookup_result.raw,
                    )
                # Anything else (not found, unknown) keeps the outcome ambiguous:
                # never adopt an unverifiable duplicate as an acceptance.
                return GatewayResult(
                    "unknown",
                    "duplicate_unverified",
                    "gateway duplicate reply could not be verified as this notification",
                    response.status_code,
                    data,
                )
            return GatewayResult("duplicate", "duplicate", None, response.status_code, data)
        if response.status_code == 307 and gateway_status == "slow_retry":
            # Gateway deliberately took too long; acceptance is not proven.
            return GatewayResult("unknown", "slow_retry", "gateway signalled slow retry", response.status_code, data)
        if 400 <= response.status_code < 500:
            return GatewayResult(
                "failed",
                str(gateway_status) if gateway_status is not None else "rejected",
                f"gateway rejected with HTTP {response.status_code}",
                response.status_code,
                data,
            )
        return GatewayResult(
            "unknown",
            str(gateway_status) if gateway_status is not None else "ambiguous",
            f"ambiguous gateway reply HTTP {response.status_code}",
            response.status_code,
            data,
        )

    @staticmethod
    def _has_identity_fields(data: dict[str, Any]) -> bool:
        return all(field in data for field in ("alarm_no", "role", "contact", "content_hash", "payload"))

    @staticmethod
    def _conflict_result(mismatch: dict[str, str], http_status: int, raw: dict[str, Any]) -> GatewayResult:
        return GatewayResult(
            "conflict",
            "identity_conflict",
            describe_mismatch(mismatch),
            http_status,
            raw,
        )

    async def lookup(
        self,
        notification_no: str,
        *,
        alarm_no: str | None = None,
        role: str | None = None,
        contact: Any = None,
        content_hash: str | None = None,
        payload: Any = None,
    ) -> GatewayResult:
        params: dict[str, str] = {}
        if alarm_no is not None:
            params["alarm_no"] = alarm_no
        if role is not None:
            params["role"] = role
        if contact is not None:
            params["contact"] = json_dumps(contact)
        if content_hash is not None:
            params["content_hash"] = content_hash
        if payload is not None:
            params["payload"] = json_dumps(payload)
        try:
            response = await self._get_client().get(
                f"/notifications/{notification_no}",
                params=params or None,
                timeout=self.lookup_timeout,
            )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            return GatewayResult(outcome="unknown", detail=_exc_detail(exc))
        except Exception as exc:  # noqa: BLE001 - never let a lookup crash a worker
            return GatewayResult(outcome="unknown", detail=_exc_detail(exc))

        try:
            data = response.json()
        except ValueError:
            data = {}

        if response.status_code == 404:
            # Gateway has no record. It is safe to (re)post: the gateway
            # deduplicates on notification number.
            return GatewayResult("failed", "not_found", "gateway has no record", 404, data)
        if response.status_code != 200 or not isinstance(data, dict):
            return GatewayResult("unknown", None, f"lookup HTTP {response.status_code}", response.status_code, data)

        # An explicit ownership verdict comes from gateways that implement the
        # verification lookup. A 409-style conflict is surfaced directly.
        if data.get("ownership") == "conflict":
            return GatewayResult(
                "conflict",
                "identity_conflict",
                data.get("ownership_detail") or "gateway record belongs to a different notification",
                response.status_code,
                data,
            )

        status = data.get("status")
        if status not in {"accepted", "rejected", "processing", "in_progress"}:
            return GatewayResult("unknown", str(status), "unrecognised gateway status", 200, data)
        if status == "in_progress":
            return GatewayResult("unknown", "in_progress", "gateway still processing request", 200, data)

        # When the caller knows which notification it is asking about, the
        # gateway's record must describe that same notification. A same-number
        # record with different ownership is a conflict, not a receipt.
        if alarm_no is not None:
            expected = expected_identity(
                alarm_no=alarm_no,
                role=role or "",
                contact=contact,
                content_hash=content_hash,
                payload=payload if payload is not None else {},
            )
            if self._has_identity_fields(data):
                mismatch = identity_mismatch(expected, record_identity(data))
                if mismatch is not None:
                    return self._conflict_result(mismatch, response.status_code, data)
            elif data.get("ownership") != "match":
                # An older gateway without identity fields and without an
                # ownership verdict cannot prove the record is ours.
                return GatewayResult(
                    "unknown",
                    "unverifiable",
                    "gateway record lacks identity fields; ownership cannot be proven",
                    response.status_code,
                    data,
                )

        if status == "rejected":
            return GatewayResult("failed", "rejected", data.get("detail"), 200, data)
        # "accepted" and "processing" both mean the gateway confirmed acceptance.
        return GatewayResult("accepted", status if status == "processing" else "accepted", None, 200, data)
