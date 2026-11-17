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

from dataclasses import dataclass
from typing import Any

import httpx


def _exc_detail(exc: BaseException) -> str:
    message = str(exc).strip()
    return f"{type(exc).__name__}: {message}" if message else type(exc).__name__


@dataclass
class GatewayResult:
    # accepted | duplicate | failed | unknown
    outcome: str
    gateway_status: str | None = None
    detail: str | None = None
    http_status: int | None = None
    raw: dict[str, Any] | None = None

    @property
    def confirmed_accepted(self) -> bool:
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
        if response.status_code == 200 and gateway_status == "duplicate":
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

    async def lookup(self, notification_no: str) -> GatewayResult:
        try:
            response = await self._get_client().get(
                f"/notifications/{notification_no}", timeout=self.lookup_timeout
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

        status = data.get("status")
        if status == "accepted":
            return GatewayResult("accepted", "accepted", None, 200, data)
        if status == "rejected":
            return GatewayResult("failed", "rejected", data.get("detail"), 200, data)
        if status == "processing":
            # Accepted by the gateway but the simulated delivery has not been
            # acknowledged yet: that is still a confirmed acceptance.
            return GatewayResult("accepted", "processing", None, 200, data)
        if status == "in_progress":
            return GatewayResult("unknown", "in_progress", "gateway still processing request", 200, data)
        return GatewayResult("unknown", str(status), "unrecognised gateway status", 200, data)
