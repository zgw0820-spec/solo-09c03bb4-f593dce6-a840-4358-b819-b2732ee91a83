"""Alarm escalation API.

Run with: ``uvicorn app.main:app``
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request, Response, status

from . import __version__
from .config import Settings
from .db import Database, parse_ts, utc_now
from .gateway_client import GatewayClient
from .models import (
    AlarmCreate,
    AlarmView,
    ConfirmRequest,
    HealthView,
    NotificationList,
    NotificationView,
)
from .service import AlarmService, ConflictError, NotFoundError
from .worker import Dispatcher

log = logging.getLogger("alarm")


def _notification_list_view(alarm_view: dict) -> list[dict]:
    return alarm_view["notifications"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings: Settings = app.state.settings
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    db = Database(settings.database_path)
    service = AlarmService(db)
    gateway = GatewayClient(
        settings.gateway_url,
        post_timeout=settings.post_timeout_seconds,
        lookup_timeout=settings.lookup_timeout_seconds,
    )
    dispatcher = Dispatcher(
        db,
        service,
        gateway,
        interval=settings.worker_interval_seconds,
        lease_seconds=settings.delivery_lease_seconds,
        max_backoff_seconds=settings.max_backoff_seconds,
        max_concurrency=settings.delivery_concurrency,
        not_found_misses_before_repost=settings.not_found_misses_before_repost,
    )
    app.state.db = db
    app.state.service = service
    app.state.gateway = gateway
    app.state.dispatcher = dispatcher
    if settings.recover_inflight_on_startup:
        recovered = dispatcher.recover_interrupted()
        if recovered:
            log.info("recovered %d interrupted notification(s) after restart", recovered)
    await dispatcher.start()
    try:
        yield
    finally:
        await dispatcher.stop()
        await gateway.aclose()
        db.close()


def create_app(settings: Settings | None = None) -> FastAPI:
    app = FastAPI(
        title="机房告警升级 API",
        version=__version__,
        description=(
            "供电柜过热告警的升级通知 API。\n\n"
            "**流程**：提交告警（主值班员通知）→ 网关确已受理后开始计入确认期限 → "
            "逾期未确认仅投递一次备班通知；确认后不再创建新的备班投递。\n\n"
            "**关键语义**\n"
            "- 同一告警号 + 相同内容的重试幂等复用原告警；内容不一致返回 409。\n"
            "- 主通知只有拿到网关的受理确认才开始计时；回执不明标记为 "
            "`receipt_unconfirmed`，查明前绝不触发升级。\n"
            "- 通知号是网关幂等键；API 重启后按数据库中的待办继续投递/查询。\n"
            "- 确认与到期处理并发时，确认优先且不新建备班投递；已在途通知通过网关查询后如实显示。"
        ),
        lifespan=lifespan,
    )
    app.state.settings = settings or Settings()

    @app.get("/health", response_model=HealthView, tags=["运维"], summary="健康检查")
    def health(request: Request) -> HealthView:
        db: Database = request.app.state.db
        try:
            db.query_one("SELECT 1 AS ok")
            db_state = "ok"
        except Exception as exc:  # noqa: BLE001 - health must always answer
            db_state = f"error: {exc}"
        return HealthView(
            status="ok" if db_state == "ok" else "degraded",
            database=db_state,
            gateway_url=request.app.state.settings.gateway_url,
        )

    @app.post(
        "/alarms",
        response_model=AlarmView,
        status_code=status.HTTP_201_CREATED,
        tags=["告警"],
        summary="提交告警并通知主值班员",
        responses={
            200: {"description": "相同告警号与内容的幂等重试，返回已存在的告警"},
            409: {"description": "告警号已存在但内容不同"},
        },
    )
    async def submit_alarm(payload: AlarmCreate, request: Request, response: Response) -> dict:
        service: AlarmService = request.app.state.service
        try:
            view, deduplicated = service.submit_alarm(payload)
        except ConflictError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        view["deduplicated"] = deduplicated
        if deduplicated:
            response.status_code = status.HTTP_200_OK
        return view

    async def _reconcile(request: Request, alarm_no: str | None = None) -> None:
        """Resolve ambiguous receipts before rendering a status.

        This performs gateway lookups (bounded by the lookup timeout) but no
        posts. It only ever touches unresolved notifications, and gateway calls
        happen outside database transactions, so a slow lookup cannot block a
        confirmation for another alarm.
        """
        settings: Settings = request.app.state.settings
        if not settings.reconcile_on_read:
            return
        dispatcher: Dispatcher = request.app.state.dispatcher
        db: Database = request.app.state.db
        try:
            if alarm_no is None:
                await dispatcher.reconcile_unknown(limit=10)
            else:
                rows = db.query(
                    "SELECT 1 FROM notifications WHERE alarm_no = ? AND state = 'unknown' LIMIT 1",
                    (alarm_no,),
                )
                if rows:
                    await dispatcher.reconcile_unknown(limit=10, alarm_no=alarm_no)
        except Exception:  # status rendering must still work if gateway is down
            log.warning("reconciliation during read failed", exc_info=True)

    @app.get(
        "/alarms/{alarm_no}",
        response_model=AlarmView,
        tags=["告警"],
        summary="查询告警、通知与确认状态",
        description=(
            "返回主/备通知的网关受理状态与已确认结果。若主通知回执不明，会先向网关"
            "查询受理结果；查明前 `status` 为 `primary_unconfirmed` 且计时未开始。"
        ),
    )
    async def get_alarm(alarm_no: str, request: Request) -> dict:
        service: AlarmService = request.app.state.service
        await _reconcile(request, alarm_no)
        try:
            return service.snapshot(alarm_no, now_real=parse_ts(utc_now()))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=f"alarm {alarm_no!r} not found") from exc

    @app.post(
        "/alarms/{alarm_no}/confirm",
        response_model=AlarmView,
        tags=["告警"],
        summary="确认告警",
        description=(
            "确认后不再创建新的备班投递。若到期处理已生成备班通知但尚未发送，会在同一"
            "事务内取消；若备班通知已在途，则不会撤回，并在查询结果中如实显示其最终受理情况。"
        ),
        responses={404: {"description": "告警不存在"}},
    )
    async def confirm_alarm(alarm_no: str, payload: ConfirmRequest, request: Request) -> dict:
        service: AlarmService = request.app.state.service
        try:
            view = service.confirm(alarm_no, payload.confirmed_by, payload.note)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=f"alarm {alarm_no!r} not found") from exc
        # Deliberately no gateway reconciliation here: confirmation must stay
        # fast and must not wait on a slow notification gateway.
        return view

    @app.get(
        "/alarms/{alarm_no}/notifications",
        response_model=NotificationList,
        tags=["告警"],
        summary="查询该告警的通知投递明细",
    )
    async def list_notifications(alarm_no: str, request: Request) -> NotificationList:
        service: AlarmService = request.app.state.service
        await _reconcile(request, alarm_no)
        try:
            view = service.snapshot(alarm_no, now_real=parse_ts(utc_now()))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=f"alarm {alarm_no!r} not found") from exc
        items = [NotificationView(**item) for item in _notification_list_view(view)]
        return NotificationList(items=items, total=len(items))

    @app.get(
        "/notifications/{notification_no}",
        response_model=NotificationView,
        tags=["通知"],
        summary="按通知号查询网关受理结果",
        description="只返回本 API 已持久化的通知；回执不明时会先向网关查询再展示。",
    )
    async def get_notification(notification_no: str, request: Request) -> NotificationView:
        db: Database = request.app.state.db
        row = db.query_one(
            "SELECT alarm_no FROM notifications WHERE notification_no = ?", (notification_no,)
        )
        if row is None:
            raise HTTPException(status_code=404, detail=f"notification {notification_no!r} not found")
        await _reconcile(request, row["alarm_no"])
        service: AlarmService = request.app.state.service
        view = service.snapshot(row["alarm_no"], now_real=parse_ts(utc_now()))
        for item in view["notifications"]:
            if item["notification_no"] == notification_no:
                return NotificationView(**item)
        raise HTTPException(status_code=404, detail=f"notification {notification_no!r} not found")

    @app.get(
        "/alarms",
        tags=["运维"],
        summary="列出告警（按创建时间倒序）",
    )
    async def list_alarms(
        request: Request,
        limit: int = Query(default=50, ge=1, le=200),
        status_filter: str | None = Query(default=None, alias="status"),
    ) -> dict:
        db: Database = request.app.state.db
        rows = db.query(
            "SELECT alarm_no FROM alarms ORDER BY created_real DESC LIMIT ?", (limit,)
        )
        service: AlarmService = request.app.state.service
        now_real = parse_ts(utc_now())
        views = [service.snapshot(r["alarm_no"], now_real=now_real) for r in rows]
        if status_filter:
            views = [v for v in views if v["status"] == status_filter]
        return {"items": views, "total": len(views)}

    return app


app = create_app()
