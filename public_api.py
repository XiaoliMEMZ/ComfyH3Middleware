from __future__ import annotations

from typing import Any

from aiohttp import web

from .auth import IS_ADMIN_KEY, PRINCIPAL_KEY, SERVICE_KEY
from .http_utils import exception_response, json_error, read_submission
from .service import GatewayService


routes = web.RouteTableDef()


def service(request: web.Request) -> GatewayService:
    return request.app[SERVICE_KEY]


def can_access(request: web.Request, job: dict[str, Any] | None) -> bool:
    return bool(job and (request.get(IS_ADMIN_KEY) or job.get("requested_by") == request.get(PRINCIPAL_KEY)))


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


async def _submission(request: web.Request, force_mode: str | None = None, sync: bool = False, legacy_i2va: bool = False) -> web.Response:
    gateway = service(request)
    job_id: str | None = None
    try:
        job_id, raw, assets = await read_submission(request, gateway.assets)
        timeout = None
        if sync:
            timeout = float(raw.get("timeout_sec") or gateway.settings.sync_timeout)
            if timeout <= 0:
                raise ValueError("timeout_sec must be greater than zero")
        if legacy_i2va and raw.get("mode") in (None, "", "auto"):
            has_last = bool(assets.get("last_frame") or raw.get("last_frame_name"))
            raw["mode"] = "fl2va" if has_last else "i2va"
        job = await gateway.submit(
            job_id,
            raw,
            assets,
            requested_by=request.get(PRINCIPAL_KEY),
            force_mode=force_mode,
        )
        if not sync:
            return web.json_response({"ok": True, "job_id": job["id"], "status": job["status"], "job": job}, status=202)
        try:
            job = await gateway.wait_for_job(job_id, timeout)
        except TimeoutError:
            job = await gateway.get_job(job_id)
            return web.json_response(
                {"ok": True, "job_id": job["id"], "status": job["status"], "job": job, "warning": "Job is still running after the synchronous wait timeout"},
                status=202,
            )
        if job["status"] == "succeeded" and as_bool(raw.get("return_binary")):
            data, content_type, filename = await gateway.get_video(job_id)
            return web.Response(
                body=data,
                content_type=content_type.split(";", 1)[0],
                headers={
                    "Content-Disposition": f'attachment; filename="{filename.replace(chr(34), "_")}"',
                    "X-Job-Id": job_id,
                },
            )
        status = 200 if job["status"] == "succeeded" else 409
        return web.json_response(
            {"ok": job["status"] == "succeeded", "job_id": job["id"], "status": job["status"], "job": job},
            status=status,
        )
    except Exception as exc:
        if job_id and not await gateway.database.get_job(job_id):
            gateway.assets.cleanup_job(job_id)
        return exception_response(exc)


@routes.get("/")
async def root(request: web.Request) -> web.Response:
    return web.json_response(
        {
            "ok": True,
            "service": "h3-middleware",
            "api": "/v1/schema",
            "admin": "/admin",
        }
    )


@routes.get("/health")
async def health(request: web.Request) -> web.Response:
    result = await service(request).health()
    return web.json_response(result, status=200 if result["ok"] else 503)


@routes.get("/v1/schema")
async def schema(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, **service(request).schema()})


@routes.post("/v1/generations")
async def create_generation(request: web.Request) -> web.Response:
    return await _submission(request)


@routes.post("/v1/generations/sync")
async def create_generation_sync(request: web.Request) -> web.Response:
    return await _submission(request, sync=True)


@routes.post("/v1/i2va")
async def legacy_i2va(request: web.Request) -> web.Response:
    return await _submission(request, legacy_i2va=True)


@routes.post("/v1/i2va/generate")
async def legacy_i2va_sync(request: web.Request) -> web.Response:
    return await _submission(request, sync=True, legacy_i2va=True)


@routes.post("/v1/t2v")
async def t2v(request: web.Request) -> web.Response:
    return await _submission(request, force_mode="t2va")


@routes.post("/v1/t2v/generate")
async def t2v_sync(request: web.Request) -> web.Response:
    return await _submission(request, force_mode="t2va", sync=True)


@routes.post("/v1/ref2va")
async def ref2va(request: web.Request) -> web.Response:
    return await _submission(request, force_mode="ref2va")


@routes.post("/v1/ref2va/generate")
async def ref2va_sync(request: web.Request) -> web.Response:
    return await _submission(request, force_mode="ref2va", sync=True)


@routes.get("/v1/jobs")
async def list_jobs(request: web.Request) -> web.Response:
    try:
        limit = int(request.query.get("limit", "100"))
        offset = int(request.query.get("offset", "0"))
        jobs, total = await service(request).list_jobs(
            status=request.query.get("status"),
            mode=request.query.get("mode"),
            upstream_id=request.query.get("upstream_id"),
            group_id=request.query.get("group_id"),
            requested_by=None if request.get(IS_ADMIN_KEY) else request.get(PRINCIPAL_KEY),
            search=request.query.get("search"),
            limit=limit,
            offset=offset,
        )
        return web.json_response({"ok": True, "jobs": jobs, "total": total, "limit": limit, "offset": offset})
    except Exception as exc:
        return exception_response(exc)


@routes.get("/v1/jobs/{job_id}")
async def get_job(request: web.Request) -> web.Response:
    job = await service(request).get_job(request.match_info["job_id"])
    if not job:
        return json_error("Job not found", status=404)
    if not can_access(request, job):
        return json_error("Job not found", status=404)
    return web.json_response({"ok": True, "job": job})


@routes.get("/v1/jobs/{job_id}/progress")
async def get_job_progress(request: web.Request) -> web.Response:
    job = await service(request).get_job(request.match_info["job_id"])
    if not job or not can_access(request, job):
        return json_error("Job not found", status=404)
    return web.json_response(
        {
            "ok": True,
            "job_id": job["id"],
            "status": job["status"],
            "progress": job["progress"],
        }
    )


@routes.post("/v1/jobs/{job_id}/cancel")
async def cancel_job(request: web.Request) -> web.Response:
    gateway = service(request)
    current = await gateway.get_job(request.match_info["job_id"])
    if not current or not can_access(request, current):
        return json_error("Job not found", status=404)
    try:
        job, canceled = await gateway.cancel_job(current["id"])
        return web.json_response({"ok": True, "canceled": canceled, "job": job})
    except Exception as exc:
        return exception_response(exc)


@routes.patch("/v1/jobs/{job_id}/queue")
async def reorder_job(request: web.Request) -> web.Response:
    gateway = service(request)
    current = await gateway.get_job(request.match_info["job_id"])
    if not current or not can_access(request, current):
        return json_error("Job not found", status=404)
    try:
        body = await request.json()
        if not isinstance(body, dict):
            raise ValueError("request body must be an object")
        job = await gateway.reorder_job(
            current["id"],
            action=body.get("action"),
            target_job_id=body.get("target_job_id"),
            priority=body.get("priority"),
        )
        return web.json_response({"ok": True, "job": job})
    except Exception as exc:
        return exception_response(exc)


@routes.post("/v1/jobs/{job_id}/retry")
async def retry_job(request: web.Request) -> web.Response:
    gateway = service(request)
    current = await gateway.get_job(request.match_info["job_id"])
    if not current or not can_access(request, current):
        return json_error("Job not found", status=404)
    try:
        job = await gateway.retry_job(current["id"])
        return web.json_response({"ok": True, "job": job}, status=202)
    except Exception as exc:
        return exception_response(exc)


@routes.get("/v1/queue")
async def queue(request: web.Request) -> web.Response:
    snapshot = await service(request).queue_snapshot()
    if not request.get(IS_ADMIN_KEY):
        snapshot["items"] = [
            job for job in snapshot["items"] if job.get("requested_by") == request.get(PRINCIPAL_KEY)
        ]
    return web.json_response({"ok": True, "queue": snapshot})


async def _output_response(request: web.Request, video_only: bool = False) -> web.Response:
    gateway = service(request)
    job_id = request.match_info["job_id"]
    job = await gateway.get_job(job_id)
    if not job or not can_access(request, job):
        return json_error("Job not found", status=404)
    try:
        if video_only:
            data, content_type, filename = await gateway.get_video(job_id)
        else:
            data, content_type, filename = await gateway.get_output(job_id, int(request.match_info["index"]))
        return web.Response(
            body=data,
            content_type=content_type.split(";", 1)[0],
            headers={"Content-Disposition": f'attachment; filename="{filename.replace(chr(34), "_")}"'},
        )
    except Exception as exc:
        return exception_response(exc)


@routes.get("/v1/jobs/{job_id}/outputs/{index}")
async def get_output(request: web.Request) -> web.Response:
    return await _output_response(request)


@routes.get("/v1/jobs/{job_id}/video")
async def get_video(request: web.Request) -> web.Response:
    return await _output_response(request, video_only=True)
