from __future__ import annotations

import hmac
from pathlib import Path
from aiohttp import web

from .auth import ADMIN_COOKIE, ADMIN_SESSIONS_KEY, SERVICE_KEY, AdminSessions, request_token
from .http_utils import exception_response, json_error, read_json
from .service import GatewayService


routes = web.RouteTableDef()
STATIC_DIR = Path(__file__).resolve().parent / "static"


def service(request: web.Request) -> GatewayService:
    return request.app[SERVICE_KEY]


@routes.get("/admin")
@routes.get("/admin/")
async def admin_index(request: web.Request) -> web.FileResponse:
    return web.FileResponse(STATIC_DIR / "index.html")


@routes.post("/admin/api/login")
async def login(request: web.Request) -> web.Response:
    try:
        body = await read_json(request)
        token = str(body.get("token") or "")
        if not token or not hmac.compare_digest(token, service(request).settings.admin_token):
            return json_error("Invalid administrator token", status=401)
        sessions: AdminSessions = request.app[ADMIN_SESSIONS_KEY]
        session_id = sessions.create()
        response = web.json_response({"ok": True})
        response.set_cookie(
            ADMIN_COOKIE,
            session_id,
            max_age=int(sessions.ttl),
            httponly=True,
            secure=request.secure,
            samesite="Strict",
            path="/",
        )
        return response
    except Exception as exc:
        return exception_response(exc)


@routes.post("/admin/api/logout")
async def logout(request: web.Request) -> web.Response:
    sessions: AdminSessions = request.app[ADMIN_SESSIONS_KEY]
    sessions.revoke(request.cookies.get(ADMIN_COOKIE))
    response = web.json_response({"ok": True})
    response.del_cookie(ADMIN_COOKIE, path="/")
    return response


@routes.get("/admin/api/session")
async def session(request: web.Request) -> web.Response:
    sessions = request.app[ADMIN_SESSIONS_KEY]
    bearer = request_token(request)
    authenticated = sessions.valid(request.cookies.get(ADMIN_COOKIE)) or bool(
        bearer and hmac.compare_digest(bearer, service(request).settings.admin_token)
    )
    return web.json_response({"ok": True, "authenticated": authenticated})


@routes.get("/admin/api/summary")
async def summary(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "summary": await service(request).summary()})


@routes.get("/admin/api/jobs")
async def jobs(request: web.Request) -> web.Response:
    try:
        limit = int(request.query.get("limit", "100"))
        offset = int(request.query.get("offset", "0"))
        values, total = await service(request).list_jobs(
            status=request.query.get("status"),
            mode=request.query.get("mode"),
            upstream_id=request.query.get("upstream_id"),
            search=request.query.get("search"),
            limit=limit,
            offset=offset,
        )
        return web.json_response({"ok": True, "jobs": values, "total": total, "limit": limit, "offset": offset})
    except Exception as exc:
        return exception_response(exc)


@routes.get("/admin/api/jobs/{job_id}")
async def job(request: web.Request) -> web.Response:
    value = await service(request).get_job(request.match_info["job_id"])
    if not value:
        return json_error("Job not found", status=404)
    return web.json_response({"ok": True, "job": value})


@routes.post("/admin/api/jobs/{job_id}/cancel")
async def cancel_job(request: web.Request) -> web.Response:
    try:
        value, canceled = await service(request).cancel_job(request.match_info["job_id"])
        if not value:
            return json_error("Job not found", status=404)
        return web.json_response({"ok": True, "canceled": canceled, "job": value})
    except Exception as exc:
        return exception_response(exc)


@routes.post("/admin/api/jobs/{job_id}/retry")
async def retry_job(request: web.Request) -> web.Response:
    try:
        value = await service(request).retry_job(request.match_info["job_id"])
        return web.json_response({"ok": True, "job": value}, status=202)
    except Exception as exc:
        return exception_response(exc)


@routes.patch("/admin/api/jobs/{job_id}/queue")
async def reorder_job(request: web.Request) -> web.Response:
    try:
        body = await read_json(request)
        value = await service(request).reorder_job(
            request.match_info["job_id"],
            action=body.get("action"),
            target_job_id=body.get("target_job_id"),
            priority=body.get("priority"),
        )
        return web.json_response({"ok": True, "job": value})
    except Exception as exc:
        return exception_response(exc)


@routes.get("/admin/api/queue")
async def queue(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "queue": await service(request).queue_snapshot()})


@routes.post("/admin/api/queue/pause")
async def pause_queue(request: web.Request) -> web.Response:
    await service(request).set_queue_paused(True)
    return web.json_response({"ok": True, "paused": True})


@routes.post("/admin/api/queue/resume")
async def resume_queue(request: web.Request) -> web.Response:
    await service(request).set_queue_paused(False)
    return web.json_response({"ok": True, "paused": False})


@routes.get("/admin/api/upstreams")
async def upstreams(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "upstreams": await service(request).list_upstreams()})


@routes.post("/admin/api/upstreams")
async def create_upstream(request: web.Request) -> web.Response:
    try:
        value = await service(request).create_upstream(await read_json(request))
        return web.json_response({"ok": True, "upstream": value}, status=201)
    except Exception as exc:
        return exception_response(exc)


@routes.patch("/admin/api/upstreams/{upstream_id}")
async def update_upstream(request: web.Request) -> web.Response:
    try:
        value = await service(request).update_upstream(request.match_info["upstream_id"], await read_json(request))
        return web.json_response({"ok": True, "upstream": value})
    except Exception as exc:
        return exception_response(exc)


@routes.delete("/admin/api/upstreams/{upstream_id}")
async def delete_upstream(request: web.Request) -> web.Response:
    try:
        deleted = await service(request).delete_upstream(request.match_info["upstream_id"])
        if not deleted:
            return json_error("Upstream not found", status=404)
        return web.json_response({"ok": True, "deleted": True})
    except Exception as exc:
        return exception_response(exc)


@routes.post("/admin/api/upstreams/{upstream_id}/test")
async def test_upstream(request: web.Request) -> web.Response:
    try:
        value = await service(request).test_upstream(request.match_info["upstream_id"])
        return web.json_response({"ok": True, "upstream": value})
    except Exception as exc:
        return exception_response(exc)


@routes.get("/admin/api/upstreams/{upstream_id}/queue")
async def upstream_queue(request: web.Request) -> web.Response:
    try:
        value = await service(request).upstream_queue(request.match_info["upstream_id"])
        return web.json_response({"ok": True, "queue": value})
    except Exception as exc:
        return exception_response(exc)


@routes.get("/admin/api/upstreams/{upstream_id}/prompt")
async def upstream_prompt_status(request: web.Request) -> web.Response:
    try:
        value = await service(request).upstream_prompt_status(request.match_info["upstream_id"])
        return web.json_response({"ok": True, "prompt": value})
    except Exception as exc:
        return exception_response(exc)


@routes.get("/admin/api/upstreams/{upstream_id}/system-stats")
async def upstream_system_stats(request: web.Request) -> web.Response:
    try:
        value = await service(request).upstream_system_stats(request.match_info["upstream_id"])
        return web.json_response({"ok": True, "system_stats": value})
    except Exception as exc:
        return exception_response(exc)


@routes.get("/admin/api/upstreams/{upstream_id}/object-info")
async def upstream_object_info(request: web.Request) -> web.Response:
    try:
        value = await service(request).upstream_object_info(request.match_info["upstream_id"])
        return web.json_response({"ok": True, "object_info": value})
    except Exception as exc:
        return exception_response(exc)


@routes.get("/admin/api/upstreams/{upstream_id}/history/{prompt_id}")
async def upstream_history(request: web.Request) -> web.Response:
    try:
        value = await service(request).upstream_history(
            request.match_info["upstream_id"], request.match_info["prompt_id"]
        )
        return web.json_response({"ok": True, "history": value})
    except Exception as exc:
        return exception_response(exc)


@routes.get("/admin/api/upstreams/{upstream_id}/history")
async def upstream_history_all(request: web.Request) -> web.Response:
    try:
        max_items = int(request.query["max_items"]) if "max_items" in request.query else None
        offset = int(request.query["offset"]) if "offset" in request.query else None
        value = await service(request).upstream_history_all(
            request.match_info["upstream_id"], max_items=max_items, offset=offset
        )
        return web.json_response({"ok": True, "history": value})
    except Exception as exc:
        return exception_response(exc)


@routes.post("/admin/api/upstreams/{upstream_id}/prompts/{prompt_id}/cancel")
async def upstream_cancel(request: web.Request) -> web.Response:
    try:
        canceled = await service(request).upstream_cancel(
            request.match_info["upstream_id"], request.match_info["prompt_id"]
        )
        return web.json_response({"ok": True, "canceled": canceled})
    except Exception as exc:
        return exception_response(exc)


@routes.post("/admin/api/upstreams/{upstream_id}/prompts/cancel")
async def upstream_cancel_many(request: web.Request) -> web.Response:
    try:
        body = await read_json(request)
        prompt_ids = body.get("prompt_ids")
        if not isinstance(prompt_ids, list) or not all(isinstance(value, str) for value in prompt_ids):
            raise ValueError("prompt_ids must be an array of strings")
        canceled = await service(request).upstream_cancel_many(request.match_info["upstream_id"], prompt_ids)
        return web.json_response({"ok": True, "canceled": canceled})
    except Exception as exc:
        return exception_response(exc)


@routes.post("/admin/api/upstreams/{upstream_id}/prompts/{prompt_id}/interrupt")
async def upstream_interrupt(request: web.Request) -> web.Response:
    try:
        await service(request).upstream_interrupt(
            request.match_info["upstream_id"], request.match_info["prompt_id"]
        )
        return web.json_response({"ok": True})
    except Exception as exc:
        return exception_response(exc)


@routes.post("/admin/api/upstreams/{upstream_id}/interrupt")
async def upstream_interrupt_all(request: web.Request) -> web.Response:
    try:
        await service(request).upstream_interrupt_all(request.match_info["upstream_id"])
        return web.json_response({"ok": True})
    except Exception as exc:
        return exception_response(exc)


@routes.post("/admin/api/upstreams/{upstream_id}/history/delete")
async def upstream_delete_history(request: web.Request) -> web.Response:
    try:
        body = await read_json(request)
        prompt_ids = body.get("prompt_ids")
        if not isinstance(prompt_ids, list) or not all(isinstance(value, str) for value in prompt_ids):
            raise ValueError("prompt_ids must be an array of strings")
        await service(request).upstream_delete_history(request.match_info["upstream_id"], prompt_ids)
        return web.json_response({"ok": True})
    except Exception as exc:
        return exception_response(exc)


@routes.post("/admin/api/upstreams/{upstream_id}/history/clear")
async def upstream_clear_history(request: web.Request) -> web.Response:
    try:
        await service(request).upstream_clear_history(request.match_info["upstream_id"])
        return web.json_response({"ok": True})
    except Exception as exc:
        return exception_response(exc)


@routes.post("/admin/api/upstreams/{upstream_id}/queue/delete")
async def upstream_delete_pending(request: web.Request) -> web.Response:
    try:
        body = await read_json(request)
        prompt_ids = body.get("prompt_ids")
        if not isinstance(prompt_ids, list) or not all(isinstance(value, str) for value in prompt_ids):
            raise ValueError("prompt_ids must be an array of strings")
        await service(request).upstream_delete_pending(request.match_info["upstream_id"], prompt_ids)
        return web.json_response({"ok": True})
    except Exception as exc:
        return exception_response(exc)


@routes.post("/admin/api/upstreams/{upstream_id}/queue/clear")
async def upstream_clear_queue(request: web.Request) -> web.Response:
    try:
        await service(request).upstream_clear(request.match_info["upstream_id"])
        return web.json_response({"ok": True})
    except Exception as exc:
        return exception_response(exc)


@routes.post("/admin/api/upstreams/{upstream_id}/free")
async def upstream_free(request: web.Request) -> web.Response:
    try:
        body = await read_json(request)
        await service(request).upstream_free(
            request.match_info["upstream_id"],
            unload_models=bool(body.get("unload_models", True)),
            free_memory=bool(body.get("free_memory", True)),
        )
        return web.json_response({"ok": True})
    except Exception as exc:
        return exception_response(exc)


@routes.get("/admin/api/api-keys")
async def api_keys(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "api_keys": await service(request).database.list_api_keys()})


@routes.post("/admin/api/api-keys")
async def create_api_key(request: web.Request) -> web.Response:
    try:
        body = await read_json(request)
        value, token = await service(request).create_api_key(
            str(body.get("name") or ""), body.get("expires_at")
        )
        return web.json_response({"ok": True, "api_key": value, "token": token}, status=201)
    except Exception as exc:
        return exception_response(exc)


@routes.patch("/admin/api/api-keys/{key_id}")
async def update_api_key(request: web.Request) -> web.Response:
    try:
        body = await read_json(request)
        if "enabled" not in body:
            raise ValueError("enabled is required")
        changed = await service(request).set_api_key_enabled(request.match_info["key_id"], bool(body["enabled"]))
        if not changed:
            return json_error("API key not found", status=404)
        return web.json_response({"ok": True})
    except Exception as exc:
        return exception_response(exc)


@routes.delete("/admin/api/api-keys/{key_id}")
async def delete_api_key(request: web.Request) -> web.Response:
    deleted = await service(request).delete_api_key(request.match_info["key_id"])
    if not deleted:
        return json_error("API key not found", status=404)
    return web.json_response({"ok": True, "deleted": True})


@routes.get("/admin/api/events")
async def events(request: web.Request) -> web.Response:
    try:
        limit = int(request.query.get("limit", "100"))
        return web.json_response({"ok": True, "events": await service(request).database.list_events(limit)})
    except Exception as exc:
        return exception_response(exc)


@routes.get("/admin/api/schema")
async def admin_schema(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, **service(request).schema()})
