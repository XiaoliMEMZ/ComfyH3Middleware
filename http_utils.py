from __future__ import annotations

import json
import uuid
from typing import Any

from aiohttp import web

from .assets import AssetStore
from .comfy_client import ComfyError, error_payload


JSON_FORM_FIELDS = {
    "node_overrides",
    "ref_image_names",
    "ref_video_names",
    "ref_video_audio_names",
    "ref_audio_names",
}


def json_error(message: str, status: int = 400, code: str | None = None, details: Any = None) -> web.Response:
    error: dict[str, Any] = {"message": message}
    if code:
        error["code"] = code
    if details is not None:
        error["details"] = details
    return web.json_response({"ok": False, "error": error}, status=status)


def exception_response(exc: Exception) -> web.Response:
    if isinstance(exc, ComfyError):
        return json_error(str(exc), status=exc.status if exc.status and 400 <= exc.status < 500 else 502, details=error_payload(exc))
    if isinstance(exc, TimeoutError):
        return json_error(str(exc), status=202, code="still_running")
    if isinstance(exc, (ValueError, json.JSONDecodeError)):
        return json_error(str(exc), status=400)
    return json_error("Internal server error", status=500)


async def read_json(request: web.Request) -> dict[str, Any]:
    try:
        value = await request.json()
    except json.JSONDecodeError as exc:
        raise ValueError("request body must be valid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("request body must be a JSON object")
    return value


async def read_submission(
    request: web.Request,
    asset_store: AssetStore,
) -> tuple[str, dict[str, Any], dict[str, list[dict[str, Any]]]]:
    job_id = str(uuid.uuid4())
    content_type = request.headers.get("Content-Type", "")
    try:
        if "multipart/form-data" in content_type:
            fields: dict[str, Any] = {}
            assets: dict[str, list[dict[str, Any]]] = {}
            reader = await request.multipart()
            while True:
                part = await reader.next()
                if part is None:
                    break
                name = part.name or ""
                if part.filename:
                    item = await asset_store.save_part(job_id, name, part, assets)
                    kind = item["kind"]
                    if kind in {"first_frame", "last_frame"} and assets.get(kind):
                        raise ValueError(f"{name} may only be uploaded once")
                    assets.setdefault(kind, []).append(item)
                else:
                    value = await part.text()
                    if name in JSON_FORM_FIELDS and value:
                        try:
                            value = json.loads(value)
                        except json.JSONDecodeError as exc:
                            raise ValueError(f"{name} must contain valid JSON") from exc
                    fields[name] = value
            return job_id, fields, asset_store.sorted_assets(assets)

        if "application/json" in content_type or not content_type:
            raw = await read_json(request) if request.can_read_body else {}
            fields, assets = asset_store.extract_json_assets(job_id, raw)
            return job_id, fields, assets

        post = await request.post()
        fields = {key: post.getone(key) for key in post}
        for key in JSON_FORM_FIELDS:
            if key in fields and fields[key]:
                fields[key] = json.loads(fields[key])
        return job_id, fields, {}
    except Exception:
        asset_store.cleanup_job(job_id)
        raise
