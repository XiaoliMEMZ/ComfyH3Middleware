from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import quote

import aiohttp


class ComfyError(RuntimeError):
    def __init__(self, message: str, status: int | None = None, payload: Any = None, transport: bool = False) -> None:
        super().__init__(message)
        self.status = status
        self.payload = payload
        self.transport = transport


class ComfyClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        base_url: str,
        auth_token: str | None = None,
    ) -> None:
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}

    async def request(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = dict(self.headers)
        headers.update(kwargs.pop("headers", {}))
        try:
            async with self.session.request(method, f"{self.base_url}{path}", headers=headers, **kwargs) as response:
                content_type = response.headers.get("Content-Type", "")
                if "json" in content_type:
                    payload = await response.json()
                else:
                    payload = await response.read()
                if response.status >= 400:
                    raise ComfyError(
                        f"ComfyUI {method} {path} returned HTTP {response.status}",
                        status=response.status,
                        payload=payload if not isinstance(payload, bytes) else payload[:1000].decode("utf-8", "replace"),
                    )
                return payload
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise ComfyError(f"ComfyUI connection failed: {exc}", transport=True) from exc

    async def system_stats(self) -> dict[str, Any]:
        value = await self.request("GET", "/system_stats")
        if not isinstance(value, dict):
            raise ComfyError("ComfyUI returned invalid system stats")
        return value

    async def object_info(self) -> dict[str, Any]:
        value = await self.request("GET", "/object_info")
        if not isinstance(value, dict):
            raise ComfyError("ComfyUI returned invalid object info")
        return value

    async def queue(self) -> dict[str, Any]:
        value = await self.request("GET", "/queue")
        if not isinstance(value, dict):
            raise ComfyError("ComfyUI returned an invalid queue")
        return value

    async def prompt_status(self) -> dict[str, Any]:
        value = await self.request("GET", "/prompt")
        if not isinstance(value, dict):
            raise ComfyError("ComfyUI returned invalid prompt status")
        return value

    async def history_all(self, max_items: int | None = None, offset: int | None = None) -> dict[str, Any]:
        params: dict[str, int] = {}
        if max_items is not None:
            params["max_items"] = max_items
        if offset is not None:
            params["offset"] = offset
        value = await self.request("GET", "/history", params=params)
        if not isinstance(value, dict):
            raise ComfyError("ComfyUI returned invalid history")
        return value

    async def history(self, prompt_id: str) -> dict[str, Any] | None:
        value = await self.request("GET", f"/history/{quote(prompt_id, safe='')}")
        if not isinstance(value, dict):
            return None
        history = value.get(prompt_id)
        return history if isinstance(history, dict) else None

    async def upload_asset(self, asset: dict[str, Any], subfolder: str) -> str:
        path = Path(asset["path"])
        form = aiohttp.FormData()
        with path.open("rb") as source:
            form.add_field(
                "image",
                source,
                filename=asset["filename"],
                content_type=asset.get("content_type") or "application/octet-stream",
            )
            form.add_field("type", "input")
            form.add_field("overwrite", "true")
            form.add_field("subfolder", subfolder)
            value = await self.request("POST", "/upload/image", data=form)
        if not isinstance(value, dict) or not value.get("name"):
            raise ComfyError("ComfyUI returned an invalid upload response", payload=value)
        returned_subfolder = value.get("subfolder") or ""
        return f"{returned_subfolder}/{value['name']}" if returned_subfolder else value["name"]

    async def submit(
        self,
        prompt: dict[str, Any],
        prompt_id: str,
        extra_data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "prompt": prompt,
            "prompt_id": prompt_id,
            "client_id": "h3-middleware",
            "extra_data": extra_data or {},
        }
        value = await self.request("POST", "/prompt", json=payload)
        if not isinstance(value, dict):
            raise ComfyError("ComfyUI returned an invalid prompt response", payload=value)
        if value.get("error") or value.get("node_errors"):
            raise ComfyError("ComfyUI rejected the prompt", status=400, payload=value)
        return value

    async def prompt_location(self, prompt_id: str) -> str | None:
        history = await self.history(prompt_id)
        if history is not None:
            return "history"
        queue = await self.queue()
        if prompt_id in queue_ids(queue.get("queue_running")):
            return "running"
        if prompt_id in queue_ids(queue.get("queue_pending")):
            return "pending"
        return None

    async def cancel(self, prompt_id: str) -> bool:
        try:
            value = await self.request("POST", f"/api/jobs/{quote(prompt_id, safe='')}/cancel", json={})
            return bool(isinstance(value, dict) and value.get("cancelled"))
        except ComfyError as exc:
            if exc.status != 404:
                raise

        queue = await self.queue()
        pending = prompt_id in queue_ids(queue.get("queue_pending"))
        running = prompt_id in queue_ids(queue.get("queue_running"))
        if pending:
            await self.delete_pending([prompt_id])
        if running:
            await self.interrupt(prompt_id)
        return pending or running

    async def cancel_many(self, prompt_ids: list[str]) -> bool:
        try:
            value = await self.request("POST", "/api/jobs/cancel", json={"job_ids": prompt_ids})
            return bool(isinstance(value, dict) and value.get("cancelled"))
        except ComfyError as exc:
            if exc.status != 404:
                raise
        canceled = False
        for prompt_id in prompt_ids:
            canceled = await self.cancel(prompt_id) or canceled
        return canceled

    async def delete_pending(self, prompt_ids: list[str]) -> None:
        await self.request("POST", "/queue", json={"delete": prompt_ids})

    async def clear_queue(self) -> None:
        await self.request("POST", "/queue", json={"clear": True})

    async def interrupt(self, prompt_id: str | None = None) -> None:
        await self.request("POST", "/interrupt", json={"prompt_id": prompt_id} if prompt_id else {})

    async def delete_history(self, prompt_ids: list[str]) -> None:
        await self.request("POST", "/history", json={"delete": prompt_ids})

    async def clear_history(self) -> None:
        await self.request("POST", "/history", json={"clear": True})

    async def free(self, unload_models: bool = True, free_memory: bool = True) -> None:
        await self.request(
            "POST",
            "/free",
            json={"unload_models": unload_models, "free_memory": free_memory},
        )

    async def view(self, output: dict[str, Any]) -> tuple[bytes, str]:
        params = {
            "filename": output["filename"],
            "subfolder": output.get("subfolder") or "",
            "type": output.get("type") or "output",
        }
        headers = dict(self.headers)
        try:
            async with self.session.get(f"{self.base_url}/view", params=params, headers=headers) as response:
                data = await response.read()
                if response.status >= 400:
                    raise ComfyError(f"ComfyUI GET /view returned HTTP {response.status}", status=response.status)
                return data, response.headers.get("Content-Type", "application/octet-stream")
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise ComfyError(f"ComfyUI connection failed: {exc}", transport=True) from exc


def queue_ids(items: Any) -> set[str]:
    result: set[str] = set()
    for item in items or []:
        if isinstance(item, (list, tuple)) and len(item) > 1:
            result.add(str(item[1]))
    return result


def history_error(history: dict[str, Any]) -> dict[str, Any] | None:
    status = history.get("status") or {}
    if status.get("status_str") != "error" and status.get("completed") is not False:
        return None
    for message in status.get("messages") or []:
        if isinstance(message, list) and message and message[0] == "execution_error":
            payload = message[1] if len(message) > 1 and isinstance(message[1], dict) else {}
            return {
                "message": payload.get("exception_message") or "ComfyUI execution failed",
                "type": payload.get("exception_type"),
                "node_id": payload.get("node_id"),
                "node_type": payload.get("node_type"),
                "details": payload,
            }
    return {"message": "ComfyUI execution failed", "details": status}


def history_complete(history: dict[str, Any]) -> bool:
    status = history.get("status") or {}
    return bool(status.get("completed") or status.get("status_str") in {"success", "error"})


def extract_outputs(history: dict[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for node_id, node_output in (history.get("outputs") or {}).items():
        if not isinstance(node_output, dict):
            continue
        for key, kind in (("videos", "video"), ("gifs", "video"), ("audio", "audio"), ("images", "image")):
            values = node_output.get(key) or []
            if isinstance(values, dict):
                values = [values]
            for item in values:
                if not isinstance(item, dict) or not item.get("filename"):
                    continue
                filename = item["filename"]
                resolved_kind = kind
                if str(filename).lower().endswith((".mp4", ".webm", ".mov", ".mkv", ".avi")):
                    resolved_kind = "video"
                result.append(
                    {
                        "node_id": str(node_id),
                        "kind": resolved_kind,
                        "filename": filename,
                        "subfolder": item.get("subfolder") or "",
                        "type": item.get("type") or "output",
                    }
                )
        for item in node_output.get("files") or []:
            if isinstance(item, dict) and item.get("filename"):
                result.append(
                    {
                        "node_id": str(node_id),
                        "kind": "file",
                        "filename": item["filename"],
                        "subfolder": item.get("subfolder") or "",
                        "type": item.get("type") or "output",
                    }
                )
    return result


def error_payload(error: ComfyError) -> dict[str, Any]:
    return {
        "message": str(error),
        "status": error.status,
        "transport": error.transport,
        "details": error.payload,
    }
