from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any
from urllib.parse import urlparse

import aiohttp

from .assets import AssetStore
from .comfy_client import (
    ComfyClient,
    ComfyError,
    error_payload,
    extract_outputs,
    history_complete,
    history_error,
    queue_ids,
)
from .config import Settings
from .database import ACTIVE_STATUSES, LOCAL_QUEUE_STATUSES, TERMINAL_STATUSES, Database
from .progress import apply_progress_event, create_progress, event_prompt_id, present_progress
from .upstreams import UpstreamManager
from .workflows import AdapterRegistry, create_registry


LOGGER = logging.getLogger(__name__)


class GatewayService:
    def __init__(self, settings: Settings, registry: AdapterRegistry | None = None) -> None:
        self.settings = settings
        self.database = Database(settings.database_path)
        self.assets = AssetStore(settings.asset_dir, settings.max_upload_bytes)
        self.registry = registry or create_registry(settings.conditioning_node)
        self.session: aiohttp.ClientSession | None = None
        self.upstreams: UpstreamManager | None = None
        self.tasks: list[asyncio.Task[Any]] = []
        self.progress_listeners: dict[str, asyncio.Task[Any]] = {}
        self.progress_listener_configs: dict[str, tuple[str, str | None]] = {}
        self.wake_scheduler = asyncio.Event()
        self.stopping = False

    async def start(self) -> None:
        self.stopping = False
        self.settings.prepare()
        await self.database.initialize()
        await self.database.seed_upstreams(self.settings.initial_upstreams)
        timeout = aiohttp.ClientTimeout(
            total=None,
            sock_connect=self.settings.request_timeout,
            sock_read=self.settings.request_timeout,
        )
        self.session = aiohttp.ClientSession(timeout=timeout)
        self.upstreams = UpstreamManager(self.database, self.session, self.settings.capability_interval)
        await self.upstreams.health_check_all(force_capabilities=True)
        await self._sync_progress_listeners()
        self.tasks = [
            asyncio.create_task(self._progress_supervisor_loop(), name="h3-progress-supervisor"),
            asyncio.create_task(self._scheduler_loop(), name="h3-scheduler"),
            asyncio.create_task(self._monitor_loop(), name="h3-monitor"),
            asyncio.create_task(self._health_loop(), name="h3-health"),
        ]

    async def stop(self) -> None:
        self.stopping = True
        self.wake_scheduler.set()
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        listeners = list(self.progress_listeners.values())
        for task in listeners:
            task.cancel()
        if listeners:
            await asyncio.gather(*listeners, return_exceptions=True)
        self.progress_listeners.clear()
        self.progress_listener_configs.clear()
        if self.session is not None:
            await self.session.close()
            self.session = None
        await self.database.close()

    async def submit(
        self,
        job_id: str,
        raw: dict[str, Any],
        assets: dict[str, list[dict[str, Any]]],
        requested_by: str | None,
        force_mode: str | None = None,
    ) -> dict[str, Any]:
        adapter_name = str(raw.get("adapter") or "minimax-h3-native")
        adapter = self.registry.get(adapter_name)
        params = adapter.normalize(raw, assets, force_mode=force_mode)
        try:
            priority = int(raw.get("priority", 0))
            max_attempts = int(raw.get("max_attempts", self.settings.default_max_attempts))
        except (TypeError, ValueError) as exc:
            raise ValueError("priority and max_attempts must be integers") from exc
        if max_attempts < 1 or max_attempts > 10:
            raise ValueError("max_attempts must be between 1 and 10")
        job = await self.database.create_job(
            job_id=job_id,
            mode=params["mode"],
            adapter=adapter_name,
            params=params,
            assets=assets,
            priority=priority,
            max_attempts=max_attempts,
            requested_by=requested_by,
        )
        await self.database.add_event(
            "job.queued",
            "job",
            job_id,
            f"Queued {params['mode']} generation",
            {"priority": priority, "requested_by": requested_by},
        )
        self.wake_scheduler.set()
        return self.public_job(job)

    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        job = await self.database.get_job(job_id)
        return self.public_job(job) if job else None

    async def list_jobs(self, **filters: Any) -> tuple[list[dict[str, Any]], int]:
        jobs, total = await self.database.list_jobs(**filters)
        return [self.public_job(job) for job in jobs], total

    def public_job(self, job: dict[str, Any]) -> dict[str, Any]:
        item = dict(job)
        item["progress"] = present_progress(item.get("progress"), item["status"], item.get("updated_at"))
        stored_assets = item.pop("assets", {})
        item["inputs"] = {
            kind: [
                {
                    "filename": asset.get("filename"),
                    "size": asset.get("size"),
                    "content_type": asset.get("content_type"),
                }
                for asset in values
            ]
            for kind, values in stored_assets.items()
        }
        for index, output in enumerate(item.get("outputs") or []):
            output["download_url"] = f"/v1/jobs/{item['id']}/outputs/{index}"
        videos = [output for output in item.get("outputs") or [] if output.get("kind") == "video"]
        if videos:
            item["video_url"] = f"/v1/jobs/{item['id']}/video"
        return item

    async def cancel_job(self, job_id: str) -> tuple[dict[str, Any] | None, bool]:
        job, changed = await self.database.request_cancel(job_id)
        if not job or not changed:
            return self.public_job(job) if job else None, False
        if job["status"] == "canceled":
            await self.database.add_event("job.canceled", "job", job_id, "Canceled in middleware queue")
            self.wake_scheduler.set()
            return self.public_job(job), True

        upstream = await self.database.get_upstream(job["upstream_id"]) if job.get("upstream_id") else None
        if upstream and job.get("prompt_id"):
            try:
                canceled = await self._client(upstream).cancel(job["prompt_id"])
                if canceled:
                    job = await self.database.update_job(job_id, status="canceled", finished_at=time.time())
                    await self.database.add_event(
                        "job.canceled",
                        "job",
                        job_id,
                        f"Canceled on {upstream['name']}",
                        {"upstream_id": upstream["id"]},
                    )
            except ComfyError as exc:
                await self._manager().mark_failure(upstream, exc)
                await self.database.add_event(
                    "job.cancel_pending",
                    "job",
                    job_id,
                    "Cancellation will be retried when the upstream is reachable",
                    error_payload(exc),
                )
        return self.public_job(job), True

    async def reorder_job(
        self,
        job_id: str,
        action: str | None = None,
        target_job_id: str | None = None,
        priority: int | None = None,
    ) -> dict[str, Any]:
        if priority is not None:
            job = await self.database.set_job_priority(job_id, int(priority))
        elif action:
            job = await self.database.reorder_job(job_id, action, target_job_id)
        else:
            raise ValueError("action or priority is required")
        await self.database.add_event(
            "job.reordered",
            "job",
            job_id,
            "Changed middleware queue order",
            {"action": action, "target_job_id": target_job_id, "priority": priority},
        )
        self.wake_scheduler.set()
        return self.public_job(job)

    async def retry_job(self, job_id: str) -> dict[str, Any]:
        job = await self.database.retry_job(job_id)
        await self.database.add_event("job.retried", "job", job_id, "Requeued terminal job")
        self.wake_scheduler.set()
        return self.public_job(job)

    async def queue_snapshot(self) -> dict[str, Any]:
        snapshot = await self.database.queue_snapshot()
        snapshot["items"] = [self.public_job(job) for job in snapshot["items"]]
        return snapshot

    async def set_queue_paused(self, paused: bool) -> None:
        await self.database.set_queue_paused(paused)
        await self.database.add_event(
            "queue.paused" if paused else "queue.resumed",
            "queue",
            None,
            "Paused middleware dispatch" if paused else "Resumed middleware dispatch",
        )
        if not paused:
            self.wake_scheduler.set()

    async def wait_for_job(self, job_id: str, timeout: float) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            job = await self.database.get_job(job_id)
            if not job:
                raise ValueError("job not found")
            if job["status"] in TERMINAL_STATUSES:
                return self.public_job(job)
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError(f"job did not finish within {timeout:g} seconds")
            await asyncio.sleep(min(self.settings.poll_interval, remaining))

    async def get_output(self, job_id: str, index: int) -> tuple[bytes, str, str]:
        job = await self.database.get_job(job_id)
        if not job:
            raise ValueError("job not found")
        outputs = job.get("outputs") or []
        if index < 0 or index >= len(outputs):
            raise ValueError("output not found")
        upstream = await self.database.get_upstream(job.get("upstream_id")) if job.get("upstream_id") else None
        if not upstream:
            raise ValueError("job has no output upstream")
        data, content_type = await self._client(upstream).view(outputs[index])
        return data, content_type, outputs[index]["filename"]

    async def get_video(self, job_id: str) -> tuple[bytes, str, str]:
        job = await self.database.get_job(job_id)
        if not job:
            raise ValueError("job not found")
        for index, output in enumerate(job.get("outputs") or []):
            if output.get("kind") == "video":
                return await self.get_output(job_id, index)
        raise ValueError("video is not ready")

    async def health(self) -> dict[str, Any]:
        upstreams = await self.list_upstreams(public=True)
        healthy = sum(
            1 for upstream in upstreams
            if upstream["enabled"] and upstream["runtime"]["healthy"]
        )
        queue = await self.database.queue_snapshot()
        return {
            "ok": healthy > 0,
            "service": "h3-middleware",
            "queue_paused": queue["paused"],
            "queued_jobs": len(queue["items"]),
            "healthy_upstreams": healthy,
            "upstream_count": len(upstreams),
            "upstreams": upstreams,
        }

    async def summary(self) -> dict[str, Any]:
        result = await self.database.summary()
        upstreams = await self.list_upstreams(public=True)
        result["upstreams"] = {
            "total": len(upstreams),
            "healthy": sum(1 for item in upstreams if item["enabled"] and item["runtime"]["healthy"]),
            "busy": sum(1 for item in upstreams if item["runtime"]["queue_running"]),
        }
        result["queue_paused"] = await self.database.queue_paused()
        return result

    async def list_upstreams(self, public: bool = False) -> list[dict[str, Any]]:
        upstreams = await self._manager().with_runtime(await self.database.list_upstreams())
        for upstream in upstreams:
            if upstream.get("auth_token"):
                upstream["has_auth_token"] = True
            upstream.pop("auth_token", None)
            if public:
                upstream.pop("stats", None)
                upstream.pop("options", None)
        return upstreams

    async def create_upstream(self, values: dict[str, Any]) -> dict[str, Any]:
        normalized = self._validate_upstream(values, partial=False)
        self.registry.get(normalized["adapter"])
        upstream = await self.database.create_upstream(normalized)
        await self.database.add_event("upstream.created", "upstream", upstream["id"], f"Added {upstream['name']}")
        if upstream["enabled"]:
            await self._manager().health_check(upstream, force_capabilities=True)
            self.wake_scheduler.set()
        return (await self.list_upstreams_by_id(upstream["id"]))

    async def update_upstream(self, upstream_id: str, values: dict[str, Any]) -> dict[str, Any]:
        normalized = self._validate_upstream(values, partial=True)
        if "adapter" in normalized:
            self.registry.get(normalized["adapter"])
        previous = await self.database.get_upstream(upstream_id)
        if not previous:
            raise ValueError("upstream not found")
        upstream = await self.database.update_upstream(upstream_id, normalized)
        if not upstream:
            raise ValueError("upstream not found")
        enabled_changed = previous["enabled"] != upstream["enabled"]
        if enabled_changed:
            action = "enabled" if upstream["enabled"] else "disabled"
            await self.database.add_event(f"upstream.{action}", "upstream", upstream_id, f"{action.title()} {upstream['name']}")
        else:
            await self.database.add_event("upstream.updated", "upstream", upstream_id, f"Updated {upstream['name']}")
        if upstream["enabled"]:
            await self._manager().health_check(upstream, force_capabilities=True)
            self.wake_scheduler.set()
        return await self.list_upstreams_by_id(upstream_id)

    async def delete_upstream(self, upstream_id: str) -> bool:
        deleted = await self.database.delete_upstream(upstream_id)
        if deleted:
            self._manager().runtime.pop(upstream_id, None)
            await self.database.add_event("upstream.deleted", "upstream", upstream_id, "Deleted upstream")
        return deleted

    async def test_upstream(self, upstream_id: str) -> dict[str, Any]:
        upstream = await self.database.get_upstream(upstream_id)
        if not upstream:
            raise ValueError("upstream not found")
        await self._manager().health_check(upstream, force_capabilities=True)
        return await self.list_upstreams_by_id(upstream_id)

    async def list_upstreams_by_id(self, upstream_id: str) -> dict[str, Any]:
        for upstream in await self.list_upstreams():
            if upstream["id"] == upstream_id:
                return upstream
        raise ValueError("upstream not found")

    @staticmethod
    def _validate_upstream(values: dict[str, Any], partial: bool) -> dict[str, Any]:
        normalized = dict(values)
        if not partial or "name" in normalized:
            normalized["name"] = str(normalized.get("name") or "").strip()
            if not normalized["name"]:
                raise ValueError("upstream name is required")
        if not partial or "base_url" in normalized:
            base_url = str(normalized.get("base_url") or "").strip().rstrip("/")
            parsed = urlparse(base_url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                raise ValueError("base_url must be an HTTP or HTTPS URL")
            normalized["base_url"] = base_url
        if "weight" in normalized:
            normalized["weight"] = float(normalized["weight"])
            if normalized["weight"] <= 0:
                raise ValueError("weight must be greater than zero")
        if "max_concurrency" in normalized:
            normalized["max_concurrency"] = int(normalized["max_concurrency"])
            if normalized["max_concurrency"] < 1:
                raise ValueError("max_concurrency must be at least 1")
        if not partial:
            normalized.setdefault("adapter", "minimax-h3-native")
        if "options" in normalized and not isinstance(normalized["options"], dict):
            raise ValueError("options must be an object")
        return normalized

    async def upstream_queue(self, upstream_id: str) -> dict[str, Any]:
        upstream = await self._require_upstream(upstream_id)
        return await self._client(upstream).queue()

    async def upstream_prompt_status(self, upstream_id: str) -> dict[str, Any]:
        upstream = await self._require_upstream(upstream_id)
        return await self._client(upstream).prompt_status()

    async def upstream_system_stats(self, upstream_id: str) -> dict[str, Any]:
        upstream = await self._require_upstream(upstream_id)
        return await self._client(upstream).system_stats()

    async def upstream_object_info(self, upstream_id: str) -> dict[str, Any]:
        upstream = await self._require_upstream(upstream_id)
        return await self._client(upstream).object_info()

    async def upstream_history(self, upstream_id: str, prompt_id: str) -> dict[str, Any] | None:
        upstream = await self._require_upstream(upstream_id)
        return await self._client(upstream).history(prompt_id)

    async def upstream_history_all(
        self, upstream_id: str, max_items: int | None = None, offset: int | None = None
    ) -> dict[str, Any]:
        upstream = await self._require_upstream(upstream_id)
        return await self._client(upstream).history_all(max_items=max_items, offset=offset)

    async def upstream_cancel(self, upstream_id: str, prompt_id: str) -> bool:
        upstream = await self._require_upstream(upstream_id)
        canceled = await self._client(upstream).cancel(prompt_id)
        await self.database.add_event(
            "upstream.cancel",
            "upstream",
            upstream_id,
            f"Requested cancellation for {prompt_id}",
            {"prompt_id": prompt_id, "canceled": canceled},
        )
        return canceled

    async def upstream_cancel_many(self, upstream_id: str, prompt_ids: list[str]) -> bool:
        upstream = await self._require_upstream(upstream_id)
        canceled = await self._client(upstream).cancel_many(prompt_ids)
        await self.database.add_event(
            "upstream.cancel_many",
            "upstream",
            upstream_id,
            f"Requested cancellation for {len(prompt_ids)} prompts",
            {"prompt_ids": prompt_ids, "canceled": canceled},
        )
        return canceled

    async def upstream_delete_pending(self, upstream_id: str, prompt_ids: list[str]) -> None:
        upstream = await self._require_upstream(upstream_id)
        await self._client(upstream).delete_pending(prompt_ids)

    async def upstream_clear(self, upstream_id: str) -> None:
        upstream = await self._require_upstream(upstream_id)
        await self._client(upstream).clear_queue()

    async def upstream_interrupt(self, upstream_id: str, prompt_id: str) -> None:
        upstream = await self._require_upstream(upstream_id)
        await self._client(upstream).interrupt(prompt_id)

    async def upstream_interrupt_all(self, upstream_id: str) -> None:
        upstream = await self._require_upstream(upstream_id)
        await self._client(upstream).interrupt()

    async def upstream_delete_history(self, upstream_id: str, prompt_ids: list[str]) -> None:
        upstream = await self._require_upstream(upstream_id)
        await self._client(upstream).delete_history(prompt_ids)

    async def upstream_clear_history(self, upstream_id: str) -> None:
        upstream = await self._require_upstream(upstream_id)
        await self._client(upstream).clear_history()

    async def upstream_free(self, upstream_id: str, unload_models: bool, free_memory: bool) -> None:
        upstream = await self._require_upstream(upstream_id)
        await self._client(upstream).free(unload_models=unload_models, free_memory=free_memory)

    async def _require_upstream(self, upstream_id: str) -> dict[str, Any]:
        upstream = await self.database.get_upstream(upstream_id)
        if not upstream:
            raise ValueError("upstream not found")
        return upstream

    async def _scheduler_loop(self) -> None:
        while not self.stopping:
            try:
                await self._dispatch_available()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("middleware scheduler failed")
            self.wake_scheduler.clear()
            try:
                await asyncio.wait_for(self.wake_scheduler.wait(), timeout=0.5)
            except TimeoutError:
                pass

    async def _dispatch_available(self) -> None:
        if await self.database.queue_paused():
            return
        while not self.stopping:
            active_counts = await self.database.active_counts()
            queued = await self.database.queued_jobs(limit=50)
            dispatched = False
            for job in queued:
                adapter = self.registry.get(job["adapter"])
                upstream_configs = await self.database.list_upstreams(enabled_only=True)
                required = {
                    upstream["id"]: adapter.required_nodes(
                        job["mode"],
                        upstream.get("options"),
                        params=job["params"],
                        assets=job["assets"],
                    )
                    for upstream in upstream_configs
                    if upstream["adapter"] == job["adapter"]
                }
                candidates = await self._manager().candidates(job["adapter"], required, active_counts)
                if not candidates:
                    continue
                claimed = await self.database.claim_job(job["id"])
                if not claimed:
                    continue
                await self._dispatch_job(claimed, candidates)
                dispatched = True
                break
            if not dispatched:
                return

    async def _dispatch_job(self, job: dict[str, Any], candidates: list[dict[str, Any]]) -> None:
        adapter = self.registry.get(job["adapter"])
        errors: list[dict[str, Any]] = []
        dispatch_prompt_id = job.get("prompt_id")
        attempted = False
        for upstream in candidates:
            latest = await self.database.get_job(job["id"])
            if not latest or latest["cancel_requested"]:
                await self.database.update_job(job["id"], status="canceled", finished_at=time.time())
                return
            current = await self.database.get_upstream(upstream["id"])
            if not current or not current["enabled"]:
                continue
            upstream = current
            attempted = True
            if not dispatch_prompt_id:
                dispatch_prompt_id = str(uuid.uuid4())
                await self.database.update_job(job["id"], prompt_id=dispatch_prompt_id)
            client = self._client(upstream)
            try:
                uploaded = await self._upload_assets(client, job)
            except ComfyError as exc:
                errors.append({"upstream_id": upstream["id"], "stage": "upload", **error_payload(exc)})
                await self._manager().mark_failure(upstream, exc)
                await self.database.add_event(
                    "job.dispatch_failed",
                    "job",
                    job["id"],
                    f"Asset upload to {upstream['name']} failed",
                    errors[-1],
                )
                continue
            try:
                prompt = adapter.build(job["params"], uploaded, upstream.get("options"))
            except ValueError as exc:
                await self.database.update_job(
                    job["id"],
                    status="failed",
                    error={"message": str(exc), "stage": "workflow"},
                    finished_at=time.time(),
                )
                await self.database.add_event("job.failed", "job", job["id"], str(exc), {"stage": "workflow"})
                return
            await self.database.update_job(job["id"], progress=create_progress(prompt))
            latest = await self.database.get_job(job["id"])
            if not latest or latest["cancel_requested"]:
                await self.database.update_job(job["id"], status="canceled", finished_at=time.time())
                return
            try:
                response = await client.submit(
                    prompt,
                    prompt_id=dispatch_prompt_id,
                    extra_data={"h3_middleware_job_id": job["id"], "adapter": job["adapter"]},
                )
            except ComfyError as exc:
                location = None
                if exc.transport:
                    try:
                        location = await client.prompt_location(dispatch_prompt_id)
                    except ComfyError:
                        pass
                if location:
                    response = {"prompt_id": dispatch_prompt_id, "recovered_location": location}
                else:
                    errors.append({"upstream_id": upstream["id"], **error_payload(exc)})
                    await self._manager().mark_failure(upstream, exc)
                    await self.database.add_event(
                        "job.dispatch_failed",
                        "job",
                        job["id"],
                        f"Dispatch to {upstream['name']} failed",
                        errors[-1],
                    )
                    continue

            prompt_id = str(response.get("prompt_id") or dispatch_prompt_id)
            latest = await self.database.get_job(job["id"])
            status = latest["status"] if latest and latest["status"] in {"running", "canceling"} else "submitted"
            await self.database.update_job(
                job["id"],
                status=status,
                upstream_id=upstream["id"],
                prompt_id=prompt_id,
                error=None,
                submitted_at=time.time(),
                not_before=0,
            )
            self._manager().selected(upstream["id"])
            await self.database.add_event(
                "job.submitted",
                "job",
                job["id"],
                f"Submitted to {upstream['name']}",
                {"upstream_id": upstream["id"], "prompt_id": prompt_id},
            )
            return

        if not attempted:
            await self.database.update_job(
                job["id"],
                status="queued",
                attempts=max(job["attempts"] - 1, 0),
                prompt_id=None,
                not_before=0,
            )
            return
        await self._retry_or_fail(job, {"message": "all eligible upstreams rejected the job", "attempts": errors})

    async def _upload_assets(self, client: ComfyClient, job: dict[str, Any]) -> dict[str, list[str]]:
        uploaded: dict[str, list[str]] = {}
        subfolder = f"h3_middleware/{job['id']}"
        for kind, assets in job.get("assets", {}).items():
            uploaded[kind] = []
            for asset in assets:
                uploaded[kind].append(await client.upload_asset(asset, subfolder))
        return uploaded

    async def _retry_or_fail(self, job: dict[str, Any], error: dict[str, Any]) -> None:
        latest = await self.database.get_job(job["id"])
        if not latest:
            return
        if latest["cancel_requested"]:
            status = "canceled"
            not_before = 0
        elif latest["attempts"] < latest["max_attempts"]:
            status = "retrying"
            not_before = time.time() + min(30, 2 ** latest["attempts"])
        else:
            status = "failed"
            not_before = 0
        await self.database.update_job(
            job["id"],
            status=status,
            upstream_id=None,
            prompt_id=None,
            error=error,
            not_before=not_before,
            finished_at=time.time() if status in TERMINAL_STATUSES else None,
        )
        await self.database.add_event(
            f"job.{status}",
            "job",
            job["id"],
            error.get("message") or status,
            error,
        )

    async def _monitor_loop(self) -> None:
        while not self.stopping:
            try:
                await self._monitor_jobs()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("middleware monitor failed")
            await asyncio.sleep(self.settings.poll_interval)

    async def _monitor_jobs(self) -> None:
        jobs = await self.database.active_jobs()
        grouped: dict[str, list[dict[str, Any]]] = {}
        for job in jobs:
            if job.get("upstream_id"):
                grouped.setdefault(job["upstream_id"], []).append(job)
        for upstream_id, upstream_jobs in grouped.items():
            upstream = await self.database.get_upstream(upstream_id)
            if upstream:
                await self._monitor_upstream(upstream, upstream_jobs)
        self.wake_scheduler.set()

    async def _monitor_upstream(self, upstream: dict[str, Any], jobs: list[dict[str, Any]]) -> None:
        client = self._client(upstream)
        try:
            queue = await client.queue()
        except ComfyError as exc:
            await self._manager().mark_failure(upstream, exc)
            for job in jobs:
                if job["status"] != "canceling":
                    await self.database.update_job(job["id"], status="upstream_unreachable")
            return

        running = queue_ids(queue.get("queue_running"))
        pending = queue_ids(queue.get("queue_pending"))
        state = self._manager().runtime.get(upstream["id"])
        if state:
            state.healthy = True
            state.failures = 0
            state.error = None
            state.queue_running = len(running)
            state.queue_pending = len(pending)

        for job in jobs:
            prompt_id = job.get("prompt_id")
            if not prompt_id:
                continue
            if job["cancel_requested"]:
                try:
                    if await client.cancel(prompt_id):
                        await self.database.update_job(job["id"], status="canceled", finished_at=time.time())
                        await self.database.add_event("job.canceled", "job", job["id"], f"Canceled on {upstream['name']}")
                        continue
                except ComfyError as exc:
                    await self._manager().mark_failure(upstream, exc)
                    continue
            try:
                history = await client.history(prompt_id)
            except ComfyError as exc:
                await self._manager().mark_failure(upstream, exc)
                continue
            if history is not None and history_complete(history):
                error = history_error(history)
                if error:
                    if job["cancel_requested"]:
                        await self.database.update_job(job["id"], status="canceled", finished_at=time.time(), error=None)
                    elif self.settings.retry_execution_errors:
                        await self._retry_or_fail(job, error)
                    else:
                        await self.database.update_job(job["id"], status="failed", error=error, finished_at=time.time())
                        await self.database.add_event("job.failed", "job", job["id"], error["message"], error)
                else:
                    outputs = extract_outputs(history)
                    await self.database.update_job(
                        job["id"],
                        status="succeeded",
                        outputs=outputs,
                        error=None,
                        finished_at=time.time(),
                    )
                    await self.database.add_event(
                        "job.succeeded",
                        "job",
                        job["id"],
                        f"Generation completed with {len(outputs)} output(s)",
                    )
                continue
            if prompt_id in running:
                await self.database.update_job(
                    job["id"],
                    status="running",
                    started_at=job.get("started_at") or time.time(),
                )
            elif prompt_id in pending:
                await self.database.update_job(job["id"], status="submitted")
            elif time.time() - (job.get("submitted_at") or job["created_at"]) >= self.settings.prompt_missing_timeout:
                await self._retry_or_fail(
                    job,
                    {"message": f"prompt disappeared from {upstream['name']} before producing history", "stage": "monitor"},
                )

    async def _progress_supervisor_loop(self) -> None:
        while not self.stopping:
            try:
                await self._sync_progress_listeners()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("progress listener supervisor failed")
            await asyncio.sleep(max(1.0, self.settings.poll_interval))

    async def _sync_progress_listeners(self) -> None:
        upstreams = await self.database.list_upstreams()
        wanted = {upstream["id"]: upstream for upstream in upstreams}
        stale: list[asyncio.Task[Any]] = []
        for upstream_id, task in list(self.progress_listeners.items()):
            upstream = wanted.get(upstream_id)
            signature = (upstream["base_url"], upstream.get("auth_token")) if upstream else None
            if signature != self.progress_listener_configs.get(upstream_id) or task.done():
                task.cancel()
                stale.append(task)
                self.progress_listeners.pop(upstream_id, None)
                self.progress_listener_configs.pop(upstream_id, None)
        if stale:
            await asyncio.gather(*stale, return_exceptions=True)

        for upstream_id, upstream in wanted.items():
            if upstream_id in self.progress_listeners:
                continue
            self.progress_listener_configs[upstream_id] = (upstream["base_url"], upstream.get("auth_token"))
            self.progress_listeners[upstream_id] = asyncio.create_task(
                self._progress_listener_loop(upstream),
                name=f"h3-progress-{upstream_id}",
            )

    async def _progress_listener_loop(self, upstream: dict[str, Any]) -> None:
        retry_delay = 1.0
        while not self.stopping:
            try:
                async for event in self._client(upstream).events():
                    retry_delay = 1.0
                    await self._handle_progress_event(upstream, event)
                if self.stopping:
                    return
                raise ComfyError("ComfyUI WebSocket closed", transport=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if retry_delay == 1.0:
                    LOGGER.warning("Progress WebSocket for %s is unavailable: %s", upstream["name"], exc)
                else:
                    LOGGER.debug("Progress WebSocket retry for %s failed: %s", upstream["name"], exc)
                await asyncio.sleep(retry_delay)
                retry_delay = min(30.0, retry_delay * 2)

    async def _handle_progress_event(self, upstream: dict[str, Any], event: dict[str, Any]) -> None:
        prompt_id = event_prompt_id(event)
        if prompt_id is None:
            active = [
                job
                for job in await self.database.active_jobs()
                if job.get("upstream_id") == upstream["id"] and job.get("prompt_id")
            ]
            running = [job for job in active if job["status"] == "running"]
            candidates = running if len(running) == 1 else active
            if len(candidates) != 1:
                return
            prompt_id = candidates[0]["prompt_id"]

        job = await self.database.get_job_by_prompt_id(prompt_id)
        if not job or job["status"] in TERMINAL_STATUSES:
            return
        if job.get("upstream_id") and job["upstream_id"] != upstream["id"]:
            return
        progress = apply_progress_event(job.get("progress"), event)
        if progress is None:
            return

        values: dict[str, Any] = {
            "progress": progress,
            "upstream_id": upstream["id"],
        }
        if job["status"] != "canceling":
            values["status"] = "running"
            values["started_at"] = job.get("started_at") or time.time()
        await self.database.update_job(job["id"], **values)

    async def _health_loop(self) -> None:
        while not self.stopping:
            try:
                await self._manager().health_check_all()
                self.wake_scheduler.set()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("upstream health check failed")
            await asyncio.sleep(self.settings.health_interval)

    def _client(self, upstream: dict[str, Any]) -> ComfyClient:
        return self._manager().client(upstream)

    def _manager(self) -> UpstreamManager:
        if self.upstreams is None:
            raise RuntimeError("gateway service is not started")
        return self.upstreams

    async def create_api_key(self, name: str, expires_at: float | None = None) -> tuple[dict[str, Any], str]:
        if not name.strip():
            raise ValueError("API key name is required")
        key, token = await self.database.create_api_key(name.strip(), float(expires_at) if expires_at is not None else None)
        await self.database.add_event("api_key.created", "api_key", key["id"], f"Created API key {key['name']}")
        return key, token

    async def set_api_key_enabled(self, key_id: str, enabled: bool) -> bool:
        changed = await self.database.set_api_key_enabled(key_id, enabled)
        if changed:
            await self.database.add_event(
                "api_key.enabled" if enabled else "api_key.disabled",
                "api_key",
                key_id,
                "Enabled API key" if enabled else "Disabled API key",
            )
        return changed

    async def delete_api_key(self, key_id: str) -> bool:
        changed = await self.database.delete_api_key(key_id)
        if changed:
            await self.database.add_event("api_key.deleted", "api_key", key_id, "Deleted API key")
        return changed

    def schema(self) -> dict[str, Any]:
        return {
            "service": "h3-middleware",
            "adapters": [adapter.schema() for adapter in self.registry.list()],
            "queue_actions": ["front", "back", "before", "after"],
            "job_statuses": [*LOCAL_QUEUE_STATUSES, *ACTIVE_STATUSES, *TERMINAL_STATUSES],
            "progress": {
                "endpoint": "/v1/jobs/{job_id}/progress",
                "phase_examples": ["dit_sampling", "vae_decoding", "video_encoding"],
                "eta_scope": "current_node",
                "workflow_percent_method": "node_weighted",
            },
        }
