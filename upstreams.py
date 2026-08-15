from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from .comfy_client import ComfyClient, ComfyError
from .database import Database


@dataclass
class UpstreamRuntime:
    healthy: bool = False
    failures: int = 0
    last_check: float | None = None
    last_capability_check: float | None = None
    error: str | None = None
    queue_running: int = 0
    queue_pending: int = 0
    node_types: set[str] = field(default_factory=set)
    selections: int = 0


class UpstreamManager:
    def __init__(
        self,
        database: Database,
        session: aiohttp.ClientSession,
        capability_interval: float,
    ) -> None:
        self.database = database
        self.session = session
        self.capability_interval = capability_interval
        self.runtime: dict[str, UpstreamRuntime] = {}
        self._lock = asyncio.Lock()

    def client(self, upstream: dict[str, Any]) -> ComfyClient:
        return ComfyClient(self.session, upstream["base_url"], upstream.get("auth_token"))

    async def health_check_all(self, force_capabilities: bool = False) -> list[dict[str, Any]]:
        upstreams = await self.database.list_upstreams(enabled_only=True)
        await asyncio.gather(
            *(self.health_check(upstream, force_capabilities=force_capabilities) for upstream in upstreams),
            return_exceptions=True,
        )
        return await self.with_runtime(await self.database.list_upstreams())

    async def health_check(self, upstream: dict[str, Any], force_capabilities: bool = False) -> UpstreamRuntime:
        state = self.runtime.setdefault(upstream["id"], UpstreamRuntime())
        client = self.client(upstream)
        timestamp = time.time()
        try:
            stats, queue = await asyncio.gather(client.system_stats(), client.queue())
            state.healthy = True
            state.failures = 0
            state.error = None
            state.queue_running = len(queue.get("queue_running") or [])
            state.queue_pending = len(queue.get("queue_pending") or [])
            if force_capabilities or state.last_capability_check is None or timestamp - state.last_capability_check >= self.capability_interval:
                object_info = await client.object_info()
                state.node_types = set(object_info)
                state.last_capability_check = timestamp
            state.last_check = timestamp
            await self.database.set_upstream_health(upstream["id"], True, None, stats)
        except ComfyError as exc:
            state.healthy = False
            state.failures += 1
            state.error = str(exc)
            state.last_check = timestamp
            await self.database.set_upstream_health(upstream["id"], False, str(exc), None)
        return state

    async def mark_failure(self, upstream: dict[str, Any], error: Exception) -> None:
        state = self.runtime.setdefault(upstream["id"], UpstreamRuntime())
        state.failures += 1
        state.error = str(error)
        if state.failures >= 2:
            state.healthy = False
        await self.database.set_upstream_health(upstream["id"], state.healthy, state.error, None)

    async def candidates(
        self,
        adapter_name: str,
        required_nodes: dict[str, set[str]],
        active_counts: dict[str, int],
        exclude: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        exclude = exclude or set()
        upstreams = await self.database.list_upstreams(enabled_only=True)
        ranked: list[tuple[float, float, dict[str, Any]]] = []
        for upstream in upstreams:
            if upstream["id"] in exclude:
                continue
            if upstream["adapter"] != adapter_name:
                continue
            state = self.runtime.setdefault(upstream["id"], UpstreamRuntime())
            if not state.healthy:
                continue
            required = required_nodes.get(upstream["id"], set())
            if not required.issubset(state.node_types):
                continue
            local_active = active_counts.get(upstream["id"], 0)
            remote_active = state.queue_running + state.queue_pending
            occupied = max(local_active, remote_active)
            if occupied >= upstream["max_concurrency"]:
                continue
            weight = max(float(upstream["weight"]), 0.01)
            score = occupied / weight
            ranked.append((score, state.selections / weight, upstream))
        ranked.sort(key=lambda item: (item[0], item[1], item[2]["name"]))
        return [item[2] for item in ranked]

    def selected(self, upstream_id: str) -> None:
        self.runtime.setdefault(upstream_id, UpstreamRuntime()).selections += 1

    async def with_runtime(self, upstreams: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result = []
        for upstream in upstreams:
            item = dict(upstream)
            state = self.runtime.get(upstream["id"], UpstreamRuntime())
            item["runtime"] = {
                "healthy": state.healthy,
                "failures": state.failures,
                "last_check": state.last_check,
                "error": state.error,
                "queue_running": state.queue_running,
                "queue_pending": state.queue_pending,
                "node_count": len(state.node_types),
            }
            result.append(item)
        return result
