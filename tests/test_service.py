from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from typing import Any

from aiohttp import web

from h3_middleware.config import Settings
from h3_middleware.service import GatewayService
from h3_middleware.workflows.minimax_h3 import MiniMaxH3Adapter


class FakeComfy:
    def __init__(
        self,
        reject_prompts: bool = False,
        reject_uploads: bool = False,
        conditioning_node: str = "MiniMaxH3ImageToVideo",
        complete_after: int = 2,
    ) -> None:
        self.reject_prompts = reject_prompts
        self.reject_uploads = reject_uploads
        self.conditioning_node = conditioning_node
        self.complete_after = complete_after
        self.prompts: dict[str, dict[str, Any]] = {}
        self.canceled: set[str] = set()
        self.history_polls: dict[str, int] = {}
        self.uploads = 0
        self.global_interrupts = 0
        self.history_cleared = False
        self.sockets: dict[str, web.WebSocketResponse] = {}
        self.prompt_clients: dict[str, str] = {}
        self.runner: web.AppRunner | None = None
        self.base_url = ""

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/system_stats", self.system_stats)
        app.router.add_get("/ws", self.websocket)
        app.router.add_get("/object_info", self.object_info)
        app.router.add_get("/queue", self.queue)
        app.router.add_get("/prompt", self.prompt_status)
        app.router.add_post("/prompt", self.prompt)
        app.router.add_post("/upload/image", self.upload)
        app.router.add_get("/history", self.history_all)
        app.router.add_get("/history/{prompt_id}", self.history)
        app.router.add_post("/history", self.history_control)
        app.router.add_post("/api/jobs/{prompt_id}/cancel", self.cancel)
        app.router.add_post("/api/jobs/cancel", self.cancel_many)
        app.router.add_get("/view", self.view)
        app.router.add_post("/free", self.ok)
        app.router.add_post("/queue", self.ok)
        app.router.add_post("/interrupt", self.interrupt)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        port = self.runner.addresses[0][1]
        self.base_url = f"http://127.0.0.1:{port}"

    async def stop(self) -> None:
        if self.runner:
            await self.runner.cleanup()

    async def system_stats(self, request: web.Request) -> web.Response:
        return web.json_response({"system": {"comfyui_version": "test"}, "devices": []})

    async def websocket(self, request: web.Request) -> web.WebSocketResponse:
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        client_id = request.query.get("clientId") or "anonymous"
        self.sockets[client_id] = websocket
        await websocket.send_json({"type": "status", "data": {"status": {"exec_info": {"queue_remaining": 0}}}})
        try:
            async for _ in websocket:
                pass
        finally:
            if self.sockets.get(client_id) is websocket:
                self.sockets.pop(client_id, None)
        return websocket

    async def object_info(self, request: web.Request) -> web.Response:
        adapter = MiniMaxH3Adapter()
        nodes = set()
        for mode in ("t2va", "i2va", "fl2va", "ref2va"):
            nodes.update(adapter.required_nodes(mode, {"conditioning_node": self.conditioning_node}))
        nodes.update({"GetVideoComponents", "MiniMaxH3SigmaShift", "PrimitiveFloat", "ComfyMathExpression"})
        return web.json_response({node: {} for node in nodes})

    async def queue(self, request: web.Request) -> web.Response:
        running = [
            [0, prompt_id, {}, {}, []]
            for prompt_id in self.prompts
            if prompt_id not in self.canceled and self.history_polls.get(prompt_id, 0) < self.complete_after
        ]
        return web.json_response({"queue_running": running, "queue_pending": []})

    async def prompt_status(self, request: web.Request) -> web.Response:
        return web.json_response({"exec_info": {"queue_remaining": len(self.prompts)}})

    async def prompt(self, request: web.Request) -> web.Response:
        body = await request.json()
        if self.reject_prompts:
            return web.json_response({"error": {"message": "rejected"}, "node_errors": {}}, status=503)
        prompt_id = body["prompt_id"]
        self.prompts[prompt_id] = body["prompt"]
        self.prompt_clients[prompt_id] = body["client_id"]
        return web.json_response({"prompt_id": prompt_id, "number": len(self.prompts), "node_errors": {}})

    async def emit(self, prompt_id: str, event_type: str, data: dict[str, Any]) -> None:
        client_id = self.prompt_clients[prompt_id]
        deadline = asyncio.get_running_loop().time() + 2
        while client_id not in self.sockets or self.sockets[client_id].closed:
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError(f"WebSocket client {client_id} did not connect")
            await asyncio.sleep(0.01)
        await self.sockets[client_id].send_json(
            {"type": event_type, "data": {"prompt_id": prompt_id, **data}}
        )

    async def upload(self, request: web.Request) -> web.Response:
        self.uploads += 1
        if self.reject_uploads:
            return web.json_response({"error": "upload unavailable"}, status=503)
        post = await request.post()
        file = post["image"]
        return web.json_response({"name": file.filename, "subfolder": post.get("subfolder", ""), "type": "input"})

    async def history(self, request: web.Request) -> web.Response:
        prompt_id = request.match_info["prompt_id"]
        if prompt_id not in self.prompts or prompt_id in self.canceled:
            return web.json_response({})
        polls = self.history_polls.get(prompt_id, 0) + 1
        self.history_polls[prompt_id] = polls
        if polls < self.complete_after:
            return web.json_response({})
        return web.json_response(
            {
                prompt_id: {
                    "status": {"completed": True, "status_str": "success", "messages": []},
                    "outputs": {
                        "14": {
                            "videos": [
                                {"filename": f"{prompt_id}.mp4", "subfolder": "video", "type": "output"}
                            ]
                        }
                    },
                }
            }
        )

    async def history_all(self, request: web.Request) -> web.Response:
        return web.json_response({prompt_id: {} for prompt_id in self.prompts})

    async def history_control(self, request: web.Request) -> web.Response:
        body = await request.json()
        if body.get("clear"):
            self.history_cleared = True
        return web.json_response({})

    async def cancel(self, request: web.Request) -> web.Response:
        prompt_id = request.match_info["prompt_id"]
        canceled = prompt_id in self.prompts and prompt_id not in self.canceled
        self.canceled.add(prompt_id)
        return web.json_response({"cancelled": canceled})

    async def cancel_many(self, request: web.Request) -> web.Response:
        body = await request.json()
        canceled = False
        for prompt_id in body.get("job_ids") or []:
            if prompt_id in self.prompts and prompt_id not in self.canceled:
                self.canceled.add(prompt_id)
                canceled = True
        return web.json_response({"cancelled": canceled})

    async def interrupt(self, request: web.Request) -> web.Response:
        body = await request.json()
        if not body.get("prompt_id"):
            self.global_interrupts += 1
        return web.json_response({})

    async def view(self, request: web.Request) -> web.Response:
        return web.Response(body=b"video-data", content_type="video/mp4")

    async def ok(self, request: web.Request) -> web.Response:
        return web.json_response({})


def settings(root: Path, upstreams: tuple[str, ...]) -> Settings:
    return Settings(
        host="127.0.0.1",
        port=0,
        data_dir=root,
        database_path=root / "gateway.sqlite3",
        asset_dir=root / "assets",
        admin_token="admin",
        bootstrap_api_token="api",
        initial_upstreams=upstreams,
        poll_interval=0.02,
        health_interval=60,
        capability_interval=60,
        request_timeout=2,
        sync_timeout=2,
        prompt_missing_timeout=0.5,
        max_upload_bytes=1024 * 1024,
        max_request_bytes=2 * 1024 * 1024,
        default_max_attempts=2,
        conditioning_node="MiniMaxH3ImageToVideo",
        retry_execution_errors=False,
    )


async def wait_status(service: GatewayService, job_id: str, statuses: set[str], timeout: float = 3) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        job = await service.database.get_job(job_id)
        if job and job["status"] in statuses:
            return job
        await asyncio.sleep(0.02)
    raise AssertionError(f"job {job_id} did not reach {statuses}")


async def wait_progress(service: GatewayService, job_id: str, predicate, timeout: float = 3) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        job = await service.get_job(job_id)
        if job and predicate(job["progress"]):
            return job["progress"]
        await asyncio.sleep(0.02)
    raise AssertionError(f"job {job_id} did not report expected progress")


class GatewayServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.fakes: list[FakeComfy] = []
        self.service: GatewayService | None = None

    async def asyncTearDown(self) -> None:
        if self.service:
            await self.service.stop()
        for fake in self.fakes:
            await fake.stop()
        self.tempdir.cleanup()

    async def fake(
        self,
        reject_prompts: bool = False,
        reject_uploads: bool = False,
        conditioning_node: str = "MiniMaxH3ImageToVideo",
        complete_after: int = 2,
    ) -> FakeComfy:
        value = FakeComfy(reject_prompts, reject_uploads, conditioning_node, complete_after)
        await value.start()
        self.fakes.append(value)
        return value

    async def test_dispatch_completion_and_output_proxy(self) -> None:
        upstream = await self.fake()
        self.service = GatewayService(settings(Path(self.tempdir.name), (upstream.base_url,)))
        await self.service.start()
        job_id = "11111111-1111-4111-8111-111111111111"
        await self.service.submit(job_id, {"prompt": "clouds", "noise_seed": 3}, {}, "test")
        job = await wait_status(self.service, job_id, {"succeeded"})
        self.assertEqual(job["upstream_id"], (await self.service.database.list_upstreams())[0]["id"])
        self.assertEqual(job["outputs"][0]["kind"], "video")
        data, content_type, filename = await self.service.get_video(job_id)
        self.assertEqual(data, b"video-data")
        self.assertEqual(content_type, "video/mp4")
        self.assertTrue(filename.endswith(".mp4"))

    async def test_websocket_reports_node_step_phase_and_eta(self) -> None:
        upstream = await self.fake(complete_after=1000)
        self.service = GatewayService(settings(Path(self.tempdir.name), (upstream.base_url,)))
        await self.service.start()
        job_id = "11111111-1111-4111-8111-111111111112"
        await self.service.submit(job_id, {"prompt": "clouds", "steps": 20}, {}, "test")
        job = await wait_status(self.service, job_id, {"submitted", "running"})
        prompt_id = job["prompt_id"]
        self.assertTrue(upstream.prompt_clients[prompt_id].startswith("h3-middleware-"))

        await upstream.emit(
            prompt_id,
            "progress_state",
            {
                "nodes": {
                    "10": {
                        "node_id": "10",
                        "prompt_id": prompt_id,
                        "state": "running",
                        "value": 4,
                        "max": 20,
                    }
                }
            },
        )
        progress = await wait_progress(self.service, job_id, lambda value: (value.get("step") or {}).get("value") == 4)
        self.assertEqual(progress["source"], "comfy_websocket")
        self.assertEqual(progress["phase"], "dit_sampling")
        self.assertEqual(progress["node"]["type"], "SamplerCustomAdvanced")
        self.assertEqual(progress["step"], {"value": 4, "max": 20, "percent": 20.0})
        self.assertGreater(progress["workflow"]["total_nodes"], 10)

        await asyncio.sleep(0.05)
        await upstream.emit(
            prompt_id,
            "progress_state",
            {
                "nodes": {
                    "10": {
                        "node_id": "10",
                        "prompt_id": prompt_id,
                        "state": "running",
                        "value": 8,
                        "max": 20,
                    }
                }
            },
        )
        progress = await wait_progress(self.service, job_id, lambda value: (value.get("step") or {}).get("value") == 8)
        self.assertGreater(progress["eta_seconds"], 0)
        self.assertEqual(progress["eta_scope"], "current_node")

        await upstream.emit(prompt_id, "executing", {"node": "11", "display_node": "11"})
        progress = await wait_progress(self.service, job_id, lambda value: (value.get("node") or {}).get("id") == "11")
        self.assertEqual(progress["phase"], "vae_decoding")
        self.assertEqual(progress["node"]["type"], "VAEDecode")

    async def test_dispatch_fails_over_to_second_upstream(self) -> None:
        rejected = await self.fake(reject_prompts=True)
        accepted = await self.fake()
        self.service = GatewayService(settings(Path(self.tempdir.name), (rejected.base_url, accepted.base_url)))
        await self.service.start()
        job_id = "22222222-2222-4222-8222-222222222222"
        await self.service.submit(job_id, {"prompt": "clouds"}, {}, "test")
        job = await wait_status(self.service, job_id, {"succeeded"})
        upstreams = await self.service.database.list_upstreams()
        accepted_config = next(item for item in upstreams if item["base_url"] == accepted.base_url)
        self.assertEqual(job["upstream_id"], accepted_config["id"])
        self.assertNotIn(job["prompt_id"], rejected.prompts)
        self.assertIn(job["prompt_id"], accepted.prompts)

    async def test_asset_upload_fails_over_to_second_upstream(self) -> None:
        rejected = await self.fake(reject_uploads=True)
        accepted = await self.fake()
        root = Path(self.tempdir.name)
        asset_path = root / "first.png"
        asset_path.write_bytes(b"fake-png")
        assets = {
            "first_frame": [
                {
                    "path": str(asset_path),
                    "filename": "first.png",
                    "content_type": "image/png",
                    "size": asset_path.stat().st_size,
                }
            ]
        }
        self.service = GatewayService(settings(root, (rejected.base_url, accepted.base_url)))
        await self.service.start()
        job_id = "22222222-2222-4222-8222-222222222223"
        await self.service.submit(job_id, {"prompt": "clouds", "mode": "i2va"}, assets, "test")
        job = await wait_status(self.service, job_id, {"succeeded"})
        upstreams = await self.service.database.list_upstreams()
        accepted_config = next(item for item in upstreams if item["base_url"] == accepted.base_url)
        self.assertEqual(job["upstream_id"], accepted_config["id"])
        self.assertEqual(rejected.uploads, 1)
        self.assertEqual(accepted.uploads, 1)
        self.assertIn(job["prompt_id"], accepted.prompts)

    async def test_retry_uses_a_new_comfy_prompt_id(self) -> None:
        upstream = await self.fake()
        self.service = GatewayService(settings(Path(self.tempdir.name), (upstream.base_url,)))
        await self.service.start()
        job_id = "22222222-2222-4222-8222-222222222224"
        await self.service.submit(job_id, {"prompt": "clouds"}, {}, "test")
        first = await wait_status(self.service, job_id, {"succeeded"})
        first_prompt_id = first["prompt_id"]
        await self.service.retry_job(job_id)
        await self.service._manager().health_check_all()
        second = await wait_status(self.service, job_id, {"succeeded"})
        self.assertNotEqual(second["prompt_id"], first_prompt_id)
        self.assertIn(first_prompt_id, upstream.prompts)
        self.assertIn(second["prompt_id"], upstream.prompts)

    async def test_idle_upstreams_use_weighted_round_robin(self) -> None:
        first = await self.fake()
        second = await self.fake()
        self.service = GatewayService(settings(Path(self.tempdir.name), (first.base_url, second.base_url)))
        await self.service.start()
        upstreams = await self.service.database.list_upstreams()
        high = next(item for item in upstreams if item["base_url"] == first.base_url)
        low = next(item for item in upstreams if item["base_url"] == second.base_url)
        await self.service.database.update_upstream(high["id"], {"weight": 3})
        required = {
            item["id"]: MiniMaxH3Adapter().required_nodes("t2va")
            for item in upstreams
        }
        selected = {high["id"]: 0, low["id"]: 0}
        for _ in range(8):
            candidates = await self.service._manager().candidates(
                "minimax-h3-native", required, active_counts={}
            )
            upstream_id = candidates[0]["id"]
            selected[upstream_id] += 1
            self.service._manager().selected(upstream_id)
        self.assertEqual(selected[high["id"]], 6)
        self.assertEqual(selected[low["id"]], 2)

    async def test_running_job_uses_atomic_comfy_cancel(self) -> None:
        upstream = await self.fake(complete_after=1000)
        self.service = GatewayService(settings(Path(self.tempdir.name), (upstream.base_url,)))
        await self.service.start()
        job_id = "22222222-2222-4222-8222-222222222225"
        await self.service.submit(job_id, {"prompt": "clouds"}, {}, "test")
        active = await wait_status(self.service, job_id, {"submitted", "running"})
        job, canceled = await self.service.cancel_job(job_id)
        self.assertTrue(canceled)
        self.assertEqual(job["status"], "canceled")
        self.assertIn(active["prompt_id"], upstream.canceled)

    async def test_upstream_can_select_a_staged_conditioning_node(self) -> None:
        staged_node = "MiniMaxH3ImageToVideoStaged"
        upstream = await self.fake(conditioning_node=staged_node)
        self.service = GatewayService(settings(Path(self.tempdir.name), (upstream.base_url,)))
        await self.service.start()
        config = (await self.service.database.list_upstreams())[0]
        await self.service.database.update_upstream(
            config["id"], {"options": {"conditioning_node": staged_node}}
        )
        job_id = "22222222-2222-4222-8222-222222222226"
        await self.service.submit(job_id, {"prompt": "clouds"}, {}, "test")
        job = await wait_status(self.service, job_id, {"succeeded"})
        self.assertEqual(upstream.prompts[job["prompt_id"]]["20"]["class_type"], staged_node)

    async def test_local_queue_reorder_and_cancel(self) -> None:
        upstream = await self.fake()
        self.service = GatewayService(settings(Path(self.tempdir.name), (upstream.base_url,)))
        await self.service.start()
        await self.service.set_queue_paused(True)
        ids = [
            "33333333-3333-4333-8333-333333333331",
            "33333333-3333-4333-8333-333333333332",
            "33333333-3333-4333-8333-333333333333",
        ]
        for job_id in ids:
            await self.service.submit(job_id, {"prompt": job_id}, {}, "test")
        await self.service.reorder_job(ids[2], action="front")
        queue = await self.service.queue_snapshot()
        self.assertEqual([item["id"] for item in queue["items"]], [ids[2], ids[0], ids[1]])
        job, canceled = await self.service.cancel_job(ids[0])
        self.assertTrue(canceled)
        self.assertEqual(job["status"], "canceled")
        self.assertNotIn(ids[0], upstream.prompts)


if __name__ == "__main__":
    unittest.main()
