from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

import aiohttp
from aiohttp.test_utils import TestClient, TestServer

from h3_middleware.server import create_app
from h3_middleware.tests.test_service import FakeComfy, settings


class HttpApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.fake = FakeComfy()
        await self.fake.start()
        app = create_app(settings(Path(self.tempdir.name), (self.fake.base_url,)))
        self.server = TestServer(app)
        self.client = TestClient(self.server, cookie_jar=aiohttp.CookieJar(unsafe=True))
        await self.client.start_server()

    async def asyncTearDown(self) -> None:
        await self.client.close()
        await self.fake.stop()
        self.tempdir.cleanup()

    async def test_auth_login_and_admin_page(self) -> None:
        response = await self.client.get("/v1/schema")
        self.assertEqual(response.status, 401)

        response = await self.client.get("/admin/api/session")
        self.assertEqual(response.status, 200)
        self.assertFalse((await response.json())["authenticated"])

        response = await self.client.get("/admin")
        self.assertEqual(response.status, 200)
        self.assertIn("Gateway Console", await response.text())

        response = await self.client.post("/admin/api/login", json={"token": "wrong"})
        self.assertEqual(response.status, 401)
        response = await self.client.post("/admin/api/login", json={"token": "admin"})
        self.assertEqual(response.status, 200)
        response = await self.client.get("/admin/api/session")
        self.assertTrue((await response.json())["authenticated"])
        response = await self.client.get("/admin/api/summary")
        self.assertEqual(response.status, 200)
        response = await self.client.get("/v1/schema")
        self.assertEqual(response.status, 200)

    async def test_json_generation_lifecycle(self) -> None:
        headers = {"Authorization": "Bearer api"}
        response = await self.client.post(
            "/v1/generations",
            json={"prompt": "clouds", "mode": "t2v", "noise_seed": 7},
            headers=headers,
        )
        self.assertEqual(response.status, 202)
        payload = await response.json()
        job_id = payload["job_id"]

        deadline = asyncio.get_running_loop().time() + 3
        while True:
            response = await self.client.get(f"/v1/jobs/{job_id}", headers=headers)
            job = (await response.json())["job"]
            if job["status"] == "succeeded":
                break
            if asyncio.get_running_loop().time() >= deadline:
                self.fail("job did not complete")
            await asyncio.sleep(0.02)
        self.assertEqual(job["mode"], "t2va")
        response = await self.client.get(f"/v1/jobs/{job_id}/video", headers=headers)
        self.assertEqual(response.status, 200)
        self.assertEqual(await response.read(), b"video-data")

    async def test_progress_endpoint_reports_comfy_node_steps(self) -> None:
        self.fake.complete_after = 1000
        headers = {"Authorization": "Bearer api"}
        response = await self.client.post(
            "/v1/generations",
            json={"prompt": "clouds", "steps": 20},
            headers=headers,
        )
        self.assertEqual(response.status, 202)
        job_id = (await response.json())["job_id"]

        deadline = asyncio.get_running_loop().time() + 3
        prompt_id = None
        while prompt_id is None:
            response = await self.client.get(f"/v1/jobs/{job_id}", headers=headers)
            prompt_id = (await response.json())["job"].get("prompt_id")
            if asyncio.get_running_loop().time() >= deadline:
                self.fail("job was not submitted to fake ComfyUI")
            if prompt_id is None:
                await asyncio.sleep(0.02)

        await self.fake.emit(
            prompt_id,
            "progress_state",
            {
                "nodes": {
                    "10": {
                        "node_id": "10",
                        "state": "running",
                        "value": 6,
                        "max": 20,
                    }
                }
            },
        )

        while True:
            response = await self.client.get(f"/v1/jobs/{job_id}/progress", headers=headers)
            payload = await response.json()
            progress = payload["progress"]
            if (progress.get("step") or {}).get("value") == 6:
                break
            if asyncio.get_running_loop().time() >= deadline:
                self.fail("progress endpoint did not receive the ComfyUI event")
            await asyncio.sleep(0.02)

        self.assertEqual(response.status, 200)
        self.assertEqual(payload["status"], "running")
        self.assertEqual(progress["source"], "comfy_websocket")
        self.assertEqual(progress["phase"], "dit_sampling")
        self.assertEqual(progress["node"]["type"], "SamplerCustomAdvanced")
        self.assertEqual(progress["step"], {"value": 6, "max": 20, "percent": 30.0})

        response = await self.client.get(f"/v1/jobs/{job_id}", headers=headers)
        job = (await response.json())["job"]
        self.assertEqual(job["progress"]["step"]["value"], 6)

    async def test_multipart_legacy_i2va_and_missing_image(self) -> None:
        headers = {"Authorization": "Bearer api"}
        response = await self.client.post("/v1/i2va", data={"prompt": "move"}, headers=headers)
        self.assertEqual(response.status, 400)
        payload = await response.json()
        self.assertIn("first_frame", payload["error"]["message"])

        form = aiohttp.FormData()
        form.add_field("prompt", "move")
        form.add_field("image", b"fake-png", filename="first.png", content_type="image/png")
        response = await self.client.post("/v1/i2va", data=form, headers=headers)
        self.assertEqual(response.status, 202)
        payload = await response.json()
        self.assertEqual(payload["job"]["mode"], "i2va")

    async def test_invalid_sync_timeout_does_not_create_a_job(self) -> None:
        headers = {"Authorization": "Bearer api"}
        response = await self.client.post(
            "/v1/generations/sync",
            json={"prompt": "clouds", "timeout_sec": "invalid"},
            headers=headers,
        )
        self.assertEqual(response.status, 400)
        response = await self.client.get("/v1/jobs", headers=headers)
        self.assertEqual((await response.json())["total"], 0)

    async def test_admin_queue_control_and_api_key(self) -> None:
        await self.client.post("/admin/api/login", json={"token": "admin"})
        response = await self.client.post("/admin/api/queue/pause")
        self.assertEqual(response.status, 200)

        response = await self.client.post("/admin/api/api-keys", json={"name": "client"})
        self.assertEqual(response.status, 201)
        token = (await response.json())["token"]
        headers = {"X-API-Key": token}
        ids = []
        for prompt in ("first", "second"):
            response = await self.client.post("/v1/generations", json={"prompt": prompt}, headers=headers)
            self.assertEqual(response.status, 202)
            ids.append((await response.json())["job_id"])

        response = await self.client.patch(
            f"/v1/jobs/{ids[1]}/queue", json={"action": "front"}, headers=headers
        )
        self.assertEqual(response.status, 200)
        response = await self.client.get("/v1/queue", headers=headers)
        queue = (await response.json())["queue"]
        self.assertEqual([job["id"] for job in queue["items"]], [ids[1], ids[0]])

        response = await self.client.post(f"/v1/jobs/{ids[0]}/cancel", headers=headers)
        self.assertEqual(response.status, 200)
        self.assertTrue((await response.json())["canceled"])

    async def test_admin_can_disable_and_enable_an_upstream(self) -> None:
        await self.client.post("/admin/api/login", json={"token": "admin"})
        response = await self.client.get("/admin/api/upstreams")
        upstream_id = (await response.json())["upstreams"][0]["id"]

        response = await self.client.patch(f"/admin/api/upstreams/{upstream_id}", json={"enabled": False})
        self.assertEqual(response.status, 200)
        self.assertFalse((await response.json())["upstream"]["enabled"])
        response = await self.client.get("/health")
        health = await response.json()
        self.assertFalse(health["ok"])
        self.assertEqual(health["healthy_upstreams"], 0)

        response = await self.client.patch(f"/admin/api/upstreams/{upstream_id}", json={"enabled": True})
        self.assertEqual(response.status, 200)
        upstream = (await response.json())["upstream"]
        self.assertTrue(upstream["enabled"])
        self.assertTrue(upstream["runtime"]["healthy"])

    async def test_admin_exposes_comfy_atomic_controls(self) -> None:
        await self.client.post("/admin/api/login", json={"token": "admin"})
        response = await self.client.get("/admin/api/upstreams")
        upstream_id = (await response.json())["upstreams"][0]["id"]

        response = await self.client.get(f"/admin/api/upstreams/{upstream_id}/prompt")
        self.assertEqual(response.status, 200)
        response = await self.client.get(f"/admin/api/upstreams/{upstream_id}/history")
        self.assertEqual(response.status, 200)
        response = await self.client.post(f"/admin/api/upstreams/{upstream_id}/interrupt")
        self.assertEqual(response.status, 200)
        self.assertEqual(self.fake.global_interrupts, 1)
        response = await self.client.post(f"/admin/api/upstreams/{upstream_id}/history/clear")
        self.assertEqual(response.status, 200)
        self.assertTrue(self.fake.history_cleared)
        response = await self.client.post(
            f"/admin/api/upstreams/{upstream_id}/prompts/cancel", json={"prompt_ids": []}
        )
        self.assertEqual(response.status, 200)


if __name__ == "__main__":
    unittest.main()
