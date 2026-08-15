from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from h3_middleware.database import Database


class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.database = Database(Path(self.tempdir.name) / "test.sqlite3")
        await self.database.initialize()

    async def asyncTearDown(self) -> None:
        await self.database.close()
        self.tempdir.cleanup()

    async def create_job(self, job_id: str) -> None:
        await self.database.create_job(
            job_id=job_id,
            mode="t2va",
            adapter="minimax-h3-native",
            params={"prompt": job_id},
            assets={},
            priority=0,
            max_attempts=2,
            requested_by="test",
        )

    async def test_queue_reorder_and_cancel_are_persistent(self) -> None:
        for job_id in ("job-a", "job-b", "job-c"):
            await self.create_job(job_id)

        await self.database.reorder_job("job-c", "front")
        queue = await self.database.queue_snapshot()
        self.assertEqual([job["id"] for job in queue["items"]], ["job-c", "job-a", "job-b"])

        await self.database.reorder_job("job-c", "after", "job-a")
        queue = await self.database.queue_snapshot()
        self.assertEqual([job["id"] for job in queue["items"]], ["job-a", "job-c", "job-b"])

        job, changed = await self.database.request_cancel("job-c")
        self.assertTrue(changed)
        self.assertEqual(job["status"], "canceled")
        queue = await self.database.queue_snapshot()
        self.assertEqual([job["id"] for job in queue["items"]], ["job-a", "job-b"])

    async def test_api_key_validation_and_disable(self) -> None:
        key, token = await self.database.create_api_key("test")
        self.assertEqual(await self.database.validate_api_key(token), key["id"])
        await self.database.set_api_key_enabled(key["id"], False)
        self.assertIsNone(await self.database.validate_api_key(token))

    async def test_dispatching_jobs_recover_on_restart(self) -> None:
        await self.create_job("job-a")
        claimed = await self.database.claim_job("job-a")
        self.assertEqual(claimed["status"], "dispatching")
        await self.database.close()

        self.database = Database(Path(self.tempdir.name) / "test.sqlite3")
        await self.database.initialize()
        recovered = await self.database.get_job("job-a")
        self.assertEqual(recovered["status"], "queued")
        self.assertIsNone(recovered["upstream_id"])


if __name__ == "__main__":
    unittest.main()
