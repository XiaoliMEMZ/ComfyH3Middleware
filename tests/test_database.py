from __future__ import annotations

import sqlite3
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

    async def test_group_assignments_and_queue_order_are_isolated(self) -> None:
        first = await self.database.create_group("GPU group A")
        second = await self.database.create_group("GPU group B")
        upstream = await self.database.create_upstream(
            {
                "name": "GPU 0",
                "base_url": "http://127.0.0.1:8188",
                "group_ids": [first["id"], second["id"]],
            }
        )
        key, _ = await self.database.create_api_key("group client", group_id=first["id"])
        await self.create_job("job-a")
        await self.database.create_job(
            "job-b", "t2va", "minimax-h3-native", {}, {}, 0, 2, key["id"], group_id=first["id"]
        )
        await self.database.create_job(
            "job-c", "t2va", "minimax-h3-native", {}, {}, 0, 2, "other", group_id=second["id"]
        )

        self.assertEqual(upstream["group_id"], first["id"])
        self.assertEqual(set(upstream["group_ids"]), {first["id"], second["id"]})
        self.assertEqual({group["name"] for group in upstream["groups"]}, {"GPU group A", "GPU group B"})
        self.assertEqual((await self.database.get_api_key(key["id"]))["group_name"], "GPU group A")
        group_queue = await self.database.queue_snapshot(first["id"])
        self.assertEqual([job["id"] for job in group_queue["items"]], ["job-b"])
        await self.database.reorder_job("job-b", "front")
        self.assertEqual(
            [job["id"] for job in (await self.database.queue_snapshot(first["id"]))["items"]],
            ["job-b"],
        )
        with self.assertRaisesRegex(ValueError, "same queue group"):
            await self.database.reorder_job("job-b", "before", "job-c")

        with self.assertRaisesRegex(ValueError, "assigned upstreams"):
            await self.database.delete_group(first["id"])
        await self.database.set_api_key_group(key["id"], None)
        updated = await self.database.update_upstream(upstream["id"], {"group_ids": [second["id"]]})
        self.assertEqual(updated["group_ids"], [second["id"]])
        self.assertEqual(updated["group_id"], second["id"])
        await self.database.update_job("job-b", group_id=None)
        await self.database.delete_group(first["id"])

    async def test_single_group_database_backfills_membership_table(self) -> None:
        group = await self.database.create_group("Legacy group")
        upstream = await self.database.create_upstream(
            {"name": "GPU 0", "base_url": "http://127.0.0.1:8188", "group_id": group["id"]}
        )
        async with self.database.lock:
            self.database.conn.execute("DROP TABLE upstream_group_memberships")
            self.database.conn.commit()
        await self.database.close()

        self.database = Database(Path(self.tempdir.name) / "test.sqlite3")
        await self.database.initialize()
        migrated = await self.database.get_upstream(upstream["id"])
        self.assertEqual(migrated["group_ids"], [group["id"]])
        self.assertEqual(migrated["groups"][0]["name"], "Legacy group")

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

    async def test_progress_is_persistent(self) -> None:
        await self.create_job("job-a")
        progress = {
            "phase": "dit_sampling",
            "step": {"value": 4, "max": 20, "percent": 20.0},
        }
        await self.database.update_job("job-a", progress=progress)
        await self.database.close()

        self.database = Database(Path(self.tempdir.name) / "test.sqlite3")
        await self.database.initialize()
        job = await self.database.get_job("job-a")
        self.assertEqual(job["progress"], progress)

    async def test_existing_database_gets_progress_column(self) -> None:
        await self.create_job("job-a")
        async with self.database.lock:
            self.database.conn.execute("ALTER TABLE jobs DROP COLUMN progress_json")
            self.database.conn.commit()
        await self.database.close()

        self.database = Database(Path(self.tempdir.name) / "test.sqlite3")
        await self.database.initialize()
        columns = {row["name"] for row in self.database.conn.execute("PRAGMA table_info(jobs)")}
        self.assertIn("progress_json", columns)
        self.assertEqual((await self.database.get_job("job-a"))["progress"], {})

    async def test_legacy_database_gets_group_columns_without_data_loss(self) -> None:
        legacy_path = Path(self.tempdir.name) / "legacy.sqlite3"
        connection = sqlite3.connect(legacy_path)
        connection.executescript(
            """
            CREATE TABLE upstreams (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, base_url TEXT NOT NULL UNIQUE,
                enabled INTEGER NOT NULL DEFAULT 1, weight REAL NOT NULL DEFAULT 1,
                max_concurrency INTEGER NOT NULL DEFAULT 1, adapter TEXT NOT NULL DEFAULT 'minimax-h3-native',
                auth_token TEXT, options_json TEXT NOT NULL DEFAULT '{}', healthy INTEGER NOT NULL DEFAULT 0,
                last_check REAL, last_error TEXT, stats_json TEXT NOT NULL DEFAULT '{}',
                created_at REAL NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE api_keys (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, token_hash TEXT NOT NULL UNIQUE,
                token_prefix TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, created_at REAL NOT NULL,
                last_used_at REAL, expires_at REAL
            );
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY, status TEXT NOT NULL, mode TEXT NOT NULL, adapter TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0, queue_order INTEGER NOT NULL, params_json TEXT NOT NULL,
                assets_json TEXT NOT NULL, outputs_json TEXT NOT NULL DEFAULT '[]', error_json TEXT,
                upstream_id TEXT, prompt_id TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 2, not_before REAL NOT NULL DEFAULT 0,
                cancel_requested INTEGER NOT NULL DEFAULT 0, requested_by TEXT, created_at REAL NOT NULL,
                updated_at REAL NOT NULL, submitted_at REAL, started_at REAL, finished_at REAL
            );
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, subject_type TEXT NOT NULL,
                subject_id TEXT, message TEXT NOT NULL, data_json TEXT NOT NULL DEFAULT '{}', created_at REAL NOT NULL
            );
            CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at REAL NOT NULL);
            INSERT INTO upstreams(id, name, base_url, created_at, updated_at)
                VALUES('legacy-upstream', 'Legacy GPU', 'http://127.0.0.1:8188', 1, 1);
            """
        )
        connection.commit()
        connection.close()

        legacy_database = Database(legacy_path)
        await legacy_database.initialize()
        try:
            columns = {
                table: {row["name"] for row in legacy_database.conn.execute(f"PRAGMA table_info({table})")}
                for table in ("upstreams", "api_keys", "jobs")
            }
            self.assertIn("group_id", columns["upstreams"])
            self.assertIn("group_id", columns["api_keys"])
            self.assertIn("group_id", columns["jobs"])
            self.assertIn("progress_json", columns["jobs"])
            tables = {
                row["name"]
                for row in legacy_database.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            self.assertIn("upstream_group_memberships", tables)
            self.assertEqual((await legacy_database.get_upstream("legacy-upstream"))["name"], "Legacy GPU")
        finally:
            await legacy_database.close()


if __name__ == "__main__":
    unittest.main()
