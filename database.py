from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any


LOCAL_QUEUE_STATUSES = ("queued", "retrying")
ACTIVE_STATUSES = ("dispatching", "submitted", "running", "canceling", "upstream_unreachable")
TERMINAL_STATUSES = ("succeeded", "failed", "canceled")

JOB_JSON_COLUMNS = {
    "params_json": "params",
    "assets_json": "assets",
    "outputs_json": "outputs",
    "error_json": "error",
}
UPSTREAM_JSON_COLUMNS = {
    "options_json": "options",
    "stats_json": "stats",
}


def now() -> float:
    return time.time()


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _decode_json(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.connection: sqlite3.Connection | None = None
        self.lock = asyncio.Lock()

    async def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=5000")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS upstreams (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                base_url TEXT NOT NULL UNIQUE,
                enabled INTEGER NOT NULL DEFAULT 1,
                weight REAL NOT NULL DEFAULT 1,
                max_concurrency INTEGER NOT NULL DEFAULT 1,
                adapter TEXT NOT NULL DEFAULT 'minimax-h3-native',
                auth_token TEXT,
                options_json TEXT NOT NULL DEFAULT '{}',
                healthy INTEGER NOT NULL DEFAULT 0,
                last_check REAL,
                last_error TEXT,
                stats_json TEXT NOT NULL DEFAULT '{}',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS api_keys (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                token_prefix TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at REAL NOT NULL,
                last_used_at REAL,
                expires_at REAL
            );

            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                mode TEXT NOT NULL,
                adapter TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                queue_order INTEGER NOT NULL,
                params_json TEXT NOT NULL,
                assets_json TEXT NOT NULL,
                outputs_json TEXT NOT NULL DEFAULT '[]',
                error_json TEXT,
                upstream_id TEXT,
                prompt_id TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 2,
                not_before REAL NOT NULL DEFAULT 0,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                requested_by TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                submitted_at REAL,
                started_at REAL,
                finished_at REAL,
                FOREIGN KEY (upstream_id) REFERENCES upstreams(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS jobs_queue_idx
                ON jobs(status, priority DESC, queue_order ASC, created_at ASC);
            CREATE INDEX IF NOT EXISTS jobs_upstream_idx ON jobs(upstream_id, status);
            CREATE INDEX IF NOT EXISTS jobs_created_idx ON jobs(created_at DESC);

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                subject_type TEXT NOT NULL,
                subject_id TEXT,
                message TEXT NOT NULL,
                data_json TEXT NOT NULL DEFAULT '{}',
                created_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS events_created_idx ON events(created_at DESC);

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            );
            """
        )
        connection.execute(
            "INSERT OR IGNORE INTO settings(key, value, updated_at) VALUES('queue_paused', 'false', ?)",
            (now(),),
        )
        connection.execute(
            "UPDATE jobs SET status='queued', upstream_id=NULL, prompt_id=NULL, updated_at=? WHERE status='dispatching'",
            (now(),),
        )
        connection.commit()
        self.connection = connection

    async def close(self) -> None:
        async with self.lock:
            if self.connection is not None:
                self.connection.close()
                self.connection = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self.connection is None:
            raise RuntimeError("database is not initialized")
        return self.connection

    async def seed_upstreams(self, base_urls: tuple[str, ...]) -> None:
        async with self.lock:
            timestamp = now()
            for index, base_url in enumerate(base_urls, start=1):
                base_url = base_url.rstrip("/")
                upstream_id = str(uuid.uuid5(uuid.NAMESPACE_URL, base_url))
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO upstreams(
                        id, name, base_url, enabled, weight, max_concurrency,
                        adapter, created_at, updated_at
                    ) VALUES(?, ?, ?, 1, 1, 1, 'minimax-h3-native', ?, ?)
                    """,
                    (upstream_id, f"ComfyUI {index}", base_url, timestamp, timestamp),
                )
            self.conn.commit()

    async def create_upstream(self, values: dict[str, Any]) -> dict[str, Any]:
        upstream_id = str(uuid.uuid4())
        timestamp = now()
        async with self.lock:
            self.conn.execute(
                """
                INSERT INTO upstreams(
                    id, name, base_url, enabled, weight, max_concurrency,
                    adapter, auth_token, options_json, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    upstream_id,
                    values["name"],
                    values["base_url"].rstrip("/"),
                    int(values.get("enabled", True)),
                    float(values.get("weight", 1)),
                    int(values.get("max_concurrency", 1)),
                    values.get("adapter", "minimax-h3-native"),
                    values.get("auth_token") or None,
                    _json(values.get("options") or {}),
                    timestamp,
                    timestamp,
                ),
            )
            self.conn.commit()
        return await self.get_upstream(upstream_id)

    async def update_upstream(self, upstream_id: str, values: dict[str, Any]) -> dict[str, Any] | None:
        allowed = {
            "name",
            "base_url",
            "enabled",
            "weight",
            "max_concurrency",
            "adapter",
            "auth_token",
            "options",
        }
        assignments: list[str] = []
        params: list[Any] = []
        for key, value in values.items():
            if key not in allowed:
                continue
            column = "options_json" if key == "options" else key
            if key == "options":
                value = _json(value or {})
            elif key == "enabled":
                value = int(bool(value))
            elif key == "base_url":
                value = str(value).rstrip("/")
            assignments.append(f"{column}=?")
            params.append(value)
        if not assignments:
            return await self.get_upstream(upstream_id)
        assignments.append("updated_at=?")
        params.extend((now(), upstream_id))
        async with self.lock:
            self.conn.execute(f"UPDATE upstreams SET {', '.join(assignments)} WHERE id=?", params)
            self.conn.commit()
        return await self.get_upstream(upstream_id)

    async def delete_upstream(self, upstream_id: str) -> bool:
        async with self.lock:
            active = self.conn.execute(
                f"SELECT COUNT(*) FROM jobs WHERE upstream_id=? AND status IN ({','.join('?' for _ in ACTIVE_STATUSES)})",
                (upstream_id, *ACTIVE_STATUSES),
            ).fetchone()[0]
            if active:
                raise ValueError("upstream still owns active jobs")
            cursor = self.conn.execute("DELETE FROM upstreams WHERE id=?", (upstream_id,))
            self.conn.commit()
            return cursor.rowcount > 0

    async def get_upstream(self, upstream_id: str) -> dict[str, Any] | None:
        async with self.lock:
            row = self.conn.execute("SELECT * FROM upstreams WHERE id=?", (upstream_id,)).fetchone()
        return self._upstream(row) if row else None

    async def list_upstreams(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        query = "SELECT * FROM upstreams"
        params: tuple[Any, ...] = ()
        if enabled_only:
            query += " WHERE enabled=1"
        query += " ORDER BY name, id"
        async with self.lock:
            rows = self.conn.execute(query, params).fetchall()
        return [self._upstream(row) for row in rows]

    async def set_upstream_health(
        self,
        upstream_id: str,
        healthy: bool,
        error: str | None,
        stats: dict[str, Any] | None,
    ) -> None:
        async with self.lock:
            self.conn.execute(
                """
                UPDATE upstreams
                SET healthy=?, last_check=?, last_error=?, stats_json=?, updated_at=?
                WHERE id=?
                """,
                (int(healthy), now(), error, _json(stats or {}), now(), upstream_id),
            )
            self.conn.commit()

    @staticmethod
    def _upstream(row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        for column, key in UPSTREAM_JSON_COLUMNS.items():
            item[key] = _decode_json(item.pop(column), {})
        item["enabled"] = bool(item["enabled"])
        item["healthy"] = bool(item["healthy"])
        return item

    async def create_api_key(self, name: str, expires_at: float | None = None) -> tuple[dict[str, Any], str]:
        raw_token = f"h3_{secrets.token_urlsafe(32)}"
        key_id = str(uuid.uuid4())
        timestamp = now()
        async with self.lock:
            self.conn.execute(
                """
                INSERT INTO api_keys(id, name, token_hash, token_prefix, enabled, created_at, expires_at)
                VALUES(?, ?, ?, ?, 1, ?, ?)
                """,
                (key_id, name, hash_token(raw_token), raw_token[:11], timestamp, expires_at),
            )
            self.conn.commit()
        key = await self.get_api_key(key_id)
        if key is None:
            raise RuntimeError("failed to create API key")
        return key, raw_token

    async def get_api_key(self, key_id: str) -> dict[str, Any] | None:
        async with self.lock:
            row = self.conn.execute(
                "SELECT id, name, token_prefix, enabled, created_at, last_used_at, expires_at FROM api_keys WHERE id=?",
                (key_id,),
            ).fetchone()
        if not row:
            return None
        item = dict(row)
        item["enabled"] = bool(item["enabled"])
        return item

    async def list_api_keys(self) -> list[dict[str, Any]]:
        async with self.lock:
            rows = self.conn.execute(
                "SELECT id, name, token_prefix, enabled, created_at, last_used_at, expires_at FROM api_keys ORDER BY created_at DESC"
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["enabled"] = bool(item["enabled"])
            result.append(item)
        return result

    async def validate_api_key(self, token: str) -> str | None:
        digest = hash_token(token)
        timestamp = now()
        async with self.lock:
            row = self.conn.execute(
                """
                SELECT id FROM api_keys
                WHERE token_hash=? AND enabled=1 AND (expires_at IS NULL OR expires_at>?)
                """,
                (digest, timestamp),
            ).fetchone()
            if row:
                self.conn.execute("UPDATE api_keys SET last_used_at=? WHERE id=?", (timestamp, row["id"]))
                self.conn.commit()
        return row["id"] if row else None

    async def set_api_key_enabled(self, key_id: str, enabled: bool) -> bool:
        async with self.lock:
            cursor = self.conn.execute("UPDATE api_keys SET enabled=? WHERE id=?", (int(enabled), key_id))
            self.conn.commit()
            return cursor.rowcount > 0

    async def delete_api_key(self, key_id: str) -> bool:
        async with self.lock:
            cursor = self.conn.execute("DELETE FROM api_keys WHERE id=?", (key_id,))
            self.conn.commit()
            return cursor.rowcount > 0

    async def create_job(
        self,
        job_id: str,
        mode: str,
        adapter: str,
        params: dict[str, Any],
        assets: dict[str, list[dict[str, Any]]],
        priority: int,
        max_attempts: int,
        requested_by: str | None,
    ) -> dict[str, Any]:
        timestamp = now()
        async with self.lock:
            queue_order = self.conn.execute(
                "SELECT COALESCE(MAX(queue_order), 0) + 1000 FROM jobs WHERE status IN ('queued', 'retrying')"
            ).fetchone()[0]
            self.conn.execute(
                """
                INSERT INTO jobs(
                    id, status, mode, adapter, priority, queue_order, params_json,
                    assets_json, max_attempts, requested_by, created_at, updated_at
                ) VALUES(?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    mode,
                    adapter,
                    priority,
                    queue_order,
                    _json(params),
                    _json(assets),
                    max_attempts,
                    requested_by,
                    timestamp,
                    timestamp,
                ),
            )
            self.conn.commit()
        return await self.get_job(job_id)

    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        async with self.lock:
            row = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._job(row) if row else None

    async def list_jobs(
        self,
        *,
        status: str | None = None,
        mode: str | None = None,
        upstream_id: str | None = None,
        requested_by: str | None = None,
        search: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status=?")
            params.append(status)
        if mode:
            clauses.append("mode=?")
            params.append(mode)
        if upstream_id:
            clauses.append("upstream_id=?")
            params.append(upstream_id)
        if requested_by:
            clauses.append("requested_by=?")
            params.append(requested_by)
        if search:
            clauses.append("(id LIKE ? OR params_json LIKE ?)")
            params.extend((f"%{search}%", f"%{search}%"))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        async with self.lock:
            total = self.conn.execute(f"SELECT COUNT(*) FROM jobs{where}", params).fetchone()[0]
            rows = self.conn.execute(
                f"SELECT * FROM jobs{where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (*params, min(max(limit, 1), 500), max(offset, 0)),
            ).fetchall()
        return [self._job(row) for row in rows], total

    async def queued_jobs(self, limit: int = 50) -> list[dict[str, Any]]:
        async with self.lock:
            rows = self.conn.execute(
                """
                SELECT * FROM jobs
                WHERE status IN ('queued', 'retrying') AND not_before<=?
                ORDER BY priority DESC, queue_order ASC, created_at ASC
                LIMIT ?
                """,
                (now(), limit),
            ).fetchall()
        return [self._job(row) for row in rows]

    async def active_jobs(self) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in ACTIVE_STATUSES)
        async with self.lock:
            rows = self.conn.execute(
                f"SELECT * FROM jobs WHERE status IN ({placeholders}) ORDER BY submitted_at, created_at",
                ACTIVE_STATUSES,
            ).fetchall()
        return [self._job(row) for row in rows]

    async def active_counts(self) -> dict[str, int]:
        placeholders = ",".join("?" for _ in ACTIVE_STATUSES)
        async with self.lock:
            rows = self.conn.execute(
                f"""
                SELECT upstream_id, COUNT(*) AS count
                FROM jobs
                WHERE upstream_id IS NOT NULL AND status IN ({placeholders})
                GROUP BY upstream_id
                """,
                ACTIVE_STATUSES,
            ).fetchall()
        return {row["upstream_id"]: row["count"] for row in rows}

    async def claim_job(self, job_id: str) -> dict[str, Any] | None:
        timestamp = now()
        async with self.lock:
            cursor = self.conn.execute(
                """
                UPDATE jobs SET status='dispatching', attempts=attempts+1, updated_at=?
                WHERE id=? AND status IN ('queued', 'retrying') AND cancel_requested=0
                """,
                (timestamp, job_id),
            )
            self.conn.commit()
            if cursor.rowcount == 0:
                return None
            row = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._job(row)

    async def update_job(self, job_id: str, **values: Any) -> dict[str, Any] | None:
        allowed = {
            "status",
            "priority",
            "queue_order",
            "params",
            "assets",
            "outputs",
            "error",
            "upstream_id",
            "prompt_id",
            "attempts",
            "max_attempts",
            "not_before",
            "cancel_requested",
            "submitted_at",
            "started_at",
            "finished_at",
        }
        columns = {value: key for key, value in JOB_JSON_COLUMNS.items()}
        assignments: list[str] = []
        params: list[Any] = []
        for key, value in values.items():
            if key not in allowed:
                continue
            column = columns.get(key, key)
            if key in columns:
                value = None if value is None and key == "error" else _json(value)
            elif key == "cancel_requested":
                value = int(bool(value))
            assignments.append(f"{column}=?")
            params.append(value)
        if not assignments:
            return await self.get_job(job_id)
        assignments.append("updated_at=?")
        params.extend((now(), job_id))
        async with self.lock:
            self.conn.execute(f"UPDATE jobs SET {', '.join(assignments)} WHERE id=?", params)
            self.conn.commit()
        return await self.get_job(job_id)

    async def request_cancel(self, job_id: str) -> tuple[dict[str, Any] | None, bool]:
        timestamp = now()
        async with self.lock:
            row = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not row:
                return None, False
            if row["status"] in TERMINAL_STATUSES:
                return self._job(row), False
            if row["status"] in LOCAL_QUEUE_STATUSES:
                status = "canceled"
                finished_at = timestamp
            else:
                status = "canceling"
                finished_at = None
            self.conn.execute(
                """
                UPDATE jobs SET status=?, cancel_requested=1, finished_at=?, updated_at=? WHERE id=?
                """,
                (status, finished_at, timestamp, job_id),
            )
            self.conn.commit()
            updated = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._job(updated), True

    async def reorder_job(self, job_id: str, action: str, target_job_id: str | None = None) -> dict[str, Any]:
        async with self.lock:
            job = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not job:
                raise ValueError("job not found")
            if job["status"] not in LOCAL_QUEUE_STATUSES:
                raise ValueError("only locally queued jobs can be reordered")

            if action == "front":
                priority = self.conn.execute(
                    "SELECT COALESCE(MAX(priority), 0) + 1 FROM jobs WHERE status IN ('queued', 'retrying')"
                ).fetchone()[0]
                self.conn.execute("UPDATE jobs SET priority=?, queue_order=0, updated_at=? WHERE id=?", (priority, now(), job_id))
            elif action == "back":
                priority = self.conn.execute(
                    "SELECT COALESCE(MIN(priority), 0) - 1 FROM jobs WHERE status IN ('queued', 'retrying')"
                ).fetchone()[0]
                queue_order = self.conn.execute(
                    "SELECT COALESCE(MAX(queue_order), 0) + 1000 FROM jobs WHERE status IN ('queued', 'retrying')"
                ).fetchone()[0]
                self.conn.execute(
                    "UPDATE jobs SET priority=?, queue_order=?, updated_at=? WHERE id=?",
                    (priority, queue_order, now(), job_id),
                )
            elif action in {"before", "after"}:
                if not target_job_id or target_job_id == job_id:
                    raise ValueError("a different target_job_id is required")
                target = self.conn.execute("SELECT * FROM jobs WHERE id=?", (target_job_id,)).fetchone()
                if not target or target["status"] not in LOCAL_QUEUE_STATUSES:
                    raise ValueError("target job is not locally queued")
                priority = target["priority"]
                rows = self.conn.execute(
                    """
                    SELECT id FROM jobs
                    WHERE status IN ('queued', 'retrying') AND priority=? AND id<>?
                    ORDER BY queue_order, created_at
                    """,
                    (priority, job_id),
                ).fetchall()
                ids = [row["id"] for row in rows]
                target_index = ids.index(target_job_id)
                ids.insert(target_index if action == "before" else target_index + 1, job_id)
                for index, queued_id in enumerate(ids, start=1):
                    self.conn.execute(
                        "UPDATE jobs SET priority=?, queue_order=?, updated_at=? WHERE id=?",
                        (priority, index * 1000, now(), queued_id),
                    )
            else:
                raise ValueError("action must be front, back, before, or after")
            self.conn.commit()
            updated = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._job(updated)

    async def set_job_priority(self, job_id: str, priority: int) -> dict[str, Any]:
        async with self.lock:
            cursor = self.conn.execute(
                """
                UPDATE jobs SET priority=?, updated_at=?
                WHERE id=? AND status IN ('queued', 'retrying')
                """,
                (priority, now(), job_id),
            )
            self.conn.commit()
            if cursor.rowcount == 0:
                raise ValueError("only locally queued jobs can change priority")
            row = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._job(row)

    async def retry_job(self, job_id: str) -> dict[str, Any]:
        async with self.lock:
            row = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not row:
                raise ValueError("job not found")
            if row["status"] not in TERMINAL_STATUSES:
                raise ValueError("only terminal jobs can be retried")
            queue_order = self.conn.execute(
                "SELECT COALESCE(MAX(queue_order), 0) + 1000 FROM jobs WHERE status IN ('queued', 'retrying')"
            ).fetchone()[0]
            self.conn.execute(
                """
                UPDATE jobs SET status='queued', queue_order=?, upstream_id=NULL, prompt_id=NULL,
                    attempts=0, not_before=0, cancel_requested=0, outputs_json='[]', error_json=NULL,
                    submitted_at=NULL, started_at=NULL, finished_at=NULL, updated_at=?
                WHERE id=?
                """,
                (queue_order, now(), job_id),
            )
            self.conn.commit()
            updated = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._job(updated)

    async def queue_snapshot(self) -> dict[str, Any]:
        async with self.lock:
            rows = self.conn.execute(
                """
                SELECT * FROM jobs WHERE status IN ('queued', 'retrying')
                ORDER BY priority DESC, queue_order ASC, created_at ASC
                """
            ).fetchall()
            paused_row = self.conn.execute("SELECT value FROM settings WHERE key='queue_paused'").fetchone()
        return {"paused": paused_row["value"] == "true", "items": [self._job(row) for row in rows]}

    async def queue_paused(self) -> bool:
        async with self.lock:
            row = self.conn.execute("SELECT value FROM settings WHERE key='queue_paused'").fetchone()
        return bool(row and row["value"] == "true")

    async def set_queue_paused(self, paused: bool) -> None:
        async with self.lock:
            self.conn.execute(
                """
                INSERT INTO settings(key, value, updated_at) VALUES('queue_paused', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                ("true" if paused else "false", now()),
            )
            self.conn.commit()

    async def summary(self) -> dict[str, Any]:
        timestamp = now()
        async with self.lock:
            rows = self.conn.execute("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status").fetchall()
            recent = self.conn.execute("SELECT COUNT(*) FROM jobs WHERE created_at>=?", (timestamp - 86400,)).fetchone()[0]
            durations = self.conn.execute(
                """
                SELECT AVG(finished_at-started_at) FROM jobs
                WHERE status='succeeded' AND started_at IS NOT NULL AND finished_at IS NOT NULL
                    AND finished_at>=?
                """,
                (timestamp - 86400,),
            ).fetchone()[0]
            keys = self.conn.execute("SELECT COUNT(*) FROM api_keys WHERE enabled=1").fetchone()[0]
        counts = {row["status"]: row["count"] for row in rows}
        return {
            "jobs": counts,
            "total_jobs": sum(counts.values()),
            "jobs_24h": recent,
            "average_duration_24h": durations,
            "active_api_keys": keys,
        }

    async def add_event(
        self,
        kind: str,
        subject_type: str,
        subject_id: str | None,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        async with self.lock:
            self.conn.execute(
                """
                INSERT INTO events(kind, subject_type, subject_id, message, data_json, created_at)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (kind, subject_type, subject_id, message, _json(data or {}), now()),
            )
            self.conn.commit()

    async def list_events(self, limit: int = 100) -> list[dict[str, Any]]:
        async with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM events ORDER BY created_at DESC LIMIT ?",
                (min(max(limit, 1), 500),),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["data"] = _decode_json(item.pop("data_json"), {})
            result.append(item)
        return result

    @staticmethod
    def _job(row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        for column, key in JOB_JSON_COLUMNS.items():
            default: Any = [] if key == "outputs" else {} if key in {"params", "assets"} else None
            item[key] = _decode_json(item.pop(column), default)
        item["cancel_requested"] = bool(item["cancel_requested"])
        return item
