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
    "progress_json": "progress",
}
UPSTREAM_JSON_COLUMNS = {
    "options_json": "options",
    "stats_json": "stats",
}

API_KEY_SELECT = """
    SELECT api_keys.id, api_keys.name, api_keys.token_prefix, api_keys.enabled,
        api_keys.created_at, api_keys.last_used_at, api_keys.expires_at,
        api_keys.group_id, upstream_groups.name AS group_name
    FROM api_keys
    LEFT JOIN upstream_groups ON upstream_groups.id = api_keys.group_id
"""


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
            CREATE TABLE IF NOT EXISTS upstream_groups (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS upstreams (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                base_url TEXT NOT NULL UNIQUE,
                enabled INTEGER NOT NULL DEFAULT 1,
                weight REAL NOT NULL DEFAULT 1,
                max_concurrency INTEGER NOT NULL DEFAULT 1,
                group_id TEXT,
                adapter TEXT NOT NULL DEFAULT 'minimax-h3-native',
                auth_token TEXT,
                options_json TEXT NOT NULL DEFAULT '{}',
                healthy INTEGER NOT NULL DEFAULT 0,
                last_check REAL,
                last_error TEXT,
                stats_json TEXT NOT NULL DEFAULT '{}',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                FOREIGN KEY (group_id) REFERENCES upstream_groups(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS upstream_group_memberships (
                upstream_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY (upstream_id, group_id),
                FOREIGN KEY (upstream_id) REFERENCES upstreams(id) ON DELETE CASCADE,
                FOREIGN KEY (group_id) REFERENCES upstream_groups(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS upstream_group_memberships_group_idx
                ON upstream_group_memberships(group_id, upstream_id);

            CREATE TABLE IF NOT EXISTS api_keys (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                token_prefix TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at REAL NOT NULL,
                last_used_at REAL,
                expires_at REAL,
                group_id TEXT,
                FOREIGN KEY (group_id) REFERENCES upstream_groups(id) ON DELETE SET NULL
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
                progress_json TEXT NOT NULL DEFAULT '{}',
                group_id TEXT,
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
                FOREIGN KEY (group_id) REFERENCES upstream_groups(id) ON DELETE SET NULL,
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
        migrations = (
            ("upstreams", "group_id", "TEXT REFERENCES upstream_groups(id) ON DELETE SET NULL"),
            ("api_keys", "group_id", "TEXT REFERENCES upstream_groups(id) ON DELETE SET NULL"),
            ("jobs", "progress_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("jobs", "group_id", "TEXT REFERENCES upstream_groups(id) ON DELETE SET NULL"),
        )
        for table, column, definition in migrations:
            columns = {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}
            if column not in columns:
                connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        connection.execute("CREATE INDEX IF NOT EXISTS upstreams_group_idx ON upstreams(group_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS api_keys_group_idx ON api_keys(group_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS jobs_group_idx ON jobs(group_id, status)")
        connection.execute(
            """
            INSERT OR IGNORE INTO upstream_group_memberships(upstream_id, group_id, created_at)
            SELECT id, group_id, ? FROM upstreams WHERE group_id IS NOT NULL
            """,
            (now(),),
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

    async def create_group(self, name: str, enabled: bool = True) -> dict[str, Any]:
        group_id = str(uuid.uuid4())
        timestamp = now()
        async with self.lock:
            self.conn.execute(
                """
                INSERT INTO upstream_groups(id, name, enabled, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?)
                """,
                (group_id, name, int(enabled), timestamp, timestamp),
            )
            self.conn.commit()
        group = await self.get_group(group_id)
        if group is None:
            raise RuntimeError("failed to create upstream group")
        return group

    async def get_group(self, group_id: str) -> dict[str, Any] | None:
        async with self.lock:
            row = self.conn.execute("SELECT * FROM upstream_groups WHERE id=?", (group_id,)).fetchone()
        if not row:
            return None
        item = dict(row)
        item["enabled"] = bool(item["enabled"])
        return item

    async def list_groups(self) -> list[dict[str, Any]]:
        async with self.lock:
            rows = self.conn.execute("SELECT * FROM upstream_groups ORDER BY name, id").fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["enabled"] = bool(item["enabled"])
            result.append(item)
        return result

    async def update_group(self, group_id: str, values: dict[str, Any]) -> dict[str, Any] | None:
        allowed = {"name", "enabled"}
        assignments: list[str] = []
        params: list[Any] = []
        for key, value in values.items():
            if key not in allowed:
                continue
            if key == "enabled":
                value = int(bool(value))
            assignments.append(f"{key}=?")
            params.append(value)
        if not assignments:
            return await self.get_group(group_id)
        assignments.append("updated_at=?")
        params.extend((now(), group_id))
        async with self.lock:
            self.conn.execute(
                f"UPDATE upstream_groups SET {', '.join(assignments)} WHERE id=?", params
            )
            self.conn.commit()
        return await self.get_group(group_id)

    async def delete_group(self, group_id: str) -> bool:
        async with self.lock:
            assignments = self.conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM upstream_group_memberships WHERE group_id=?) AS upstreams,
                    (SELECT COUNT(*) FROM api_keys WHERE group_id=?) AS api_keys,
                    (SELECT COUNT(*) FROM jobs WHERE group_id=? AND status IN (?, ?, ?, ?, ?, ?, ?)) AS jobs
                """,
                (group_id, group_id, group_id, *LOCAL_QUEUE_STATUSES, *ACTIVE_STATUSES),
            ).fetchone()
            if any(assignments):
                raise ValueError("group still has assigned upstreams, API keys, or active jobs")
            self.conn.execute("UPDATE jobs SET group_id=NULL WHERE group_id=?", (group_id,))
            cursor = self.conn.execute("DELETE FROM upstream_groups WHERE id=?", (group_id,))
            self.conn.commit()
            return cursor.rowcount > 0

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
        group_ids = list(dict.fromkeys(values.get("group_ids") or []))
        if "group_ids" not in values and values.get("group_id"):
            group_ids = [values["group_id"]]
        legacy_group_id = group_ids[0] if group_ids else None
        async with self.lock:
            self.conn.execute(
                """
                INSERT INTO upstreams(
                    id, name, base_url, enabled, weight, max_concurrency, group_id,
                    adapter, auth_token, options_json, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    upstream_id,
                    values["name"],
                    values["base_url"].rstrip("/"),
                    int(values.get("enabled", True)),
                    float(values.get("weight", 1)),
                    int(values.get("max_concurrency", 1)),
                    legacy_group_id,
                    values.get("adapter", "minimax-h3-native"),
                    values.get("auth_token") or None,
                    _json(values.get("options") or {}),
                    timestamp,
                    timestamp,
                ),
            )
            self.conn.executemany(
                """
                INSERT INTO upstream_group_memberships(upstream_id, group_id, created_at)
                VALUES(?, ?, ?)
                """,
                ((upstream_id, group_id, timestamp) for group_id in group_ids),
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
            "group_id",
            "adapter",
            "auth_token",
            "options",
        }
        group_ids: list[str] | None = None
        if "group_ids" in values:
            group_ids = list(dict.fromkeys(values.get("group_ids") or []))
        elif "group_id" in values:
            group_ids = [values["group_id"]] if values.get("group_id") else []
        sql_values = dict(values)
        if group_ids is not None:
            sql_values["group_id"] = group_ids[0] if group_ids else None
        assignments: list[str] = []
        params: list[Any] = []
        for key, value in sql_values.items():
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
        if not assignments and group_ids is None:
            return await self.get_upstream(upstream_id)
        assignments.append("updated_at=?")
        params.extend((now(), upstream_id))
        async with self.lock:
            self.conn.execute(f"UPDATE upstreams SET {', '.join(assignments)} WHERE id=?", params)
            if group_ids is not None:
                self.conn.execute(
                    "DELETE FROM upstream_group_memberships WHERE upstream_id=?", (upstream_id,)
                )
                self.conn.executemany(
                    """
                    INSERT INTO upstream_group_memberships(upstream_id, group_id, created_at)
                    VALUES(?, ?, ?)
                    """,
                    ((upstream_id, group_id, now()) for group_id in group_ids),
                )
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
            groups = self._upstream_groups_locked((upstream_id,))
        return self._upstream(row, groups.get(upstream_id, [])) if row else None

    async def list_upstreams(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        query = "SELECT * FROM upstreams"
        params: tuple[Any, ...] = ()
        if enabled_only:
            query += " WHERE enabled=1"
        query += " ORDER BY name, id"
        async with self.lock:
            rows = self.conn.execute(query, params).fetchall()
            groups = self._upstream_groups_locked(tuple(row["id"] for row in rows))
        return [self._upstream(row, groups.get(row["id"], [])) for row in rows]

    def _upstream_groups_locked(
        self, upstream_ids: tuple[str, ...]
    ) -> dict[str, list[dict[str, Any]]]:
        if not upstream_ids:
            return {}
        placeholders = ",".join("?" for _ in upstream_ids)
        rows = self.conn.execute(
            f"""
            SELECT memberships.upstream_id, groups.id, groups.name, groups.enabled
            FROM upstream_group_memberships AS memberships
            JOIN upstream_groups AS groups ON groups.id=memberships.group_id
            WHERE memberships.upstream_id IN ({placeholders})
            ORDER BY groups.name, groups.id
            """,
            upstream_ids,
        ).fetchall()
        result: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            result.setdefault(row["upstream_id"], []).append(
                {"id": row["id"], "name": row["name"], "enabled": bool(row["enabled"])}
            )
        return result

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
    def _upstream(row: sqlite3.Row, groups: list[dict[str, Any]]) -> dict[str, Any]:
        item = dict(row)
        for column, key in UPSTREAM_JSON_COLUMNS.items():
            item[key] = _decode_json(item.pop(column), {})
        item["enabled"] = bool(item["enabled"])
        item["healthy"] = bool(item["healthy"])
        item["groups"] = groups
        item["group_ids"] = [group["id"] for group in groups]
        primary = next(
            (group for group in groups if group["id"] == item.get("group_id")),
            groups[0] if groups else None,
        )
        item["group_id"] = primary["id"] if primary else None
        item["group_name"] = primary["name"] if primary else None
        item["group_enabled"] = primary["enabled"] if primary else None
        return item

    async def create_api_key(
        self,
        name: str,
        expires_at: float | None = None,
        group_id: str | None = None,
    ) -> tuple[dict[str, Any], str]:
        raw_token = f"h3_{secrets.token_urlsafe(32)}"
        key_id = str(uuid.uuid4())
        timestamp = now()
        async with self.lock:
            self.conn.execute(
                """
                INSERT INTO api_keys(
                    id, name, token_hash, token_prefix, enabled, created_at, expires_at, group_id
                ) VALUES(?, ?, ?, ?, 1, ?, ?, ?)
                """,
                (key_id, name, hash_token(raw_token), raw_token[:11], timestamp, expires_at, group_id),
            )
            self.conn.commit()
        key = await self.get_api_key(key_id)
        if key is None:
            raise RuntimeError("failed to create API key")
        return key, raw_token

    async def get_api_key(self, key_id: str) -> dict[str, Any] | None:
        async with self.lock:
            row = self.conn.execute(
                f"{API_KEY_SELECT} WHERE api_keys.id=?",
                (key_id,),
            ).fetchone()
        if not row:
            return None
        return self._api_key(row)

    async def list_api_keys(self) -> list[dict[str, Any]]:
        async with self.lock:
            rows = self.conn.execute(f"{API_KEY_SELECT} ORDER BY api_keys.created_at DESC").fetchall()
        return [self._api_key(row) for row in rows]

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

    async def set_api_key_group(self, key_id: str, group_id: str | None) -> bool:
        async with self.lock:
            cursor = self.conn.execute(
                "UPDATE api_keys SET group_id=? WHERE id=?", (group_id, key_id)
            )
            self.conn.commit()
            return cursor.rowcount > 0

    async def delete_api_key(self, key_id: str) -> bool:
        async with self.lock:
            cursor = self.conn.execute("DELETE FROM api_keys WHERE id=?", (key_id,))
            self.conn.commit()
            return cursor.rowcount > 0

    @staticmethod
    def _api_key(row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        item["enabled"] = bool(item["enabled"])
        return item

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
        group_id: str | None = None,
    ) -> dict[str, Any]:
        timestamp = now()
        async with self.lock:
            group_clause = "group_id IS NULL" if group_id is None else "group_id=?"
            group_params: tuple[Any, ...] = () if group_id is None else (group_id,)
            queue_order = self.conn.execute(
                f"SELECT COALESCE(MAX(queue_order), 0) + 1000 FROM jobs "
                f"WHERE status IN ('queued', 'retrying') AND {group_clause}",
                group_params,
            ).fetchone()[0]
            self.conn.execute(
                """
                INSERT INTO jobs(
                    id, status, mode, adapter, priority, queue_order, params_json,
                    assets_json, group_id, max_attempts, requested_by, created_at, updated_at
                ) VALUES(?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    mode,
                    adapter,
                    priority,
                    queue_order,
                    _json(params),
                    _json(assets),
                    group_id,
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

    async def get_job_by_prompt_id(self, prompt_id: str) -> dict[str, Any] | None:
        async with self.lock:
            row = self.conn.execute("SELECT * FROM jobs WHERE prompt_id=?", (prompt_id,)).fetchone()
        return self._job(row) if row else None

    async def list_jobs(
        self,
        *,
        status: str | None = None,
        mode: str | None = None,
        upstream_id: str | None = None,
        requested_by: str | None = None,
        group_id: str | None = None,
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
        if group_id:
            clauses.append("group_id=?")
            params.append(group_id)
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
            "progress",
            "group_id",
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
            group_clause = "group_id IS NULL" if job["group_id"] is None else "group_id=?"
            group_params: tuple[Any, ...] = () if job["group_id"] is None else (job["group_id"],)

            if action == "front":
                priority = self.conn.execute(
                    f"SELECT COALESCE(MAX(priority), 0) + 1 FROM jobs "
                    f"WHERE status IN ('queued', 'retrying') AND {group_clause}",
                    group_params,
                ).fetchone()[0]
                self.conn.execute("UPDATE jobs SET priority=?, queue_order=0, updated_at=? WHERE id=?", (priority, now(), job_id))
            elif action == "back":
                priority = self.conn.execute(
                    f"SELECT COALESCE(MIN(priority), 0) - 1 FROM jobs "
                    f"WHERE status IN ('queued', 'retrying') AND {group_clause}",
                    group_params,
                ).fetchone()[0]
                queue_order = self.conn.execute(
                    f"SELECT COALESCE(MAX(queue_order), 0) + 1000 FROM jobs "
                    f"WHERE status IN ('queued', 'retrying') AND {group_clause}",
                    group_params,
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
                if target["group_id"] != job["group_id"]:
                    raise ValueError("target job is not in the same queue group")
                priority = target["priority"]
                rows = self.conn.execute(
                    f"""
                    SELECT id FROM jobs
                    WHERE status IN ('queued', 'retrying') AND priority=? AND id<>? AND {group_clause}
                    ORDER BY queue_order, created_at
                    """,
                    (priority, job_id, *group_params),
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
                (
                    "SELECT COALESCE(MAX(queue_order), 0) + 1000 FROM jobs "
                    "WHERE status IN ('queued', 'retrying') AND group_id IS NULL"
                    if row["group_id"] is None
                    else "SELECT COALESCE(MAX(queue_order), 0) + 1000 FROM jobs "
                    "WHERE status IN ('queued', 'retrying') AND group_id=?"
                ),
                () if row["group_id"] is None else (row["group_id"],),
            ).fetchone()[0]
            self.conn.execute(
                """
                UPDATE jobs SET status='queued', queue_order=?, upstream_id=NULL, prompt_id=NULL,
                    attempts=0, not_before=0, cancel_requested=0, outputs_json='[]', error_json=NULL, progress_json='{}',
                    submitted_at=NULL, started_at=NULL, finished_at=NULL, updated_at=?
                WHERE id=?
                """,
                (queue_order, now(), job_id),
            )
            self.conn.commit()
            updated = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._job(updated)

    async def queue_snapshot(self, group_id: str | None = None) -> dict[str, Any]:
        group_clause = ""
        group_params: tuple[Any, ...] = ()
        if group_id:
            group_clause = " AND group_id=?"
            group_params = (group_id,)
        async with self.lock:
            rows = self.conn.execute(
                f"""
                SELECT * FROM jobs WHERE status IN ('queued', 'retrying')
                {group_clause}
                ORDER BY priority DESC, queue_order ASC, created_at ASC
                """,
                group_params,
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
            default: Any = [] if key == "outputs" else {} if key in {"params", "assets", "progress"} else None
            item[key] = _decode_json(item.pop(column), default)
        item["cancel_requested"] = bool(item["cancel_requested"])
        return item
