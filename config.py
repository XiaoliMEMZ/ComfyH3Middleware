from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    data_dir: Path
    database_path: Path
    asset_dir: Path
    admin_token: str
    bootstrap_api_token: str
    initial_upstreams: tuple[str, ...]
    poll_interval: float
    health_interval: float
    capability_interval: float
    request_timeout: float
    sync_timeout: float
    prompt_missing_timeout: float
    max_upload_bytes: int
    max_request_bytes: int
    default_max_attempts: int
    conditioning_node: str
    retry_execution_errors: bool

    @classmethod
    def from_env(cls) -> "Settings":
        root = Path(__file__).resolve().parent
        data_dir = Path(os.environ.get("H3_DATA_DIR", root / "data")).resolve()
        upstreams = tuple(
            value.strip().rstrip("/")
            for value in os.environ.get("H3_UPSTREAMS", "http://127.0.0.1:8188").split(",")
            if value.strip()
        )
        return cls(
            host=os.environ.get("H3_HOST", "0.0.0.0"),
            port=int(os.environ.get("H3_PORT", "8191")),
            data_dir=data_dir,
            database_path=Path(os.environ.get("H3_DATABASE", data_dir / "gateway.sqlite3")).resolve(),
            asset_dir=Path(os.environ.get("H3_ASSET_DIR", data_dir / "assets")).resolve(),
            admin_token=os.environ.get("H3_ADMIN_TOKEN", "h3-admin-change-me"),
            bootstrap_api_token=os.environ.get("H3_API_TOKEN", "h3-api-change-me"),
            initial_upstreams=upstreams,
            poll_interval=float(os.environ.get("H3_POLL_INTERVAL", "1")),
            health_interval=float(os.environ.get("H3_HEALTH_INTERVAL", "5")),
            capability_interval=float(os.environ.get("H3_CAPABILITY_INTERVAL", "300")),
            request_timeout=float(os.environ.get("H3_REQUEST_TIMEOUT", "30")),
            sync_timeout=float(os.environ.get("H3_SYNC_TIMEOUT", "1800")),
            prompt_missing_timeout=float(os.environ.get("H3_PROMPT_MISSING_TIMEOUT", "30")),
            max_upload_bytes=int(os.environ.get("H3_MAX_UPLOAD_BYTES", str(512 * 1024 * 1024))),
            max_request_bytes=int(os.environ.get("H3_MAX_REQUEST_BYTES", str(1024 * 1024 * 1024))),
            default_max_attempts=max(1, int(os.environ.get("H3_MAX_ATTEMPTS", "2"))),
            conditioning_node=os.environ.get("H3_CONDITIONING_NODE", "MiniMaxH3ImageToVideo"),
            retry_execution_errors=_env_bool("H3_RETRY_EXECUTION_ERRORS", False),
        )

    def prepare(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.asset_dir.mkdir(parents=True, exist_ok=True)
