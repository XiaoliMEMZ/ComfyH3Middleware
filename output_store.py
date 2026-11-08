from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path, PurePath
from typing import Any

from .assets import safe_filename


class OutputStore:
    """Durable per-job output files with atomic writes."""

    def __init__(self, root: Path | None) -> None:
        self.root = root.resolve() if root is not None else None

    def prepare(self) -> None:
        if self.root is not None:
            self.root.mkdir(parents=True, exist_ok=True)

    def store_bytes(
        self,
        job_id: str,
        index: int,
        filename: str,
        data: bytes,
        content_type: str,
    ) -> dict[str, Any]:
        if self.root is None:
            raise RuntimeError("output storage is disabled")
        destination = self._destination(job_id, index, filename)
        self._atomic_write(destination, data)
        return self._metadata(destination, data_size=len(data), content_type=content_type)

    def store_path(
        self,
        job_id: str,
        index: int,
        filename: str,
        source: Path,
        content_type: str,
    ) -> dict[str, Any]:
        if self.root is None:
            raise RuntimeError("output storage is disabled")
        if not source.is_file():
            raise FileNotFoundError(source)
        destination = self._destination(job_id, index, filename)
        destination.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        size = 0
        fd, temporary_name = tempfile.mkstemp(prefix=".partial-", dir=destination.parent)
        try:
            with os.fdopen(fd, "wb") as target, source.open("rb") as source_file:
                while True:
                    chunk = source_file.read(1024 * 1024)
                    if not chunk:
                        break
                    target.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary_name, destination)
        except BaseException:
            Path(temporary_name).unlink(missing_ok=True)
            raise
        return {
            "storage": "local",
            "path": str(destination.relative_to(self.root)),
            "size": size,
            "sha256": digest.hexdigest(),
            "content_type": content_type or "application/octet-stream",
        }

    def read(self, metadata: dict[str, Any]) -> tuple[bytes, str]:
        path = self._stored_path(metadata)
        data = path.read_bytes()
        return data, str(metadata.get("content_type") or "application/octet-stream")

    def stored_path(self, metadata: dict[str, Any]) -> Path:
        path = self._stored_path(metadata)
        if not path.is_file():
            raise FileNotFoundError(path)
        return path

    def is_local(self, metadata: dict[str, Any]) -> bool:
        return metadata.get("storage") == "local" and bool(metadata.get("path"))

    def _destination(self, job_id: str, index: int, filename: str) -> Path:
        safe_job_id = safe_filename(job_id, "job")
        safe_name = safe_filename(filename, f"output_{index}.bin")
        directory = self.root / safe_job_id / f"{index:04d}"
        directory.mkdir(parents=True, exist_ok=True)
        return directory / safe_name

    def _stored_path(self, metadata: dict[str, Any]) -> Path:
        if self.root is None:
            raise FileNotFoundError("output storage is disabled")
        relative = PurePath(str(metadata.get("path") or ""))
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("invalid stored output path")
        path = (self.root / relative).resolve()
        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise ValueError("stored output path escapes output directory") from exc
        return path

    @staticmethod
    def _atomic_write(destination: Path, data: bytes) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(prefix=".partial-", dir=destination.parent)
        try:
            with os.fdopen(fd, "wb") as target:
                target.write(data)
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary_name, destination)
        except BaseException:
            Path(temporary_name).unlink(missing_ok=True)
            raise

    def _metadata(self, destination: Path, data_size: int, content_type: str) -> dict[str, Any]:
        digest = hashlib.sha256(destination.read_bytes()).hexdigest()
        return {
            "storage": "local",
            "path": str(destination.relative_to(self.root)),
            "size": data_size,
            "sha256": digest,
            "content_type": content_type or "application/octet-stream",
        }
