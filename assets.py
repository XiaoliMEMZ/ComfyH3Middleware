from __future__ import annotations

import base64
import binascii
import mimetypes
import re
import shutil
from pathlib import Path, PurePath
from typing import Any

from aiohttp.multipart import BodyPartReader


SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")
INDEXED_FIELDS = {
    "ref_image": ("ref_images", 9),
    "ref_video": ("ref_videos", 3),
    "ref_video_audio": ("ref_video_audios", 3),
    "ref_audio": ("ref_audios", 3),
}
SINGLE_FIELDS = {
    "image": "first_frame",
    "first_frame": "first_frame",
    "last_frame": "last_frame",
}


def safe_filename(filename: str | None, fallback: str) -> str:
    name = PurePath(filename or fallback).name
    name = SAFE_NAME.sub("_", name).strip("._")
    return (name or fallback)[:180]


def asset_field(field: str, existing: dict[str, list[dict[str, Any]]]) -> tuple[str, int]:
    if field in SINGLE_FIELDS:
        return SINGLE_FIELDS[field], 0
    for prefix, (kind, maximum) in INDEXED_FIELDS.items():
        if field == prefix:
            index = len(existing.get(kind, []))
        elif field.startswith(prefix + "_") and field[len(prefix) + 1 :].isdigit():
            index = int(field[len(prefix) + 1 :])
        else:
            continue
        if index >= maximum:
            raise ValueError(f"{prefix} supports at most {maximum} inputs")
        return kind, index
    raise ValueError(f"unsupported upload field: {field}")


class AssetStore:
    def __init__(self, root: Path, max_file_bytes: int) -> None:
        self.root = root
        self.max_file_bytes = max_file_bytes

    def job_dir(self, job_id: str) -> Path:
        return self.root / job_id

    async def save_part(
        self,
        job_id: str,
        field: str,
        part: BodyPartReader,
        existing: dict[str, list[dict[str, Any]]],
    ) -> dict[str, Any]:
        kind, index = asset_field(field, existing)
        fallback = f"{kind}_{index}.bin"
        filename = safe_filename(part.filename, fallback)
        path = self._path(job_id, kind, index, filename)
        size = 0
        with path.open("wb") as output:
            while True:
                chunk = await part.read_chunk(size=1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > self.max_file_bytes:
                    output.close()
                    path.unlink(missing_ok=True)
                    raise ValueError(f"{field} exceeds the per-file upload limit")
                output.write(chunk)
        if size == 0:
            path.unlink(missing_ok=True)
            raise ValueError(f"{field} is empty")
        return {
            "kind": kind,
            "index": index,
            "filename": filename,
            "path": str(path),
            "size": size,
            "content_type": part.headers.get("Content-Type", "application/octet-stream"),
        }

    def save_base64(
        self,
        job_id: str,
        field: str,
        encoded: str,
        existing: dict[str, list[dict[str, Any]]],
        filename: str | None = None,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        kind, index = asset_field(field, existing)
        value = encoded.strip()
        if value.startswith("data:") and "," in value:
            header, value = value.split(",", 1)
            if ";base64" not in header:
                raise ValueError(f"{field} data URL must be base64 encoded")
            content_type = header[5:].split(";", 1)[0] or content_type
        try:
            data = base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError(f"invalid base64 data for {field}") from exc
        if not data:
            raise ValueError(f"{field} is empty")
        if len(data) > self.max_file_bytes:
            raise ValueError(f"{field} exceeds the per-file upload limit")
        extension = mimetypes.guess_extension(content_type or "") or ".bin"
        filename = safe_filename(filename, f"{kind}_{index}{extension}")
        path = self._path(job_id, kind, index, filename)
        path.write_bytes(data)
        return {
            "kind": kind,
            "index": index,
            "filename": filename,
            "path": str(path),
            "size": len(data),
            "content_type": content_type or "application/octet-stream",
        }

    def extract_json_assets(
        self,
        job_id: str,
        raw: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
        fields = dict(raw)
        assets: dict[str, list[dict[str, Any]]] = {}
        single_base64 = {
            "image_base64": "first_frame",
            "first_frame_base64": "first_frame",
            "last_frame_base64": "last_frame",
        }
        for key, field in single_base64.items():
            value = fields.pop(key, None)
            if not value:
                continue
            item = self.save_base64(
                job_id,
                field,
                str(value),
                assets,
                filename=fields.pop(f"{field}_filename", None),
                content_type=fields.pop(f"{field}_content_type", None),
            )
            assets.setdefault(item["kind"], []).append(item)

        plural_base64 = {
            "ref_images_base64": "ref_image",
            "ref_videos_base64": "ref_video",
            "ref_video_audios_base64": "ref_video_audio",
            "ref_audios_base64": "ref_audio",
        }
        for key, field in plural_base64.items():
            values = fields.pop(key, None) or []
            if isinstance(values, str):
                values = [values]
            if not isinstance(values, list):
                raise ValueError(f"{key} must be an array")
            for value in values:
                if isinstance(value, str):
                    encoded = value
                    filename = None
                    content_type = None
                elif isinstance(value, dict):
                    encoded = value.get("data") or value.get("base64")
                    filename = value.get("filename")
                    content_type = value.get("content_type")
                else:
                    raise ValueError(f"{key} entries must be strings or objects")
                if not encoded:
                    raise ValueError(f"{key} contains an empty entry")
                item = self.save_base64(job_id, field, str(encoded), assets, filename, content_type)
                assets.setdefault(item["kind"], []).append(item)
        return fields, self.sorted_assets(assets)

    def cleanup_job(self, job_id: str) -> None:
        directory = self.job_dir(job_id)
        if directory.exists():
            shutil.rmtree(directory)

    @staticmethod
    def sorted_assets(assets: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
        return {kind: sorted(items, key=lambda item: item["index"]) for kind, items in assets.items()}

    def _path(self, job_id: str, kind: str, index: int, filename: str) -> Path:
        directory = self.job_dir(job_id)
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f"{kind}_{index}_{filename}"
