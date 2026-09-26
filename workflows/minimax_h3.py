from __future__ import annotations

import copy
import json
import random
from typing import Any

from .base import WorkflowAdapter


MODE_ALIASES = {
    "text": "t2va",
    "txt2vid": "t2va",
    "t2v": "t2va",
    "t2va": "t2va",
    "first": "i2va",
    "first_frame": "i2va",
    "i2v": "i2va",
    "i2va": "i2va",
    "first_last": "fl2va",
    "flf2v": "fl2va",
    "flf2va": "fl2va",
    "fl2v": "fl2va",
    "fl2va": "fl2va",
    "reference": "ref2va",
    "r2v": "ref2va",
    "ref2v": "ref2va",
    "ref2va": "ref2va",
}

DEFAULTS: dict[str, Any] = {
    "prompt": "",
    "duration": 5.0,
    "length": None,
    "length_expression": None,
    "width": None,
    "height": None,
    "megapixels": 1.0,
    "upscale_method": "bicubic",
    "resolution_steps": 32,
    "sampler_name": "res_multistep",
    "scheduler": "simple",
    "steps": 20,
    "denoise": 1.0,
    "noise_seed": None,
    "fps": 24.0,
    "bit_depth": 8,
    "video_vae": "minimax_h3_video_vae_fp16.safetensors",
    "audio_vae": "minimax_h3_audio_vae_fp32.safetensors",
    "fl2va_unet": "minimax_h3_fl2va_pruned_int8_convrot.safetensors",
    "ref2va_unet": "minimax_h3_ref2va_pruned_int8_convrot.safetensors",
    "weight_dtype": "default",
    "clip_name": "qwen3vl_32b_minimax_h3_int8_convrot.safetensors",
    "clip_type": "minimax",
    "clip_device": "default",
    "filename_prefix": None,
    "format": "auto",
    "codec": "auto",
    "ref_image_size": "match",
    "use_embedded_video_audio": True,
    "shift_video": None,
    "shift_audio": None,
    "node_overrides": {},
}

ASSET_KEYS = (
    "first_frame",
    "last_frame",
    "ref_images",
    "ref_videos",
    "ref_video_audios",
    "ref_audios",
)


def _as_int(value: Any, name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _as_float(value: Any, name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number") from exc


def _as_bool(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def _as_string_list(value: Any, name: str) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("["):
            try:
                value = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{name} must be a JSON array or string") from exc
        else:
            return [stripped]
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise ValueError(f"{name} must be an array of non-empty strings")
    return value


def _aligned_length(duration: float) -> int:
    length = max(5, round(duration * 24))
    return length + (5 - length % 17) % 17


class MiniMaxH3Adapter(WorkflowAdapter):
    name = "minimax-h3-native"

    def __init__(self, conditioning_node: str = "MiniMaxH3ImageToVideo") -> None:
        self.conditioning_node = conditioning_node

    def normalize(self, raw: dict[str, Any], assets: dict[str, list[dict[str, Any]]], force_mode: str | None = None) -> dict[str, Any]:
        params = copy.deepcopy(DEFAULTS)
        for key in DEFAULTS:
            if key in raw and raw[key] not in (None, ""):
                params[key] = raw[key]
        if raw.get("unet_name") not in (None, ""):
            params["fl2va_unet"] = raw["unet_name"]

        prompt = str(raw.get("prompt", params["prompt"]) or "").strip()
        if not prompt:
            raise ValueError("prompt is required")
        params["prompt"] = prompt

        names = {
            "first_frame": _as_string_list(raw.get("first_frame_name") or raw.get("image_name"), "first_frame_name"),
            "last_frame": _as_string_list(raw.get("last_frame_name"), "last_frame_name"),
            "ref_images": _as_string_list(raw.get("ref_image_names"), "ref_image_names"),
            "ref_videos": _as_string_list(raw.get("ref_video_names"), "ref_video_names"),
            "ref_video_audios": _as_string_list(raw.get("ref_video_audio_names"), "ref_video_audio_names"),
            "ref_audios": _as_string_list(raw.get("ref_audio_names"), "ref_audio_names"),
        }
        present = {key: bool(assets.get(key) or names[key]) for key in ASSET_KEYS}
        for key in ("first_frame", "last_frame"):
            count = len(assets.get(key) or []) + len(names[key])
            if count > 1:
                raise ValueError(f"{key} accepts exactly one input")
        limits = {"ref_images": 9, "ref_videos": 3, "ref_video_audios": 3, "ref_audios": 3}
        for key, maximum in limits.items():
            count = len(assets.get(key) or []) + len(names[key])
            if count > maximum:
                raise ValueError(f"{key} supports at most {maximum} inputs")
        if (len(assets.get("ref_video_audios") or []) + len(names["ref_video_audios"])) > (
            len(assets.get("ref_videos") or []) + len(names["ref_videos"])
        ):
            raise ValueError("each ref_video_audio must have a matching ref_video")

        requested_mode = force_mode or raw.get("mode")
        if requested_mode in (None, "", "auto"):
            if present["ref_images"] or present["ref_videos"] or present["ref_audios"]:
                mode = "ref2va"
            elif present["first_frame"] and present["last_frame"]:
                mode = "fl2va"
            elif present["first_frame"]:
                mode = "i2va"
            else:
                mode = "t2va"
        else:
            try:
                mode = MODE_ALIASES[str(requested_mode).strip().lower()]
            except KeyError as exc:
                raise ValueError("mode must be one of t2va, i2va, fl2va, ref2va") from exc

        if mode == "t2va" and any(present.values()):
            raise ValueError("mode=t2va does not accept frame or reference inputs")
        if mode == "i2va" and not present["first_frame"]:
            raise ValueError("first_frame is required for mode=i2va")
        if mode == "i2va" and present["last_frame"]:
            raise ValueError("last_frame requires mode=fl2va")
        if mode in {"i2va", "fl2va"} and any(present[key] for key in ("ref_images", "ref_videos", "ref_video_audios", "ref_audios")):
            raise ValueError(f"reference inputs require mode=ref2va, not mode={mode}")
        if mode == "fl2va" and not (present["first_frame"] and present["last_frame"]):
            raise ValueError("first_frame and last_frame are required for mode=fl2va")
        if mode == "ref2va" and not (present["ref_images"] or present["ref_videos"] or present["ref_audios"]):
            raise ValueError("at least one reference image, video, or audio is required for mode=ref2va")
        if mode == "ref2va" and (present["first_frame"] or present["last_frame"]):
            raise ValueError("first_frame and last_frame cannot be mixed with mode=ref2va")

        params["mode"] = mode
        params["asset_names"] = names
        params["duration"] = _as_float(params["duration"], "duration")
        if params["duration"] <= 0:
            raise ValueError("duration must be greater than zero")
        explicit_length = params["length"] is not None
        if not explicit_length:
            params["length"] = _aligned_length(params["duration"])
        else:
            params["length"] = _as_int(params["length"], "length")
            if params["length"] < 5:
                raise ValueError("length must be at least 5")
            params["length_expression"] = None

        width = params["width"]
        height = params["height"]
        if (width is None) != (height is None):
            raise ValueError("width and height must be provided together")
        if width is not None:
            params["width"] = _as_int(width, "width")
            params["height"] = _as_int(height, "height")
            if params["width"] < 32 or params["height"] < 32:
                raise ValueError("width and height must be at least 32")
        elif mode in {"t2va", "ref2va"}:
            params["width"] = 1344
            params["height"] = 768

        for key in ("megapixels", "denoise", "fps"):
            params[key] = _as_float(params[key], key)
        for key in ("resolution_steps", "steps", "bit_depth"):
            params[key] = _as_int(params[key], key)
        if params["megapixels"] <= 0 or params["steps"] <= 0 or params["fps"] <= 0:
            raise ValueError("megapixels, steps, and fps must be greater than zero")
        if not 0 <= params["denoise"] <= 1:
            raise ValueError("denoise must be between 0 and 1")

        params["noise_seed"] = random.randint(0, 2**63 - 1) if params["noise_seed"] is None else _as_int(params["noise_seed"], "noise_seed")
        params["use_embedded_video_audio"] = _as_bool(params["use_embedded_video_audio"], "use_embedded_video_audio")
        if params["ref_image_size"] not in {"match", "max"}:
            raise ValueError("ref_image_size must be match or max")

        if params["shift_video"] is not None or params["shift_audio"] is not None:
            params["shift_video"] = _as_float(params["shift_video"] if params["shift_video"] is not None else 12.0, "shift_video")
            params["shift_audio"] = _as_float(params["shift_audio"] if params["shift_audio"] is not None else 3.0, "shift_audio")
            if params["shift_video"] <= 0 or params["shift_audio"] <= 0:
                raise ValueError("shift_video and shift_audio must be greater than zero")

        overrides = params["node_overrides"]
        if isinstance(overrides, str):
            try:
                overrides = json.loads(overrides)
            except json.JSONDecodeError as exc:
                raise ValueError("node_overrides must be a JSON object") from exc
        if not isinstance(overrides, dict) or not all(isinstance(value, dict) for value in overrides.values()):
            raise ValueError("node_overrides must map node ids to input objects")
        params["node_overrides"] = overrides

        if not params["filename_prefix"]:
            params["filename_prefix"] = f"video/MiniMax_H3_{mode}"
        return params

    def build(
        self,
        params: dict[str, Any],
        uploaded_assets: dict[str, list[str]],
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        options = options or {}
        p = params
        mode = p["mode"]
        model_node = "3"
        graph: dict[str, Any] = {
            "1": {"class_type": "VAELoader", "inputs": {"vae_name": p["video_vae"]}},
            "2": {"class_type": "VAELoader", "inputs": {"vae_name": p["audio_vae"]}},
            "3": {
                "class_type": "UNETLoader",
                "inputs": {
                    "unet_name": p["ref2va_unet"] if mode == "ref2va" else p["fl2va_unet"],
                    "weight_dtype": p["weight_dtype"],
                },
            },
            "5": {
                "class_type": "CLIPLoader",
                "inputs": {"clip_name": p["clip_name"], "type": p["clip_type"], "device": p["clip_device"]},
            },
            "6": {"class_type": "KSamplerSelect", "inputs": {"sampler_name": p["sampler_name"]}},
            "8": {"class_type": "RandomNoise", "inputs": {"noise_seed": p["noise_seed"]}},
            "10": {
                "class_type": "SamplerCustomAdvanced",
                "inputs": {
                    "noise": ["8", 0],
                    "guider": ["9", 0],
                    "sampler": ["6", 0],
                    "sigmas": ["7", 0],
                    "latent_image": ["20", 1],
                },
            },
            "11": {"class_type": "VAEDecode", "inputs": {"samples": ["10", 0], "vae": ["1", 0]}},
            "12": {"class_type": "VAEDecodeAudio", "inputs": {"samples": ["10", 0], "vae": ["2", 0]}},
            "13": {
                "class_type": "CreateVideo",
                "inputs": {"images": ["11", 0], "audio": ["12", 0], "fps": p["fps"], "bit_depth": p["bit_depth"]},
            },
            "14": {
                "class_type": "SaveVideo",
                "inputs": {
                    "video": ["13", 0],
                    "filename_prefix": p["filename_prefix"],
                    "format": p["format"],
                    "codec": p["codec"],
                },
            },
        }

        if p["shift_video"] is not None:
            graph["4"] = {
                "class_type": "MiniMaxH3SigmaShift",
                "inputs": {"model": ["3", 0], "shift_video": p["shift_video"], "shift_audio": p["shift_audio"]},
            }
            model_node = "4"
        graph["7"] = {
            "class_type": "BasicScheduler",
            "inputs": {"model": [model_node, 0], "scheduler": p["scheduler"], "steps": p["steps"], "denoise": p["denoise"]},
        }
        graph["9"] = {
            "class_type": "BasicGuider",
            "inputs": {"model": [model_node, 0], "conditioning": ["20", 0]},
        }

        names = self._asset_names(p, uploaded_assets)
        length_input: Any = p["length"]
        if p["length_expression"]:
            graph["15"] = {"class_type": "PrimitiveFloat", "inputs": {"value": p["duration"]}}
            graph["16"] = {
                "class_type": "ComfyMathExpression",
                "inputs": {"expression": str(p["length_expression"]), "values.a": ["15", 0]},
            }
            length_input = ["16", 1]
        if mode == "ref2va":
            self._build_reference_conditioning(graph, p, names, length_input)
        else:
            self._build_frame_conditioning(graph, p, names, options, length_input)

        for node_id, override in p["node_overrides"].items():
            node_id = str(node_id)
            if node_id not in graph:
                raise ValueError(f"node_overrides references unknown node {node_id}")
            graph[node_id]["inputs"].update(copy.deepcopy(override))
        return graph

    @staticmethod
    def _asset_names(params: dict[str, Any], uploaded: dict[str, list[str]]) -> dict[str, list[str]]:
        stored = params["asset_names"]
        return {key: [*(uploaded.get(key) or []), *(stored.get(key) or [])] for key in ASSET_KEYS}

    def _build_frame_conditioning(
        self,
        graph: dict[str, Any],
        p: dict[str, Any],
        names: dict[str, list[str]],
        options: dict[str, Any],
        length_input: Any,
    ) -> None:
        conditioning_node = str(options.get("conditioning_node") or self.conditioning_node)
        inputs: dict[str, Any] = {
            "clip": ["5", 0],
            "vae": ["1", 0],
            "prompt": p["prompt"],
            "length": length_input,
        }
        if p["mode"] == "t2va":
            inputs["width"] = p["width"]
            inputs["height"] = p["height"]
        else:
            graph["30"] = {"class_type": "LoadImage", "inputs": {"image": names["first_frame"][0]}}
            inputs["first_frame"] = ["30", 0]
            if p["mode"] == "fl2va":
                graph["31"] = {"class_type": "LoadImage", "inputs": {"image": names["last_frame"][0]}}
                inputs["last_frame"] = ["31", 0]
            if p["width"] is None:
                graph["32"] = {
                    "class_type": "ImageScaleToTotalPixels",
                    "inputs": {
                        "image": ["30", 0],
                        "upscale_method": p["upscale_method"],
                        "megapixels": p["megapixels"],
                        "resolution_steps": p["resolution_steps"],
                    },
                }
                graph["33"] = {"class_type": "GetImageSize", "inputs": {"image": ["32", 0]}}
                inputs["width"] = ["33", 0]
                inputs["height"] = ["33", 1]
            else:
                inputs["width"] = p["width"]
                inputs["height"] = p["height"]
        graph["20"] = {"class_type": conditioning_node, "inputs": inputs}

    @staticmethod
    def _build_reference_conditioning(
        graph: dict[str, Any], p: dict[str, Any], names: dict[str, list[str]], length_input: Any
    ) -> None:
        inputs: dict[str, Any] = {
            "clip": ["5", 0],
            "vae": ["1", 0],
            "audio_vae": ["2", 0],
            "prompt": p["prompt"],
            "width": p["width"],
            "height": p["height"],
            "length": length_input,
            "ref_image_size": p["ref_image_size"],
        }
        for index, name in enumerate(names["ref_images"]):
            node_id = str(40 + index)
            graph[node_id] = {"class_type": "LoadImage", "inputs": {"image": name}}
            inputs[f"ref_images.ref_image_{index}"] = [node_id, 0]

        for index, name in enumerate(names["ref_videos"]):
            load_id = str(60 + index * 2)
            components_id = str(61 + index * 2)
            graph[load_id] = {"class_type": "LoadVideo", "inputs": {"file": name}}
            graph[components_id] = {"class_type": "GetVideoComponents", "inputs": {"video": [load_id, 0]}}
            inputs[f"ref_videos.ref_video_{index}"] = [components_id, 0]
            if index < len(names["ref_video_audios"]):
                audio_id = str(80 + index)
                graph[audio_id] = {"class_type": "LoadAudio", "inputs": {"audio": names["ref_video_audios"][index]}}
                inputs[f"ref_video_audios.ref_video_audio_{index}"] = [audio_id, 0]
            elif p["use_embedded_video_audio"]:
                inputs[f"ref_video_audios.ref_video_audio_{index}"] = [components_id, 1]

        for index, name in enumerate(names["ref_audios"]):
            node_id = str(90 + index)
            graph[node_id] = {"class_type": "LoadAudio", "inputs": {"audio": name}}
            inputs[f"ref_audios.ref_audio_{index}"] = [node_id, 0]
        graph["20"] = {"class_type": "MiniMaxH3ReferenceToVideo", "inputs": inputs}

    def required_nodes(
        self,
        mode: str,
        options: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        assets: dict[str, list[dict[str, Any]]] | None = None,
    ) -> set[str]:
        options = options or {}
        assets = assets or {}
        required = {
            "VAELoader",
            "UNETLoader",
            "CLIPLoader",
            "KSamplerSelect",
            "BasicScheduler",
            "RandomNoise",
            "BasicGuider",
            "SamplerCustomAdvanced",
            "VAEDecode",
            "VAEDecodeAudio",
            "CreateVideo",
            "SaveVideo",
        }
        if params and params.get("shift_video") is not None:
            required.add("MiniMaxH3SigmaShift")
        if params and params.get("length_expression"):
            required.update({"PrimitiveFloat", "ComfyMathExpression"})
        if mode == "ref2va":
            required.add("MiniMaxH3ReferenceToVideo")
            names = params.get("asset_names", {}) if params else {}
            if not params or assets.get("ref_images") or names.get("ref_images"):
                required.add("LoadImage")
            if not params or assets.get("ref_videos") or names.get("ref_videos"):
                required.update({"LoadVideo", "GetVideoComponents"})
            if not params or assets.get("ref_video_audios") or assets.get("ref_audios") or names.get("ref_video_audios") or names.get("ref_audios"):
                required.add("LoadAudio")
        else:
            required.add(str(options.get("conditioning_node") or self.conditioning_node))
            if mode != "t2va":
                required.add("LoadImage")
                if not params or params.get("width") is None:
                    required.update({"ImageScaleToTotalPixels", "GetImageSize"})
        return required

    def schema(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "modes": {
                "t2va": "Text to video with native audio",
                "i2va": "First-frame image to video with native audio",
                "fl2va": "First and last frame to video with native audio",
                "ref2va": "Reference images, videos, and audio to video with native audio",
            },
            "parameters": {
                **copy.deepcopy(DEFAULTS),
                "mode": "auto | t2va | i2va | fl2va | ref2va",
                "first_frame": "multipart file, base64, or first_frame_name",
                "last_frame": "multipart file, base64, or last_frame_name",
                "ref_images": "up to 9 multipart files/base64 values or ref_image_names",
                "ref_videos": "up to 3 multipart files/base64 values or ref_video_names",
                "ref_video_audios": "optional audio paired by index with each reference video",
                "ref_audios": "up to 3 standalone reference audio inputs",
            },
        }
