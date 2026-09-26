from __future__ import annotations

import copy
from typing import Any

from .minimax_h3 import MiniMaxH3Adapter, _as_float


TURBO_LORA = "minimax_h3_fl2v_turbo_8step_v1.0_comfyui_bf16.safetensors"
FRAME_DEFAULTS = {
    "megapixels": 0.4,
    "sampler_name": "euler",
    "steps": 8,
    "shift_video": 12.0,
    "shift_audio": 3.0,
    "lora_name": TURBO_LORA,
}


class MiniMaxH3Turbo480pAdapter(MiniMaxH3Adapter):
    name = "minimax-h3-fl2va-turbo-480p"

    def normalize(
        self,
        raw: dict[str, Any],
        assets: dict[str, list[dict[str, Any]]],
        force_mode: str | None = None,
    ) -> dict[str, Any]:
        params = super().normalize(raw, assets, force_mode=force_mode)
        if params["mode"] in {"i2va", "fl2va"}:
            for key, value in FRAME_DEFAULTS.items():
                if key not in raw or raw[key] in (None, ""):
                    params[key] = value
            if "lora_name" in raw and raw["lora_name"] in (None, ""):
                params["lora_name"] = None
        else:
            params["lora_name"] = None
            if raw.get("lora_name") not in (None, ""):
                params["lora_name"] = raw["lora_name"]

        params["lora_strength"] = _as_float(raw.get("lora_strength", 1.0) or 1.0, "lora_strength")
        if not -100 <= params["lora_strength"] <= 100:
            raise ValueError("lora_strength must be between -100 and 100")
        if params["lora_name"] is not None:
            if not isinstance(params["lora_name"], str) or not params["lora_name"].strip():
                raise ValueError("lora_name must be a non-empty file name")
            params["lora_name"] = params["lora_name"].strip()
            if "/" in params["lora_name"] or "\\" in params["lora_name"]:
                raise ValueError("lora_name must be a file name without a path")
        return params

    def build(
        self,
        params: dict[str, Any],
        uploaded_assets: dict[str, list[str]],
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        p = copy.deepcopy(params)
        overrides = p.get("node_overrides", {})
        lora_overrides = overrides.pop("18", None)
        if lora_overrides is None:
            lora_overrides = overrides.pop(18, None)
        p["node_overrides"] = overrides
        graph = super().build(p, uploaded_assets, options)
        lora_name = p.get("lora_name")
        if not lora_name:
            if lora_overrides is not None:
                raise ValueError("node_overrides references unknown node 18")
            return graph

        graph["18"] = {
            "class_type": "LoraLoaderModelOnly",
            "inputs": {
                "model": ["3", 0],
                "lora_name": lora_name,
                "strength_model": p.get("lora_strength", 1.0),
            },
        }
        if lora_overrides is not None:
            graph["18"]["inputs"].update(copy.deepcopy(lora_overrides))

        model_node = "18"
        if graph.get("4", {}).get("class_type") == "MiniMaxH3SigmaShift":
            if graph["4"]["inputs"].get("model") == ["3", 0]:
                graph["4"]["inputs"]["model"] = ["18", 0]
            model_node = "4"
        for node_id in ("7", "9"):
            if graph[node_id]["inputs"].get("model") in (["3", 0], ["4", 0]):
                graph[node_id]["inputs"]["model"] = [model_node, 0]
        return graph

    def required_nodes(
        self,
        mode: str,
        options: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        assets: dict[str, list[dict[str, Any]]] | None = None,
    ) -> set[str]:
        required = super().required_nodes(mode, options, params=params, assets=assets)
        if (params and params.get("lora_name")) or (params is None and mode in {"i2va", "fl2va"}):
            required.add("LoraLoaderModelOnly")
        return required

    def schema(self) -> dict[str, Any]:
        schema = super().schema()
        schema["name"] = self.name
        schema["description"] = "MiniMax H3 FL2VA Turbo 480p adapter with the 8-step LoRA"
        schema["parameters"]["lora_name"] = f"optional file name; defaults to {TURBO_LORA} for i2va/fl2va; empty disables"
        schema["parameters"]["lora_strength"] = 1.0
        schema["mode_defaults"] = {
            mode: {**copy.deepcopy(FRAME_DEFAULTS), "lora_strength": 1.0}
            for mode in ("i2va", "fl2va")
        }
        return schema
