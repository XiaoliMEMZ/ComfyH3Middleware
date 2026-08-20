from __future__ import annotations

import copy
import time
from typing import Any


PROGRESS_EVENTS = {
    "executing",
    "executed",
    "execution_cached",
    "execution_error",
    "execution_interrupted",
    "execution_start",
    "execution_success",
    "progress",
    "progress_state",
}


def event_prompt_id(event: dict[str, Any]) -> str | None:
    data = event.get("data")
    if not isinstance(data, dict):
        return None
    prompt_id = data.get("prompt_id")
    if prompt_id:
        return str(prompt_id)
    for node in (data.get("nodes") or {}).values():
        if isinstance(node, dict) and node.get("prompt_id"):
            return str(node["prompt_id"])
    return None


def create_progress(graph: dict[str, Any], timestamp: float | None = None) -> dict[str, Any]:
    timestamp = time.time() if timestamp is None else timestamp
    nodes = {}
    for node_id, node in graph.items():
        node_id = str(node_id)
        nodes[node_id] = {
            "id": node_id,
            "type": node.get("class_type") if isinstance(node, dict) else None,
            "state": "pending",
            "value": 0,
            "max": 1,
            "percent": 0.0,
        }
    progress = {
        "source": "middleware",
        "phase": "submitted",
        "node": None,
        "step": None,
        "workflow": None,
        "eta_seconds": None,
        "eta_scope": None,
        "updated_at": timestamp,
        "nodes": nodes,
    }
    return _summarize(progress, timestamp=timestamp)


def lifecycle_progress(status: str, timestamp: float | None = None) -> dict[str, Any]:
    timestamp = time.time() if timestamp is None else timestamp
    return {
        "source": "middleware",
        "phase": _lifecycle_phase(status),
        "node": None,
        "step": None,
        "workflow": {
            "completed_nodes": 0,
            "total_nodes": 0,
            "percent": 100.0 if status == "succeeded" else 0.0,
            "method": "node_weighted",
        },
        "eta_seconds": None,
        "eta_scope": None,
        "updated_at": timestamp,
        "nodes": {},
    }


def apply_progress_event(
    current: dict[str, Any] | None,
    event: dict[str, Any],
    timestamp: float | None = None,
) -> dict[str, Any] | None:
    event_type = event.get("type")
    data = event.get("data")
    if event_type not in PROGRESS_EVENTS or not isinstance(data, dict):
        return None

    timestamp = time.time() if timestamp is None else timestamp
    progress = copy.deepcopy(current) if isinstance(current, dict) else lifecycle_progress("running", timestamp)
    progress["source"] = "comfy_websocket"
    progress["updated_at"] = timestamp
    nodes = progress.setdefault("nodes", {})
    current_id = _current_node_id(progress)
    phase = progress.get("phase") or "running"

    if event_type == "execution_start":
        phase = "starting"
    elif event_type == "execution_cached":
        for node_id in data.get("nodes") or []:
            _update_node(nodes, str(node_id), state="finished", timestamp=timestamp)
        phase = "preparing"
    elif event_type == "executing":
        node_id = data.get("node")
        if node_id is None:
            _finish_running_nodes(nodes, timestamp)
            current_id = None
            phase = "finalizing"
        else:
            node_id = str(node_id)
            _finish_running_nodes(nodes, timestamp, except_id=node_id)
            _update_node(
                nodes,
                node_id,
                state="running",
                display_id=data.get("display_node"),
                timestamp=timestamp,
            )
            current_id = node_id
            phase = phase_for_node_type(nodes[node_id].get("type"))
    elif event_type == "progress_state":
        for node_id, node_state in (data.get("nodes") or {}).items():
            if not isinstance(node_state, dict):
                continue
            node_id = str(node_state.get("node_id") or node_id)
            lookup_id = str(node_state.get("real_node_id") or node_state.get("display_node_id") or node_id)
            node_type = (nodes.get(node_id) or nodes.get(lookup_id) or {}).get("type")
            _update_node(
                nodes,
                node_id,
                state=str(node_state.get("state") or "running"),
                value=node_state.get("value"),
                maximum=node_state.get("max"),
                node_type=node_type,
                display_id=node_state.get("display_node_id"),
                timestamp=timestamp,
            )
            if nodes[node_id]["state"] == "running":
                current_id = node_id
        if current_id:
            phase = phase_for_node_type(nodes[current_id].get("type"))
    elif event_type == "progress":
        node_id = data.get("node") or current_id
        if node_id is not None:
            node_id = str(node_id)
            _update_node(
                nodes,
                node_id,
                state="running",
                value=data.get("value"),
                maximum=data.get("max"),
                timestamp=timestamp,
            )
            current_id = node_id
            phase = phase_for_node_type(nodes[node_id].get("type"))
    elif event_type == "executed":
        node_id = data.get("node")
        if node_id is not None:
            node_id = str(node_id)
            _update_node(nodes, node_id, state="finished", timestamp=timestamp)
            if current_id == node_id:
                current_id = None
        phase = "finalizing" if current_id is None else phase
    elif event_type == "execution_success":
        for node_id in list(nodes):
            _update_node(nodes, node_id, state="finished", timestamp=timestamp)
        current_id = None
        phase = "completed"
    elif event_type == "execution_error":
        node_id = data.get("node_id")
        if node_id is not None:
            node_id = str(node_id)
            _update_node(nodes, node_id, state="error", timestamp=timestamp)
            current_id = node_id
        phase = "failed"
    elif event_type == "execution_interrupted":
        phase = "interrupted"

    progress["phase"] = phase
    return _summarize(progress, current_id=current_id, timestamp=timestamp)


def present_progress(progress: dict[str, Any] | None, status: str, updated_at: float | None) -> dict[str, Any]:
    value = copy.deepcopy(progress) if isinstance(progress, dict) and progress else lifecycle_progress(status, updated_at)
    value.setdefault("source", "middleware")
    value.setdefault("nodes", {})
    value.setdefault("updated_at", updated_at)

    if status in {"queued", "retrying", "dispatching", "submitted", "upstream_unreachable", "canceling"}:
        if value.get("source") != "comfy_websocket" or status in {"upstream_unreachable", "canceling"}:
            value["phase"] = _lifecycle_phase(status)
    elif status == "running" and value.get("source") != "comfy_websocket":
        value["phase"] = "running"
    elif status in {"succeeded", "failed", "canceled"}:
        value["phase"] = _lifecycle_phase(status)
        value["eta_seconds"] = None
        value["eta_scope"] = None
        if status == "succeeded":
            for node_id in list(value["nodes"]):
                _update_node(value["nodes"], node_id, state="finished", timestamp=updated_at or time.time())
            value = _summarize(value, current_id=None, timestamp=value.get("updated_at") or updated_at)
            value["phase"] = "completed"
    return value


def phase_for_node_type(node_type: str | None) -> str:
    value = (node_type or "").lower()
    if "sampler" in value:
        return "dit_sampling"
    if "vaedecode" in value:
        return "vae_decoding"
    if "vaeencode" in value:
        return "vae_encoding"
    if value in {"vaeloader", "unetloader", "cliploader", "checkpointloader", "checkpointloadersimple"} or value.endswith("loader"):
        return "model_loading"
    if value.startswith("load") or "imagescale" in value or "getimagesize" in value or "getvideocomponents" in value:
        return "input_processing"
    if "conditioning" in value or "textencode" in value or value.startswith("minimaxh3"):
        return "conditioning"
    if "scheduler" in value or "guider" in value or "noise" in value:
        return "sampling_setup"
    if value == "createvideo":
        return "video_assembly"
    if value == "savevideo" or "videoencode" in value:
        return "video_encoding"
    return "executing"


def _update_node(
    nodes: dict[str, Any],
    node_id: str,
    *,
    state: str,
    timestamp: float,
    value: Any = None,
    maximum: Any = None,
    node_type: str | None = None,
    display_id: Any = None,
) -> None:
    node = nodes.setdefault(
        node_id,
        {"id": node_id, "type": node_type, "state": "pending", "value": 0, "max": 1, "percent": 0.0},
    )
    if node_type and not node.get("type"):
        node["type"] = node_type
    if display_id is not None:
        node["display_id"] = str(display_id)

    state = state if state in {"pending", "running", "finished", "error"} else "running"
    if state == "running" and node.get("state") != "running":
        node["started_at"] = timestamp
    node["state"] = state
    if value is not None:
        node["value"] = _number(value, node.get("value", 0))
    if maximum is not None:
        node["max"] = max(0, _number(maximum, node.get("max", 1)))
    if state == "finished":
        node["value"] = node.get("max", 1)
        node["finished_at"] = timestamp
    node["updated_at"] = timestamp
    node["percent"] = _percent(node.get("value", 0), node.get("max", 1))


def _summarize(
    progress: dict[str, Any],
    current_id: str | None = None,
    timestamp: float | None = None,
) -> dict[str, Any]:
    timestamp = time.time() if timestamp is None else timestamp
    nodes = progress.setdefault("nodes", {})
    if current_id is None:
        current_id = next((node_id for node_id, node in nodes.items() if node.get("state") == "running"), None)

    completed = sum(1 for node in nodes.values() if node.get("state") == "finished")
    partial = sum(
        min(1.0, max(0.0, float(node.get("percent", 0)) / 100.0))
        for node in nodes.values()
        if node.get("state") == "running"
    )
    total = len(nodes)
    workflow_percent = (
        round((completed + partial) * 100 / total, 2)
        if total
        else 100.0 if progress.get("phase") == "completed" else 0.0
    )
    progress["workflow"] = {
        "completed_nodes": completed,
        "total_nodes": total,
        "percent": workflow_percent,
        "method": "node_weighted",
    }

    node = nodes.get(current_id) if current_id else None
    if node:
        progress["node"] = {key: node.get(key) for key in ("id", "display_id", "type", "state") if node.get(key) is not None}
        progress["step"] = {
            "value": node.get("value", 0),
            "max": node.get("max", 1),
            "percent": node.get("percent", 0.0),
        }
        eta = _node_eta(node, timestamp)
        progress["eta_seconds"] = eta
        progress["eta_scope"] = "current_node" if eta is not None else None
    else:
        progress["node"] = None
        progress["step"] = None
        progress["eta_seconds"] = None
        progress["eta_scope"] = None
    progress["updated_at"] = timestamp
    return progress


def _finish_running_nodes(nodes: dict[str, Any], timestamp: float, except_id: str | None = None) -> None:
    for node_id, node in nodes.items():
        if node_id != except_id and node.get("state") == "running":
            _update_node(nodes, node_id, state="finished", timestamp=timestamp)


def _current_node_id(progress: dict[str, Any]) -> str | None:
    node = progress.get("node")
    if isinstance(node, dict) and node.get("id") is not None:
        return str(node["id"])
    return next((node_id for node_id, value in (progress.get("nodes") or {}).items() if value.get("state") == "running"), None)


def _node_eta(node: dict[str, Any], timestamp: float) -> float | None:
    started_at = node.get("started_at")
    value = float(node.get("value") or 0)
    maximum = float(node.get("max") or 0)
    if not started_at or value <= 0 or maximum <= value:
        return None
    elapsed = timestamp - float(started_at)
    if elapsed <= 0:
        return None
    return round(max(0.0, (maximum - value) * elapsed / value), 2)


def _number(value: Any, default: float | int) -> float | int:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return int(number) if number.is_integer() else number


def _percent(value: Any, maximum: Any) -> float:
    try:
        maximum = float(maximum)
        if maximum <= 0:
            return 0.0
        return round(min(100.0, max(0.0, float(value) * 100 / maximum)), 2)
    except (TypeError, ValueError):
        return 0.0


def _lifecycle_phase(status: str) -> str:
    return {
        "succeeded": "completed",
        "upstream_unreachable": "upstream_unreachable",
    }.get(status, status)
