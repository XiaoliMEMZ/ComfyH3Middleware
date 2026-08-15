#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def summarize_ui(data: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    for node in data.get("nodes") or []:
        result.append(
            {
                "id": str(node.get("id")),
                "class_type": node.get("type"),
                "inputs": [value.get("name") for value in node.get("inputs") or []],
                "widgets": node.get("widgets_values") or [],
            }
        )
    return result


def summarize_api(data: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    for node_id, node in data.items():
        if not isinstance(node, dict) or "class_type" not in node:
            continue
        links = {
            name: value
            for name, value in (node.get("inputs") or {}).items()
            if isinstance(value, list) and len(value) == 2 and isinstance(value[0], (str, int))
        }
        literals = {
            name: value
            for name, value in (node.get("inputs") or {}).items()
            if name not in links
        }
        result.append(
            {
                "id": str(node_id),
                "class_type": node.get("class_type"),
                "links": links,
                "literals": literals,
            }
        )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize a ComfyUI UI workflow or API prompt")
    parser.add_argument("workflow", type=Path)
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()
    data = json.loads(args.workflow.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        parser.error("workflow root must be an object")
    workflow_type = "ui" if isinstance(data.get("nodes"), list) else "api"
    nodes = summarize_ui(data) if workflow_type == "ui" else summarize_api(data)
    if args.as_json:
        print(json.dumps({"type": workflow_type, "nodes": nodes}, indent=2, ensure_ascii=False))
    else:
        print(f"format: {workflow_type}; nodes: {len(nodes)}")
        for node in nodes:
            details = node.get("inputs") if workflow_type == "ui" else list(node.get("links", {}))
            print(f"{node['id']}: {node['class_type']} inputs={details}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
