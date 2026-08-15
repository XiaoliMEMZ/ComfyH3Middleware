#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def validate_graph(graph: dict[str, Any], object_info: dict[str, Any] | None = None) -> list[str]:
    errors: list[str] = []
    node_ids = {str(node_id) for node_id in graph}
    emitted_classes: set[str] = set()
    for node_id, node in graph.items():
        node_id = str(node_id)
        if not isinstance(node, dict):
            errors.append(f"node {node_id} must be an object")
            continue
        class_type = node.get("class_type")
        inputs = node.get("inputs")
        if not isinstance(class_type, str) or not class_type:
            errors.append(f"node {node_id} has no class_type")
        else:
            emitted_classes.add(class_type)
        if not isinstance(inputs, dict):
            errors.append(f"node {node_id} inputs must be an object")
            continue
        for name, value in inputs.items():
            if not (
                isinstance(value, list)
                and len(value) == 2
                and isinstance(value[1], int)
                and (isinstance(value[0], str) or str(value[0]) in node_ids)
            ):
                continue
            source_id = str(value[0])
            if source_id not in node_ids:
                errors.append(f"node {node_id} input {name} references missing node {source_id}")
            if not isinstance(value[1], int) or value[1] < 0:
                errors.append(f"node {node_id} input {name} has invalid output index {value[1]!r}")
    if object_info is not None:
        missing = sorted(emitted_classes - set(object_info))
        if missing:
            errors.append("missing node classes: " + ", ".join(missing))
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate structural references in a ComfyUI API prompt")
    parser.add_argument("graph", type=Path)
    parser.add_argument("--object-info", type=Path)
    args = parser.parse_args()
    graph = json.loads(args.graph.read_text(encoding="utf-8"))
    if not isinstance(graph, dict) or isinstance(graph.get("nodes"), list):
        parser.error("graph must be an API-format object, not a UI workflow")
    object_info = None
    if args.object_info:
        object_info = json.loads(args.object_info.read_text(encoding="utf-8"))
        if not isinstance(object_info, dict):
            parser.error("object info root must be an object")
    errors = validate_graph(graph, object_info)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print(f"OK: {len(graph)} nodes, {len({node['class_type'] for node in graph.values()})} classes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
