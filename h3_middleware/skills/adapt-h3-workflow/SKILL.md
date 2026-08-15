---
name: adapt-h3-workflow
description: Adapt a differently shaped MiniMax H3 ComfyUI workflow to the h3_middleware gateway. Use when adding or updating a WorkflowAdapter, mapping T2VA/I2VA/FL2VA/Ref2VA inputs into another API-format graph, changing node IDs or custom node classes, checking an exported workflow, or validating an upstream's required ComfyUI nodes.
---

# Adapt an H3 Workflow

Create the smallest adapter change that maps the target workflow into the gateway without modifying ComfyUI core or leaking workflow details into queue, API, or upstream layers.

## Inspect the target

1. Read the repository `AGENTS.md`, `h3_middleware/workflows/base.py`, the closest existing adapter, and its tests.
2. Obtain an API-format prompt whenever possible. A UI workflow's `nodes` and `links` are useful evidence but are not directly accepted by `POST /prompt`.
3. Run `scripts/inspect_workflow.py <workflow.json>` to inventory node IDs, class types, widget values, and linked inputs.
4. Query only user-authorized ComfyUI instances. Use `GET /object_info/<class_type>` or `GET /object_info`; do not submit a live generation merely to validate an adapter.

Read [references/adapter-contract.md](references/adapter-contract.md) before implementing or reviewing an adapter.

## Implement the adapter

1. Add one focused module under `h3_middleware/workflows/` that implements `WorkflowAdapter`.
2. Give it a stable, unique `name` and register one instance in `workflows/__init__.py`.
3. Normalize aliases, numeric values, frame counts, optional inputs, and mode requirements at the adapter boundary. Keep local asset paths out of the graph.
4. Build a fresh API-format graph for every call. Connect uploaded ComfyUI input names supplied by `uploaded_assets` and any explicitly named upstream inputs in stable request order. Reject multiple values for singular frame inputs.
5. Return every node class that a mode can execute from `required_nodes()`. Include loaders and optional custom nodes selected through upstream `options`.
6. Keep per-upstream structural differences in `options`, such as `conditioning_node`. Keep per-request inference controls in normalized parameters.
7. Allow deliberate node input overrides only when the target node exists. Do not accept arbitrary class replacement through a generation request.

## Preserve gateway behavior

- Support every mode the adapter advertises. Do not silently turn a missing I2VA image into T2VA.
- Keep first/last frames and reference media mutually consistent with the selected mode.
- Preserve seed, model, dtype, sampler, scheduler, step, denoise, size, duration/length, output, audio/video, and model-specific controls that the source workflow exposes.
- Upload assets only after an upstream is selected so another upstream can be tried after a dispatch failure.
- Do not put queue priority, API identity, persistence IDs, or admin state into workflow code.
- Do not add outbound telemetry, model downloads, or background internet access.

## Verify

1. Add graph-shape tests for every supported mode and negative tests for invalid media combinations.
2. Add a fake-upstream service test when the adapter changes uploads, dispatch, outputs, or capability selection.
3. Write a representative graph to a temporary JSON file and run:

   ```bash
   python scripts/validate_graph.py graph.json
   python scripts/validate_graph.py graph.json --object-info object_info.json
   ```

4. Run the middleware suite from the repository root:

   ```bash
   .venv/bin/python -m unittest discover -s h3_middleware/tests -v
   ```

5. If a real upstream is authorized, compare required classes with its object info and validate through a non-executing local validation path. Ask before queueing a real model job.
