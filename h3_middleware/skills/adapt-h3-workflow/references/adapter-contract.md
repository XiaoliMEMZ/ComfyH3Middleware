# H3 Middleware Adapter Contract

## Contents

- Adapter boundary
- Normalized parameters
- Asset contract
- Graph contract
- Capability selection
- Test checklist

## Adapter boundary

`WorkflowAdapter` has four required methods:

```python
normalize(raw, assets, force_mode=None) -> dict
build(params, uploaded_assets, options=None) -> dict
required_nodes(mode, options=None, params=None, assets=None) -> set[str]
schema() -> dict
```

`normalize` owns public input validation and aliases. `build` owns only ComfyUI graph structure. `required_nodes` lets the scheduler avoid an incompatible upstream. `schema` describes the public surface.

The scheduler calls them in this order:

```text
HTTP request -> normalize -> persist local job -> select upstream
             -> upload local assets -> build graph -> POST /prompt
```

Do not upload during `normalize`; dispatch-time upload is required for failover.

## Normalized parameters

The built-in adapter uses canonical modes:

| Mode | Required media | Native H3 conditioning |
| --- | --- | --- |
| `t2va` | none | `MiniMaxH3ImageToVideo` without keyframes |
| `i2va` | first frame | `MiniMaxH3ImageToVideo` |
| `fl2va` | first and last frame | `MiniMaxH3ImageToVideo` |
| `ref2va` | reference image, video, or audio | `MiniMaxH3ReferenceToVideo` |

Preserve controls that affect current inference behavior. At minimum account for prompt, size, duration or length, seed, sampler, scheduler, steps, denoise, model files, weight dtype, text encoder, VAEs, FPS, bit depth, output format, codec, and model-specific patch values.

Normalize third-party conventions in the adapter. The scheduler and API must never unwrap model-specific values or inspect node class identities.

## Asset contract

`assets` contains local records retained with a queued job:

```json
{
  "first_frame": [{"path": "/local/file", "filename": "first.png", "index": 0}],
  "ref_videos": [{"path": "/local/file", "filename": "motion.mp4", "index": 0}]
}
```

`uploaded_assets` contains the corresponding names returned by the selected ComfyUI:

```json
{
  "first_frame": ["h3_middleware/<job-id>/first.png"],
  "ref_videos": ["h3_middleware/<job-id>/motion.mp4"]
}
```

Never put local paths in a ComfyUI prompt. Keep numbered reference inputs ordered and enforce the model's real limits. Pair reference video audio by index.

ComfyUI currently accepts arbitrary input media through `POST /upload/image` because that endpoint writes the supplied file without image decoding. Treat this as an upstream integration convention and keep it in `ComfyClient`, not an adapter.

## Graph contract

An API-format graph is a mapping from string node IDs to objects with `class_type` and `inputs`:

```json
{
  "20": {
    "class_type": "MiniMaxH3ImageToVideo",
    "inputs": {"clip": ["5", 0], "prompt": "...", "width": 1344}
  }
}
```

Every link must reference an existing node and a non-negative output index. The graph must have at least one output node reachable from the conditioning path.

ComfyUI v3 autogrow inputs use the serialized input names visible in the workflow, for example:

```text
ref_images.ref_image_0
ref_videos.ref_video_0
ref_video_audios.ref_video_audio_0
ref_audios.ref_audio_0
```

Do not infer these names from display labels. Confirm them in the exported workflow or current node schema.

## Capability selection

`required_nodes(mode, options, params, assets)` is a scheduling contract, not documentation. Missing a class can send a job to an upstream that rejects it; adding an unused class can incorrectly exclude a compatible upstream. Use `params` and `assets` to include request-specific optional nodes without excluding upstreams from simpler requests.

When an upstream option changes a class name, include that exact class. The built-in example accepts `options.conditioning_node` for the standard or staged I2VA node.

## Test checklist

- T2VA graph has no image loader or frame input.
- I2VA has exactly one first-frame loader.
- FL2VA requires and connects both keyframes.
- Ref2VA covers images, videos, paired soundtracks, and standalone audio.
- Explicit dimensions bypass derived sizing only where intended.
- Duration and explicit length reach the conditioning node.
- Every advertised request parameter changes a graph input or documented middleware behavior.
- Node overrides reject unknown node IDs.
- All graph links resolve.
- `required_nodes` covers each emitted class.
- Fake upstream accepts uploads, prompt submission, history polling, cancellation, and output proxying.
