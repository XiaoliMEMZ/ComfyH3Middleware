from __future__ import annotations

import unittest

from h3_middleware.workflows.minimax_h3 import MiniMaxH3Adapter


def asset(kind: str, index: int = 0) -> dict:
    return {
        "kind": kind,
        "index": index,
        "filename": f"{kind}_{index}.png",
        "path": f"/tmp/{kind}_{index}.png",
        "size": 10,
        "content_type": "image/png",
    }


class MiniMaxH3AdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.adapter = MiniMaxH3Adapter()

    def test_t2va_graph(self) -> None:
        params = self.adapter.normalize({"prompt": "clouds", "noise_seed": 7}, {})
        self.assertEqual(params["mode"], "t2va")
        self.assertEqual(params["length"], 124)
        graph = self.adapter.build(params, {})
        self.assertEqual(graph["20"]["class_type"], "MiniMaxH3ImageToVideo")
        self.assertEqual(graph["5"]["inputs"]["clip_name"], "qwen3vl_32b_minimax_h3_nvfp4_awq.safetensors")
        self.assertNotIn("first_frame", graph["20"]["inputs"])
        self.assertEqual(graph["20"]["inputs"]["width"], 1344)
        self.assertEqual(graph["8"]["inputs"]["noise_seed"], 7)

    def test_i2va_derives_size_from_first_frame(self) -> None:
        assets = {"first_frame": [asset("first_frame")]}
        params = self.adapter.normalize({"prompt": "move"}, assets)
        graph = self.adapter.build(params, {"first_frame": ["job/first.png"]})
        self.assertEqual(params["mode"], "i2va")
        self.assertEqual(graph["30"]["inputs"]["image"], "job/first.png")
        self.assertEqual(graph["20"]["inputs"]["first_frame"], ["30", 0])
        self.assertEqual(graph["20"]["inputs"]["width"], ["33", 0])
        self.assertNotIn("31", graph)

    def test_fl2va_uses_both_frames_and_explicit_size(self) -> None:
        assets = {
            "first_frame": [asset("first_frame")],
            "last_frame": [asset("last_frame")],
        }
        params = self.adapter.normalize(
            {"mode": "fl2va", "prompt": "transition", "width": 864, "height": 480}, assets
        )
        graph = self.adapter.build(
            params,
            {"first_frame": ["job/first.png"], "last_frame": ["job/last.png"]},
        )
        self.assertEqual(graph["20"]["inputs"]["last_frame"], ["31", 0])
        self.assertEqual(graph["20"]["inputs"]["width"], 864)
        self.assertNotIn("32", graph)

    def test_ref2va_builds_all_reference_types(self) -> None:
        params = self.adapter.normalize(
            {
                "mode": "ref2va",
                "prompt": "Use <Picture 1>, <Video 1>, and <Audio 2>",
                "ref_image_names": ["shared/person.png"],
                "ref_video_names": ["shared/motion.mp4"],
                "ref_video_audio_names": ["shared/dialog.wav"],
                "ref_audio_names": ["shared/music.wav"],
            },
            {},
        )
        graph = self.adapter.build(params, {})
        inputs = graph["20"]["inputs"]
        self.assertEqual(graph["20"]["class_type"], "MiniMaxH3ReferenceToVideo")
        self.assertEqual(inputs["ref_images.ref_image_0"], ["40", 0])
        self.assertEqual(inputs["ref_videos.ref_video_0"], ["61", 0])
        self.assertEqual(inputs["ref_video_audios.ref_video_audio_0"], ["80", 0])
        self.assertEqual(inputs["ref_audios.ref_audio_0"], ["90", 0])
        self.assertEqual(graph["3"]["inputs"]["unet_name"], params["ref2va_unet"])

    def test_legacy_unet_and_length_expression_are_effective(self) -> None:
        params = self.adapter.normalize(
            {
                "prompt": "clouds",
                "unet_name": "custom.safetensors",
                "length_expression": "round(a * 24)",
                "node_overrides": {"7": {"steps": 9}},
            },
            {},
        )
        graph = self.adapter.build(params, {})
        self.assertEqual(graph["3"]["inputs"]["unet_name"], "custom.safetensors")
        self.assertEqual(graph["20"]["inputs"]["length"], ["16", 1])
        self.assertEqual(graph["7"]["inputs"]["steps"], 9)

    def test_explicit_length_overrides_expression(self) -> None:
        params = self.adapter.normalize(
            {"prompt": "clouds", "length": 141, "length_expression": "round(a * 24)"},
            {},
        )
        graph = self.adapter.build(params, {})
        self.assertEqual(graph["20"]["inputs"]["length"], 141)
        self.assertNotIn("15", graph)
        self.assertNotIn("ComfyMathExpression", self.adapter.required_nodes("t2va", params=params))

    def test_request_specific_nodes_are_reported(self) -> None:
        params = self.adapter.normalize(
            {"prompt": "clouds", "length_expression": "round(a * 24)", "shift_video": 12},
            {},
        )
        required = self.adapter.required_nodes("t2va", params=params)
        self.assertTrue({"PrimitiveFloat", "ComfyMathExpression", "MiniMaxH3SigmaShift"}.issubset(required))

    def test_invalid_mode_inputs_fail_clearly(self) -> None:
        with self.assertRaisesRegex(ValueError, "first_frame and last_frame"):
            self.adapter.normalize(
                {"mode": "fl2va", "prompt": "transition"},
                {"first_frame": [asset("first_frame")]},
            )
        with self.assertRaisesRegex(ValueError, "reference inputs"):
            self.adapter.normalize(
                {"mode": "i2va", "prompt": "move", "ref_image_names": ["ref.png"]},
                {"first_frame": [asset("first_frame")]},
            )


if __name__ == "__main__":
    unittest.main()
