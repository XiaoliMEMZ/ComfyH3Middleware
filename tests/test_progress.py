from __future__ import annotations

import unittest

from h3_middleware.progress import apply_progress_event, create_progress, present_progress


class ProgressTests(unittest.TestCase):
    def test_progress_state_tracks_phase_percent_and_eta(self) -> None:
        graph = {
            "1": {"class_type": "UNETLoader", "inputs": {}},
            "10": {"class_type": "SamplerCustomAdvanced", "inputs": {}},
            "11": {"class_type": "VAEDecode", "inputs": {}},
        }
        progress = create_progress(graph, timestamp=10)
        progress = apply_progress_event(
            progress,
            {
                "type": "progress_state",
                "data": {
                    "prompt_id": "prompt",
                    "nodes": {
                        "1": {"node_id": "1", "state": "finished", "value": 1, "max": 1},
                        "10": {"node_id": "10", "state": "running", "value": 5, "max": 20},
                    },
                },
            },
            timestamp=20,
        )
        self.assertEqual(progress["phase"], "dit_sampling")
        self.assertEqual(progress["step"]["percent"], 25.0)
        self.assertEqual(progress["workflow"]["percent"], 41.67)

        progress = apply_progress_event(
            progress,
            {
                "type": "progress_state",
                "data": {
                    "prompt_id": "prompt",
                    "nodes": {
                        "10": {"node_id": "10", "state": "running", "value": 10, "max": 20},
                    },
                },
            },
            timestamp=30,
        )
        self.assertEqual(progress["eta_seconds"], 10.0)

        progress = apply_progress_event(
            progress,
            {"type": "executing", "data": {"prompt_id": "prompt", "node": "11"}},
            timestamp=31,
        )
        self.assertEqual(progress["phase"], "vae_decoding")
        self.assertEqual(progress["nodes"]["10"]["state"], "finished")

    def test_terminal_status_is_presented_without_stale_eta(self) -> None:
        progress = create_progress({"10": {"class_type": "SamplerCustomAdvanced", "inputs": {}}}, timestamp=10)
        progress = apply_progress_event(
            progress,
            {
                "type": "progress",
                "data": {"prompt_id": "prompt", "node": "10", "value": 5, "max": 20},
            },
            timestamp=20,
        )
        completed = present_progress(progress, "succeeded", 30)
        self.assertEqual(completed["phase"], "completed")
        self.assertEqual(completed["workflow"]["percent"], 100.0)
        self.assertIsNone(completed["eta_seconds"])

    def test_migrated_completed_job_without_nodes_is_complete(self) -> None:
        completed = present_progress({}, "succeeded", 30)
        self.assertEqual(completed["phase"], "completed")
        self.assertEqual(completed["workflow"]["percent"], 100.0)
        self.assertEqual(completed["workflow"]["total_nodes"], 0)


if __name__ == "__main__":
    unittest.main()
