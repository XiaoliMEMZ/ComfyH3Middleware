from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from h3_middleware.output_store import OutputStore


class OutputStoreTests(unittest.TestCase):
    def test_store_path_is_atomic_and_readable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "outputs"
            source = Path(temporary) / "source.mp4"
            source.write_bytes(b"source-video")
            store = OutputStore(root)

            metadata = store.store_path("job-1", 0, "../result.mp4", source, "video/mp4")

            self.assertEqual(metadata["storage"], "local")
            self.assertEqual(store.read(metadata), (b"source-video", "video/mp4"))
            self.assertTrue((root / "job-1" / "0000" / "result.mp4").is_file())
            self.assertFalse(list(root.rglob(".partial-*")))

    def test_rejects_paths_outside_store(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = OutputStore(Path(temporary))
            with self.assertRaises(ValueError):
                store.read({"storage": "local", "path": "../../outside.mp4"})
