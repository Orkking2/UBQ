import json
import tempfile
import unittest
from pathlib import Path

from scripts.plot_head_reload import aggregate, cell_label, load_samples


def row(strategy, repeat, throughput, failures, wide_loads):
    return {
        "repeat_index": repeat,
        "thread_count": 4,
        "batch_size": 16,
        "strategy": strategy,
        "reservations_per_sec": throughput,
        "cas_failures_per_reservation": failures,
        "wide_loads_per_reservation": wide_loads,
    }


class HeadReloadPlotTests(unittest.TestCase):
    def test_load_and_pair_samples(self):
        payload = {
            "benchmark": "head_reload",
            "schema_version": 1,
            "meta": {"machine_label": "test-box"},
            "results": [
                row("always_wide", 1, 100.0, 0.5, 1.5),
                row("token_gated", 1, 120.0, 0.4, 1.0),
                row("always_wide", 2, 80.0, 0.6, 1.6),
                row("token_gated", 2, 100.0, 0.5, 1.0),
            ],
        }
        with tempfile.TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "run.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            samples = load_samples(path)

        cell = aggregate(samples)["test-box"][(4, 16)]
        self.assertEqual(cell["paired_samples"], 2)
        self.assertAlmostEqual(cell["ratio"], (1.2 * 1.25) ** 0.5)
        self.assertAlmostEqual(cell["token_wide_loads"]["mean"], 1.0)

    def test_cell_labels_name_the_faster_strategy(self):
        self.assertEqual(cell_label(1.0), "1.00×")
        self.assertEqual(cell_label(1.2), "1.20× T")
        self.assertEqual(cell_label(0.8), "1.25× A")


if __name__ == "__main__":
    unittest.main()
