import csv
import json
import tempfile
import unittest
from pathlib import Path

from scripts.summarize_atomic_fairness import (
    CSV_FIELDS,
    format_table,
    load_rows,
    summarize,
    write_csv,
)


def result(method, think_iters, updaters, repeat, mops, same, share, failures=0):
    return {
        "repeat_index": repeat,
        "updater_count": updaters,
        "block_size": 4095,
        "layout": "u64",
        "method": method,
        "think_iters": think_iters,
        "operations": 1_000,
        "ops_per_sec": mops * 1e6,
        "cas_failures": failures,
        "same_updater_fraction": same,
        "max_updater_share": share,
    }


def payload(results, schema_version=4):
    return {
        "benchmark": "atomic_updates",
        "schema_version": schema_version,
        "meta": {
            "machine_label": "test-box",
            "ordering": "ubq",
            "think_iteration_ns": 2.0,
        },
        "results": results,
    }


class AtomicFairnessSummaryTests(unittest.TestCase):
    def load(self, data):
        with tempfile.TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "run.json"
            path.write_text(json.dumps(data), encoding="utf-8")
            return load_rows(path)

    def test_ratio_and_fairness_are_reported_per_think_level(self):
        rows = self.load(
            payload(
                [
                    result("faa", 0, 2, 1, 90.0, 0.66, 0.50),
                    result("faa", 0, 2, 2, 100.0, 0.68, 0.50),
                    result("faa", 0, 2, 3, 95.0, 0.67, 0.51),
                    result("cas_backoff", 0, 2, 1, 130.0, 1.00, 0.72, 10),
                    result("cas_backoff", 0, 2, 2, 133.0, 1.00, 0.70, 10),
                    result("cas_backoff", 0, 2, 3, 120.0, 0.99, 0.74, 10),
                    result("faa", 32, 2, 1, 28.0, 0.00, 0.50),
                    result("cas_backoff", 32, 2, 1, 14.0, 1.00, 0.54, 100),
                    result("mixed", 0, 2, 1, 1.0, 0.0, 0.5),
                ]
            )
        )
        self.assertEqual(len(rows), 9)
        summaries = summarize(rows)
        by_key = {(s["think_iters"], s["method"]): s for s in summaries}

        zero_backoff = by_key[(0, "cas_backoff")]
        self.assertEqual(zero_backoff["samples"], 3)
        self.assertEqual(zero_backoff["median_mops"], 130.0)
        self.assertAlmostEqual(zero_backoff["ratio_to_faa"], 130.0 / 95.0)
        self.assertEqual(zero_backoff["same_updater_fraction"], 1.00)
        self.assertEqual(zero_backoff["max_updater_share"], 0.72)
        self.assertEqual(zero_backoff["fair_share"], 0.5)
        self.assertEqual(zero_backoff["counter_addresses"], 0)
        self.assertAlmostEqual(zero_backoff["cas_failures_per_update"], 0.01)

        paced_backoff = by_key[(32, "cas_backoff")]
        self.assertAlmostEqual(paced_backoff["ratio_to_faa"], 0.5)
        self.assertEqual(paced_backoff["think_ns"], 64.0)
        self.assertEqual(by_key[(0, "faa")]["ratio_to_faa"], 1.0)

        table = format_table(summaries)
        self.assertIn("test-box | ordering ubq | block 4095", table)
        self.assertIn("cas_backoff=1.00(0.72/0.50)", table)

    def test_other_layouts_and_blocks_are_filtered(self):
        other = result("faa", 0, 2, 1, 50.0, 0.5, 0.5)
        other["layout"] = "mixed_u128_u64"
        small = result("faa", 0, 2, 1, 40.0, 0.5, 0.5)
        small["block_size"] = 31
        rows = self.load(payload([other, small]))
        self.assertEqual(
            [s["block_size"] for s in summarize(rows)], [31]
        )
        self.assertEqual(summarize(rows, block_size=4095), [])
        self.assertEqual(
            [s["layout"] for s in summarize(rows, layout="mixed_u128_u64")],
            ["mixed_u128_u64"],
        )

    def test_distinct_counter_addresses_are_counted(self):
        results = [result("faa", 0, 2, repeat, 90.0, 0.6, 0.5) for repeat in (1, 2, 3)]
        for sample, address in zip(results, ("0x1000", "0x2000", "0x1000")):
            sample["counter_address_hex"] = address
        summaries = summarize(self.load(payload(results)))
        self.assertEqual(summaries[0]["counter_addresses"], 2)

    def test_older_schemas_are_ignored(self):
        self.assertEqual(
            self.load(payload([result("faa", 0, 1, 1, 10.0, 1.0, 1.0)], 3)), []
        )

    def test_csv_has_one_row_per_method_and_configuration(self):
        rows = self.load(
            payload(
                [
                    result("faa", 0, 4, 1, 80.0, 0.6, 0.26),
                    result("cas", 0, 4, 1, 10.0, 0.03, 0.25, 990),
                ]
            )
        )
        with tempfile.TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "summary.csv"
            write_csv(path, summarize(rows))
            with path.open(encoding="utf-8") as stream:
                written = list(csv.DictReader(stream))
        self.assertEqual(tuple(written[0].keys()), CSV_FIELDS)
        self.assertEqual([row["method"] for row in written], ["cas", "faa"])
        self.assertAlmostEqual(float(written[0]["ratio_to_faa"]), 0.125)


if __name__ == "__main__":
    unittest.main()
