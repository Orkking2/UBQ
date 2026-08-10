import csv
import json
import tempfile
import unittest
from pathlib import Path

from scripts.plot_atomic_updates import (
    LAYOUT_MARKERS,
    METHOD_COLORS,
    aggregate,
    collect_jsons,
    load_samples,
    monochrome_shades,
    select_weighted_winners,
    write_machine_csv,
    write_winners_csv,
)


def result(
    layout,
    method,
    block_size,
    updaters,
    repeat,
    ops_per_sec,
    failures=0,
    wide_loads=0,
):
    operations = 1_000
    return {
        "repeat_index": repeat,
        "updater_count": updaters,
        "block_size": block_size,
        "layout": layout,
        "method": method,
        "operations": operations,
        "elapsed_ns": 1_000,
        "ops_per_sec": ops_per_sec,
        "cas_failures": failures,
        "wide_loads": wide_loads,
    }


class AtomicUpdatePlotTests(unittest.TestCase):
    def test_collect_jsons_accepts_comma_and_space_separated_files(self):
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            first = root / "first.json"
            second = root / "second.json"
            third = root / "third.json"
            for path in (first, second, third):
                path.write_text("{}", encoding="utf-8")

            files = collect_jsons(
                [f"{first}, {second}", str(third)],
                [],
            )

        self.assertEqual(files, [first, second, third])

    def test_collect_jsons_filters_runs_dir_by_basename(self):
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            selected = root / "machine-a" / "atomic_updates" / "selected.json"
            other_selected = (
                root / "machine-b" / "atomic_updates" / "selected.json"
            )
            ignored = root / "machine-a" / "atomic_updates" / "ignored.json"
            for path in (selected, other_selected, ignored):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("{}", encoding="utf-8")

            filtered = collect_jsons(["selected.json"], [root])
            unfiltered = collect_jsons([], [root])

        self.assertEqual(filtered, [selected, other_selected])
        self.assertEqual(unfiltered, [ignored, selected, other_selected])

    def test_visual_encoding_uses_ordered_shades_and_layout_shapes(self):
        shades = monochrome_shades(METHOD_COLORS["cas"], 5)
        luminance = lambda color: sum(color) / len(color)

        self.assertEqual(len(shades), 5)
        self.assertTrue(
            all(
                luminance(left) > luminance(right)
                for left, right in zip(shades, shades[1:])
            )
        )
        self.assertEqual(LAYOUT_MARKERS["u64"], "s")
        self.assertEqual(LAYOUT_MARKERS["mixed_u128_u64"], "*")

    def test_load_and_aggregate_samples(self):
        payload = {
            "benchmark": "atomic_updates",
            "schema_version": 3,
            "meta": {
                "machine_label": "test-box",
                "ordering": "ubq",
                "block_sizes": [31, 511],
                "alignment": 4096,
            },
            "results": [
                result("u64", "cas", 31, 2, 1, 10.0, 100),
                result("u64", "cas", 31, 2, 2, 14.0, 200),
                result(
                    "mixed_u128_u64",
                    "cas_backoff",
                    511,
                    2,
                    1,
                    18.0,
                    50,
                    10,
                ),
                result(
                    "mixed_u128_u64",
                    "cas_backoff",
                    511,
                    2,
                    2,
                    22.0,
                    70,
                    20,
                ),
                result("u64", "faa", 31, 2, 1, 30.0),
                result("u64", "faa", 31, 2, 2, 34.0),
                result("u64", "segqueue", 31, 2, 1, 24.0, 80),
                result("u64", "segqueue", 31, 2, 2, 28.0, 120),
                result("mixed_u128_u64", "segqueue", 31, 2, 1, 99.0),
            ],
        }
        with tempfile.TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "run.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            samples = load_samples(path)

        self.assertEqual(len(samples), 8)
        updater_group = aggregate(samples)["test-box"]["ubq"][4096][2]
        cas = updater_group[("u64", "cas", 31)]
        cas_backoff = updater_group[("mixed_u128_u64", "cas_backoff", 511)]
        faa = updater_group[("u64", "faa", 31)]
        segqueue = updater_group[("u64", "segqueue", 31)]
        self.assertEqual(cas["throughput"]["mean"], 12.0)
        self.assertAlmostEqual(cas["retries"]["mean"], 0.15)
        self.assertEqual(cas_backoff["throughput"]["mean"], 20.0)
        self.assertAlmostEqual(cas_backoff["retries"]["mean"], 0.06)
        self.assertAlmostEqual(cas_backoff["wide_loads"]["mean"], 0.015)
        self.assertEqual(faa["throughput"]["mean"], 32.0)
        self.assertNotIn("retries", faa)
        self.assertEqual(segqueue["throughput"]["mean"], 26.0)
        self.assertAlmostEqual(segqueue["retries"]["mean"], 0.1)

    def test_high_thread_weighting_selects_the_scaling_block(self):
        samples = []
        for updaters, small, large in ((1, 100.0, 70.0), (8, 80.0, 100.0)):
            samples.extend(
                [
                    {
                        "machine": "test-box",
                        "ordering": "ubq",
                        "alignment": 4096,
                        "block_size": 31,
                        "updaters": updaters,
                        "repeat": 1,
                        "layout": "u64",
                        "method": "faa",
                        "operations": 1000,
                        "ops_per_sec": small,
                        "cas_failures": 0,
                        "cas_retries_per_update": None,
                        "wide_loads_per_update": None,
                    },
                    {
                        "machine": "test-box",
                        "ordering": "ubq",
                        "alignment": 4096,
                        "block_size": 511,
                        "updaters": updaters,
                        "repeat": 1,
                        "layout": "u64",
                        "method": "faa",
                        "operations": 1000,
                        "ops_per_sec": large,
                        "cas_failures": 0,
                        "cas_retries_per_update": None,
                        "wide_loads_per_update": None,
                    },
                ]
            )
        updater_groups = aggregate(samples)["test-box"]["ubq"][4096]
        winner = select_weighted_winners(updater_groups)[("u64", "faa")]
        self.assertEqual(winner["block_size"], 511)
        self.assertGreater(winner["score"], 0.96)

    def test_non_atomic_schema_is_ignored(self):
        payload = {
            "benchmark": "queue",
            "schema_version": 5,
            "meta": {"machine_label": "test-box"},
            "results": [result("u64", "cas", 31, 1, 1, 10.0)],
        }
        with tempfile.TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "run.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            self.assertEqual(load_samples(path), [])

    def test_csvs_include_block_and_winner_selection(self):
        samples = [
            {
                "machine": "test-box",
                "ordering": "relaxed",
                "alignment": 4096,
                "block_size": 511,
                "updaters": 1,
                "repeat": 1,
                "layout": "mixed_u128_u64",
                "method": "cas",
                "operations": 100,
                "ops_per_sec": 20.0,
                "cas_failures": 5,
                "cas_retries_per_update": 0.05,
                "wide_loads_per_update": 0.01,
            }
        ]
        grouped = aggregate(samples)["test-box"]
        with tempfile.TemporaryDirectory() as raw_dir:
            samples_path = Path(raw_dir) / "atomic_updates.csv"
            winners_path = Path(raw_dir) / "atomic_update_winners.csv"
            write_machine_csv(samples_path, grouped)
            write_winners_csv(winners_path, grouped)
            with samples_path.open(newline="", encoding="utf-8") as stream:
                rows = list(csv.DictReader(stream))
            with winners_path.open(newline="", encoding="utf-8") as stream:
                winner_rows = list(csv.DictReader(stream))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ordering"], "relaxed")
        self.assertEqual(rows[0]["block_size"], "511")
        self.assertEqual(rows[0]["layout"], "mixed_u128_u64")
        self.assertEqual(rows[0]["mean_wide_loads_per_update"], "0.010000000")
        self.assertEqual(len(winner_rows), 1)
        self.assertEqual(winner_rows[0]["selected_block_size"], "511")


if __name__ == "__main__":
    unittest.main()
