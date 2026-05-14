import csv
import json
import tempfile
import unittest
from pathlib import Path

from scripts.plot_bench import (
    immediate_winner_variant_report,
    label_sort_key,
    load_records,
    queue_metadata,
    scenario_family,
    write_immediate_variant_csv,
)


def stats(ops):
    return {
        "mean_ops_per_sec": float(ops),
        "stddev_ops_per_sec": 0.0,
        "sem_ops_per_sec": 0.0,
        "samples": 1,
    }


class ImmediateWinnerVariantReportTest(unittest.TestCase):
    def test_four_part_immediate_neighbors_are_present(self):
        entries = {
            "segqueue": stats(1),
            "ubq_balanced,8,127,crossbeam": stats(100),
            "ubq_balanced,0,127,crossbeam": stats(90),
            "ubq_balanced,4,127,crossbeam": stats(90),
            "ubq_balanced,16,127,crossbeam": stats(90),
            "ubq_balanced,8,63,crossbeam": stats(90),
            "ubq_balanced,8,255,crossbeam": stats(90),
            "ubq_balanced,8,127,yield": stats(90),
        }

        report = immediate_winner_variant_report(entries, "1p1c")

        self.assertEqual([], report["missing_required_labels"])
        self.assertNotIn(
            "ubq_balanced,8,127,crossbeam,cas",
            report["required_labels"],
        )

    def test_pool_zero_is_an_immediate_neighbor(self):
        entries = {
            "ubq_balanced,1,127,crossbeam": stats(100),
            "ubq_balanced,0,127,crossbeam": stats(90),
            "ubq_balanced,2,127,crossbeam": stats(90),
            "ubq_balanced,1,63,crossbeam": stats(90),
            "ubq_balanced,1,255,crossbeam": stats(90),
            "ubq_balanced,1,127,yield": stats(90),
        }

        report = immediate_winner_variant_report(entries, "1p1c")

        self.assertIn(
            "ubq_balanced,0,127,crossbeam",
            report["present_required_labels"],
        )
        self.assertEqual([], report["missing_required_labels"])

    def test_pool_zero_is_included_for_nonzero_winner(self):
        entries = {
            "ubq_balanced,8,127,crossbeam": stats(100),
            "ubq_balanced,4,127,crossbeam": stats(90),
            "ubq_balanced,16,127,crossbeam": stats(90),
            "ubq_balanced,8,63,crossbeam": stats(90),
            "ubq_balanced,8,255,crossbeam": stats(90),
            "ubq_balanced,8,127,yield": stats(90),
        }

        report = immediate_winner_variant_report(entries, "1p1c")

        self.assertIn(
            "ubq_balanced,0,127,crossbeam",
            report["required_labels"],
        )
        self.assertIn(
            "ubq_balanced,0,127,crossbeam",
            report["missing_required_labels"],
        )
        self.assertEqual(
            ["ubq_balanced,0,127,crossbeam"],
            report["zero_pool_labels"],
        )

    def test_present_zero_pool_variant_is_selected_for_primary_plot(self):
        entries = {
            "segqueue": stats(1),
            "ubq_balanced,8,127,crossbeam": stats(100),
            "ubq_balanced,0,127,crossbeam": stats(90),
            "ubq_balanced,4,127,crossbeam": stats(80),
            "ubq_balanced,16,127,crossbeam": stats(70),
            "ubq_balanced,8,63,crossbeam": stats(60),
            "ubq_balanced,8,255,crossbeam": stats(50),
            "ubq_balanced,8,127,yield": stats(40),
            "ubq_balanced,32,127,crossbeam": stats(30),
        }

        report = immediate_winner_variant_report(entries, "1p1c")

        self.assertIn(
            "ubq_balanced,0,127,crossbeam",
            report["selected_labels"],
        )
        self.assertNotIn(
            "ubq_balanced,32,127,crossbeam",
            report["selected_labels"],
        )

    def test_immediate_variant_csv_marks_zero_pool_rows(self):
        entries = {
            "ubq_balanced,8,127,crossbeam": stats(100),
            "ubq_balanced,0,127,crossbeam": stats(90),
        }
        labels = [
            "ubq_balanced,0,127,crossbeam",
            "ubq_balanced,8,127,crossbeam",
            "ubq_balanced,16,127,crossbeam",
        ]

        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "coverage.csv"
            write_immediate_variant_csv(
                out_path,
                entries,
                "ubq_balanced,8,127,crossbeam",
                labels,
            )
            with out_path.open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))

        self.assertEqual("yes", rows[0]["is_zero_pool"])
        self.assertEqual("present", rows[0]["status"])
        self.assertEqual("no", rows[1]["is_zero_pool"])
        self.assertEqual("missing", rows[2]["status"])

    def test_explicit_cas_alias_counts_as_present(self):
        entries = {
            "ubq_balanced,8,127,crossbeam,cas": stats(100),
            "ubq_balanced,0,127,crossbeam,cas": stats(90),
            "ubq_balanced,4,127,crossbeam": stats(90),
            "ubq_balanced,16,127,crossbeam": stats(90),
            "ubq_balanced,8,63,crossbeam": stats(90),
            "ubq_balanced,8,255,crossbeam": stats(90),
            "ubq_balanced,8,127,yield": stats(90),
        }

        report = immediate_winner_variant_report(entries, "1p1c")

        self.assertEqual([], report["missing_required_labels"])

    def test_scenario_excludes_block_neighbor_below_producer_count(self):
        entries = {
            "ubq_balanced,8,127,crossbeam": stats(100),
            "ubq_balanced,0,127,crossbeam": stats(90),
            "ubq_balanced,4,127,crossbeam": stats(90),
            "ubq_balanced,16,127,crossbeam": stats(90),
            "ubq_balanced,8,255,crossbeam": stats(90),
            "ubq_balanced,8,127,yield": stats(90),
        }

        report = immediate_winner_variant_report(entries, "64p1c")

        self.assertNotIn(
            "ubq_balanced,8,63,crossbeam",
            report["required_labels"],
        )
        self.assertEqual([], report["missing_required_labels"])


class PublicationQueuePlotTest(unittest.TestCase):
    def test_publication_queue_labels_sort_after_existing_baselines(self):
        labels = [
            "wcq_65536",
            "lfqueue_32",
            "fastfifo_256",
            "concurrent-queue",
            "segqueue",
        ]

        self.assertEqual(
            [
                "segqueue",
                "concurrent-queue",
                "fastfifo_256",
                "lfqueue_32",
                "wcq_65536",
            ],
            sorted(labels, key=label_sort_key),
        )

    def test_publication_queue_metadata_identifies_paper_family(self):
        self.assertEqual("Nikolaev, DISC 2019", queue_metadata("lfqueue_32")["publication"])
        self.assertEqual(
            "Nikolaev/Ravindran, SPAA 2022",
            queue_metadata("wcq_65536")["publication"],
        )


class MetricExtractionTest(unittest.TestCase):
    def test_load_records_emits_derived_timing_metrics(self):
        payload = {
            "schema_version": 2,
            "meta": {
                "machine_label": "local",
                "scenario": "1p1c",
                "ubq_label": "balanced,8,127,crossbeam",
            },
            "results": [
                {
                    "queue": "ubq",
                    "mode": "throughput",
                    "ops_per_sec": 100.0,
                    "push_elapsed_ns": 11,
                    "pop_elapsed_ns": 17,
                },
                {
                    "queue": "segqueue",
                    "mode": "fill_drain",
                    "ops_per_sec": 50.0,
                    "fill_elapsed_ns": 23,
                    "drain_elapsed_ns": 29,
                },
                {
                    "queue": "concurrent-queue",
                    "mode": "app_log_fan_in",
                    "ops_per_sec": 75.0,
                    "avg_data_latency_ns": 31,
                },
            ],
        }

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "run.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            rows = list(load_records(path))

        by_mode = {(mode, label): value for _machine, mode, _scenario, label, value in rows}
        self.assertEqual(
            11.0,
            by_mode[("throughput_push_elapsed", "ubq_balanced,8,127,crossbeam")],
        )
        self.assertEqual(
            17.0,
            by_mode[("throughput_pop_elapsed", "ubq_balanced,8,127,crossbeam")],
        )
        self.assertEqual(23.0, by_mode[("fill_drain_fill_elapsed", "segqueue")])
        self.assertEqual(29.0, by_mode[("fill_drain_drain_elapsed", "segqueue")])
        self.assertEqual(75.0, by_mode[("app_log_fan_in", "concurrent-queue")])
        self.assertEqual(
            31.0,
            by_mode[("app_log_fan_in_data_latency", "concurrent-queue")],
        )


class ScenarioFamilyTest(unittest.TestCase):
    def test_classifies_paper_scaling_families(self):
        self.assertEqual("spsc", scenario_family("1p1c"))
        self.assertEqual("mpsc", scenario_family("8p1c"))
        self.assertEqual("spmc", scenario_family("1p8c"))
        self.assertEqual("mpmc", scenario_family("8p8c"))
        self.assertEqual("mixed", scenario_family("8p16c"))


if __name__ == "__main__":
    unittest.main()
