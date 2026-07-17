import csv
import json
import tempfile
import unittest
from pathlib import Path

from scripts.plot_bench import (
    combined_scenario_line_labels,
    immediate_winner_variant_report,
    format_speedup_label,
    infer_machine_label_from_csv_dir,
    label_sort_key,
    load_generated_csv_grouped,
    load_records,
    machine_display_label,
    machine_family_entries,
    plot_scenario_lines,
    publication_display_label,
    publication_machine_entries,
    publication_metric_axis_label,
    publication_metric_column,
    publication_metric_value,
    publication_scenario_line_labels,
    queue_metadata,
    scenario_family,
    scenario_line_uses_log_y,
    scenario_line_labels,
    throughput_speedup_rows,
    write_immediate_variant_csv,
    write_machine_line_csv,
)


def stats(ops):
    return {
        "mean_ops_per_sec": float(ops),
        "stddev_ops_per_sec": 0.0,
        "sem_ops_per_sec": 0.0,
        "samples": 1,
    }


class FakeFigure:
    def tight_layout(self, **_kwargs):
        pass

    def savefig(self, *_args, **_kwargs):
        pass


class FakeAxes:
    def __init__(self):
        self.yscale = None
        self.grid_kwargs = None

    def plot(self, *_args, **_kwargs):
        pass

    def errorbar(self, *_args, **_kwargs):
        pass

    def set_xticks(self, *_args, **_kwargs):
        pass

    def set_xlabel(self, *_args, **_kwargs):
        pass

    def set_ylabel(self, *_args, **_kwargs):
        pass

    def set_yscale(self, scale):
        self.yscale = scale

    def set_title(self, *_args, **_kwargs):
        pass

    def grid(self, **kwargs):
        self.grid_kwargs = kwargs

    def legend(self, *_args, **_kwargs):
        pass


class FakePyplot:
    def __init__(self):
        self.last_axes = None

    def subplots(self, **_kwargs):
        self.last_axes = FakeAxes()
        return FakeFigure(), self.last_axes

    def get_cmap(self, *_args, **_kwargs):
        return lambda _idx: "black"

    def close(self, *_args, **_kwargs):
        pass


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


class ScenarioLineLabelsTest(unittest.TestCase):
    def test_max_series_retains_per_scenario_ubq_winners(self):
        baseline_entries = {
            "segqueue": stats(10),
            "concurrent-queue": stats(10),
            "fastfifo_64": stats(10),
            "fastfifo_256": stats(10),
            "fastfifo_1024": stats(10),
            "fastfifo_4096": stats(10),
            "lfqueue_32": stats(10),
            "lfqueue_256": stats(10),
            "lfqueue_1024": stats(10),
        }
        first_ubq_winner = "ubq_balanced,0,255,crossbeam"
        second_ubq_winner = "ubq_balanced,8,255,crossbeam"
        non_winning_ubq = "ubq_balanced,64,255,crossbeam"
        entries_by_scenario = {
            "2p1c": {
                **baseline_entries,
                first_ubq_winner: stats(300),
                second_ubq_winner: stats(200),
                non_winning_ubq: stats(100),
            },
            "4p1c": {
                **baseline_entries,
                first_ubq_winner: stats(1),
                second_ubq_winner: stats(400),
                non_winning_ubq: stats(100),
            },
        }

        labels = scenario_line_labels(entries_by_scenario, 10, "app_log_mpsc_file")

        self.assertGreater(len(labels), 10)
        self.assertIn(first_ubq_winner, labels)
        self.assertIn(second_ubq_winner, labels)
        self.assertNotIn(non_winning_ubq, labels)


class CombinedScenarioLineLabelsTest(unittest.TestCase):
    def test_csv_dir_machine_label_defaults_to_parent_for_plot_csv_folder(self):
        self.assertEqual(
            "grace",
            infer_machine_label_from_csv_dir(Path("bench_results/plots/grace/csv")),
        )
        self.assertEqual(
            "GraceData",
            infer_machine_label_from_csv_dir(Path("bench_results/GraceData")),
        )

    def test_machine_family_entries_filters_to_mpsc_modes(self):
        grouped = {
            "grace": {
                "app_log_mpsc_file_producer_throughput": {
                    "2p1c": {"segqueue": stats(20)},
                    "4p1c": {"segqueue": stats(40)},
                    "1p2c": {"segqueue": stats(30)},
                }
            },
            "mn5": {
                "throughput": {
                    "1p1c": {"segqueue": stats(10)},
                    "2p1c": {"segqueue": stats(20)},
                }
            },
        }

        entries = machine_family_entries(
            grouped,
            "app_log_mpsc_file_producer_throughput",
            "mpsc",
        )

        self.assertEqual(["grace"], list(entries))
        self.assertEqual(["2p1c", "4p1c"], entries["grace"][0])

    def test_publication_machine_names_and_filter_drop_lab(self):
        machine_entries = {
            "grace": (["2p1c"], {"2p1c": {"segqueue": stats(10)}}),
            "hebrides": (["2p1c"], {"2p1c": {"segqueue": stats(10)}}),
            "MN5": (["2p1c"], {"2p1c": {"segqueue": stats(10)}}),
            "lab": (["2p1c"], {"2p1c": {"segqueue": stats(10)}}),
        }

        filtered = publication_machine_entries(machine_entries)

        self.assertEqual({"grace", "hebrides", "MN5"}, set(filtered))
        self.assertEqual("Grace", machine_display_label("grace"))
        self.assertEqual("N1", machine_display_label("hebrides"))
        self.assertEqual("Xeon", machine_display_label("MN5"))

    def test_machine_line_csv_can_emit_publication_machine_names(self):
        machine_entries = {
            "hebrides": (["2p1c"], {"2p1c": {"segqueue": stats(10)}}),
        }

        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "machines.csv"
            write_machine_line_csv(
                out_path,
                "throughput",
                machine_entries,
                ["segqueue"],
                machine_formatter=machine_display_label,
            )
            rows = out_path.read_text(encoding="utf-8").splitlines()

        self.assertEqual("machine,scenario,queue,ops_per_sec,stddev,sem,samples", rows[0])
        self.assertTrue(rows[1].startswith("N1,2p1c,segqueue,10.000000"))

    def test_combined_labels_keep_global_baselines_and_machine_ubq_winners(self):
        first_winner = "ubq_balanced,0,127,crossbeam"
        second_winner = "ubq_balanced,8,127,crossbeam"
        machine_entries = {
            "grace": (
                ["2p1c"],
                {
                    "2p1c": {
                        "segqueue": stats(10),
                        "fastfifo_64": stats(11),
                        first_winner: stats(100),
                        second_winner: stats(1),
                    }
                },
            ),
            "mn5": (
                ["4p1c"],
                {
                    "4p1c": {
                        "segqueue": stats(10),
                        "fastfifo_64": stats(11),
                        first_winner: stats(1),
                        second_winner: stats(120),
                    }
                },
            ),
        }

        labels = combined_scenario_line_labels(
            machine_entries,
            2,
            "app_log_mpsc_file_producer_throughput",
        )

        self.assertEqual(["segqueue", "fastfifo_64"], labels[:2])
        self.assertIn(first_winner, labels)
        self.assertIn(second_winner, labels)

    def test_publication_labels_pick_one_representative_per_queue_family(self):
        slow_ubq = "ubq_balanced,0,127,crossbeam"
        fast_ubq = "ubq_balanced,8,127,crossbeam"
        machine_entries = {
            "grace": (
                ["2p1c"],
                {
                    "2p1c": {
                        "segqueue": stats(10),
                        "concurrent-queue": stats(11),
                        "fastfifo_64": stats(50),
                        "fastfifo_256": stats(40),
                        "lfqueue_32": stats(30),
                        "lfqueue_256": stats(20),
                        slow_ubq: stats(90),
                        fast_ubq: stats(100),
                    }
                },
            )
        }

        labels = publication_scenario_line_labels(
            machine_entries,
            "app_log_mpsc_file_producer_throughput",
        )

        self.assertEqual(
            [
                "segqueue",
                "concurrent-queue",
                "fastfifo_64",
                "lfqueue_32",
                fast_ubq,
            ],
            labels,
        )
        self.assertEqual(
            ["segqueue", "concurrent-queue", "BBQ", "LSCQ", "UBQ"],
            [publication_display_label(label) for label in labels],
        )

    def test_publication_elapsed_uses_milliseconds(self):
        mode = "app_log_mpsc_file_push_elapsed"

        self.assertEqual("push_elapsed_ms", publication_metric_column(mode))
        self.assertEqual("Elapsed time (ms)", publication_metric_axis_label(mode))
        self.assertEqual(12.5, publication_metric_value(mode, 12_500_000.0))


class ScenarioLinePlotTest(unittest.TestCase):
    def test_push_elapsed_scaling_uses_log_y_axis(self):
        self.assertTrue(scenario_line_uses_log_y("throughput_push_elapsed"))
        self.assertFalse(scenario_line_uses_log_y("throughput"))

        plot = FakePyplot()
        with tempfile.TemporaryDirectory() as tmp:
            plot_scenario_lines(
                plot,
                Path(tmp) / "push_elapsed.png",
                "local",
                "throughput_push_elapsed",
                ["1p1c", "2p1c"],
                ["segqueue"],
                {
                    "1p1c": {"segqueue": stats(1000)},
                    "2p1c": {"segqueue": stats(2000)},
                },
                error_bars="none",
                family="mpsc",
            )

        self.assertEqual("log", plot.last_axes.yscale)
        self.assertEqual("both", plot.last_axes.grid_kwargs["which"])

    def test_non_push_elapsed_scaling_keeps_linear_y_axis(self):
        plot = FakePyplot()
        with tempfile.TemporaryDirectory() as tmp:
            plot_scenario_lines(
                plot,
                Path(tmp) / "throughput.png",
                "local",
                "throughput",
                ["1p1c", "2p1c"],
                ["segqueue"],
                {
                    "1p1c": {"segqueue": stats(1000)},
                    "2p1c": {"segqueue": stats(2000)},
                },
                error_bars="none",
            )

        self.assertIsNone(plot.last_axes.yscale)
        self.assertEqual("major", plot.last_axes.grid_kwargs["which"])


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


class ThroughputSpeedupGridTest(unittest.TestCase):
    def test_speedup_labels_stay_compact_for_heatmap_cells(self):
        self.assertEqual("3.63x", format_speedup_label(3.626))
        self.assertEqual("32.5x", format_speedup_label(32.51))
        self.assertEqual("534x", format_speedup_label(533.9))

    def test_rows_compare_best_valid_ubq_to_selected_baselines(self):
        entries_by_scenario = {
            "64p1c": {
                "ubq_balanced,8,31,crossbeam": stats(100.0),
                "ubq_balanced,8,127,crossbeam": stats(50.0),
                "segqueue": stats(10.0),
                "fastfifo_64": stats(20.0),
                "fastfifo_256": stats(25.0),
                "lfqueue_32": stats(5.0),
                "lfqueue_256": stats(40.0),
            }
        }

        rows = throughput_speedup_rows(entries_by_scenario)
        by_comparison = {row["comparison"]: row for row in rows}

        self.assertEqual({"segqueue", "bbq", "lscq"}, set(by_comparison))
        self.assertEqual(
            "ubq_balanced,8,127,crossbeam",
            by_comparison["segqueue"]["ubq_queue"],
        )
        self.assertEqual(5.0, by_comparison["segqueue"]["speedup"])
        self.assertEqual("fastfifo_256", by_comparison["bbq"]["baseline_queue"])
        self.assertEqual(2.0, by_comparison["bbq"]["speedup"])
        self.assertEqual("lfqueue_256", by_comparison["lscq"]["baseline_queue"])
        self.assertEqual(1.25, by_comparison["lscq"]["speedup"])


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
                {
                    "queue": "segqueue",
                    "mode": "app_log_mpsc_file",
                    "ops_per_sec": 91.0,
                    "producer_ops_per_sec": 123.0,
                    "consumer_ops_per_sec": 89.0,
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
        self.assertEqual(91.0, by_mode[("app_log_mpsc_file", "segqueue")])
        self.assertEqual(
            123.0,
            by_mode[("app_log_mpsc_file_producer_throughput", "segqueue")],
        )
        self.assertEqual(
            89.0,
            by_mode[("app_log_mpsc_file_consumer_throughput", "segqueue")],
        )

    def test_load_generated_csv_grouped_reads_existing_plot_csv_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_dir = Path(tmp)
            mode_dir = csv_dir / "throughput"
            mode_dir.mkdir()
            (mode_dir / "1p1c_throughput.csv").write_text(
                "\n".join(
                    [
                        "queue,ops_per_sec,stddev,sem,samples",
                        "segqueue,10.5,1.5,0.5,4",
                        "concurrent-queue,9.0,0.0,0.0,4",
                    ]
                ),
                encoding="utf-8",
            )
            (mode_dir / "scenarios_line_throughput.csv").write_text(
                "\n".join(
                    [
                        "scenario,queue,ops_per_sec,stddev,sem,samples",
                        "1p1c,segqueue,10.5,1.5,0.5,4",
                    ]
                ),
                encoding="utf-8",
            )

            grouped = load_generated_csv_grouped(csv_dir, "grace")

        stats_row = grouped["grace"]["throughput"]["1p1c"]["segqueue"]
        self.assertEqual(10.5, stats_row["mean_ops_per_sec"])
        self.assertEqual(1.5, stats_row["stddev_ops_per_sec"])
        self.assertEqual(0.5, stats_row["sem_ops_per_sec"])
        self.assertEqual(4, stats_row["samples"])
        self.assertNotIn("scenarios_line", grouped["grace"]["throughput"])


class ScenarioFamilyTest(unittest.TestCase):
    def test_classifies_paper_scaling_families(self):
        self.assertEqual("spsc", scenario_family("1p1c"))
        self.assertEqual("mpsc", scenario_family("8p1c"))
        self.assertEqual("spmc", scenario_family("1p8c"))
        self.assertEqual("mpmc", scenario_family("8p8c"))
        self.assertEqual("mixed", scenario_family("8p16c"))


if __name__ == "__main__":
    unittest.main()
