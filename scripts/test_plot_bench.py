import csv
import json
import tempfile
import unittest
from pathlib import Path

from scripts.plot_bench import (
    aggregate_family_variation_labels,
    batch_comparison_line_labels,
    batch_comparison_scenario_groups,
    batch_comparison_series_styles,
    best_family_variation_labels,
    clear_generated_outputs,
    combined_scenario_line_labels,
    display_label,
    deduplicate_logical_samples,
    format_batched_dubq_label,
    format_batched_segqueue_label,
    format_batched_ubq_label,
    grid_coverage_report,
    grid_winner_variant_report,
    immediate_winner_variant_report,
    format_speedup_label,
    infer_machine_label_from_csv_dir,
    is_obsolete_plot_mode,
    label_sort_key,
    load_generated_csv_grouped,
    load_grid_coverage,
    load_record_samples,
    load_records,
    merge_grid_coverage,
    machine_display_label,
    machine_family_entries,
    plot_scenario_lines,
    parse_batched_dubq_label,
    parse_batched_segqueue_label,
    parse_dubq_variant,
    preferred_core_placements,
    publication_display_label,
    publication_machine_entries,
    publication_metric_axis_label,
    publication_metric_column,
    publication_metric_value,
    publication_scenario_line_labels,
    queue_label_legend_title,
    queue_metadata,
    scenario_family,
    scenario_line_uses_log_y,
    scenario_line_labels,
    split_bar_entries_by_method,
    summarize_ops,
    symmetric_scenarios,
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
        self.legend_kwargs = None
        self.transAxes = None

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
        self.legend_kwargs = _kwargs

    def text(self, *_args, **_kwargs):
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


class DisplayLabelTest(unittest.TestCase):
    def test_queue_variants_use_human_readable_labels(self):
        self.assertEqual("SegQueue", display_label("segqueue"))
        self.assertEqual("ConcurrentQueue", display_label("concurrent-queue"))
        self.assertEqual(
            "UBQ 8,127,Cycle",
            display_label("ubq_balanced,8,127,crossbeam"),
        )
        self.assertEqual(
            "UBQ 8,127,Yield",
            display_label("ubq_balanced,8,127,yield"),
        )

    def test_batched_ubq_label_omits_internal_preset(self):
        label = format_batched_ubq_label("balanced,4,255,yield", 64)

        self.assertEqual(
            "UBQb (64) 4,255,Yield",
            display_label(label),
        )

    def test_batched_segqueue_label_is_distinct_from_scalar(self):
        label = format_batched_segqueue_label(32)

        self.assertEqual("SegQueueb (32)", display_label(label))
        self.assertEqual(32, parse_batched_segqueue_label(label))
        self.assertIsNone(parse_batched_segqueue_label("segqueue"))

    def test_dynamic_variants_follow_the_same_vocabulary(self):
        self.assertEqual(
            "DUBQ 2,31,Cycle",
            display_label("dubq_2,31,crossbeam"),
        )
        label = format_batched_dubq_label("2,31,yield", 8)
        self.assertEqual(
            "DUBQb (8) 2,31,Yield",
            display_label(label),
        )
        self.assertEqual("DUBQb", publication_display_label(label))

    def test_queue_label_legend_explains_and_aligns_fields(self):
        title = queue_label_legend_title(
            [
                "ubq_balanced,8,127,crossbeam",
                format_batched_dubq_label("2,31,yield", 8),
            ]
        )

        self.assertIsNotNone(title)
        _heading, batched_schema, scalar_schema = title.splitlines()
        self.assertEqual(batched_schema.index("POOL_SZ"), scalar_schema.index("POOL_SZ"))
        self.assertEqual(
            "D?UBQb (BATCH_SZ)  POOL_SZ,BLK_SZ,BACKOFF",
            batched_schema,
        )
        self.assertEqual(
            "D?UBQ              POOL_SZ,BLK_SZ,BACKOFF",
            scalar_schema,
        )


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
    def test_line_plot_selects_one_variation_per_queue_family(self):
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

        labels = scenario_line_labels(entries_by_scenario, 10, "throughput")

        self.assertEqual(5, len(labels))
        self.assertNotIn(first_ubq_winner, labels)
        self.assertIn(second_ubq_winner, labels)
        self.assertNotIn(non_winning_ubq, labels)
        self.assertEqual(
            1,
            len([label for label in labels if label.startswith("fastfifo_")]),
        )
        self.assertEqual(
            1,
            len([label for label in labels if label.startswith("lfqueue_")]),
        )

    def test_contention_weight_prefers_high_contention_winner(self):
        low_contention_winner = "ubq_balanced,0,127,crossbeam"
        high_contention_winner = "ubq_balanced,8,127,crossbeam"
        entries_by_scenario = {
            "4p1c": {
                low_contention_winner: stats(1_000),
                high_contention_winner: stats(100),
            },
            "64p1c": {
                low_contention_winner: stats(1),
                high_contention_winner: stats(100),
            },
        }

        labels = aggregate_family_variation_labels(entries_by_scenario, "throughput")

        self.assertEqual([high_contention_winner], labels)

    def test_substantial_low_contention_win_can_overcome_weighting(self):
        low_contention_winner = "ubq_balanced,0,127,crossbeam"
        high_contention_winner = "ubq_balanced,8,127,crossbeam"
        entries_by_scenario = {
            "4p1c": {
                low_contention_winner: stats(1_000),
                high_contention_winner: stats(1),
            },
            "64p1c": {
                low_contention_winner: stats(99),
                high_contention_winner: stats(100),
            },
        }

        labels = aggregate_family_variation_labels(entries_by_scenario, "throughput")

        self.assertEqual([low_contention_winner], labels)

    def test_batch_comparison_expands_winner_across_batch_sizes(self):
        scalar_slow = "ubq_balanced,0,127,crossbeam"
        scalar_winner = "ubq_balanced,8,127,crossbeam"
        matching_scalar = "ubq_balanced,4,127,yield"
        config_batch_8 = format_batched_ubq_label(
            "balanced,4,127,yield",
            8,
        )
        config_batch_64 = format_batched_ubq_label(
            "balanced,4,127,yield",
            64,
        )
        other_config = format_batched_ubq_label(
            "balanced,64,127,crossbeam",
            8,
        )
        entries_by_scenario = {
            "4p1c": {
                scalar_slow: stats(1_000),
                scalar_winner: stats(100),
                matching_scalar: stats(50),
                config_batch_8: stats(100),
                config_batch_64: stats(200),
                other_config: stats(300),
            },
            "64p1c": {
                scalar_slow: stats(1),
                scalar_winner: stats(100),
                matching_scalar: stats(50),
                config_batch_8: stats(100),
                config_batch_64: stats(200),
                other_config: stats(1),
            },
        }

        labels = batch_comparison_line_labels(entries_by_scenario)

        self.assertEqual(
            [scalar_winner, matching_scalar, config_batch_8, config_batch_64],
            labels,
        )
        self.assertNotIn(other_config, labels)

    def test_batch_comparison_styles_group_batches_around_winner(self):
        class ColorPlot:
            def get_cmap(self, name):
                return lambda position: (name, round(position, 2))

        scalar_winner = "ubq_balanced,8,127,crossbeam"
        matching_scalar = "ubq_balanced,4,127,yield"
        below = format_batched_ubq_label("balanced,4,127,yield", 8)
        best = format_batched_ubq_label("balanced,4,127,yield", 64)
        above = format_batched_ubq_label("balanced,4,127,yield", 128)
        entries_by_scenario = {
            scenario: {
                scalar_winner: stats(100),
                matching_scalar: stats(50),
                below: stats(100),
                best: stats(200),
                above: stats(150),
            }
            for scenario in ("4p1c", "64p1c")
        }
        labels = batch_comparison_line_labels(entries_by_scenario)

        styles = batch_comparison_series_styles(
            ColorPlot(),
            labels,
            entries_by_scenario,
        )

        self.assertEqual("#111111", styles[scalar_winner]["color"])
        self.assertEqual("--", styles[matching_scalar]["linestyle"])
        self.assertEqual(("coolwarm", 0.05), styles[below]["color"])
        self.assertEqual("#009E73", styles[best]["color"])
        self.assertEqual("*", styles[best]["marker"])
        self.assertEqual(("coolwarm", 0.6), styles[above]["color"])
        self.assertIn("(below best)", styles[below]["label"])
        self.assertIn("(above best)", styles[above]["label"])


class FamilyVariationSelectionTest(unittest.TestCase):
    def test_scalar_and_batched_bar_entries_are_separated(self):
        segqueue_batched = format_batched_segqueue_label(32)
        ubq_batched = format_batched_ubq_label(
            "balanced,8,127,crossbeam",
            32,
        )
        groups = split_bar_entries_by_method(
            {
                "segqueue": stats(80),
                segqueue_batched: stats(120),
                "ubq_balanced,8,127,crossbeam": stats(150),
                ubq_batched: stats(180),
            }
        )

        self.assertEqual(
            {"segqueue", "ubq_balanced,8,127,crossbeam"},
            set(groups["scalar"]),
        )
        self.assertEqual(
            {segqueue_batched, ubq_batched},
            set(groups["batched"]),
        )

    def test_dubq_scalar_and_batched_are_independent_families(self):
        scalar = "dubq_8,127,crossbeam"
        batched = format_batched_dubq_label("8,127,crossbeam", 32)
        labels = best_family_variation_labels(
            {
                "ubq_balanced,8,127,crossbeam": stats(100),
                scalar: stats(120),
                "dubq_1,31,yield": stats(90),
                batched: stats(150),
            },
            "throughput",
            "64p1c",
        )
        self.assertEqual(
            ["ubq_balanced,8,127,crossbeam", scalar, batched], labels
        )
        self.assertEqual((8, 127, "crossbeam"), parse_dubq_variant(scalar))
        self.assertEqual(
            (32, (8, 127, "crossbeam")), parse_batched_dubq_label(batched)
        )

    def test_dubq_batch_comparison_uses_the_best_dynamic_configuration(self):
        scalar = "dubq_2,31,crossbeam"
        batch8 = format_batched_dubq_label("2,31,crossbeam", 8)
        batch32 = format_batched_dubq_label("2,31,crossbeam", 32)
        labels = batch_comparison_line_labels(
            {
                "2p1c": {
                    scalar: stats(100),
                    batch8: stats(150),
                    batch32: stats(140),
                },
                "4p1c": {
                    scalar: stats(110),
                    batch8: stats(160),
                    batch32: stats(170),
                },
            },
            queue_family="DUBQ",
        )
        self.assertIn(scalar, labels)
        self.assertIn(batch8, labels)
        self.assertIn(batch32, labels)

    def test_bar_plot_picks_best_variation_of_every_family(self):
        scalar = "ubq_balanced,8,127,crossbeam"
        batched = format_batched_ubq_label("balanced,8,127,crossbeam", 64)
        labels = best_family_variation_labels(
            {
                "segqueue": stats(80),
                "fastfifo_64": stats(100),
                "fastfifo_256": stats(120),
                "lfqueue_32": stats(70),
                "lfqueue_256": stats(90),
                scalar: stats(150),
                "ubq_balanced,0,127,crossbeam": stats(130),
                batched: stats(180),
            },
            "throughput",
            "1p1c",
        )

        self.assertEqual(
            [
                "segqueue",
                "fastfifo_256",
                "lfqueue_256",
                scalar,
                batched,
            ],
            labels,
        )

    def test_elapsed_plot_uses_lowest_cost_within_each_family(self):
        labels = best_family_variation_labels(
            {
                "fastfifo_64": stats(900),
                "fastfifo_256": stats(500),
                "ubq_balanced,0,127,crossbeam": stats(700),
                "ubq_balanced,8,127,crossbeam": stats(300),
            },
            "throughput_pop_elapsed",
            "1p1c",
        )

        self.assertEqual(
            ["fastfifo_256", "ubq_balanced,8,127,crossbeam"],
            labels,
        )


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

    def test_combined_labels_keep_one_global_winner_per_family(self):
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
            "throughput",
        )

        self.assertEqual(["segqueue", "fastfifo_64", second_winner], labels)
        self.assertNotIn(first_winner, labels)
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
            ["SegQueue", "ConcurrentQueue", "BBQ", "LSCQ", "UBQ"],
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
        ubq_label = "ubq_balanced,8,127,crossbeam"
        with tempfile.TemporaryDirectory() as tmp:
            plot_scenario_lines(
                plot,
                Path(tmp) / "throughput.png",
                "local",
                "throughput",
                ["1p1c", "2p1c"],
                [ubq_label],
                {
                    "1p1c": {ubq_label: stats(1000)},
                    "2p1c": {ubq_label: stats(2000)},
                },
                error_bars="none",
            )

        self.assertIsNone(plot.last_axes.yscale)
        self.assertEqual("major", plot.last_axes.grid_kwargs["which"])
        self.assertIn("POOL_SZ,BLK_SZ,BACKOFF", plot.last_axes.legend_kwargs["title"])
        self.assertEqual(
            "monospace",
            plot.last_axes.legend_kwargs["title_fontproperties"]["family"],
        )


class PublicationQueuePlotTest(unittest.TestCase):
    def test_publication_queue_labels_sort_after_existing_baselines(self):
        labels = [
            "wcq_65536",
            "lfqueue_32",
            "fastfifo_256",
            "concurrent-queue",
            format_batched_segqueue_label(32),
            "segqueue",
        ]

        self.assertEqual(
            [
                "segqueue",
                format_batched_segqueue_label(32),
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
        self.assertEqual(
            "crossbeam SegQueue batched",
            queue_metadata(format_batched_segqueue_label(8))["family"],
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

        self.assertEqual(
            {"scalar_segqueue", "scalar_bbq", "scalar_lscq"},
            set(by_comparison),
        )
        self.assertEqual(
            "ubq_balanced,8,127,crossbeam",
            by_comparison["scalar_segqueue"]["ubq_queue"],
        )
        self.assertEqual(5.0, by_comparison["scalar_segqueue"]["speedup"])
        self.assertEqual("fastfifo_256", by_comparison["scalar_bbq"]["baseline_queue"])
        self.assertEqual(2.0, by_comparison["scalar_bbq"]["speedup"])
        self.assertEqual("lfqueue_256", by_comparison["scalar_lscq"]["baseline_queue"])
        self.assertEqual(1.25, by_comparison["scalar_lscq"]["speedup"])

    def test_rows_report_scalar_and_batched_ubq_separately(self):
        scalar = "ubq_balanced,8,127,crossbeam"
        batched = format_batched_ubq_label("balanced,1,511,yield", 64)
        rows = throughput_speedup_rows(
            {"1p1c": {scalar: stats(100), batched: stats(180), "segqueue": stats(60)}}
        )
        by_comparison = {row["comparison"]: row for row in rows}

        self.assertEqual(100 / 60, by_comparison["scalar_segqueue"]["speedup"])
        self.assertEqual(3.0, by_comparison["batched_segqueue"]["speedup"])
        self.assertEqual(batched, by_comparison["batched_segqueue"]["ubq_queue"])


class MetricExtractionTest(unittest.TestCase):
    def test_schema_v5_loads_dubq_scalar_batch_and_coverage(self):
        payload = {
            "schema_version": 5,
            "meta": {
                "machine_label": "local",
                "scenario": "1p1c",
                "repeat_index": 1,
                "core_placement": "interleaved",
                "dubq_label": "2,31,crossbeam",
                "dubq_min_block_size": 31,
                "ubq_grid": "sparse",
                "expected_ubq_configurations": 0,
                "expected_dubq_configurations": 1,
                "ubq_batch_sizes": [8],
                "planned_repeats": 1,
                "planned_items_per_producer": [10],
            },
            "results": [
                {
                    "queue": "dubq",
                    "mode": "throughput",
                    "items_per_producer": 10,
                    "ops_per_sec": 100.0,
                },
                {
                    "queue": "dubq",
                    "mode": "throughput",
                    "batch_size": 8,
                    "items_per_producer": 10,
                    "ops_per_sec": 140.0,
                },
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dubq.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            rows = list(load_records(path))
            coverage_rows = list(load_grid_coverage(path))

        labels = {row[4] for row in rows}
        self.assertEqual(
            {
                "dubq_2,31,crossbeam",
                "dubq_batched_8_2,31,crossbeam",
            },
            labels,
        )
        self.assertEqual(2, len(coverage_rows))
        self.assertTrue(all(row[4][0] == "dubq" for row in coverage_rows))

    def test_schema_v5_loads_interleaved_core_placement(self):
        payload = {
            "schema_version": 5,
            "meta": {
                "machine_label": "local",
                "scenario": "1p1c",
                "repeat_index": 1,
                "core_placement": "interleaved",
                "item_policy": "scenario_scaled_v1",
            },
            "results": [
                {
                    "queue": "segqueue",
                    "mode": "throughput",
                    "items_per_producer": 10,
                    "ops_per_sec": 100.0,
                }
            ],
        }

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "run.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            rows = list(load_records(path))

        self.assertEqual(1, len(rows))
        self.assertEqual("interleaved", rows[0][1])

    def test_schema_v5_grid_coverage_accepts_scenario_scaled_counts(self):
        def payload(scenario, producers, items):
            return {
                "schema_version": 5,
                "meta": {
                    "machine_label": "local",
                    "scenario": scenario,
                    "repeat_index": 1,
                    "core_placement": "interleaved",
                    "item_policy": "scenario_scaled_v1",
                    "ubq_label": "balanced,8,127,crossbeam",
                    "ubq_grid": "sparse",
                    "expected_ubq_configurations": 1,
                    "ubq_batch_sizes": [8, 32, 256],
                    "planned_repeats": 1,
                    "planned_items_per_producer": [items],
                    "producers": producers,
                    "consumers": producers,
                },
                "results": [
                    {
                        "queue": "ubq",
                        "mode": "throughput",
                        "items_per_producer": items,
                        "ops_per_sec": 100.0,
                    }
                ],
            }

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = [root / "8p8c.json", root / "64p64c.json"]
            paths[0].write_text(json.dumps(payload("8p8c", 8, 1_000_000)))
            paths[1].write_text(json.dumps(payload("64p64c", 64, 15_625)))
            rows = [row for path in paths for row in load_grid_coverage(path)]

        planned_by_scenario = {row[2]: row[3]["planned_items"] for row in rows}
        self.assertEqual((1_000_000,), planned_by_scenario["8p8c"])
        self.assertEqual((15_625,), planned_by_scenario["64p64c"])

    def test_schema_v3_loads_scalar_and_batched_ubq_as_distinct_series(self):
        payload = {
            "schema_version": 3,
            "meta": {
                "machine_label": "local",
                "scenario": "1p1c",
                "repeat_index": 1,
                "ubq_label": "balanced,8,127,crossbeam",
                "ubq_grid": "sparse",
                "expected_ubq_configurations": 40,
                "ubq_batch_sizes": [2, 4],
                "planned_repeats": 1,
                "planned_items_per_producer": [10],
            },
            "results": [
                {
                    "queue": "ubq",
                    "mode": "throughput",
                    "items_per_producer": 10,
                    "ops_per_sec": 100.0,
                },
                {
                    "queue": "ubq",
                    "mode": "throughput",
                    "batch_size": 4,
                    "items_per_producer": 10,
                    "ops_per_sec": 140.0,
                },
            ],
        }

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "run.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            rows = list(load_records(path))
            coverage_rows = list(load_grid_coverage(path))

        labels = {
            label
            for _machine, _placement, _mode, _scenario, label, _value in rows
        }
        self.assertEqual({"legacy_grouped"}, {row[1] for row in rows})
        self.assertEqual(
            {
                "ubq_balanced,8,127,crossbeam",
                "ubq_batched_4_balanced,8,127,crossbeam",
            },
            labels,
        )
        self.assertEqual(2, len(coverage_rows))

    def test_grid_coverage_reports_complete_and_incomplete_samples(self):
        coverage = {
            "grid": "sparse",
            "core_placement": "interleaved",
            "expected_configurations": 2,
            "planned_repeats": 2,
            "batch_sizes": (2, 4),
            "planned_items": (10,),
            "present": {("label", None, repeat, 10) for repeat in (1, 2)},
        }
        incomplete = grid_coverage_report(coverage, "throughput")
        self.assertEqual(12, incomplete["expected"])
        self.assertEqual(2, incomplete["present"])
        self.assertFalse(incomplete["complete"])

        coverage["present"] = {(str(index), None, 1, 10) for index in range(12)}
        complete = grid_coverage_report(coverage, "throughput")
        self.assertTrue(complete["complete"])
        self.assertEqual(100.0, complete["percent"])

    def test_interleaved_coverage_replaces_legacy_grouped_coverage(self):
        target = {}
        base = {
            "grid": "sparse",
            "expected_configurations": 1,
            "planned_repeats": 1,
            "batch_sizes": (),
            "planned_items": (10,),
        }
        merge_grid_coverage(
            target,
            "local",
            "throughput",
            "1p1c",
            {**base, "core_placement": "legacy_grouped"},
            ("legacy", None, 1, 10),
        )
        merge_grid_coverage(
            target,
            "local",
            "throughput",
            "1p1c",
            {**base, "core_placement": "interleaved"},
            ("current", None, 1, 10),
        )

        coverage = target[("local", "throughput", "1p1c")]
        self.assertEqual("interleaved", coverage["core_placement"])
        self.assertEqual({("current", None, 1, 10)}, coverage["present"])

    def test_grid_report_selects_best_scalar_and_best_batch(self):
        scalar_slow = "ubq_balanced,0,31,crossbeam"
        scalar_best = "ubq_balanced,8,127,crossbeam"
        batched_slow = format_batched_ubq_label("balanced,0,31,crossbeam", 2)
        batched_best = format_batched_ubq_label("balanced,64,4095,yield", 256)
        report = grid_winner_variant_report(
            {
                "segqueue": stats(80),
                scalar_slow: stats(90),
                scalar_best: stats(100),
                batched_slow: stats(95),
                batched_best: stats(150),
            },
            "throughput",
            "1p1c",
            None,
        )

        self.assertEqual(scalar_best, report["winner"])
        self.assertEqual(batched_best, report["batched_winner"])
        self.assertEqual(
            ["segqueue", scalar_best, batched_best],
            report["selected_labels"],
        )

    def test_load_records_emits_timing_metrics_and_skips_obsolete_groups(self):
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
                    "mode": "throughput",
                    "batch_size": 8,
                    "ops_per_sec": 105.0,
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

        by_mode = {
            (mode, label): value
            for _machine, _placement, mode, _scenario, label, value in rows
        }
        self.assertEqual(
            11.0,
            by_mode[("throughput_push_elapsed", "ubq_balanced,8,127,crossbeam")],
        )
        self.assertEqual(
            17.0,
            by_mode[("throughput_pop_elapsed", "ubq_balanced,8,127,crossbeam")],
        )
        self.assertEqual(
            105.0,
            by_mode[("throughput", format_batched_segqueue_label(8))],
        )
        self.assertEqual(23.0, by_mode[("fill_drain_fill_elapsed", "segqueue")])
        self.assertEqual(29.0, by_mode[("fill_drain_drain_elapsed", "segqueue")])
        self.assertFalse(any(mode.startswith("app_log_") for mode, _label in by_mode))

    def test_obsolete_plot_mode_prefixes(self):
        for mode in (
            "app_log_fan_in",
            "app_log_mpsc_file_push_elapsed",
            "complex_throughput",
            "complex_throughput_pop_elapsed",
            "data_latency",
        ):
            self.assertTrue(is_obsolete_plot_mode(mode), mode)
        self.assertFalse(is_obsolete_plot_mode("throughput_pop_elapsed"))
        self.assertFalse(is_obsolete_plot_mode("app_pipeline_data_latency"))

    def test_clean_removes_entire_obsolete_mode_directories(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            obsolete = root / "local" / "csv" / "complex_throughput"
            obsolete.mkdir(parents=True)
            (obsolete / "queue_metadata.csv").write_text("queue,family\n")
            retained = root / "local" / "csv" / "throughput"
            retained.mkdir()
            (retained / "queue_metadata.csv").write_text("queue,family\n")

            clear_generated_outputs(root)

            self.assertFalse(obsolete.exists())
            self.assertTrue((retained / "queue_metadata.csv").exists())

    def test_interleaved_schema_v4_supersedes_legacy_grouped_data(self):
        raw_data = {
            ("local", "legacy_grouped", "throughput", "1p1c", "segqueue"): [1.0],
            ("local", "interleaved", "throughput", "1p1c", "segqueue"): [2.0],
        }
        self.assertEqual(
            {("local", "throughput", "1p1c"): "interleaved"},
            preferred_core_placements(raw_data),
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

    def test_symmetric_scaling_includes_spsc_and_mpmc(self):
        scenarios = ["1p1c", "1p4c", "4p1c", "4p4c", "8p8c", "8p16c"]

        self.assertEqual(
            ["1p1c", "4p4c", "8p8c"],
            symmetric_scenarios(scenarios),
        )

    def test_batch_comparison_groups_include_symmetric_scaling(self):
        scenarios = ["1p1c", "1p4c", "4p1c", "4p4c", "8p1c", "8p8c"]

        self.assertEqual(
            {
                "mpsc": ["4p1c", "8p1c"],
                "spmc": ["1p4c"],
                "symmetric": ["1p1c", "4p4c", "8p8c"],
            },
            dict(batch_comparison_scenario_groups(scenarios)),
        )


class SchemaV6StatisticsTest(unittest.TestCase):
    def test_summary_uses_median_and_keeps_repeat_gate(self):
        summary = summarize_ops([1.0, 2.0, 100.0])
        self.assertEqual(2.0, summary["median_ops_per_sec"])
        self.assertEqual(2.0, summary["mean_ops_per_sec"])
        self.assertEqual(
            {
                "mean_ops_per_sec",
                "median_ops_per_sec",
                "arithmetic_mean_ops_per_sec",
                "stddev_ops_per_sec",
                "sem_ops_per_sec",
                "samples",
                "authoritative",
                "provisional",
            },
            set(summary),
        )
        self.assertFalse(summary["provisional"])
        self.assertTrue(summarize_ops([1.0, 2.0])["provisional"])
        self.assertTrue(summarize_ops([1.0, 2.0, 3.0], authoritative=False)["provisional"])

    def test_schema_v6_emits_three_isolated_throughput_metrics(self):
        payload = {
            "schema_version": 6,
            "meta": {
                "machine_label": "local",
                "scenario": "1p1c",
                "repeat_index": 2,
                "timestamp_unix_ms": 10,
                "core_placement": "interleaved",
                "affinity_authoritative": True,
                "experiment_fingerprint": "abc",
            },
            "results": [{
                "queue": "segqueue",
                "mode": "throughput",
                "ops_per_sec": 10.0,
                "throughput_metrics": {
                    "enqueue_ops_per_sec": 20.0,
                    "dequeue_ops_per_sec": 30.0,
                },
            }],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "run.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            samples = list(load_record_samples(path))
        self.assertEqual(
            {
                "throughput": 10.0,
                "throughput_enqueue_ceiling": 20.0,
                "throughput_dequeue_ceiling": 30.0,
            },
            {sample["mode"]: sample["value"] for sample in samples},
        )
        self.assertTrue(all(sample["fingerprint"] == "abc" for sample in samples))

    def test_duplicate_reruns_prefer_newest_completed_sample(self):
        base = {
            "fingerprint": "fp",
            "scenario": "1p1c",
            "queue": "segqueue",
            "mode": "throughput",
            "repeat_index": 1,
        }
        deduped = deduplicate_logical_samples([
            {**base, "timestamp": 10, "value": 1.0},
            {**base, "timestamp": 20, "value": 2.0},
            {**base, "fingerprint": "other", "timestamp": 30, "value": 3.0},
        ])
        self.assertEqual(2, len(deduped))
        self.assertEqual(
            2.0,
            next(sample["value"] for sample in deduped if sample["fingerprint"] == "fp"),
        )


if __name__ == "__main__":
    unittest.main()
