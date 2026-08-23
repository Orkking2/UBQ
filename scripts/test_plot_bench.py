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
    combined_batch_comparison_color_key,
    combined_batch_comparison_families,
    combined_batch_comparison_line_labels,
    combined_batch_comparison_series_styles,
    combined_scenario_line_labels,
    display_label,
    deduplicate_logical_samples,
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
    lubq_throughput_speedup_rows,
    merge_grid_coverage,
    machine_display_label,
    machine_family_entries,
    plot_scenario_lines,
    parse_batched_plain_label,
    parse_batched_segqueue_label,
    plain_batch_comparison_line_labels,
    plain_batch_comparison_series_styles,
    plain_batch_family_queues,
    plain_queue_display_name,
    pool_size_effect_observations,
    pool_size_effect_rows,
    preferred_core_placements,
    publication_display_label,
    publication_machine_entries,
    publication_metric_axis_label,
    publication_metric_column,
    publication_metric_value,
    publication_scenario_line_labels,
    queue_label_legend_title,
    queue_method_kind,
    queue_metadata,
    scenario_family,
    scenario_line_uses_log_y,
    scenario_line_labels,
    split_bar_entries_by_method,
    summarize_ops,
    symmetric_scenarios,
    throughput_speedup_rows,
    format_batched_plain_label,
    write_immediate_variant_csv,
    write_lubq_throughput_speedup_csv,
    write_machine_line_csv,
    write_pool_size_effect_csv,
)


def stats(ops):
    return {
        "mean_ops_per_sec": float(ops),
        "stddev_ops_per_sec": 0.0,
        "sem_ops_per_sec": 0.0,
        "samples": 1,
    }


class FakeBbox:
    def __init__(self, x0=0.1, y0=0.15, width=0.7, height=0.75):
        self.x0 = x0
        self.y0 = y0
        self.width = width
        self.height = height


class FakeWindowExtent:
    width = 120.0  # pixels; paired with FakeFigure.dpi=100 -> 1.2in


class FakeLegend:
    def get_window_extent(self, _renderer=None):
        return FakeWindowExtent()


class FakeCanvas:
    def draw(self):
        pass

    def get_renderer(self):
        return None


class FakeFigure:
    def __init__(self):
        self.canvas = FakeCanvas()
        self.dpi = 100

    def tight_layout(self, **_kwargs):
        pass

    def savefig(self, *_args, **_kwargs):
        pass

    def get_size_inches(self):
        return (10.0, 6.5)

    def set_size_inches(self, *_args, **_kwargs):
        pass


class FakeSpine:
    def set_visible(self, *_args, **_kwargs):
        pass


class FakeAxes:
    def __init__(self):
        self.yscale = None
        self.grid_kwargs = None
        self.legend_kwargs = None
        self.transAxes = None
        self.spines = {
            "top": FakeSpine(),
            "right": FakeSpine(),
            "bottom": FakeSpine(),
            "left": FakeSpine(),
        }

    def plot(self, *_args, **_kwargs):
        pass

    def errorbar(self, *_args, **_kwargs):
        pass

    def imshow(self, *_args, **_kwargs):
        pass

    def tick_params(self, *_args, **_kwargs):
        pass

    def set_xticks(self, *_args, **_kwargs):
        pass

    def set_yticks(self, *_args, **_kwargs):
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
        return FakeLegend()

    def get_position(self):
        return FakeBbox()

    def set_position(self, *_args, **_kwargs):
        pass

    def text(self, *_args, **_kwargs):
        pass


class FakePyplot:
    def __init__(self):
        self.last_axes = None

    def subplots(self, *args, **_kwargs):
        if len(args) >= 2 and args[0] == 1 and args[1] == 2:
            self.last_axes = (FakeAxes(), FakeAxes())
        else:
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

    def test_legacy_large_block_labels_share_the_ordinary_ubq_family(self):
        self.assertEqual(
            "UBQ",
            queue_metadata("ubq_balanced,1,65535,crossbeam")["family"],
        )
        self.assertEqual(
            "UBQ",
            queue_metadata("ubq_balanced,1,4095,crossbeam")["family"],
        )
        self.assertEqual(
            "UBQ batched",
            queue_metadata(format_batched_ubq_label("balanced,1,65535,crossbeam", 8192))["family"],
        )
        self.assertEqual(
            "UBQ 1,65535,Cycle",
            display_label("ubq_balanced,1,65535,crossbeam"),
        )
        self.assertEqual(
            "UBQ 1,page,Cycle",
            display_label("ubq_balanced,1,page,crossbeam"),
        )

    def test_batched_segqueue_label_is_distinct_from_scalar(self):
        label = format_batched_segqueue_label(32)

        self.assertEqual("SegQueueb (32)", display_label(label))
        self.assertEqual(32, parse_batched_segqueue_label(label))
        self.assertIsNone(parse_batched_segqueue_label("segqueue"))

    def test_queue_label_legend_explains_and_aligns_fields(self):
        title = queue_label_legend_title(
            [
                "ubq_balanced,8,127,crossbeam",
                format_batched_ubq_label("balanced,2,31,yield", 8),
            ]
        )

        self.assertIsNotNone(title)
        _heading, batched_schema, scalar_schema = title.splitlines()
        self.assertEqual(batched_schema.index("POOL_SZ"), scalar_schema.index("POOL_SZ"))
        self.assertEqual(
            "UBQb (BATCH_SZ)  POOL_SZ,BLK_SZ,BACKOFF",
            batched_schema,
        )
        self.assertEqual(
            "UBQ              POOL_SZ,BLK_SZ,BACKOFF",
            scalar_schema,
        )


class ImmediateWinnerVariantReportTest(unittest.TestCase):
    def test_current_fixed_pool_grid_varies_only_block_and_backoff(self):
        entries = {
            "ubq_balanced,1,127,crossbeam": stats(100),
            "ubq_balanced,1,63,crossbeam": stats(90),
            "ubq_balanced,1,255,crossbeam": stats(90),
            "ubq_balanced,1,127,yield": stats(90),
        }

        report = immediate_winner_variant_report(entries, "1p1c")

        self.assertEqual([], report["missing_required_labels"])
        self.assertEqual([], report["zero_pool_labels"])

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
        self.assertEqual(("coolwarm", 0.25), styles[below]["color"])
        self.assertEqual(("coolwarm", 1.0), styles[best]["color"])
        self.assertEqual("*", styles[best]["marker"])
        self.assertEqual(("coolwarm", 0.6), styles[above]["color"])
        self.assertEqual("batch=8", styles[below]["label"])
        self.assertEqual("batch=128", styles[above]["label"])
        self.assertEqual("batch=64 (best)", styles[best]["label"])

    def test_batch_comparison_styles_prefix_labels_with_family_name(self):
        class ColorPlot:
            def get_cmap(self, name):
                return lambda position: (name, round(position, 2))

        scalar_winner = "ubq_balanced,8,127,crossbeam"
        best = format_batched_ubq_label("balanced,8,127,crossbeam", 64)
        entries_by_scenario = {
            scenario: {scalar_winner: stats(100), best: stats(200)}
            for scenario in ("4p1c", "64p1c")
        }
        labels = batch_comparison_line_labels(entries_by_scenario)

        styles = batch_comparison_series_styles(
            ColorPlot(),
            labels,
            entries_by_scenario,
            cmap=ColorPlot().get_cmap("Blues"),
            family_label="UBQ",
        )

        self.assertTrue(styles[scalar_winner]["label"].startswith("UBQ "))
        self.assertTrue(styles[best]["label"].startswith("UBQ "))
        self.assertEqual(("Blues", 1.0), styles[best]["color"])


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
        segqueue_batched = format_batched_segqueue_label(16)
        rows = throughput_speedup_rows(
            {
                "1p1c": {
                    scalar: stats(100),
                    batched: stats(180),
                    "segqueue": stats(60),
                    segqueue_batched: stats(90),
                }
            }
        )
        by_comparison = {row["comparison"]: row for row in rows}

        self.assertEqual(100 / 60, by_comparison["scalar_segqueue"]["speedup"])
        self.assertEqual(180 / 90, by_comparison["batched_segqueue"]["speedup"])
        self.assertEqual(batched, by_comparison["batched_segqueue"]["ubq_queue"])
        self.assertEqual(
            segqueue_batched, by_comparison["batched_segqueue"]["baseline_queue"]
        )
        self.assertEqual(
            "best batched UBQ vs best batched SegQueue",
            by_comparison["batched_segqueue"]["comparison_label"],
        )

    def test_batched_ubq_only_compares_against_baselines_with_batched_data(self):
        """A baseline with no batched measurement (segqueue here) or no
        batched form at all (BBQ) must not silently pull in its scalar value
        when compared against batched UBQ — that would conflate UBQ's own
        batching gain with its baseline architectural gain."""
        scalar = "ubq_balanced,8,127,crossbeam"
        batched = format_batched_ubq_label("balanced,1,511,yield", 64)
        rows = throughput_speedup_rows(
            {
                "1p1c": {
                    scalar: stats(100),
                    batched: stats(180),
                    "segqueue": stats(60),
                    "fastfifo_256": stats(30),
                }
            }
        )
        by_comparison = {row["comparison"]: row for row in rows}

        self.assertIn("scalar_segqueue", by_comparison)
        self.assertIn("scalar_bbq", by_comparison)
        self.assertNotIn("batched_segqueue", by_comparison)
        self.assertNotIn("batched_bbq", by_comparison)

    def test_batched_ubq_compares_against_batched_plain_baselines(self):
        scalar = "ubq_balanced,8,127,crossbeam"
        batched = format_batched_ubq_label("balanced,1,511,yield", 64)
        mutex_batched = format_batched_plain_label("mutex-vecdeque", 32)
        moodycamel_batched = format_batched_plain_label("moodycamel-cq", 8)
        rows = throughput_speedup_rows(
            {
                "1p1c": {
                    scalar: stats(100),
                    batched: stats(200),
                    "mutex-vecdeque": stats(4),
                    mutex_batched: stats(20),
                    "moodycamel-cq": stats(25),
                    moodycamel_batched: stats(40),
                }
            }
        )
        by_comparison = {row["comparison"]: row for row in rows}

        self.assertEqual(200 / 20, by_comparison["batched_mutex-vecdeque"]["speedup"])
        self.assertEqual(
            mutex_batched, by_comparison["batched_mutex-vecdeque"]["baseline_queue"]
        )
        self.assertEqual(
            "best batched UBQ vs best batched Mutex+VecDeque",
            by_comparison["batched_mutex-vecdeque"]["comparison_label"],
        )
        self.assertEqual(200 / 40, by_comparison["batched_moodycamel-cq"]["speedup"])
        self.assertEqual(
            moodycamel_batched,
            by_comparison["batched_moodycamel-cq"]["baseline_queue"],
        )

    def test_rows_compare_ubq_to_newly_added_scalar_baselines(self):
        entries_by_scenario = {
            "1p1c": {
                "ubq_balanced,8,127,crossbeam": stats(100.0),
                "mutex-vecdeque": stats(4.0),
                "ms-queue": stats(20.0),
                "naive-faa-queue": stats(2.0),
                "moodycamel-cq": stats(25.0),
            }
        }

        rows = throughput_speedup_rows(entries_by_scenario)
        by_comparison = {row["comparison"]: row for row in rows}

        self.assertEqual(
            {
                "scalar_mutex-vecdeque",
                "scalar_ms-queue",
                "scalar_naive-faa-queue",
                "scalar_moodycamel-cq",
            },
            set(by_comparison),
        )
        self.assertEqual(25.0, by_comparison["scalar_mutex-vecdeque"]["speedup"])
        self.assertEqual(5.0, by_comparison["scalar_ms-queue"]["speedup"])
        self.assertEqual(50.0, by_comparison["scalar_naive-faa-queue"]["speedup"])
        self.assertEqual(4.0, by_comparison["scalar_moodycamel-cq"]["speedup"])

    def test_lubq_gets_its_own_scalar_and_native_batch_baseline_suite(self):
        lubq_batched = format_batched_plain_label("lubq", 64)
        segqueue_batched = format_batched_segqueue_label(16)
        mutex_batched = format_batched_plain_label("mutex-vecdeque", 32)
        moodycamel_batched = format_batched_plain_label("moodycamel-cq", 8)
        rows = lubq_throughput_speedup_rows(
            {
                "1p1c": {
                    "lubq": stats(120),
                    lubq_batched: stats(240),
                    "segqueue": stats(60),
                    segqueue_batched: stats(80),
                    "concurrent-queue": stats(40),
                    "fastfifo_256": stats(30),
                    "lfqueue_32": stats(24),
                    "wcq_65536": stats(20),
                    "mutex-vecdeque": stats(12),
                    mutex_batched: stats(30),
                    "ms-queue": stats(15),
                    "naive-faa-queue": stats(10),
                    "moodycamel-cq": stats(48),
                    moodycamel_batched: stats(60),
                    # UBQ is the subject of its own comparison suite, not an
                    # external baseline in LUBQ's dedicated view.
                    "ubq_balanced,1,511,crossbeam": stats(500),
                }
            }
        )
        by_comparison = {row["comparison"]: row for row in rows}

        self.assertEqual(
            {
                "scalar_segqueue",
                "scalar_concurrent-queue",
                "scalar_bbq",
                "scalar_lscq",
                "scalar_wcq",
                "scalar_mutex-vecdeque",
                "scalar_ms-queue",
                "scalar_naive-faa-queue",
                "scalar_moodycamel-cq",
                "batched_segqueue",
                "batched_mutex-vecdeque",
                "batched_moodycamel-cq",
            },
            set(by_comparison),
        )
        self.assertEqual(3.0, by_comparison["scalar_concurrent-queue"]["speedup"])
        self.assertEqual(240 / 80, by_comparison["batched_segqueue"]["speedup"])
        self.assertEqual(
            lubq_batched,
            by_comparison["batched_moodycamel-cq"]["lubq_queue"],
        )
        self.assertEqual(
            "best batched LUBQ vs best batched moodycamel::CQ",
            by_comparison["batched_moodycamel-cq"]["comparison_label"],
        )

    def test_lubq_speedup_csv_has_lubq_specific_columns(self):
        rows = lubq_throughput_speedup_rows(
            {"1p1c": {"lubq": stats(90), "segqueue": stats(30)}}
        )
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "lubq.csv"
            write_lubq_throughput_speedup_csv(out_path, rows)
            with out_path.open(encoding="utf-8", newline="") as f:
                written = list(csv.DictReader(f))

        self.assertEqual("scalar", written[0]["lubq_kind"])
        self.assertEqual("lubq", written[0]["lubq_queue"])
        self.assertNotIn("ubq_kind", written[0])


class PlainBatchNativeQueueTest(unittest.TestCase):
    """Covers the generic 'plain batch-native queue' path (no internal
    variant params, unlike UBQ) that LUBQ, segqueue, mutex-vecdeque, and
    moodycamel-cq all share — see format_batched_plain_label."""

    def test_lubq_scalar_and_batch_metadata(self):
        batch_label = format_batched_plain_label("lubq", 256)
        self.assertEqual("LUBQ", display_label("lubq"))
        self.assertEqual("LUBQb (256)", display_label(batch_label))
        self.assertEqual("LUBQ", queue_metadata("lubq")["family"])
        self.assertEqual("LUBQ batched", queue_metadata(batch_label)["family"])

    def test_plain_label_round_trips_and_rejects_ubq_shaped_labels(self):
        label = format_batched_plain_label("mutex-vecdeque", 32)
        self.assertEqual("mutex-vecdeque_batched_32", label)
        self.assertEqual(("mutex-vecdeque", 32), parse_batched_plain_label(label))

        self.assertIsNone(parse_batched_plain_label("mutex-vecdeque"))
        self.assertIsNone(parse_batched_plain_label("not_batched_at_all"))
        # A UBQ-shaped label has non-digit content after the batch
        # number (the variant params) — must not be mistaken for plain.
        ubq_batched = format_batched_ubq_label("balanced,1,255,crossbeam", 8)
        self.assertIsNone(parse_batched_plain_label(ubq_batched))

    def test_segqueue_specific_helpers_delegate_to_the_generic_ones(self):
        label = format_batched_segqueue_label(16)
        self.assertEqual("segqueue_batched_16", label)
        self.assertEqual(16, parse_batched_segqueue_label(label))
        self.assertEqual(("segqueue", 16), parse_batched_plain_label(label))
        # A different queue's batched label must not look like segqueue's.
        self.assertIsNone(
            parse_batched_segqueue_label(format_batched_plain_label("moodycamel-cq", 16))
        )

    def test_method_kind_and_metadata_recognize_any_plain_batched_queue(self):
        label = format_batched_plain_label("moodycamel-cq", 256)
        self.assertEqual("batched", queue_method_kind(label))
        self.assertEqual("scalar", queue_method_kind("moodycamel-cq"))

        meta = queue_metadata(label)
        self.assertEqual("moodycamel-cq batched", meta["family"])
        self.assertEqual("(256)", meta["variant"])
        self.assertIn("moodycamel", meta["publication"].lower())

        scalar_meta = queue_metadata("moodycamel-cq")
        self.assertEqual("moodycamel-cq", scalar_meta["family"])

    def test_display_label_uses_short_names_for_scalar_and_batched(self):
        self.assertEqual("moodycamel::CQ", display_label("moodycamel-cq"))
        self.assertEqual(
            "moodycamel::CQb (256)", display_label(format_batched_plain_label("moodycamel-cq", 256))
        )
        self.assertEqual("Mutex+VecDeque", display_label("mutex-vecdeque"))
        self.assertEqual("MS-Queue", plain_queue_display_name("ms-queue"))

    def test_family_queues_discovers_every_plain_batch_native_queue_present(self):
        entries_by_scenario = {
            "1p1c": {
                "segqueue": stats(10),
                format_batched_plain_label("segqueue", 8): stats(12),
                format_batched_plain_label("mutex-vecdeque", 32): stats(9),
                "ubq_balanced,0,255,crossbeam": stats(20),
            },
            "4p4c": {
                format_batched_plain_label("moodycamel-cq", 8): stats(30),
            },
        }
        self.assertEqual(
            ["moodycamel-cq", "mutex-vecdeque", "segqueue"],
            plain_batch_family_queues(entries_by_scenario),
        )

    def test_line_labels_include_scalar_plus_every_batch_size_sorted(self):
        entries_by_scenario = {
            "2p2c": {
                "mutex-vecdeque": stats(100),
                format_batched_plain_label("mutex-vecdeque", 256): stats(300),
                format_batched_plain_label("mutex-vecdeque", 8): stats(150),
                "moodycamel-cq": stats(50),  # a different queue, must be excluded
            },
            "4p4c": {
                format_batched_plain_label("mutex-vecdeque", 32): stats(220),
            },
        }

        labels = plain_batch_comparison_line_labels(
            entries_by_scenario, "throughput", "mutex-vecdeque"
        )

        self.assertEqual(
            [
                "mutex-vecdeque",
                format_batched_plain_label("mutex-vecdeque", 8),
                format_batched_plain_label("mutex-vecdeque", 32),
                format_batched_plain_label("mutex-vecdeque", 256),
            ],
            labels,
        )

    def test_line_labels_omit_scalar_when_the_queue_has_no_scalar_data(self):
        entries_by_scenario = {
            "2p2c": {format_batched_plain_label("mutex-vecdeque", 8): stats(150)},
        }

        labels = plain_batch_comparison_line_labels(
            entries_by_scenario, "throughput", "mutex-vecdeque"
        )

        self.assertEqual([format_batched_plain_label("mutex-vecdeque", 8)], labels)

    def test_series_styles_give_scalar_a_fixed_color_and_batches_a_gradient(self):
        labels = [
            "mutex-vecdeque",
            format_batched_plain_label("mutex-vecdeque", 8),
            format_batched_plain_label("mutex-vecdeque", 256),
        ]

        styles = plain_batch_comparison_series_styles(FakePyplot(), labels, "mutex-vecdeque")

        self.assertEqual(set(labels), set(styles))
        self.assertEqual("#111111", styles["mutex-vecdeque"]["color"])
        for label in labels[1:]:
            self.assertIn("batch=", styles[label]["label"])

    def test_plain_series_styles_take_a_family_hue_and_label_prefix(self):
        class ColorPlot:
            def get_cmap(self, name):
                return lambda position: (name, round(position, 2))

        labels = ["mutex-vecdeque", format_batched_plain_label("mutex-vecdeque", 8)]
        styles = plain_batch_comparison_series_styles(
            ColorPlot(),
            labels,
            "mutex-vecdeque",
            cmap=ColorPlot().get_cmap("Greens"),
            family_label="Mutex+VecDeque",
        )

        self.assertTrue(styles["mutex-vecdeque"]["label"].startswith("Mutex+VecDeque "))
        batched_label = format_batched_plain_label("mutex-vecdeque", 8)
        self.assertEqual("Greens", styles[batched_label]["color"][0])


class CombinedBatchComparisonTest(unittest.TestCase):
    def test_combined_families_include_ubq_and_every_batched_plain_queue(self):
        ubq_scalar = "ubq_balanced,8,127,crossbeam"
        ubq_batched = format_batched_ubq_label("balanced,8,127,crossbeam", 32)
        mutex_batched = format_batched_plain_label("mutex-vecdeque", 8)
        entries_by_scenario = {
            "1p1c": {
                ubq_scalar: stats(100),
                ubq_batched: stats(200),
                "mutex-vecdeque": stats(10),
                mutex_batched: stats(20),
                # segqueue never measured batched: must be excluded entirely.
                "segqueue": stats(5),
            }
        }

        families = combined_batch_comparison_families(entries_by_scenario)
        family_keys = [key for key, _labels in families]

        self.assertEqual(["UBQ", "mutex-vecdeque"], family_keys)
        self.assertNotIn("segqueue", family_keys)

        labels = combined_batch_comparison_line_labels(entries_by_scenario)
        self.assertIn(ubq_batched, labels)
        self.assertIn(mutex_batched, labels)

    def test_combined_styles_give_each_family_a_distinct_colormap(self):
        class ColorPlot:
            def get_cmap(self, name):
                return lambda position: (name, round(position, 2))

        ubq_scalar = "ubq_balanced,8,127,crossbeam"
        ubq_batched = format_batched_ubq_label("balanced,8,127,crossbeam", 32)
        mutex_batched = format_batched_plain_label("mutex-vecdeque", 8)
        entries_by_scenario = {
            "1p1c": {
                ubq_scalar: stats(100),
                ubq_batched: stats(200),
                "mutex-vecdeque": stats(10),
                mutex_batched: stats(20),
            }
        }

        plt = ColorPlot()
        families = combined_batch_comparison_families(entries_by_scenario)
        styles = combined_batch_comparison_series_styles(plt, families, entries_by_scenario)

        self.assertEqual("Blues", styles[ubq_batched]["color"][0])
        self.assertEqual("Oranges", styles[mutex_batched]["color"][0])
        self.assertTrue(styles[ubq_scalar]["label"].startswith("UBQ "))
        self.assertTrue(styles["mutex-vecdeque"]["label"].startswith("Mutex+VecDeque "))

    def test_color_key_lays_out_families_as_rows_and_batch_sizes_as_columns(self):
        ubq_scalar = "ubq_balanced,8,127,crossbeam"
        ubq_batch8 = format_batched_ubq_label("balanced,8,127,crossbeam", 8)
        ubq_batch32 = format_batched_ubq_label("balanced,8,127,crossbeam", 32)
        mutex_batch8 = format_batched_plain_label("mutex-vecdeque", 8)
        entries_by_scenario = {
            "1p1c": {
                ubq_scalar: stats(100),
                ubq_batch8: stats(150),
                ubq_batch32: stats(200),
                "mutex-vecdeque": stats(10),
                mutex_batch8: stats(20),
            }
        }

        plt = FakePyplot()
        families = combined_batch_comparison_families(entries_by_scenario)
        styles = combined_batch_comparison_series_styles(plt, families, entries_by_scenario)
        row_labels, col_keys, cell_colors, best_cells = combined_batch_comparison_color_key(
            families, styles
        )

        self.assertEqual(["UBQ", "Mutex+VecDeque"], row_labels)
        self.assertEqual(["scalar", 8, 32], col_keys)
        # The best (highest-throughput) batch for UBQ is 32; mutex-vecdeque
        # only has one batch size measured, so there's nothing to compare it
        # against and it gets no star.
        self.assertEqual({(0, 32)}, best_cells)
        # No batch=32 measurement exists for mutex-vecdeque: that cell must
        # be absent so the renderer falls back to its "n/a" blank fill,
        # rather than silently reusing another cell's color.
        self.assertNotIn((1, 32), cell_colors)

    def test_combined_families_do_not_split_legacy_large_block_labels(self):
        ubq_scalar = "ubq_balanced,8,127,crossbeam"
        ubq_batched = format_batched_ubq_label("balanced,8,127,crossbeam", 32)
        legacy_scalar = "ubq_balanced,1,65535,crossbeam"
        legacy_batched = format_batched_ubq_label("balanced,1,65535,crossbeam", 8192)
        entries_by_scenario = {
            "1p1c": {
                ubq_scalar: stats(100),
                ubq_batched: stats(200),
                legacy_scalar: stats(90),
                legacy_batched: stats(150),
            }
        }

        families = combined_batch_comparison_families(entries_by_scenario)
        family_keys = [key for key, _labels in families]

        self.assertEqual(["UBQ"], family_keys)
        self.assertNotIn("UBQ (huge-heap)", dict(families))

    def test_legacy_large_block_labels_use_normal_ubq_batch_cells(self):
        legacy_scalar = "ubq_balanced,1,65535,crossbeam"
        legacy_batch8192 = format_batched_ubq_label("balanced,1,65535,crossbeam", 8192)
        legacy_batch65536 = format_batched_ubq_label("balanced,1,65535,crossbeam", 65536)
        entries_by_scenario = {
            "1p1c": {
                legacy_scalar: stats(90),
                legacy_batch8192: stats(150),
                legacy_batch65536: stats(200),
            }
        }

        plt = FakePyplot()
        families = combined_batch_comparison_families(entries_by_scenario)
        styles = combined_batch_comparison_series_styles(plt, families, entries_by_scenario)
        row_labels, col_keys, cell_colors, best_cells = combined_batch_comparison_color_key(
            families, styles
        )

        self.assertEqual(["UBQ"], row_labels)
        self.assertEqual(["scalar", 8192, 65536], col_keys)
        # Before the fix, both batch sizes collapsed into (0, "scalar") and
        # neither (0, 8192) nor (0, 65536) was ever set.
        self.assertIn((0, 8192), cell_colors)
        self.assertIn((0, 65536), cell_colors)
        self.assertEqual({(0, 65536)}, best_cells)

    def test_color_key_scalar_column_is_uniform_across_families(self):
        """All scalar entries share one fixed color regardless of family
        (see batch_comparison_series_styles/plain_batch_comparison_series_
        styles) — the color key's "scalar" column should show that
        directly: every row's scalar cell is the same color."""
        ubq_scalar = "ubq_balanced,8,127,crossbeam"
        ubq_batched = format_batched_ubq_label("balanced,8,127,crossbeam", 8)
        mutex_batched = format_batched_plain_label("mutex-vecdeque", 8)
        entries_by_scenario = {
            "1p1c": {
                ubq_scalar: stats(100),
                ubq_batched: stats(150),
                "mutex-vecdeque": stats(10),
                mutex_batched: stats(20),
            }
        }

        plt = FakePyplot()
        families = combined_batch_comparison_families(entries_by_scenario)
        styles = combined_batch_comparison_series_styles(plt, families, entries_by_scenario)
        row_labels, _col_keys, cell_colors, _best_cells = combined_batch_comparison_color_key(
            families, styles
        )

        scalar_colors = {cell_colors[(idx, "scalar")] for idx in range(len(row_labels))}
        self.assertEqual({"#111111"}, scalar_colors)


class ColorKeyRenderingSmokeTest(unittest.TestCase):
    def test_plot_scenario_lines_renders_a_color_key_without_crashing(self):
        ubq_scalar = "ubq_balanced,8,127,crossbeam"
        ubq_batched = format_batched_ubq_label("balanced,8,127,crossbeam", 8)
        mutex_batched = format_batched_plain_label("mutex-vecdeque", 8)
        entries_by_scenario = {
            "1p1c": {
                ubq_scalar: stats(100),
                ubq_batched: stats(150),
                "mutex-vecdeque": stats(10),
                mutex_batched: stats(20),
            },
            "4p4c": {
                ubq_scalar: stats(90),
                ubq_batched: stats(160),
                "mutex-vecdeque": stats(9),
                mutex_batched: stats(22),
            },
        }
        plt = FakePyplot()
        families = combined_batch_comparison_families(entries_by_scenario)
        labels = [label for _family, family_labels in families for label in family_labels]
        styles = combined_batch_comparison_series_styles(plt, families, entries_by_scenario)

        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "batchcomp.png"
            plot_scenario_lines(
                plt,
                out_path,
                "local",
                "throughput",
                ["1p1c", "4p4c"],
                labels,
                entries_by_scenario,
                "sem",
                series_styles=styles,
                color_key_families=families,
            )

        # FakeFigure.savefig is a no-op, so this just proves the whole
        # render path (building the grid, drawing both axes) runs clean.
        self.assertEqual(2, len(plt.last_axes))


class PoolSizeEffectTest(unittest.TestCase):
    def test_pool_effects_are_matched_on_every_other_ubq_knob(self):
        batched_pool0_8 = format_batched_ubq_label("balanced,0,127,crossbeam", 8)
        batched_pool1_8 = format_batched_ubq_label("balanced,1,127,crossbeam", 8)
        batched_pool0_32 = format_batched_ubq_label("balanced,0,127,crossbeam", 32)
        batched_pool1_32 = format_batched_ubq_label("balanced,1,127,crossbeam", 32)
        entries = {
            "1p1c": {
                "ubq_balanced,0,127,crossbeam": stats(100),
                "ubq_balanced,1,127,crossbeam": stats(110),
                "ubq_balanced,8,127,crossbeam": stats(80),
                "ubq_balanced,0,511,yield": stats(200),
                "ubq_balanced,1,511,yield": stats(180),
                # No matching pool=0: this observation must not be compared.
                "ubq_balanced,8,511,crossbeam": stats(500),
                batched_pool0_8: stats(50),
                batched_pool1_8: stats(100),
                batched_pool0_32: stats(100),
                batched_pool1_32: stats(50),
            }
        }

        observations = pool_size_effect_observations(entries)
        rows = pool_size_effect_rows(entries)
        by_key = {(row["method"], row["pool_size"]): row for row in rows}

        self.assertEqual(9, len(observations))
        self.assertEqual(2, by_key[("scalar", 1)]["matched_configurations"])
        self.assertAlmostEqual(1.0, by_key[("scalar", 1)]["median_relative_performance_vs_pool0"])
        self.assertEqual(0.5, by_key[("scalar", 1)]["beneficial_fraction"])
        self.assertEqual(1, by_key[("scalar", 8)]["matched_configurations"])
        self.assertAlmostEqual(0.8, by_key[("scalar", 8)]["median_relative_performance_vs_pool0"])
        self.assertEqual("8,32", by_key[("batched", 1)]["batch_sizes"])
        self.assertAlmostEqual(1.25, by_key[("batched", 1)]["median_relative_performance_vs_pool0"])

    def test_lower_is_better_metrics_are_oriented_as_performance(self):
        entries = {
            "1p1c": {
                "ubq_balanced,0,127,crossbeam": stats(100),
                "ubq_balanced,1,127,crossbeam": stats(50),
            }
        }

        rows = pool_size_effect_rows(entries, "throughput_push_elapsed")
        pooled = next(row for row in rows if row["pool_size"] == 1)

        self.assertEqual(2.0, pooled["median_relative_performance_vs_pool0"])

    def test_summary_csv_records_match_counts_and_claim_status(self):
        rows = pool_size_effect_rows(
            {
                "1p1c": {
                    "ubq_balanced,0,127,crossbeam": stats(100),
                    "ubq_balanced,1,127,crossbeam": stats(120),
                }
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "pool.csv"
            write_pool_size_effect_csv(path, rows)
            with path.open(encoding="utf-8") as f:
                written = list(csv.DictReader(f))

        pooled = next(row for row in written if row["pool_size"] == "1")
        self.assertEqual("1", pooled["matched_configurations"])
        self.assertEqual("1.200000", pooled["median_relative_performance_vs_pool0"])
        self.assertEqual("eligible", pooled["claim_status"])


class MetricExtractionTest(unittest.TestCase):
    def _record(self, **overrides):
        record = {
            "repeat_index": 1,
            "status": "completed",
            "items_per_producer": 10,
            "ops_per_sec": 100.0,
            "protocol": {
                "core_placement": "interleaved",
                "affinity_authoritative": True,
            },
        }
        record.update(overrides)
        return record

    def test_ubq_scalar_and_batched_load_as_distinct_series(self):
        payload = {
            "schema_version": 7,
            "meta": {
                "machine_label": "local",
                "scenario": "1p1c",
                "ubq_grid": "page",
                "expected_ubq_configurations": 2,
                "ubq_batch_sizes": [2, 4],
                "planned_repeats": 1,
                "planned_items_per_producer": [10],
            },
            "results": [
                self._record(queue="ubq", mode="throughput", ubq_label="balanced,8,127,crossbeam"),
                self._record(
                    queue="ubq",
                    mode="throughput",
                    ubq_label="balanced,8,127,crossbeam",
                    batch_size=4,
                    ops_per_sec=140.0,
                ),
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
        self.assertEqual({"interleaved"}, {row[1] for row in rows})
        self.assertEqual(
            {
                "ubq_balanced,8,127,crossbeam",
                "ubq_batched_4_balanced,8,127,crossbeam",
            },
            labels,
        )
        self.assertEqual(2, len(coverage_rows))

    def test_non_interleaved_protocol_is_excluded(self):
        payload = {
            "schema_version": 7,
            "meta": {"machine_label": "local", "scenario": "1p1c"},
            "results": [
                self._record(
                    queue="segqueue",
                    mode="throughput",
                    protocol={"core_placement": "unpinned", "affinity_authoritative": False},
                )
            ],
        }

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "run.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            rows = list(load_records(path))

        self.assertEqual(0, len(rows))

    def test_grid_coverage_accepts_scenario_scaled_counts(self):
        def payload(scenario, items):
            return {
                "schema_version": 7,
                "meta": {
                    "machine_label": "local",
                    "scenario": scenario,
                    "ubq_grid": "sparse",
                    "expected_ubq_configurations": 1,
                    "ubq_batch_sizes": [8, 32, 256],
                    "planned_repeats": 1,
                    "planned_items_per_producer": [items],
                },
                "results": [
                    self._record(
                        queue="ubq",
                        mode="throughput",
                        ubq_label="balanced,8,127,crossbeam",
                        items_per_producer=items,
                    )
                ],
            }

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = [root / "8p8c.json", root / "64p64c.json"]
            paths[0].write_text(json.dumps(payload("8p8c", 1_000_000)))
            paths[1].write_text(json.dumps(payload("64p64c", 15_625)))
            rows = [row for path in paths for row in load_grid_coverage(path)]

        planned_by_scenario = {row[2]: row[3]["planned_items"] for row in rows}
        self.assertEqual((1_000_000,), planned_by_scenario["8p8c"])
        self.assertEqual((15_625,), planned_by_scenario["64p64c"])

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

    def test_timed_out_and_failed_records_are_tracked_but_not_counted_present(self):
        payload = {
            "schema_version": 7,
            "meta": {
                "machine_label": "local",
                "scenario": "1p1c",
                "ubq_grid": "sparse",
                "expected_ubq_configurations": 1,
                "ubq_batch_sizes": [],
                "planned_repeats": 3,
                "planned_items_per_producer": [10],
            },
            "results": [
                self._record(
                    queue="ubq",
                    mode="throughput",
                    ubq_label="balanced,8,127,crossbeam",
                    repeat_index=1,
                    status="completed",
                ),
                self._record(
                    queue="ubq",
                    mode="throughput",
                    ubq_label="balanced,8,127,crossbeam",
                    repeat_index=2,
                    status="timed_out",
                    ops_per_sec=None,
                ),
                self._record(
                    queue="ubq",
                    mode="throughput",
                    ubq_label="balanced,8,127,crossbeam",
                    repeat_index=3,
                    status="failed",
                    ops_per_sec=None,
                ),
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "run.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            coverage_rows = list(load_grid_coverage(path))

        statuses_by_repeat = {row[4][3]: row[5] for row in coverage_rows}
        self.assertEqual(
            {1: "completed", 2: "timed_out", 3: "failed"}, statuses_by_repeat
        )

        target = {}
        for machine, mode, scenario, specification, sample, status in coverage_rows:
            merge_grid_coverage(target, machine, mode, scenario, specification, sample, status)
        report = grid_coverage_report(target[("local", "throughput", "1p1c")], "throughput")

        self.assertEqual(1, report["present"])
        self.assertEqual(1, report["timed_out"])
        self.assertEqual(1, report["failed"])
        self.assertEqual(0, report["not_attempted"])
        self.assertEqual(3, report["expected"])
        self.assertFalse(report["complete"])

    def test_completed_status_wins_over_timed_out_for_same_sample(self):
        target = {}
        base = {
            "grid": "sparse",
            "core_placement": "interleaved",
            "expected_configurations": 1,
            "planned_repeats": 1,
            "batch_sizes": (),
            "planned_items": (10,),
        }
        sample = ("ubq", "label", None, 1, 10)
        merge_grid_coverage(target, "local", "throughput", "1p1c", base, sample, "timed_out")
        merge_grid_coverage(target, "local", "throughput", "1p1c", base, sample, "completed")

        coverage = target[("local", "throughput", "1p1c")]
        self.assertEqual({sample}, coverage["present"])
        self.assertEqual(set(), coverage["timed_out"])

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
        protocol = {"core_placement": "interleaved", "affinity_authoritative": True}
        payload = {
            "schema_version": 7,
            "meta": {
                "machine_label": "local",
                "scenario": "1p1c",
            },
            "results": [
                {
                    "queue": "ubq",
                    "ubq_label": "balanced,8,127,crossbeam",
                    "mode": "throughput",
                    "ops_per_sec": 100.0,
                    "push_elapsed_ns": 11,
                    "pop_elapsed_ns": 17,
                    "protocol": protocol,
                },
                {
                    "queue": "segqueue",
                    "mode": "throughput",
                    "batch_size": 8,
                    "ops_per_sec": 105.0,
                    "protocol": protocol,
                },
                {
                    "queue": "segqueue",
                    "mode": "fill_drain",
                    "ops_per_sec": 50.0,
                    "fill_elapsed_ns": 23,
                    "drain_elapsed_ns": 29,
                    "protocol": protocol,
                },
                {
                    "queue": "concurrent-queue",
                    "mode": "app_log_fan_in",
                    "ops_per_sec": 75.0,
                    "avg_data_latency_ns": 31,
                    "protocol": protocol,
                },
                {
                    "queue": "segqueue",
                    "mode": "app_log_mpsc_file",
                    "ops_per_sec": 91.0,
                    "producer_ops_per_sec": 123.0,
                    "consumer_ops_per_sec": 89.0,
                    "protocol": protocol,
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


class SchemaV7StatisticsTest(unittest.TestCase):
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

    def test_schema_v7_emits_three_isolated_throughput_metrics(self):
        payload = {
            "schema_version": 7,
            "meta": {
                "machine_label": "local",
                "scenario": "1p1c",
            },
            "results": [{
                "repeat_index": 2,
                "timestamp_unix_ms": 10,
                "queue": "segqueue",
                "mode": "throughput",
                "ops_per_sec": 10.0,
                "throughput_metrics": {
                    "enqueue_ops_per_sec": 20.0,
                    "dequeue_ops_per_sec": 30.0,
                },
                "protocol": {
                    "core_placement": "interleaved",
                    "affinity_authoritative": True,
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
        self.assertTrue(all(sample["repeat_index"] == 2 for sample in samples))

    def test_load_record_samples_carries_available_parallelism(self):
        payload = {
            "schema_version": 7,
            "meta": {"machine_label": "local", "scenario": "1p1c"},
            "results": [{
                "repeat_index": 1,
                "queue": "segqueue",
                "mode": "throughput",
                "ops_per_sec": 10.0,
                "protocol": {
                    "core_placement": "interleaved",
                    "affinity_authoritative": True,
                    "available_parallelism": 112,
                },
            }],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "run.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            samples = list(load_record_samples(path))
        self.assertTrue(samples)
        self.assertTrue(all(sample["available_parallelism"] == 112 for sample in samples))

    def test_load_record_samples_tolerates_missing_available_parallelism(self):
        payload = {
            "schema_version": 7,
            "meta": {"machine_label": "local", "scenario": "1p1c"},
            "results": [{
                "repeat_index": 1,
                "queue": "segqueue",
                "mode": "throughput",
                "ops_per_sec": 10.0,
                "protocol": {
                    "core_placement": "interleaved",
                    "affinity_authoritative": True,
                },
            }],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "run.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            samples = list(load_record_samples(path))
        self.assertTrue(samples)
        self.assertTrue(all(sample["available_parallelism"] is None for sample in samples))

    def test_duplicate_reruns_prefer_newest_completed_sample(self):
        base = {
            "machine": "local",
            "scenario": "1p1c",
            "queue": "segqueue",
            "mode": "throughput",
            "repeat_index": 1,
        }
        deduped = deduplicate_logical_samples([
            {**base, "timestamp": 10, "value": 1.0},
            {**base, "timestamp": 20, "value": 2.0},
            {**base, "machine": "other", "timestamp": 30, "value": 3.0},
        ])
        self.assertEqual(2, len(deduped))
        self.assertEqual(
            2.0,
            next(sample["value"] for sample in deduped if sample["machine"] == "local"),
        )


if __name__ == "__main__":
    unittest.main()
