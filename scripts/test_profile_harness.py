import csv
import json
import os
import shlex
import stat
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

from scripts.aggregate_profile_results import aggregate_results, build_independent_variant_effects
from scripts.make_profile_plan import PlanError, expand_campaign
from scripts.run_profile_case import CapabilityUnavailable, build_workload_command, enriched_case, run_apx, run_shadow
from unittest.mock import patch
import scripts.run_profile_case as run_profile_case
from scripts.import_performix_runs import discover as discover_performix_runs
from scripts.select_profile_plan import (
    candidate_coordinates,
    cliff_scores,
    filter_candidate_cases,
    load_benchmark_frontier,
    select_coordinates,
    select_representative_variations,
)
from scripts.select_profile_variations import select_variations

try:
    import matplotlib  # noqa: F401

    from scripts.plot_profile_results import main as plot_main
except ModuleNotFoundError:
    plot_main = None


ROOT = Path(__file__).resolve().parents[1]


def campaign_config() -> dict:
    return {
        "schema_version": 1,
        "campaign": "test-campaign",
        "schedule_seed": 17,
        "variants": [
            {
                "name": "system",
                "binary": "artifacts/{cluster}/bench_profile",
                "factors": {"allocator": "system", "page_distributor": "global"},
            }
        ],
        "stages": [
            {
                "name": "screen",
                "backend": "perf",
                "clusters": ["mn5"],
                "variants": ["system"],
                "queues": [
                    {
                        "name": "ubq",
                        "ubq_labels": ["balanced,1,page,crossbeam"],
                        "batch_sizes": [None, 32],
                    },
                    {"name": "ms-queue", "batch_sizes": [None]},
                ],
                "scenarios": ["1p1c", "2p2c"],
                "repeats": 2,
                "duration_secs": 1,
                "warmup_secs": 0,
            }
        ],
    }


def benchmark(queue: str, scenario: str, items: int = 1000, *, fixed: bool = False) -> dict:
    producers = int(scenario.split("p", 1)[0])
    return {
        "benchmark": "queue_handoff_profile",
        "schema_version": 1,
        "queue": queue,
        "queue_implementation": queue,
        "scenario": scenario,
        "warmup_ms": 0,
        "requested_duration_ms": 1000,
        "actual_to_requested_duration": 1.0,
        "elapsed_ns": 1_000_000_000,
        "producer_elapsed_ns": 900_000_000,
        "items_per_producer": items // producers,
        "items": items,
        "ops_per_sec": float(items),
        "affinity_ok": True,
        "fixed_work": fixed,
        "measurement_role": "unprofiled_shadow",
        "metrics": [
            {
                "metric_id": "allocator.allocated_bytes_per_item",
                "raw_name": "allocated bytes per item",
                "value": 16.0,
                "unit": "bytes/item",
                "kind": "gauge",
                "source": "allocator_probe",
                "phase": "measure",
                "scope": "process",
            },
            {
                "metric_id": "allocator.alloc_calls_per_million_items",
                "raw_name": "allocation calls per million items",
                "value": 100.0,
                "unit": "count/Mitem",
                "kind": "gauge",
                "source": "allocator_probe",
                "phase": "measure",
                "scope": "process",
            },
            {
                "metric_id": "os.minor_faults_per_million_items",
                "raw_name": "minor faults per million items",
                "value": 5.0,
                "unit": "count/Mitem",
                "kind": "gauge",
                "source": "getrusage",
                "phase": "measure",
                "scope": "process",
            },
            {
                "metric_id": "os.cpu_time_per_item_ns",
                "raw_name": "CPU time per item",
                "value": 20.0,
                "unit": "ns/item",
                "kind": "gauge",
                "source": "getrusage",
                "phase": "measure",
                "scope": "process",
            },
        ],
    }


class ProfilePlanTests(unittest.TestCase):
    def test_plan_is_deterministic_complete_and_machine_safe(self):
        config = campaign_config()
        first = expand_campaign(config)
        second = expand_campaign(config)
        self.assertEqual(first, second)
        self.assertEqual(12, len(first))
        self.assertEqual(len(first), len({case["case_id"] for case in first}))
        self.assertTrue(all(case["machine"] == "mn5" for case in first))
        self.assertTrue(all(case["collector"] == "perf" for case in first))
        self.assertTrue(all(case["core_placement"] == "interleaved" for case in first))
        self.assertTrue(all(case["allocator_probe"] for case in first))
        self.assertTrue(
            all(case["allocator_binary"].endswith("bench_profile_alloc") for case in first)
        )

    def test_mn5_rejects_arm_only_deep_recipe(self):
        config = campaign_config()
        stage = config["stages"][0]
        stage["backend"] = "apx"
        stage["recipes"] = ["cpu_microarchitecture"]
        with self.assertRaisesRegex(PlanError, "unsupported APX recipe"):
            expand_campaign(config)

    def test_fixed_prewarm_requires_fixed_measurement(self):
        config = campaign_config()
        config["stages"][0]["prewarm_items_per_producer"] = 100
        with self.assertRaisesRegex(PlanError, "requires items_per_producer"):
            expand_campaign(config)

    def test_fixed_item_plan_keeps_an_exact_unprofiled_shadow(self):
        config = campaign_config()
        config["stages"][0]["items_per_producer"] = 128
        cases = expand_campaign(config)
        self.assertTrue(all(case["items_per_producer"] == 128 for case in cases))
        self.assertTrue(all(not case["calibrate_outside_collector"] for case in cases))

    def test_plan_rejects_shadow_calibration_mode_that_conflicts_with_item_count(self):
        config = campaign_config()
        config["stages"][0]["items_per_producer"] = 128
        config["stages"][0]["calibrate_outside_collector"] = True
        with self.assertRaisesRegex(PlanError, "conflicts|must be false"):
            expand_campaign(config)

        config = campaign_config()
        config["stages"][0]["calibrate_outside_collector"] = False
        with self.assertRaisesRegex(PlanError, "requires|must be true"):
            expand_campaign(config)

    def test_independent_slurm_cases_cannot_be_mislabeled_as_pairs(self):
        config = campaign_config()
        config["stages"][0]["pair_variants"] = True
        with self.assertRaisesRegex(PlanError, "unsafe for independent Slurm"):
            expand_campaign(config)

    def test_lubq_backoffs_are_distinct_plan_variations(self):
        config = campaign_config()
        stage = config["stages"][0]
        stage["queues"] = [
            {
                "name": "lubq",
                "lubq_backoffs": ["crossbeam", "yield"],
                "batch_sizes": [256],
            }
        ]
        stage["scenarios"] = ["1p1c"]
        stage["repeats"] = 1
        cases = expand_campaign(config)
        self.assertEqual({"lubq", "lubq-yield"}, {case["queue_variant"] for case in cases})
        self.assertEqual(
            {"crossbeam", "yield"}, {case["lubq_backoff"] for case in cases}
        )

    def test_optional_profile_baselines_expand_exact_sizing_variants(self):
        config = campaign_config()
        stage = config["stages"][0]
        stage["queues"] = [
            {
                "name": "fastfifo",
                "batch_sizes": [None],
                "fastfifo_block_sizes": [64, 256],
                "fastfifo_capacities": [1024],
            },
            {
                "name": "lfqueue",
                "batch_sizes": [None],
                "lfqueue_segment_sizes": [32, 256],
            },
            {"name": "moodycamel-cq", "batch_sizes": [None, 64]},
        ]
        stage["scenarios"] = ["1p1c"]
        stage["repeats"] = 1
        cases = expand_campaign(config)
        self.assertEqual(6, len(cases))
        self.assertEqual(
            {"fastfifo_b64_c1024", "fastfifo_b256_c1024"},
            {case["queue_variant"] for case in cases if case["queue"] == "fastfifo"},
        )
        self.assertEqual(
            {"lfqueue_32", "lfqueue_256"},
            {case["queue_variant"] for case in cases if case["queue"] == "lfqueue"},
        )
        self.assertTrue(all(case["apx_run_name"] for case in cases))
        self.assertTrue(all(case["comparison_group_id"] for case in cases))

    def test_apx_selector_keeps_cross_queue_anchor_endpoint_and_cliff(self):
        cases = []
        rows = []
        for queue, rates in (("ubq", [100.0, 120.0, 60.0]), ("segqueue", [90.0, 100.0, 80.0])):
            for scenario, threads, rate in zip(("1p1c", "2p2c", "4p4c"), (2, 4, 8), rates):
                cases.append(
                    {
                        "cluster": "grace",
                        "scenario": scenario,
                        "batch_size": None,
                        "backend": "apx",
                        "queue": queue,
                    }
                )
                producers = threads // 2
                rows.append(
                    {
                        "machine": "grace",
                        "queue": queue,
                        "queue_variant": queue,
                        "variant": "system",
                        "batch_size": "",
                        "allocator": "system",
                        "page_distributor": "global",
                        "scenario": scenario,
                        "producers": str(producers),
                        "consumers": str(producers),
                        "total_threads": str(threads),
                        "median": str(rate),
                    }
                )
        selected = select_coordinates(
            cases,
            rows,
            cliffs_per_shape=1,
            include_anchors=True,
            include_endpoints=True,
        )
        self.assertIn(("grace", "1p1c", None), selected)
        self.assertIn(("grace", "2p2c", None), selected)
        self.assertIn(("grace", "4p4c", None), selected)
        self.assertTrue(
            any(
                evidence["reason"] == "cliff_predecessor"
                for evidence in selected[("grace", "2p2c", None)]
            )
        )
        self.assertTrue(
            any(
                evidence["reason"] == "scaling_cliff"
                for evidence in selected[("grace", "4p4c", None)]
            )
        )

    def test_focus_queue_drives_cliff_and_comparators_remain_candidates(self):
        cases = []
        rows = []
        scenarios = (("1p1c", 1), ("2p2c", 2), ("4p4c", 4))
        for queue, rates in (("lubq", (100.0, 190.0, 100.0)), ("segqueue", (100.0, 10.0, 20.0))):
            for (scenario, workers), rate in zip(scenarios, rates):
                cases.append(
                    {
                        "cluster": "grace",
                        "scenario": scenario,
                        "batch_size": 256,
                        "backend": "apx",
                        "queue": queue,
                        "queue_variant": queue,
                    }
                )
                rows.append(
                    {
                        "machine": "grace",
                        "queue": queue,
                        "queue_variant": queue,
                        "variant": "source-tree",
                        "batch_size": "256",
                        "allocator": "system",
                        "page_distributor": "global",
                        "scenario": scenario,
                        "producers": str(workers),
                        "consumers": str(workers),
                        "total_threads": str(2 * workers),
                        "median": str(rate),
                        "n": "3",
                    }
                )
        selected = select_coordinates(
            cases,
            rows,
            cliffs_per_shape=1,
            include_anchors=False,
            include_endpoints=False,
            score_queue_variants={"lubq"},
            min_evidence_repeats=3,
            selected_shapes={"balanced"},
        )
        self.assertEqual(
            {("grace", "2p2c", 256), ("grace", "4p4c", 256)},
            set(selected),
        )
        retained = [case for case in cases if (case["cluster"], case["scenario"], 256) in selected]
        self.assertEqual({"lubq", "segqueue"}, {case["queue"] for case in retained})

    def test_missing_intermediate_raw_point_is_not_called_a_local_cliff(self):
        cases = [
            {
                "cluster": "grace",
                "scenario": scenario,
                "batch_size": None,
                "queue": "lubq",
                "queue_variant": "lubq",
            }
            for scenario in ("1p1c", "2p2c", "4p4c")
        ]
        rows = [
            {
                "machine": "grace",
                "queue": "lubq",
                "queue_variant": "lubq",
                "variant": "source-tree",
                "batch_size": "",
                "allocator": "system",
                "page_distributor": "global",
                "scenario": scenario,
                "producers": workers,
                "consumers": workers,
                "total_threads": str(2 * int(workers)),
                "median": rate,
                "n": "3",
            }
            for scenario, workers, rate in (("1p1c", "1", "100"), ("4p4c", "4", "10"))
        ]
        self.assertEqual({}, cliff_scores(rows, candidate_coordinates(cases)))

    def test_benchmark_frontier_loader_filters_failures_and_deduplicates_repeats(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "grace-6/1p1c"
            root.mkdir(parents=True)
            protocol = {
                "available_parallelism": 144,
                "core_placement": "interleaved",
                "affinity_authoritative": True,
                "item_policy": "scenario_scaled_v1",
                "throughput_policy": {"phase_ms": 1000},
            }

            def sample(repeat: int, rate: float, timestamp: int, **extra) -> dict:
                value = {
                    "repeat_index": repeat,
                    "queue": "lubq",
                    "mode": "throughput",
                    "batch_size": 256,
                    "total_items": 1000,
                    "consumed_items": 1000,
                    "ops_per_sec": rate,
                    "timestamp_unix_ms": timestamp,
                    "protocol": dict(protocol),
                }
                value.update(extra)
                return value

            invalid_protocol = dict(protocol)
            invalid_protocol["affinity_authoritative"] = False
            record = {
                "schema_version": 7,
                "meta": {
                    "machine_label": "grace-6",
                    "scenario": "1p1c",
                    "producers": 1,
                    "consumers": 1,
                    "git_commit": "abcdef0",
                    "git_dirty": True,
                },
                "results": [
                    sample(1, 100.0, 1),
                    sample(1, 110.0, 2),
                    sample(2, 120.0, 3),
                    sample(3, 130.0, 4),
                    sample(4, 0.0, 5, status="timed_out"),
                    sample(5, 140.0, 6, protocol=invalid_protocol),
                ],
            }
            (root / "record.json").write_text(json.dumps(record), encoding="utf-8")
            cases = [
                {
                    "cluster": "grace",
                    "scenario": "1p1c",
                    "batch_size": 256,
                    "queue": "lubq",
                    "queue_variant": "lubq",
                }
            ]
            rows, provenance = load_benchmark_frontier(root.parent, cases)
            self.assertEqual(1, len(rows))
            self.assertEqual("3", rows[0]["n"])
            self.assertEqual("120.0", rows[0]["median"])
            self.assertEqual(1, provenance["discarded_samples"]["non_completed_status"])
            self.assertEqual(1, provenance["discarded_samples"]["non_authoritative_affinity"])
            self.assertEqual(1, provenance["discarded_samples"]["superseded_duplicate"])

    def test_benchmark_frontier_maps_lubq_yield_to_profile_variant(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "grace-7/1p1c"
            root.mkdir(parents=True)
            protocol = {
                "available_parallelism": 144,
                "core_placement": "interleaved",
                "affinity_authoritative": True,
                "item_policy": "scenario_scaled_v1",
                "throughput_policy": {"phase_ms": 1000},
            }
            record = {
                "schema_version": 7,
                "meta": {
                    "machine_label": "grace-7",
                    "scenario": "1p1c",
                    "producers": 1,
                    "consumers": 1,
                    "git_commit": "abcdef0",
                    "git_dirty": True,
                },
                "results": [
                    {
                        "repeat_index": 1,
                        "queue": "lubq-yield",
                        "mode": "throughput",
                        "batch_size": 256,
                        "total_items": 1000,
                        "consumed_items": 1000,
                        "ops_per_sec": 123.0,
                        "timestamp_unix_ms": 1,
                        "protocol": protocol,
                    }
                ],
            }
            (root / "record.json").write_text(json.dumps(record), encoding="utf-8")
            cases = [
                {
                    "cluster": "grace",
                    "scenario": "1p1c",
                    "batch_size": 256,
                    "queue": "lubq",
                    "queue_implementation": "lubq-yield",
                    "queue_variant": "lubq-yield",
                }
            ]
            rows, _provenance = load_benchmark_frontier(root.parent, cases)
            self.assertEqual("lubq", rows[0]["queue"])
            self.assertEqual("lubq-yield", rows[0]["queue_variant"])

    def test_candidate_filters_keep_exact_variant_batch_and_first_repeats(self):
        cases = [
            {
                "queue": queue,
                "queue_variant": variant,
                "batch_size": batch,
                "repeat": repeat,
            }
            for queue, variant, batch, repeat in (
                ("ubq", "ubq:a", 256, 1),
                ("ubq", "ubq:a", 256, 2),
                ("ubq", "ubq:b", 256, 1),
                ("lubq", "lubq", None, 1),
            )
        ]
        selected = filter_candidate_cases(
            cases,
            queues=None,
            queue_variants={"ubq:a"},
            ubq_labels=None,
            batches={256},
            repeat_limit=1,
        )
        self.assertEqual([cases[0]], selected)

    def test_representatives_keep_cliff_fast_slow_and_ignore_missing_candidates(self):
        target = ("grace", "4p4c", 256)
        cases = [
            {
                "cluster": target[0],
                "scenario": target[1],
                "batch_size": target[2],
                "queue": queue,
                "queue_variant": queue_variant,
            }
            for queue, queue_variant in (
                ("lubq", "lubq-yield"),
                ("segqueue", "crossbeam-batchqueue"),
                ("mutex-vecdeque", "mutex-vecdeque-native-batch"),
                ("ubq", "ubq:missing"),
            )
        ]
        rows = [
            {
                "machine": target[0],
                "scenario": target[1],
                "batch_size": str(target[2]),
                "queue": queue,
                "queue_variant": queue_variant,
                "median": str(rate),
                "n": "3",
            }
            for queue, queue_variant, rate in (
                ("lubq", "lubq-yield", 80.0),
                ("segqueue", "crossbeam-batchqueue", 200.0),
                ("mutex-vecdeque", "mutex-vecdeque-native-batch", 20.0),
            )
        ]
        selected = {
            target: [
                {
                    "reason": "scaling_cliff",
                    "score": 3.0,
                    "queue_variant": "lubq-yield",
                }
            ]
        }
        representatives = select_representative_variations(
            cases,
            rows,
            selected,
            maximum=3,
            min_evidence_repeats=3,
        )
        by_variant = {item["queue_variant"]: item for item in representatives[target]}
        self.assertEqual(
            {
                "lubq-yield",
                "crossbeam-batchqueue",
                "mutex-vecdeque-native-batch",
            },
            set(by_variant),
        )
        self.assertIn("cliff_subject", by_variant["lubq-yield"]["reasons"])
        self.assertIn("fast_control", by_variant["crossbeam-batchqueue"]["reasons"])
        self.assertIn("slow_contrast", by_variant["mutex-vecdeque-native-batch"]["reasons"])
        self.assertEqual(
            ["ubq:missing"],
            by_variant["lubq-yield"]["unavailable_candidate_variants"],
        )

    def test_winner_and_one_factor_selection_use_authoritative_throughput(self):
        cases = []
        rows = []
        for batch, backoff, rate in (
            (64, "crossbeam", 100.0),
            (256, "crossbeam", 200.0),
            (256, "yield", 150.0),
            (4096, "crossbeam", 175.0),
        ):
            label = f"balanced,1,page,{backoff}"
            queue_variant = f"ubq:{label}"
            cases.append(
                {
                    "campaign": "campaign-a",
                    "stage": "hotspots",
                    "cluster": "grace",
                    "scenario": "64p1c",
                    "queue": "ubq",
                    "queue_variant": queue_variant,
                    "ubq_label": label,
                    "batch_size": batch,
                    "recipe": "code_hotspots",
                    "repeat": 1,
                    "apx_run_name": (
                        f"grace__{queue_variant}__64p1c__b{batch}__code_hotspots__r01__old__case"
                    ),
                }
            )
            rows.append(
                {
                    "machine": "grace",
                    "scenario": "64p1c",
                    "queue": "ubq",
                    "queue_variant": queue_variant,
                    "batch_size": str(batch),
                    "median": str(rate),
                    "n": "3",
                }
            )
        winners, _manifest = select_variations(
            cases,
            rows,
            policy="winners",
            dimensions={"batch_size", "backoff"},
            minimum_repeats=3,
            repeat_limit=1,
        )
        self.assertEqual(1, len(winners))
        self.assertEqual(256, winners[0]["batch_size"])
        self.assertEqual("crossbeam", winners[0]["selection_evidence"]["parameters"]["backoff"])
        self.assertEqual(
            winners[0]["comparison_group_id"], winners[0]["apx_run_name"].split("__")[-2]
        )

        contrasts, _manifest = select_variations(
            cases,
            rows,
            policy="one-factor",
            dimensions={"batch_size", "backoff"},
            minimum_repeats=3,
            repeat_limit=1,
        )
        self.assertEqual(4, len(contrasts))
        winner = next(case for case in contrasts if case["comparison_role"] == "throughput_winner")
        self.assertEqual(
            {"backoff", "batch_size"},
            {item["dimension"] for item in winner["comparison_sets"]},
        )

    def test_performix_manifest_correlates_archive_case_and_shadow(self):
        with tempfile.TemporaryDirectory() as temporary:
            case_dir = Path(temporary) / "campaign" / "case"
            case_dir.mkdir(parents=True)
            (case_dir / "case.json").write_text(
                json.dumps(
                    {
                        "backend": "apx",
                        "campaign": "campaign",
                        "case_id": "case",
                        "queue": "ubq",
                        "queue_variant": "ubq:balanced,1,page,crossbeam",
                        "ubq_label": "balanced,1,page,crossbeam",
                        "scenario": "64p1c",
                        "batch_size": 256,
                        "recipe": "code_hotspots",
                        "repeat": 1,
                        "apx_run_name": "grace__ubq__64p1c__b256",
                        "comparison_group_id": "group-1",
                    }
                ),
                encoding="utf-8",
            )
            (case_dir / "status.json").write_text(
                json.dumps({"status": "completed", "apx_run_id": "run-1"}),
                encoding="utf-8",
            )
            (case_dir / "benchmark.json").write_text(
                json.dumps({"ops_per_sec": 123.0}), encoding="utf-8"
            )
            (case_dir / "apx-run-id.txt").write_text("run-1\n", encoding="utf-8")
            with zipfile.ZipFile(case_dir / "run-1.zip", "w") as archive:
                archive.writestr("run/metadata.json", "{}")
            rows = discover_performix_runs(Path(temporary))
            self.assertEqual(1, len(rows))
            self.assertEqual("run-1", rows[0]["apx_run_id"])
            self.assertEqual("group-1", rows[0]["comparison_group_id"])
            self.assertEqual(123.0, rows[0]["shadow_ops_per_sec"])


class ProfileRunnerContractTests(unittest.TestCase):
    def test_apx_capture_is_renamed_before_export(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            calls = root / "calls.txt"
            apx = fake_bin / "apx"
            apx.write_text(
                """#!/usr/bin/env python3
import json
import os
import sys
import zipfile
from pathlib import Path
args = sys.argv[1:]
with Path(os.environ['FAKE_APX_CALLS']).open('a', encoding='utf-8') as stream:
    stream.write(json.dumps(args) + '\\n')
if args[:2] == ['recipe', 'run']:
    print('Run ID: run-123')
elif args[:2] == ['run', 'export']:
    destination = Path(args[3]) / 'run-123.zip'
    with zipfile.ZipFile(destination, 'w') as archive:
        archive.writestr('run/metadata.json', json.dumps({'run.result': 'success', 'run.id': 'run-123', 'run.recipe_name': 'code_hotspots'}))
        archive.writestr('run/manifest.json', '{}')
raise SystemExit(0)
""",
                encoding="utf-8",
            )
            apx.chmod(apx.stat().st_mode | stat.S_IXUSR)
            environment = dict(os.environ)
            environment["PATH"] = f"{fake_bin}:{environment['PATH']}"
            environment["FAKE_APX_CALLS"] = str(calls)
            case = {
                "case_id": "case-1",
                "recipe": "code_hotspots",
                "collector_timeout_secs": 60,
                "apx_run_name": "grace__ubq__64p1c__b256__r01",
                "comparison_group_id": "group-1",
                "comparison_role": "throughput_winner",
            }
            _profiled, run_id, run_name, renamed = run_apx(
                case, ["/usr/bin/true"], root, ROOT, environment
            )
            self.assertEqual("run-123", run_id)
            self.assertEqual(case["apx_run_name"], run_name)
            self.assertTrue(renamed)
            recorded = [json.loads(line) for line in calls.read_text().splitlines()]
            rename_index = recorded.index(["run", "rename", "run-123", run_name])
            export_index = recorded.index(["run", "export", "run-123", str(root)])
            self.assertLess(rename_index, export_index)
            identity = json.loads((root / "apx-identity.json").read_text())
            self.assertEqual("group-1", identity["comparison_group_id"])

    def _fake_apx_with_flaky_version(self, root, fail_versions):
        """An `apx` stub whose `version` subcommand fails `fail_versions`
        times (simulating a daemon still binding its readiness endpoint)
        before succeeding; every other subcommand succeeds immediately."""
        fake_bin = root / "bin"
        fake_bin.mkdir()
        calls = root / "calls.txt"
        counter = root / "version_calls.txt"
        counter.write_text("0")
        apx = fake_bin / "apx"
        apx.write_text(
            f"""#!/usr/bin/env python3
import json
import os
import sys
import zipfile
from pathlib import Path
args = sys.argv[1:]
with Path(os.environ['FAKE_APX_CALLS']).open('a', encoding='utf-8') as stream:
    stream.write(json.dumps(args) + '\\n')
if args[:1] == ['version']:
    counter_path = Path({str(counter)!r})
    seen = int(counter_path.read_text())
    counter_path.write_text(str(seen + 1))
    if seen < {fail_versions}:
        sys.exit(1)
    sys.exit(0)
if args[:2] == ['recipe', 'run']:
    print('Run ID: run-123')
elif args[:2] == ['run', 'export']:
    destination = Path(args[3]) / 'run-123.zip'
    with zipfile.ZipFile(destination, 'w') as archive:
        archive.writestr('run/metadata.json', json.dumps({{'run.result': 'success', 'run.id': 'run-123', 'run.recipe_name': 'code_hotspots'}}))
        archive.writestr('run/manifest.json', '{{}}')
raise SystemExit(0)
""",
            encoding="utf-8",
        )
        apx.chmod(apx.stat().st_mode | stat.S_IXUSR)
        environment = dict(os.environ)
        environment["PATH"] = f"{fake_bin}:{environment['PATH']}"
        environment["FAKE_APX_CALLS"] = str(calls)
        return environment, calls

    def test_apx_version_readiness_check_retries_past_a_slow_daemon_start(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            environment, calls = self._fake_apx_with_flaky_version(root, fail_versions=2)
            case = {
                "case_id": "case-1",
                "recipe": "code_hotspots",
                "collector_timeout_secs": 60,
                "apx_run_name": "grace__ubq__64p1c__b256__r01",
            }
            with patch.object(run_profile_case, "APX_VERSION_RETRY_DELAY_SECS", 0):
                _profiled, run_id, _run_name, _renamed = run_apx(
                    case, ["/usr/bin/true"], root, ROOT, environment
                )
            self.assertEqual("run-123", run_id)
            recorded = [json.loads(line) for line in calls.read_text().splitlines()]
            version_calls = [entry for entry in recorded if entry[:1] == ["version"]]
            self.assertEqual(3, len(version_calls))

    def test_instruction_mix_disassembles_binary_and_ignores_stale_wrapper_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            environment, calls = self._fake_apx_with_flaky_version(root, fail_versions=0)
            stale = {"queue": "stale", "scenario": "1p1c", "elapsed_ns": 1, "items": 1}
            (root / "workload-stdout.json").write_text(json.dumps(stale))
            command = ["/path with spaces/bench_hardware", "--config", "/config with spaces.json"]
            case = {"case_id": "imix", "recipe": "instruction_mix", "collector_timeout_secs": 60}
            with patch.object(run_profile_case, "parse_apx_workload", return_value={"from_archive": True}):
                result, _, _, _ = run_apx(case, command, root, ROOT, environment)
            self.assertEqual({"from_archive": True}, result)
            recorded = [json.loads(line) for line in calls.read_text().splitlines()]
            recipe_calls = [entry for entry in recorded if entry[:2] in (["recipe", "ready"], ["recipe", "run"])]
            self.assertEqual(2, len(recipe_calls))
            for entry in recipe_calls:
                self.assertEqual(command, shlex.split(entry[entry.index("--workload") + 1]))
            self.assertFalse((root / "apx-workload.sh").exists())

    def test_apx_version_readiness_check_gives_up_after_bounded_retries(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            environment, calls = self._fake_apx_with_flaky_version(root, fail_versions=1000)
            case = {
                "case_id": "case-1",
                "recipe": "code_hotspots",
                "collector_timeout_secs": 60,
                "apx_run_name": "grace__ubq__64p1c__b256__r01",
            }
            with patch.object(run_profile_case, "APX_VERSION_RETRY_DELAY_SECS", 0):
                with self.assertRaises(CapabilityUnavailable):
                    run_apx(case, ["/usr/bin/true"], root, ROOT, environment)
            recorded = [json.loads(line) for line in calls.read_text().splitlines()]
            version_calls = [entry for entry in recorded if entry[:1] == ["version"]]
            self.assertEqual(run_profile_case.APX_VERSION_ATTEMPTS, len(version_calls))

    def test_fixed_command_carries_exact_count_and_optional_prewarm(self):
        case = {
            "queue": "ubq",
            "scenario": "2p2c",
            "duration_secs": 10,
            "warmup_secs": 2,
            "ubq_label": "balanced,1,page,crossbeam",
            "prewarm_items_per_producer": 256,
        }
        command = build_workload_command(case, Path("/tmp/bench_profile"), items_per_producer=4096, fixed=True)
        self.assertIn("--items-per-producer", command)
        self.assertIn("4096", command)
        self.assertIn("--prewarm-items-per-producer", command)
        self.assertEqual("0", command[command.index("--warmup-secs") + 1])
        probe_command = build_workload_command(
            case,
            Path("/tmp/bench_profile_alloc"),
            items_per_producer=4096,
            fixed=True,
            allocator_probe=True,
        )
        self.assertIn("--allocator-probe", probe_command)

    def test_lubq_command_carries_selected_backoff(self):
        case = {
            "queue": "lubq",
            "lubq_backoff": "yield",
            "scenario": "1p1c",
            "duration_secs": 10,
            "warmup_secs": 2,
        }
        command = build_workload_command(
            case, Path("/tmp/bench_profile"), items_per_producer=None, fixed=False
        )
        self.assertEqual("yield", command[command.index("--lubq-backoff") + 1])

    def test_sized_baseline_command_carries_exact_parameters(self):
        rbbq = {
            "queue": "fastfifo",
            "scenario": "1p1c",
            "duration_secs": 10,
            "warmup_secs": 2,
            "fastfifo_block_size": 256,
            "fastfifo_capacity": 1048576,
        }
        command = build_workload_command(
            rbbq, Path("/tmp/bench_profile"), items_per_producer=4096, fixed=True
        )
        self.assertEqual("256", command[command.index("--fastfifo-block-size") + 1])
        self.assertEqual("1048576", command[command.index("--fastfifo-capacity") + 1])
        lscq = {
            "queue": "lfqueue",
            "scenario": "1p1c",
            "duration_secs": 10,
            "warmup_secs": 2,
            "lfqueue_segment_size": 32,
        }
        command = build_workload_command(
            lscq, Path("/tmp/bench_profile"), items_per_producer=4096, fixed=True
        )
        self.assertEqual("32", command[command.index("--lfqueue-segment-size") + 1])

    def test_perf_case_gets_explicit_recipe_identity(self):
        case = enriched_case({"cluster": "mn5", "backend": "perf", "queue": "segqueue"})
        self.assertEqual("perf_stat", case["recipe"])
        self.assertEqual("perf", case["collector"])

    def test_explicit_count_shadow_is_unprofiled_fixed_work(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "bench_profile"
            binary.write_text(
                """#!/usr/bin/env python3
import json
import sys
args = sys.argv[1:]
fixed = '--items-per-producer' in args
items = int(args[args.index('--items-per-producer') + 1]) if fixed else 1
print(json.dumps({'queue': 'segqueue', 'scenario': '1p1c', 'elapsed_ns': 1000, 'items': items, 'items_per_producer': items, 'ops_per_sec': float(items), 'affinity_ok': True, 'fixed_work': fixed}))
""",
                encoding="utf-8",
            )
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            case = {
                "queue": "segqueue",
                "scenario": "1p1c",
                "duration_secs": 1,
                "warmup_secs": 0,
                "items_per_producer": 64,
                "calibrate_outside_collector": False,
            }
            shadow, command = run_shadow(case, binary, root, ROOT, os.environ)
            self.assertTrue(shadow["fixed_work"])
            self.assertEqual(64, shadow["items_per_producer"])
            self.assertIn("--items-per-producer", command)
            self.assertNotIn("--allocator-probe", command)

    def test_runner_separates_shadow_allocator_and_perf_workloads(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            workload_source = """#!/usr/bin/env python3
import json
import sys
args = sys.argv[1:]
fixed = '--items-per-producer' in args
items_per_producer = int(args[args.index('--items-per-producer') + 1]) if fixed else 1000
metrics = []
if '--allocator-probe' in args:
    metrics.append({'metric_id': 'allocator.alloc_calls', 'raw_name': 'allocation calls', 'value': 7, 'unit': 'count', 'kind': 'counter', 'source': 'allocator_probe', 'phase': 'measure', 'scope': 'process'})
print(json.dumps({'benchmark': 'queue_handoff_profile', 'schema_version': 1, 'queue': 'segqueue', 'queue_implementation': 'crossbeam-segqueue', 'scenario': '1p1c', 'warmup_ms': 0, 'requested_duration_ms': 1000, 'actual_to_requested_duration': 1.0, 'elapsed_ns': 1000000000, 'producer_elapsed_ns': 900000000, 'items_per_producer': items_per_producer, 'items': items_per_producer, 'ops_per_sec': float(items_per_producer), 'affinity_ok': True, 'fixed_work': fixed, 'metrics': metrics}))
"""
            primary = root / "bench_profile"
            allocator = root / "bench_profile_alloc"
            for binary in (primary, allocator):
                binary.write_text(workload_source, encoding="utf-8")
                binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            perf = fake_bin / "perf"
            perf.write_text(
                """#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path
args = sys.argv[1:]
if args == ['--version']:
    print('perf version test')
    raise SystemExit(0)
output = Path(args[args.index('-o') + 1])
output.write_text('1000;;cycles;1000000;100.00\\n2000;;instructions;1000000;100.00\\n', encoding='utf-8')
command = args[args.index('--') + 1:]
completed = subprocess.run(command, text=True, capture_output=True, check=False)
sys.stdout.write(completed.stdout)
sys.stderr.write(completed.stderr)
raise SystemExit(completed.returncode)
""",
                encoding="utf-8",
            )
            perf.chmod(perf.stat().st_mode | stat.S_IXUSR)
            case = {
                "schema_version": 1,
                "case_id": "runner-case",
                "campaign": "runner-campaign",
                "stage": "screen",
                "cluster": "mn5",
                "backend": "perf",
                "binary": str(primary),
                "allocator_binary": str(allocator),
                "allocator_probe": True,
                "queue": "segqueue",
                "scenario": "1p1c",
                "duration_secs": 1,
                "warmup_secs": 0,
                "collector_timeout_secs": 60,
                "perf_events": ["cycles", "instructions"],
            }
            plan = root / "plan.jsonl"
            plan.write_text(json.dumps(case) + "\n", encoding="utf-8")
            results = root / "results"
            environment = dict(os.environ)
            environment["PATH"] = f"{fake_bin}:{environment['PATH']}"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts/run_profile_case.py"),
                    "--plan",
                    str(plan),
                    "--index",
                    "0",
                    "--results-root",
                    str(results),
                    "--repo-root",
                    str(ROOT),
                ],
                cwd=ROOT,
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)
            case_dir = results / "runner-campaign/runner-case"
            merged = json.loads((case_dir / "benchmark.json").read_text(encoding="utf-8"))
            self.assertEqual("unprofiled_shadow", merged["measurement_role"])
            self.assertEqual("allocator.alloc_calls", merged["allocator_metrics"][0]["metric_id"])
            commands = json.loads((case_dir / "commands.json").read_text(encoding="utf-8"))
            self.assertNotIn("--allocator-probe", commands["shadow"])
            self.assertIn("--allocator-probe", commands["allocator_probe"])
            self.assertNotIn("--allocator-probe", commands["collected_workload"])


class ProfileAggregateAndPlotTests(unittest.TestCase):
    def test_independent_variant_effect_normalizes_lower_is_better(self):
        trials = []
        metrics = []
        for variant, baseline, values in (("base", True, [10.0, 12.0]), ("sharded", False, [5.0, 6.0])):
            for repeat, value in enumerate(values):
                trial_id = f"{variant}-{repeat}"
                trials.append(
                    {
                        "trial_id": trial_id,
                        "authoritative": True,
                        "campaign_id": "campaign",
                        "machine": "grace",
                        "recipe": "perf_stat",
                        "queue": "ubq",
                        "queue_variant": "ubq",
                        "scenario": "8p8c",
                        "producers": 8,
                        "consumers": 8,
                        "total_threads": 16,
                        "batch_size": "",
                        "variant": variant,
                        "is_baseline": baseline,
                        "reference_variant": "base",
                    }
                )
                metrics.append(
                    {
                        "trial_id": trial_id,
                        "source": "allocator_probe",
                        "metric_id": "allocator.allocated_bytes_per_item",
                        "unit": "bytes/item",
                        "phase": "measure",
                        "scope": "process",
                        "dimensions_json": "{}",
                        "value": value,
                    }
                )
        effects = build_independent_variant_effects(trials, metrics, 20)
        self.assertEqual(1, len(effects))
        self.assertAlmostEqual(2.0, effects[0]["performance_median"])
        self.assertEqual("independent_repeats", effects[0]["design"])

    def make_apx_case(self, root: Path) -> None:
        directory = root / "test-campaign" / "apx-case"
        directory.mkdir(parents=True)
        case = {
            "campaign": "test-campaign",
            "case_id": "apx-case",
            "trial_id": "test-campaign/apx-case",
            "cell_id": "apx-cell",
            "repeat_index": 0,
            "machine": "grace",
            "collector": "apx",
            "recipe": "code_hotspots",
            "queue": "ubq",
            "queue_variant": "ubq:balanced",
            "comparison_variant": "system",
            "scenario": "2p2c",
            "producers": 2,
            "consumers": 2,
            "batch_size": 32,
            "allocator": "system",
            "page_distributor": "global",
            "core_placement": "interleaved",
            "constraint": "perfparanoid",
            "quality_policy": {"require_affinity": True, "minimum_samples": 1000},
        }
        (directory / "case.json").write_text(json.dumps(case), encoding="utf-8")
        (directory / "status.json").write_text(json.dumps({"status": "completed"}), encoding="utf-8")
        (directory / "benchmark.json").write_text(json.dumps(benchmark("ubq", "2p2c")), encoding="utf-8")
        profiled = benchmark("ubq", "2p2c", fixed=True)
        archive_path = directory / "apx-run.zip"
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr(
                "run/metadata.json",
                json.dumps(
                    {
                        "run.result": "success",
                        "run.id": "apx-run",
                        "run.recipe_name": "code_hotspots",
                    }
                ),
            )
            archive.writestr("run/manifest.json", "{}")
            archive.writestr("run/tool/neoprof/0/capture_log.txt", json.dumps(profiled) + "\n")
            archive.writestr(
                "run/output/callpath_total_metrics.json",
                json.dumps(
                    {
                        "columns": [{"name": "Backend Bound", "units": "%"}],
                        "rows": [{"call_frame_id": 0, "column_data": [45.0]}],
                    }
                ),
            )
            archive.writestr(
                "run/output/callpath_total_samples.json",
                json.dumps(
                    {
                        "columns": [{"name": "Sample Count", "units": "sample_counter"}],
                        "rows": [{"call_frame_id": 0, "column_data": [2000]}],
                    }
                ),
            )
            archive.writestr(
                "run/output/functions-capture-periodic_sampling.csv",
                "image,symbol,inlined from,uid,Periodic Samples\nbench,atomic_hot,,1,400\n",
            )

    def make_perf_case(self, root: Path) -> None:
        directory = root / "test-campaign" / "perf-case"
        directory.mkdir(parents=True)
        case = {
            "campaign": "test-campaign",
            "case_id": "perf-case",
            "trial_id": "test-campaign/perf-case",
            "cell_id": "perf-cell",
            "repeat_index": 1,
            "machine": "mn5",
            "collector": "perf",
            "recipe": None,
            "queue": "segqueue",
            "queue_variant": "crossbeam-segqueue",
            "comparison_variant": "system",
            "scenario": "1p1c",
            "producers": 1,
            "consumers": 1,
            "allocator": "system",
            "page_distributor": "global",
            "core_placement": "interleaved",
            "constraint": "perfparanoid",
        }
        (directory / "case.json").write_text(json.dumps(case), encoding="utf-8")
        (directory / "status.json").write_text(json.dumps({"status": "completed"}), encoding="utf-8")
        (directory / "benchmark.json").write_text(json.dumps(benchmark("segqueue", "1p1c")), encoding="utf-8")
        (directory / "profiled-benchmark.json").write_text(
            json.dumps(benchmark("segqueue", "1p1c", fixed=True)), encoding="utf-8"
        )
        (directory / "perf.csv").write_text(
            "1000;;cycles;1000000;100.00\n2000;;instructions;1000000;100.00\n",
            encoding="utf-8",
        )

    def test_aggregate_dynamic_apx_perf_quality_and_plot_suite(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "raw"
            aggregate = Path(temporary) / "aggregate"
            plots = Path(temporary) / "plots"
            self.make_apx_case(root)
            self.make_perf_case(root)
            campaign_dir = root / "test-campaign"
            (campaign_dir / "campaign.json").write_text(
                json.dumps({"expected_cases": 2}), encoding="utf-8"
            )

            paths = aggregate_results(root, aggregate, bootstrap_samples=20)
            with paths["trials"].open(newline="", encoding="utf-8") as stream:
                trials = list(csv.DictReader(stream))
            self.assertEqual(2, len(trials))
            self.assertTrue(all(trial["authoritative"] == "True" for trial in trials))
            apx_trial = next(trial for trial in trials if trial["collector"] == "apx")
            perf_trial = next(trial for trial in trials if trial["collector"] == "perf")
            self.assertEqual("0", apx_trial["repeat_index"])
            self.assertEqual("interleaved", apx_trial["placement"])
            self.assertEqual("perf_stat", perf_trial["recipe"])

            with paths["metrics"].open(newline="", encoding="utf-8") as stream:
                metrics = list(csv.DictReader(stream))
            metric_ids = {row["metric_id"] for row in metrics}
            self.assertIn("apx.sample_count", metric_ids)
            self.assertIn("perf.cycles_per_op", metric_ids)
            with paths["hotspots"].open(newline="", encoding="utf-8") as stream:
                hotspots = list(csv.DictReader(stream))
            self.assertAlmostEqual(0.2, float(hotspots[0]["self_fraction"]))

            if plot_main is not None:
                self.assertEqual(0, plot_main([str(aggregate), "--out-dir", str(plots)]))
                self.assertTrue((plots / "coverage.png").is_file())
                self.assertTrue((plots / "bottleneck_fingerprint_grace.png").is_file())
                self.assertTrue((plots / "index.md").is_file())


class ProfileSlurmTests(unittest.TestCase):
    def test_submitter_requests_perfparanoid_and_full_mn5_node(self):
        with tempfile.TemporaryDirectory() as temporary:
            temp = Path(temporary)
            fake_bin = temp / "bin"
            fake_bin.mkdir()
            sbatch = fake_bin / "sbatch"
            sbatch.write_text(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$SBATCH_ARGS_FILE\"\nprintf '4242\\n'\n",
                encoding="utf-8",
            )
            sbatch.chmod(sbatch.stat().st_mode | stat.S_IXUSR)
            deploy = temp / "deploy"
            sbatch_args = temp / "sbatch-args.txt"
            plan = temp / "plan.jsonl"
            plan.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "case_id": "case-1",
                        "campaign": "campaign",
                        "stage": "screen",
                        "cluster": "mn5",
                        "backend": "perf",
                        "binary": "artifacts/mn5/bench_profile",
                        "queue": "segqueue",
                        "scenario": "1p1c",
                        "repeat": 1,
                        "duration_secs": 1,
                        "warmup_secs": 0,
                        "collector_timeout_secs": 60,
                        "perf_events": ["cycles"],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            environment = dict(os.environ)
            environment.update(
                {
                    "PATH": f"{fake_bin}:{environment['PATH']}",
                    "UBQ": str(deploy),
                    "ACCOUNT": "test-account",
                    "USER": "tester",
                    "SBATCH_ARGS_FILE": str(sbatch_args),
                }
            )
            completed = subprocess.run(
                [str(ROOT / "slurm/submit_profile_plan.sh"), "mn5", str(plan)],
                cwd=ROOT,
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)
            self.assertEqual("4242", completed.stdout.strip())
            submitted_args = sbatch_args.read_text(encoding="utf-8")
            self.assertIn("--constraint=perfparanoid", submitted_args)
            self.assertIn("--cpus-per-task=112", submitted_args)
            self.assertIn("--hint=nomultithread", submitted_args)
            manifest = deploy / "profiling_results/raw/campaign/campaign.json"
            manifest_data = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertEqual(1, manifest_data["expected_cases"])
            self.assertEqual("4242", manifest_data["submissions"][0]["job_id"])

    def test_rejected_sbatch_does_not_record_a_submission(self):
        with tempfile.TemporaryDirectory() as temporary:
            temp = Path(temporary)
            fake_bin = temp / "bin"
            fake_bin.mkdir()
            sbatch = fake_bin / "sbatch"
            sbatch.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
            sbatch.chmod(sbatch.stat().st_mode | stat.S_IXUSR)
            deploy = temp / "deploy"
            plan = temp / "plan.jsonl"
            plan.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "case_id": "case-1",
                        "campaign": "campaign",
                        "stage": "screen",
                        "cluster": "grace",
                        "backend": "perf",
                        "binary": "artifacts/grace/bench_profile",
                        "queue": "segqueue",
                        "scenario": "1p1c",
                        "repeat": 1,
                        "duration_secs": 1,
                        "warmup_secs": 0,
                        "collector_timeout_secs": 60,
                        "perf_events": ["cycles"],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            environment = dict(os.environ)
            environment.update(
                {
                    "PATH": f"{fake_bin}:{environment['PATH']}",
                    "UBQ": str(deploy),
                    "ACCOUNT": "test-account",
                    "USER": "tester",
                }
            )
            completed = subprocess.run(
                [str(ROOT / "slurm/submit_profile_plan.sh"), "grace", str(plan)],
                cwd=ROOT,
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(0, completed.returncode)
            self.assertFalse(
                (deploy / "profiling_results/raw/campaign/campaign.json").exists()
            )

    def test_submit_task_ceiling_rejects_before_sbatch(self):
        with tempfile.TemporaryDirectory() as temporary:
            temp = Path(temporary)
            fake_bin = temp / "bin"
            fake_bin.mkdir()
            marker = temp / "sbatch-called"
            sbatch = fake_bin / "sbatch"
            sbatch.write_text(
                f"#!/bin/sh\ntouch {marker}\nprintf '4242\\n'\n",
                encoding="utf-8",
            )
            sbatch.chmod(sbatch.stat().st_mode | stat.S_IXUSR)
            deploy = temp / "deploy"
            case = {
                "schema_version": 1,
                "campaign": "campaign",
                "stage": "screen",
                "cluster": "grace",
                "backend": "perf",
                "binary": "artifacts/grace/bench_profile",
                "queue": "segqueue",
                "scenario": "1p1c",
                "duration_secs": 1,
                "warmup_secs": 0,
                "collector_timeout_secs": 60,
                "perf_events": ["cycles"],
            }
            plan = temp / "plan.jsonl"
            plan.write_text(
                "".join(
                    json.dumps({**case, "case_id": f"case-{repeat}", "repeat": repeat}) + "\n"
                    for repeat in (1, 2)
                ),
                encoding="utf-8",
            )
            environment = dict(os.environ)
            environment.update(
                {
                    "PATH": f"{fake_bin}:{environment['PATH']}",
                    "UBQ": str(deploy),
                    "ACCOUNT": "test-account",
                    "USER": "tester",
                }
            )
            completed = subprocess.run(
                [
                    str(ROOT / "slurm/submit_profile_plan.sh"),
                    "grace",
                    str(plan),
                    "--max-submit-tasks",
                    "1",
                ],
                cwd=ROOT,
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(2, completed.returncode)
            self.assertIn("exceeding the explicit ceiling", completed.stderr)
            self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
