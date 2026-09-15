"""Lab regressions that need no cluster: specs, statistics, parsers, Slurm
commands, and the deployment state machine driven by a fake scheduler.

    python3 -m unittest discover -s lab/tests

AB_E2E=1 also runs a real, seconds-long study on this machine.
"""
import math
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

LAB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB))

from ablab import analysis, collectors, deploy, scheduler, spec as specs  # noqa: E402
from ablab.common import read_json, t975, write_json  # noqa: E402

SPEC = {
    "name": "t",
    "arms": [{"name": "base", "ref": "HEAD"}, {"name": "cand", "ref": "WORKTREE"}],
    "throughput": {"scenarios": ["1p1c", "2p2c", "HpHc"], "batch_sizes": [None],
                   "replicates": 2, "pairs": 2, "run_seconds": 4},
    "memory": {"depths": []},
    "deep": {"winners": 1, "losers": 1, "collectors": ["counters"]},
}
CLUSTER = {"scheduler": "slurm", "cores": 8, "account": "a", "partition": "p", "qos": "q",
           "build_qos": "debug", "collectors": ["counters", "perf_mem"], "counter_groups": {"ipc": ["cycles:u"]}}


class FakeScheduler:
    def __init__(self):
        self.submitted, self.cancelled, self.slurm_states, self.found, self.queued = [], [], {}, {}, []
        self.next = 100

    def submit(self, name, args, *, minutes, log_dir, kind, array=None, dependency=None):
        self.next += 1
        self.submitted.append({"id": str(self.next), "name": name, "args": args, "kind": kind,
                               "array": array, "dependency": dependency})
        return str(self.next)

    def states(self, job_ids):
        return {job: self.slurm_states[job] for job in job_ids if job in self.slurm_states}

    def find(self, name):
        return self.found.get(name, [])

    def cancel(self, job_ids):
        self.cancelled.extend(job_ids)

    def ours(self):
        return self.queued


def make_study(root, spec=SPEC, study_id="t-0123abcd"):
    study_dir = Path(root) / "studies" / study_id
    merged = specs.validate(spec)
    arms = [{"name": a["name"], "ref": a["ref"], "commit": "x", "source": f"src{i}" * 8, "harness": "h",
             "features": [], "workload": specs.arm_workload(merged, a)} for i, a in enumerate(merged["arms"])]
    write_json(study_dir / "study.json", {"id": study_id, "spec": merged, "arms": arms,
                                          "config": {"clusters": {"fake": CLUSTER}}})
    return study_dir


def throughput_result(ratio, base_ops=1e8, pairs=2, verify_cand=True):
    runs = lambda ops: [{"pair": p, "slot": 0, "ops_per_sec": ops, "elapsed_ns": 3e9, "rounds": 10,
                         "round_items": 65536, "barrier_fraction": 0.01} for p in range(pairs)]
    cand = {"verify": {"ok": True}, "runs": runs(base_ops * ratio)}
    if not verify_cand:
        cand = {"verify": {"ok": False, "error": "integrity mismatch: expected 8 sent 8 consumed 9"}, "runs": []}
    return {"node": "n1", "schedule": {"round_items": 65536, "rounds": 10, "warmup_rounds": 2},
            "arms": {"base": {"verify": {"ok": True}, "runs": runs(base_ops)}, "cand": cand}}


def finish(dep, task_id, result, status="done"):
    entry = dep.load_state()["tasks"][task_id]
    directory = dep.attempt_dir(task_id, entry["attempts"])
    write_json(directory / "result.json", result)
    write_json(directory / "status.json", {"status": status, "error": "boom"})


def mark_built(dep):
    for key in dep.build_targets():
        write_json(dep.root / "builds" / dep.cluster / key / "build.json", {"sha256": "x"})


class SpecTests(unittest.TestCase):
    def test_tokens_scale_to_each_cluster_and_infeasible_shapes_are_skipped(self):
        spec = specs.validate({**SPEC, "throughput": {"scenarios": ["Np1c", "HpHc", "16p16c"], "batch_sizes": [None, 256],
                                                     "skewed_batch_sizes": []}})
        grace, skipped = specs.cells(spec, 144)
        self.assertEqual([c["cell"] for c in grace],
                         ["143p1c-scalar", "143p1c-b256", "72p72c-scalar", "72p72c-b256", "16p16c-scalar", "16p16c-b256"])
        laptop, skipped = specs.cells(spec, 16)
        self.assertEqual(skipped, ["16p16c"])
        self.assertIn("8p8c-b256", [c["cell"] for c in laptop])

    def test_batch_pairs_set_the_producer_and_consumer_sides_apart(self):
        spec = specs.validate({**SPEC, "throughput": {"scenarios": ["Np1c"], "skewed_batch_sizes": [],
                                                     "batch_sizes": [None, 1, [1, 256], [None, 256], [256, 1], 16, [16, 16], [4096, 16]]}})
        self.assertEqual(spec["throughput"]["batch_sizes"],
                         [(None, None), (None, 256), (256, None), (16, 16), (4096, 16)])
        cells, _ = specs.cells(spec, 144)
        self.assertEqual([c["cell"] for c in cells],
                         ["143p1c-scalar", "143p1c-b1x256", "143p1c-b256x1", "143p1c-b16", "143p1c-b4096x16"])
        self.assertEqual((cells[1]["producer_batch"], cells[1]["consumer_batch"]), (None, 256))
        for bad in ([0, 16], [16], [16, 16, 16], ["16", 16]):
            with self.assertRaises(specs.SpecError):
                specs.validate({**SPEC, "throughput": {"scenarios": ["1p1c"], "batch_sizes": [bad]}})
        study = {"arms": [{"name": "v8", "workload": {"queue": "ubq-v8-s4096"}}]}
        ranking = [{"cell": c["cell"], "scenario": "143p1c", "producer_batch": c["producer_batch"],
                    "consumer_batch": c["consumer_batch"], "arm": "v8", "family": "ubq-v8", "rss_median": 1.0,
                    "ops_median": ops} for c, ops in zip(cells, (1.0, 3.0, 1.5, 2.0, 6.0))]
        rows = analysis.batch_matrix(study, ranking)
        self.assertEqual([r["best_batch"] for r in rows], [(4096, 16)] * 5)
        # Rows come in batch order: scalar, then by the larger side.
        self.assertEqual([r["cell"] for r in rows],
                         ["143p1c-scalar", "143p1c-b16", "143p1c-b1x256", "143p1c-b256x1", "143p1c-b4096x16"])
        self.assertEqual([r["share_of_best"] for r in rows], [1 / 6, 1 / 3, 0.5, 0.25, 1.0])

    def test_skewed_batch_pairs_are_measured_only_where_the_sides_differ_and_face_the_lone_side(self):
        # The default matrix: three uneven pairs at every skewed shape, none at a balanced one.
        spec = specs.validate({**SPEC, "throughput": {"scenarios": ["Np1c", "1pNc", "16p4c", "4p16c", "HpHc", "1p1c"]}})
        self.assertEqual(spec["throughput"]["skewed_batch_sizes"], [(16, 256), (16, 4096), (256, 4096)])
        cells, _ = specs.cells(spec, 144)
        by_shape = {}
        for cell in cells:
            by_shape.setdefault(cell["scenario"], []).append(cell["cell"].split("-", 1)[1])
        matched = ["scalar", "b16", "b256", "b4096"]
        # Producer-heavy shapes push the smaller batch and pop the larger one.
        self.assertEqual(by_shape["143p1c"], matched + ["b16x256", "b16x4096", "b256x4096"])
        self.assertEqual(by_shape["16p4c"], matched + ["b16x256", "b16x4096", "b256x4096"])
        # Consumer-heavy shapes take the mirror image, so the lone producer pushes the larger batch.
        self.assertEqual(by_shape["1p143c"], matched + ["b256x16", "b4096x16", "b4096x256"])
        self.assertEqual(by_shape["4p16c"], matched + ["b256x16", "b4096x16", "b4096x256"])
        self.assertEqual(by_shape["72p72c"], matched)
        self.assertEqual(by_shape["1p1c"], matched)
        self.assertEqual(len(cells), 4 * 6 + 3 * 4)
        # A pair already in batch_sizes is measured once; matched pairs do not belong here.
        spec = specs.validate({**SPEC, "throughput": {"scenarios": ["1p4c"], "batch_sizes": [[256, 16]],
                                                     "skewed_batch_sizes": [[16, 256], [1, 4096]]}})
        cells, _ = specs.cells(spec, 144)
        self.assertEqual([c["cell"] for c in cells if c["kind"] == "throughput"], ["1p4c-b256x16", "1p4c-b4096x1"])
        for bad in ([[16, 16]], [256], [[1, None]], [[16, 256, 4096]], "16x256"):
            with self.assertRaises(specs.SpecError):
                specs.validate({**SPEC, "throughput": {"scenarios": ["2p1c"], "skewed_batch_sizes": bad}})
        # The laptop smoke test exercises one uneven pair at its skewed shape.
        quick = specs.validate(specs.quick(SPEC))
        cells, _ = specs.cells(quick, 16)
        self.assertEqual([c["cell"] for c in cells if "x" in c["cell"]], ["4p1c-b16x256"])

    def test_typos_bad_arms_and_mixed_fifo_classes_are_rejected(self):
        with self.assertRaisesRegex(specs.SpecError, "unknown throughput fields"):
            specs.validate({**SPEC, "throughput": {"replicate": 3}})
        with self.assertRaisesRegex(specs.SpecError, "at least two arms"):
            specs.validate({**SPEC, "arms": SPEC["arms"][:1]})
        classes = {"ubq-v8-s4096": "strict", "lubq": "relaxed"}
        mixed = {**SPEC, "arms": [SPEC["arms"][0], {"name": "cand", "ref": "HEAD", "workload": {"queue": "lubq"}}]}
        with self.assertRaisesRegex(specs.SpecError, "strict and relaxed"):
            specs.validate(mixed, classes.__getitem__)

    def test_study_id_ignores_where_it_runs_but_not_what_it_measures(self):
        spec = specs.validate(SPEC)
        same = specs.study_id({**spec, "clusters": ["grace"]}, {"a": 1})
        self.assertEqual(same, specs.study_id({**spec, "clusters": ["mn5"]}, {"a": 1}))
        self.assertNotEqual(same, specs.study_id(spec, {"a": 2}))


class AnalysisTests(unittest.TestCase):
    def test_t_quantiles_are_conservative_between_table_entries(self):
        self.assertEqual(t975(2), 4.303)
        self.assertEqual(t975(22), 2.086)

    def test_paired_effects_give_verdicts_and_flag_integrity(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Three replicates: with two, a 95% t interval uses t(1) = 12.7
            # and even a steady 10% effect reads as unclear.
            spec = {**SPEC, "throughput": {**SPEC["throughput"], "replicates": 3}}
            dep = deploy.Deployment.create(make_study(tmp, spec), "fake", tmp)
            fake = FakeScheduler()
            mark_built(dep)
            deploy.advance(dep, fake)
            jitter = {0: 1.0, 1: 1.02, 2: 0.99}
            for task in dep.plan()["tasks"]:
                ratio = {"1p1c-scalar": 1.10, "2p2c-scalar": 0.90}.get(task["cell"], 1.0) * jitter[task["replicate"]]
                finish(dep, task["id"], throughput_result(ratio, verify_cand=task["cell"] != "4p4c-scalar"))
            deploy.advance(dep, fake)  # Results count once advance has recorded them.
            rows = {row["cell"]: row for row in analysis.effects(dep.study, dep.path)}
        win, loss, bad = rows["1p1c-scalar"], rows["2p2c-scalar"], rows["4p4c-scalar"]
        self.assertAlmostEqual(win["effect"], 1.10 * (1.02 * 0.99) ** (1 / 3), places=6)
        self.assertEqual((win["verdict"], win["ci_basis"], win["pair_wins"]), ("win", "replicates", 6))
        self.assertEqual(loss["verdict"], "loss")
        self.assertEqual((bad["verdict"], bad["integrity"]), ("integrity", ["cand"]))
        selection = analysis.select(dep.study, list(rows.values()))
        self.assertEqual(sorted(entry["cell"] for entry in selection), ["1p1c-scalar", "2p2c-scalar"])


def multi_result(ops, pairs=2):
    """A throughput task result for any number of arms, each with `pairs` clean runs."""
    return {"node": "n1", "schedule": {"round_items": 65536, "rounds": 10, "warmup_rounds": 2},
            "arms": {name: {"verify": {"ok": True},
                            "runs": [{"pair": p, "slot": 0, "ops_per_sec": value, "elapsed_ns": 3e9, "rounds": 10,
                                      "round_items": 65536, "barrier_fraction": 0.01} for p in range(pairs)]}
                     for name, value in ops.items()}}


class FamilyTests(unittest.TestCase):
    GRID = {"name": "g", "grid": {"queues": ["ubq-v8", "ubq-example-fork"], "geometries": ["s4096-v32", "s256"]},
            "throughput": {"scenarios": ["1p1c"], "batch_sizes": [None]}, "memory": {"depths": []}}

    def test_grid_expands_to_one_arm_per_queue_and_geometry_with_families(self):
        spec = specs.validate(self.GRID)
        self.assertEqual([a["name"] for a in spec["arms"]],
                         ["v8-s4096-v32", "v8-s256", "example-fork-s4096-v32", "example-fork-s256"])
        self.assertEqual(spec["arms"][3]["workload"], {"queue": "ubq-example-fork-s256"})
        self.assertEqual([specs.arm_family(spec, a) for a in spec["arms"]],
                         ["ubq-v8", "ubq-v8", "ubq-example-fork", "ubq-example-fork"])
        explicit = specs.validate({**self.GRID, "arms": [{"name": "ubq", "ref": "HEAD", "workload": {"queue": "ubq"}}]})
        self.assertEqual(explicit["arms"][0]["name"], "ubq")
        self.assertEqual(specs.arm_family(explicit, explicit["arms"][0]), "ubq")
        for geometry in ("x", "s0", "-s64", "s64-v"):
            with self.assertRaisesRegex(specs.SpecError, "geometry"):
                specs.validate({**self.GRID, "grid": {"queues": ["ubq-v8"], "geometries": [geometry]}})
        self.assertEqual(specs.arm_family(spec, {"name": "n", "workload": {"queue": "ubq-v8-s64-v3"}}), "ubq-v8")
        self.assertEqual(specs.arm_family(spec, {"name": "n", "family": "mine", "workload": {"queue": "ubq"}}), "mine")

    def test_best_variation_per_family_drives_ranking_and_selection(self):
        spec = {"name": "f", "throughput": {"scenarios": ["1p1c", "2p2c", "4p1c"], "batch_sizes": [None],
                                            "skewed_batch_sizes": [], "replicates": 2, "pairs": 2, "run_seconds": 4},
                "memory": {"depths": []}, "deep": {"winners": 1, "losers": 1, "collectors": ["counters"]},
                "arms": [{"name": "base", "ref": "HEAD", "workload": {"queue": "ubq-v8-s4096-v32"}},
                         {"name": "v8-s256", "ref": "HEAD", "workload": {"queue": "ubq-v8-s256-v32"}},
                         {"name": "fork", "ref": "HEAD", "workload": {"queue": "ubq-example-fork-s4096-v32"}}]}
        ratios = {"1p1c-scalar": (1.2, 1.1), "2p2c-scalar": (0.9, 1.3), "4p1c-scalar": (1.0, 0.7)}
        with tempfile.TemporaryDirectory() as tmp:
            study_dir = make_study(tmp, spec)
            study = read_json(study_dir / "study.json")
            for arm in study["arms"]:
                arm["family"] = specs.arm_family(study["spec"], arm)
            write_json(study_dir / "study.json", study)
            dep = deploy.Deployment.create(study_dir, "fake", tmp)
            fake = FakeScheduler()
            mark_built(dep)
            deploy.advance(dep, fake)
            for task in dep.plan()["tasks"]:
                v8, fork = ratios[task["cell"]]
                finish(dep, task["id"], multi_result({"base": 1e8, "v8-s256": v8 * 1e8, "fork": fork * 1e8}))
            deploy.advance(dep, fake)
            rows = analysis.effects(dep.study, dep.path)
            self.assertEqual(analysis.families(dep.study), {"ubq-v8": ["base", "v8-s256"], "ubq-example-fork": ["fork"]})
            best = analysis.best_per_cell(dep.study, rows)
            self.assertEqual({f: {c: r["candidate"] for c, r in per.items()} for f, per in best.items()},
                             {"ubq-v8": {c: "v8-s256" for c in ratios}, "ubq-example-fork": {c: "fork" for c in ratios}})
            overall = {row["family"]: row for row in analysis.best_overall(dep.study, rows)}
            self.assertEqual(overall["ubq-v8"]["arm"], "v8-s256")
            self.assertAlmostEqual(overall["ubq-example-fork"]["geomean_effect"], (1.1 * 1.3 * 0.7) ** (1 / 3))
            self.assertEqual((overall["ubq-example-fork"]["wins"], overall["ubq-example-fork"]["losses"]), (2, 1))
            selection = {entry["cell"]: entry for entry in analysis.select(dep.study, rows)}
            self.assertEqual(sorted(selection), ["1p1c-scalar", "2p2c-scalar", "4p1c-scalar"])
            self.assertEqual(selection["2p2c-scalar"]["arms"], ["base", "v8-s256", "fork"])
            self.assertEqual(selection["1p1c-scalar"]["arms"], ["base", "v8-s256"])
            same = analysis.same_geometry(dep.study, rows)
            self.assertEqual([(r["family"], r["geometry"], r["cells"]) for r in same], [("ubq-example-fork", "s4096-v32", 3)])
            self.assertAlmostEqual(same[0]["geomean_ratio"], (1.1 * 1.3 * 0.7) ** (1 / 3))  # fork / base, base = 1.0
            self.assertEqual(same[0]["cells_ahead"], 2)
            ranking = analysis.ranking(dep.study, dep.path, rows)
            cell = [r for r in ranking if r["cell"] == "2p2c-scalar"]
            self.assertEqual([r["arm"] for r in cell], ["fork", "base", "v8-s256"])
            self.assertEqual([r["best_in_family"] for r in cell], [True, True, False])
            self.assertAlmostEqual(cell[2]["share_of_best"], 0.9 / 1.3)
            self.assertIsNone(cell[0]["rss_median"])  # Fixtures record no memory; real runs do.
            # The plots pick the same bests without needing a display.
            try:
                import matplotlib  # noqa: F401
            except ImportError:
                return
            from ablab import plot, report
            report.write(dep.study, study_dir, ["fake"])
            paths = plot.write(dep.study, study_dir, ["fake"])
            # Fixtures record no RSS, so there is no memory matrix; fork has one
            # geometry, so only v8 gets a per-family geometry figure.
            self.assertEqual(sorted(p.name for p in paths),
                             ["fake-cells.png", "fake-geometry-matrix.png", "fake-geometry-ubq-v8.png",
                              "fake-layout-matrix.png", "fake-scaling.png"])
            text = (study_dir / "report.md").read_text()
            self.assertIn("### Throughput by shape", text)
            medians = analysis.arm_medians(dep.study, dep.path)
            self.assertIn(("1p1c-scalar", "base"), medians)
            shape = {(r["cell"], r["family"]): r for r in analysis.shape_matrix(dep.study, medians)}
            self.assertEqual(shape[("2p2c-scalar", "ubq-v8")]["arm"], "base")
            self.assertTrue((study_dir / "csv" / "family_summary.csv").exists())
            self.assertIn("### Layouts at equal geometry", text)
            self.assertIn("### Best variation per family", text)
            self.assertIn("### Ranking per cell", text)
            self.assertIn("### Geometry matrix", text)
            self.assertTrue((study_dir / "csv" / "geometry_matrix.csv").exists())

    def test_backoff_is_a_variation_within_a_family(self):
        raw = {"name": "b", "arms": [
            {"name": "base", "ref": "HEAD", "workload": {"queue": "ubq-v8-s4096-v32", "backoff": "crossbeam"}},
            {"name": "base-yield", "ref": "HEAD", "workload": {"queue": "ubq-v8-s4096-v32", "backoff": "yield"}},
            {"name": "fork", "ref": "HEAD", "workload": {"queue": "ubq-example-fork-s4096-v32"}},
            {"name": "fork-yield", "ref": "HEAD", "workload": {"queue": "ubq-example-fork-s4096-v32", "backoff": "yield"}},
            {"name": "ubq", "ref": "HEAD", "workload": {"queue": "ubq"}}]}
        spec = specs.validate(raw)
        study = {"spec": spec, "arms": [{**arm, "workload": specs.arm_workload(spec, arm),
                                         "family": specs.arm_family(spec, arm)} for arm in spec["arms"]]}
        arms = {arm["name"]: arm for arm in study["arms"]}
        self.assertEqual(analysis.families(study),
                         {"ubq-v8": ["base", "base-yield"], "ubq-example-fork": ["fork", "fork-yield"], "ubq": ["ubq"]})
        self.assertEqual(analysis.backoffs(study), ["crossbeam", "yield"])
        self.assertEqual([analysis.variation_of(study, a) for a in study["arms"]],
                         ["s4096-v32, crossbeam", "s4096-v32, yield", "s4096-v32, crossbeam", "s4096-v32, yield",
                          "crossbeam"])
        # Equal geometry means equal backoff too: fork-yield is measured against base-yield.
        rows = [{"candidate": c, "cell": "1p1c-scalar", "producer_batch": None, "consumer_batch": None,
                 "effect": e, "verdict": "win"}
                for c, e in (("base-yield", 1.5), ("fork", 2.0), ("fork-yield", 0.6), ("ubq", 0.9))]
        same = {(r["arm"], r["reference_arm"]): r for r in analysis.same_geometry(study, rows)}
        self.assertEqual(set(same), {("fork", "base"), ("fork-yield", "base-yield")})
        self.assertAlmostEqual(same[("fork", "base")]["geomean_ratio"], 2.0)
        self.assertAlmostEqual(same[("fork-yield", "base-yield")]["geomean_ratio"], 0.6 / 1.5)
        self.assertEqual(same[("fork-yield", "base-yield")]["backoff"], "yield")
        from ablab import plot
        self.assertEqual(plot.series_label(study, "ubq-v8", arms["base-yield"]), "ubq-v8 (s4096-v32, yield)")
        self.assertEqual(plot.series_label(study, "ubq", arms["ubq"]), "ubq")
        self.assertEqual(plot.matrix_groups(study, ["ubq-v8", "ubq"]),
                         [("ubq-v8", "crossbeam"), ("ubq-v8", "yield"), ("ubq", "crossbeam")])
        self.assertEqual(plot.geometries(study, "ubq-v8", "yield"), {"base-yield": (4096, 32)})
        # Without a second backoff nothing is labelled by policy.
        plain = {"spec": spec, "arms": [arms["base"], arms["fork"]]}
        self.assertEqual(analysis.variation_of(plain, arms["fork"]), "s4096-v32")
        self.assertEqual(plot.matrix_groups(plain, ["ubq-v8"]), [("ubq-v8", None)])
        with self.assertRaisesRegex(specs.SpecError, "backoff"):
            specs.validate({**raw, "workload": {"queue": "ubq", "backoff": "spin"}})

    def test_scaling_rows_pair_a_skewed_batch_with_its_mirror_unless_a_shape_has_both(self):
        from ablab import plot

        def row(scenario, producer, consumer):
            return {"scenario": scenario, "producer_batch": producer, "consumer_batch": consumer}
        default = [row("1p1c", None, None), row("4p1c", None, None), row("4p1c", 16, 256),
                   row("1p4c", 256, 16), row("4p1c", 256, 256), row("1p4c", 256, 4096)]
        self.assertEqual(plot.batch_groups(default),
                         [[(None, None)], [(16, 256), (256, 16)], [(256, 256)], [(256, 4096)]])
        crossed = default + [row("1p1c", 16, 256), row("1p1c", 256, 16)]
        self.assertEqual(plot.batch_groups(crossed),
                         [[(None, None)], [(16, 256)], [(256, 16)], [(256, 256)], [(256, 4096)]])

    def test_scaling_figure_picks_each_familys_best_arm_per_batch_and_names_it_in_the_panel(self):
        from ablab import plot
        study = {"arms": [{"name": "base", "workload": {"queue": "ubq-v8-s4096-v32"}},
                          {"name": "small", "workload": {"queue": "ubq-v8-s256-v2"}},
                          {"name": "ubq", "workload": {"queue": "ubq"}}]}
        # Small VBlocks win at scalar, the default geometry at batch 4096; ubq has one arm.
        ops = {(None, "base"): 1.0, (None, "small"): 1.4, (None, "ubq"): 1.6,
               (4096, "base"): 30.0, (4096, "small"): 12.0, (4096, "ubq"): 8.0}
        rows = []
        for scenario in ("1p1c", "4p1c"):
            for batch in (None, 4096):
                for arm in study["arms"]:
                    rows.append({"cell": f"{scenario}-{batch or 'scalar'}", "scenario": scenario,
                                 "producer_batch": batch, "consumer_batch": batch, "arm": arm["name"],
                                 "family": analysis.family_of(arm), "ops_median": ops[(batch, arm["name"])] * 1e6,
                                 "rss_median": None})
        matrix = analysis.geometry_matrix(study, rows)
        self.assertEqual(plot.best_per_batch(matrix),
                         {((None, None), "ubq-v8"): "small", ((None, None), "ubq"): "ubq",
                          ((4096, 4096), "ubq-v8"): "base", ((4096, 4096), "ubq"): "ubq"})
        try:
            import matplotlib
        except ImportError:
            return
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        families = list(analysis.families(study))
        fig = plot.scaling_figure(plt, rows, study, matrix, families, plot.Palette(families), "t")
        # Rows are scalar then batch 4096; 1p1c lies on all three axes, so every panel is drawn.
        legends = [[t.get_text() for t in ax.get_legend().get_texts()] for ax in fig.axes]
        self.assertEqual(len(legends), 6)
        self.assertEqual(legends[0], ["ubq-v8 (s256-v2)", "ubq"])
        self.assertEqual(legends[3], ["ubq-v8 (s4096-v32)", "ubq"])
        self.assertEqual(fig.legends, [])
        plt.close(fig)
        # Families with one arm each share one legend below the panels instead.
        plain = {"arms": study["arms"][::2]}
        plain_rows = [r for r in rows if r["arm"] != "small"]
        fig = plot.scaling_figure(plt, plain_rows, plain, analysis.geometry_matrix(plain, plain_rows),
                                  families, plot.Palette(families), "t")
        self.assertTrue(all(ax.get_legend() is None for ax in fig.axes))
        self.assertEqual([t.get_text() for t in fig.legends[0].get_texts()], ["ubq-v8", "ubq"])
        plt.close(fig)

    def test_shape_matrix_picks_each_familys_fastest_arm_and_summarises_against_the_control_family(self):
        study = {"arms": [{"name": "ubq", "workload": {"queue": "ubq", "backoff": "crossbeam"}},
                          {"name": "ubq-yield", "workload": {"queue": "ubq", "backoff": "yield"}},
                          {"name": "v8", "workload": {"queue": "ubq-v8-s4096-v32"}},
                          {"name": "fork", "workload": {"queue": "ubq-example-fork-s64-v2048"}}]}
        medians = {}
        for cell, ops in {"1p1c-scalar": (1.0, 1.2, 0.5, 1.5), "2p2c-scalar": (1.0, 0.8, 2.0, 1.5)}.items():
            for arm, value in zip(("ubq", "ubq-yield", "v8", "fork"), ops):
                medians[(cell, arm)] = {"scenario": cell.split("-")[0], "producer_batch": None,
                                        "consumer_batch": None, "ops_median": value * 1e8, "rss_median": None}
        rows = analysis.shape_matrix(study, medians)
        first = {row["family"]: row for row in rows if row["cell"] == "1p1c-scalar"}
        self.assertEqual([row["family"] for row in rows if row["cell"] == "1p1c-scalar"],
                         ["ubq", "ubq-v8", "ubq-example-fork"])
        self.assertEqual((first["ubq"]["arm"], first["ubq"]["best_family"]), ("ubq-yield", "ubq-example-fork"))
        self.assertAlmostEqual(first["ubq"]["share_of_best"], 1.2 / 1.5)
        second = {row["family"]: row for row in rows if row["cell"] == "2p2c-scalar"}
        self.assertEqual((second["ubq"]["arm"], second["ubq"]["best_family"]), ("ubq", "ubq-v8"))
        summary = {row["family"]: row for row in analysis.family_summary(study, rows)}
        self.assertAlmostEqual(summary["ubq"]["ops_geomean"], math.sqrt(1.2 * 1.0) * 1e8)
        self.assertIsNone(summary["ubq"]["ratio_to_reference"])
        self.assertIsNone(summary["ubq"]["cells_ahead"])
        self.assertAlmostEqual(summary["ubq-example-fork"]["ratio_to_reference"], math.sqrt(1.5 / 1.2 * 1.5 / 1.0))
        self.assertEqual((summary["ubq-example-fork"]["cells_ahead"], summary["ubq-example-fork"]["cells_fastest"]), (2, 1))
        self.assertAlmostEqual(summary["ubq-v8"]["ratio_to_reference"], math.sqrt(0.5 / 1.2 * 2.0 / 1.0))
        self.assertEqual((summary["ubq-v8"]["cells_ahead"], summary["ubq-v8"]["cells_fastest"]), (1, 1))
        self.assertEqual(summary["ubq"]["arms"], ["ubq", "ubq-yield"])

    def test_geometry_matrix_is_measured_against_the_best_arm_not_the_control(self):
        study = {"arms": [{"name": "base", "workload": {"queue": "ubq-v8-s4096-v32"}},
                          {"name": "v8-s256", "workload": {"queue": "ubq-v8-s256-v32"}},
                          {"name": "fork", "workload": {"queue": "ubq-example-fork-s4096-v32"}},
                          {"name": "ubq", "workload": {"queue": "ubq"}}]}
        ranking = []
        for cell, ops in {"1p1c-scalar": (1.0, 1.2, 1.1, 0.5), "2p2c-scalar": (1.0, 0.9, 1.3, 0.5)}.items():
            for arm, value in zip(("base", "v8-s256", "fork", "ubq"), ops):
                ranking.append({"cell": cell, "producer_batch": None, "consumer_batch": None, "arm": arm,
                                "ops_median": value * 1e8, "rss_median": value * 2 ** 20})
        rows = {row["arm"]: row for row in analysis.geometry_matrix(study, ranking)}
        self.assertEqual(rows["fork"]["best_arm"], "fork")  # (1.1 * 1.3) beats (1.2 * 0.9) and 1.0.
        self.assertAlmostEqual(rows["fork"]["share_of_best"], 1.0)
        self.assertAlmostEqual(rows["base"]["share_of_best"], math.sqrt(1 / 1.1 * 1 / 1.3))
        self.assertAlmostEqual(rows["ubq"]["share_of_best"], math.sqrt(0.5 / 1.1 * 0.5 / 1.3))
        self.assertEqual((rows["v8-s256"]["slots_per_vblock"], rows["v8-s256"]["vblocks_per_block"]), (256, 32))
        self.assertEqual((rows["ubq"]["slots_per_vblock"], rows["ubq"]["family"]), (None, "ubq"))
        self.assertEqual(rows["v8-s256"]["rss_max"], 1.2 * 2 ** 20)
        self.assertAlmostEqual(rows["v8-s256"]["rss_geomean"], math.sqrt(1.2 * 0.9) * 2 ** 20)


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dep = deploy.Deployment.create(make_study(self.tmp.name), "fake", self.tmp.name)
        self.fake = FakeScheduler()

    def tearDown(self):
        self.tmp.cleanup()

    def test_pipeline_builds_measures_selects_and_deep_dives(self):
        dep, fake = self.dep, self.fake
        status = deploy.advance(dep, fake)
        self.assertEqual((status["phase"], fake.submitted[-1]["kind"]), ("building", "build"))
        self.assertEqual(deploy.advance(dep, fake)["phase"], "building")  # No duplicate build.
        self.assertEqual(len(fake.submitted), 1)
        mark_built(dep)
        fake.slurm_states["101"] = "COMPLETED"
        status = deploy.advance(dep, fake)
        array, watchdog = fake.submitted[1], fake.submitted[2]
        # 1p1c, 2p2c and HpHc (4p4c on 8 cores), two replicates each.
        self.assertEqual((array["kind"], array["array"]), ("task", 6))
        self.assertEqual((watchdog["kind"], watchdog["dependency"]), ("control", array["id"]))
        self.assertEqual(status["counts"]["stage1"], {"pending": 6})
        for task in dep.plan()["tasks"]:
            finish(dep, task["id"], throughput_result({"1p1c-scalar": 1.2}.get(task["cell"], 0.8)))
        status = deploy.advance(dep, fake)
        self.assertEqual(status["phase"], "deep")
        deep = [t for t in dep.plan()["tasks"] if t["kind"] == "deep"]
        self.assertEqual(sorted(t["id"] for t in deep), ["s2-1p1c-scalar-counters-r0", "s2-2p2c-scalar-counters-r0"])
        # Stage 1's schedule, its 10 rounds scaled from 4 s runs to the 20 s deep default.
        self.assertEqual(deep[0]["schedule"], {"round_items": 65536, "rounds": 50, "warmup_rounds": 2})
        for task in deep:
            finish(dep, task["id"], {"collector": "counters", "arms": {}})
        self.assertEqual(deploy.advance(dep, fake)["phase"], "done")

    def test_a_crashed_task_is_retried_once_then_failed(self):
        dep, fake = self.dep, self.fake
        mark_built(dep)
        deploy.advance(dep, fake)
        task_id, entry = next(iter(dep.load_state()["tasks"].items()))
        fake.slurm_states[entry["job"]] = "NODE_FAIL"
        deploy.advance(dep, fake)
        entry = dep.load_state()["tasks"][task_id]
        self.assertEqual((entry["status"], entry["attempts"]), ("pending", 2))
        self.assertIn("NODE_FAIL", entry["last_error"])
        fake.slurm_states[entry["job"]] = "OUT_OF_MEMORY"
        deploy.advance(dep, fake)
        self.assertEqual(dep.load_state()["tasks"][task_id]["status"], "failed")

    def test_an_interrupted_submission_is_adopted_rather_than_repeated(self):
        dep, fake = self.dep, self.fake
        mark_built(dep)
        state = dep.load_state()
        state["pending"] = {"name": "ab.0123abcd.s1.dead00", "tasks": list(state["tasks"])}
        dep.save_state(state)
        fake.found["ab.0123abcd.s1.dead00"] = ["777", "778"]
        deploy.advance(dep, fake)
        state = dep.load_state()
        self.assertEqual({e["job"].split("_")[0] for e in state["tasks"].values()}, {"777"})
        self.assertEqual(fake.cancelled, ["778"])
        self.assertFalse([s for s in fake.submitted if s["kind"] == "task"])

    def test_a_new_study_supersedes_older_ones_on_its_cluster_only(self):
        root = Path(self.tmp.name)
        self.fake.queued = [("555", "ab.feedf00d.s1.aaaaaa"), ("556", "ab.99999999.s2.bbbbbb")]
        mark_built(self.dep)
        deploy.advance(self.dep, self.fake)
        old_jobs = self.dep.load_state()["jobs"]
        other_cluster = make_study(root, study_id="other-11111111")
        write_json(other_cluster / "mn5" / "deploy.json", {})
        write_json(other_cluster / "mn5" / "state.json", {"phase": "measuring"})
        newer = deploy.Deployment.create(make_study(root, study_id="t-99999999"), "fake", root)
        cancelled = deploy.supersede_others(newer, self.fake)
        self.assertEqual(self.dep.load_state()["phase"], "superseded")
        self.assertTrue(set(old_jobs) <= set(self.fake.cancelled))
        self.assertIn("555", self.fake.cancelled)
        self.assertNotIn("556", self.fake.cancelled)
        self.assertEqual(read_json(other_cluster / "mn5" / "state.json")["phase"], "measuring")
        self.assertIn("t-0123abcd", cancelled)
        self.assertEqual(deploy.advance(self.dep, self.fake)["phase"], "superseded")

    def test_task_arrays_are_chunked_to_the_cluster_array_limit(self):
        study = make_study(self.tmp.name, study_id="t-chunked")
        deploy_cfg = read_json(study / "study.json")
        deploy_cfg["config"]["clusters"]["fake"] = {**CLUSTER, "array_limit": 4}
        write_json(study / "study.json", deploy_cfg)
        dep = deploy.Deployment.create(study, "fake", self.tmp.name)
        mark_built(dep)
        deploy.advance(dep, self.fake)
        arrays = [job["array"] for job in self.fake.submitted if job["kind"] == "task"]
        self.assertEqual(arrays, [4, 2])  # 3 shapes x 2 replicates in pieces of at most 4.

    def test_requested_deep_cells_reopen_a_finished_deployment(self):
        dep, fake = self.dep, self.fake
        mark_built(dep)
        deploy.advance(dep, fake)
        for task in dep.plan()["tasks"]:
            finish(dep, task["id"], throughput_result(1.0))
        self.assertEqual(deploy.advance(dep, fake)["phase"], "done")  # No effect, nothing selected.
        status = deploy.request_deep(dep, ["4p4c-scalar"], ["perf_mem"], scheduler=fake)
        self.assertEqual(status["phase"], "deep")
        self.assertIn("s2-4p4c-scalar-perf_mem-r0", {t["id"] for t in dep.plan()["tasks"]})
        # Requesting the same cell again captures it again as the next replicate.
        deploy.request_deep(dep, ["4p4c-scalar"], ["perf_mem"], scheduler=fake)
        self.assertIn("s2-4p4c-scalar-perf_mem-r1", {t["id"] for t in dep.plan()["tasks"]})
        self.assertNotIn("s2-4p4c-scalar-perf_mem-r2", {t["id"] for t in dep.plan()["tasks"]})


class PerformixTests(unittest.TestCase):
    def test_import_reply_forms_yield_the_run_id(self):
        from ablab import performix
        self.assertEqual(performix.find_run_id('{"data":{"return_code":0,"new_id":{"value":"bae28537a9b0"}}}'), "bae28537a9b0")
        self.assertEqual(performix.find_run_id('{"run_id": "abc"}'), "abc")
        self.assertEqual(performix.find_run_id("Imported run. Run ID: 0123abcd"), "0123abcd")
        self.assertEqual(performix.find_run_id("nothing here"), "")


class SchedulerTests(unittest.TestCase):
    def test_array_ids_expand_like_sacct_prints_them(self):
        self.assertEqual(scheduler.expand_array_ids("12_[0-2,5%4]"), ["12_0", "12_1", "12_2", "12_5"])
        self.assertEqual(scheduler.expand_array_ids("12_3"), ["12_3"])

    def test_task_arrays_ask_for_exclusive_pinned_nodes_and_quota_means_retry(self):
        slurm = scheduler.Slurm({**CLUSTER, "exclusive_mem0": True, "constraint": "perfparanoid",
                                 "task_modules": ["apx/1"]}, "/study")
        reply = subprocess.CompletedProcess([], 0, "4242;cluster\n", "")
        with mock.patch.object(scheduler, "run", return_value=reply) as call:
            self.assertEqual(slurm.submit("ab.x.s1.t", ["task", "d", "m"], minutes=12, log_dir="/l",
                                          kind="task", array=3), "4242")
        command = call.call_args[0][0]
        for flag in ("--exclusive", "--cpus-per-task=8", "--mem=0", "--constraint=perfparanoid",
                     "--array=0-2", "--time=12", "--qos=q", "--export=ALL,AB_MODULES=apx/1"):
            self.assertIn(flag, command)
        quota = subprocess.CompletedProcess([], 1, "", "sbatch: error: QOSMaxSubmitJobPerUserLimit")
        with mock.patch.object(scheduler, "run", return_value=quota):
            self.assertIsNone(slurm.submit("n", [], minutes=1, log_dir="/l", kind="task"))

    def test_a_rejected_debug_qos_falls_back_for_the_build(self):
        slurm = scheduler.Slurm(CLUSTER, "/study")
        replies = [subprocess.CompletedProcess([], 1, "", "Invalid qos specification"),
                   subprocess.CompletedProcess([], 0, "77\n", "")]
        with mock.patch.object(scheduler, "run", side_effect=replies) as call:
            self.assertEqual(slurm.submit("b", [], minutes=60, log_dir="/l", kind="build"), "77")
        self.assertIn("--qos=q", call.call_args[0][0])


class CollectorParserTests(unittest.TestCase):
    def test_perf_stat_rejects_unsupported_and_multiplexed_counters(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "perf.csv"
            path.write_text("# started\n1000;;cycles;100;100.00;;\n500;;instructions:u;100;100.00;;\n")
            found = collectors.parse_perf_stat(path, ["cycles:u", "instructions:u"])
            self.assertEqual(found["instructions:u"]["value"], 500)
            path.write_text("1000;;cycles;100;50.00;;\n")
            with self.assertRaisesRegex(collectors.CollectorError, "50%"):
                collectors.parse_perf_stat(path, ["cycles:u"])
            path.write_text("<not supported>;;stalled-cycles-frontend;0;0;;\n")
            with self.assertRaisesRegex(collectors.CollectorError, "not supported"):
                collectors.parse_perf_stat(path, ["stalled-cycles-frontend:u"])

    def test_report_tables_and_c2c_statistics_parse(self):
        report = "# Overhead Samples Symbol\n    41.50%   1234  [.] ubq::v8::queue::UBQ<T>::pop_batch\n     2.00%  60  [k] do_idle\n"
        self.assertEqual(collectors.parse_overhead_table(report),
                         [[41.5, 1234, "ubq::v8::queue::UBQ<T>::pop_batch"], [2.0, 60, "do_idle"]])
        stats = collectors.parse_c2c_stats("  Total records   :   329219\n  LLC Misses to Remote cache (HITM) :  56.1%\n")
        self.assertEqual(stats, {"Total records": 329219.0, "LLC Misses to Remote cache (HITM)": 56.1})

    def test_counter_groups_flatten_to_per_handoff_values_and_ratios(self):
        metrics = {"groups": {"ipc": {"handoffs": 100, "events": {"cycles:u": {"value": 500.0},
                                                                  "instructions:u": {"value": 1000.0}}}}}
        flat = analysis.flatten("counters", metrics)
        self.assertEqual((flat["cycles per handoff"], flat["ipc"]), (5.0, 2.0))


@unittest.skipUnless(os.environ.get("AB_E2E"), "set AB_E2E=1 to build and run a real local study")
class LocalEndToEnd(unittest.TestCase):
    def test_quick_local_study_completes_with_effects(self):
        repo = LAB.parent
        result = subprocess.run([str(repo / "ab"), "launch", "v8-geometry", "--local", "--quick", "--again"],
                                cwd=repo, capture_output=True, text=True, timeout=3600)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("report:", result.stdout)


if __name__ == "__main__":
    unittest.main()
