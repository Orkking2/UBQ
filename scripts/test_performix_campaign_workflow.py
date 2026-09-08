import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from scripts.performix_campaign_workflow import (
    WorkflowError,
    auto_workflow,
    merge_plans,
    plan_sha256,
    resolved_campaign,
    save_state,
    source_fingerprint,
    start_workflow,
    submit_workflow,
)


class PerformixCampaignWorkflowTests(unittest.TestCase):
    def test_start_records_build_and_dependent_frontier_jobs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for directory in ("src", "scripts", "slurm", "profiling", "manifests"):
                (root / directory).mkdir()
            (root / "Cargo.toml").write_text("[package]\nname='x'\n", encoding="utf-8")
            (root / "profiling" / "campaign.contention-performix.json").write_text(
                json.dumps(
                    {
                        "campaign": "base",
                        "description": "test",
                        "variants": [{"name": "candidate", "factors": {}}],
                    }
                ),
                encoding="utf-8",
            )
            for script in ("submit_build.sh", "submit_contention_sweep.sh"):
                (root / "slurm" / script).write_text("", encoding="utf-8")
            args = SimpleNamespace(cluster="grace", machine_label="grace-new", resume=False)
            with mock.patch(
                "scripts.performix_campaign_workflow.submit_command",
                side_effect=("41", "42"),
            ) as submit, redirect_stdout(io.StringIO()):
                self.assertEqual(0, start_workflow(args, root))

            commands = [call.args[0] for call in submit.call_args_list]
            self.assertEqual("41", commands[1][commands[1].index("--after") + 1])
            state = json.loads(
                (root / "profiling_results/workflows/grace-new/workflow.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(["41"], state["jobs"]["build"])
            self.assertEqual(["42"], state["jobs"]["frontier"])
            self.assertEqual("base-grace-new", state["campaign"])

    def test_auto_adds_controller_after_the_full_frontier(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for directory in ("src", "scripts", "slurm", "profiling", "manifests"):
                (root / directory).mkdir()
            (root / "Cargo.toml").write_text("[package]\nname='x'\n", encoding="utf-8")
            (root / "profiling" / "campaign.contention-performix.json").write_text(
                json.dumps(
                    {
                        "campaign": "base",
                        "description": "test",
                        "variants": [{"name": "candidate", "factors": {}}],
                    }
                ),
                encoding="utf-8",
            )
            for script in (
                "submit_build.sh",
                "submit_contention_sweep.sh",
                "submit_performix_campaign_controller.sh",
            ):
                (root / "slurm" / script).write_text("", encoding="utf-8")
            args = SimpleNamespace(
                cluster="grace",
                machine_label="grace-auto",
                resume=False,
                min_evidence_repeats=3,
                plan_repeats=1,
                cliffs_per_shape=1,
                score_queue=[],
                tuning_queue=[],
                static_control_queue=[],
                no_static_controls=False,
                max_cases=750,
                max_concurrent=1,
                chunk_size=300,
                max_submit_tasks=300,
                no_hardware=False,
            )
            with mock.patch(
                "scripts.performix_campaign_workflow.submit_command",
                side_effect=("51", "52", "53"),
            ) as submit, redirect_stdout(io.StringIO()):
                self.assertEqual(0, auto_workflow(args, root))

            controller = submit.call_args_list[2].args[0]
            self.assertEqual("52", controller[controller.index("--after") + 1])
            state = json.loads(
                (root / "profiling_results/workflows/grace-auto/workflow.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(["53"], state["jobs"]["controllers"])
            self.assertTrue(state["automation"]["include_hardware"])

    def test_source_fingerprint_tracks_executable_inputs_but_not_results(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            (root / "scripts").mkdir()
            (root / "profiling_results").mkdir()
            (root / "Cargo.toml").write_text("[package]\nname='x'\n", encoding="utf-8")
            source = root / "src" / "lib.rs"
            source.write_text("pub fn value() -> u8 { 1 }\n", encoding="utf-8")
            first = source_fingerprint(root)

            (root / "profiling_results" / "result.json").write_text("{}", encoding="utf-8")
            self.assertEqual(first, source_fingerprint(root))

            source.write_text("pub fn value() -> u8 { 2 }\n", encoding="utf-8")
            self.assertNotEqual(first, source_fingerprint(root))

    def test_resolved_campaign_names_and_fingerprints_the_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "profiling").mkdir()
            source = root / "profiling" / "campaign.contention-performix.json"
            source.write_text(
                json.dumps(
                    {
                        "campaign": "base",
                        "description": "test",
                        "variants": [{"name": "candidate", "factors": {}}],
                    }
                ),
                encoding="utf-8",
            )
            destination = root / "generated" / "campaign.json"
            campaign = resolved_campaign(root, "grace-bulk-v2", "abc123", destination)
            value = json.loads(destination.read_text(encoding="utf-8"))
            self.assertEqual("base-grace-bulk-v2", campaign)
            self.assertEqual(campaign, value["campaign"])
            self.assertEqual("abc123", value["source_sha256"])
            self.assertEqual(
                "grace-bulk-v2", value["variants"][0]["factors"]["frontier_machine_label"]
            )

    def test_merge_prefers_tuning_metadata_and_deduplicates_case_ids(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            tuning = root / "tuning.jsonl"
            winners = root / "winners.jsonl"
            output = root / "selected.jsonl"
            tuning.write_text(
                json.dumps({"case_id": "same", "comparison_role": "ab_contrast"}) + "\n",
                encoding="utf-8",
            )
            winners.write_text(
                "".join(
                    (
                        json.dumps({"case_id": "same", "comparison_role": "winner"}) + "\n",
                        json.dumps({"case_id": "other", "comparison_role": "winner"}) + "\n",
                    )
                ),
                encoding="utf-8",
            )
            merge_plans((tuning, winners), output, 2)
            rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(["same", "other"], [row["case_id"] for row in rows])
            self.assertEqual("ab_contrast", rows[0]["comparison_role"])

            with self.assertRaises(WorkflowError):
                merge_plans((tuning, winners), output, 1)

    def test_submit_freezes_and_serially_chains_bounded_plan_chunks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            (root / "scripts").mkdir()
            (root / "slurm").mkdir()
            (root / "Cargo.toml").write_text("[package]\nname='x'\n", encoding="utf-8")
            (root / "slurm" / "submit_profile_plan.sh").write_text("", encoding="utf-8")
            label = "grace-test"
            directory = root / "profiling_results" / "workflows" / label
            directory.mkdir(parents=True)
            plan = directory / "hotspots-selected.jsonl"
            plan.write_text(
                "".join(json.dumps({"case_id": f"case-{index}"}) + "\n" for index in range(5)),
                encoding="utf-8",
            )
            fingerprint = source_fingerprint(root)
            state = {
                "schema_version": 1,
                "cluster": "grace",
                "machine_label": label,
                "source_sha256": fingerprint,
                "selection_source_sha256": fingerprint,
                "results_root": str(root / "profiling_results" / "raw" / label),
                "plans": {
                    "hotspots": {
                        "path": str(plan),
                        "cases": 5,
                        "sha256": plan_sha256(plan),
                    }
                },
                "jobs": {"build": [], "frontier": [], "profiles": []},
            }
            save_state(root, state)
            args = SimpleNamespace(
                cluster="grace",
                machine_label=label,
                kind="hotspots",
                after=None,
                max_concurrent=1,
                max_submit_tasks=2,
                chunk_size=2,
                resubmit=False,
            )
            with mock.patch(
                "scripts.performix_campaign_workflow.submit_command",
                side_effect=("101", "102", "103"),
            ) as submit, redirect_stdout(io.StringIO()):
                self.assertEqual(0, submit_workflow(args, root))

            commands = [call.args[0] for call in submit.call_args_list]
            self.assertNotIn("--after", commands[0])
            self.assertEqual("101", commands[1][commands[1].index("--after") + 1])
            self.assertEqual("102", commands[2][commands[2].index("--after") + 1])
            submitted = sorted((directory / "submitted").glob("*.jsonl"))
            self.assertEqual([2, 2, 1], [len(path.read_text().splitlines()) for path in submitted])
            recorded = json.loads((directory / "workflow.json").read_text(encoding="utf-8"))
            self.assertEqual(3, len(recorded["jobs"]["profiles"]))


if __name__ == "__main__":
    unittest.main()
