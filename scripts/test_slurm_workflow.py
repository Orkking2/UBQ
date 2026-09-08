import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SLURM_DIR = REPO_ROOT / "slurm"


class SlurmWorkflowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.deployed_root = Path(self.tmp.name) / "UBQ"
        self.fake_bin = Path(self.tmp.name) / "bin"
        self.sbatch_args = Path(self.tmp.name) / "sbatch-args.txt"
        (self.deployed_root / "manifests").mkdir(parents=True)
        self.fake_bin.mkdir()
        fake_sbatch = self.fake_bin / "sbatch"
        fake_sbatch.write_text(
            "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"$SBATCH_ARGS_FILE\"\n"
            "printf '4242\\n'\n",
            encoding="utf-8",
        )
        fake_sbatch.chmod(0o755)

        self.env = os.environ.copy()
        self.env.update(
            {
                "ACCOUNT": "test-account",
                "UBQ": str(self.deployed_root),
                "PATH": f"{self.fake_bin}{os.pathsep}{self.env['PATH']}",
                "SBATCH_ARGS_FILE": str(self.sbatch_args),
            }
        )

    def submit_args(self, script_name, *args):
        result = subprocess.run(
            [str(SLURM_DIR / script_name), *args],
            check=True,
            capture_output=True,
            text=True,
            env=self.env,
        )
        self.assertEqual("4242", result.stdout.strip())
        return self.sbatch_args.read_text(encoding="utf-8").splitlines()

    def assert_repo_root_follows_job_script(self, args, job_script):
        script_path = str(self.deployed_root / "slurm" / job_script)
        script_index = args.index(script_path)
        self.assertEqual(str(self.deployed_root), args[script_index + 1])
        self.assertNotIn(str(self.deployed_root / "src"), args)

    def test_build_submission_uses_cargo_workspace_root(self):
        args = self.submit_args("submit_build.sh", "mn5")
        self.assert_repo_root_follows_job_script(args, "build.sbatch")

    def test_grid_submission_uses_workspace_root(self):
        manifest = self.deployed_root / "manifests" / "test.txt"
        manifest.write_text("1p1c\n", encoding="utf-8")
        args = self.submit_args(
            "submit_bench_grid.sh",
            "mn5",
            "test-machine",
            str(manifest),
        )
        self.assertIn("--array=0-0", args)
        self.assertIn("--parsable", args)
        self.assert_repo_root_follows_job_script(args, "bench_grid_array.sbatch")

    def test_performix_submission_uses_workspace_root_and_accepts_lubq(self):
        args = self.submit_args(
            "submit_performix.sh",
            "grace",
            "lubq",
            "1p1c",
            "--batch-size",
            "256",
        )
        self.assert_repo_root_follows_job_script(args, "performix_grace.sbatch")
        self.assertIn("lubq", args)
        self.assertIn("256", args)

    def test_campaign_controller_depends_on_complete_frontier(self):
        args = self.submit_args(
            "submit_performix_campaign_controller.sh",
            "grace",
            "grace-auto-v1",
            "--after",
            "1234",
        )
        self.assertIn("--dependency=afterok:1234", args)
        self.assertIn("--cpus-per-task=1", args)
        self.assert_repo_root_follows_job_script(args, "performix_campaign_controller.sbatch")

    def test_grid_array_declares_and_passes_lubq(self):
        source = (SLURM_DIR / "bench_grid_array.sbatch").read_text(encoding="utf-8")
        match = re.search(r"^queue_set=([^\n]+)$", source, re.MULTILINE)
        self.assertIsNotNone(match)
        queues = match.group(1).split(",")
        self.assertIn("lubq", queues)
        self.assertIn('--queues "$queue_set"', source)
        self.assertIn("printf 'queues=%s\\n' \"$queue_set\"", source)


if __name__ == "__main__":
    unittest.main()
