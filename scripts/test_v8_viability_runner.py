"""Fault-injection checks for the acceptance runner; independent of libubq."""
import sys
import time
import unittest

import test_v8_viability as runner


class RunnerTests(unittest.TestCase):
    def run_child(self, source, timeout=2):
        return runner.run_case([sys.executable, "-c", source], timeout)

    def test_exit_zero_without_executing_a_test_is_failure(self):
        for source in ["pass", "print('test result: ok. 0 passed; 0 failed; 1 ignored;')"]:
            self.assertEqual(self.run_child(source)["status"], "failed")

    def test_success_requires_one_executed_test(self):
        result = self.run_child("print('test result: ok. 1 passed; 0 failed; 0 ignored;')")
        self.assertEqual(result["status"], "passed")

    def test_nonzero_exit_overrides_success_text(self):
        result = self.run_child(
            "print('test result: ok. 1 passed; 0 failed; 0 ignored;'); raise SystemExit(7)"
        )
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["returncode"], 7)

    def test_stuck_call_is_killed_and_next_case_runs(self):
        started = time.monotonic()
        result = self.run_child("import time; print('entered', flush=True); time.sleep(60)", 0.2)
        self.assertEqual(result["status"], "timed_out")
        self.assertIn("entered", result["stdout"])
        self.assertLess(time.monotonic() - started, 3)
        self.test_success_requires_one_executed_test()

    def test_crash_is_failure(self):
        result = self.run_child("import os, signal; os.kill(os.getpid(), signal.SIGKILL)")
        self.assertEqual(result["status"], "failed")

    def test_missing_required_case_cannot_turn_green(self):
        complete = [f"{name}: test" for name in runner.REQUIRED_CASES]
        self.assertEqual(runner.discover_cases("\n".join(complete)), sorted(runner.REQUIRED_CASES))
        with self.assertRaises(ValueError):
            runner.discover_cases("\n".join(complete[:-1]))
        with self.assertRaises(ValueError):
            runner.discover_cases("")


if __name__ == "__main__":
    unittest.main()
