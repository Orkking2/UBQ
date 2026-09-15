#!/usr/bin/env python3
"""Run the v8 correctness milestone with an external deadline for every case.

The reference control is standalone and cannot certify v8. No dependencies
beyond Python, rustc, and (for v8) the repository's existing Cargo dependencies.
"""
import argparse
import json
import math
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
# Queue modules that run the shared acceptance cases, by Cargo test target.
TARGETS = {"v8": "v8_viability"}
REQUIRED_CASES = {
    "cases::empty_and_zero_capacity_operations",
    "cases::scalar_fifo_across_empty_refill_cycles",
    "cases::batches_cross_virtual_and_physical_boundaries",
    "cases::seeded_operations_match_vecdeque",
    "cases::partial_drain_and_teardown_drop_each_owned_value_once",
    "cases::zero_sized_overaligned_and_send_only_values",
    "cases::flushing_preserves_live_values_and_allows_refill",
    "cases::concurrent_transfers_deliver_and_drop_every_value_once",
    "cases::concurrent_producers_preserve_per_producer_fifo",
}


def run_process(command, timeout, env=None):
    started = time.monotonic()
    try:
        # subprocess.run kills and reaps this process on timeout. Rust test
        # workers are threads in that process, so an internal spin cannot
        # strand a scope join or keep the next case from starting.
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=timeout, env=env)
        output = dict(status="passed" if result.returncode == 0 else "failed",
                      returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
    except subprocess.TimeoutExpired as exc:
        def decode(value):
            return value.decode(errors="replace") if isinstance(value, bytes) else value or ""
        output = dict(status="timed_out", returncode=None,
                      stdout=decode(exc.stdout), stderr=decode(exc.stderr))
    except OSError as exc:
        output = dict(status="failed", returncode=None, stdout="", stderr=str(exc))
    output.update(command=[str(arg) for arg in command], seconds=time.monotonic() - started)
    return output


def run_case(command, timeout):
    env = None
    if "--exact" in command:
        name = command[command.index("--exact") + 1]
        # Python already owns the deadline. Execute the workload in this child
        # instead of spawning the extra isolation worker used by plain Cargo.
        # Isolation's own fault-injection tests must still create their workers.
        if "::isolated::" not in name:
            env = dict(os.environ, UBQ_V8_TEST_ISOLATED=name)
    result = run_process(command, timeout, env=env)
    if result["status"] == "passed" and not re.search(
        r"test result: ok\. 1 passed; 0 failed; 0 ignored;", result["stdout"]
    ):
        result["status"] = "failed"
        result["reason"] = "Expected exactly one passing, non-ignored test"
    return result


def discover_cases(output):
    names = sorted(line.removesuffix(": test") for line in output.splitlines()
                   if line.endswith(": test"))
    missing = REQUIRED_CASES - set(names)
    if missing:
        raise ValueError("Missing required cases: " + ", ".join(sorted(missing)))
    return names


def positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return number


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", action="store_true", help="validate fixtures only, without building v8")
    parser.add_argument("--target", choices=sorted(TARGETS), default="v8",
                        help="queue module to run the cases against (default: v8)")
    parser.add_argument("--profile", choices=["debug", "release", "all"], default="all")
    parser.add_argument("--repeat", type=positive_int, default=3)
    parser.add_argument("--timeout", type=positive_float, default=20, help="seconds per case (default: 20)")
    parser.add_argument("--build-timeout", type=positive_float, default=120)
    parser.add_argument("--report", type=Path, help="JSON output (default: target/v8-viability/<target>.json)")
    args = parser.parse_args(argv)
    target = "reference" if args.reference else args.target
    test_target = TARGETS[args.target]
    profiles = ["debug", "release"] if args.profile == "all" else [args.profile]
    report = dict(schema_version=1, target=target, milestone="correctness_and_bounded_completion",
                  status="passed", profiles=profiles, repeat=args.repeat, timeout_seconds=args.timeout,
                  builds=[], discovery=[], cases=[])
    path = args.report or ROOT / "target" / "v8-viability" / f"{target}.json"
    with tempfile.TemporaryDirectory(prefix="ubq-v8-tests-") as temporary:
        for profile in profiles:
            binary = Path(temporary) / f"reference-{profile}"
            if args.reference:
                command = ["rustc", "--edition=2024", "--test", "tests/support/v8_reference.rs",
                           "-o", str(binary)]
                command += ["-O"] if profile == "release" else []
            else:
                command = ["cargo", "test", "--offline", "--no-default-features", "--features", "std",
                           "--test", test_target, "--no-run", "--message-format=json-render-diagnostics"]
                command += ["--release"] if profile == "release" else []
            print(f"BUILD {target} {profile}", flush=True)
            build = run_process(command, args.build_timeout)
            report["builds"].append(dict(profile=profile, **build))
            if build["status"] != "passed":
                report["status"] = "build_failed"
                print(build["stderr"] or build["stdout"], flush=True)
                break
            if not args.reference:
                binaries = set()
                for line in build["stdout"].splitlines():
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if (event.get("reason") == "compiler-artifact"
                            and event.get("target", {}).get("name") == test_target
                            and event.get("profile", {}).get("test")
                            and event.get("executable")):
                        binaries.add(event["executable"])
                if len(binaries) != 1:
                    report.update(status="build_failed", reason=f"Expected one {test_target} test executable")
                    break
                binary = Path(binaries.pop())
            discovery = run_process([str(binary), "--list", "--format", "terse"], args.timeout)
            report["discovery"].append(dict(profile=profile, **discovery))
            try:
                if discovery["status"] != "passed":
                    raise ValueError("Test discovery failed")
                cases = discover_cases(discovery["stdout"])
            except ValueError as exc:
                report.update(status="failed", reason=str(exc))
                break
            for repeat in range(1, args.repeat + 1):
                for name in cases:
                    result = run_case([str(binary), "--exact", name, "--nocapture", "--test-threads=1"], args.timeout)
                    report["cases"].append(dict(profile=profile, repeat=repeat, name=name, **result))
                    print(f"{result['status'].upper()} {profile} {repeat}/{args.repeat} {name}", flush=True)
                    if result["status"] != "passed":
                        report["status"] = "failed"
                        print(result["stdout"] + result["stderr"], flush=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n")
    label = "REFERENCE CONTROL ONLY" if args.reference else f"{target.upper()} CORRECTNESS MILESTONE"
    print(f"{label}: {report['status']}; report: {path}")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
