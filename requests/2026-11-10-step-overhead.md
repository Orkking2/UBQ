# Where the time goes between steps (2026-11-10)

> **When to reply.** Message Guillem **once**, for Nicolas, when this request is complete (step 5), or at once if a command of the request itself fails. Send no progress messages before that.

Read `README.md` in this directory first. This request is short: a diagnosis on a few debug nodes, not a benchmark. It also ends the request of 2026-10-30.

## What this is

Guillem's note of 2026-11-09 reported that every step of the request of 2026-10-30 costs 30 to 90 s, however small the benchmark is. Most of the `ubq`-family runs and the mutex time out because of it. The time sits outside the region the benchmark measures. In one Grace task, a verify step measured 21 ms but took 34 s, with about a minute's gap before it.

Nicolas asks your agent to find where that time goes, say why, and recommend a fix. You are closer to the machines than he is. Nothing here changes the earlier runs' `tool/`, `sources/`, `builds/` or `run.json`.

What is known so far:
- **The same tool was fast before.** The earlier request's tool and binary ran `ubq-dynblock`'s verify step on MareNostrum 5 in 0.2 s on 2026-11-03, and in 48 to 72 s from 2026-11-04 (Guillem's note). In September, the same lab's overhead outside the measured region had a median of 0.2 to 0.6 s on 300 MareNostrum 5 nodes and on Grace. Slow spells like this one happened on MareNostrum 5 on 2026-10-22 and 23, and on Grace on 2026-11-03.
- **What a step does.** The lab's Python, on the compute node:
  - writes `workload.json` into the step's directory on GPFS, as a temporary file renamed into place;
  - starts the runner binary from `$ROOT/builds/<cluster>/…/bench_hardware`, also on GPFS, with its stdout and stderr going to files in that directory;
  - writes `benchmark.json` and rewrites the task's `result.json` the same way.

  That is about five file creations and renames per step, all on `/gpfs/projects`, while about twenty deployments do the same on hundreds of nodes.
- **What a task does first.** It reads each arm's binary whole from GPFS, to check it against its build's hash, once per arm: up to 20 times per task (v8's arms), all of the same binary. The binaries carry full debug information for the profiler. A build with the same flags on x86 is 246 MB, of which 15 MB is code; `ls -la` in step 1 shows the size here.
- **What Guillem ruled out:** the node (each job is exclusive), the project quota, and slow small writes from a login node (20 writes took 0.18 s).
- **What the next bundle's tool will do differently** (not yet published):
  - it copies the binary to the node's own disk (`$TMPDIR`) and runs it from there;
  - it keeps a task's files on the node and copies them to GPFS once, at the task's end;
  - a task that died at its time limit gets three times the limit on its next attempt.

  Your recommendation should say whether that would remove the cost, or what else is needed.

Nicolas suspects the lab itself, since the earlier rounds were clean. The facts above point at the filesystem, or at the load the lab puts on it. This request is built to tell those apart.

## The budget

Two to six one-node jobs per cluster in the debug QOS, each at most one hour: at most 12 node-hours in all. If the debug QOS refuses an exclusive one-hour job, use the measurement QOS (`ngp_bsccs` on Grace, `gp_bsccs` on MareNostrum 5) with the same options.

## 0. The earlier request: its state now

On `grace1.bsc.es`, then on `glogin1.bsc.es` with `C=mn5`. This reads each run's `state.json` and changes nothing:

```sh
export ROOT=/gpfs/projects/bsc18/$USER/ubq-lab   # the earlier request's directory
C=grace   # on glogin1.bsc.es: C=mn5
cd $ROOT/runs && python3 - $C <<'PY'
import json, os, sys
from collections import Counter
cluster = sys.argv[1]
for run in sorted(os.listdir(".")):
    path = f"{run}/{cluster}/state.json"
    if os.path.exists(path):
        state = json.load(open(path))
        counts = dict(Counter(t.get("status") for t in state.get("tasks", {}).values()))
        print(run, cluster, state.get("phase"), counts)
PY
squeue -u $USER -h -o '%i %j %T %M %N' | head -50
squeue -u $USER -h | wc -l
```

Keep both outputs for the reply.

## 1. The first pass, with the earlier runs still going

Write the two scripts into a directory of your own, once (both login nodes see `$HOME`). `diagnose-step.sh` is bash and uses `strace` and GNU time (`/usr/bin/time`) where they exist, and skips them where they do not. `summarize.py` is Python 3.9, standard library only.

```sh
D=~/step-overhead-2026-11-10
mkdir -p $D
cat > $D/diagnose-step.sh <<'SH'
#!/bin/bash
# Where does a step's time go outside the measured region? One pass, inside an
# exclusive Slurm job. Standard tools only; nothing of the earlier runs is changed.
#
#   diagnose-step.sh OUT STEP BIN ROOT
#     OUT   a new directory under $HOME for this pass's outputs
#     STEP  one step directory of the earlier request: .../tasks/<task>/a<n>/verify-<arm>
#     BIN   the runner binary that step ran
#     ROOT  the earlier request's directory on GPFS (scratch files go in ROOT/diag-<job>)
set -u
OUT=$1 STEP=$2 BIN=$3 ROOT=$4
JOB=${SLURM_JOB_ID:-$$}
LOCAL=${TMPDIR:-/tmp}/ubq-diag-$JOB
SHARED=$ROOT/diag-$JOB
mkdir -p "$OUT" "$LOCAL" "$SHARED"
T=$OUT/timeline.tsv
mark() { printf '%s\t%s\n' "$(date +%s.%N)" "$*" >> "$T"; }
have() { command -v "$1" > /dev/null 2>&1; }
timed() { local out=$1; shift; if [ -x /usr/bin/time ]; then /usr/bin/time -v -o "$out" "$@"; else "$@"; fi; }

# The node, the filesystems, and our own load on the cluster at this moment.
{
  hostname; date -u; uname -r; nproc; free -g
  df -h "$LOCAL" "$SHARED"
  mount | grep -E 'gpfs|tmp'
  if have squeue; then
    echo "our running jobs: $(squeue -u "$USER" -h -t R | wc -l)"
    echo "our pending jobs: $(squeue -u "$USER" -h -t PD | wc -l)"
  fi
  have strace && strace -V | head -1
  [ -x /usr/bin/time ] && echo "GNU time: present"
} > "$OUT/node.txt" 2>&1
sha256sum "$BIN" > "$OUT/binary.sha256"
cp "$(dirname "$BIN")/build.json" "$OUT/build.json" 2> /dev/null
cp "$STEP/workload.json" "$OUT/workload.json"
mark "start"

# A. Small files: 20 create-and-rename pairs, the kind of bookkeeping a step does,
#    on GPFS and on the node's own disk.
for place in shared local; do
  if [ $place = shared ]; then d=$SHARED/files; else d=$LOCAL/files; fi
  mkdir -p "$d"
  for i in 1 2 3; do
    mark "A files-$place $i start"
    for k in $(seq 20); do echo x > "$d/.f$k.tmp" && mv "$d/.f$k.tmp" "$d/f$k"; done
    mark "A files-$place $i end"
  done
done

# B. Starting a program: a copy of /bin/true on GPFS and on the node, and Python.
cp /bin/true "$SHARED/true"
cp /bin/true "$LOCAL/true"
for i in 1 2 3; do
  mark "B true-shared $i start"; "$SHARED/true"; mark "B true-shared $i end"
  mark "B true-local $i start"; "$LOCAL/true"; mark "B true-local $i end"
  mark "B python $i start"; python3 -c pass; mark "B python $i end"
done

# C. The step itself, three ways, three times each:
#    shared-shared  binary and files on GPFS, as the lab ran it;
#    shared-local   binary on GPFS, files on the node;
#    local-local    binary and files on the node.
cp "$BIN" "$LOCAL/bench_hardware"
for i in 1 2 3; do
  for mode in shared-shared shared-local local-local; do
    case $mode in
      shared-shared) b=$BIN; d=$SHARED/$mode-$i ;;
      shared-local)  b=$BIN; d=$LOCAL/$mode-$i ;;
      local-local)   b=$LOCAL/bench_hardware; d=$LOCAL/$mode-$i ;;
    esac
    mkdir -p "$d"
    cp "$OUT/workload.json" "$d/workload.json"
    mark "C $mode $i start"
    ( cd "$d" && timed "$OUT/time-$mode-$i.txt" timeout -s KILL 600 "$b" --config "$d/workload.json" \
        > "$d/stdout.log" 2> "$d/stderr.log" )
    mark "C $mode $i end exit=$?"
    cp "$d/stdout.log" "$OUT/stdout-$mode-$i.log"
    cp "$d/stderr.log" "$OUT/stderr-$mode-$i.log"
  done
done

# D. One run each way under strace: every system call with its start and duration,
#    leaving out the calls the queue's waiting loops make.
if have strace; then
  for mode in shared-shared local-local; do
    case $mode in
      shared-shared) b=$BIN; d=$SHARED/strace ;;
      local-local)   b=$LOCAL/bench_hardware; d=$LOCAL/strace ;;
    esac
    mkdir -p "$d"
    cp "$OUT/workload.json" "$d/workload.json"
    mark "D strace-$mode 1 start"
    ( cd "$d" && timeout -s KILL 900 strace -f -tt -T -e 'trace=!sched_yield,futex,nanosleep,clock_nanosleep' \
        -o "$OUT/strace-$mode.txt" "$b" --config "$d/workload.json" > "$d/stdout.log" 2> "$d/stderr.log" )
    mark "D strace-$mode 1 end exit=$?"
  done
fi

rm -rf "$SHARED" "$LOCAL"
mark "done"
SH
cat > $D/summarize.py <<'PY'
"""Summarize one or more passes of diagnose-step.sh. Python 3.9, standard library only.

    python3 -I summarize.py OUT [OUT ...]

For each pass: our load at its start; the median, least and greatest seconds of every
timed phase; for each run of the step, its wall time, its measured time (elapsed_ns)
and the difference, with the CPU and page faults GNU time saw; and, for each strace
capture, the slowest system calls and the time per call name.
"""
import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

TIME_FIELDS = {
    "Elapsed (wall clock) time (h:mm:ss or m:ss)": "wall",
    "User time (seconds)": "user_s",
    "System time (seconds)": "sys_s",
    "Major (requiring I/O) page faults": "major_faults",
    "Minor (reclaiming a frame) page faults": "minor_faults",
    "Voluntary context switches": "voluntary_switches",
    "File system inputs": "fs_inputs",
}
CALL = re.compile(r"^(\d+)\s+(\d\d:\d\d:\d\d\.\d+)\s+(?:<\.\.\. )?(\w+)(.*?)<(\d+\.\d+)>\s*$")


def seconds(text):
    parts = [float(p) for p in text.split(":")]
    total = 0.0
    for part in parts:
        total = total * 60 + part
    return total


def phases(out):
    starts, spans = {}, defaultdict(list)
    for line in (out / "timeline.tsv").read_text().splitlines():
        stamp, label = line.split("\t", 1)
        words = label.split()
        if len(words) >= 4 and words[3] == "start":
            starts[tuple(words[:3])] = float(stamp)
        elif len(words) >= 4 and words[3] == "end":
            key = tuple(words[:3])
            if key in starts:
                spans[f"{key[0]} {key[1]}"].append(float(stamp) - starts.pop(key))
    return spans


def gnu_time(path):
    found = {}
    if path.exists():
        for line in path.read_text().splitlines():
            name, _, value = line.strip().rpartition(": ")
            if name in TIME_FIELDS:
                found[TIME_FIELDS[name]] = seconds(value) if TIME_FIELDS[name] == "wall" else float(value)
    return found


def elapsed(path):
    match = re.search(r'"elapsed_ns":\s*(\d+)', path.read_text()) if path.exists() else None
    return int(match.group(1)) / 1e9 if match else None


def strace_summary(path, top=12):
    calls, per_name = [], defaultdict(float)
    for line in path.read_text(errors="replace").splitlines():
        match = CALL.match(line)
        if match:
            pid, clock, name, rest, took = match.groups()
            took = float(took)
            calls.append((took, clock, pid, name, rest[:100]))
            per_name[name] += took
    calls.sort(reverse=True)
    print(f"  {path.name}: {len(calls)} calls with a duration")
    for took, clock, pid, name, rest in calls[:top]:
        print(f"    {took:9.3f} s  {clock}  pid {pid}  {name}{rest}")
    names = sorted(per_name.items(), key=lambda item: -item[1])[:8]
    print("    by name: " + ", ".join(f"{name} {total:.2f} s" for name, total in names))


def main(outs):
    for out in map(Path, outs):
        print(f"== {out}")
        node = (out / "node.txt").read_text().splitlines()
        for line in node[:1] + [line for line in node if line.startswith("our ")]:
            print("  " + line)
        print("  phase                      n   median      min      max   (seconds)")
        for name, values in sorted(phases(out).items()):
            print(f"  {name:<24} {len(values):>3} {statistics.median(values):8.3f} {min(values):8.3f} {max(values):8.3f}")
        print("  run                 wall   measured  outside  user_s  sys_s  major  minor  vol.sw")
        for stdout in sorted(out.glob("stdout-*.log")):
            run = stdout.stem[len("stdout-"):]
            seen = gnu_time(out / f"time-{run}.txt")
            measured = elapsed(stdout)
            wall = seen.get("wall")
            outside = wall - measured if wall is not None and measured is not None else None
            cells = [wall, measured, outside, seen.get("user_s"), seen.get("sys_s"),
                     seen.get("major_faults"), seen.get("minor_faults"), seen.get("voluntary_switches")]
            print(f"  {run:<17} " + " ".join("     -  " if v is None else f"{v:8.2f}" for v in cells))
        for capture in sorted(out.glob("strace-*.txt")):
            strace_summary(capture)


if __name__ == "__main__":
    main(sys.argv[1:])
PY
```

`diagnose-step.sh` times one step of the earlier request on a node of its own, in four parts:
- **A.** 20 small create-and-rename pairs on GPFS and on the node's disk;
- **B.** starting a trivial program from GPFS and from the node's disk, and starting Python;
- **C.** the step's own binary and workload, run three ways, three times each:
  - `shared-shared`: binary and files on GPFS, as the lab runs them;
  - `shared-local`: binary on GPFS, files on the node;
  - `local-local`: both on the node.

  Each run is under `/usr/bin/time -v`, which records its wall time, CPU time, page faults and context switches.
- **D.** one run each way under `strace`, with every system call's start and duration.

It writes only into its output directory, `$TMPDIR`, and `$ROOT/diag-<job>`, and it removes the last two when it ends.

Then, on each cluster, choose the step and submit the job. The step is the one Guillem examined (`mutex-vecdeque-29cf7314`, task `s1-2p2c-b1024-u64x4-r2`). If that task has no verify step on this cluster, the step is the latest verify step of the run's first failed task. The binary is the one that run's builds made on this cluster.

```sh
export ROOT=/gpfs/projects/bsc18/$USER/ubq-lab
D=~/step-overhead-2026-11-10
C=grace; P=ngpp; Q=ngp_debug     # on glogin1.bsc.es: C=mn5; P=gpp; Q=gp_debug
RUN=mutex-vecdeque-29cf7314
STEP=$(python3 - $ROOT/runs/$RUN/$C <<'PY'
import json, sys
from pathlib import Path
dep = Path(sys.argv[1])
tasks = json.load(open(dep / "state.json")).get("tasks", {})
order = ["s1-2p2c-b1024-u64x4-r2"] + sorted(t for t, v in tasks.items() if v.get("status") == "failed")
for task in order:
    for attempt in sorted((dep / "tasks" / task).glob("a*"), reverse=True):
        steps = sorted(attempt.glob("verify-*/workload.json"))
        if steps:
            print(steps[0].parent)
            sys.exit(0)
sys.exit("no verify step found")
PY
)
BIN=$ROOT/builds/$C/a0e150ae93c0be998e22129b-default/bench_hardware
echo "$STEP"; ls -la "$BIN"
sbatch --account=bsc18 --partition=$P --qos=$Q --nodes=1 --exclusive --time=01:00:00 \
  --job-name=ubq-diag --output=$D/$C-pass1-%j.log \
  --wrap="bash $D/diagnose-step.sh $D/$C-pass1 $STEP $BIN $ROOT"
```

Wait for both jobs to end, then:

```sh
python3 -I ~/step-overhead-2026-11-10/summarize.py ~/step-overhead-2026-11-10/grace-pass1 ~/step-overhead-2026-11-10/mn5-pass1
```

In its table, `outside` is a run's wall time minus the time the benchmark measured.

## 2. The earlier request: archive everything, then cancel

Nicolas has agreed with Guillem to replace the request of 2026-10-30. **Nothing that has ended is to be lost.** On `grace1.bsc.es` with `C=grace`, then on `glogin1.bsc.es` with `C=mn5`:

```sh
export ROOT=/gpfs/projects/bsc18/$USER/ubq-lab
D=~/step-overhead-2026-11-10
C=grace   # on glogin1.bsc.es: C=mn5
mkdir -p $D/earlier
cd $ROOT/runs
for id in segqueue-94b068a9 concurrent-queue-c97267e1 ms-queue-18b5c940 lscq-97e54331 rbbq-25736508 \
          ubq-02479967 mutex-vecdeque-29cf7314 ubq-dynblock-9bdc7d82 ubq-bitmask-657c4ce6 ubq-v8-3831c8bd; do
  [ -d $id/$C ] || { echo "$id $C: not deployed"; continue; }
  python3 $id/tool/ab.py remote advance $id/$C | tail -1
  tar czf $D/earlier/$id-$C.tar.gz --exclude='perf.data*' --exclude=lock --exclude='.*.tmp' -C $id $C
  phase=$(python3 -c "import json;print(json.load(open('$id/$C/state.json')).get('phase'))")
  case $phase in done|failed|superseded|cancelled) echo "$id $C: $phase, archived, left as it is";;
    *) python3 $id/tool/ab.py remote cancel $id/$C | tail -1; echo "$id $C: was $phase, archived, then cancelled";;
  esac
done
```

Every archive is taken before its run is cancelled, so every task that had ended is in it. The runs' directories stay where they are. Keep both outputs for the reply.

## 3. The second pass, with our own runs gone

On each cluster, wait until `squeue -u $USER -h -o %j | grep -c '^ab\.'` prints 0: the earlier runs' jobs are the ones whose names start with `ab.`. Then submit the same job again, with `pass2` in place of `pass1` (the `--output` and the first argument of `--wrap`), and the same `STEP` and `BIN`. When both have ended:

```sh
D=~/step-overhead-2026-11-10
python3 -I $D/summarize.py $D/grace-pass1 $D/grace-pass2 $D/mn5-pass1 $D/mn5-pass2
```

If pass 2 is fast where pass 1 was slow, the cost came from load on the filesystem while our runs were going. If both are slow, it does not depend on our runs.

## 4. Find the cause

Read the two passes, and look further where they point. Some readings:
- **`A files-shared` is slow** (seconds where `A files-local` takes milliseconds). Then the lab's small files on GPFS are the cost, and keeping them on the node is the fix.
- **`C shared-*` is slow and `C local-local` is fast**, with high major page faults or file-system inputs. Then starting the binary from GPFS is the cost, and running a copy from the node is the fix.
- **`outside` is large even for `local-local`.** Then the time is in the process itself, before or after its measured region. The `strace` files show which system call or which gap holds it, and in which thread. Say which call, and what it was doing.
- **Every part is fast here, but the real tasks were slow.** Then the cost is in something the lab does that a single step does not show, for example around its tasks, in its `advance` calls or in its job submissions. Look there.

You may run up to four more one-hour jobs per cluster, as in step 1, and any read-only command on the login nodes or in your jobs: `strace` of the lab's own Python around a step, GPFS tools a user may run, the node's logs, BSC's documentation. Write only under `$D`, `$TMPDIR` and `$ROOT/diag-*`. Do not change the earlier runs' `tool/`, `sources/`, `builds/` or `run.json`, and do not launch the earlier runs again.

## 5. Pack and reply

```sh
cd ~ && tar czf step-overhead-2026-11-10.tar.gz step-overhead-2026-11-10 && sha256sum step-overhead-2026-11-10.tar.gz
```

Then send Guillem one message, for Nicolas, with:
1. **the cause**, in a few sentences, and the measurements it rests on;
2. **the recommended fix**, and whether the next bundle's node-local files and binary (described under "What this is") would be enough;
3. the output of `summarize.py` over all four passes;
4. the outputs of steps 0 and 2;
5. `step-overhead-2026-11-10.tar.gz` and its checksum. It holds the passes, their `strace` files and the earlier runs' archives.
