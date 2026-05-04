# lbatch

`lbatch` is an `sbatch`-style local queue front end for Slurm. It lets you queue large loops or large array jobs locally, then feeds work into Slurm gradually so you do not exceed user-level job limits.

The core idea is:

```text
lbatch submission -> local group -> atomic units -> gradual sbatch submissions
```

Examples:

- `lbatch job.batch` creates one local group with one unit.
- `lbatch --array=1-300 job.batch` creates one local group with 300 local units.
- `lbatch daemon --max-remote-visible 10` submits at most 10 unreleased units to Slurm at a time.

`lbatch` is not a scientific workflow scheduler. It does not decide whether your analysis succeeded. If the remote job exits, `lbatch` records the exit code and releases the local slot.

## Status

This repository contains a first working MVP of the v1.0 spec:

- local SQLite queue;
- `sbatch`-style submission parsing;
- `#SBATCH` directive parsing;
- Slurm array expansion into one local unit per task;
- local group dependencies;
- external Slurm dependency pass-through;
- daemon dispatch loop;
- wrapper scripts and release events;
- status, release, reconcile, cancel, config, and doctor commands.

## Requirements

- Python `>=3.10`.
- Slurm client commands available on `PATH` for real dispatch: `sbatch`, `squeue`, `sacct`, and `scancel`.
- No required third-party Python packages.

## Installation

From the project root:

```bash
python -m pip install -e .
```

If your cluster does not allow writing to the active environment:

```bash
python -m pip install --user -e .
export PATH="$HOME/.local/bin:$PATH"
```

Verify installation:

```bash
lbatch version
lbatch doctor
```

## Persistent State

By default, `lbatch` stores local queue state under standard XDG-style user paths:

```text
~/.local/share/lbatch/lbatch.db
~/.local/share/lbatch/events/
~/.local/share/lbatch/wrappers/
~/.local/state/lbatch/
~/.config/lbatch/config.json
```

Useful reset command during testing:

```bash
rm -rf ~/.local/share/lbatch ~/.local/state/lbatch ~/.config/lbatch
```

Use that only when you intentionally want to delete the local `lbatch` queue.

## Mental Model

Submitting with `lbatch` only creates local records. It does not immediately submit everything to Slurm.

```bash
lbatch --array=1-100 job.batch
```

This queues 100 local units. A daemon must be running to feed them into Slurm:

```bash
lbatch daemon --max-remote-visible 10 --capacity-mode local-owned --foreground
```

At most 10 units become Slurm-visible at one time. As wrappers finish and emit release events, the daemon submits more queued units.

## Basic Usage

### 1. Submit A Single Job

```bash
lbatch job/my_job.batch
lbatch status --groups --units
lbatch daemon --max-remote-visible 1 --capacity-mode local-owned --foreground
```

### 2. Submit A Small Array

```bash
mkdir -p logs

lbatch --array=1-5 \
  --partition=workq \
  --account=YOUR_ACCOUNT \
  --time=00:05:00 \
  --output=logs/small_%A_%a.out \
  --error=logs/small_%A_%a.err \
  --export=ALL,GROUP=1 \
  job/test_sleep.batch

lbatch status --groups --units
lbatch daemon --max-remote-visible 2 --capacity-mode local-owned --foreground
```

Check Slurm and logs from another terminal:

```bash
squeue -u "$USER"
ls -lh logs
cat logs/small_*_1.out
```

### 3. Dry Run Before Writing Queue Records

Use `--lbatch-dry-run` to inspect how a submission will expand:

```bash
lbatch --lbatch-dry-run --array=1-5 --export=ALL,GROUP=1 job/test_sleep.batch
```

Dry-run prints planned units and effective options without writing to SQLite.

## Demo Scripts In `job/`

This repository includes two demo scripts:

```text
job/test_sleep.batch
job/submit_lbatch_test.sh
```

### `job/test_sleep.batch`

This is the batch script executed by Slurm. It prints the group, array task ID, Slurm job ID, hostname, and timestamps, sleeps for 10 seconds, then prints a completion line.

It expects `GROUP` to be provided through `--export=ALL,GROUP=<N>` and uses `SLURM_ARRAY_TASK_ID` from Slurm.

### `job/submit_lbatch_test.sh`

This script queues five local groups:

```text
GROUP=1 -> array 1-50
GROUP=2 -> array 1-50
GROUP=3 -> array 1-50
GROUP=4 -> array 1-50
GROUP=5 -> array 1-50
```

Total local work:

```text
5 groups x 50 array tasks = 250 local units
```

It does not run the daemon. It only queues local work.

### Full Demo Workflow

Start from the project root:

```bash
mkdir -p logs
bash job/submit_lbatch_test.sh
```

Confirm the local queue:

```bash
lbatch status --groups
lbatch status --groups --units
```

You should see 5 groups and 250 total units.

Start dispatching to Slurm:

```bash
lbatch daemon --max-remote-visible 10 --capacity-mode local-owned --foreground
```

In another terminal, monitor Slurm and `lbatch`:

```bash
watch -n 2 'squeue -u "$USER"; echo; lbatch status --groups'
```

Inspect logs:

```bash
ls -lh logs
head -n 20 logs/test_g1_*_1.out
```

Expected behavior:

- `lbatch status` initially shows many `QUEUED` units.
- The daemon submits up to 10 remote-visible units.
- As the 10-second jobs finish, wrapper release events mark units `RELEASED`.
- The daemon submits more units until all 250 are released.

## Running The Daemon

### Foreground Mode

```bash
lbatch daemon --max-remote-visible 10 --capacity-mode local-owned --foreground
```

Foreground mode is best for first tests because errors are visible immediately. Stop it with `Ctrl-C`.

### Background With `nohup`

The current MVP does not fork into the background itself. Use shell tools:

```bash
mkdir -p logs
nohup lbatch daemon --max-remote-visible 10 --capacity-mode local-owned > logs/lbatch-daemon.log 2>&1 &
echo $! > logs/lbatch-daemon.pid
```

Check it:

```bash
tail -f logs/lbatch-daemon.log
ps -p "$(cat logs/lbatch-daemon.pid)"
```

Stop it:

```bash
kill "$(cat logs/lbatch-daemon.pid)"
```

### Background With `tmux`

Recommended on clusters:

```bash
tmux new -s lbatch
lbatch daemon --max-remote-visible 10 --capacity-mode local-owned --foreground
```

Detach with `Ctrl-b d` and reattach with:

```bash
tmux attach -t lbatch
```

## Capacity Modes

### `local-owned`

```bash
lbatch daemon --max-remote-visible 10 --capacity-mode local-owned --foreground
```

Counts only jobs submitted by this `lbatch` queue. This is the simplest and recommended mode for initial use.

### `slurm-on-demand`

```bash
lbatch daemon --max-remote-visible 10 --capacity-mode slurm-on-demand --foreground
```

Queries `squeue` before dispatching to account for other visible Slurm jobs owned by the user. It is more conservative, but depends on `squeue` behavior at your site.

### `manual`

```bash
lbatch daemon --max-remote-visible 10 --capacity-mode manual --foreground
```

Does not query Slurm during normal dispatch. Use explicit commands such as `lbatch capacity-check` or `lbatch reconcile` when needed.

## Status And Inspection

Summary:

```bash
lbatch status
```

Groups:

```bash
lbatch status --groups
```

Units:

```bash
lbatch status --units
```

JSON:

```bash
lbatch status --json
```

Capacity:

```bash
lbatch capacity-check
```

Doctor:

```bash
lbatch doctor
```

## Dependencies

`lbatch --parsable` returns a local group ID:

```bash
a=$(lbatch --parsable --array=1-10 job/A.batch)
echo "$a"
# lb:g000001
```

### Local `afterany`

Run group B only after all units in group A have released:

```bash
a=$(lbatch --parsable --array=1-10 job/A.batch)
lbatch --lbatch-afterany "$a" --array=1-20 job/B.batch
```

### Local `afterok`

Run group B only after all units in group A release with exit code `0`:

```bash
a=$(lbatch --parsable --array=1-10 job/A.batch)
lbatch --lbatch-afterok "$a" job/B.batch
```

### Local `afternotok`

Run group B after group A releases and at least one upstream unit has nonzero or unknown exit code:

```bash
a=$(lbatch --parsable --array=1-10 job/A.batch)
lbatch --lbatch-afternotok "$a" job/B.batch
```

### Mixed Local And Slurm Dependencies

Numeric Slurm dependencies are passed through. `lb:` dependencies are handled locally:

```bash
a=$(lbatch --parsable --array=1-10 job/A.batch)
lbatch --dependency=afterany:123456:$a job/C.batch
```

In this example:

- `lb:g000001` is consumed by `lbatch` as a local dependency;
- `123456` remains in the remote `sbatch --dependency` option;
- group C does not become locally eligible until group A releases.

## Array Behavior

`lbatch` expands arrays locally:

```bash
lbatch --array=1-300 job.batch
```

This creates 300 local units. Each unit is submitted to Slurm as a one-element array:

```text
--array=1
--array=2
...
--array=300
```

Important filename behavior:

- `%a` remains the intended array task ID.
- `%A` is the Slurm job ID of the one-element remote array, not one shared ID for the original local array.

The wrapper exports compatibility variables:

```bash
LBATCH_GROUP_ID
LBATCH_UNIT_ID
LBATCH_ORIGINAL_ARRAY_SPEC
LBATCH_ARRAY_TASK_ID
LBATCH_ARRAY_TASK_COUNT
LBATCH_ARRAY_TASK_MIN
LBATCH_ARRAY_TASK_MAX
LBATCH_ARRAY_TASK_STEP
```

`lbatch` does not override `SLURM_JOB_ID`, `SLURM_ARRAY_JOB_ID`, or `SLURM_ARRAY_TASK_ID`.

## Releasing And Reconciling

If a wrapper event is missing because of manual `scancel`, node failure, or another cluster issue, local units may remain `REMOTE_VISIBLE`.

Inspect:

```bash
lbatch status --units
```

Reconcile dry-run:

```bash
lbatch reconcile --dry-run
```

Apply reconciliation:

```bash
lbatch reconcile --apply
```

Manual release one unit:

```bash
lbatch release UNIT_ID
```

Manual release all remote-visible units in a group:

```bash
lbatch release --group lb:g000001
```

## Canceling

Cancel a known remote Slurm job for one local unit:

```bash
lbatch cancel --unit UNIT_ID
```

Cancel all known remote Slurm jobs in a group:

```bash
lbatch cancel --group lb:g000001
```

Cancel and locally force release:

```bash
lbatch cancel --group lb:g000001 --force-release
```

Cancellation does not automatically mean scientific failure. It is local queue cleanup plus Slurm cancellation.

## Configuration

Show config:

```bash
lbatch config show
```

Set config:

```bash
lbatch config set max_remote_visible 20
lbatch config set capacity_mode local-owned
lbatch config set dispatch_batch_size 10
```

Config is stored at:

```text
~/.config/lbatch/config.json
```

Daemon command-line flags can override config for that daemon run:

```bash
lbatch daemon --max-remote-visible 5 --dispatch-batch-size 5 --foreground
```


## Failure Modes And Recovery

This section describes what happens when common operational failures occur and what you should do next.

### Daemon Stops Or You Close The Terminal

If the daemon exits, is killed, loses its terminal, or the login session closes, the local queue is not lost. Queue state is durable in SQLite under `~/.local/share/lbatch/lbatch.db`.

What happens:

- `QUEUED` units stay queued locally.
- `HELD_DEPENDENCY` units stay held locally.
- `REMOTE_VISIBLE` units already submitted to Slurm keep running or pending in Slurm.
- No new units are submitted while the daemon is stopped.
- Completed remote jobs may write release events, but those events are not ingested until a later `lbatch status` or daemon run.

Resume by starting the daemon again:

```bash
lbatch daemon --max-remote-visible 10 --capacity-mode local-owned --foreground
```

Or resume in the background:

```bash
nohup lbatch daemon --max-remote-visible 10 --capacity-mode local-owned > logs/lbatch-daemon.log 2>&1 &
```

Recommended checks after restart:

```bash
lbatch doctor
lbatch status --groups --units
squeue -u "$USER"
```

`lbatch doctor` recovers ambiguous `SUBMITTING` units. A unit stuck in `SUBMITTING` without a recorded Slurm job ID is returned to `QUEUED` and may be submitted again. This is part of the at-least-once delivery model.

### Daemon Crashes During Submission

There is one inherently ambiguous window: Slurm may accept a job, but the daemon may crash before storing the Slurm job ID in SQLite.

What happens:

- If the unit was `SUBMITTING` with no `slurm_job_id`, restart recovery returns it to `QUEUED`.
- That unit may be submitted again.
- This can create duplicate execution for the same logical unit.

What to do:

```bash
lbatch doctor
lbatch status --units
lbatch daemon --max-remote-visible 10 --capacity-mode local-owned --foreground
```

Your job scripts should be idempotent so duplicate execution is safe.

### Slurm Job Finishes While Daemon Is Down

The wrapper writes a release event at job exit. If the daemon is not running, the event just sits in `~/.local/share/lbatch/events/`.

What happens:

- The local unit may still show `REMOTE_VISIBLE` until events are ingested.
- Running `lbatch status` ingests events.
- Starting the daemon also ingests events before dispatching more work.

Resume:

```bash
lbatch status --groups --units
lbatch daemon --max-remote-visible 10 --capacity-mode local-owned --foreground
```

### You Run `scancel` Directly On Current Slurm Jobs

If you run `scancel` manually, Slurm cancels the remote jobs, but `lbatch` does not automatically know your intent.

```bash
scancel 123456
```

Possible outcomes:

- If the wrapper receives the signal and exits cleanly, it writes a release event. `lbatch status` or the daemon will mark the unit `RELEASED` with a signal-style exit code such as `143`.
- If the job is canceled before the wrapper starts, or the node fails before the event is written, no release event exists. The unit can remain `REMOTE_VISIBLE` locally.
- In `local-owned` mode, a markerless canceled job still occupies local capacity until you reconcile or force release it.

After manual `scancel`, inspect state:

```bash
lbatch status --units
squeue -u "$USER"
```

If units remain `REMOTE_VISIBLE` but are no longer in Slurm, use reconciliation:

```bash
lbatch reconcile --dry-run
lbatch reconcile --apply
```

Or release explicitly:

```bash
lbatch release UNIT_ID
lbatch release --group lb:g000001
```

### Prefer `lbatch cancel` Over Raw `scancel`

When possible, cancel through `lbatch` so it can use known Slurm job IDs:

```bash
lbatch cancel --unit UNIT_ID
lbatch cancel --group lb:g000001
```

If you also want to free local capacity immediately:

```bash
lbatch cancel --group lb:g000001 --force-release
```

Tradeoff: `--force-release` tells `lbatch` to stop counting those units locally even if no wrapper event is written. This is useful for cleanup, but it means downstream `afterok` dependencies will not be satisfied by those force-released units.

### Slurm Rejects A Submission Because Of Limits

If `sbatch` returns a retryable limit error, such as `QOSMaxJobsPerUserLimit` or `MaxSubmit`, `lbatch` returns that unit from `SUBMITTING` to `QUEUED` and stops the current dispatch cycle.

What happens:

- The unit is not failed.
- `submit_attempts` increments.
- `last_error` records the Slurm rejection.
- A later daemon cycle can try again.

Inspect errors:

```bash
sqlite3 ~/.local/share/lbatch/lbatch.db \
  "select unit_id,state,submit_attempts,last_error from units where last_error is not null limit 20;"
```

Resume with a smaller cap if needed:

```bash
lbatch daemon --max-remote-visible 2 --capacity-mode local-owned --foreground
```

### Slurm Rejects A Submission Permanently

If `sbatch` rejects the job for a non-retryable reason, such as an invalid account, partition, QOS, script path, or malformed option, the unit becomes `HELD_INVALID`.

Inspect:

```bash
lbatch status --units
sqlite3 ~/.local/share/lbatch/lbatch.db \
  "select unit_id,state,last_error from units where state='HELD_INVALID' limit 20;"
```

Current MVP behavior is conservative: fix the underlying submission issue and submit a new corrected `lbatch` group. Do not edit the SQLite DB unless you are deliberately doing manual recovery.

### Node Failure Or Missing Wrapper Event

If the compute node dies, the wrapper may not write its release JSON event.

What happens:

- Slurm may show the job as gone or terminal.
- `lbatch` may still show the unit as `REMOTE_VISIBLE`.
- Local capacity remains occupied until reconciliation or manual release.

Recover:

```bash
lbatch reconcile --dry-run
lbatch reconcile --apply
```

If reconciliation cannot determine enough information, manually force release:

```bash
lbatch release UNIT_ID
```

### Local Queue DB Is Deleted

If you delete `~/.local/share/lbatch/lbatch.db` while Slurm jobs are still running, `lbatch` loses the mapping between local units and remote Slurm jobs.

What happens:

- Already submitted Slurm jobs continue independently.
- New `lbatch status` starts from an empty queue.
- Wrapper scripts may still write event files, but there are no matching unit records.

Avoid deleting the DB unless all related Slurm jobs are finished or canceled.

If you accidentally delete it, inspect and clean Slurm directly:

```bash
squeue -u "$USER"
scancel <jobid>
```

### Machine Reboot Or Filesystem Interruption

SQLite uses WAL mode and the queue is durable, but an interruption can leave temporary files or stale locks.

Recover:

```bash
lbatch doctor
lbatch status --groups --units
lbatch daemon --max-remote-visible 10 --capacity-mode local-owned --foreground
```

If a stale daemon lock prevents startup and you are sure no daemon is running:

```bash
ps -fu "$USER" | grep '[l]batch daemon'
rm -f ~/.local/state/lbatch/daemon.lock
```

Only remove the lock after confirming no daemon process is active.

### Dependency Behavior After Failures

Local dependencies use release metadata, not scientific interpretation except for `afterok` and `afternotok`.

- `afterany`: downstream group is eligible after all upstream units are `RELEASED` or `FORCE_RELEASED`.
- `afterok`: downstream group is eligible only if all upstream units are `RELEASED` with exit code `0`.
- `afternotok`: downstream group is eligible if all upstream units are released and at least one has nonzero or unknown exit code.

Important consequence:

- Manual `lbatch release` creates `FORCE_RELEASED` units.
- `FORCE_RELEASED` satisfies `afterany`.
- `FORCE_RELEASED` does not satisfy `afterok`.
- `FORCE_RELEASED` can satisfy `afternotok` because the exit code is unknown.

## Troubleshooting

### `bash job/submit_lbatch_test.sh` produces no `squeue` output

Expected. That script only queues local work. Start the daemon:

```bash
lbatch daemon --max-remote-visible 10 --capacity-mode local-owned --foreground
```

### No logs appear in `logs/`

Logs are Slurm stdout/stderr files. They appear only after Slurm starts and runs wrapper jobs.

Check:

```bash
lbatch status --units
squeue -u "$USER"
```

If units are `HELD_INVALID`, inspect errors:

```bash
sqlite3 ~/.local/share/lbatch/lbatch.db \
  "select unit_id,state,last_error from units where last_error is not null limit 10;"
```

Common causes:

- invalid partition;
- invalid account;
- invalid QOS;
- cluster does not allow the requested time/resources;
- `sbatch` is not available on the node where the daemon is running.

### Install fails because Python is too old

`lbatch` requires Python `>=3.10`.

Check:

```bash
python --version
python -m pip --version
```

### `lbatch` command not found after `pip install --user -e .`

Add the user bin directory:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

Put that line in your shell startup file if needed.

## At-Least-Once Delivery

`lbatch` is intentionally at-least-once. A daemon crash after Slurm accepts a job but before the local DB records the Slurm job ID can cause resubmission.

Write job scripts to tolerate duplicates:

```bash
out="results/${SLURM_ARRAY_TASK_ID}.txt"
tmp="${out}.tmp.${SLURM_JOB_ID}"
done="${out}.done"

if [[ -s "$done" ]]; then
  echo "Already done: ${SLURM_ARRAY_TASK_ID}"
  exit 0
fi

run_analysis > "$tmp"
mv "$tmp" "$out"
touch "$done"
```

Recommended rules:

1. Check whether final output already exists and is valid.
2. Write temporary outputs first.
3. Atomically rename temporary outputs to final outputs.
4. Use `.done` markers where appropriate.
5. Avoid destructive overwrites.

## Development

Run tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python -m unittest discover -s tests -v
```

Run without installing:

```bash
PYTHONPATH=src python -m lbatch version
```

Useful local smoke test:

```bash
PYTHONPATH=src python -m lbatch --lbatch-dry-run --array=1-5 job/test_sleep.batch
```
