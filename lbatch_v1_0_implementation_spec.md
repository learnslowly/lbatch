# `lbatch` v1.0 Implementation Specification

**Status:** Implementation-ready specification  
**Target audience:** Software engineers implementing the first production-quality version of `lbatch`  
**Primary use case:** A user wants to replace `sbatch` with `lbatch` in large shell loops or large array submissions so work is queued locally and fed gradually into Slurm without exceeding user-level job limits.

---

## 1. Executive Summary

`lbatch` is an `sbatch`-style local submission front end for Slurm. It accepts common `sbatch` command-line syntax, converts each submission into a local, durable, flattened reservoir of atomic execution units, and gradually submits those units to Slurm according to a configurable remote-visible job limit.

The central model is:

```text
User-level submission
    -> lbatch submission group
        -> one or more atomic execution units
            -> gradual Slurm submissions
```

Examples:

```text
Normal sbatch-style job
    lbatch job.batch
    -> one group
    -> one atomic unit

Array job
    lbatch --array=1-300 job.batch
    -> one group
    -> 300 atomic units

Five dependent arrays
    -> five groups
    -> all array tasks flattened into one global reservoir
    -> dependency-held units become eligible only after upstream groups release
```

`lbatch` is not a full scheduler and does not judge whether a scientific job succeeded. A job that exits with code `0`, `1`, `137`, or another code is considered **released** from the perspective of `lbatch`; the exit code is recorded as metadata but does not define queue success/failure. User job scripts must be idempotent and resume-safe.

---

## 2. Product Contract

### 2.1 User-facing promise

Users should be able to write:

```bash
lbatch [common sbatch options] script.batch [script arguments...]
```

and treat it operationally like an `sbatch`-style local queue wrapper.

For example:

```bash
for CHR in 1 5 6 19 20; do
    N=$(wc -l < data/gene_list_chr${CHR}.txt)
    NT=$(( (N + 9) / 10 ))

    lbatch --partition=workq --account=YOUR_ACCOUNT \
        --nodes=1 --cpus-per-task=64 --time=23:00:00 \
        --job-name=fd_${CHR} --array=1-${NT} \
        --output=logs/fd_chr${CHR}_%A_%a.out \
        --error=logs/fd_chr${CHR}_%A_%a.err \
        --export=ALL,CHR=$CHR,GENE_LIST=data/gene_list_chr${CHR}.txt \
        job/per_chr_array.batch

    echo "chr${CHR}: locally queued ${NT} tasks"
done
```

The loop above must queue all chromosome tasks locally while exposing only a controlled number of remote-visible jobs to Slurm.

### 2.2 What `lbatch` preserves

`lbatch` must preserve the **intent** of common `sbatch` submissions:

- same batch script;
- same script arguments;
- same working directory by default;
- same resource options, unless transformed explicitly by `lbatch`;
- same exported user variables;
- correct `SLURM_ARRAY_TASK_ID` for array tasks;
- unique stdout/stderr log files under the user-provided patterns;
- dependency ordering when dependencies are expressed through local `lbatch` group IDs;
- gradual submission under a configured remote-visible limit.

### 2.3 What `lbatch` does not guarantee

`lbatch` does **not** guarantee byte-for-byte equivalence to native `sbatch`.

Known differences:

- A large Slurm array may be split into many one-element array submissions.
- `%a` remains the intended array task index, but `%A` may differ between split units because each one-element array gets its own Slurm array job ID.
- Native Slurm array variables such as `SLURM_ARRAY_TASK_COUNT`, `SLURM_ARRAY_TASK_MIN`, `SLURM_ARRAY_TASK_MAX`, and `SLURM_ARRAY_TASK_STEP` may reflect the one-element remote array rather than the original large array. `lbatch` must provide its own compatibility variables, described later.
- `lbatch --parsable` returns a local `lbatch` group ID, not a Slurm job ID.
- Exactly-once execution is not guaranteed. `lbatch` provides at-least-once delivery.
- Scientific job success is not interpreted by `lbatch` unless local dependency semantics explicitly require exit-code inspection.

---

## 3. Design Principles

1. **Local reservoir, remote backend**  
   Slurm remains the execution backend. `lbatch` only controls how much work is submitted to Slurm at a time.

2. **Flatten everything**  
   Independent jobs, arrays, and dependent groups are normalized into a global table of atomic execution units.

3. **At-least-once delivery**  
   If a local submission transaction is interrupted, `lbatch` may resubmit. User jobs must be safe to run more than once.

4. **Queue-only semantics**  
   `lbatch` does not classify user-code failure as queue failure. It records exit code as metadata and releases the remote slot.

5. **Event-driven locally; Slurm queries on demand**  
   `lbatch` must not use fixed-interval `squeue` polling as its scheduling heartbeat. It may query Slurm at startup, before dispatch, after a Slurm rejection, or when the user runs a reconciliation/capacity command.

6. **Small dependency surface**  
   v1.0 supports group-level local dependencies. Unitwise or corresponding-task dependency may be added later.

7. **Minimal runtime dependencies**  
   The first implementation should be reliable on HPC login nodes, which often have constrained or inconsistent Python environments.

---

## 4. Technical Stack

### 4.1 Language and runtime

- Python `>=3.11`.
- Package layout using `src/` style.
- Entry point installed as `lbatch`.

### 4.2 Required Python dependencies

MVP should use only the Python standard library:

- `argparse` for CLI parsing;
- `sqlite3` for durable queue state;
- `subprocess` for calling Slurm commands;
- `pathlib` for filesystem paths;
- `json` for serialized argv/config/event fields;
- `datetime` / `time` for timestamps;
- `fcntl` on Unix for daemon lock files;
- `hashlib` for event identity/checksums;
- `logging` for internal logs;
- `shlex` for directive parsing.

Optional later dependencies:

- `rich` for nicer status tables;
- `watchdog` for event-file watching;
- `typer` for a richer CLI;
- `pydantic` for stricter config validation.

These optional dependencies must not be required for the first working version.

### 4.3 Persistent storage

- SQLite database at:

```text
~/.local/share/lbatch/lbatch.db
```

- SQLite mode:

```sql
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;
```

- Event spool directory:

```text
~/.local/share/lbatch/events/
```

- Wrapper directory:

```text
~/.local/share/lbatch/wrappers/
```

- Runtime lock directory:

```text
~/.local/state/lbatch/
```

- User-facing config location:

```text
~/.config/lbatch/config.json
```

The implementation may also store effective config in the SQLite `settings` table so `lbatch config set ...` works without a TOML/YAML writer.

---

## 5. Codebase Architecture

Recommended repository structure:

```text
lbatch/
  pyproject.toml
  README.md
  LICENSE
  src/
    lbatch/
      __init__.py
      __main__.py
      cli.py
      config.py
      db.py
      models.py
      parser.py
      sbatch_directives.py
      arrays.py
      dependencies.py
      scheduler.py
      slurm.py
      wrapper.py
      events.py
      capacity.py
      status.py
      locks.py
      recovery.py
      errors.py
      logging_setup.py
      version.py
  tests/
    unit/
      test_parser.py
      test_arrays.py
      test_directives.py
      test_dependencies.py
      test_state_transitions.py
      test_capacity.py
    integration/
      fake_slurm/
        sbatch
        squeue
        sacct
        scancel
      test_submit_normal.py
      test_submit_array.py
      test_dependencies.py
      test_recovery.py
      test_chromosome_loop.py
  docs/
    implementation_spec.md
    user_guide.md
    developer_notes.md
```

### 5.1 Module responsibilities

#### `cli.py`

- Dispatches between reserved subcommands and sbatch-style submission mode.
- Reserved subcommands:
  - `daemon`
  - `status`
  - `config`
  - `reconcile`
  - `capacity-check`
  - `release`
  - `cancel`
  - `doctor`
- If the first argument is not a reserved subcommand, parse as an `sbatch`-style submission.

#### `parser.py`

- Parses top-level `lbatch` submission command lines.
- Separates:
  - `lbatch` control options;
  - sbatch options;
  - script path;
  - script arguments.
- Supports both `--option=value` and `--option value` forms for recognized options.
- Preserves unknown sbatch options for pass-through where possible.

#### `sbatch_directives.py`

- Extracts leading `#SBATCH` directives from batch scripts.
- Mimics Slurm parsing rule: directives are read only before the first non-comment, non-whitespace executable line.
- Merges script directives with command-line options.

#### `arrays.py`

- Parses `--array` specifications.
- Expands array specs into task IDs.
- Handles optional `%concurrency` as a local group-level cap.

#### `dependencies.py`

- Parses local and external dependencies.
- Builds group-level dependency records.
- Evaluates whether a group is eligible.

#### `scheduler.py`

- Implements the daemon dispatch loop.
- Updates dependency eligibility.
- Selects eligible units.
- Calls `SlurmClient.submit_unit(...)`.
- Enforces remote-visible capacity.

#### `slurm.py`

- Wraps `sbatch`, `squeue`, `sacct`, and optionally `scancel`.
- Provides a fakeable `SlurmClient` interface for tests.
- Parses `sbatch --parsable` output.

#### `wrapper.py`

- Generates per-unit wrapper scripts.
- Writes environment setup.
- Runs the original user batch script.
- Emits atomic release event files.

#### `events.py`

- Scans event directory.
- Validates and ingests event JSON.
- Ensures idempotent event processing.

#### `capacity.py`

- Implements capacity modes:
  - `local-owned`;
  - `slurm-on-demand`;
  - `manual`.

#### `recovery.py`

- Handles stale `SUBMITTING` units.
- Implements startup recovery rules.
- Implements explicit `lbatch reconcile`.

---

## 6. Core Domain Model

### 6.1 Submission group

A **group** corresponds to one user-facing `lbatch` submission command.

Examples:

```bash
lbatch job.batch
```

creates one group with one unit.

```bash
lbatch --array=1-300 job.batch
```

creates one group with 300 units.

A group records the user’s original intent:

- original argv;
- working directory;
- script path;
- script args;
- original array spec, if any;
- normalized sbatch options;
- local dependencies;
- group concurrency limit, if any.

### 6.2 Atomic execution unit

A **unit** is the smallest item `lbatch` may submit to Slurm.

- Normal submission: one unit.
- Array submission: one unit per array task ID.
- Unit state controls queue behavior.

### 6.3 Remote-visible unit

A unit is **remote-visible** after Slurm accepts it and returns a Slurm job ID.

Remote-visible units count against `lbatch` capacity until a release event is ingested or the user explicitly reconciles/forces release.

### 6.4 Release event

A release event means:

```text
The wrapper reached a terminal point and the Slurm-visible slot should be considered released by lbatch.
```

It does not mean the scientific computation succeeded.

---

## 7. State Machine

### 7.1 Unit states

Allowed unit states:

```text
HELD_DEPENDENCY
QUEUED
SUBMITTING
REMOTE_VISIBLE
RELEASED
HELD_INVALID
FORCE_RELEASED
```

Definitions:

| State | Meaning |
|---|---|
| `HELD_DEPENDENCY` | Unit exists but its group dependency is not satisfied. |
| `QUEUED` | Unit is eligible and waiting for remote capacity. |
| `SUBMITTING` | `lbatch` has selected the unit and is calling `sbatch`. |
| `REMOTE_VISIBLE` | Slurm accepted the unit and returned a job ID. |
| `RELEASED` | Wrapper event was ingested. Remote slot is considered released. |
| `HELD_INVALID` | Submission cannot proceed without user correction. |
| `FORCE_RELEASED` | User manually told `lbatch` to release the slot locally. |

There is intentionally no `FAILED` queue state.

### 7.2 Valid state transitions

```text
HELD_DEPENDENCY -> QUEUED
QUEUED          -> SUBMITTING
SUBMITTING      -> REMOTE_VISIBLE
SUBMITTING      -> QUEUED         # transient Slurm limit/retryable submit error
SUBMITTING      -> HELD_INVALID   # invalid script/options/non-retryable submit error
REMOTE_VISIBLE  -> RELEASED       # wrapper release event
REMOTE_VISIBLE  -> FORCE_RELEASED # explicit user action or reconciliation policy
```

### 7.3 Group state

Group state is derived from units:

| Group state | Condition |
|---|---|
| `PENDING` | At least one unit not released and no remote-visible unit. |
| `ACTIVE` | At least one unit is `REMOTE_VISIBLE` or `SUBMITTING`. |
| `RELEASED` | All units are `RELEASED` or `FORCE_RELEASED`. |
| `HELD` | All unreleased units are dependency-held or invalid-held. |

Group state should be computed, not manually stored as authoritative state. Storing cached group state is allowed but must be recomputable.

---

## 8. CLI Specification

### 8.1 Reserved subcommands

If the first positional argument is one of the following, `lbatch` enters command mode:

```text
daemon
status
config
reconcile
capacity-check
release
cancel
doctor
help
version
```

Otherwise, `lbatch` treats the command as an `sbatch`-style submission.

### 8.2 Submission syntax

Primary form:

```bash
lbatch [LBATCH_CONTROL_OPTIONS] [SBATCH_OPTIONS] script [script_args...]
```

Explicit form:

```bash
lbatch submit [LBATCH_CONTROL_OPTIONS] [SBATCH_OPTIONS] script [script_args...]
```

The explicit `submit` form is recommended for scripts that need complete clarity. The alias-style form is recommended for users replacing `sbatch` with `lbatch`.

### 8.3 `lbatch` control options

All `lbatch`-specific options must use the `--lbatch-*` prefix unless they are a reserved subcommand.

Supported v1.0 options:

```text
--lbatch-name NAME              Optional local group label.
--lbatch-priority INT           Local priority; higher values dispatch first. Default 0.
--lbatch-afterany GROUP_ID      Local dependency: dispatch after upstream group releases.
--lbatch-afterok GROUP_ID       Local dependency: dispatch after all upstream units release with exit_code 0.
--lbatch-afternotok GROUP_ID    Local dependency: dispatch after at least one upstream unit releases nonzero.
--lbatch-max-remote INT         Override configured remote-visible limit for daemon command only.
--lbatch-dry-run                Show expansion and planned records without writing DB.
--lbatch-no-start-daemon        Do not auto-start daemon after submit.
```

For compatibility with common `sbatch` workflows:

```text
--parsable
```

When used with `lbatch`, `--parsable` must print a local group ID, not a Slurm job ID.

Recommended output format:

```text
lb:g000001
```

### 8.4 Local dependency syntax

Preferred local dependency syntax:

```bash
j1=$(lbatch --parsable job_A.batch)
lbatch --lbatch-afterany "$j1" job_B.batch
```

Alternative dependency compatibility syntax:

```bash
j1=$(lbatch --parsable job_A.batch)
lbatch --dependency=afterany:$j1 job_B.batch
```

If `--dependency` contains at least one token beginning with `lb:`, `lbatch` must consume that local dependency token and not pass it to Slurm.

If `--dependency` contains only numeric Slurm job IDs, it must be treated as an external Slurm dependency and passed through when the unit is eventually submitted.

Mixed dependencies are allowed:

```bash
lbatch --dependency=afterany:123456:lb:g000001 job_C.batch
```

In this case:

- `123456` remains an external Slurm dependency passed to `sbatch`;
- `lb:g000001` becomes a local dependency;
- job C does not become locally eligible until `lb:g000001` is satisfied;
- when job C is submitted, its effective `sbatch` options still include `--dependency=afterany:123456`.

### 8.5 Daemon command

```bash
lbatch daemon [options]
```

Options:

```text
--max-remote-visible INT        Maximum remote-visible jobs allowed.
--capacity-mode MODE            local-owned | slurm-on-demand | manual.
--dispatch-batch-size INT       Maximum units submitted per dispatch trigger. Default 10.
--sleep SECONDS                 Local event-loop sleep. Default 5.
--once                          Run one dispatch/reconciliation cycle and exit.
--foreground                    Do not daemonize.
```

The daemon must enforce a single active daemon per queue root using a lock file.

### 8.6 Status command

```bash
lbatch status [--groups] [--units] [--json]
```

Default output must include:

```text
Queue root
Capacity mode
Max remote visible
Total groups
Total units
Held by dependency
Queued eligible
Submitting
Remote visible
Released
Invalid-held
Available local slots
```

### 8.7 Reconcile command

```bash
lbatch reconcile [--since DATE] [--apply] [--dry-run]
```

Purpose:

- query Slurm accounting/status on demand;
- identify lbatch units that are no longer visible in Slurm but lack local release events;
- optionally mark them `FORCE_RELEASED` or `RELEASED_BY_RECONCILE` depending on final implementation naming.

Reconciliation must not run automatically on a fixed interval.

### 8.8 Release command

```bash
lbatch release UNIT_ID
lbatch release --group GROUP_ID
```

Purpose:

- manually release markerless remote-visible units from local accounting.

This is a user-controlled escape hatch for cases such as manual `scancel`, node crash, or missing wrapper event.

---

## 9. `sbatch` Option Parsing and Preservation

### 9.1 Supported v1.0 option forms

The parser must support:

```text
--option=value
--option value
-short value
```

for recognized options.

At minimum, recognized options must include:

```text
--array, -a
--partition, -p
--account, -A
--nodes, -N
--ntasks, -n
--cpus-per-task, -c
--time, -t
--job-name, -J
--output, -o
--error, -e
--export
--dependency, -d
--mem
--mem-per-cpu
--gres
--qos
--constraint, -C
--mail-type
--mail-user
--chdir, -D
--signal
--parsable
```

Unknown long options must be passed through if they have a syntactically clear form:

```text
--unknown=value
```

Unknown options in ambiguous forms may be rejected with a message advising the user to use `--unknown=value`.

### 9.2 Script path detection

The first non-option token is the script path unless an option immediately preceding it consumes a value.

Everything after the script path is treated as script arguments and must be passed to the wrapper, then to the original script.

### 9.3 Unsupported `sbatch` features in v1.0

v1.0 may reject the following with explicit messages:

```text
--wrap
heterogeneous jobs using ':' syntax
stdin-provided batch scripts
multiple scripts in one command
advanced federation-specific options not safely passable
```

The error should say:

```text
This sbatch feature is not supported by lbatch v1.0. Submit it directly with sbatch or open a feature request.
```

---

## 10. `#SBATCH` Directive Handling

### 10.1 Extraction rule

When a user submits a script, `lbatch` must read leading `#SBATCH` directives from the script.

Rules:

1. Start at the top of the file.
2. Skip blank lines.
3. Skip ordinary shell comments that do not begin with `#SBATCH`.
4. Collect lines beginning with `#SBATCH`.
5. Stop permanently at the first non-comment, non-whitespace executable line.
6. Do not process any later `#SBATCH` lines.

This mimics Slurm’s documented behavior.

### 10.2 Merge precedence

Effective options are constructed in this order:

```text
script #SBATCH directives
then command-line sbatch options
then lbatch transformations
```

Command-line options override script directives for singleton options.

Examples of singleton options:

```text
--time
--partition
--account
--job-name
--array
--output
--error
--cpus-per-task
```

Repeatable options should be preserved unless explicitly known to be singleton.

### 10.3 Array transformation precedence

If the merged effective options contain `--array`, `lbatch` must:

1. parse the original array spec;
2. create one unit per array task ID;
3. remove the original array spec from the effective options of each unit;
4. add a replacement single-element array option for each unit:

```text
--array=<task_id>
```

It must never submit the original full array spec directly to Slurm in array-expansion mode.

---

## 11. Array Expansion Algorithm

### 11.1 Supported syntax

v1.0 must support Slurm-style array specs:

```text
1
1,3,5
1-10
1-10:2
1,5-10,20-30:5
1-300%20
```

The `%N` portion is a per-group local concurrency limit, not a remote Slurm array limit after expansion.

### 11.2 Expansion result

Example:

```text
--array=1-7:2
```

expands to:

```text
1, 3, 5, 7
```

Example:

```text
--array=1-5%2
```

expands to units:

```text
1, 2, 3, 4, 5
```

and records:

```text
group_array_concurrency_limit = 2
```

The scheduler may not have more than two `REMOTE_VISIBLE` or `SUBMITTING` units from that group at the same time.

### 11.3 Environment compatibility variables

Because lbatch submits one-element arrays, Slurm-native variables may not fully match the original large array. The wrapper must export additional lbatch variables:

```bash
LBATCH_GROUP_ID
LBATCH_UNIT_ID
LBATCH_ORIGINAL_ARRAY_SPEC
LBATCH_ARRAY_TASK_ID
LBATCH_ARRAY_TASK_COUNT
LBATCH_ARRAY_TASK_MIN
LBATCH_ARRAY_TASK_MAX
LBATCH_ARRAY_TASK_STEP
LBATCH_REMOTE_SLURM_JOB_ID
```

`SLURM_ARRAY_TASK_ID` must remain the correct task ID because each unit is submitted as:

```bash
sbatch --array=<task_id> ...
```

`lbatch` must not override `SLURM_JOB_ID` or `SLURM_ARRAY_JOB_ID`.

### 11.4 `%A` and `%a` behavior

For split arrays:

- `%a` remains the intended array task ID.
- `%A` becomes the Slurm array job ID of the one-element remote array, not one shared ID for the original local group.

The spec must document this clearly in the user guide.

---

## 12. Dependency Semantics

### 12.1 Dependency levels

v1.0 supports **group-level local dependencies**.

If group B depends on group A:

```text
No unit in group B becomes eligible until group A satisfies the dependency condition.
```

### 12.2 Supported local dependency types

#### `afterany`

Satisfied when all upstream units are `RELEASED` or `FORCE_RELEASED`.

#### `afterok`

Satisfied when all upstream units are released and all recorded exit codes are `0`.

This uses exit-code metadata but does not create a queue-level `FAILED` state.

#### `afternotok`

Satisfied when all upstream units are released and at least one recorded exit code is nonzero or missing due to force release/reconciliation policy.

The exact treatment of missing exit code must be documented. Recommended default:

```text
FORCE_RELEASED with unknown exit code does not satisfy afterok and does satisfy afternotok.
```

### 12.3 Multiple upstream dependencies

All local upstream dependencies must be satisfied before the downstream group becomes eligible.

### 12.4 Cycles

On submission, `lbatch` must reject dependency cycles.

Example:

```text
A depends on B and B depends on A
```

must produce `HELD_INVALID` or reject the submission before records are committed.

### 12.5 External Slurm dependencies

If a dependency references numeric Slurm job IDs only, it must be preserved and passed through to `sbatch` at remote submission time.

Example:

```bash
lbatch --dependency=afterok:123456 job.batch
```

The local unit may be eligible immediately from the lbatch perspective, but Slurm will hold it remotely until the external dependency is satisfied.

### 12.6 Mixed dependencies

Example:

```bash
lbatch --dependency=afterany:123456:lb:g000002 job.batch
```

Rules:

- local dependency token `lb:g000002` is consumed by `lbatch`;
- external dependency token `123456` is passed to Slurm;
- the downstream group remains locally held until `lb:g000002` is satisfied;
- at dispatch time, the unit is submitted with `--dependency=afterany:123456`.

---

## 13. Capacity Model

### 13.1 Definitions

```text
max_remote_visible
```

Maximum number of remote-visible jobs `lbatch` should allow under the selected capacity mode.

```text
remote_visible
```

Number of units considered to occupy remote Slurm capacity.

### 13.2 Capacity modes

#### `local-owned`

Counts only units submitted by this `lbatch` instance and not yet released.

Use this mode when the user routes large work through `lbatch` and wants zero regular Slurm queries.

#### `slurm-on-demand`

Queries Slurm only before dispatching work, after startup, after submission rejection, or when explicitly requested.

It must not query Slurm on a fixed interval solely because time passed.

Recommended command:

```bash
squeue -h -u "$USER" -r -o "%i|%T|%j|%K"
```

where `-r` requests one array element per line where supported.

Remote-visible states should include at least:

```text
PENDING
RUNNING
CONFIGURING
COMPLETING
SUSPENDED
```

Terminal states should not count.

#### `manual`

Does not query Slurm during daemon dispatch. User may run:

```bash
lbatch capacity-check
lbatch reconcile
```

manually.

### 13.3 Conservative phantom accounting

In `slurm-on-demand` mode, if a unit is marked `REMOTE_VISIBLE` locally but no longer appears in `squeue` and no release event exists, `lbatch` must not silently free the slot unless a reconciliation policy says so.

Recommended conservative formula:

```text
occupied = squeue_visible_user_jobs + local_phantom_lbatch_units
```

where:

```text
local_phantom_lbatch_units = lbatch REMOTE_VISIBLE units not seen in squeue and not released locally
```

This prevents accidental over-submission if a wrapper failed to write an event.

### 13.4 Dispatch rule

At every dispatch trigger:

```text
available_slots = max_remote_visible - occupied
```

If `available_slots <= 0`, submit nothing.

Otherwise, select up to:

```text
min(available_slots, dispatch_batch_size)
```

eligible units.

---

## 14. Scheduling Algorithm

### 14.1 Eligibility

A unit is eligible if:

```text
state = QUEUED
and group dependencies are satisfied
and group concurrency limit is not exceeded
```

### 14.2 Ordering

Default ordering:

```text
higher group priority first
then group created_at
then unit array order
then unit_id
```

### 14.3 Dispatch pseudocode

```python
def dispatch_once():
    ingest_local_release_events()
    update_dependency_eligibility()

    capacity = capacity_provider.compute_capacity()
    slots = capacity.max_remote_visible - capacity.occupied
    if slots <= 0:
        return

    n = min(slots, config.dispatch_batch_size)
    units = db.select_eligible_units(limit=n)

    for unit in units:
        submit_one_unit_transactionally(unit)
```

### 14.4 Submission transaction

`submit_one_unit_transactionally(unit)`:

```text
1. Begin DB transaction.
2. Verify unit is still QUEUED and eligible.
3. Set unit state to SUBMITTING.
4. Commit.
5. Generate wrapper script.
6. Call sbatch --parsable with effective options.
7. If sbatch succeeds:
       parse Slurm job ID;
       set state REMOTE_VISIBLE;
       store slurm_job_id;
       store submitted_at.
8. If sbatch returns retryable limit error:
       set state QUEUED;
       increment submit_attempts;
       store last_error;
       trigger backoff;
       stop current dispatch cycle.
9. If sbatch returns non-retryable error:
       set state HELD_INVALID;
       store last_error.
```

### 14.5 Crash recovery for `SUBMITTING`

At daemon startup:

- If a unit is `SUBMITTING` with no `slurm_job_id`, return it to `QUEUED` and record an audit warning.
- If a unit is `SUBMITTING` with a `slurm_job_id`, mark it `REMOTE_VISIBLE`.

There is one unavoidable ambiguity:

```text
crash after sbatch succeeded but before slurm_job_id was committed
```

In that case, `lbatch` may resubmit. This is accepted under at-least-once semantics. To bound the ambiguity, v1.0 must allow only one active `SUBMITTING` unit per daemon process unless the implementation has a stronger transaction/outbox design.

---

## 15. Wrapper and Event Protocol

### 15.1 Wrapper generation

For every unit, generate a wrapper script:

```text
~/.local/share/lbatch/wrappers/<unit_id>.sh
```

The wrapper must:

1. define `LBATCH_*` environment variables;
2. install exit/signal traps;
3. run the original user script;
4. capture exit code;
5. write an atomic release event JSON;
6. exit with the user script’s exit code.

### 15.2 Wrapper template

Implementation may vary, but must be equivalent to:

```bash
#!/usr/bin/env bash
set +e

LBATCH_GROUP_ID="__GROUP_ID__"
LBATCH_UNIT_ID="__UNIT_ID__"
LBATCH_EVENT_DIR="__EVENT_DIR__"
LBATCH_ORIGINAL_SCRIPT="__ORIGINAL_SCRIPT__"
LBATCH_ORIGINAL_CWD="__ORIGINAL_CWD__"
LBATCH_ARRAY_TASK_ID="__ARRAY_TASK_ID__"
LBATCH_ORIGINAL_ARRAY_SPEC="__ORIGINAL_ARRAY_SPEC__"
LBATCH_ARRAY_TASK_COUNT="__ARRAY_TASK_COUNT__"
LBATCH_ARRAY_TASK_MIN="__ARRAY_TASK_MIN__"
LBATCH_ARRAY_TASK_MAX="__ARRAY_TASK_MAX__"
LBATCH_ARRAY_TASK_STEP="__ARRAY_TASK_STEP__"

write_release_event() {
    local code="$1"
    local state="$2"
    local now
    now=$(date -Is)

    mkdir -p "$LBATCH_EVENT_DIR"
    local tmp="$LBATCH_EVENT_DIR/${LBATCH_UNIT_ID}.release.json.tmp.$$"
    local final="$LBATCH_EVENT_DIR/${LBATCH_UNIT_ID}.release.json"

    cat > "$tmp" <<EOF_JSON
{
  "schema_version": 1,
  "event_type": "release",
  "group_id": "$LBATCH_GROUP_ID",
  "unit_id": "$LBATCH_UNIT_ID",
  "slurm_job_id": "${SLURM_JOB_ID:-}",
  "slurm_array_job_id": "${SLURM_ARRAY_JOB_ID:-}",
  "slurm_array_task_id": "${SLURM_ARRAY_TASK_ID:-}",
  "lbatch_array_task_id": "$LBATCH_ARRAY_TASK_ID",
  "exit_code": "$code",
  "terminal_state": "$state",
  "hostname": "$(hostname)",
  "timestamp": "$now"
}
EOF_JSON

    mv "$tmp" "$final"
}

on_exit() {
    local code="$?"
    write_release_event "$code" "exit"
    exit "$code"
}

on_term() {
    write_release_event "143" "signal_term"
    exit 143
}

on_int() {
    write_release_event "130" "signal_int"
    exit 130
}

trap on_exit EXIT
trap on_term TERM
trap on_int INT

cd "$LBATCH_ORIGINAL_CWD" || exit 111

export LBATCH_GROUP_ID LBATCH_UNIT_ID
export LBATCH_ARRAY_TASK_ID LBATCH_ORIGINAL_ARRAY_SPEC
export LBATCH_ARRAY_TASK_COUNT LBATCH_ARRAY_TASK_MIN LBATCH_ARRAY_TASK_MAX LBATCH_ARRAY_TASK_STEP

if [[ -x "$LBATCH_ORIGINAL_SCRIPT" ]]; then
    "$LBATCH_ORIGINAL_SCRIPT" "$@"
else
    bash "$LBATCH_ORIGINAL_SCRIPT" "$@"
fi
```

The implementation must avoid double-writing if both a signal trap and `EXIT` trap fire. A simple guard variable is acceptable.

### 15.3 Event atomicity

Events must be written as:

```text
write temp file
fsync optional but recommended
atomic rename to final path
```

The event ingester must ignore `*.tmp` files.

### 15.4 Event idempotency

If the same event file is seen more than once, it must be ingested once.

Recommended table:

```sql
CREATE TABLE ingested_events (
    event_path TEXT PRIMARY KEY,
    unit_id TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    sha256 TEXT
);
```

---

## 16. SQLite Schema

### 16.1 Groups

```sql
CREATE TABLE groups (
    group_id TEXT PRIMARY KEY,
    label TEXT,
    original_argv_json TEXT NOT NULL,
    normalized_sbatch_options_json TEXT NOT NULL,
    external_dependency_json TEXT DEFAULT '[]',
    script_path TEXT NOT NULL,
    script_args_json TEXT DEFAULT '[]',
    workdir TEXT NOT NULL,
    array_spec TEXT,
    array_count INTEGER NOT NULL DEFAULT 1,
    array_min INTEGER,
    array_max INTEGER,
    array_step INTEGER,
    array_concurrency_limit INTEGER,
    priority INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
```

### 16.2 Units

```sql
CREATE TABLE units (
    unit_id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL REFERENCES groups(group_id) ON DELETE CASCADE,
    array_task_id INTEGER,
    array_order INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    effective_sbatch_options_json TEXT NOT NULL,
    wrapper_path TEXT,
    slurm_job_id TEXT,
    submit_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    exit_code INTEGER,
    release_event_path TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    submitted_at TEXT,
    released_at TEXT
);

CREATE INDEX idx_units_state ON units(state);
CREATE INDEX idx_units_group_state ON units(group_id, state);
CREATE INDEX idx_units_slurm_job_id ON units(slurm_job_id);
```

### 16.3 Dependencies

```sql
CREATE TABLE dependencies (
    dependency_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dependent_group_id TEXT NOT NULL REFERENCES groups(group_id) ON DELETE CASCADE,
    upstream_group_id TEXT NOT NULL REFERENCES groups(group_id) ON DELETE CASCADE,
    dependency_type TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(dependent_group_id, upstream_group_id, dependency_type)
);

CREATE INDEX idx_dependencies_dependent ON dependencies(dependent_group_id);
CREATE INDEX idx_dependencies_upstream ON dependencies(upstream_group_id);
```

### 16.4 Ingested events

```sql
CREATE TABLE ingested_events (
    event_path TEXT PRIMARY KEY,
    unit_id TEXT NOT NULL REFERENCES units(unit_id),
    event_type TEXT NOT NULL,
    sha256 TEXT,
    ingested_at TEXT NOT NULL
);
```

### 16.5 Settings

```sql
CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
```

### 16.6 Audit log

```sql
CREATE TABLE audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    level TEXT NOT NULL,
    action TEXT NOT NULL,
    group_id TEXT,
    unit_id TEXT,
    message TEXT NOT NULL,
    details_json TEXT DEFAULT '{}'
);
```

---

## 17. Error Classification

### 17.1 Retryable submission errors

The following should return unit state to `QUEUED` with backoff:

- QOS/job-limit rejection;
- Max jobs per user;
- Max submit jobs per user;
- temporary Slurm controller communication error;
- transient authentication/connection error.

Error classification should be regex-configurable.

Default retryable regex examples:

```text
QOSMax.*Limit
MaxJobs
MaxSubmit
Socket timed out
Unable to contact slurm controller
Resource temporarily unavailable
```

### 17.2 Non-retryable submission errors

The following should move unit to `HELD_INVALID`:

- script not found;
- invalid partition;
- invalid account;
- invalid QOS;
- malformed option;
- unsupported `sbatch` feature;
- dependency references unknown local group.

### 17.3 Backoff

After retryable Slurm rejection:

```text
stop current dispatch cycle
sleep at least backoff_seconds before next dispatch attempt
```

Default:

```text
initial backoff = 60 seconds
max backoff = 900 seconds
backoff factor = 2
reset after successful submission or release event
```

---

## 18. On-demand Slurm Introspection

### 18.1 `squeue` usage

`lbatch` may use `squeue` in these situations:

- daemon startup, if capacity mode is `slurm-on-demand`;
- immediately before dispatching eligible units;
- after Slurm rejects a submission due to limit;
- user runs `lbatch capacity-check`;
- user runs `lbatch reconcile`.

It must not run `squeue` merely because a fixed timer elapsed.

### 18.2 `sacct` usage

`sacct` is used only by `reconcile` or explicit diagnostic commands.

Purpose:

- determine final state/exit code for markerless Slurm jobs;
- support user-controlled cleanup after manual `scancel`, timeout, node crash, or missing event.

### 18.3 `scancel` usage

`lbatch cancel` may call `scancel` for known `slurm_job_id`s.

Recommended commands:

```bash
lbatch cancel --unit UNIT_ID
lbatch cancel --group GROUP_ID
```

Cancellation should not imply scientific failure. If cancellation succeeds, local units may remain `REMOTE_VISIBLE` until a wrapper event is seen or the user requests `--force-release`.

---

## 19. Idempotency Requirements for User Jobs

Because `lbatch` provides at-least-once delivery, users must write jobs so duplicate execution is safe.

Recommended job rules:

1. Check whether final output already exists and is valid.
2. Write to temporary files first.
3. Atomically rename temporary output to final output.
4. Optionally write a user-level `.done` marker.
5. Avoid destructive overwrites.
6. Use per-task locks if two duplicate units could touch the same output.

Example pattern:

```bash
out="results/${GENE}.tsv"
tmp="${out}.tmp.${SLURM_JOB_ID}"
done="${out}.done"

if [[ -s "$done" ]]; then
    echo "Already done: $GENE"
    exit 0
fi

run_analysis > "$tmp"
mv "$tmp" "$out"
touch "$done"
```

---

## 20. Acceptance Test Matrix

### 20.1 Independent normal jobs

Input:

```bash
for i in {1..100}; do
    lbatch --job-name=test_$i job.batch $i
done
```

Expected:

- 100 groups;
- 100 units;
- no more than `max_remote_visible` remote-visible units;
- all units eventually become `RELEASED` when fake wrappers emit events.

### 20.2 Single large array

Input:

```bash
lbatch --array=1-300 job.batch
```

Expected:

- 1 group;
- 300 units;
- each unit submitted with `--array=<task_id>`, not `--array=1-300`;
- `SLURM_ARRAY_TASK_ID` equals intended task ID;
- `%a` in output pattern corresponds to intended task ID.

### 20.3 Chromosome loop

Input: the chromosome loop from Section 2.1.

Expected:

- one group per chromosome;
- total units equal sum of all `NT` values;
- remote-visible count never exceeds configured capacity;
- each unit receives correct `CHR`, `GENE_LIST`, and array task ID.

### 20.4 Group-level dependency chain

Input:

```bash
a=$(lbatch --parsable --array=1-10 job_A.batch)
b=$(lbatch --parsable --lbatch-afterany "$a" --array=1-20 job_B.batch)
```

Expected:

- group B units start as `HELD_DEPENDENCY`;
- no B unit is submitted before all A units release;
- after all A units release, B units become `QUEUED`.

### 20.5 Mixed external and local dependency

Input:

```bash
a=$(lbatch --parsable --array=1-10 job_A.batch)
lbatch --dependency=afterany:123456:$a job_B.batch
```

Expected:

- local group A dependency is stored locally;
- external `123456` is preserved in effective `sbatch` options;
- B is locally held until A releases.

### 20.6 Retryable Slurm limit rejection

Fake `sbatch` returns error containing `QOSMaxJobsPerUserLimit`.

Expected:

- unit returns from `SUBMITTING` to `QUEUED`;
- `submit_attempts` increments;
- daemon backs off;
- no unit is marked failed.

### 20.7 Non-retryable invalid script

Input:

```bash
lbatch missing_script.batch
```

Expected:

- submission is rejected before units are created, or units are marked `HELD_INVALID`;
- clear error message.

### 20.8 Daemon crash after `SUBMITTING`

Simulate crash after state becomes `SUBMITTING` but before `sbatch` returns.

Expected:

- startup recovery returns unit to `QUEUED`;
- audit log records ambiguity;
- resubmission is allowed under at-least-once semantics.

### 20.9 Wrapper release event

Fake wrapper writes release JSON.

Expected:

- event ingested once;
- unit becomes `RELEASED`;
- exit code recorded;
- remote-visible count decrements.

### 20.10 Local `afterok`

Input:

```bash
a=$(lbatch --parsable --array=1-3 job_A.batch)
lbatch --lbatch-afterok "$a" job_B.batch
```

Case 1: all A units release with exit code `0`.

Expected:

- B becomes eligible.

Case 2: one A unit releases with exit code `1`.

Expected:

- B remains dependency-held;
- if there is a matching `afternotok` group, that group becomes eligible.

---

## 21. MVP Scope

v1.0 must implement:

- sbatch-style submission front end;
- explicit `submit` command alias;
- local SQLite queue;
- group/unit model;
- normal job support;
- array expansion support;
- group-level local dependencies;
- external Slurm dependency pass-through;
- local daemon;
- capacity modes `local-owned` and `slurm-on-demand`;
- wrapper generation;
- release event ingestion;
- status output;
- basic reconcile;
- fake Slurm test backend.

v1.0 may exclude:

- `--wrap`;
- heterogeneous jobs;
- stdin batch scripts;
- remote SSH submission;
- web dashboard;
- multi-user shared queue;
- exactly-once execution;
- taskwise/corresponding array dependency;
- sophisticated priority scheduling.

---

## 22. Developer Implementation Milestones

### Milestone 1: Parser and DB foundation

- Implement config paths.
- Implement SQLite schema/migrations.
- Implement sbatch-style parser.
- Implement `#SBATCH` extraction.
- Implement array parser.
- Unit tests for parser/array/directive behavior.

### Milestone 2: Local submission records

- Implement group creation.
- Implement unit expansion.
- Implement `lbatch --parsable` returning local group ID.
- Implement `lbatch status`.

### Milestone 3: Fake Slurm backend

- Implement `SlurmClient` interface.
- Implement fake `sbatch` returning fake job IDs.
- Implement fake `squeue`/`sacct` test commands.

### Milestone 4: Daemon and dispatch

- Implement daemon lock.
- Implement dispatch loop.
- Implement local-owned capacity mode.
- Implement retryable/non-retryable submit handling.

### Milestone 5: Wrapper and event protocol

- Generate wrapper scripts.
- Submit wrapper through fake/real sbatch.
- Ingest release events.
- Update unit state to `RELEASED`.

### Milestone 6: Dependencies

- Implement local group dependencies.
- Implement cycle detection.
- Implement mixed local/external dependency parsing.

### Milestone 7: On-demand Slurm introspection

- Implement `capacity-check`.
- Implement `slurm-on-demand` mode.
- Implement `reconcile` dry-run and apply mode.

### Milestone 8: Real Slurm smoke test

On a real cluster, test:

```bash
lbatch --array=1-5 --time=00:05:00 test.batch
lbatch daemon --max-remote-visible 2 --capacity-mode local-owned --foreground
```

Expected:

- at most two remote-visible units at a time;
- all five eventually release;
- logs are unique;
- `SLURM_ARRAY_TASK_ID` is correct.

---

## 23. Documentation Requirements

The final project must include:

1. `README.md`
   - one-paragraph description;
   - installation;
   - quick start;
   - warnings about at-least-once semantics.

2. `docs/user_guide.md`
   - replacing `sbatch` with `lbatch`;
   - array examples;
   - dependency examples;
   - how to run daemon;
   - how to inspect status;
   - how to reconcile after manual `scancel`.

3. `docs/developer_notes.md`
   - architecture;
   - database schema;
   - fake Slurm testing;
   - state machine.

4. `docs/idempotent_jobs.md`
   - how to make scripts duplicate-safe;
   - done markers;
   - atomic output rename.

---

## 24. Reference Slurm Behaviors Assumed by This Spec

The implementation relies on the following Slurm behaviors:

- `sbatch` submits a batch script, exits after Slurm accepts it, and the job may remain pending before resources are allocated.
- `#SBATCH` directives are processed only before the first non-comment, non-whitespace executable line.
- `sbatch --parsable` outputs only the job ID and optionally the cluster name separated by a semicolon.
- Slurm job arrays support ranges, lists, step sizes, and `%` concurrency limits.
- Array tasks set `SLURM_ARRAY_TASK_ID`.
- `%A` and `%a` in filenames are expanded to array job ID and array task ID, respectively.
- Array tasks still count as regular jobs for job-related limits.
- `squeue` views jobs in Slurm’s queue and can show array elements separately with array-oriented options.
- `sacct` displays accounting information, including job state and exit-code fields, when accounting is configured.

Official documentation:

- Slurm `sbatch`: https://slurm.schedmd.com/sbatch.html
- Slurm job arrays: https://slurm.schedmd.com/job_array.html
- Slurm `squeue`: https://slurm.schedmd.com/squeue.html
- Slurm `sacct`: https://slurm.schedmd.com/sacct.html
- Slurm job states: https://slurm.schedmd.com/job_state_codes.html

---

## 25. Final v1.0 Contract

`lbatch` is an `sbatch`-style local queue front end that converts user submissions into a durable, flattened reservoir of atomic execution units. It gradually submits eligible units to Slurm while respecting local dependencies and a configurable remote-visible capacity. It is designed to solve large loop and large array submission workflows without requiring users to manually resubmit jobs after Slurm quota rejections.

The core queue state is intentionally simple:

```text
HELD_DEPENDENCY -> QUEUED -> SUBMITTING -> REMOTE_VISIBLE -> RELEASED
```

`lbatch` does not define scientific success or failure. It releases slots when remote units terminate and records exit codes as metadata. Correct restart behavior is achieved by making user job scripts idempotent and output-resume-safe.
