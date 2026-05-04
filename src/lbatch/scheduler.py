from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass

from .capacity import compute_capacity
from .config import Config
from .db import Database, utcnow
from .dependencies import group_dependencies_satisfied, update_dependency_eligibility
from .events import ingest_events
from .models import SbatchOption
from .recovery import recover_submitting
from .slurm import SlurmClient
from .submission import option_from_json
from .wrapper import write_wrapper


@dataclass
class DispatchResult:
    submitted: int = 0
    released: int = 0
    dependency_released: int = 0
    retryable_errors: int = 0
    invalid_errors: int = 0


def classify_retryable(error: str, config: Config) -> bool:
    return any(re.search(pattern, error, re.I) for pattern in config.retryable_error_regexes)


def _row_dict(row) -> dict:
    return dict(row)


def _group_for_unit(db: Database, group_id: str) -> dict:
    row = db.conn.execute("SELECT * FROM groups WHERE group_id = ?", (group_id,)).fetchone()
    data = dict(row)
    data["script_args"] = json.loads(data["script_args_json"])
    return data


def _select_eligible_units(db: Database, limit: int) -> list[dict]:
    candidates = db.conn.execute(
        """
        SELECT u.*, g.priority, g.created_at AS group_created_at, g.array_concurrency_limit
        FROM units u JOIN groups g ON g.group_id = u.group_id
        WHERE u.state = 'QUEUED'
        ORDER BY g.priority DESC, g.created_at ASC, u.array_order ASC, u.unit_id ASC
        """
    ).fetchall()
    selected: list[dict] = []
    for row in candidates:
        unit = dict(row)
        if not group_dependencies_satisfied(db, unit["group_id"]):
            continue
        cap = unit["array_concurrency_limit"]
        if cap is not None:
            active = db.conn.execute(
                "SELECT COUNT(*) AS c FROM units WHERE group_id = ? AND state IN ('SUBMITTING', 'REMOTE_VISIBLE')",
                (unit["group_id"],),
            ).fetchone()["c"]
            active += sum(1 for picked in selected if picked["group_id"] == unit["group_id"])
            if active >= cap:
                continue
        selected.append(unit)
        if len(selected) >= limit:
            break
    return selected


def submit_one(db: Database, unit: dict, config: Config, slurm: SlurmClient) -> tuple[str, str | None]:
    now = utcnow()
    with db.transaction():
        fresh = db.conn.execute("SELECT * FROM units WHERE unit_id = ?", (unit["unit_id"],)).fetchone()
        if not fresh or fresh["state"] != "QUEUED" or not group_dependencies_satisfied(db, fresh["group_id"]):
            return "skipped", None
        db.conn.execute("UPDATE units SET state = 'SUBMITTING', updated_at = ? WHERE unit_id = ?", (now, unit["unit_id"]))
    group = _group_for_unit(db, unit["group_id"])
    wrapper_path = write_wrapper(db.paths, unit, group)
    db.conn.execute("UPDATE units SET wrapper_path = ?, updated_at = ? WHERE unit_id = ?", (wrapper_path, utcnow(), unit["unit_id"]))
    db.conn.commit()
    options = option_from_json(unit["effective_sbatch_options_json"])
    result = slurm.submit(options, wrapper_path, [])
    now = utcnow()
    if result.ok:
        db.conn.execute(
            "UPDATE units SET state = 'REMOTE_VISIBLE', slurm_job_id = ?, submitted_at = ?, updated_at = ? WHERE unit_id = ?",
            (result.job_id, now, now, unit["unit_id"]),
        )
        db.conn.commit()
        return "submitted", result.job_id
    error = (result.stderr or result.stdout or f"sbatch failed with {result.returncode}").strip()
    attempts = unit["submit_attempts"] + 1
    if classify_retryable(error, config):
        db.conn.execute(
            "UPDATE units SET state = 'QUEUED', submit_attempts = ?, last_error = ?, updated_at = ? WHERE unit_id = ?",
            (attempts, error, now, unit["unit_id"]),
        )
        db.conn.commit()
        return "retryable", error
    db.conn.execute(
        "UPDATE units SET state = 'HELD_INVALID', submit_attempts = ?, last_error = ?, updated_at = ? WHERE unit_id = ?",
        (attempts, error, now, unit["unit_id"]),
    )
    db.conn.commit()
    return "invalid", error


def dispatch_once(db: Database, config: Config, slurm: SlurmClient | None = None, max_remote_visible: int | None = None, capacity_mode: str | None = None, fill_to_cap: bool = False) -> DispatchResult:
    """Run one dispatch pass.

    Steady-state behaviour: dispatch up to `dispatch_batch_size` units,
    enough to keep the queue ticking but not so many that we burst the
    slurm controller.

    Initial-fill behaviour (`fill_to_cap=True`): ignore `dispatch_batch_size`
    and dispatch up to the *full* available capacity in one pass — i.e., on
    daemon startup, drive `Remote visible` straight up to `max_remote_visible`
    so the cluster slots aren't sitting idle while we wait for the next
    sleep cycle. Submissions are still **sequential** so the per-second
    sbatch rate is the same as steady state; we just don't artificially
    throttle the *count* of units we fire on the first pass.
    """
    slurm = slurm or SlurmClient()
    result = DispatchResult()
    result.released = ingest_events(db)
    result.dependency_released = update_dependency_eligibility(db)
    capacity = compute_capacity(db, max_remote_visible or config.max_remote_visible, capacity_mode or config.capacity_mode, slurm)
    if fill_to_cap:
        slots = capacity.available
    else:
        slots = min(capacity.available, config.dispatch_batch_size)
    if slots <= 0:
        return result
    units = _select_eligible_units(db, slots)
    for unit in units:
        status, _ = submit_one(db, unit, config, slurm)
        if status == "submitted":
            result.submitted += 1
        elif status == "retryable":
            result.retryable_errors += 1
            break
        elif status == "invalid":
            result.invalid_errors += 1
    return result


def run_daemon(db: Database, config: Config, slurm: SlurmClient | None = None, once: bool = False, sleep_seconds: float | None = None, max_remote_visible: int | None = None, capacity_mode: str | None = None) -> None:
    recover_submitting(db)
    # Initial fill: drive Remote visible up to max_remote_visible immediately
    # so the cluster slots aren't idle while the steady-state loop ramps. The
    # sbatch RPCs themselves remain sequential (one at a time) — we just
    # don't cap the count to `dispatch_batch_size` on the first pass.
    dispatch_once(db, config, slurm, max_remote_visible, capacity_mode, fill_to_cap=True)
    if once:
        return
    while True:
        dispatch_once(db, config, slurm, max_remote_visible, capacity_mode)
        time.sleep(config.sleep_seconds if sleep_seconds is None else sleep_seconds)
