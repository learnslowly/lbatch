from __future__ import annotations

import json
from pathlib import Path

from .arrays import parse_array_spec
from .db import Database, public_group_id, utcnow
from .dependencies import insert_dependencies, validate_dependencies
from .errors import LBatchError
from .models import SbatchOption, Submission
from .parser import get_option, without_option, with_option


def option_to_json(options: list[SbatchOption]) -> list[dict[str, str | None]]:
    return [{"name": option.name, "value": option.value} for option in options]


def option_from_json(data: str) -> list[SbatchOption]:
    return [SbatchOption(item["name"], item.get("value")) for item in json.loads(data)]


def create_submission(db: Database, submission: Submission) -> str:
    script = Path(submission.script_path)
    if not script.exists():
        raise LBatchError(f"script not found: {submission.script_path}")
    group_id = db.next_group_id()
    deps = validate_dependencies(db, group_id, submission.local_dependencies)
    array_spec = get_option(submission.sbatch_options, "--array")
    if array_spec:
        expansion = parse_array_spec(array_spec)
        base_options = without_option(submission.sbatch_options, "--array")
        task_ids = expansion.task_ids
        concurrency = expansion.concurrency_limit
        array_min = expansion.minimum
        array_max = expansion.maximum
        array_step = expansion.step
    else:
        expansion = None
        base_options = submission.sbatch_options
        task_ids = [None]
        concurrency = None
        array_min = None
        array_max = None
        array_step = None
    state = "HELD_DEPENDENCY" if deps else "QUEUED"
    now = utcnow()
    with db.transaction():
        db.conn.execute(
            """
            INSERT INTO groups(group_id, label, original_argv_json, normalized_sbatch_options_json,
                external_dependency_json, script_path, script_args_json, workdir, array_spec, array_count,
                array_min, array_max, array_step, array_concurrency_limit, priority, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                group_id,
                submission.lbatch_options.get("name"),
                json.dumps(submission.original_argv),
                json.dumps(option_to_json(submission.sbatch_options)),
                json.dumps([submission.external_dependency] if submission.external_dependency else []),
                str(script),
                json.dumps(submission.script_args),
                submission.workdir,
                array_spec,
                len(task_ids),
                array_min,
                array_max,
                array_step,
                concurrency,
                int(submission.lbatch_options.get("priority", 0)),
                now,
                now,
            ),
        )
        insert_dependencies(db, group_id, deps)
        for order, task_id in enumerate(task_ids):
            options = with_option(base_options, "--array", str(task_id)) if task_id is not None else base_options
            unit_id = db.next_unit_id(group_id, order + 1)
            db.conn.execute(
                """
                INSERT INTO units(unit_id, group_id, array_task_id, array_order, state, effective_sbatch_options_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (unit_id, group_id, task_id, order, state, json.dumps(option_to_json(options)), now, now),
            )
    return public_group_id(group_id)


def dry_run_plan(submission: Submission) -> dict:
    array_spec = get_option(submission.sbatch_options, "--array")
    if array_spec:
        expansion = parse_array_spec(array_spec)
        units = expansion.task_ids
        concurrency = expansion.concurrency_limit
    else:
        units = [None]
        concurrency = None
    return {
        "script_path": submission.script_path,
        "script_args": submission.script_args,
        "workdir": submission.workdir,
        "units": len(units),
        "array_task_ids": units,
        "array_concurrency_limit": concurrency,
        "local_dependencies": submission.local_dependencies,
        "sbatch_options": option_to_json(submission.sbatch_options),
    }
