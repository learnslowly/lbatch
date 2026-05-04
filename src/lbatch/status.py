from __future__ import annotations

import json

from .capacity import compute_capacity
from .config import Config
from .db import Database, public_group_id


def status_data(db: Database, config: Config) -> dict:
    counts = db.count_units_by_state()
    groups = db.conn.execute("SELECT COUNT(*) AS c FROM groups").fetchone()["c"]
    units = db.conn.execute("SELECT COUNT(*) AS c FROM units").fetchone()["c"]
    capacity = compute_capacity(db, config.max_remote_visible, config.capacity_mode)
    return {
        "queue_root": str(db.paths.data_dir),
        "capacity_mode": config.capacity_mode,
        "max_remote_visible": config.max_remote_visible,
        "total_groups": groups,
        "total_units": units,
        "held_by_dependency": counts.get("HELD_DEPENDENCY", 0),
        "queued_eligible": counts.get("QUEUED", 0),
        "submitting": counts.get("SUBMITTING", 0),
        "remote_visible": counts.get("REMOTE_VISIBLE", 0),
        "released": counts.get("RELEASED", 0) + counts.get("FORCE_RELEASED", 0),
        "invalid_held": counts.get("HELD_INVALID", 0),
        "available_local_slots": capacity.available,
    }


def format_status(data: dict, active_only: bool = False) -> str:
    """Render the status dict as a fixed-width table.

    `active_only=True` drops the lifetime-cumulative rows
    (`Total groups`, `Total units`, `Released`) so the user sees only
    what's currently in-flight: queued, dispatching, on slurm, or held.
    That's almost always what "is my run progressing?" actually means.
    """
    labels = [
        ("Queue root", "queue_root"),
        ("Capacity mode", "capacity_mode"),
        ("Max remote visible", "max_remote_visible"),
    ]
    if not active_only:
        labels += [
            ("Total groups", "total_groups"),
            ("Total units", "total_units"),
        ]
    labels += [
        ("Held by dependency", "held_by_dependency"),
        ("Queued eligible", "queued_eligible"),
        ("Submitting", "submitting"),
        ("Remote visible", "remote_visible"),
    ]
    if not active_only:
        labels += [("Released", "released")]
    labels += [
        ("Invalid-held", "invalid_held"),
        ("Available local slots", "available_local_slots"),
    ]
    width = max(len(label) for label, _ in labels)
    return "\n".join(f"{label:<{width}}  {data[key]}" for label, key in labels)


def groups_text(db: Database) -> str:
    rows = db.conn.execute(
        """
        SELECT g.group_id, g.label, COUNT(u.unit_id) AS units,
               SUM(CASE WHEN u.state IN ('RELEASED','FORCE_RELEASED') THEN 1 ELSE 0 END) AS released,
               SUM(CASE WHEN u.state = 'REMOTE_VISIBLE' THEN 1 ELSE 0 END) AS remote_visible,
               SUM(CASE WHEN u.state = 'HELD_INVALID' THEN 1 ELSE 0 END) AS invalid_held
        FROM groups g LEFT JOIN units u ON u.group_id = g.group_id
        GROUP BY g.group_id ORDER BY g.created_at
        """
    ).fetchall()
    lines = ["GROUP\tLABEL\tUNITS\tRELEASED\tREMOTE\tINVALID"]
    for row in rows:
        lines.append(f"{public_group_id(row['group_id'])}\t{row['label'] or ''}\t{row['units']}\t{row['released']}\t{row['remote_visible']}\t{row['invalid_held']}")
    return "\n".join(lines)


def units_text(db: Database) -> str:
    rows = db.conn.execute("SELECT unit_id, group_id, array_task_id, state, slurm_job_id FROM units ORDER BY created_at, array_order").fetchall()
    lines = ["UNIT\tGROUP\tARRAY_TASK\tSTATE\tSLURM_JOB"]
    for row in rows:
        lines.append(f"{row['unit_id']}\t{public_group_id(row['group_id'])}\t{row['array_task_id'] or ''}\t{row['state']}\t{row['slurm_job_id'] or ''}")
    return "\n".join(lines)
