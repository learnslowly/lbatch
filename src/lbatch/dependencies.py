from __future__ import annotations

from .db import Database, internal_group_id, utcnow
from .errors import LBatchError

VALID_TYPES = {"afterany", "afterok", "afternotok"}


def validate_dependencies(db: Database, dependent_group_id: str, dependencies: list[tuple[str, str]]) -> list[tuple[str, str]]:
    normalized: list[tuple[str, str]] = []
    for dep_type, group in dependencies:
        if dep_type not in VALID_TYPES:
            raise LBatchError(f"unsupported local dependency type: {dep_type}")
        upstream = internal_group_id(group)
        if not db.conn.execute("SELECT 1 FROM groups WHERE group_id = ?", (upstream,)).fetchone():
            raise LBatchError(f"unknown local dependency group: {group}")
        normalized.append((dep_type, upstream))
    for _, upstream in normalized:
        if _would_create_cycle(db, dependent_group_id, upstream):
            raise LBatchError(f"dependency cycle rejected for {dependent_group_id} -> {upstream}")
    return normalized


def _would_create_cycle(db: Database, dependent: str, upstream: str) -> bool:
    if dependent == upstream:
        return True
    stack = [upstream]
    seen: set[str] = set()
    while stack:
        group = stack.pop()
        if group in seen:
            continue
        seen.add(group)
        if group == dependent:
            return True
        rows = db.conn.execute(
            "SELECT upstream_group_id FROM dependencies WHERE dependent_group_id = ?", (group,)
        ).fetchall()
        stack.extend(row["upstream_group_id"] for row in rows)
    return False


def insert_dependencies(db: Database, dependent_group_id: str, dependencies: list[tuple[str, str]]) -> None:
    now = utcnow()
    for dep_type, upstream in dependencies:
        db.conn.execute(
            "INSERT OR IGNORE INTO dependencies(dependent_group_id, upstream_group_id, dependency_type, created_at) VALUES (?, ?, ?, ?)",
            (dependent_group_id, upstream, dep_type, now),
        )


def dependency_satisfied(db: Database, upstream_group_id: str, dep_type: str) -> bool:
    rows = db.conn.execute("SELECT state, exit_code FROM units WHERE group_id = ?", (upstream_group_id,)).fetchall()
    if not rows:
        return False
    all_released = all(row["state"] in {"RELEASED", "FORCE_RELEASED"} for row in rows)
    if dep_type == "afterany":
        return all_released
    if dep_type == "afterok":
        return all_released and all(row["state"] == "RELEASED" and row["exit_code"] == 0 for row in rows)
    if dep_type == "afternotok":
        return all_released and any(row["state"] == "FORCE_RELEASED" or row["exit_code"] is None or row["exit_code"] != 0 for row in rows)
    return False


def group_dependencies_satisfied(db: Database, group_id: str) -> bool:
    deps = db.conn.execute(
        "SELECT upstream_group_id, dependency_type FROM dependencies WHERE dependent_group_id = ?", (group_id,)
    ).fetchall()
    return all(dependency_satisfied(db, row["upstream_group_id"], row["dependency_type"]) for row in deps)


def update_dependency_eligibility(db: Database) -> int:
    groups = db.conn.execute("SELECT DISTINCT group_id FROM units WHERE state = 'HELD_DEPENDENCY'").fetchall()
    changed = 0
    with db.transaction():
        for row in groups:
            group_id = row["group_id"]
            if group_dependencies_satisfied(db, group_id):
                now = utcnow()
                cur = db.conn.execute("UPDATE units SET state = 'QUEUED', updated_at = ? WHERE group_id = ? AND state = 'HELD_DEPENDENCY'", (now, group_id))
                changed += cur.rowcount
    return changed
