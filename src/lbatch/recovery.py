from __future__ import annotations

from .db import Database, utcnow


def recover_submitting(db: Database) -> int:
    rows = db.conn.execute("SELECT unit_id, slurm_job_id FROM units WHERE state = 'SUBMITTING'").fetchall()
    changed = 0
    with db.transaction():
        for row in rows:
            now = utcnow()
            if row["slurm_job_id"]:
                db.conn.execute("UPDATE units SET state = 'REMOTE_VISIBLE', updated_at = ? WHERE unit_id = ?", (now, row["unit_id"]))
                message = "recovered submitting unit with slurm id as remote-visible"
            else:
                db.conn.execute("UPDATE units SET state = 'QUEUED', updated_at = ? WHERE unit_id = ?", (now, row["unit_id"]))
                message = "recovered ambiguous submitting unit to queued for at-least-once retry"
            db.conn.execute(
                "INSERT INTO audit_log(timestamp, level, action, unit_id, message) VALUES (?, 'WARNING', 'recover_submitting', ?, ?)",
                (now, row["unit_id"], message),
            )
            changed += 1
    return changed
