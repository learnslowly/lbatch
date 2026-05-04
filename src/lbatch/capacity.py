from __future__ import annotations

from dataclasses import dataclass

from .db import Database
from .slurm import SlurmClient


@dataclass
class Capacity:
    max_remote_visible: int
    occupied: int
    mode: str

    @property
    def available(self) -> int:
        return self.max_remote_visible - self.occupied


def compute_capacity(db: Database, max_remote_visible: int, mode: str, slurm: SlurmClient | None = None) -> Capacity:
    local_remote = db.conn.execute("SELECT COUNT(*) AS c FROM units WHERE state = 'REMOTE_VISIBLE'").fetchone()["c"]
    if mode in {"local-owned", "manual"}:
        return Capacity(max_remote_visible, local_remote, mode)
    if mode == "slurm-on-demand":
        slurm = slurm or SlurmClient()
        visible = slurm.squeue_visible_job_ids()
        local_rows = db.conn.execute("SELECT slurm_job_id FROM units WHERE state = 'REMOTE_VISIBLE' AND slurm_job_id IS NOT NULL").fetchall()
        local_ids = {row["slurm_job_id"] for row in local_rows}
        phantom = len(local_ids - visible)
        return Capacity(max_remote_visible, len(visible) + phantom, mode)
    raise ValueError(f"unknown capacity mode: {mode}")
