from __future__ import annotations

import json
import os
import queue
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from .errors import LBatchError


_MEMORY_PATTERN = re.compile(
    r"^(?P<value>[0-9]+(?:\.[0-9]+)?)"
    r"(?P<unit>[kmgtp]i?b?|b)?$",
    re.IGNORECASE,
)
_MEMORY_MULTIPLIERS = {
    "": 1,
    "b": 1,
    "k": 1024,
    "kb": 1024,
    "ki": 1024,
    "kib": 1024,
    "m": 1024**2,
    "mb": 1024**2,
    "mi": 1024**2,
    "mib": 1024**2,
    "g": 1024**3,
    "gb": 1024**3,
    "gi": 1024**3,
    "gib": 1024**3,
    "t": 1024**4,
    "tb": 1024**4,
    "ti": 1024**4,
    "tib": 1024**4,
    "p": 1024**5,
    "pb": 1024**5,
    "pi": 1024**5,
    "pib": 1024**5,
}


def parse_memory(value: str | int | float) -> int:
    """Parse a non-negative binary memory quantity into bytes."""

    text = str(value).strip()
    match = _MEMORY_PATTERN.fullmatch(text)
    if match is None:
        raise LBatchError(f"invalid memory quantity: {value}")
    number = float(match.group("value"))
    unit = (match.group("unit") or "").lower()
    result = int(number * _MEMORY_MULTIPLIERS[unit])
    if result < 0:
        raise LBatchError(f"memory quantity must be non-negative: {value}")
    return result


@dataclass(frozen=True)
class ResourcePool:
    cores: int
    memory_bytes: int = 0
    gpu_devices: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.cores < 1:
            raise LBatchError("pool cores must be positive")
        if self.memory_bytes < 0:
            raise LBatchError("pool memory must be non-negative")
        if len(set(self.gpu_devices)) != len(self.gpu_devices):
            raise LBatchError("pool GPU device identifiers must be unique")


@dataclass(frozen=True)
class WorkerResources:
    cores: int = 1
    memory_bytes: int = 0
    gpus: int = 0

    def __post_init__(self) -> None:
        if self.cores < 1:
            raise LBatchError("worker cores must be positive")
        if self.memory_bytes < 0:
            raise LBatchError("worker memory must be non-negative")
        if self.gpus < 0:
            raise LBatchError("worker GPUs must be non-negative")


def worker_capacity(
    pool: ResourcePool,
    demand: WorkerResources,
    *,
    task_count: int,
    maximum_workers: int | None = None,
) -> int:
    """Return the largest safe uniform-worker concurrency across all pools."""

    if task_count < 1:
        raise LBatchError("task count must be positive")
    limits = [task_count, pool.cores // demand.cores]
    if demand.memory_bytes:
        if not pool.memory_bytes:
            raise LBatchError(
                "worker memory was declared but the pool memory is unknown"
            )
        limits.append(pool.memory_bytes // demand.memory_bytes)
    if demand.gpus:
        limits.append(len(pool.gpu_devices) // demand.gpus)
    if maximum_workers is not None:
        if maximum_workers < 1:
            raise LBatchError("maximum workers must be positive")
        limits.append(maximum_workers)
    capacity = min(limits)
    if capacity < 1:
        raise LBatchError(
            "one worker does not fit in the declared core, memory and GPU pools"
        )
    return capacity


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_json(path: Path, payload: dict) -> None:
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _render_command(command: Sequence[str], task_id: int, task_index: int) -> list[str]:
    return [
        argument.replace("{task_id}", str(task_id)).replace(
            "{task_index}", str(task_index)
        )
        for argument in command
    ]


def _gpu_slots(
    devices: tuple[str, ...],
    gpus_per_worker: int,
    capacity: int,
) -> list[tuple[str, ...]]:
    if not gpus_per_worker:
        return [()] * capacity
    return [
        devices[index * gpus_per_worker : (index + 1) * gpus_per_worker]
        for index in range(capacity)
    ]


def run_pack(
    *,
    task_ids: Sequence[int],
    command: Sequence[str],
    log_dir: Path,
    pool: ResourcePool,
    demand: WorkerResources,
    maximum_workers: int | None = None,
) -> tuple[Path, bool]:
    """Run array tasks through a bounded core/RAM/GPU resource pool."""

    tasks = [int(task_id) for task_id in task_ids]
    if not tasks:
        raise LBatchError("run-pack requires at least one task")
    if len(set(tasks)) != len(tasks):
        raise LBatchError("run-pack task IDs must be unique")
    if not command:
        raise LBatchError("run-pack requires a command after '--'")
    capacity = worker_capacity(
        pool,
        demand,
        task_count=len(tasks),
        maximum_workers=maximum_workers,
    )
    log_dir.mkdir(parents=True, exist_ok=True)
    slot_queue: queue.Queue[int] = queue.Queue()
    for slot in range(capacity):
        slot_queue.put(slot)
    gpu_slots = _gpu_slots(pool.gpu_devices, demand.gpus, capacity)
    started_at = _utcnow()
    pack_start = time.monotonic()

    def execute(task_index: int, task_id: int) -> dict:
        slot = slot_queue.get()
        task_start = time.monotonic()
        started = _utcnow()
        assigned_gpus = gpu_slots[slot]
        argv = _render_command(command, task_id, task_index)
        environment = os.environ.copy()
        environment.update(
            {
                "SLURM_CPUS_PER_TASK": str(demand.cores),
                "LBATCH_PACK_TASK_ID": str(task_id),
                "LBATCH_PACK_TASK_INDEX": str(task_index),
                "LBATCH_PACK_TASK_COUNT": str(len(tasks)),
                "LBATCH_PACK_WORKER_CORES": str(demand.cores),
                "LBATCH_PACK_WORKER_MEMORY_BYTES": str(demand.memory_bytes),
                "LBATCH_PACK_WORKER_GPUS": str(demand.gpus),
                "LBATCH_PACK_SLOT": str(slot),
            }
        )
        if demand.gpus:
            environment["CUDA_VISIBLE_DEVICES"] = ",".join(assigned_gpus)
        stdout_path = log_dir / f"task-{task_id}.out"
        stderr_path = log_dir / f"task-{task_id}.err"
        try:
            with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
                process = subprocess.run(
                    argv,
                    env=environment,
                    stdout=stdout,
                    stderr=stderr,
                    check=False,
                )
            returncode = int(process.returncode)
            launch_error = None
        except OSError as exc:
            returncode = 127
            launch_error = str(exc)
            with stderr_path.open("ab") as stderr:
                stderr.write(f"lbatch run-pack launch error: {exc}\n".encode())
        finally:
            slot_queue.put(slot)
        payload = {
            "schema_version": "lbatch.run_pack_task.v1",
            "task_id": task_id,
            "task_index": task_index,
            "slot": slot,
            "argv": argv,
            "cores": demand.cores,
            "memory_bytes": demand.memory_bytes,
            "gpu_devices": list(assigned_gpus),
            "returncode": returncode,
            "launch_error": launch_error,
            "started_at": started,
            "finished_at": _utcnow(),
            "elapsed_seconds": time.monotonic() - task_start,
            "stdout": str(stdout_path),
            "stderr": str(stderr_path),
        }
        _atomic_json(log_dir / f"task-{task_id}.status.json", payload)
        return payload

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=capacity) as executor:
        futures = {
            executor.submit(execute, index, task_id): task_id
            for index, task_id in enumerate(tasks)
        }
        for future in as_completed(futures):
            results.append(future.result())
    results.sort(key=lambda item: item["task_index"])
    succeeded = all(result["returncode"] == 0 for result in results)
    manifest = {
        "schema_version": "lbatch.run_pack.v1",
        "status": "completed" if succeeded else "failed",
        "task_ids": tasks,
        "task_count": len(tasks),
        "maximum_concurrent_workers": capacity,
        "pool": {
            "cores": pool.cores,
            "memory_bytes": pool.memory_bytes,
            "gpu_devices": list(pool.gpu_devices),
        },
        "worker_resources": {
            "cores": demand.cores,
            "memory_bytes": demand.memory_bytes,
            "gpus": demand.gpus,
        },
        "command": list(command),
        "started_at": started_at,
        "finished_at": _utcnow(),
        "elapsed_seconds": time.monotonic() - pack_start,
        "succeeded_tasks": sum(result["returncode"] == 0 for result in results),
        "failed_tasks": sum(result["returncode"] != 0 for result in results),
        "results": results,
    }
    manifest_path = log_dir / "manifest.json"
    _atomic_json(manifest_path, manifest)
    if succeeded:
        (log_dir / ".done").touch()
    return manifest_path, succeeded


def detect_pool(
    *,
    pool_cores: int | None = None,
    pool_memory: str | None = None,
    pool_gpus: str | None = None,
) -> ResourcePool:
    """Resolve a resource pool from explicit values, then Slurm environment."""

    if pool_cores is None:
        raw_cores = (
            os.environ.get("SLURM_CPUS_PER_TASK")
            or os.environ.get("SLURM_CPUS_ON_NODE")
            or os.cpu_count()
            or 1
        )
        pool_cores = int(raw_cores)
    if pool_memory is not None:
        memory_bytes = parse_memory(pool_memory)
    elif os.environ.get("SLURM_MEM_PER_NODE"):
        memory_bytes = int(os.environ["SLURM_MEM_PER_NODE"]) * 1024**2
    elif os.environ.get("SLURM_MEM_PER_CPU"):
        memory_bytes = (
            int(os.environ["SLURM_MEM_PER_CPU"]) * 1024**2 * pool_cores
        )
    else:
        memory_bytes = 0
    if pool_gpus is not None:
        text = pool_gpus.strip()
        if text.isdigit():
            gpu_devices = tuple(str(index) for index in range(int(text)))
        else:
            gpu_devices = tuple(
                device.strip() for device in text.split(",") if device.strip()
            )
    else:
        visible = os.environ.get("CUDA_VISIBLE_DEVICES", "").strip()
        gpu_devices = (
            tuple(device.strip() for device in visible.split(",") if device.strip())
            if visible and visible != "NoDevFiles"
            else ()
        )
    return ResourcePool(
        cores=pool_cores,
        memory_bytes=memory_bytes,
        gpu_devices=gpu_devices,
    )
