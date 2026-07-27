import json
import sys
import tempfile
import unittest
from pathlib import Path

from lbatch.errors import LBatchError
from lbatch.pool import (
    ResourcePool,
    WorkerResources,
    parse_memory,
    run_pack,
    worker_capacity,
)


class ResourcePoolTests(unittest.TestCase):
    def test_memory_parser(self):
        self.assertEqual(parse_memory("0"), 0)
        self.assertEqual(parse_memory("4G"), 4 * 1024**3)
        self.assertEqual(parse_memory("1.5GiB"), int(1.5 * 1024**3))

    def test_capacity_is_minimum_across_cores_memory_gpus_and_user_cap(self):
        pool = ResourcePool(
            cores=16,
            memory_bytes=parse_memory("24G"),
            gpu_devices=("0", "1", "2", "3"),
        )
        demand = WorkerResources(
            cores=2,
            memory_bytes=parse_memory("4G"),
            gpus=1,
        )
        self.assertEqual(
            worker_capacity(pool, demand, task_count=20),
            4,
        )
        self.assertEqual(
            worker_capacity(pool, demand, task_count=20, maximum_workers=3),
            3,
        )

    def test_rejects_declared_worker_memory_when_pool_memory_is_unknown(self):
        with self.assertRaises(LBatchError):
            worker_capacity(
                ResourcePool(cores=4),
                WorkerResources(cores=1, memory_bytes=1),
                task_count=1,
            )

    def test_run_pack_exports_resources_and_records_every_task(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            code = (
                "import os,time; "
                "print(os.environ['LBATCH_PACK_TASK_ID'], "
                "os.environ['SLURM_CPUS_PER_TASK'], "
                "os.environ['CUDA_VISIBLE_DEVICES']); "
                "time.sleep(0.1)"
            )
            manifest_path, succeeded = run_pack(
                task_ids=[2, 4, 6, 8],
                command=[sys.executable, "-c", code],
                log_dir=root / "logs",
                pool=ResourcePool(
                    cores=4,
                    memory_bytes=parse_memory("8G"),
                    gpu_devices=("0", "1"),
                ),
                demand=WorkerResources(
                    cores=2,
                    memory_bytes=parse_memory("2G"),
                    gpus=1,
                ),
            )
            self.assertTrue(succeeded)
            payload = json.loads(manifest_path.read_text())
            self.assertEqual(payload["maximum_concurrent_workers"], 2)
            self.assertEqual(payload["succeeded_tasks"], 4)
            self.assertEqual(payload["failed_tasks"], 0)
            self.assertTrue((root / "logs" / ".done").is_file())
            intervals = []
            for task_id in (2, 4, 6, 8):
                status = json.loads(
                    (root / "logs" / f"task-{task_id}.status.json").read_text()
                )
                self.assertEqual(status["returncode"], 0)
                self.assertEqual(status["cores"], 2)
                self.assertEqual(len(status["gpu_devices"]), 1)
                self.assertIn(
                    status["gpu_devices"][0],
                    {"0", "1"},
                )
                intervals.append(status)
            overlapping_pairs = 0
            for left_index, left in enumerate(intervals):
                for right in intervals[left_index + 1 :]:
                    overlap = (
                        left["started_at"] < right["finished_at"]
                        and right["started_at"] < left["finished_at"]
                    )
                    if overlap:
                        overlapping_pairs += 1
                        self.assertTrue(
                            set(left["gpu_devices"]).isdisjoint(
                                right["gpu_devices"]
                            )
                        )
            self.assertGreater(overlapping_pairs, 0)

    def test_failure_is_pack_failure_but_other_tasks_are_retained(self):
        with tempfile.TemporaryDirectory() as temporary:
            code = (
                "import os,sys; "
                "sys.exit(7 if os.environ['LBATCH_PACK_TASK_ID']=='3' else 0)"
            )
            manifest_path, succeeded = run_pack(
                task_ids=[1, 2, 3, 4],
                command=[sys.executable, "-c", code],
                log_dir=Path(temporary) / "logs",
                pool=ResourcePool(cores=2),
                demand=WorkerResources(cores=1),
            )
            self.assertFalse(succeeded)
            payload = json.loads(manifest_path.read_text())
            self.assertEqual(payload["succeeded_tasks"], 3)
            self.assertEqual(payload["failed_tasks"], 1)
            self.assertFalse((Path(temporary) / "logs" / ".done").exists())


if __name__ == "__main__":
    unittest.main()
