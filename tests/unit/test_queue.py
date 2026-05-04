import os
import tempfile
import unittest
from pathlib import Path

from lbatch.config import Config, Paths
from lbatch.db import Database, internal_group_id
from lbatch.events import ingest_events
from lbatch.parser import parse_submission
from lbatch.sbatch_directives import extract_directive_argv
from lbatch.scheduler import dispatch_once
from lbatch.submission import create_submission


class FakeSlurm:
    def __init__(self):
        self.calls = []
        self.next_id = 100

    def submit(self, options, wrapper_path, script_args):
        self.calls.append((options, wrapper_path, script_args))
        self.next_id += 1
        return type("Result", (), {"ok": True, "job_id": str(self.next_id), "stdout": str(self.next_id), "stderr": "", "returncode": 0})()

    def squeue_visible_job_ids(self):
        return set()


class QueueTests(unittest.TestCase):
    def make_paths(self, root):
        data = Path(root) / "data"
        return Paths(data, data / "lbatch.db", data / "events", data / "wrappers", Path(root) / "state", Path(root) / "config.json")

    def test_array_submission_dispatch_and_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = self.make_paths(tmp)
            db = Database(paths)
            script = Path(tmp) / "job.batch"
            script.write_text("#!/usr/bin/env bash\nexit 0\n")
            sub = parse_submission(["--array=1-3%2", str(script)], extract_directive_argv(str(script)))
            public = create_submission(db, sub)
            group_id = internal_group_id(public)
            count = db.conn.execute("SELECT COUNT(*) AS c FROM units WHERE group_id = ?", (group_id,)).fetchone()["c"]
            self.assertEqual(count, 3)
            fake = FakeSlurm()
            cfg = Config(max_remote_visible=10, dispatch_batch_size=10)
            result = dispatch_once(db, cfg, fake)
            self.assertEqual(result.submitted, 2)
            first_options = {opt.name: opt.value for opt in fake.calls[0][0]}
            self.assertEqual(first_options["--array"], "1")
            remote = db.conn.execute("SELECT COUNT(*) AS c FROM units WHERE state = 'REMOTE_VISIBLE'").fetchone()["c"]
            self.assertEqual(remote, 2)
            unit = db.conn.execute("SELECT unit_id FROM units ORDER BY array_order LIMIT 1").fetchone()["unit_id"]
            (paths.events_dir / f"{unit}.release.json").write_text('{"event_type":"release","unit_id":"%s","exit_code":"0"}' % unit)
            self.assertEqual(ingest_events(db), 1)
            state = db.conn.execute("SELECT state, exit_code FROM units WHERE unit_id = ?", (unit,)).fetchone()
            self.assertEqual(state["state"], "RELEASED")
            self.assertEqual(state["exit_code"], 0)
            db.close()


if __name__ == "__main__":
    unittest.main()
