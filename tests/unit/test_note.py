import os
import sqlite3
import tempfile
import unittest

from lbatch.parser import parse_submission


def _isolate_xdg():
    d = tempfile.mkdtemp()
    os.environ.update(XDG_DATA_HOME=d + "/data", XDG_STATE_HOME=d + "/state", XDG_CONFIG_HOME=d + "/cfg")
    return d


class NoteTests(unittest.TestCase):
    def test_parser_captures_note(self):
        sub = parse_submission(["--lbatch-note=genome-wide", "--array=1-3", "job.batch"])
        self.assertEqual(sub.lbatch_options.get("note"), "genome-wide")

    def test_note_stored_shown_and_filtered(self):
        _isolate_xdg()
        from lbatch.db import Database
        from lbatch.status import groups_text
        from lbatch.submission import create_submission
        db = Database()
        create_submission(db, parse_submission(["--lbatch-note=genome-wide", "/bin/echo"]))
        create_submission(db, parse_submission(["--lbatch-note=immune-panel", "/bin/echo"]))
        create_submission(db, parse_submission(["/bin/echo"]))  # no note
        self.assertIn("NOTE", groups_text(db))                  # note is a column
        filtered = groups_text(db, note="genome")
        self.assertIn("genome-wide", filtered)
        self.assertNotIn("immune-panel", filtered)              # filter excludes other notes
        db.close()

    def test_migration_adds_note_to_preexisting_db(self):
        d = _isolate_xdg()
        os.makedirs(d + "/data", exist_ok=True)
        c = sqlite3.connect(d + "/data/lbatch.db")
        c.execute("CREATE TABLE groups (group_id TEXT PRIMARY KEY, label TEXT)")  # pre-note schema
        c.commit()
        c.close()
        from lbatch.db import Database
        db = Database()
        cols = {row["name"] for row in db.conn.execute("PRAGMA table_info(groups)")}
        self.assertIn("note", cols)
        db.close()


if __name__ == "__main__":
    unittest.main()
