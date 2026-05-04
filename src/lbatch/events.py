from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .db import Database, utcnow


def _try_unlink(path: Path) -> None:
    """Best-effort delete; never raise. Used to keep the events dir bounded."""
    try:
        path.unlink()
    except OSError:
        pass


def ingest_events(db: Database) -> int:
    count = 0
    for path in sorted(db.paths.events_dir.glob("*.release.json")):
        # Already ingested in a prior loop — drop the file so future loops
        # don't re-stat it. Without this, every dispatch_once does O(N)
        # DB lookups against ingested_events for every event ever processed.
        if db.conn.execute(
            "SELECT 1 FROM ingested_events WHERE event_path = ?", (str(path),)
        ).fetchone():
            _try_unlink(path)
            continue
        raw = path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        try:
            event = json.loads(raw.decode())
        except json.JSONDecodeError:
            # Malformed event — drop it so we don't repeatedly re-parse it.
            _try_unlink(path)
            continue
        if event.get("event_type") != "release" or not event.get("unit_id"):
            _try_unlink(path)
            continue
        unit_id = event["unit_id"]
        try:
            exit_code = int(event.get("exit_code")) if event.get("exit_code") not in (None, "") else None
        except ValueError:
            exit_code = None
        now = utcnow()
        # Set both flags inside the with-block; act on them after the
        # transaction successfully commits (or skips). On exception inside
        # the transaction, neither flag is set, so the event file is kept
        # for the next ingest pass.
        ingested = False
        orphan = False
        with db.transaction():
            row = db.conn.execute("SELECT state FROM units WHERE unit_id = ?", (unit_id,)).fetchone()
            if not row:
                orphan = True
            else:
                db.conn.execute(
                    "INSERT INTO ingested_events(event_path, unit_id, event_type, sha256, ingested_at) VALUES (?, ?, ?, ?, ?)",
                    (str(path), unit_id, "release", digest, now),
                )
                ingested = True
                if row["state"] not in {"RELEASED", "FORCE_RELEASED"}:
                    db.conn.execute(
                        "UPDATE units SET state = 'RELEASED', exit_code = ?, release_event_path = ?, released_at = ?, updated_at = ? WHERE unit_id = ?",
                        (exit_code, str(path), now, now, unit_id),
                    )
                    count += 1
        if ingested or orphan:
            _try_unlink(path)
    return count
