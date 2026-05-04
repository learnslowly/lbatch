from __future__ import annotations

import argparse
import json
import sys

from .config import Config, Paths
from .db import Database, internal_group_id, public_group_id, utcnow
from .errors import LBatchError, ParseError
from .events import ingest_events
from .locks import FileLock
from .parser import parse_submission
from .recovery import recover_submitting
from .sbatch_directives import extract_directive_argv
from .scheduler import dispatch_once, run_daemon
from .slurm import SlurmClient
from .status import format_status, groups_text, status_data, units_text
from .submission import create_submission, dry_run_plan
from .version import __version__

RESERVED = {"daemon", "status", "config", "reconcile", "capacity-check", "release", "cancel", "doctor", "prune", "help", "version", "submit"}


def _db_config() -> tuple[Paths, Config, Database]:
    paths = Paths.defaults()
    paths.ensure()
    cfg = Config.load(paths)
    db = Database(paths)
    return paths, cfg, db


def _submission(argv: list[str]) -> int:
    db = None
    try:
        preliminary = parse_submission(argv)
        directive_tokens = extract_directive_argv(preliminary.script_path)
        submission = parse_submission(argv, directive_tokens)
        if submission.lbatch_options.get("dry_run"):
            print(json.dumps(dry_run_plan(submission), indent=2, sort_keys=True))
            return 0
        _, _, db = _db_config()
        group_id = create_submission(db, submission)
        if submission.parsable:
            print(group_id)
        else:
            row = db.conn.execute("SELECT array_count FROM groups WHERE group_id = ?", (internal_group_id(group_id),)).fetchone()
            print(f"Queued {group_id} ({row['array_count']} unit{'s' if row['array_count'] != 1 else ''})")
        return 0
    finally:
        if db is not None:
            db.close()


def cmd_daemon(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lbatch daemon")
    parser.add_argument("--max-remote-visible", type=int)
    parser.add_argument("--capacity-mode", choices=["local-owned", "slurm-on-demand", "manual"])
    parser.add_argument("--dispatch-batch-size", type=int)
    parser.add_argument("--sleep", type=float)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--foreground", action="store_true")
    ns = parser.parse_args(argv)
    paths, cfg, db = _db_config()
    if ns.dispatch_batch_size is not None:
        cfg.dispatch_batch_size = ns.dispatch_batch_size
    sleep = ns.sleep if ns.sleep is not None else cfg.sleep_seconds
    try:
        with FileLock(paths.state_dir / "daemon.lock"):
            run_daemon(db, cfg, SlurmClient(), ns.once, sleep, ns.max_remote_visible, ns.capacity_mode)
        return 0
    finally:
        db.close()


def cmd_status(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lbatch status")
    parser.add_argument("--groups", action="store_true")
    parser.add_argument("--units", action="store_true")
    parser.add_argument("--active", action="store_true",
                        help="show only currently-active counts (drop lifetime totals like Released, Total groups, Total units)")
    parser.add_argument("--json", action="store_true")
    ns = parser.parse_args(argv)
    _, cfg, db = _db_config()
    try:
        ingest_events(db)
        data = status_data(db, cfg)
        if ns.json:
            print(json.dumps(data, indent=2, sort_keys=True))
        else:
            print(format_status(data, active_only=ns.active))
            if ns.groups:
                print("\n" + groups_text(db))
            if ns.units:
                print("\n" + units_text(db))
        return 0
    finally:
        db.close()


def cmd_config(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lbatch config")
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("show")
    setp = sub.add_parser("set")
    setp.add_argument("key")
    setp.add_argument("value")
    ns = parser.parse_args(argv or ["show"])
    paths, cfg, db = _db_config()
    try:
        if ns.cmd == "set":
            if not hasattr(cfg, ns.key):
                raise LBatchError(f"unknown config key: {ns.key}")
            current = getattr(cfg, ns.key)
            value: object = ns.value
            if isinstance(current, bool):
                value = ns.value.lower() in {"1", "true", "yes", "on"}
            elif isinstance(current, int):
                value = int(ns.value)
            elif isinstance(current, float):
                value = float(ns.value)
            setattr(cfg, ns.key, value)
            cfg.save(paths)
            db.set_setting(ns.key, json.dumps(value))
            print(f"{ns.key}={value}")
        else:
            print(json.dumps(cfg.as_dict(), indent=2, sort_keys=True))
        return 0
    finally:
        db.close()


def cmd_capacity(argv: list[str]) -> int:
    _, cfg, db = _db_config()
    try:
        from .capacity import compute_capacity

        cap = compute_capacity(db, cfg.max_remote_visible, cfg.capacity_mode, SlurmClient())
        print(json.dumps({"mode": cap.mode, "max_remote_visible": cap.max_remote_visible, "occupied": cap.occupied, "available": cap.available}, indent=2))
        return 0
    finally:
        db.close()


def cmd_release(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lbatch release")
    parser.add_argument("unit_id", nargs="?")
    parser.add_argument("--group")
    ns = parser.parse_args(argv)
    _, _, db = _db_config()
    try:
        now = utcnow()
        if ns.group:
            group_id = internal_group_id(ns.group)
            cur = db.conn.execute(
                "UPDATE units SET state = 'FORCE_RELEASED', released_at = ?, updated_at = ? WHERE group_id = ? AND state = 'REMOTE_VISIBLE'",
                (now, now, group_id),
            )
        elif ns.unit_id:
            cur = db.conn.execute(
                "UPDATE units SET state = 'FORCE_RELEASED', released_at = ?, updated_at = ? WHERE unit_id = ? AND state = 'REMOTE_VISIBLE'",
                (now, now, ns.unit_id),
            )
        else:
            raise LBatchError("provide UNIT_ID or --group GROUP_ID")
        db.conn.commit()
        print(f"force released {cur.rowcount} unit(s)")
        return 0
    finally:
        db.close()


def cmd_reconcile(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lbatch reconcile")
    parser.add_argument("--since")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    ns = parser.parse_args(argv)
    _, _, db = _db_config()
    try:
        rows = db.conn.execute("SELECT unit_id, slurm_job_id FROM units WHERE state = 'REMOTE_VISIBLE' AND slurm_job_id IS NOT NULL").fetchall()
        visible = SlurmClient().squeue_visible_job_ids()
        missing = [row for row in rows if row["slurm_job_id"] not in visible]
        if ns.apply:
            now = utcnow()
            with db.transaction():
                for row in missing:
                    db.conn.execute(
                        "UPDATE units SET state = 'FORCE_RELEASED', released_at = ?, updated_at = ?, last_error = ? WHERE unit_id = ?",
                        (now, now, "force released by reconcile; no local event found", row["unit_id"]),
                    )
        print(json.dumps({"missing_remote_visible_events": [dict(row) for row in missing], "applied": bool(ns.apply)}, indent=2))
        return 0
    finally:
        db.close()


def cmd_cancel(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lbatch cancel")
    parser.add_argument("--unit")
    parser.add_argument("--group")
    parser.add_argument("--force-release", action="store_true")
    ns = parser.parse_args(argv)
    _, _, db = _db_config()
    try:
        if ns.unit:
            rows = db.conn.execute("SELECT unit_id, slurm_job_id FROM units WHERE unit_id = ? AND slurm_job_id IS NOT NULL", (ns.unit,)).fetchall()
        elif ns.group:
            rows = db.conn.execute("SELECT unit_id, slurm_job_id FROM units WHERE group_id = ? AND slurm_job_id IS NOT NULL", (internal_group_id(ns.group),)).fetchall()
        else:
            raise LBatchError("provide --unit UNIT_ID or --group GROUP_ID")
        slurm = SlurmClient()
        cancelled = 0
        for row in rows:
            proc = slurm.cancel(row["slurm_job_id"])
            if proc.returncode == 0:
                cancelled += 1
                if ns.force_release:
                    now = utcnow()
                    db.conn.execute("UPDATE units SET state = 'FORCE_RELEASED', released_at = ?, updated_at = ? WHERE unit_id = ?", (now, now, row["unit_id"]))
        db.conn.commit()
        print(f"cancelled {cancelled} Slurm job(s)")
        return 0
    finally:
        db.close()


def cmd_doctor(argv: list[str]) -> int:
    paths, cfg, db = _db_config()
    try:
        recovered = recover_submitting(db)
        print(f"Queue root: {paths.data_dir}")
        print(f"Database: {paths.db_path}")
        print(f"Config: {paths.config_path}")
        print(f"Recovered submitting units: {recovered}")
        return 0
    finally:
        db.close()


def cmd_prune(argv: list[str]) -> int:
    """Drop terminal-state rows from the queue so `lbatch status` reflects
    only currently-active work. Without prune, the DB carries every unit
    ever submitted, and lifetime totals (`Released`, `Total units`)
    accumulate forever, which makes per-run progress hard to read.
    """
    parser = argparse.ArgumentParser(prog="lbatch prune")
    parser.add_argument("--released", action="store_true",
                        help="prune RELEASED + FORCE_RELEASED units (default if no flag given)")
    parser.add_argument("--invalid", action="store_true",
                        help="also prune HELD_INVALID units")
    parser.add_argument("--all", action="store_true",
                        help="prune every terminal state (RELEASED, FORCE_RELEASED, HELD_INVALID)")
    parser.add_argument("--dry-run", action="store_true",
                        help="report counts only; don't touch the DB")
    ns = parser.parse_args(argv)
    states = []
    if ns.all:
        states = ["RELEASED", "FORCE_RELEASED", "HELD_INVALID"]
    else:
        if ns.released or not (ns.released or ns.invalid):
            states += ["RELEASED", "FORCE_RELEASED"]
        if ns.invalid:
            states += ["HELD_INVALID"]
    paths, cfg, db = _db_config()
    try:
        placeholders = ",".join("?" * len(states))
        n = db.conn.execute(
            f"SELECT COUNT(*) FROM units WHERE state IN ({placeholders})", states
        ).fetchone()[0]
        if ns.dry_run:
            print(f"would prune {n} unit row(s) in states: {', '.join(states)}")
            return 0
        with db.transaction():
            # ingested_events.unit_id has a FK on units(unit_id) without
            # ON DELETE CASCADE, so wipe the dependent rows first.
            db.conn.execute(
                f"DELETE FROM ingested_events WHERE unit_id IN "
                f"(SELECT unit_id FROM units WHERE state IN ({placeholders}))",
                states,
            )
            db.conn.execute(
                f"DELETE FROM units WHERE state IN ({placeholders})", states
            )
            # drop groups that have no units left (cascades to dependencies)
            db.conn.execute(
                "DELETE FROM groups WHERE group_id NOT IN (SELECT DISTINCT group_id FROM units)"
            )
        # vacuum so the .db file actually shrinks
        db.conn.execute("VACUUM")
        print(f"pruned {n} unit row(s) in states: {', '.join(states)}")
        return 0
    finally:
        db.close()


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        if not argv or argv[0] == "help":
            print("usage: lbatch [daemon|status|config|reconcile|capacity-check|release|cancel|doctor|prune|version|submit] ...")
            print("       lbatch [common sbatch options] script [script args...]")
            return 0
        cmd = argv[0]
        if cmd == "version":
            print(__version__)
            return 0
        if cmd == "daemon":
            return cmd_daemon(argv[1:])
        if cmd == "status":
            return cmd_status(argv[1:])
        if cmd == "config":
            return cmd_config(argv[1:])
        if cmd == "capacity-check":
            return cmd_capacity(argv[1:])
        if cmd == "release":
            return cmd_release(argv[1:])
        if cmd == "reconcile":
            return cmd_reconcile(argv[1:])
        if cmd == "cancel":
            return cmd_cancel(argv[1:])
        if cmd == "doctor":
            return cmd_doctor(argv[1:])
        if cmd == "prune":
            return cmd_prune(argv[1:])
        return _submission(argv)
    except (LBatchError, ParseError, OSError) as exc:
        print(f"lbatch: error: {exc}", file=sys.stderr)
        return 2
