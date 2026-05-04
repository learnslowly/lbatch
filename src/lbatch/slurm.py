from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass

from .models import SbatchOption


@dataclass
class SubmitResult:
    ok: bool
    job_id: str | None = None
    stdout: str = ""
    stderr: str = ""
    returncode: int = 0


class SlurmClient:
    def __init__(self, sbatch: str = "sbatch", squeue: str = "squeue", sacct: str = "sacct", scancel: str = "scancel"):
        self.sbatch = sbatch
        self.squeue = squeue
        self.sacct = sacct
        self.scancel = scancel

    def submit(self, options: list[SbatchOption], wrapper_path: str, script_args: list[str]) -> SubmitResult:
        argv = [self.sbatch, "--parsable"]
        for option in options:
            argv.extend(option.argv())
        argv.append(wrapper_path)
        argv.extend(script_args)
        proc = subprocess.run(argv, text=True, capture_output=True, check=False)
        job_id = parse_sbatch_job_id(proc.stdout) if proc.returncode == 0 else None
        return SubmitResult(proc.returncode == 0, job_id, proc.stdout, proc.stderr, proc.returncode)

    def squeue_visible_job_ids(self, user: str | None = None) -> set[str]:
        user = user or os.environ.get("USER", "")
        proc = subprocess.run(
            [self.squeue, "-h", "-u", user, "-r", "-o", "%i|%T|%j|%K"],
            text=True,
            capture_output=True,
            check=False,
        )
        if proc.returncode != 0:
            return set()
        ids: set[str] = set()
        visible = {"PENDING", "RUNNING", "CONFIGURING", "COMPLETING", "SUSPENDED"}
        for line in proc.stdout.splitlines():
            fields = line.split("|")
            if len(fields) >= 2 and fields[1] in visible:
                ids.add(fields[0].split("_")[0])
        return ids

    def sacct_jobs(self, job_ids: list[str]) -> dict[str, tuple[str, int | None]]:
        if not job_ids:
            return {}
        proc = subprocess.run(
            [self.sacct, "-n", "-P", "-j", ",".join(job_ids), "-o", "JobID,State,ExitCode"],
            text=True,
            capture_output=True,
            check=False,
        )
        result: dict[str, tuple[str, int | None]] = {}
        if proc.returncode != 0:
            return result
        for line in proc.stdout.splitlines():
            parts = line.strip().split("|")
            if len(parts) < 3 or "." in parts[0]:
                continue
            code = None
            if parts[2] and ":" in parts[2]:
                try:
                    code = int(parts[2].split(":", 1)[0])
                except ValueError:
                    code = None
            result[parts[0].split("_")[0]] = (parts[1], code)
        return result

    def cancel(self, job_id: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run([self.scancel, job_id], text=True, capture_output=True, check=False)


def parse_sbatch_job_id(output: str) -> str:
    """Extract a Slurm job id from sbatch --parsable output.

    The naive "first line, first integer" approach breaks on clusters that
    inject site lua hooks ahead of the parsable line, e.g. some sites print

        sbatch: 4252226.7 SUs available in <account>
        sbatch: 256.00 SUs estimated for this job.
        sbatch: lua: Submitted job 732234
        732234

    A naive parser would return the SU-balance number (4252226) instead of
    the real job id (732234), and lbatch would later track many units all
    pointing at the same phantom id.

    Robust strategy:
      1. Skip any line that starts with "sbatch:" (those are lua output).
      2. Among the remaining lines, walk from the BOTTOM up — sbatch writes
         the job id last, so the bottom-most pure-number-ish line is the
         job id. With --parsable that's "<jobid>" or "<jobid>;<cluster>".
      3. Fall back to the first integer found anywhere if nothing else
         matches (preserves old behaviour for clusters without lua hooks).
    """
    if not output:
        return ""
    candidate_lines = [
        ln.strip() for ln in output.strip().splitlines()
        if ln.strip() and not ln.lstrip().lower().startswith("sbatch:")
    ]
    for ln in reversed(candidate_lines):
        head = ln.split(";", 1)[0]
        if re.fullmatch(r"\d+", head):
            return head
    # Fallback: first integer anywhere in stdout.
    match = re.search(r"\b\d+\b", output)
    return match.group(0) if match else output.strip().splitlines()[0].strip()
