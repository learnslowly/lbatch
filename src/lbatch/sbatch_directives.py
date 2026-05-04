from __future__ import annotations

import shlex
from pathlib import Path

from .errors import LBatchError


def extract_directive_argv(script_path: str) -> list[str]:
    path = Path(script_path)
    if not path.exists():
        raise LBatchError(f"script not found: {script_path}")
    argv: list[str] = []
    with path.open() as handle:
        for raw_line in handle:
            stripped = raw_line.strip()
            if not stripped:
                continue
            if stripped.startswith("#SBATCH"):
                rest = stripped[len("#SBATCH") :].strip()
                if rest:
                    argv.extend(shlex.split(rest))
                continue
            if stripped.startswith("#"):
                continue
            break
    return argv
