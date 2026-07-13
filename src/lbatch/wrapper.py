from __future__ import annotations

import os
import shlex
import stat
from pathlib import Path

from .config import Paths


def bash_quote(value: object) -> str:
    return shlex.quote("" if value is None else str(value))


def write_wrapper(paths: Paths, unit: dict, group: dict) -> str:
    paths.ensure()
    path = paths.wrappers_dir / f"{unit['unit_id']}.sh"
    array_task_id = "" if unit.get("array_task_id") is None else str(unit.get("array_task_id"))
    script_args = " ".join(bash_quote(arg) for arg in group.get("script_args", []))
    content = f"""#!/usr/bin/env bash
set +e

LBATCH_GROUP_ID={bash_quote(group['group_id'])}
LBATCH_UNIT_ID={bash_quote(unit['unit_id'])}
LBATCH_EVENT_DIR={bash_quote(paths.events_dir)}
LBATCH_ORIGINAL_SCRIPT={bash_quote(group['script_path'])}
LBATCH_ORIGINAL_CWD={bash_quote(group['workdir'])}
LBATCH_ARRAY_TASK_ID={bash_quote(array_task_id)}
LBATCH_ORIGINAL_ARRAY_SPEC={bash_quote(group.get('array_spec') or '')}
LBATCH_ARRAY_TASK_COUNT={bash_quote(group.get('array_count') or 1)}
LBATCH_ARRAY_TASK_MIN={bash_quote(group.get('array_min') or '')}
LBATCH_ARRAY_TASK_MAX={bash_quote(group.get('array_max') or '')}
LBATCH_ARRAY_TASK_STEP={bash_quote(group.get('array_step') or '')}
LBATCH_EVENT_WRITTEN=0

write_release_event() {{
    if [[ "$LBATCH_EVENT_WRITTEN" == "1" ]]; then
        return
    fi
    LBATCH_EVENT_WRITTEN=1
    local code="$1"
    local state="$2"
    local now
    now=$(date -Is)
    mkdir -p "$LBATCH_EVENT_DIR"
    local tmp="$LBATCH_EVENT_DIR/${{LBATCH_UNIT_ID}}.release.json.tmp.$$"
    local final="$LBATCH_EVENT_DIR/${{LBATCH_UNIT_ID}}.release.json"
    cat > "$tmp" <<EOF_JSON
{{
  "schema_version": 1,
  "event_type": "release",
  "group_id": "$LBATCH_GROUP_ID",
  "unit_id": "$LBATCH_UNIT_ID",
  "slurm_job_id": "${{SLURM_JOB_ID:-}}",
  "slurm_array_job_id": "${{SLURM_ARRAY_JOB_ID:-}}",
  "slurm_array_task_id": "${{SLURM_ARRAY_TASK_ID:-}}",
  "lbatch_array_task_id": "$LBATCH_ARRAY_TASK_ID",
  "exit_code": "$code",
  "terminal_state": "$state",
  "hostname": "$(hostname)",
  "timestamp": "$now"
}}
EOF_JSON
    mv "$tmp" "$final"
}}

on_exit() {{
    local code="$?"
    write_release_event "$code" "exit"
    exit "$code"
}}

on_term() {{
    trap - EXIT TERM
    write_release_event "143" "signal_term"
    exit 143
}}

on_int() {{
    trap - EXIT INT
    write_release_event "130" "signal_int"
    exit 130
}}

trap on_exit EXIT
trap on_term TERM
trap on_int INT

cd "$LBATCH_ORIGINAL_CWD" || exit 111
export LBATCH_GROUP_ID LBATCH_UNIT_ID
export LBATCH_ARRAY_TASK_ID LBATCH_ORIGINAL_ARRAY_SPEC
export LBATCH_ARRAY_TASK_COUNT LBATCH_ARRAY_TASK_MIN LBATCH_ARRAY_TASK_MAX LBATCH_ARRAY_TASK_STEP

# Defense in depth: if the stored script path lacks a '/', bash would treat
# "$LBATCH_ORIGINAL_SCRIPT" as a PATH lookup and fail to find the cwd-local
# script ('.' is not on PATH). Force a './' prefix so it resolves cwd-relative.
case "$LBATCH_ORIGINAL_SCRIPT" in
    */*) ;;
    *)   LBATCH_ORIGINAL_SCRIPT="./$LBATCH_ORIGINAL_SCRIPT" ;;
esac

if [[ -x "$LBATCH_ORIGINAL_SCRIPT" ]]; then
    "$LBATCH_ORIGINAL_SCRIPT" {script_args} "$@"
else
    bash "$LBATCH_ORIGINAL_SCRIPT" {script_args} "$@"
fi
"""
    path.write_text(content)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return str(path)
