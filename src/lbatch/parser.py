from __future__ import annotations

import os
from pathlib import Path

from .errors import ParseError, UnsupportedFeatureError
from .models import SbatchOption, Submission

LBATCH_VALUE_OPTIONS = {
    "--lbatch-name": "name",
    "--lbatch-priority": "priority",
    "--lbatch-afterany": "afterany",
    "--lbatch-afterok": "afterok",
    "--lbatch-afternotok": "afternotok",
    "--lbatch-max-remote": "max_remote",
}
LBATCH_FLAGS = {"--lbatch-dry-run": "dry_run", "--lbatch-no-start-daemon": "no_start_daemon"}
SBATCH_VALUE_OPTIONS = {
    "--array", "-a", "--partition", "-p", "--account", "-A", "--nodes", "-N",
    "--ntasks", "-n", "--cpus-per-task", "-c", "--time", "-t", "--job-name", "-J",
    "--output", "-o", "--error", "-e", "--export", "--dependency", "-d", "--mem",
    "--mem-per-cpu", "--gres", "--qos", "--constraint", "-C", "--mail-type",
    "--mail-user", "--chdir", "-D", "--signal",
}
SBATCH_FLAGS = {"--parsable"}
ALIASES = {
    "-a": "--array", "-p": "--partition", "-A": "--account", "-N": "--nodes",
    "-n": "--ntasks", "-c": "--cpus-per-task", "-t": "--time", "-J": "--job-name",
    "-o": "--output", "-e": "--error", "-d": "--dependency", "-C": "--constraint",
    "-D": "--chdir",
}
SINGLETONS = {
    "--array", "--partition", "--account", "--nodes", "--ntasks", "--cpus-per-task",
    "--time", "--job-name", "--output", "--error", "--export", "--dependency", "--mem",
    "--mem-per-cpu", "--gres", "--qos", "--constraint", "--mail-type", "--mail-user",
    "--chdir", "--signal",
}


def canonical(name: str) -> str:
    return ALIASES.get(name, name)


def parse_sbatch_tokens(tokens: list[str]) -> list[SbatchOption]:
    options: list[SbatchOption] = []
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token == "--wrap":
            raise UnsupportedFeatureError("--wrap")
        if token.startswith("--wrap="):
            raise UnsupportedFeatureError("--wrap")
        if token == ":":
            raise UnsupportedFeatureError("heterogeneous jobs using ':' syntax")
        if token.startswith("--") and "=" in token:
            name, value = token.split("=", 1)
            options.append(SbatchOption(canonical(name), value))
            i += 1
            continue
        if token in SBATCH_FLAGS:
            options.append(SbatchOption(canonical(token), None))
            i += 1
            continue
        if token in SBATCH_VALUE_OPTIONS:
            if i + 1 >= len(tokens):
                raise ParseError(f"option requires a value: {token}")
            options.append(SbatchOption(canonical(token), tokens[i + 1]))
            i += 2
            continue
        if token.startswith("-"):
            raise ParseError(f"ambiguous or unsupported option form: {token}; use --option=value when possible")
        raise ParseError(f"unexpected positional token while parsing options: {token}")
    return options


def merge_options(first: list[SbatchOption], second: list[SbatchOption]) -> list[SbatchOption]:
    result: list[SbatchOption] = []
    singleton_positions: dict[str, int] = {}
    for option in [*first, *second]:
        name = canonical(option.name)
        normalized = SbatchOption(name, option.value)
        if name in SINGLETONS:
            if name in singleton_positions:
                result[singleton_positions[name]] = normalized
            else:
                singleton_positions[name] = len(result)
                result.append(normalized)
        else:
            result.append(normalized)
    return result


def get_option(options: list[SbatchOption], name: str) -> str | None:
    for option in reversed(options):
        if canonical(option.name) == name:
            return option.value
    return None


def without_option(options: list[SbatchOption], name: str) -> list[SbatchOption]:
    return [option for option in options if canonical(option.name) != name]


def with_option(options: list[SbatchOption], name: str, value: str | None) -> list[SbatchOption]:
    return [*without_option(options, name), SbatchOption(name, value)]


def split_local_external_dependency(value: str | None) -> tuple[list[tuple[str, str]], str | None]:
    if not value:
        return [], None
    local: list[tuple[str, str]] = []
    external_specs: list[str] = []
    for spec in value.split(","):
        if ":" not in spec:
            external_specs.append(spec)
            continue
        dep_type, rest = spec.split(":", 1)
        local_ids: list[str] = []
        external_ids: list[str] = []
        parts = rest.split(":") if rest else []
        i = 0
        while i < len(parts):
            if parts[i] == "lb" and i + 1 < len(parts):
                group_id = f"lb:{parts[i + 1]}"
                local.append((dep_type, group_id))
                local_ids.append(group_id)
                i += 2
            elif parts[i].startswith("lb:"):
                local.append((dep_type, parts[i]))
                local_ids.append(parts[i])
                i += 1
            else:
                external_ids.append(parts[i])
                i += 1
        if external_ids:
            external_specs.append(f"{dep_type}:{':'.join(external_ids)}")
    return local, ",".join(external_specs) if external_specs else None


def parse_submission(argv: list[str], directive_tokens: list[str] | None = None) -> Submission:
    if not argv:
        raise ParseError("missing script path")
    if argv and argv[0] == "submit":
        argv = argv[1:]
    if "--" in argv:
        marker = argv.index("--")
        before, after = argv[:marker], argv[marker + 1 :]
    else:
        before, after = argv, []
    lbatch_options: dict[str, object] = {"priority": 0, "dry_run": False, "no_start_daemon": False}
    sbatch_tokens: list[str] = []
    script_path: str | None = None
    script_args: list[str] = []
    i = 0
    while i < len(before):
        token = before[i]
        if token.startswith("--lbatch-"):
            if "=" in token:
                name, value = token.split("=", 1)
                if name in LBATCH_VALUE_OPTIONS:
                    key = LBATCH_VALUE_OPTIONS[name]
                    if key.startswith("after"):
                        lbatch_options.setdefault("dependencies", []).append((key, value))
                    else:
                        lbatch_options[key] = int(value) if key in {"priority", "max_remote"} else value
                    i += 1
                    continue
            if token in LBATCH_FLAGS:
                lbatch_options[LBATCH_FLAGS[token]] = True
                i += 1
                continue
            if token in LBATCH_VALUE_OPTIONS:
                if i + 1 >= len(before):
                    raise ParseError(f"option requires a value: {token}")
                key = LBATCH_VALUE_OPTIONS[token]
                value = before[i + 1]
                if key.startswith("after"):
                    lbatch_options.setdefault("dependencies", []).append((key, value))
                else:
                    lbatch_options[key] = int(value) if key in {"priority", "max_remote"} else value
                i += 2
                continue
            raise ParseError(f"unknown lbatch control option: {token}")
        if token.startswith("-"):
            if token.startswith("--") and "=" in token:
                sbatch_tokens.append(token)
                i += 1
                continue
            if token in SBATCH_FLAGS:
                sbatch_tokens.append(token)
                i += 1
                continue
            if token in SBATCH_VALUE_OPTIONS:
                if i + 1 >= len(before):
                    raise ParseError(f"option requires a value: {token}")
                sbatch_tokens.extend([token, before[i + 1]])
                i += 2
                continue
            raise ParseError(f"ambiguous or unsupported option form: {token}; use --option=value when possible")
        script_path = token
        script_args = before[i + 1 :] + after
        break
    if script_path is None:
        raise ParseError("missing script path")
    cli_options = parse_sbatch_tokens(sbatch_tokens)
    directive_options = parse_sbatch_tokens(directive_tokens or [])
    options = merge_options(directive_options, cli_options)
    parsable = any(option.name == "--parsable" for option in options)
    local_from_external, external_dep = split_local_external_dependency(get_option(options, "--dependency"))
    options = without_option(options, "--parsable")
    if external_dep:
        options = with_option(options, "--dependency", external_dep)
    else:
        options = without_option(options, "--dependency")
    local_dependencies: list[tuple[str, str]] = []
    for dep_type, group_id in lbatch_options.get("dependencies", []):
        local_dependencies.append((dep_type, group_id))
    local_dependencies.extend(local_from_external)
    workdir = get_option(options, "--chdir") or os.getcwd()
    # Resolve script_path to absolute at submission time. The wrapper later
    # invokes "$LBATCH_ORIGINAL_SCRIPT" as a bare command, which goes through
    # PATH lookup; a relative script name in cwd would fail with "command not
    # found" because '.' is not on PATH. Resolving here makes the stored value
    # unambiguous and unblocks any cwd-shift between submit and dispatch.
    expanded = Path(script_path).expanduser()
    if not expanded.is_absolute():
        expanded = (Path(workdir) / expanded).resolve()
    return Submission(
        original_argv=argv,
        lbatch_options=lbatch_options,
        sbatch_options=options,
        script_path=str(expanded),
        script_args=script_args,
        workdir=workdir,
        parsable=parsable,
        local_dependencies=local_dependencies,
        external_dependency=external_dep,
    )
