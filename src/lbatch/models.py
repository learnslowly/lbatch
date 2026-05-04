from __future__ import annotations

from dataclasses import dataclass, field

TERMINAL_RELEASE_STATES = {"RELEASED", "FORCE_RELEASED"}
REMOTE_STATES = {"REMOTE_VISIBLE", "SUBMITTING"}


@dataclass
class SbatchOption:
    name: str
    value: str | None = None

    def argv(self) -> list[str]:
        if self.value is None:
            return [self.name]
        if self.name.startswith("--"):
            return [f"{self.name}={self.value}"]
        return [self.name, self.value]


@dataclass
class Submission:
    original_argv: list[str]
    lbatch_options: dict[str, object]
    sbatch_options: list[SbatchOption]
    script_path: str
    script_args: list[str]
    workdir: str
    parsable: bool = False
    local_dependencies: list[tuple[str, str]] = field(default_factory=list)
    external_dependency: str | None = None
