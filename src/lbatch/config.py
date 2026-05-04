from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

APP = "lbatch"


def _xdg_path(env: str, default_suffix: str) -> Path:
    base = os.environ.get(env)
    if base:
        return Path(base) / APP
    return Path.home() / default_suffix / APP


@dataclass(frozen=True)
class Paths:
    data_dir: Path
    db_path: Path
    events_dir: Path
    wrappers_dir: Path
    state_dir: Path
    config_path: Path

    @classmethod
    def defaults(cls) -> "Paths":
        data_dir = _xdg_path("XDG_DATA_HOME", ".local/share")
        state_dir = _xdg_path("XDG_STATE_HOME", ".local/state")
        config_dir = _xdg_path("XDG_CONFIG_HOME", ".config")
        return cls(
            data_dir=data_dir,
            db_path=data_dir / "lbatch.db",
            events_dir=data_dir / "events",
            wrappers_dir=data_dir / "wrappers",
            state_dir=state_dir,
            config_path=config_dir / "config.json",
        )

    def ensure(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.wrappers_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.config_path.parent.mkdir(parents=True, exist_ok=True)


@dataclass
class Config:
    max_remote_visible: int = 100
    dispatch_batch_size: int = 10
    sleep_seconds: float = 5.0
    capacity_mode: str = "local-owned"
    retryable_error_regexes: tuple[str, ...] = (
        "QOSMax.*Limit",
        "MaxJobs",
        "MaxSubmit",
        "Socket timed out",
        "Unable to contact slurm controller",
        "Resource temporarily unavailable",
    )
    backoff_initial_seconds: int = 60
    backoff_max_seconds: int = 900
    backoff_factor: int = 2

    @classmethod
    def load(cls, paths: Paths | None = None) -> "Config":
        paths = paths or Paths.defaults()
        cfg = cls()
        if paths.config_path.exists():
            data = json.loads(paths.config_path.read_text())
            for key, value in data.items():
                if hasattr(cfg, key):
                    setattr(cfg, key, value)
        if isinstance(cfg.retryable_error_regexes, list):
            cfg.retryable_error_regexes = tuple(cfg.retryable_error_regexes)
        return cfg

    def as_dict(self) -> dict[str, Any]:
        return {
            "max_remote_visible": self.max_remote_visible,
            "dispatch_batch_size": self.dispatch_batch_size,
            "sleep_seconds": self.sleep_seconds,
            "capacity_mode": self.capacity_mode,
            "retryable_error_regexes": list(self.retryable_error_regexes),
            "backoff_initial_seconds": self.backoff_initial_seconds,
            "backoff_max_seconds": self.backoff_max_seconds,
            "backoff_factor": self.backoff_factor,
        }

    def save(self, paths: Paths | None = None) -> None:
        paths = paths or Paths.defaults()
        paths.ensure()
        paths.config_path.write_text(json.dumps(self.as_dict(), indent=2, sort_keys=True) + "\n")
